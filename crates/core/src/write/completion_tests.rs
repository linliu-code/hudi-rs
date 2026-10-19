/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
//! Real-store completion failures: publication may succeed before its response is lost.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

use crate::config::table::TableTypeValue;
use crate::storage::Storage;
use crate::table::{ReadOptions, Table};

#[derive(Debug)]
struct CompletionFailureStore {
    inner: Arc<dyn ObjectStore>,
    publish: bool,
    prefix: Path,
    target: Mutex<Option<Path>>,
    data_at_completion: Mutex<BTreeMap<Path, Bytes>>,
    deny_delete: Mutex<Vec<Path>>,
}

impl Display for CompletionFailureStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompletionFailureStore")
    }
}

async fn data_objects(store: &dyn ObjectStore, prefix: &Path) -> BTreeMap<Path, Bytes> {
    let files = store
        .list(Some(prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut data = BTreeMap::new();
    for file in files {
        let path = file.location.as_ref();
        if !path.contains("/.hoodie/") && (path.ends_with(".parquet") || path.contains(".log.")) {
            let bytes = store
                .get(&file.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            data.insert(file.location, bytes);
        }
    }
    data
}

#[async_trait]
impl ObjectStore for CompletionFailureStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let path = location.as_ref();
        let completion = !path.contains("/.hoodie/metadata/")
            && path.contains("/.hoodie/timeline/")
            && [".commit", ".deltacommit", ".replacecommit"]
                .iter()
                .any(|suffix| path.ends_with(suffix));
        let inject = completion && {
            let mut target = self.target.lock().unwrap();
            if target.is_none() {
                *target = Some(location.clone());
                true
            } else {
                false
            }
        };
        if inject {
            *self.data_at_completion.lock().unwrap() =
                data_objects(self.inner.as_ref(), &self.prefix).await;
            if self.publish {
                self.inner.put_opts(location, payload, opts).await?;
            }
            return Err(object_store::Error::Generic {
                store: "completion failure test",
                source: std::io::Error::other(if self.publish {
                    "lost completion response after durable success"
                } else {
                    "completion failed before publication"
                })
                .into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let denied = self.deny_delete.lock().unwrap().clone();
        locations
            .then(move |location| {
                let inner = inner.clone();
                let denied = denied.clone();
                async move {
                    let path = location?;
                    if denied.contains(&path) {
                        return Err(object_store::Error::Generic {
                            store: "completion failure test",
                            source: std::io::Error::other("injected fence deletion failure").into(),
                        });
                    }
                    inner.delete(&path).await?;
                    Ok(path)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Append,
    Upsert,
    Update,
    Delete,
    Overwrite,
}

fn batch(rows: &[(&str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn rows(table: &Table) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in table.read(&ReadOptions::new()).await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i).to_string(), values.value(i))));
    }
    rows.sort();
    rows
}

async fn assert_objects_preserved(store: &dyn ObjectStore, expected: &BTreeMap<Path, Bytes>) {
    for (path, bytes) in expected {
        assert_eq!(
            &store.get(path).await.unwrap().bytes().await.unwrap(),
            bytes,
            "changed data: {path}"
        );
    }
}

async fn completion_error_case(
    operation: Operation,
    mor: bool,
    version: &str,
    metadata: bool,
    publish: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut table = Table::create(uri)
        .with_table_name("completion_failure")
        .with_record_key_fields(["id"])
        .with_table_type(if mor {
            TableTypeValue::MergeOnRead
        } else {
            TableTypeValue::CopyOnWrite
        })
        .with_table_version(version.parse().unwrap())
        .with_metadata(metadata)
        .with_record_index(metadata)
        .with_column_stats(metadata)
        .create()
        .await
        .unwrap();
    assert_eq!(table.hudi_options()["hoodie.table.version"], version);
    table
        .append([batch(&[("seed", 1), ("keep", 3)])])
        .await
        .unwrap();
    let prefix = Path::from_absolute_path(dir.path()).unwrap();
    let original = data_objects(
        table.file_system_view.storage.object_store.as_ref(),
        &prefix,
    )
    .await;
    let fault = Arc::new(CompletionFailureStore {
        inner: table.file_system_view.storage.object_store.clone(),
        publish,
        prefix,
        target: Mutex::new(None),
        data_at_completion: Mutex::new(BTreeMap::new()),
        deny_delete: Mutex::new(Vec::new()),
    });
    table.file_system_view.storage = Storage::new_with_object_store(
        (*table.file_system_view.storage.base_url).clone(),
        fault.clone(),
        table.hudi_configs.clone(),
    );
    let result = match operation {
        Operation::Append => table.append([batch(&[("durable", 2)])]).await.map(|_| ()),
        Operation::Upsert => table.upsert([batch(&[("seed", 2)])]).await.map(|_| ()),
        Operation::Update => {
            let updates = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![2]))],
            )
            .unwrap();
            table.update("v = 1", updates).await.map(|_| ())
        }
        Operation::Delete => table.delete("v = 1").await.map(|_| ()),
        Operation::Overwrite => table
            .overwrite([batch(&[("durable", 2)])])
            .await
            .map(|_| ()),
    };
    assert!(result.is_err());
    let target = fault
        .target
        .lock()
        .unwrap()
        .clone()
        .expect("write must reach completion PUT");
    assert_eq!(
        fault.inner.get(&target).await.is_ok(),
        publish,
        "durable completion status"
    );
    let instant: String = target
        .filename()
        .unwrap()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    let markers = super::markers::read_markers(table.file_system_view.storage.as_ref(), &instant)
        .await
        .unwrap();
    assert!(
        !markers.is_empty(),
        "retain recovery markers on ambiguous completion"
    );
    let fences = table
        .file_system_view
        .storage
        .list_files(Some(".hoodie/timeline"))
        .await
        .unwrap();
    assert!(
        fences
            .iter()
            .any(|file| file.name.starts_with(&instant) && file.name.ends_with(".requested"))
    );
    assert!(
        fences
            .iter()
            .any(|file| file.name.starts_with(&instant) && file.name.ends_with(".inflight"))
    );
    let expected: Vec<_> = if !publish {
        vec![("keep", 3), ("seed", 1)]
    } else {
        match operation {
            Operation::Append => vec![("durable", 2), ("keep", 3), ("seed", 1)],
            Operation::Upsert | Operation::Update => vec![("keep", 3), ("seed", 2)],
            Operation::Delete => vec![("keep", 3)],
            Operation::Overwrite => vec![("durable", 2)],
        }
    }
    .into_iter()
    .map(|(id, v)| (id.to_string(), v))
    .collect();
    assert_objects_preserved(fault.inner.as_ref(), &original).await;
    let mut reopened = Table::new(uri).await.unwrap();
    assert_eq!(
        rows(&reopened).await,
        expected,
        "reopened snapshot after completion error"
    );
    let at_completion = fault.data_at_completion.lock().unwrap().clone();
    assert_objects_preserved(fault.inner.as_ref(), &at_completion).await;
    // A subsequent public write runs eager recovery; failed publication rolls back,
    // whereas a published completion must protect every referenced base/log file.
    reopened.append([batch(&[("later", 4)])]).await.unwrap();
    let mut after_recovery = expected;
    after_recovery.push(("later".to_string(), 4));
    after_recovery.sort();
    assert_eq!(rows(&Table::new(uri).await.unwrap()).await, after_recovery);
    assert_objects_preserved(fault.inner.as_ref(), &original).await;
    if publish {
        assert_objects_preserved(fault.inner.as_ref(), &at_completion).await;
    } else {
        for marker in markers {
            let (path, _) = super::markers::parse_marker_name(&marker).unwrap();
            assert!(
                reopened
                    .file_system_view
                    .storage
                    .get_file_data(&path)
                    .await
                    .is_err(),
                "rollback left {path}"
            );
        }
        assert!(
            super::markers::read_markers(reopened.file_system_view.storage.as_ref(), &instant)
                .await
                .unwrap()
                .is_empty()
        );
    }
    println!(
        "verified {operation:?}, mor={mor}, tv={version}, metadata={metadata}, publish={publish}"
    );
}

async fn completion_error_matrix(operation: Operation, mor: bool) {
    for version in ["8", "9"] {
        for metadata in [false, true] {
            for publish in [true, false] {
                completion_error_case(operation, mor, version, metadata, publish).await;
            }
        }
    }
}

macro_rules! completion_error_test {
    ($name:ident, $operation:ident, $mor:literal) => {
        #[tokio::test]
        async fn $name() {
            completion_error_matrix(Operation::$operation, $mor).await;
        }
    };
}

completion_error_test!(test_completion_error_append_cow, Append, false);
completion_error_test!(test_completion_error_append_mor, Append, true);
completion_error_test!(test_completion_error_upsert_cow, Upsert, false);
completion_error_test!(test_completion_error_upsert_mor, Upsert, true);
completion_error_test!(test_completion_error_update_cow, Update, false);
completion_error_test!(test_completion_error_update_mor, Update, true);
completion_error_test!(test_completion_error_delete_cow, Delete, false);
completion_error_test!(test_completion_error_delete_mor, Delete, true);
completion_error_test!(test_completion_error_overwrite_cow, Overwrite, false);

#[tokio::test]
async fn test_completion_error_survives_failed_archival_fence_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut table = Table::create(uri)
        .with_table_name("completion_archive_failure")
        .with_table_version(9)
        .with_record_key_fields(["id"])
        .with_metadata(false)
        .with_record_index(false)
        .with_column_stats(false)
        .with_option("hoodie.keep.min.commits", "1")
        .with_option("hoodie.keep.max.commits", "2")
        .create()
        .await
        .unwrap();
    table.append([batch(&[("seed", 1)])]).await.unwrap();
    let fault = Arc::new(CompletionFailureStore {
        inner: table.file_system_view.storage.object_store.clone(),
        publish: true,
        prefix: Path::from_absolute_path(dir.path()).unwrap(),
        target: Mutex::new(None),
        data_at_completion: Mutex::new(BTreeMap::new()),
        deny_delete: Mutex::new(Vec::new()),
    });
    table.file_system_view.storage = Storage::new_with_object_store(
        (*table.file_system_view.storage.base_url).clone(),
        fault.clone(),
        table.hudi_configs.clone(),
    );
    assert!(table.append([batch(&[("durable", 2)])]).await.is_err());
    let completed = fault.target.lock().unwrap().clone().unwrap();
    assert!(fault.inner.get(&completed).await.is_ok());
    let instant: String = completed
        .filename()
        .unwrap()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    let timeline_prefix = Path::from_absolute_path(dir.path().join(".hoodie/timeline")).unwrap();
    let fences: Vec<_> = fault
        .inner
        .list(Some(&timeline_prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .filter(|file| {
            let name = file.location.filename().unwrap();
            name.starts_with(&instant)
                && (name.ends_with(".requested") || name.ends_with(".inflight"))
        })
        .map(|file| file.location)
        .collect();
    assert_eq!(fences.len(), 2);
    *fault.deny_delete.lock().unwrap() = fences.clone();
    table.append([batch(&[("later", 3)])]).await.unwrap();
    // The completed commit reached archival; only its fence deletions fail.
    let archived = super::archival::archived_instant_times(
        table.file_system_view.storage.as_ref(),
        ".hoodie/timeline",
    )
    .await
    .unwrap();
    assert!(
        archived.contains(&instant),
        "must exercise published archive cleanup"
    );
    for fence in &fences {
        assert!(fault.inner.get(fence).await.is_ok());
    }
    println!(
        "archived completion still active: {}",
        fault.inner.get(&completed).await.is_ok()
    );
    fault.deny_delete.lock().unwrap().clear();
    // Healthy recovery must not reinterpret the archived committed write as failed.
    table
        .append([batch(&[("after_archive", 4)])])
        .await
        .unwrap();
    assert_eq!(
        rows(&Table::new(uri).await.unwrap()).await,
        vec![
            ("after_archive".to_string(), 4),
            ("durable".to_string(), 2),
            ("later".to_string(), 3),
            ("seed".to_string(), 1),
        ]
    );
    let committed_data = fault.data_at_completion.lock().unwrap().clone();
    assert_objects_preserved(fault.inner.as_ref(), &committed_data).await;
}
