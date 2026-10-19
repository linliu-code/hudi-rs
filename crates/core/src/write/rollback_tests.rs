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
//! Real-store rollback failures must retain the evidence needed for safe retry.

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
use crate::index::{HoodieIndex, HoodieKey, RecordIndex};
use crate::storage::Storage;
use crate::table::{ReadOptions, Table};

#[derive(Clone, Copy, Debug)]
enum PutFailure {
    Data,
    MetadataLog,
    MetadataCompletion,
    DataCompletion,
}

#[derive(Clone, Copy, Debug)]
enum CleanupFailure {
    DataDelete,
    MetadataTimelineListing,
    MetadataRootListing,
    MetadataPartitionListing,
    MetadataLogDelete,
    MetadataInstantDelete,
    DataFenceDelete,
}

#[derive(Debug)]
struct RollbackFailureStore {
    inner: Arc<dyn ObjectStore>,
    failure: PutFailure,
    target: Mutex<Option<Path>>,
    deny_delete: Mutex<Option<Path>>,
    deny_listing: Mutex<Option<Path>>,
}

impl Display for RollbackFailureStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RollbackFailureStore")
    }
}

fn fault(message: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "rollback failure test",
        source: std::io::Error::other(message).into(),
    }
}

#[async_trait]
impl ObjectStore for RollbackFailureStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let path = location.as_ref();
        let metadata = path.contains("/.hoodie/metadata/");
        let matches = match self.failure {
            PutFailure::Data => {
                !path.contains("/.hoodie/")
                    && (path.ends_with(".parquet") || path.contains(".log."))
            }
            PutFailure::MetadataLog => {
                metadata && path.contains("/record_index/.") && path.contains(".log.")
            }
            PutFailure::MetadataCompletion => metadata && path.ends_with(".deltacommit"),
            PutFailure::DataCompletion => {
                !metadata && path.contains("/.hoodie/timeline/") && path.ends_with(".commit")
            }
        };
        let inject = matches && {
            let mut target = self.target.lock().unwrap();
            if target.is_none() {
                *target = Some(location.clone());
                true
            } else {
                false
            }
        };
        if inject {
            if matches!(self.failure, PutFailure::Data | PutFailure::MetadataLog) {
                self.inner.put_opts(location, payload, opts).await?;
            }
            return Err(fault("injected PUT response failure"));
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
                    if denied.as_ref() == Some(&path) {
                        return Err(fault("injected rollback DELETE failure"));
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
        if self
            .deny_listing
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|path| Some(path) == prefix)
        {
            return Err(fault("injected rollback LIST failure"));
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn batch(id: &str, value: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![id])) as ArrayRef,
            Arc::new(Int64Array::from(vec![value])),
        ],
    )
    .unwrap()
}

async fn rows(table: &Table) -> Vec<(String, i64)> {
    let mut result = Vec::new();
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
        result.extend((0..batch.num_rows()).map(|i| (ids.value(i).to_string(), values.value(i))));
    }
    result.sort();
    result
}

async fn objects(store: &dyn ObjectStore, prefix: &Path) -> BTreeMap<Path, Bytes> {
    let mut objects = BTreeMap::new();
    for object in store
        .list(Some(prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
    {
        objects.insert(
            object.location.clone(),
            store
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        );
    }
    objects
}

async fn rollback_case(
    version: isize,
    mor: bool,
    put: PutFailure,
    cleanup: Option<CleanupFailure>,
) {
    let metadata = !matches!(put, PutFailure::Data);
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut table = Table::create(uri)
        .with_table_name("rollback_failure")
        .with_table_version(version)
        .with_table_type(if mor {
            TableTypeValue::MergeOnRead
        } else {
            TableTypeValue::CopyOnWrite
        })
        .with_record_key_fields(["id"])
        .with_metadata(metadata)
        .with_record_index(metadata)
        .with_column_stats(metadata)
        .with_option("hoodie.keep.min.commits", "1")
        .with_option("hoodie.keep.max.commits", "2")
        .create()
        .await
        .unwrap();
    assert_eq!(
        table.hudi_options()["hoodie.table.version"],
        version.to_string()
    );
    table.append([batch("seed", 1)]).await.unwrap();
    let prefix = Path::from_absolute_path(dir.path()).unwrap();
    let before = objects(
        table.file_system_view.storage.object_store.as_ref(),
        &prefix,
    )
    .await;
    let fault = Arc::new(RollbackFailureStore {
        inner: table.file_system_view.storage.object_store.clone(),
        failure: put,
        target: Mutex::new(None),
        deny_delete: Mutex::new(None),
        deny_listing: Mutex::new(None),
    });
    table.file_system_view.storage = Storage::new_with_object_store(
        (*table.file_system_view.storage.base_url).clone(),
        fault.clone(),
        table.hudi_configs.clone(),
    );
    let failed = if mor {
        table.upsert([batch("seed", 99)]).await.map(|_| ())
    } else {
        table.append([batch("ghost", 99)]).await.map(|_| ())
    };
    assert!(failed.is_err());
    let target = fault
        .target
        .lock()
        .unwrap()
        .clone()
        .expect("fault boundary reached");
    assert_eq!(
        fault.inner.get(&target).await.is_ok(),
        matches!(put, PutFailure::Data | PutFailure::MetadataLog)
    );
    let pending =
        super::find_pending_instants(table.file_system_view.storage.as_ref(), ".hoodie/timeline")
            .await
            .unwrap();
    assert_eq!(pending.len(), 1);
    let instant = pending[0].timestamp.clone();
    let marker_prefix =
        Path::from_absolute_path(dir.path().join(".hoodie/.temp").join(&instant)).unwrap();
    let markers = objects(fault.inner.as_ref(), &marker_prefix).await;
    assert!(!markers.is_empty());
    let metadata_objects: Vec<_> = objects(fault.inner.as_ref(), &prefix)
        .await
        .into_keys()
        .filter(|path| {
            path.as_ref().contains("/.hoodie/metadata/")
                && path.filename().unwrap().contains(&instant)
        })
        .collect();
    if metadata {
        assert!(
            metadata_objects
                .iter()
                .any(|path| path.as_ref().contains(".log."))
        );
        let mdt_fences = metadata_objects
            .iter()
            .filter(|path| path.as_ref().contains("/.hoodie/timeline/"))
            .count();
        assert_eq!(
            mdt_fences,
            match put {
                PutFailure::MetadataLog => 0,
                PutFailure::MetadataCompletion => 2,
                PutFailure::DataCompletion => 3,
                PutFailure::Data => unreachable!(),
            }
        );
    }
    if let Some(cleanup) = cleanup {
        match cleanup {
            CleanupFailure::DataDelete => *fault.deny_delete.lock().unwrap() = Some(target.clone()),
            CleanupFailure::MetadataTimelineListing => {
                *fault.deny_listing.lock().unwrap() = Some(
                    Path::from_absolute_path(dir.path().join(".hoodie/metadata/.hoodie/timeline"))
                        .unwrap(),
                )
            }
            CleanupFailure::MetadataRootListing => {
                *fault.deny_listing.lock().unwrap() =
                    Some(Path::from_absolute_path(dir.path().join(".hoodie/metadata")).unwrap())
            }
            CleanupFailure::MetadataPartitionListing => {
                *fault.deny_listing.lock().unwrap() = Some(
                    Path::from_absolute_path(dir.path().join(".hoodie/metadata/record_index"))
                        .unwrap(),
                )
            }
            CleanupFailure::MetadataLogDelete => {
                *fault.deny_delete.lock().unwrap() = metadata_objects
                    .iter()
                    .find(|path| {
                        path.as_ref().contains("/record_index/") && path.as_ref().contains(".log.")
                    })
                    .cloned()
            }
            CleanupFailure::MetadataInstantDelete => {
                *fault.deny_delete.lock().unwrap() = metadata_objects
                    .iter()
                    .find(|path| path.as_ref().ends_with(".deltacommit"))
                    .cloned()
            }
            CleanupFailure::DataFenceDelete => {
                *fault.deny_delete.lock().unwrap() = Some(
                    Path::from_absolute_path(
                        dir.path()
                            .join(".hoodie/timeline")
                            .join(format!("{instant}.commit.requested")),
                    )
                    .unwrap(),
                )
            }
        }
        let after_failure = table.append([batch("blocked", 2)]).await;
        assert!(
            after_failure.is_err(),
            "failed recovery must block new writes: tv={version}, mor={mor}, put={put:?}, cleanup={cleanup:?}"
        );
        for (path, bytes) in &markers {
            assert_eq!(
                &fault.inner.get(path).await.unwrap().bytes().await.unwrap(),
                bytes,
                "preserve markers on failed recovery"
            );
        }
        let remaining = super::find_pending_instants(
            table.file_system_view.storage.as_ref(),
            ".hoodie/timeline",
        )
        .await
        .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].timestamp, instant);
        if !matches!(cleanup, CleanupFailure::DataFenceDelete) {
            assert_eq!(
                remaining[0].fencing_files.len(),
                2,
                "all original fences must remain until file cleanup succeeds"
            );
        }
        *fault.deny_delete.lock().unwrap() = None;
        *fault.deny_listing.lock().unwrap() = None;
    }
    table.append([batch("next", 2)]).await.unwrap();
    if metadata {
        let key = HoodieKey {
            record_key: "ghost".to_string(),
            partition_path: String::new(),
        };
        let tagged = RecordIndex
            .tag_location(&table, std::slice::from_ref(&key))
            .await
            .unwrap();
        assert!(
            tagged[&key].is_none(),
            "rollback must not resurrect unfinished MDT record-index logs: {put:?}, location={:?}",
            tagged[&key]
        );
        for path in &metadata_objects {
            assert!(
                fault.inner.get(path).await.is_err(),
                "orphan MDT object survived: {path}"
            );
        }
    }
    assert!(
        super::find_pending_instants(table.file_system_view.storage.as_ref(), ".hoodie/timeline")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        objects(fault.inner.as_ref(), &marker_prefix)
            .await
            .is_empty()
    );
    table.append([batch("final", 3)]).await.unwrap();
    let reopened = Table::new(uri).await.unwrap();
    assert!(
        reopened
            .timeline
            .all_pending_instant_times()
            .await
            .unwrap()
            .is_empty(),
        "healthy retry must not leave an incomplete rollback pinning archival"
    );
    assert_eq!(
        rows(&reopened).await,
        vec![
            ("final".to_string(), 3),
            ("next".to_string(), 2),
            ("seed".to_string(), 1)
        ]
    );
    assert!(
        reopened.timeline.earliest_active_instant.as_ref().unwrap() > &instant,
        "test must advance archival beyond orphan timestamp"
    );
    assert!(
        !crate::write::archival::archived_instant_times(
            reopened.file_system_view.storage.as_ref(),
            ".hoodie/timeline"
        )
        .await
        .unwrap()
        .is_empty()
    );
    if matches!(put, PutFailure::Data) {
        assert!(fault.inner.get(&target).await.is_err());
    }
    for (path, bytes) in before {
        if !path.as_ref().contains("/.hoodie/") {
            assert_eq!(
                fault.inner.get(&path).await.unwrap().bytes().await.unwrap(),
                bytes,
                "prior committed data changed: {path}"
            );
        }
    }
    println!("verified tv={version}, mor={mor}, put={put:?}, cleanup={cleanup:?}");
}

macro_rules! rollback_test {
    ($name:ident, $mor:literal, $put:ident, $cleanup:expr) => {
        #[tokio::test]
        async fn $name() {
            for version in [8, 9] {
                rollback_case(version, $mor, PutFailure::$put, $cleanup).await;
            }
        }
    };
}

rollback_test!(
    test_rollback_data_delete_failure_cow,
    false,
    Data,
    Some(CleanupFailure::DataDelete)
);
rollback_test!(
    test_rollback_data_delete_failure_mor,
    true,
    Data,
    Some(CleanupFailure::DataDelete)
);
rollback_test!(
    test_rollback_metadata_without_timeline,
    false,
    MetadataLog,
    None
);
rollback_test!(
    test_rollback_metadata_without_completion,
    false,
    MetadataCompletion,
    None
);
rollback_test!(
    test_rollback_metadata_timeline_listing_failure,
    false,
    DataCompletion,
    Some(CleanupFailure::MetadataTimelineListing)
);
rollback_test!(
    test_rollback_metadata_root_listing_failure,
    false,
    DataCompletion,
    Some(CleanupFailure::MetadataRootListing)
);
rollback_test!(
    test_rollback_metadata_partition_listing_failure,
    false,
    DataCompletion,
    Some(CleanupFailure::MetadataPartitionListing)
);
rollback_test!(
    test_rollback_metadata_log_delete_failure,
    false,
    DataCompletion,
    Some(CleanupFailure::MetadataLogDelete)
);
rollback_test!(
    test_rollback_metadata_instant_delete_failure,
    false,
    DataCompletion,
    Some(CleanupFailure::MetadataInstantDelete)
);
rollback_test!(
    test_rollback_fence_delete_failure,
    false,
    DataCompletion,
    Some(CleanupFailure::DataFenceDelete)
);
