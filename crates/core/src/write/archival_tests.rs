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
//! Archival retry coverage through the public writer and a real object store.

use super::*;
use crate::config::table::TableTypeValue;
use crate::table::{ReadOptions, Table};
use arrow_array::Int64Array;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;

type Result<T> = object_store::Result<T>;

#[derive(Debug)]
struct ArchiveFailureStore {
    inner: Arc<dyn ObjectStore>,
    deny_delete: Mutex<Vec<Path>>,
    // History suffix, whether to persist before returning the error, fired path.
    fail_put: Mutex<Option<(String, bool)>>,
    fired: Mutex<Vec<Path>>,
}
impl Display for ArchiveFailureStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArchiveFailureStore")
    }
}
#[async_trait]
impl ObjectStore for ArchiveFailureStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let fail = {
            let mut fault = self.fail_put.lock().unwrap();
            if fault.as_ref().is_some_and(|(suffix, _)| {
                location.as_ref().contains("/history/") && location.as_ref().ends_with(suffix)
            }) {
                fault.take()
            } else {
                None
            }
        };
        if let Some((_, persist)) = fail {
            self.fired.lock().unwrap().push(location.clone());
            if persist {
                self.inner.put_opts(location, payload, opts).await?;
            }
            return Err(object_store::Error::Generic {
                store: "archive failure test",
                source: std::io::Error::other("injected archive publication failure").into(),
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
                            store: "archive failure test",
                            source: std::io::Error::other("injected completed deletion failure")
                                .into(),
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

fn batch(id: &str) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![id])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap()
}
async fn create(dir: &std::path::Path, version: isize, mor: bool) -> Table {
    Table::create(dir.to_str().unwrap())
        .with_table_name("archive_retry")
        .with_record_key_fields(["id"])
        .with_table_version(version)
        .with_table_type(if mor {
            TableTypeValue::MergeOnRead
        } else {
            TableTypeValue::CopyOnWrite
        })
        .with_metadata(false)
        .with_record_index(false)
        .with_column_stats(false)
        .with_option("hoodie.keep.min.commits", "1")
        .with_option("hoodie.keep.max.commits", "3")
        .create()
        .await
        .unwrap()
}
fn inject(table: &mut Table) -> Arc<ArchiveFailureStore> {
    let store = Arc::new(ArchiveFailureStore {
        inner: table.file_system_view.storage.object_store.clone(),
        deny_delete: Mutex::new(vec![]),
        fail_put: Mutex::new(None),
        fired: Mutex::new(vec![]),
    });
    table.file_system_view.storage = Storage::new_with_object_store(
        (*table.file_system_view.storage.base_url).clone(),
        store.clone(),
        table.hudi_configs.clone(),
    );
    store
}
fn history_bytes(dir: &std::path::Path) -> BTreeMap<String, Bytes> {
    std::fs::read_dir(dir.join(".hoodie/timeline/history"))
        .into_iter()
        .flatten()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().to_str().unwrap().to_owned(),
                Bytes::from(std::fs::read(entry.path()).unwrap()),
            )
        })
        .filter(|(name, _)| name != "_version_")
        .collect()
}
fn assert_immutable(dir: &std::path::Path, before: &BTreeMap<String, Bytes>) {
    let after = history_bytes(dir);
    for (name, bytes) in before {
        assert!(
            after.get(name) == Some(bytes),
            "published object changed: {name}"
        );
    }
}
async fn ids(table: &Table) -> Vec<String> {
    let mut ids = vec![];
    for batch in table.read(&ReadOptions::new()).await.unwrap() {
        ids.extend(
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.unwrap().to_string()),
        );
    }
    ids.sort();
    ids
}
#[tokio::test]
async fn test_archive_retry_preserves_published_middle_instant() {
    for version in [8, 9] {
        for mor in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = create(dir.path(), version, mor).await;
            let a = table.append([batch("a")]).await.unwrap();
            let b = table.append([batch("b")]).await.unwrap();
            let c = table.append([batch("c")]).await.unwrap();
            let store = inject(&mut table);
            for rel in [&a.commit_relative_path, &c.commit_relative_path] {
                store
                    .deny_delete
                    .lock()
                    .unwrap()
                    .push(Path::from_absolute_path(dir.path().join(rel)).unwrap());
            }
            table.overwrite([batch("replacement")]).await.unwrap();
            assert!(dir.path().join(&a.commit_relative_path).exists());
            assert!(!dir.path().join(&b.commit_relative_path).exists());
            assert!(dir.path().join(&c.commit_relative_path).exists());
            let archived =
                archived_instant_times(&table.file_system_view.storage, ".hoodie/timeline")
                    .await
                    .unwrap();
            assert!(archived.contains(&b.instant));
            let before = history_bytes(dir.path());
            let expected = archive_records(&table).await;
            store.deny_delete.lock().unwrap().clear();
            table.append([batch("later")]).await.unwrap();
            assert_immutable(dir.path(), &before);
            let archived =
                archived_instant_times(&table.file_system_view.storage, ".hoodie/timeline")
                    .await
                    .unwrap();
            for instant in [&a.instant, &b.instant, &c.instant] {
                assert!(archived.contains(instant));
            }
            let (_, manifest) =
                read_manifest(&table.file_system_view.storage, ".hoodie/timeline/history")
                    .await
                    .unwrap();
            let names: std::collections::HashSet<_> =
                manifest.files.iter().map(|f| &f.file_name).collect();
            assert_eq!(names.len(), manifest.files.len());
            assert!(!dir.path().join(&a.commit_relative_path).exists());
            assert!(!dir.path().join(&c.commit_relative_path).exists());
            let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            assert_eq!(ids(&reopened).await, vec!["later", "replacement"]);
            assert_eq!(archive_records(&table).await, expected);
            export_fixture(
                dir.path(),
                &format!("partial-v{version}-mor{mor}"),
                &expected,
            );
        }
    }
}

async fn archive_records(table: &Table) -> BTreeMap<(String, String), LsmRecord> {
    let storage = &table.file_system_view.storage;
    let (_, manifest) = read_manifest(storage, ".hoodie/timeline/history")
        .await
        .unwrap();
    let mut records = BTreeMap::new();
    for entry in manifest.files {
        for record in read_archive_file(storage, ".hoodie/timeline/history", &entry)
            .await
            .unwrap()
        {
            let key = (record.instant.clone(), record.action.clone());
            assert!(
                records.insert(key.clone(), record).is_none(),
                "duplicate archived identity {key:?}"
            );
        }
    }
    records
}
fn export_fixture(
    dir: &std::path::Path,
    name: &str,
    records: &BTreeMap<(String, String), LsmRecord>,
) {
    let Ok(export) = std::env::var("HUDI_ARCHIVE_RETRY_EXPORT") else {
        return;
    };
    fn copy(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy(&entry.path(), &target)
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let target = std::path::Path::new(&export).join(name);
    copy(dir, &target);
    let expected:Vec<_>=records.values().map(|r| serde_json::json!({"instant":r.instant,"completion":r.completion,"action":r.action,"metadata":r.metadata})).collect();
    std::fs::write(
        target.join("expected-history.json"),
        serde_json::to_vec(&expected).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn test_archive_publication_failures_retry_without_replacing_history() {
    for suffix in [".parquet", "manifest_1", "_version_"] {
        for persist in [false, true] {
            for pinned in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut table = create(dir.path(), 8, false).await;
                let mut commits = vec![];
                for id in ["a", "b", "c"] {
                    commits.push(table.append([batch(id)]).await.unwrap());
                }
                let mut expected = BTreeMap::new();
                for commit in &commits {
                    let metadata =
                        std::fs::read(dir.path().join(&commit.commit_relative_path)).unwrap();
                    expected.insert(commit.instant.clone(), metadata);
                }
                let store = inject(&mut table);
                *store.fail_put.lock().unwrap() = Some((suffix.to_string(), persist));
                // The data commit succeeded; post-commit archival errors are maintenance failures.
                if pinned {
                    table.overwrite([batch("d")]).await.unwrap();
                } else {
                    table.append([batch("d")]).await.unwrap();
                }
                assert_eq!(store.fired.lock().unwrap().len(), 1);
                for commit in &commits {
                    assert!(dir.path().join(&commit.commit_relative_path).exists());
                }
                let before = history_bytes(dir.path());
                let mut reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                reopened.append([batch("e")]).await.unwrap();
                assert_immutable(dir.path(), &before);
                let records = archive_records(&reopened).await;
                assert_eq!(records.len(), if pinned { 3 } else { 4 });
                for (instant, metadata) in expected {
                    assert_eq!(
                        records[&(instant, "commit".into())].metadata,
                        Some(metadata)
                    );
                }
                assert_eq!(
                    ids(&reopened).await,
                    if pinned {
                        vec!["d", "e"]
                    } else {
                        vec!["a", "b", "c", "d", "e"]
                    }
                );
                export_fixture(
                    dir.path(),
                    &format!(
                        "publication-{}-{persist}-pinned{pinned}",
                        suffix.replace('.', "_")
                    ),
                    &records,
                );
            }
        }
    }
}

#[tokio::test]
async fn test_archive_wider_retry_excludes_already_published_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = create(dir.path(), 9, true).await;
    let a = table.append([batch("a")]).await.unwrap();
    let b = table.append([batch("b")]).await.unwrap();
    let c = table.append([batch("c")]).await.unwrap();
    let store = inject(&mut table);
    for commit in [&a, &c] {
        store
            .deny_delete
            .lock()
            .unwrap()
            .push(Path::from_absolute_path(dir.path().join(&commit.commit_relative_path)).unwrap());
    }
    table.append([batch("d")]).await.unwrap();
    assert!(!dir.path().join(&b.commit_relative_path).exists());
    let before = history_bytes(dir.path());
    store.deny_delete.lock().unwrap().clear();
    table.append([batch("e")]).await.unwrap();
    assert_immutable(dir.path(), &before);
    let records = archive_records(&table).await;
    assert_eq!(records.len(), 4);
    assert_eq!(
        ids(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await,
        vec!["a", "b", "c", "d", "e"]
    );
    export_fixture(dir.path(), "wider-v9-mortrue", &records);
}

async fn seed_active(table: &Table) -> Vec<LsmRecord> {
    let mut records = vec![];
    for i in 1..=4 {
        let instant = format!("{i:03}");
        let completion = format!("1{i:02}");
        let metadata = format!("metadata-{instant}").into_bytes();
        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{instant}_{completion}.commit"),
                metadata.clone(),
            )
            .await
            .unwrap();
        records.push(LsmRecord {
            instant,
            completion,
            action: "commit".into(),
            metadata: Some(metadata),
            plan: None,
        });
    }
    records
}
async fn put_archive(table: &Table, name: &str, records: &[LsmRecord]) -> LsmFileEntry {
    let bytes = write_lsm_parquet(records).unwrap();
    let len = bytes.len() as i64;
    table
        .file_system_view
        .storage
        .put_file(&format!(".hoodie/timeline/history/{name}"), bytes)
        .await
        .unwrap();
    LsmFileEntry {
        file_name: name.into(),
        file_len: len,
    }
}
async fn put_manifest(table: &Table, version: i64, files: Vec<LsmFileEntry>, publish: bool) {
    let storage = &table.file_system_view.storage;
    storage
        .put_file(
            &format!(".hoodie/timeline/history/manifest_{version}"),
            serde_json::to_vec(&LsmManifest { files }).unwrap(),
        )
        .await
        .unwrap();
    if publish {
        storage
            .put_file(
                ".hoodie/timeline/history/_version_",
                version.to_string().into_bytes(),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_archive_orphan_collision_requires_identical_coverage() {
    for kind in ["same", "missing", "different", "corrupt", "out-of-range"] {
        let dir = tempfile::tempdir().unwrap();
        let table = create(dir.path(), 8, false).await;
        let records = seed_active(&table).await;
        let mut existing = records[..3].to_vec();
        match kind {
            "missing" => {
                existing.remove(1);
            }
            "different" => existing[1].metadata = Some(b"other".to_vec()),
            "out-of-range" => existing.push(records[3].clone()),
            _ => {}
        }
        let name = "001_003_0.parquet";
        put_archive(&table, name, &existing).await;
        if kind == "corrupt" {
            table
                .file_system_view
                .storage
                .put_file(&format!(".hoodie/timeline/history/{name}"), b"bad".to_vec())
                .await
                .unwrap();
        }
        let before = history_bytes(dir.path());
        let result =
            archive_timeline_if_needed(&table.file_system_view.storage, ".hoodie/timeline", 1, 3)
                .await;
        assert_immutable(dir.path(), &before);
        if kind == "same" {
            assert!(result.is_ok());
            assert_eq!(archive_records(&table).await.len(), 3);
        } else {
            assert!(result.is_err(), "accepted {kind} collision");
            for record in &records {
                assert!(
                    dir.path()
                        .join(format!(
                            ".hoodie/timeline/{}_{}.commit",
                            record.instant, record.completion
                        ))
                        .exists()
                );
            }
        }
    }
}

#[tokio::test]
async fn test_archive_successor_must_preserve_published_entries() {
    for invalid in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let table = create(dir.path(), 8, false).await;
        let records = seed_active(&table).await;
        let first = put_archive(&table, "001_001_0.parquet", &records[..1]).await;
        put_manifest(&table, 1, vec![first.clone()], true).await;
        let second = put_archive(&table, "002_002_0.parquet", &records[1..2]).await;
        put_manifest(
            &table,
            2,
            if invalid {
                vec![second]
            } else {
                vec![first, second]
            },
            false,
        )
        .await;
        let before = history_bytes(dir.path());
        let result =
            archive_timeline_if_needed(&table.file_system_view.storage, ".hoodie/timeline", 1, 3)
                .await;
        assert_immutable(dir.path(), &before);
        if invalid {
            assert!(result.is_err());
            assert!(dir.path().join(".hoodie/timeline/001_101.commit").exists());
        } else {
            result.unwrap();
            assert_eq!(archive_records(&table).await.len(), 3);
        }
    }
}

#[tokio::test]
async fn test_archive_covered_records_still_require_all_fences_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = create(dir.path(), 8, false).await;
    let records = seed_active(&table).await;
    let entry = put_archive(&table, "001_003_0.parquet", &records[..3]).await;
    put_manifest(&table, 1, vec![entry], true).await;
    let fence = ".hoodie/timeline/002.commit.inflight";
    table
        .file_system_view
        .storage
        .put_file(fence, vec![1])
        .await
        .unwrap();
    let store = inject(&mut table);
    store
        .deny_delete
        .lock()
        .unwrap()
        .push(Path::from_absolute_path(dir.path().join(fence)).unwrap());
    let before = history_bytes(dir.path());
    assert!(
        archive_timeline_if_needed(&table.file_system_view.storage, ".hoodie/timeline", 1, 3)
            .await
            .is_err()
    );
    for record in &records {
        assert!(
            dir.path()
                .join(format!(
                    ".hoodie/timeline/{}_{}.commit",
                    record.instant, record.completion
                ))
                .exists()
        );
    }
    assert_immutable(dir.path(), &before);
    store.deny_delete.lock().unwrap().clear();
    archive_timeline_if_needed(&table.file_system_view.storage, ".hoodie/timeline", 1, 3)
        .await
        .unwrap();
    assert!(!dir.path().join(".hoodie/timeline/001_101.commit").exists());
    assert_immutable(dir.path(), &before);
}

#[tokio::test]
async fn test_archive_range_gaps_and_other_actions_are_not_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let table = create(dir.path(), 8, false).await;
    let records = seed_active(&table).await;
    let entry = put_archive(
        &table,
        "001_003_0.parquet",
        &[records[0].clone(), records[2].clone()],
    )
    .await;
    put_manifest(&table, 1, vec![entry], true).await;
    table
        .file_system_view
        .storage
        .put_file(".hoodie/timeline/001_105.rollback", b"rollback".to_vec())
        .await
        .unwrap();
    let before = history_bytes(dir.path());
    archive_timeline_if_needed(&table.file_system_view.storage, ".hoodie/timeline", 1, 3)
        .await
        .unwrap();
    assert_immutable(dir.path(), &before);
    let archived = archive_records(&table).await;
    assert_eq!(archived.len(), 4);
    assert_eq!(archived[&("002".into(), "commit".into())], records[1]);
    assert_eq!(
        archived[&("001".into(), "rollback".into())].metadata,
        Some(b"rollback".to_vec())
    );
}
