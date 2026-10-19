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
//! Timeline archival into the LSM history timeline (Java `TimelineArchiverV2`
//! + `LSMTimelineWriter`).
//!
//! When the active timeline holds more than `hoodie.keep.max.commits`
//! completed commits, the oldest are archived down to `hoodie.keep.min.commits`:
//! written as one level-0 parquet of `HoodieLSMTimelineInstant` rows under
//! `{timeline}/history/`, registered in a new `manifest_{N}` (JSON) pointed at
//! by `_version_`, then removed from the active timeline (pending fencing
//! files first, completed files after — Java's ordering). Runs inside the
//! completion critical section (user design; Java runs it post-commit).
//!
//! LSM level compaction (merging L0 files into L1+) is not yet implemented —
//! readers are unaffected since every file stays manifest-listed.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::error::CoreError;
use crate::storage::Storage;

/// Java `hoodie.keep.min.commits` default.
pub(crate) const DEFAULT_KEEP_MIN_COMMITS: usize = 20;
/// Java `hoodie.keep.max.commits` default.
pub(crate) const DEFAULT_KEEP_MAX_COMMITS: usize = 30;
/// Java `hoodie.timeline.manifest.retained.versions` default.
const RETAINED_MANIFEST_VERSIONS: i64 = 3;

/// Java `HoodieLSMTimelineManifest`: `{"files":[{"fileName":..,"fileLen":..}]}`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LsmManifest {
    files: Vec<LsmFileEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LsmFileEntry {
    file_name: String,
    file_len: i64,
}

#[derive(Debug, Clone)]
struct CompletedInstant {
    requested: String,
    completion: String,
    action: String,
    file_name: String,
}

/// Archive the timeline at `timeline_dir` if it exceeds `max_keep` completed
/// commits, retaining the newest `min_keep`.
pub(crate) async fn archive_timeline_if_needed(
    storage: &Storage,
    timeline_dir: &str,
    min_keep: usize,
    max_keep: usize,
) -> Result<()> {
    let listed = match storage.list_files(Some(timeline_dir)).await {
        Ok(files) => files,
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    let mut completed: Vec<CompletedInstant> = Vec::new();
    let mut fencing_by_instant: HashMap<String, Vec<String>> = HashMap::new();
    let mut earliest_pending: Option<String> = None;
    for file in &listed {
        let name = &file.name;
        let Some(first) = name.chars().next() else {
            continue;
        };
        if !first.is_ascii_digit() {
            continue;
        }
        let requested: String = name.chars().take_while(char::is_ascii_digit).collect();
        if name.ends_with(".requested") || name.ends_with(".inflight") {
            fencing_by_instant
                .entry(requested.clone())
                .or_default()
                .push(name.clone());
            continue;
        }
        // Completed layout-v2 name: `{req}_{comp}.{action}`.
        let Some((stem, action)) = name.rsplit_once('.') else {
            continue;
        };
        let Some((req, comp)) = stem.split_once('_') else {
            continue;
        };
        completed.push(CompletedInstant {
            requested: req.to_string(),
            completion: comp.to_string(),
            action: action.to_string(),
            file_name: name.clone(),
        });
        let _ = requested;
    }
    // Pending = fenced without a completed file.
    let completed_set: std::collections::HashSet<&str> =
        completed.iter().map(|c| c.requested.as_str()).collect();
    for ts in fencing_by_instant.keys() {
        if !completed_set.contains(ts.as_str()) && earliest_pending.as_ref().is_none_or(|e| ts < e)
        {
            earliest_pending = Some(ts.clone());
        }
    }

    completed.sort_by(|a, b| a.requested.cmp(&b.requested));
    // A replacecommit's partitionToReplaceFileIds is what hides the file
    // groups it replaced; until a cleaner physically deletes those groups,
    // archiving it resurrects their rows. Java blocks archival at the first
    // uncleaned replace instant; with no cleaner implemented, that means
    // never archiving at or past the earliest active replacecommit.
    let earliest_replace = completed
        .iter()
        .filter(|c| c.action == "replacecommit")
        .map(|c| c.requested.clone())
        .min();
    // A savepoint pins its commit and everything after it: archiving past one
    // would discard the very files the savepoint exists to retain (Java's
    // TimelineArchiverV2 stops at the first savepoint unless
    // hoodie.archive.beyond.savepoint, default false).
    let earliest_savepoint = completed
        .iter()
        .filter(|c| c.action == "savepoint")
        .map(|c| c.requested.clone())
        .min();
    let earliest_pending = [earliest_pending, earliest_replace, earliest_savepoint]
        .into_iter()
        .flatten()
        .min();
    let is_commit_action =
        |action: &str| matches!(action, "commit" | "deltacommit" | "replacecommit");
    let commit_count = completed
        .iter()
        .filter(|c| is_commit_action(&c.action))
        .count();
    // Misconfigured min > max would underflow below and archive the whole
    // timeline; treat min as the effective floor.
    let min_keep = min_keep.min(max_keep);
    if commit_count <= max_keep {
        return Ok(());
    }

    // Archive the oldest commits down to `min_keep`, but never at or past the
    // earliest pending instant; sweep along non-commit actions (rollbacks) up
    // to the newest archived commit.
    let mut commits_to_archive = commit_count - min_keep;
    let mut max_archived_commit: Option<String> = None;
    let mut to_archive: Vec<CompletedInstant> = Vec::new();
    for instant in &completed {
        if let Some(pending) = &earliest_pending
            && instant.requested.as_str() >= pending.as_str()
        {
            break;
        }
        if is_commit_action(&instant.action) {
            if commits_to_archive == 0 {
                break;
            }
            commits_to_archive -= 1;
            max_archived_commit = Some(instant.requested.clone());
            to_archive.push(instant.clone());
        } else if instant.action == "savepoint" {
            // A savepoint must never be swept along as an incidental
            // non-commit action; it also shares its commit's instant time, so
            // the fencing-file deletion below must not reach it.
            break;
        } else {
            to_archive.push(instant.clone());
        }
    }
    // Trim trailing non-commit instants newer than the last archived commit.
    if let Some(max_commit) = &max_archived_commit {
        to_archive.retain(|i| i.requested.as_str() <= max_commit.as_str());
    } else {
        return Ok(());
    }
    if to_archive.is_empty() {
        return Ok(());
    }

    // Build HoodieLSMTimelineInstant rows: metadata = completed file bytes;
    // plan = inflight (fallback requested) bytes for replacecommits.
    let mut records = Vec::new();
    for instant in &to_archive {
        let metadata = storage
            .get_file_data(&format!("{timeline_dir}/{}", instant.file_name))
            .await?;
        let plan = if instant.action == "replacecommit" {
            let mut plan = None;
            for fencing in fencing_by_instant
                .get(&instant.requested)
                .into_iter()
                .flatten()
            {
                if fencing.ends_with(".inflight")
                    && let Ok(bytes) = storage
                        .get_file_data(&format!("{timeline_dir}/{fencing}"))
                        .await
                    && !bytes.is_empty()
                {
                    plan = Some(bytes.to_vec());
                }
            }
            if plan.is_none() {
                for fencing in fencing_by_instant
                    .get(&instant.requested)
                    .into_iter()
                    .flatten()
                {
                    if fencing.ends_with(".requested")
                        && let Ok(bytes) = storage
                            .get_file_data(&format!("{timeline_dir}/{fencing}"))
                            .await
                        && !bytes.is_empty()
                    {
                        plan = Some(bytes.to_vec());
                    }
                }
            }
            plan
        } else {
            None
        };
        records.push(LsmRecord {
            instant: instant.requested.clone(),
            completion: instant.completion.clone(),
            action: instant.action.clone(),
            metadata: Some(metadata.to_vec()),
            plan,
        });
    }

    let history_dir = format!("{timeline_dir}/history");
    let (published_version, mut manifest) = read_manifest(storage, &history_dir).await?;
    validate_manifest(&manifest)?;
    let mut version = published_version;
    // A manifest PUT can succeed before its response (or the pointer PUT) fails.
    // Preserve that immutable successor before planning any additional archive.
    loop {
        let next = next_manifest_version(version)?;
        let path = format!("{history_dir}/manifest_{next}");
        let bytes = match storage.get_file_data(&path).await {
            Ok(bytes) => bytes,
            Err(crate::storage::error::StorageError::ObjectStoreError(
                object_store::Error::NotFound { .. },
            )) => break,
            Err(error) => return Err(error.into()),
        };
        let successor: LsmManifest = serde_json::from_slice(&bytes)
            .map_err(|e| CoreError::Timeline(format!("invalid LSM manifest {path}: {e}")))?;
        validate_manifest(&successor)?;
        if !manifest
            .files
            .iter()
            .all(|entry| successor.files.contains(entry))
        {
            return Err(CoreError::Timeline(format!(
                "LSM manifest {path} drops published files"
            )));
        }
        for entry in &successor.files {
            if !manifest.files.contains(entry) {
                read_archive_file(storage, &history_dir, entry).await?;
            }
        }
        manifest = successor;
        version = next;
    }

    // Range endpoints are only a pruning hint: gaps and distinct actions can
    // share a range. Only an identical archived record permits active cleanup.
    let min_instant = &to_archive[0].requested;
    let max_instant = &to_archive[to_archive.len() - 1].requested;
    let mut covered = HashSet::new();
    for entry in &manifest.files {
        let (min, max) = archive_file_range(&entry.file_name)?;
        if max < min_instant.as_str() || min > max_instant.as_str() {
            continue;
        }
        let archived = read_archive_file(storage, &history_dir, entry).await?;
        for (i, record) in records.iter().enumerate() {
            if record_is_covered(record, &archived)? {
                covered.insert(i);
            }
        }
    }
    records = records
        .into_iter()
        .enumerate()
        .filter_map(|(i, record)| (!covered.contains(&i)).then_some(record))
        .collect();
    if !records.is_empty() {
        let min = &records[0].instant;
        let max = &records[records.len() - 1].instant;
        let name = format!("{min}_{max}_0.parquet");
        let path = format!("{history_dir}/{name}");
        let parquet = write_lsm_parquet(&records)?;
        let mut entry = LsmFileEntry {
            file_name: name,
            file_len: parquet.len() as i64,
        };
        match storage.put_file_if_absent(&path, parquet).await {
            Ok(()) => {}
            Err(crate::storage::error::StorageError::AlreadyExists(..)) => {
                // This may be an unpublished archive from a lost response.
                // Never replace it, even when it contains extra middle instants.
                let bytes = storage.get_file_data(&path).await?;
                entry.file_len = bytes.len() as i64;
                let archived = read_lsm_parquet(bytes)?;
                validate_archive_range(&entry.file_name, &archived)?;
                for record in &records {
                    if !record_is_covered(record, &archived)? {
                        return Err(CoreError::Timeline(format!(
                            "LSM archive collision at {path}: missing {}",
                            record.instant
                        )));
                    }
                }
            }
            Err(error) => return Err(error.into()),
        }
        if let Some(existing) = manifest
            .files
            .iter()
            .find(|e| e.file_name == entry.file_name)
        {
            if existing != &entry {
                return Err(CoreError::Timeline(format!(
                    "conflicting LSM archive length at {path}"
                )));
            }
        } else {
            manifest.files.push(entry);
            version = next_manifest_version(version)?;
            let json = serde_json::to_vec(&manifest)
                .map_err(|e| CoreError::Timeline(format!("serialize LSM manifest: {e}")))?;
            storage
                .put_file_if_absent(&format!("{history_dir}/manifest_{version}"), json)
                .await?;
        }
    }
    // Manifest protocol: immutable parquet → immutable manifest → mutable pointer.
    // A pointer error must leave every active completion intact for the retry.
    if version != published_version {
        storage
            .put_file(
                &format!("{history_dir}/_version_"),
                version.to_string().into_bytes(),
            )
            .await?;
    }
    let mut stale = version - RETAINED_MANIFEST_VERSIONS;
    while stale > 0 {
        if storage
            .delete_file(&format!("{history_dir}/manifest_{stale}"))
            .await
            .is_err()
        {
            break;
        }
        stale -= 1;
    }

    // Remove all fences before any completed files. A surviving fence without
    // its completion looks pending to eager rollback, which could delete data
    // still listed in markers retained after a lost completion response.
    for instant in &to_archive {
        for fencing in fencing_by_instant
            .get(&instant.requested)
            .into_iter()
            .flatten()
        {
            storage
                .delete_file(&format!("{timeline_dir}/{fencing}"))
                .await?;
        }
    }
    for instant in &to_archive {
        let _ = storage
            .delete_file(&format!("{timeline_dir}/{}", instant.file_name))
            .await;
    }
    Ok(())
}

async fn read_manifest(storage: &Storage, history_dir: &str) -> Result<(i64, LsmManifest)> {
    let version_path = format!("{history_dir}/_version_");
    match storage.get_file_data(&version_path).await {
        Ok(bytes) => {
            let version: i64 = String::from_utf8_lossy(&bytes)
                .trim()
                .parse()
                .map_err(|e| CoreError::Timeline(format!("invalid LSM _version_: {e}")))?;
            let manifest_bytes = storage
                .get_file_data(&format!("{history_dir}/manifest_{version}"))
                .await?;
            let manifest: LsmManifest = serde_json::from_slice(&manifest_bytes)
                .map_err(|e| CoreError::Timeline(format!("invalid LSM manifest: {e}")))?;
            Ok((version, manifest))
        }
        Err(crate::storage::error::StorageError::ObjectStoreError(
            object_store::Error::NotFound { .. },
        )) => Ok((0, LsmManifest::default())),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LsmRecord {
    instant: String,
    completion: String,
    action: String,
    metadata: Option<Vec<u8>>,
    plan: Option<Vec<u8>>,
}

fn next_manifest_version(version: i64) -> Result<i64> {
    // Java parses the version as an int.
    if !(0..i64::from(i32::MAX)).contains(&version) {
        return Err(CoreError::Timeline(format!(
            "invalid LSM manifest version {version}"
        )));
    }
    Ok(version + 1)
}

fn archive_file_range(name: &str) -> Result<(&str, &str)> {
    if let Some(stem) = name.strip_suffix(".parquet") {
        let parts: Vec<_> = stem.split('_').collect();
        if parts.len() == 3
            && parts[2].len() == 1
            && parts
                .iter()
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
            && parts[0] <= parts[1]
        {
            return Ok((parts[0], parts[1]));
        }
    }
    Err(CoreError::Timeline(format!(
        "invalid LSM archive filename {name}"
    )))
}

fn validate_manifest(manifest: &LsmManifest) -> Result<()> {
    let mut names = HashSet::new();
    for entry in &manifest.files {
        archive_file_range(&entry.file_name)?;
        if entry.file_len <= 0 || !names.insert(&entry.file_name) {
            return Err(CoreError::Timeline(format!(
                "invalid or duplicate LSM entry {}",
                entry.file_name
            )));
        }
    }
    Ok(())
}

async fn read_archive_file(
    storage: &Storage,
    history_dir: &str,
    entry: &LsmFileEntry,
) -> Result<Vec<LsmRecord>> {
    let path = format!("{history_dir}/{}", entry.file_name);
    let bytes = storage.get_file_data(&path).await?;
    if bytes.len() as i64 != entry.file_len {
        return Err(CoreError::Timeline(format!(
            "LSM archive length mismatch at {path}"
        )));
    }
    let records = read_lsm_parquet(bytes)?;
    validate_archive_range(&entry.file_name, &records)?;
    Ok(records)
}

fn validate_archive_range(name: &str, records: &[LsmRecord]) -> Result<()> {
    let (min, max) = archive_file_range(name)?;
    if records.is_empty()
        || records
            .iter()
            .any(|r| r.instant.as_str() < min || r.instant.as_str() > max)
    {
        return Err(CoreError::Timeline(format!(
            "LSM records outside archive range {name}"
        )));
    }
    Ok(())
}

fn record_is_covered(record: &LsmRecord, archived: &[LsmRecord]) -> Result<bool> {
    let mut found = false;
    for existing in archived {
        if existing.instant == record.instant && existing.action == record.action {
            if existing != record {
                return Err(CoreError::Timeline(format!(
                    "conflicting LSM record {}.{}",
                    record.instant, record.action
                )));
            }
            found = true;
        }
    }
    Ok(found)
}

fn read_lsm_parquet(bytes: bytes::Bytes) -> Result<Vec<LsmRecord>> {
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| CoreError::Timeline(format!("open LSM parquet: {e}")))?
        .build()
        .map_err(|e| CoreError::Timeline(format!("read LSM parquet: {e}")))?;
    let mut records = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| CoreError::Timeline(format!("read LSM batch: {e}")))?;
        fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<T>())
                .ok_or_else(|| CoreError::Timeline(format!("invalid LSM column {name}")))
        }
        let instants = column::<StringArray>(&batch, "instantTime")?;
        let completions = column::<StringArray>(&batch, "completionTime")?;
        let actions = column::<StringArray>(&batch, "action")?;
        let metadata = column::<BinaryArray>(&batch, "metadata")?;
        let plans = column::<BinaryArray>(&batch, "plan")?;
        let versions = column::<Int32Array>(&batch, "version")?;
        for i in 0..batch.num_rows() {
            if instants.is_null(i)
                || completions.is_null(i)
                || actions.is_null(i)
                || versions.is_null(i)
                || versions.value(i) != 1
            {
                return Err(CoreError::Timeline(
                    "invalid LSM instant identity or version".into(),
                ));
            }
            records.push(LsmRecord {
                instant: instants.value(i).to_string(),
                completion: completions.value(i).to_string(),
                action: actions.value(i).to_string(),
                metadata: (!metadata.is_null(i)).then(|| metadata.value(i).to_vec()),
                plan: (!plans.is_null(i)).then(|| plans.value(i).to_vec()),
            });
        }
    }
    Ok(records)
}

/// Arrow shape of Java `HoodieLSMTimelineInstant.avsc`.
fn lsm_instant_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("instantTime", DataType::Utf8, true),
        Field::new("completionTime", DataType::Utf8, true),
        Field::new("action", DataType::Utf8, true),
        Field::new("metadata", DataType::Binary, true),
        Field::new("plan", DataType::Binary, true),
        Field::new("version", DataType::Int32, false),
    ]))
}

fn write_lsm_parquet(records: &[LsmRecord]) -> Result<Vec<u8>> {
    let schema = lsm_instant_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(
                records.iter().map(|r| r.instant.as_str()),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                records.iter().map(|r| r.completion.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                records.iter().map(|r| r.action.as_str()),
            )),
            Arc::new(BinaryArray::from_iter(
                records.iter().map(|r| r.metadata.as_deref()),
            )),
            Arc::new(BinaryArray::from_iter(
                records.iter().map(|r| r.plan.as_deref()),
            )),
            Arc::new(Int32Array::from(vec![1; records.len()])),
        ],
    )?;
    let mut out = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut out, schema, None)
        .map_err(|e| CoreError::Timeline(format!("create LSM parquet writer: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| CoreError::Timeline(format!("write LSM parquet: {e}")))?;
    writer
        .close()
        .map_err(|e| CoreError::Timeline(format!("close LSM parquet: {e}")))?;
    Ok(out)
}

/// Requested times of all archived instants (for MDT valid-instants fencing:
/// archived instants are completed instants whose active-timeline files are
/// gone). Empty when no history exists.
pub(crate) async fn archived_instant_times(
    storage: &Storage,
    timeline_dir: &str,
) -> Result<std::collections::HashSet<String>> {
    let history_dir = format!("{timeline_dir}/history");
    let (version, manifest) = read_manifest(storage, &history_dir).await?;
    let mut out = std::collections::HashSet::new();
    if version == 0 {
        return Ok(out);
    }
    for entry in &manifest.files {
        let bytes = storage
            .get_file_data(&format!("{history_dir}/{}", entry.file_name))
            .await?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes.to_vec()),
        )
        .map_err(|e| CoreError::Timeline(format!("open LSM parquet: {e}")))?
        .build()
        .map_err(|e| CoreError::Timeline(format!("read LSM parquet: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| CoreError::Timeline(format!("read LSM batch: {e}")))?;
            let Some(column) = batch.column_by_name("instantTime") else {
                continue;
            };
            if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
                for i in 0..strings.len() {
                    if !strings.is_null(i) {
                        out.insert(strings.value(i).to_string());
                    }
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod lifecycle_tests {
    use crate::table::Table;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(id: &str, v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])) as ArrayRef,
                Arc::new(Int64Array::from(vec![v])),
            ],
        )
        .unwrap()
    }

    /// A savepoint pins its commit: archival must not cross it, and must not
    /// sweep the `.savepoint` instant off the active timeline.
    #[tokio::test]
    async fn test_archival_stops_at_savepoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_option("hoodie.keep.min.commits", "2")
            .with_option("hoodie.keep.max.commits", "3")
            .create()
            .await
            .unwrap();
        for i in 0..3 {
            table.append([batch(&format!("k{i}"), i)]).await.unwrap();
        }
        let timeline = dir.path().join(".hoodie/timeline");
        // Savepoint the OLDEST commit, the one archival would take first.
        let mut completed: Vec<String> = std::fs::read_dir(&timeline)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".commit") || n.ends_with(".deltacommit"))
            .collect();
        completed.sort();
        let oldest = completed.first().expect("a completed commit").clone();
        let requested = oldest.split('_').next().unwrap().to_string();
        std::fs::write(
            timeline.join(format!("{requested}_{requested}.savepoint")),
            b"{}",
        )
        .unwrap();

        for i in 3..10 {
            table.append([batch(&format!("k{i}"), i)]).await.unwrap();
        }

        let names: Vec<String> = std::fs::read_dir(&timeline)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            names.iter().any(|n| n.ends_with(".savepoint")),
            "the savepoint instant must stay on the active timeline: {names:?}"
        );
        assert!(
            names.iter().any(|n| n.starts_with(&format!("{requested}_"))
                && (n.ends_with(".commit") || n.ends_with(".deltacommit"))),
            "the savepointed commit must not be archived: {names:?}"
        );
    }

    /// Drives repeated archival from inside the crate: manifest rotation,
    /// retention, and reads across the boundary on the writing handle.
    #[tokio::test]
    async fn test_archival_rotation_in_crate() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .with_option("hoodie.keep.min.commits", "4")
            .with_option("hoodie.keep.max.commits", "6")
            .create()
            .await
            .unwrap();
        for i in 0..12 {
            table.append([batch(&format!("k{i}"), i)]).await.unwrap();
        }
        let history = dir.path().join(".hoodie/timeline/history");
        let names: Vec<String> = std::fs::read_dir(&history)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(names.iter().filter(|n| n.ends_with(".parquet")).count() >= 2);
        assert!(names.iter().filter(|n| n.starts_with("manifest_")).count() <= 3);
        let rows: usize = table
            .read(&crate::table::ReadOptions::new())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 12);
    }
}

#[cfg(test)]
#[path = "archival_tests.rs"]
mod retry_tests;
