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
use super::*;
use crate::file_group::log_file::log_block::BlockMetadataKey;
use crate::file_group::log_file::writer::LogFileWriter;
use crate::hfile::HFileWriter;
use crate::metadata::table::encode::{
    RecordIndexEntry, decode_record_index_entry, encode_record_index_entry,
    record_index_metadata_avro_schema_json,
};
use std::collections::{BTreeMap, HashMap};

const FIRST: &str = "20261001000000000";
const DELETE: &str = "20261001000001000";
const LAST: &str = "20261001000002000";

fn scanner() -> (tempfile::TempDir, LogFileScanner) {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Storage::new_with_base_url(url::Url::from_directory_path(dir.path()).unwrap()).unwrap();
    (
        dir,
        LogFileScanner::new(Arc::new(HudiConfigs::empty()), storage),
    )
}
fn delete(instant: &str) -> Vec<u8> {
    crate::write::build_delete_log_block(instant, &[("a".into(), "city=sf".into())], 0).unwrap()
}
fn put(instant: &str, file_id: &str) -> Vec<u8> {
    let schema = record_index_metadata_avro_schema_json().unwrap();
    let entry = RecordIndexEntry {
        record_key: "a".into(),
        partition_path: "city=sf".into(),
        file_id: file_id.into(),
        instant_time_millis: 1,
        is_deleted: false,
    };
    let hfile = HFileWriter::write(
        &[("a".into(), encode_record_index_entry(&entry).unwrap())],
        BTreeMap::from([("schema".into(), schema.as_bytes().to_vec())]),
    )
    .unwrap();
    LogFileWriter::write_log_block(
        BlockType::HfileData,
        HashMap::from([
            (BlockMetadataKey::InstantTime, instant.into()),
            (BlockMetadataKey::Schema, schema.into()),
        ]),
        &hfile,
    )
}
fn rollback(target: &str) -> Vec<u8> {
    LogFileWriter::write_log_block(
        BlockType::Command,
        HashMap::from([
            (BlockMetadataKey::InstantTime, LAST.into()),
            (BlockMetadataKey::TargetInstantTime, target.into()),
            (BlockMetadataKey::CommandBlockType, "0".into()),
        ]),
        &[],
    )
}
fn range() -> InstantRange {
    InstantRange::up_to("99991231235959999", "UTC")
}

#[tokio::test]
async fn test_delete_only_context_preserves_regular_data_and_hfile_semantics() {
    let (_dir, scanner) = scanner();
    scanner
        .storage
        .put_file("delete.log", delete(DELETE))
        .await
        .unwrap();
    let regular = scanner
        .scan(vec!["delete.log".into()], &range())
        .await
        .unwrap()
        .unwrap_record_batches();
    assert_eq!(regular.num_data_batches(), 0);
    assert_eq!(regular.num_delete_batches(), 1);
    let batch = &regular.delete_batches[0].0;
    assert_eq!(
        batch
            .column_by_name("recordKey")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
    let hfile = scanner
        .with_hfile_context()
        .scan(vec!["delete.log".into()], &range())
        .await
        .unwrap()
        .unwrap_hfile_records();
    assert_eq!(hfile.len(), 1);
    assert_eq!(hfile[0].key_as_str(), Some("a"));
    assert!(hfile[0].value().is_empty());
    assert!(
        decode_record_index_entry("a", hfile[0].value(), None)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_hfile_context_keeps_put_delete_reinsert_order_and_schema() {
    let (_dir, scanner) = scanner();
    let scanner = scanner.with_hfile_context();
    scanner
        .storage
        .put_file("first.log", put(FIRST, "first-file"))
        .await
        .unwrap();
    let mut tail = delete(DELETE);
    tail.extend(put(LAST, "last-file"));
    scanner.storage.put_file("tail.log", tail).await.unwrap();
    let batches = scanner
        .scan_hfile(vec!["first.log".into(), "tail.log".into()], &range())
        .await
        .unwrap();
    assert_eq!(batches.len(), 3);
    let decoded = batches
        .iter()
        .flat_map(|batch| {
            batch.records.iter().map(|record| {
                decode_record_index_entry("a", record.value(), batch.schema.as_deref())
                    .unwrap()
                    .map(|entry| entry.file_id)
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        decoded,
        vec![Some("first-file".into()), None, Some("last-file".into())]
    );
    assert!(batches[0].schema.is_some());
    assert!(batches[1].schema.is_none());
    assert!(batches[1].records[0].is_deleted());
    assert!(batches[2].schema.is_some());
}

#[tokio::test]
async fn test_hfile_delete_filters_preserve_base_location() {
    let (_dir, scanner) = scanner();
    let scanner = scanner.with_hfile_context();
    scanner
        .storage
        .put_file("delete.log", delete(DELETE))
        .await
        .unwrap();
    scanner
        .storage
        .put_file("rollback.log", rollback(DELETE))
        .await
        .unwrap();
    for (paths, window) in [
        (
            vec!["delete.log".into()],
            InstantRange::exact_match([FIRST.to_string()], "UTC"),
        ),
        (vec!["delete.log".into(), "rollback.log".into()], range()),
    ] {
        let result = scanner.scan(paths, &window).await.unwrap();
        let records = match result {
            ScanResult::Empty => Vec::new(),
            ScanResult::HFileRecords(records) => records,
            _ => panic!("metadata context changed"),
        };
        let mut base = BTreeMap::from([("a".to_string(), "compacted-file".to_string())]);
        for record in records {
            if record.value().is_empty() {
                base.remove(record.key_as_str().unwrap());
            }
        }
        assert_eq!(
            base,
            BTreeMap::from([("a".into(), "compacted-file".into())])
        );
    }
    assert!(
        scanner
            .scan(vec!["rollback.log".into()], &range())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(scanner.scan(Vec::new(), &range()).await.unwrap().is_empty());
    scanner
        .storage
        .put_file("empty.log", Vec::new())
        .await
        .unwrap();
    assert!(
        scanner
            .scan(vec!["empty.log".into()], &range())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn test_hfile_context_rejects_real_arrow_data_and_mixed_formats() {
    let (_dir, scanner) = scanner();
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Utf8,
            false,
        )])),
        vec![Arc::new(arrow_array::StringArray::from(vec!["a"]))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let schema = r#"{"type":"record","name":"record","fields":[{"name":"id","type":"string"}]}"#;
    let block = LogFileWriter::write_log_block(
        BlockType::ParquetData,
        HashMap::from([
            (BlockMetadataKey::InstantTime, FIRST.into()),
            (BlockMetadataKey::Schema, schema.into()),
        ]),
        &bytes,
    );
    scanner.storage.put_file("arrow.log", block).await.unwrap();
    scanner
        .storage
        .put_file("hfile.log", put(LAST, "last-file"))
        .await
        .unwrap();
    assert!(
        scanner
            .scan(vec!["arrow.log".into()], &range())
            .await
            .unwrap()
            .is_record_batches()
    );
    let error = scanner
        .scan(vec!["arrow.log".into(), "hfile.log".into()], &range())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("mixed block types"));
    let error = scanner
        .with_hfile_context()
        .scan(vec!["arrow.log".into()], &range())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Expected HFile"));
}
