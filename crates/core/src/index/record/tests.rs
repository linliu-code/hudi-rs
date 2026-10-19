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
use std::collections::BTreeMap;

use crate::config::table::{HudiTableConfig, TableTypeValue};
use crate::file_group::log_file::log_block::BlockType;
use crate::file_group::log_file::reader::LogFileReader;
use crate::hfile::HFileWriter;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::metadata::table::encode::record_index_metadata_avro_schema_json;
use crate::table::ReadOptions;
use crate::timeline::instant::{Action, Instant};
use crate::write::metadata::mdt_partition_latest_slices;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn batch(rows: &[(&str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["sf"; rows.len()])),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
async fn rows(table: &Table) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for b in table.read(&ReadOptions::new()).await.unwrap() {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ts = b
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i).into(), ts.value(i)));
        }
    }
    out.sort();
    out
}
fn key(id: &str) -> HoodieKey {
    HoodieKey {
        record_key: id.into(),
        partition_path: "city=sf".into(),
    }
}

// Materialize the actual put payloads in a new, completed MDT compaction slice.
// This is a native stored-state fixture, not execution of a Spark table service.
async fn compact_record_index_fixture(table: &Table) {
    let storage = table.file_system_view.storage.clone();
    let slices = mdt_partition_latest_slices(storage.as_ref(), "record_index")
        .await
        .unwrap();
    let requested = crate::write::append::generate_instant_time().await;
    let schema = record_index_metadata_avro_schema_json().unwrap();
    let mut keys_by_shard = BTreeMap::new();
    for (id, base, logs) in &slices {
        let mut reader = HFileReader::open(storage.as_ref(), base).await.unwrap();
        let mut records = reader.collect_records().unwrap();
        if !logs.is_empty() {
            let scanner = LogFileScanner::new(table.hudi_configs.clone(), storage.clone());
            let ScanResult::HFileRecords(log_records) = scanner
                .scan(
                    logs.clone(),
                    &InstantRange::up_to("99991231235959999", "UTC"),
                )
                .await
                .unwrap()
            else {
                panic!("fixture must contain genuine HFile puts")
            };
            records.extend(log_records);
        }
        let mut entries = BTreeMap::new();
        for record in records {
            let key = record.key_as_str().unwrap().to_string();
            assert!(
                decode_record_index_entry(&key, record.value(), record.avro_schema())
                    .unwrap()
                    .is_some()
            );
            keys_by_shard.insert(key.clone(), id.clone());
            entries.insert(key, record.value().to_vec());
        }
        let entries = entries.into_iter().collect::<Vec<_>>();
        let bytes = HFileWriter::write(
            &entries,
            BTreeMap::from([("schema".into(), schema.as_bytes().to_vec())]),
        )
        .unwrap();
        storage
            .put_file(
                &format!(".hoodie/metadata/record_index/{id}_0-0-0_{requested}.hfile"),
                bytes,
            )
            .await
            .unwrap();
    }
    assert_ne!(
        keys_by_shard["a"], keys_by_shard["b"],
        "fixture must cover distinct RLI shards"
    );
    let completion = crate::write::append::generate_instant_time().await;
    let instant =
        Instant::new_completed(requested.clone(), Action::Commit, Some(completion)).unwrap();
    let metadata = HoodieCommitMetadata {
        operation_type: Some("COMPACT".into()),
        compacted: Some(true),
        ..Default::default()
    };
    storage
        .put_file(
            &instant
                .relative_path_with_base(".hoodie/metadata/.hoodie/timeline")
                .unwrap(),
            metadata.to_avro_bytes().unwrap(),
        )
        .await
        .unwrap();
    let selected = mdt_partition_latest_slices(storage.as_ref(), "record_index")
        .await
        .unwrap();
    assert_eq!(selected.len(), slices.len());
    let mut found = BTreeMap::new();
    for (_, base, logs) in selected {
        assert!(base.ends_with(&format!("_{requested}.hfile")));
        assert!(
            logs.is_empty(),
            "old put logs must not mask the delete-only scan"
        );
        for record in HFileReader::open(storage.as_ref(), &base)
            .await
            .unwrap()
            .collect_records()
            .unwrap()
        {
            let key = record.key_as_str().unwrap();
            let entry = decode_record_index_entry(key, record.value(), record.avro_schema())
                .unwrap()
                .unwrap();
            found.insert(key.to_string(), entry.partition_path);
        }
    }
    assert_eq!(
        found,
        BTreeMap::from([
            ("a".into(), "city=sf".into()),
            ("b".into(), "city=sf".into())
        ])
    );
}

#[tokio::test]
async fn test_record_index_delete_only_after_compaction() {
    let mut failures = Vec::new();
    for version in [8, 9] {
        for mor in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = Table::create(dir.path().to_str().unwrap())
                .with_table_name("compacted_rli")
                .with_table_version(version)
                .with_table_type(if mor {
                    TableTypeValue::MergeOnRead
                } else {
                    TableTypeValue::CopyOnWrite
                })
                .with_record_key_fields(["id"])
                .with_partition_fields(["city"])
                .with_ordering_fields(["ts"])
                .with_metadata(true)
                .with_record_index(true)
                .create()
                .await
                .unwrap();
            let actual_version: isize = table
                .hudi_configs
                .get(HudiTableConfig::TableVersion)
                .unwrap()
                .into();
            assert_eq!(actual_version, version);
            table.append([batch(&[("a", 1), ("b", 1)])]).await.unwrap();
            let before = RecordIndex
                .tag_location(&table, &[key("a"), key("b")])
                .await
                .unwrap();
            assert!(before[&key("a")].is_some());
            assert!(before[&key("b")].is_some());
            compact_record_index_fixture(&table).await;
            let compacted = RecordIndex
                .tag_location(&table, &[key("a"), key("b")])
                .await
                .unwrap();
            assert_eq!(
                compacted[&key("a")].as_ref().unwrap().file_id,
                before[&key("a")].as_ref().unwrap().file_id
            );
            table.delete_keys([key("a")]).await.unwrap();
            let mut reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            assert_eq!(rows(&reopened).await, vec![("b".into(), 1)]);
            let storage = reopened.file_system_view.storage.clone();
            let mut delete_blocks = 0;
            for (_, _, logs) in mdt_partition_latest_slices(storage.as_ref(), "record_index")
                .await
                .unwrap()
            {
                for log in logs {
                    let blocks =
                        LogFileReader::new(reopened.hudi_configs.clone(), storage.clone(), &log)
                            .await
                            .unwrap()
                            .read_all_blocks(&InstantRange::up_to("99991231235959999", "UTC"))
                            .await
                            .unwrap();
                    for block in blocks {
                        assert_eq!(block.block_type, BlockType::Delete);
                        delete_blocks += 1;
                    }
                }
            }
            assert!(delete_blocks > 0);
            // This lookup must precede any new put, which would mask format inference.
            let tagged = RecordIndex
                .tag_location(&reopened, &[key("a"), key("b")])
                .await;
            let locations = match tagged {
                Ok(value) => value,
                Err(error) => {
                    failures.push(format!("tv={version} mor={mor}: {error}"));
                    continue;
                }
            };
            assert!(locations[&key("a")].is_none());
            let survivor = locations[&key("b")].as_ref().unwrap();
            assert_eq!(
                survivor.file_id,
                before[&key("b")].as_ref().unwrap().file_id
            );
            assert_eq!(survivor.partition_path, "city=sf");
            reopened.upsert([batch(&[("c", 2)])]).await.unwrap();
            reopened.upsert([batch(&[("a", 3)])]).await.unwrap();
            let final_table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            assert_eq!(
                rows(&final_table).await,
                vec![("a".into(), 3), ("b".into(), 1), ("c".into(), 2)]
            );
            let final_locations = RecordIndex
                .tag_location(&final_table, &[key("a"), key("b"), key("c")])
                .await
                .unwrap();
            for location in final_locations.values() {
                assert_eq!(location.as_ref().unwrap().partition_path, "city=sf");
            }
        }
    }
    assert!(
        failures.is_empty(),
        "delete-only latest RLI logs must clear compacted locations: {failures:#?}"
    );
}
