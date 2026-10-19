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
use super::{HFileReader, HFileRecord, HFileRecordBatch, HFileWriter};
use crate::config::HudiConfigs;
use crate::file_group::base_file::reader::KeyPredicate;
use crate::file_group::log_file::content::Decoder;
use crate::file_group::log_file::log_block::{BlockMetadataKey, BlockType, LogBlockContent};
use crate::file_group::log_file::log_format::LogFormatVersion;
use crate::file_group::log_file::reader::LogFileReader;
use crate::file_group::log_file::scanner::LogFileScanner;
use crate::file_group::log_file::writer::LogFileWriter;
use crate::metadata::merger::FilesPartitionMerger;
use crate::metadata::table::encode::{
    ColumnStatValue, ColumnStatsMetadata, FilesMetadataEntry, RecordIndexEntry, StatsIndexVersion,
    decode_column_stats_entry, decode_record_index_entry, encode_column_stats, encode_files_record,
    encode_record_index_entry, hoodie_metadata_schema_json,
};
use crate::metadata::table::records::MetadataRecordType;
use crate::storage::Storage;
use crate::timeline::InstantRange;
use apache_avro::{Schema, types::Value};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::sync::Arc;

const FIRST: &str = "20261001000000000";
const LAST: &str = "20261001000001000";

pub(crate) fn alternate_schema() -> String {
    let mut json: serde_json::Value =
        serde_json::from_str(hoodie_metadata_schema_json().unwrap()).unwrap();
    json["fields"].as_array_mut().unwrap().swap(0, 1);
    serde_json::to_string(&json).unwrap()
}

pub(crate) fn alternate_bytes(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let original = Schema::parse_str(hoodie_metadata_schema_json().unwrap()).unwrap();
    let Value::Record(mut fields) =
        apache_avro::from_avro_datum(&original, &mut &bytes[..], None).unwrap()
    else {
        panic!("fixture record");
    };
    fields.swap(0, 1);
    apache_avro::to_avro_datum(
        &Schema::parse_str(&alternate_schema()).unwrap(),
        Value::Record(fields),
    )
    .unwrap()
}

fn hfile(entries: &[(String, Vec<u8>)], schema: Option<&str>) -> Vec<u8> {
    HFileWriter::write(
        entries,
        schema
            .map(|s| BTreeMap::from([("schema".into(), s.as_bytes().to_vec())]))
            .unwrap_or_default(),
    )
    .unwrap()
}

fn log(entries: &[(String, Vec<u8>)], schema: Option<&str>, instant: &str) -> Vec<u8> {
    // Deliberately disagree with the embedded schema: raw MDT context belongs to the HFile.
    LogFileWriter::write_log_block(
        BlockType::HfileData,
        HashMap::from([
            (BlockMetadataKey::InstantTime, instant.into()),
            (
                BlockMetadataKey::Schema,
                hoodie_metadata_schema_json().unwrap().into(),
            ),
        ]),
        &hfile(entries, schema),
    )
}

fn fixture() -> (tempfile::TempDir, Arc<Storage>, Arc<HudiConfigs>) {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Storage::new_with_base_url(url::Url::from_directory_path(dir.path()).unwrap()).unwrap();
    (dir, storage, Arc::new(HudiConfigs::empty()))
}
fn range() -> InstantRange {
    InstantRange::up_to("99991231235959999", "UTC")
}
fn files(name: &str, size: i64) -> Vec<u8> {
    encode_files_record(
        "city=sf",
        MetadataRecordType::Files,
        [FilesMetadataEntry {
            name: name.into(),
            size,
            is_deleted: false,
        }],
    )
    .unwrap()
}

#[tokio::test]
async fn test_schema_context_files_base_and_successive_logs_keep_origins_and_raw_bytes() {
    let (_dir, storage, configs) = fixture();
    let a = hoodie_metadata_schema_json().unwrap();
    let b = alternate_schema();
    assert_ne!(
        Schema::parse_str(a).unwrap(),
        Schema::parse_str(&b).unwrap()
    );
    let first = files("first.parquet", 11);
    let last = alternate_bytes(&files("last.parquet", 23));
    let entries_a = vec![("city=sf".into(), first.clone())];
    let entries_b = vec![("city=sf".into(), last.clone())];
    let mut base_reader = HFileReader::new(hfile(
        &[("city=sf".into(), files("base.parquet", 7))],
        Some(a),
    ))
    .unwrap();
    let base = HFileRecordBatch {
        schema: base_reader.get_avro_schema_arc().unwrap(),
        records: base_reader.collect_records().unwrap(),
    };
    drop(base_reader);
    let mut bytes = log(&entries_a, Some(a), FIRST);
    bytes.extend(log(&entries_b, Some(&b), LAST));
    storage.put_file("two.log", bytes).await.unwrap();
    let scanner = LogFileScanner::new(configs.clone(), storage.clone());
    let batches = scanner
        .scan_hfile(vec!["two.log".into()], &range())
        .await
        .unwrap();
    assert_eq!(batches.len(), 2);
    let raw = scanner
        .scan(vec!["two.log".into()], &range())
        .await
        .unwrap()
        .unwrap_hfile_records();
    assert_eq!(
        raw,
        vec![
            HFileRecord::from_str_key("city=sf", first),
            HFileRecord::from_str_key("city=sf", last)
        ]
    );
    let mut reader = LogFileReader::new(configs, storage, "two.log")
        .await
        .unwrap();
    let blocks = reader.read_all_blocks(&range()).await.unwrap();
    assert_eq!(
        blocks
            .into_iter()
            .flat_map(|b| b.content.into_hfile_records().unwrap())
            .collect::<Vec<_>>(),
        raw
    );
    drop(reader);
    let retained = batches.clone();
    drop(batches);
    let merger = FilesPartitionMerger::new(base.schema.as_deref().unwrap().clone());
    for keys in [vec![], vec!["city=sf"]] {
        let merged = merger
            .merge_contextual_for_keys(&base.records, &retained, &keys)
            .unwrap();
        let actual = merged["city=sf"]
            .files
            .iter()
            .map(|(name, info)| (name.as_str(), info.size))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            actual,
            BTreeMap::from([
                ("base.parquet", 7),
                ("first.parquet", 11),
                ("last.parquet", 23)
            ])
        );
    }
    // Relabeling B bytes with A must not accidentally satisfy the semantic oracle.
    assert!(merger.merge(&base.records, &raw).is_err());
    assert!(
        merger
            .merge_contextual_for_keys(&base.records, &retained, &["other"])
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn test_schema_context_selected_keys_prefix_and_ranged_raw_reads() {
    let (_dir, storage, configs) = fixture();
    let schema = alternate_schema();
    let expected = RecordIndexEntry {
        record_key: "keep-a".into(),
        partition_path: "city=sf".into(),
        file_id: "file-b".into(),
        instant_time_millis: 123,
        is_deleted: false,
    };
    let value = alternate_bytes(&encode_record_index_entry(&expected).unwrap());
    let entries = vec![
        ("keep-a".into(), value.clone()),
        ("skip-b".into(), value.clone()),
    ];
    let bytes = hfile(&entries, Some(&schema));
    storage.put_file("base.hfile", bytes.clone()).await.unwrap();
    for predicate in [
        KeyPredicate::Keys(vec!["keep-a".into()]),
        KeyPredicate::Prefixes(vec!["keep-".into()]),
    ] {
        let decoder = Decoder::new(configs.clone()).with_key_predicate(Some(predicate));
        let (content, origin) = decoder
            .decode_content_with_schema(
                &mut Cursor::new(&bytes),
                &LogFormatVersion::V0,
                bytes.len() as u64,
                &BlockType::HfileData,
                &HashMap::new(),
                true,
            )
            .unwrap();
        let records = content.into_hfile_records().unwrap();
        assert_eq!(
            records,
            vec![HFileRecord::from_str_key("keep-a", value.clone())]
        );
        assert_eq!(
            decode_record_index_entry("keep-a", records[0].value(), origin.as_deref()).unwrap(),
            Some(expected.clone())
        );
    }
    assert_ne!(
        decode_record_index_entry("keep-a", &value, None)
            .ok()
            .flatten(),
        Some(expected)
    );
    let mut reader = HFileReader::new(bytes).unwrap();
    let schema = reader.get_avro_schema_arc().unwrap();
    assert_eq!(
        reader.lookup_records(&["keep-a"]).unwrap()[0]
            .1
            .as_ref()
            .unwrap()
            .value(),
        value
    );
    assert_eq!(reader.collect_records_by_prefix("keep-").unwrap().len(), 1);
    let ranged = HFileReader::open_ranged(&storage, "base.hfile")
        .await
        .unwrap();
    let records = ranged
        .read_records_batched(&ranged.blocks_for_keys(&["keep-a"]))
        .await
        .unwrap();
    drop(reader);
    drop(ranged);
    assert_eq!(
        decode_record_index_entry("keep-a", records[0].value(), schema.as_deref())
            .unwrap()
            .unwrap()
            .file_id,
        "file-b"
    );
}

#[tokio::test]
async fn test_schema_context_missing_and_malformed_schema_policies() {
    let (_dir, storage, configs) = fixture();
    let entry = RecordIndexEntry {
        record_key: "a".into(),
        partition_path: "p".into(),
        file_id: "f".into(),
        instant_time_millis: 17,
        is_deleted: false,
    };
    let value = encode_record_index_entry(&entry).unwrap();
    for schema in [None, Some("not-avro")] {
        let bytes = hfile(&[("a".into(), value.clone())], schema);
        let mut reader = HFileReader::new(bytes.clone()).unwrap();
        assert_eq!(reader.collect_records().unwrap()[0].value(), value);
        assert_eq!(
            reader
                .record_iter()
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .value(),
            value
        );
        assert_eq!(
            reader.lookup_records(&["a"]).unwrap()[0]
                .1
                .as_ref()
                .unwrap()
                .value(),
            value
        );
        assert_eq!(
            reader.collect_records_by_prefix("a").unwrap()[0].value(),
            value
        );
        assert_eq!(
            reader
                .records_in_block(&reader.blocks_for_keys(&["a"])[0])
                .unwrap()[0]
                .value(),
            value
        );
        storage.put_file("schema.hfile", bytes).await.unwrap();
        let ranged = HFileReader::open_ranged(&storage, "schema.hfile")
            .await
            .unwrap();
        assert_eq!(
            ranged
                .read_records_batched(&ranged.blocks_for_keys(&["a"]))
                .await
                .unwrap()[0]
                .value(),
            value
        );
        storage
            .put_file(
                "schema.log",
                log(&[("a".into(), value.clone())], schema, FIRST),
            )
            .await
            .unwrap();
        let scanner = LogFileScanner::new(configs.clone(), storage.clone());
        assert_eq!(
            scanner
                .scan(vec!["schema.log".into()], &range())
                .await
                .unwrap()
                .unwrap_hfile_records()[0]
                .value(),
            value
        );
        let typed = scanner
            .scan_hfile(vec!["schema.log".into()], &range())
            .await;
        if schema.is_some() {
            assert!(reader.get_avro_schema_arc().is_err());
            assert!(
                typed
                    .unwrap_err()
                    .to_string()
                    .contains("Invalid Avro schema")
            );
        } else {
            let batch = &typed.unwrap()[0];
            assert!(batch.schema.is_none());
            assert_eq!(
                decode_record_index_entry("a", batch.records[0].value(), batch.schema.as_deref())
                    .unwrap(),
                Some(entry.clone())
            );
        }
    }
    storage
        .put_file("empty.log", log(&[("a".into(), vec![])], None, FIRST))
        .await
        .unwrap();
    let batches = LogFileScanner::new(configs, storage)
        .scan_hfile(vec!["empty.log".into()], &range())
        .await
        .unwrap();
    assert!(batches[0].schema.is_none());
    assert!(
        decode_record_index_entry("a", batches[0].records[0].value(), None)
            .unwrap()
            .is_none()
    );
}

pub(crate) async fn rewrite_logs_with_alternate_schema(
    storage: Arc<Storage>,
    configs: Arc<HudiConfigs>,
    paths: &[String],
) {
    let schema = alternate_schema();
    for path in paths {
        let mut reader = LogFileReader::new(configs.clone(), storage.clone(), path)
            .await
            .unwrap();
        let mut bytes = Vec::new();
        for block in reader.read_all_blocks(&range()).await.unwrap() {
            let LogBlockContent::HFileRecords(records) = block.content else {
                panic!("fixture HFile puts");
            };
            let entries = records
                .iter()
                .map(|r| {
                    (
                        r.key_as_str().unwrap().to_string(),
                        alternate_bytes(r.value()),
                    )
                })
                .collect::<Vec<_>>();
            bytes.extend(LogFileWriter::write_log_block(
                block.block_type,
                block.header,
                &hfile(&entries, Some(&schema)),
            ));
        }
        storage.put_file(path, bytes).await.unwrap();
    }
}

#[tokio::test]
async fn test_schema_context_column_stats_loader_decodes_base_and_alternate_log() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    let dir = tempfile::tempdir().unwrap();
    let mut table = crate::table::Table::create(dir.path().to_str().unwrap())
        .with_table_name("stats-schema")
        .with_record_key_fields(["id"])
        .with_column_stats(true)
        .create()
        .await
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![37])),
        ],
    )
    .unwrap();
    table.append([batch]).await.unwrap();
    let storage = table.file_system_view.storage.clone();
    let slices = crate::write::metadata::mdt_partition_latest_slices(&storage, "column_stats")
        .await
        .unwrap();
    let logs = slices
        .iter()
        .flat_map(|(_, _, logs)| logs.clone())
        .collect::<Vec<_>>();
    let batches = LogFileScanner::new(table.hudi_configs.clone(), storage.clone())
        .scan_hfile(logs.clone(), &range())
        .await
        .unwrap();
    let (key, file) = batches
        .iter()
        .flat_map(|b| b.records.iter().map(move |r| (r, b.schema.as_deref())))
        .find_map(|(record, schema)| {
            let stats = decode_column_stats_entry(record.value(), schema).unwrap()?;
            (stats.column_name == "value")
                .then(|| (record.key_as_str().unwrap().to_string(), stats.file_name))
        })
        .unwrap();
    let base_stats = ColumnStatsMetadata {
        file_name: "base.parquet".into(),
        column_name: "value".into(),
        min_value: Some(ColumnStatValue::Long(11)),
        max_value: Some(ColumnStatValue::Long(19)),
        value_count: 2,
        null_count: 0,
        total_size: 16,
        total_uncompressed_size: 16,
        is_deleted: false,
        is_tight_bound: true,
        decoded_value_type_ordinal: None,
    };
    let base_bytes = hfile(
        &[(
            "base-only".into(),
            encode_column_stats(&base_stats, StatsIndexVersion::V1).unwrap(),
        )],
        Some(hoodie_metadata_schema_json().unwrap()),
    );
    storage.put_file(&slices[0].1, base_bytes).await.unwrap();
    rewrite_logs_with_alternate_schema(storage.clone(), table.hudi_configs.clone(), &logs).await;
    let wanted = HashMap::from([
        ("base-only".into(), ("".into(), "base.parquet".into())),
        (key.clone(), ("".into(), file)),
    ]);
    let got = crate::write::metadata::load_column_stats_records(
        storage,
        table.hudi_configs.clone(),
        &wanted,
        &table.valid_metadata_instants().await.unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(got["base-only"], base_stats);
    assert_eq!(got[&key].min_value, Some(ColumnStatValue::Long(37)));
    assert_eq!(got[&key].max_value, Some(ColumnStatValue::Long(37)));
    assert_eq!(got[&key].value_count, 1);
    assert_eq!(got.len(), 2);
}
