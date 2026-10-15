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
//! Creation and write-input boundary regressions.

use arrow_array::{
    Array, ArrayRef, Int8Array, Int16Array, LargeStringArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::{HudiTableConfig, TableTypeValue};
use hudi_core::table::{ReadOptions, Table};
use std::sync::Arc;

fn batch(id: &str, partition: &str) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("p", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(StringArray::from(vec![partition])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_encoded_partitions_preserve_equality_filters() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap();
            let mut table = Table::create(path)
                .with_table_name("encoded")
                .with_table_type(table_type.clone())
                .with_table_version(version)
                .with_record_key_fields(["id"])
                .with_partition_fields(["p"])
                .with_option(HudiTableConfig::IsPartitionPathUrlencoded, "true")
                .create()
                .await
                .unwrap();
            for (id, value) in [
                ("a", "a%20b"),
                ("b", "a/b"),
                ("c", "a=b"),
                ("d", "café"),
                ("e", ""),
                ("f", "__HIVE_DEFAULT_PARTITION__"),
            ] {
                table.append([batch(id, value)]).await.unwrap();
            }
            let table = Table::new(path).await.unwrap();
            let all = table.read(&ReadOptions::new()).await.unwrap();
            assert_eq!(all.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
            for (id, value, encoded) in [
                ("a", "a%20b", "p=a%2520b"),
                ("b", "a/b", "p=a%2Fb"),
                ("c", "a=b", "p=a%3Db"),
                ("d", "café", "p=café"),
                ("e", "", "p=__HIVE_DEFAULT_PARTITION__"),
                (
                    "f",
                    "__HIVE_DEFAULT_PARTITION__",
                    "p=__HIVE_DEFAULT_PARTITION__",
                ),
            ] {
                let rows = table
                    .read(
                        &ReadOptions::new()
                            .with_filters([("p", "=", value)])
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    1,
                    "{table_type:?} v{version} {value}"
                );
                let row = rows.iter().find(|b| b.num_rows() > 0).unwrap();
                let strings = |name| {
                    row.column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                };
                assert_eq!(strings("id").value(0), id);
                assert_eq!(strings("_hoodie_partition_path").value(0), encoded);
            }
        }
    }
}

#[tokio::test]
async fn test_properties_preserve_names_and_formatted_schema() {
    for (name, escaped) in [
        ("café", r"caf\u00E9"),
        (r"review\table", r"review\\table"),
        (" review", r"\ review"),
        ("table😀", r"table\uD83D\uDE00"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let schema = serde_json::json!({
            "type": "record",
            "name": "record",
            "fields": [{"name": "id", "type": "string"}]
        });
        let schema = serde_json::to_string_pretty(&schema).unwrap();
        let table = Table::create(path)
            .with_table_name(name)
            .with_option(HudiTableConfig::CreateSchema, schema)
            .create()
            .await
            .unwrap();
        assert_eq!(table.get_schema().await.unwrap().fields()[0].name(), "id");
        let reopened = Table::new(path).await.unwrap();
        let actual: String = reopened
            .hudi_configs
            .get(HudiTableConfig::TableName)
            .unwrap()
            .into();
        assert_eq!(actual, name);
        let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
        assert!(
            props.contains(&format!("hoodie.table.name={escaped}\n")),
            "{props}"
        );
        let metadata = std::fs::read_to_string(
            dir.path()
                .join(".hoodie/metadata/.hoodie/hoodie.properties"),
        )
        .unwrap();
        assert!(
            metadata.contains(&format!("hoodie.table.name={escaped}_metadata\n")),
            "{metadata}"
        );
    }
}

#[tokio::test]
async fn test_create_rejects_managed_option_overrides_before_persisting() {
    for (key, value) in [
        ("hoodie.table.name", "renamed"),
        ("hoodie.table.version", "6"),
        ("hoodie.table.type", "MERGE_ON_READ"),
        ("hoodie.table.metadata.partitions", "files"),
        ("hoodie.table.recordkey.fields", "other"),
        ("hoodie.populate.meta.fields", "false"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let result = Table::create(dir.path().to_str().unwrap())
            .with_table_name("original")
            .with_option(key, value)
            .create()
            .await;
        assert!(result.is_err(), "managed override accepted: {key}");
        assert!(
            !dir.path().join(".hoodie/hoodie.properties").exists(),
            "{key} left a partial table"
        );
    }
}

#[tokio::test]
async fn test_narrow_and_large_arrow_types_support_repeated_writes() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let cases: Vec<(ArrayRef, DataType)> = vec![
            (Arc::new(Int8Array::from(vec![7])), DataType::Int32),
            (Arc::new(Int16Array::from(vec![300])), DataType::Int32),
            (
                Arc::new(LargeStringArray::from(vec!["value"])),
                DataType::Utf8,
            ),
        ];
        for (values, expected_type) in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap();
            let mut table = Table::create(path)
                .with_table_name("types")
                .with_table_type(table_type.clone())
                .with_record_key_fields(["id"])
                .create()
                .await
                .unwrap();
            let input = |id| {
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Utf8, false),
                        Field::new("v", values.data_type().clone(), false),
                    ])),
                    vec![Arc::new(StringArray::from(vec![id])), values.clone()],
                )
                .unwrap()
            };
            table.append([input("a")]).await.unwrap();
            table.append([input("b")]).await.unwrap();
            table.upsert([input("a")]).await.unwrap();
            let table = Table::new(path).await.unwrap();
            assert_eq!(
                table
                    .get_schema()
                    .await
                    .unwrap()
                    .field_with_name("v")
                    .unwrap()
                    .data_type(),
                &expected_type
            );
            let rows = table.read(&ReadOptions::new()).await.unwrap();
            assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
            for row in rows {
                assert_eq!(row.column_by_name("v").unwrap().data_type(), &expected_type);
                let as_text =
                    arrow::compute::cast(row.column_by_name("v").unwrap(), &DataType::Utf8)
                        .unwrap();
                let expected = arrow::compute::cast(&values, &DataType::Utf8).unwrap();
                for value in as_text
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                {
                    assert_eq!(
                        value,
                        Some(
                            expected
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(0)
                        )
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn test_empty_explicit_key_rejected_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("keys")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    assert!(table.append([batch("", "sf")]).await.is_err());
    assert!(table.timeline.completed_commits.is_empty());
    assert!(table.upsert([batch("", "sf")]).await.is_err());
    assert!(table.timeline.completed_commits.is_empty());
    table.append([batch("valid", "sf")]).await.unwrap();
    assert_eq!(
        table
            .read(&ReadOptions::new())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn test_nested_aliases_preserve_null_elements_after_repeated_writes() {
    use arrow_array::{Int32Array, LargeListArray, ListArray, StructArray, types::Int16Type};
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("nested")
            .with_table_type(table_type)
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        let list = Arc::new(LargeListArray::from_iter_primitive::<Int16Type, _, _>([
            Some(vec![Some(7), None]),
        ]));
        let details = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("values", list.data_type().clone(), false)),
            list as ArrayRef,
        )]));
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("details", details.data_type().clone(), false),
            ])),
            vec![Arc::new(StringArray::from(vec!["a"])), details],
        )
        .unwrap();
        table.append([input.clone()]).await.unwrap();
        table.upsert([input]).await.unwrap();
        let rows = table.read(&ReadOptions::new()).await.unwrap();
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let row = rows.iter().find(|b| b.num_rows() > 0).unwrap();
        let details = row
            .column_by_name("details")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let values = details
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.value(0), 7);
        assert!(values.is_null(1));
    }
}

#[tokio::test]
async fn test_binary_timestamp_decimal_aliases_preserve_values_and_nulls() {
    use arrow::datatypes::i256;
    use arrow_array::{
        BinaryArray, Decimal128Array, Decimal256Array, LargeBinaryArray, TimestampMicrosecondArray,
    };
    use arrow_schema::TimeUnit;
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("aliases")
            .with_table_type(table_type)
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        let bytes: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(&[0xff, 0x00][..]),
            None,
            Some(&b"payload"[..]),
        ]));
        let times: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_123_456),
                None,
                Some(-1_234_567),
            ])
            .with_timezone("Europe/Berlin"),
        );
        let decimals: ArrayRef = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(-12_345)),
                None,
                Some(i256::from_i128(987_654_321)),
            ])
            .with_precision_and_scale(38, 4)
            .unwrap(),
        );
        let input = |ids: Vec<&str>| {
            let columns = vec![
                Arc::new(StringArray::from(ids)) as ArrayRef,
                bytes.clone(),
                times.clone(),
                decimals.clone(),
            ];
            let fields = ["id", "bytes", "time", "decimal"]
                .into_iter()
                .zip(&columns)
                .map(|(name, column)| Field::new(name, column.data_type().clone(), name != "id"))
                .collect::<Vec<_>>();
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        };
        table.append([input(vec!["a", "b", "c"])]).await.unwrap();
        table.append([input(vec!["d", "e", "f"])]).await.unwrap();
        table.upsert([input(vec!["a", "b", "c"])]).await.unwrap();
        let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        let rows = table.read(&ReadOptions::new()).await.unwrap();
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
        let mut seen_ids = std::collections::HashSet::new();
        for batch in rows {
            let column = |name| batch.column_by_name(name).unwrap();
            assert_eq!(column("bytes").data_type(), &DataType::Binary);
            assert_eq!(
                column("time").data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            );
            assert_eq!(column("decimal").data_type(), &DataType::Decimal128(38, 4));
            let ids = column("id").as_any().downcast_ref::<StringArray>().unwrap();
            let bytes = column("bytes")
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let times = column("time")
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let decimals = column("decimal")
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(seen_ids.insert(ids.value(row).to_string()));
                let expected_null = matches!(ids.value(row), "b" | "e");
                assert_eq!(bytes.is_null(row), expected_null);
                assert_eq!(times.is_null(row), expected_null);
                assert_eq!(decimals.is_null(row), expected_null);
                match ids.value(row) {
                    "b" | "e" => {
                        assert!(bytes.is_null(row));
                        assert!(times.is_null(row));
                        assert!(decimals.is_null(row));
                    }
                    "a" | "d" => {
                        assert_eq!(bytes.value(row), &[0xff, 0x00]);
                        assert_eq!(times.value(row), 1_700_000_000_123_456);
                        assert_eq!(decimals.value(row), -12_345);
                    }
                    "c" | "f" => {
                        assert_eq!(bytes.value(row), b"payload");
                        assert_eq!(times.value(row), -1_234_567);
                        assert_eq!(decimals.value(row), 987_654_321);
                    }
                    id => panic!("unexpected record {id}"),
                }
            }
        }
        assert_eq!(seen_ids.len(), 6);
    }
}

#[tokio::test]
async fn test_decimal_precision_overflow_rejected_before_commit() {
    use arrow::datatypes::i256;
    use arrow_array::Decimal256Array;
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("precision")
            .with_table_type(table_type)
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        let values = Arc::new(
            Decimal256Array::from(vec![i256::from_i128(1)])
                .with_precision_and_scale(39, 0)
                .unwrap(),
        );
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("v", DataType::Decimal256(39, 0), false),
            ])),
            vec![Arc::new(StringArray::from(vec!["a"])), values],
        )
        .unwrap();
        for error in [
            table.append([input.clone()]).await.unwrap_err(),
            table.upsert([input]).await.unwrap_err(),
        ] {
            assert!(matches!(error, hudi_core::error::CoreError::Unsupported(_)));
            assert!(error.to_string().contains("precision 39"));
        }
        assert!(table.timeline.completed_commits.is_empty());
        let timeline = dir.path().join(".hoodie/timeline");
        for entry in std::fs::read_dir(timeline).unwrap() {
            assert_eq!(entry.unwrap().file_name(), ".keep");
        }
    }
}
