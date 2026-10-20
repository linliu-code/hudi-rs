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
//! Ordinary prefixed columns must survive mutation commit schemas and reader projection.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::read::HudiReadConfig;
use hudi_core::config::table::TableTypeValue;
use hudi_core::index::HoodieKey;
use hudi_core::table::{ReadOptions, Table};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

type Row = (String, i64, i64, Option<String>);

fn input(rows: &[(&str, i64, i64, Option<&str>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("_hoodie_custom", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let strings = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
        };
        let ints = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        };
        for i in 0..batch.num_rows() {
            let custom = strings("_hoodie_custom");
            rows.push((
                strings("id").value(i).into(),
                ints("ts").value(i),
                ints("value").value(i),
                (!custom.is_null(i)).then(|| custom.value(i).into()),
            ));
        }
    }
    rows.sort();
    rows
}

async fn assert_reopened(path: &Path, expected: &[Row], stage: &str) {
    let table = Table::new(path.to_str().unwrap()).await.unwrap();
    let fields = ["id", "ts", "value", "_hoodie_custom"];
    let arrow = table.get_schema().await.unwrap();
    assert_eq!(
        arrow
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        fields,
        "Arrow schema after {stage}"
    );
    assert_eq!(
        arrow.field_with_name("_hoodie_custom").unwrap().data_type(),
        &DataType::Utf8
    );
    assert!(
        arrow
            .field_with_name("_hoodie_custom")
            .unwrap()
            .is_nullable()
    );
    let avro: serde_json::Value =
        serde_json::from_str(&table.get_schema_in_avro_str().await.unwrap()).unwrap();
    assert_eq!(
        avro["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        fields,
        "Avro schema after {stage}"
    );
    for engine in ["1", "2"] {
        let options = ReadOptions::new()
            .with_hudi_option(HudiReadConfig::FileGroupReaderVersion.as_ref(), engine);
        assert_eq!(
            rows(&table.read(&options).await.unwrap()),
            expected,
            "reader{engine} after {stage}"
        );
    }
}

fn assert_physical_column(path: &Path) -> usize {
    let mut count = 0;
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            count += assert_physical_column(&path);
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
                    .unwrap();
            assert!(
                builder.schema().field_with_name("_hoodie_custom").is_ok(),
                "{}",
                path.display()
            );
            let physical = builder
                .build()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(!rows(&physical).is_empty());
            count += 1;
        }
    }
    count
}

async fn lifecycle_case(version: isize, mor: bool, metadata: bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("prefixed_columns")
        .with_table_type(if mor {
            TableTypeValue::MergeOnRead
        } else {
            TableTypeValue::CopyOnWrite
        })
        .with_table_version(version)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["ts"])
        .with_metadata(metadata)
        .create()
        .await
        .unwrap();
    let seed = input(&[("a", 10, 20, Some("retained-payload")), ("b", 30, 30, None)]);
    let expected = rows(std::slice::from_ref(&seed));
    table.append([seed.clone()]).await.unwrap();
    assert_reopened(dir.path(), &expected, "append").await;
    table.upsert([seed]).await.unwrap();
    // Physical presence distinguishes a commit-schema projection error from missing data.
    assert!(assert_physical_column(dir.path()) > 0);
    assert_reopened(dir.path(), &expected, "same-schema upsert").await;
    table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    table
        .append([input(&[("c", 40, 40, Some("subsequent-append"))])])
        .await
        .unwrap();
    table
        .upsert([input(&[("a", 50, 60, Some("replacement-payload"))])])
        .await
        .unwrap();
    let expected = rows(&[input(&[
        ("a", 50, 60, Some("replacement-payload")),
        ("b", 30, 30, None),
        ("c", 40, 40, Some("subsequent-append")),
    ])]);
    assert_reopened(dir.path(), &expected, "subsequent append and upsert").await;
    assert!(assert_physical_column(dir.path()) > 0);
    table
        .delete_keys([HoodieKey {
            record_key: "b".into(),
            partition_path: "".into(),
        }])
        .await
        .unwrap();
    let expected = expected
        .into_iter()
        .filter(|row| row.0 != "b")
        .collect::<Vec<_>>();
    assert_reopened(dir.path(), &expected, "delete_keys").await;
}

async fn lifecycle(version: isize, mor: bool) {
    for metadata in [false, true] {
        lifecycle_case(version, mor, metadata).await;
    }
}

#[tokio::test]
async fn test_prefixed_column_cow_v8_lifecycle() {
    lifecycle(8, false).await;
}
#[tokio::test]
async fn test_prefixed_column_cow_v9_lifecycle() {
    lifecycle(9, false).await;
}
#[tokio::test]
async fn test_prefixed_column_mor_v8_lifecycle() {
    lifecycle(8, true).await;
}
#[tokio::test]
async fn test_prefixed_column_mor_v9_lifecycle() {
    lifecycle(9, true).await;
}

#[tokio::test]
async fn test_false_delete_marker_survives_mutation_schema() {
    for version in [8, 9] {
        for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = Table::create(dir.path().to_str().unwrap())
                .with_table_name("delete_marker_schema")
                .with_table_type(kind)
                .with_table_version(version)
                .with_record_key_fields(["id"])
                .with_ordering_fields(["ts"])
                .with_metadata(false)
                .create()
                .await
                .unwrap();
            let data = input(&[("a", 10, 20, Some("payload"))]);
            let mut fields = data.schema().fields().to_vec();
            fields.push(Arc::new(Field::new(
                "_hoodie_is_deleted",
                DataType::Boolean,
                false,
            )));
            let mut columns = data.columns().to_vec();
            columns.push(Arc::new(BooleanArray::from(vec![false])));
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            table.append([batch.clone()]).await.unwrap();
            table.upsert([batch.clone()]).await.unwrap();
            let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            assert_eq!(
                reopened
                    .get_schema()
                    .await
                    .unwrap()
                    .field_with_name("_hoodie_is_deleted")
                    .unwrap()
                    .data_type(),
                &DataType::Boolean
            );
            let avro: serde_json::Value =
                serde_json::from_str(&reopened.get_schema_in_avro_str().await.unwrap()).unwrap();
            assert_eq!(
                avro["fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|field| field["name"] == "_hoodie_is_deleted")
                    .unwrap()["type"],
                "boolean"
            );
            for engine in ["1", "2"] {
                let options = ReadOptions::new()
                    .with_hudi_option(HudiReadConfig::FileGroupReaderVersion.as_ref(), engine);
                let actual = reopened.read(&options).await.unwrap();
                assert_eq!(rows(&actual), rows(std::slice::from_ref(&batch)));
                for batch in actual {
                    let marker = batch
                        .column_by_name("_hoodie_is_deleted")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        assert!(!marker.is_null(row));
                        assert!(!marker.value(row));
                    }
                }
            }
        }
    }
}
