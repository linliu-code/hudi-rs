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
//! Current startup schema behavior, separate from ordering-field UPDATE policy.
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::{HudiTableConfig, TableTypeValue};
use hudi_core::error::CoreError;
use hudi_core::table::{ReadOptions, Table};
use hudi_core::write::WriteResult;

fn declared(value_type: &str) -> String {
    serde_json::json!({"type":"record","name":"row","fields":[
        {"name":"id","type":"string"}, {"name":"ts","type":"long"},
        {"name":"value","type":value_type}
    ]})
    .to_string()
}

fn input(id: &str, value: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("value", value.data_type().clone(), false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(Int64Array::from(vec![1])),
            value,
        ],
    )
    .unwrap()
}
fn full(id: &str, value: i64) -> RecordBatch {
    input(id, Arc::new(Int64Array::from(vec![value])))
}
fn set(column: &str, value: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            column,
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap()
}
async fn create(
    path: &Path,
    kind: TableTypeValue,
    version: isize,
    schema: Option<&str>,
    metadata: bool,
) -> Table {
    let mut builder = Table::create(path.to_str().unwrap())
        .with_table_name("schema-policy")
        .with_table_type(kind)
        .with_table_version(version)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["ts"])
        .with_metadata(metadata);
    if let Some(schema) = schema {
        builder = builder.with_option(HudiTableConfig::CreateSchema, schema);
    }
    builder.create().await.unwrap()
}
async fn rows(table: &Table) -> Vec<(String, i64, i64)> {
    let mut rows = Vec::new();
    for batch in table.read(&ReadOptions::new()).await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i).into(), ts.value(i), values.value(i)));
        }
    }
    rows.sort();
    rows
}
fn files(path: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, path: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().to_str().unwrap().into(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(path, path, &mut out);
    out
}
fn assert_no_unfinished_fences(path: &Path) {
    let files = files(path);
    for name in files.keys() {
        assert!(!name.contains("/.temp/"), "leftover marker: {name}");
        if name.ends_with(".requested") || name.ends_with(".inflight") {
            let path = Path::new(name);
            let instant = path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .split('.')
                .next()
                .unwrap();
            // Completed instants may retain same-instant fences until maintenance.
            assert!(
                files.keys().any(|other| {
                    let other = Path::new(other);
                    other.parent() == path.parent()
                        && other
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .starts_with(&format!("{instant}_"))
                        && ["commit", "deltacommit", "replacecommit", "rollback"]
                            .contains(&other.extension().and_then(|e| e.to_str()).unwrap_or(""))
                }),
                "unfinished fence: {name}"
            );
        }
    }
}
async fn full_write(
    table: &mut Table,
    batch: RecordBatch,
    upsert: bool,
) -> hudi_core::error::Result<()> {
    if upsert {
        table.upsert([batch]).await?;
    } else {
        table.append([batch]).await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_first_full_input_establishes_schema_then_mismatches_reject_without_artifacts() {
    let schema = declared("string");
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            // Append with MDT/RLI and full upsert with metadata disabled exercise
            // both entrypoints without repeating the general first-upsert matrix.
            for upsert in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut table =
                    create(dir.path(), kind.clone(), version, Some(&schema), !upsert).await;
                assert_eq!(
                    table
                        .get_schema()
                        .await
                        .unwrap()
                        .field_with_name("value")
                        .unwrap()
                        .data_type(),
                    &DataType::Utf8
                );
                assert!(
                    table
                        .read(&ReadOptions::new())
                        .await
                        .unwrap()
                        .iter()
                        .all(|batch| batch.num_rows() == 0)
                );
                full_write(&mut table, full("a", 11), upsert).await.unwrap();
                assert_eq!(rows(&table).await, vec![("a".into(), 1, 11)]);
                let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                assert_eq!(
                    table
                        .get_schema()
                        .await
                        .unwrap()
                        .field_with_name("value")
                        .unwrap()
                        .data_type(),
                    &DataType::Int64
                );
                let stored: String = table
                    .hudi_configs
                    .get(HudiTableConfig::CreateSchema)
                    .unwrap()
                    .into();
                assert_eq!(stored, schema);
                let before = files(dir.path());
                let completed = table.get_timeline().completed_commits.clone();
                let wrong = input("bad", Arc::new(StringArray::from(vec!["not-int"])));
                assert!(
                    full_write(&mut table, wrong, upsert).await.is_err(),
                    "{kind:?} v{version} upsert={upsert}"
                );
                assert_eq!(files(dir.path()), before);
                assert_eq!(table.get_timeline().completed_commits, completed);
                full_write(&mut table, full("b", 23), upsert).await.unwrap();
                assert_eq!(
                    rows(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await,
                    vec![("a".into(), 1, 11), ("b".into(), 1, 23)]
                );
                assert_no_unfinished_fences(dir.path());
            }
        }
    }
}

#[tokio::test]
async fn test_update_requires_known_schema_and_valid_empty_updates_leave_no_pending_write() {
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            for has_schema in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let schema = declared("long");
                let mut table = create(
                    dir.path(),
                    kind.clone(),
                    version,
                    has_schema.then_some(schema.as_str()),
                    true,
                )
                .await;
                let before = files(dir.path());
                let result = table.update("id = 'a'", set("value", 19)).await;
                if has_schema {
                    assert_eq!(result.unwrap(), WriteResult::default());
                    assert_eq!(files(dir.path()), before);
                } else {
                    assert!(
                        matches!(result, Err(CoreError::SchemaNotFound(_))),
                        "{kind:?} v{version}: {result:?}"
                    );
                    assert_eq!(files(dir.path()), before);
                }
                assert!(table.get_timeline().completed_commits.is_empty());
                assert_no_unfinished_fences(dir.path());
                let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                if has_schema {
                    assert!(rows(&table).await.is_empty());
                    assert_eq!(
                        table
                            .get_schema()
                            .await
                            .unwrap()
                            .field_with_name("value")
                            .unwrap()
                            .data_type(),
                        &DataType::Int64
                    );
                    let stored: String = table
                        .hudi_configs
                        .get(HudiTableConfig::CreateSchema)
                        .unwrap()
                        .into();
                    assert_eq!(stored, schema);
                }
                table.append([full("a", 11)]).await.unwrap();
                assert_eq!(
                    table
                        .update("id = 'a'", set("value", 19))
                        .await
                        .unwrap()
                        .num_updates,
                    1
                );
                assert_eq!(rows(&table).await, vec![("a".into(), 1, 19)]);
                assert_eq!(table.delete("id = 'a'").await.unwrap().num_deletes, 1);
                assert!(rows(&table).await.is_empty());
                let completed = table.get_timeline().completed_commits.clone();
                assert_eq!(
                    table.update("id = 'a'", set("value", 29)).await.unwrap(),
                    WriteResult::default()
                );
                assert_eq!(table.get_timeline().completed_commits, completed);
                assert_no_unfinished_fences(dir.path());
                let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                assert!(rows(&reopened).await.is_empty());
                assert_eq!(
                    reopened
                        .get_schema()
                        .await
                        .unwrap()
                        .field_with_name("value")
                        .unwrap()
                        .data_type(),
                    &DataType::Int64
                );
            }
        }
    }
}

#[tokio::test]
async fn test_declared_empty_invalid_fields_report_validation_without_claiming_row_changes() {
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = create(
                dir.path(),
                kind.clone(),
                version,
                Some(&declared("long")),
                true,
            )
            .await;
            for (filter, column) in [("id = 'a'", "missing"), ("missing = 'a'", "value")] {
                let result = table.update(filter, set(column, 19)).await;
                // Observe the current validation difference without prescribing
                // whether invalid fields must be checked when there are no rows.
                println!(
                    "declared-empty {kind:?} v{version} filter={filter} set={column}: {result:?}"
                );
                if let Ok(result) = result {
                    assert_eq!(result, WriteResult::default());
                }
                assert!(table.get_timeline().completed_commits.is_empty());
                assert!(rows(&table).await.is_empty());
                assert_no_unfinished_fences(dir.path());
            }
        }
    }
}
