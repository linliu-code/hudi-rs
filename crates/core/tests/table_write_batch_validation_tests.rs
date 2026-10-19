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
//! Multi-batch input schema validation must precede positional concatenation.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{
    Array, Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::error::{CoreError, Result};
use hudi_core::table::{ReadOptions, Table, UpsertOptions};
use hudi_core::write;

type Row = (String, String, i64, Option<i64>);

fn batch(rows: &[(&str, &str, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn rows(table: &Table) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for batch in table.read(&ReadOptions::new()).await? {
        let strings = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
        };
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let value = batch.column_by_name("value").and_then(|array| {
                if array.is_null(i) {
                    return None;
                }
                match array.data_type() {
                    DataType::Int64 => Some(
                        array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(i),
                    ),
                    DataType::Int32 => Some(
                        array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(i) as i64,
                    ),
                    _ => panic!("unexpected payload type"),
                }
            });
            rows.push((
                strings("id").value(i).into(),
                strings("city").value(i).into(),
                ts.value(i),
                value,
            ));
        }
    }
    rows.sort();
    Ok(rows)
}

fn store_files(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(root, &entry.path(), files);
            } else {
                files.insert(
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(path, path, &mut files);
    files
}

async fn create(path: &Path, kind: TableTypeValue, version: isize, metadata: bool) -> Table {
    Table::create(path.to_str().unwrap())
        .with_table_name("batches")
        .with_table_type(kind)
        .with_table_version(version)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .with_metadata(metadata)
        .create()
        .await
        .unwrap()
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    Shorter,
    ShorterThird,
    Longer,
    NarrowFirst,
    Renamed,
    Reordered,
    WrongType,
    EmptyLater,
    EmptyFirst,
}

fn malformed(shape: Shape) -> Vec<RecordBatch> {
    let first = batch(&[("a", "sf", 1, 10)]);
    let second = batch(&[("b", "sf", 2, 20)]);
    let rename = |batch: &RecordBatch| {
        let mut fields = batch.schema().fields().to_vec();
        fields[3] = Arc::new(Field::new("renamed", DataType::Int64, false));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap()
    };
    match shape {
        Shape::Shorter => vec![first, second.project(&[0, 1, 2]).unwrap()],
        Shape::ShorterThird => vec![first.clone(), second, first.project(&[0, 1, 2]).unwrap()],
        Shape::NarrowFirst => vec![first.project(&[0, 1, 2]).unwrap(), second],
        Shape::Longer => {
            let mut fields = second.schema().fields().to_vec();
            fields.push(Arc::new(Field::new("extra", DataType::Int64, false)));
            let mut columns = second.columns().to_vec();
            columns.push(Arc::new(Int64Array::from(vec![999])));
            vec![
                first,
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
            ]
        }
        Shape::Renamed => vec![first, rename(&second)],
        Shape::Reordered => vec![first, second.project(&[0, 1, 3, 2]).unwrap()],
        Shape::WrongType => {
            let mut fields = second.schema().fields().to_vec();
            fields[3] = Arc::new(Field::new("value", DataType::Utf8, false));
            let mut columns = second.columns().to_vec();
            columns[3] = Arc::new(StringArray::from(vec!["twenty"]));
            vec![
                first,
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
            ]
        }
        Shape::EmptyLater => vec![first, RecordBatch::new_empty(rename(&second).schema())],
        Shape::EmptyFirst => vec![RecordBatch::new_empty(rename(&first).schema()), second],
    }
}

async fn call_upsert(
    table: &mut Table,
    batches: Vec<RecordBatch>,
    boundary: usize,
) -> Result<write::WriteResult> {
    match boundary % 3 {
        0 => table.upsert(batches).await,
        1 => table.upsert_with(batches, UpsertOptions::default()).await,
        _ => write::upsert_batches(table, &batches, UpsertOptions::default()).await,
    }
}

async fn check_rejected(shape: Shape) {
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            for established in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path();
                let mut table = create(path, kind.clone(), version, version == 9).await;
                if established {
                    table
                        .append([batch(&[("existing", "old", 0, 5)])])
                        .await
                        .unwrap();
                }
                let before_files = store_files(path);
                let before_rows = rows(&table).await.unwrap();
                let context = format!("{shape:?} {kind:?} v{version} established={established}");
                let result = call_upsert(
                    &mut table,
                    malformed(shape),
                    version as usize + usize::from(established),
                )
                .await;
                if result.is_ok() {
                    let actual = rows(&Table::new(path.to_str().unwrap()).await.unwrap()).await;
                    panic!(
                        "{context}: heterogeneous input unexpectedly accepted; exact reopened readback={actual:?}"
                    );
                }
                let error = result.unwrap_err();
                assert!(
                    matches!(&error, CoreError::Schema(message) if message.contains("batches must share one schema")),
                    "{context}: unexpected error: {error}"
                );
                assert_eq!(
                    store_files(path),
                    before_files,
                    "{context}: invalid input changed storage"
                );
                assert_eq!(
                    rows(&table).await.unwrap(),
                    before_rows,
                    "{context}: established rows changed"
                );
                let result = table
                    .upsert([batch(&[("a", "sf", 1, 10)]), batch(&[("b", "sf", 2, 20)])])
                    .await
                    .unwrap();
                assert_eq!((result.num_inserts, result.num_updates), (2, 0));
                let mut expected = vec![
                    ("a".into(), "sf".into(), 1, Some(10)),
                    ("b".into(), "sf".into(), 2, Some(20)),
                ];
                if established {
                    expected.push(("existing".into(), "old".into(), 0, Some(5)));
                }
                expected.sort();
                assert_eq!(
                    rows(&Table::new(path.to_str().unwrap()).await.unwrap())
                        .await
                        .unwrap(),
                    expected,
                    "{context}: healthy retry"
                );
            }
        }
    }
}

#[tokio::test]
async fn test_shorter_later_batch_rejected() {
    check_rejected(Shape::Shorter).await;
}
#[tokio::test]
async fn test_shorter_third_batch_rejected() {
    check_rejected(Shape::ShorterThird).await;
}
#[tokio::test]
async fn test_longer_later_batch_rejected() {
    check_rejected(Shape::Longer).await;
}
#[tokio::test]
async fn test_narrow_first_batch_rejected() {
    check_rejected(Shape::NarrowFirst).await;
}
#[tokio::test]
async fn test_renamed_later_batch_rejected() {
    check_rejected(Shape::Renamed).await;
}
#[tokio::test]
async fn test_reordered_later_batch_rejected() {
    check_rejected(Shape::Reordered).await;
}
#[tokio::test]
async fn test_wrong_type_later_batch_rejected() {
    check_rejected(Shape::WrongType).await;
}
#[tokio::test]
async fn test_heterogeneous_zero_row_later_batch_rejected() {
    check_rejected(Shape::EmptyLater).await;
}
#[tokio::test]
async fn test_heterogeneous_empty_first_batch_rejected() {
    check_rejected(Shape::EmptyFirst).await;
}

fn metadata_and_nullability(input: &RecordBatch, nullable: bool, null: bool) -> RecordBatch {
    let mut fields = input.schema().fields().to_vec();
    fields[3] = Arc::new(
        Field::new("value", DataType::Int64, nullable)
            .with_metadata(HashMap::from([("note".into(), "payload".into())])),
    );
    let mut columns = input.columns().to_vec();
    if null {
        columns[3] = Arc::new(Int64Array::from(vec![None::<i64>]));
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([("source".into(), "second".into())]),
        )),
        columns,
    )
    .unwrap()
}

fn alias(id: &str, narrow: bool) -> RecordBatch {
    let normal = batch(&[(id, "sf", 1, 10)]);
    let mut fields = normal.schema().fields().to_vec();
    let mut columns = normal.columns().to_vec();
    if narrow {
        fields[0] = Arc::new(Field::new("id", DataType::LargeUtf8, false));
        columns[0] = Arc::new(LargeStringArray::from(vec![id]));
        fields[3] = Arc::new(Field::new("value", DataType::Int16, false));
        columns[3] = Arc::new(Int16Array::from(vec![10]));
    } else {
        fields[3] = Arc::new(Field::new("value", DataType::Int32, false));
        columns[3] = Arc::new(Int32Array::from(vec![10]));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn test_compatible_batches_keep_winners_aliases_and_predicate_update() {
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = create(dir.path(), kind.clone(), version, true).await;
            let first = batch(&[("a", "sf", 1, 10), ("b", "sf", 1, 20)]);
            let second = metadata_and_nullability(&batch(&[("a", "sf", 2, 30)]), true, false);
            let result = table
                .upsert([RecordBatch::new_empty(first.schema()), first, second])
                .await
                .unwrap();
            assert_eq!(result.num_inserts, 2);
            assert_eq!(
                rows(&table).await.unwrap(),
                vec![
                    ("a".into(), "sf".into(), 2, Some(30)),
                    ("b".into(), "sf".into(), 1, Some(20))
                ]
            );
            let set = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![31]))],
            )
            .unwrap();
            table.update("id = 'a'", set).await.unwrap();
            assert_eq!(
                rows(&Table::new(dir.path().to_str().unwrap()).await.unwrap())
                    .await
                    .unwrap(),
                vec![
                    ("a".into(), "sf".into(), 2, Some(31)),
                    ("b".into(), "sf".into(), 1, Some(20))
                ]
            );
            let before = store_files(dir.path());
            assert!(
                table
                    .upsert([batch(&[("c", "sf", 3, 40)]).project(&[0, 1, 2]).unwrap()])
                    .await
                    .is_err()
            );
            assert_eq!(store_files(dir.path()), before);

            let dir = tempfile::tempdir().unwrap();
            let mut table = create(dir.path(), kind.clone(), version, false).await;
            table
                .upsert([alias("a", true), alias("b", false)])
                .await
                .unwrap();
            assert_eq!(
                rows(&table).await.unwrap(),
                vec![
                    ("a".into(), "sf".into(), 1, Some(10)),
                    ("b".into(), "sf".into(), 1, Some(10))
                ]
            );
        }
    }
}

#[tokio::test]
async fn test_actual_null_tightening_rejected_and_nullable_payload_preserved() {
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = create(dir.path(), kind, 9, false).await;
        let first = batch(&[("a", "sf", 1, 10)]);
        let second = metadata_and_nullability(&batch(&[("b", "sf", 2, 20)]), true, true);
        let before = store_files(dir.path());
        assert!(table.upsert([first.clone(), second.clone()]).await.is_err());
        assert_eq!(store_files(dir.path()), before);
        table
            .upsert([metadata_and_nullability(&first, true, false), second])
            .await
            .unwrap();
        assert_eq!(
            rows(&table).await.unwrap(),
            vec![
                ("a".into(), "sf".into(), 1, Some(10)),
                ("b".into(), "sf".into(), 2, None)
            ]
        );
    }
}

#[tokio::test]
async fn test_other_write_boundaries_reject_heterogeneous_input() {
    for verb in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = Table::create(dir.path().to_str().unwrap())
            .with_table_name("boundary")
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_metadata(false);
        if verb == 1 {
            builder = builder.with_populates_meta_fields(false);
        }
        let mut table = builder.create().await.unwrap();
        let before = store_files(dir.path());
        let malformed = malformed(Shape::Shorter);
        let result = match verb {
            0 => table.append(malformed).await.map(|_| ()),
            1 => table.append_only(malformed).await.map(|_| ()),
            2 => table.overwrite(malformed).await.map(|_| ()),
            _ => table
                .dynamic_partition_overwrite(malformed)
                .await
                .map(|_| ()),
        };
        assert!(result.is_err());
        assert_eq!(store_files(dir.path()), before);
    }
    let dir = tempfile::tempdir().unwrap();
    let mut table = create(dir.path(), TableTypeValue::MergeOnRead, 9, true).await;
    let before = store_files(dir.path());
    assert!(matches!(
        table.upsert(Vec::<RecordBatch>::new()).await,
        Err(CoreError::Write(_))
    ));
    assert!(matches!(
        table.upsert([batch(&[]), batch(&[])]).await,
        Err(CoreError::Write(_))
    ));
    assert_eq!(store_files(dir.path()), before);
    let error = table
        .upsert_with(
            malformed(Shape::Shorter),
            UpsertOptions {
                update_columns: Some(vec!["value".into()]),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, CoreError::Unsupported(_)));
    assert_eq!(store_files(dir.path()), before);
}
