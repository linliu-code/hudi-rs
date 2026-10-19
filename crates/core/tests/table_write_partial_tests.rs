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
//! Partial upserts resolve whole-row winners before applying selected columns and routing.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::index::{HoodieIndex, HoodieKey};

use hudi_core::table::{ReadOptions, Table, UpsertOptions};

type Row = (String, String, i64, i64);
type StoredRow = (Row, String, String);

fn row(id: &str, city: &str, ts: i64, value: i64) -> Row {
    (id.into(), city.into(), ts, value)
}

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
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

async fn snapshot(table: &Table) -> Vec<StoredRow> {
    let mut result = Vec::new();
    for batch in table.read(&ReadOptions::new()).await.unwrap() {
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
            result.push((
                row(
                    strings("id").value(i),
                    strings("city").value(i),
                    ints("ts").value(i),
                    ints("value").value(i),
                ),
                strings("_hoodie_commit_time").value(i).into(),
                strings("_hoodie_commit_seqno").value(i).into(),
            ));
        }
    }
    result.sort();
    result
}

async fn rows(table: &Table) -> Vec<Row> {
    let mut result: Vec<_> = snapshot(table).await.into_iter().map(|r| r.0).collect();
    result.sort();
    result
}

async fn create(path: &std::path::Path, version: isize, metadata: bool) -> Table {
    Table::create(path.to_str().unwrap())
        .with_table_name("partial")
        .with_table_version(version)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .with_metadata(metadata)
        .with_record_index(metadata)
        .create()
        .await
        .unwrap()
}
fn partial(columns: &[&str]) -> UpsertOptions {
    UpsertOptions {
        update_columns: Some(columns.iter().map(|s| s.to_string()).collect()),
    }
}

#[tokio::test]
async fn test_partial_moves_preserve_excluded_fields() {
    let mut failures = vec![];
    for version in [8, 9] {
        for metadata in [false, true] {
            for move_city in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut table = create(dir.path(), version, metadata).await;
                table
                    .append([batch(&[row("a", "sf", 1, 10)])])
                    .await
                    .unwrap();
                table
                    .upsert_with(
                        [batch(&[row("a", "ny", 2, 20)])],
                        partial(if move_city {
                            &["city", "value"]
                        } else {
                            &["value"]
                        }),
                    )
                    .await
                    .unwrap();
                let expected = vec![row("a", if move_city { "ny" } else { "sf" }, 1, 20)];
                let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                for actual in [rows(&table).await, rows(&reopened).await] {
                    if actual != expected {
                        failures.push(format!("v{version} metadata={metadata} move={move_city}: actual={actual:?}, expected={expected:?}"));
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn test_partial_duplicate_baseline_uses_existing_winner() {
    let mut failures = vec![];
    for reverse in [false, true] {
        for incoming_ts in [4, 20] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = create(dir.path(), 8, false).await;
            let mut old = vec![row("a", "sf", 10, 100), row("a", "sf", 5, 50)];
            if reverse {
                old.reverse();
            }
            table.append([batch(&old)]).await.unwrap();
            let before = snapshot(&table).await;
            let winner = before.iter().find(|r| r.0.2 == 10).unwrap().clone();
            let write = table
                .upsert_with(
                    [batch(&[row("a", "sf", incoming_ts, 999)])],
                    partial(&["value"]),
                )
                .await
                .unwrap();
            let after = snapshot(&table).await;
            let expected = if incoming_ts < 10 {
                vec![winner]
            } else {
                vec![(
                    row("a", "sf", 10, 999),
                    write.instant.clone(),
                    after[0].2.clone(),
                )]
            };
            if after != expected {
                failures.push(format!("reverse={reverse} incoming_ts={incoming_ts}: actual={after:?}, expected={expected:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn test_partial_raw_order_and_selected_order_are_distinct() {
    for event in [false, true] {
        for ts in [5, 10, 20] {
            for move_city in [false, true] {
                for select_ts in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let mut table = Table::create(dir.path().to_str().unwrap())
                        .with_table_name("ordering")
                        .with_record_key_fields(["id"])
                        .with_partition_fields(["city"])
                        .with_ordering_fields(["ts"])
                        .with_option(
                            "hoodie.record.merge.mode",
                            if event {
                                "EVENT_TIME_ORDERING"
                            } else {
                                "COMMIT_TIME_ORDERING"
                            },
                        )
                        .with_metadata(false)
                        .create()
                        .await
                        .unwrap();
                    table
                        .append([batch(&[row("a", "sf", 10, 100)])])
                        .await
                        .unwrap();
                    let original = snapshot(&table).await;
                    let mut columns = vec!["value"];
                    if move_city {
                        columns.push("city");
                    }
                    if select_ts {
                        columns.push("ts");
                    }
                    let write = table
                        .upsert_with([batch(&[row("a", "ny", ts, 999)])], partial(&columns))
                        .await
                        .unwrap();
                    let accepted = !event || ts >= 10;
                    let expected = row(
                        "a",
                        if accepted && move_city { "ny" } else { "sf" },
                        if accepted && select_ts { ts } else { 10 },
                        if accepted { 999 } else { 100 },
                    );
                    let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                    assert_eq!(
                        rows(&reopened).await,
                        vec![expected],
                        "event={event} ts={ts} move={move_city} selected_ts={select_ts}"
                    );
                    let after = snapshot(&reopened).await;
                    if accepted {
                        assert_eq!(after[0].1, write.instant);
                    } else {
                        assert_eq!(after, original);
                    }
                }
            }
        }
    }
}

async fn assert_location(table: &Table, root: &std::path::Path, partition: &str) {
    let request = HoodieKey {
        record_key: "a".into(),
        partition_path: "city=irrelevant".into(),
    };
    let locations = hudi_core::index::for_table(table)
        .tag_location(table, std::slice::from_ref(&request))
        .await
        .unwrap();
    let location = locations[&request].as_ref().unwrap();
    assert_eq!(location.partition_path, partition);
    let mut found = 0;
    for slice in table.get_file_slices(&ReadOptions::new()).await.unwrap() {
        let Some(path) = slice.base_file_relative_path().unwrap() else {
            continue;
        };
        let file = std::fs::File::open(root.join(path)).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let strings = |name| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
            };
            for i in 0..batch.num_rows() {
                if strings("id").value(i) == "a" {
                    found += 1;
                    assert_eq!(strings("_hoodie_partition_path").value(i), partition);
                    assert_eq!(slice.partition_path, partition);
                    assert_eq!(slice.file_id(), location.file_id);
                }
            }
        }
    }
    assert_eq!(found, 1);
}

#[tokio::test]
async fn test_partial_effective_partition_index_and_continuation() {
    for metadata in [false, true] {
        for move_city in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = create(dir.path(), 9, metadata).await;
            table
                .append([batch(&[row("a", "sf", 1, 10), row("keep", "sf", 1, 30)])])
                .await
                .unwrap();
            let columns = if move_city {
                vec!["city", "value"]
            } else {
                vec!["value"]
            };
            let write = table
                .upsert_with([batch(&[row("a", "ny", 2, 20)])], partial(&columns))
                .await
                .unwrap();
            assert_eq!((write.num_updates, write.num_inserts), (1, 0));
            let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let partition = if move_city { "city=ny" } else { "city=sf" };
            assert_location(&table, dir.path(), partition).await;
            table
                .upsert_with([batch(&[row("a", "ignored", 3, 99)])], partial(&["value"]))
                .await
                .unwrap();
            let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            assert_location(&table, dir.path(), partition).await;
            assert_eq!(
                rows(&table).await,
                vec![
                    row("a", if move_city { "ny" } else { "sf" }, 1, 99),
                    row("keep", "sf", 1, 30)
                ]
            );
            table
                .delete_keys([HoodieKey {
                    record_key: "a".into(),
                    partition_path: partition.into(),
                }])
                .await
                .unwrap();
            assert_eq!(
                rows(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await,
                vec![row("keep", "sf", 1, 30)]
            );
        }
    }
}

#[tokio::test]
async fn test_partial_mixed_groups_preserve_untouched_multiset_with_packing() {
    for metadata in [false, true] {
        for packing in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = Table::create(dir.path().to_str().unwrap())
                .with_table_name("mixed")
                .with_record_key_fields(["id"])
                .with_partition_fields(["city"])
                .with_ordering_fields(["ts"])
                .with_metadata(metadata)
                .with_record_index(metadata)
                .with_option(
                    "hoodie.parquet.small.file.limit",
                    if packing { "104857600" } else { "0" },
                )
                .create()
                .await
                .unwrap();
            table
                .append([batch(&[
                    row("a", "sf", 10, 100),
                    row("stale", "sf", 10, 200),
                    row("dup", "sf", 1, 10),
                    row("dup", "sf", 2, 20),
                ])])
                .await
                .unwrap();
            table
                .append([batch(&[
                    row("b", "ny", 10, 300),
                    row("other", "ny", 1, 10),
                    row("other", "ny", 2, 20),
                ])])
                .await
                .unwrap();
            let before = snapshot(&table).await;
            let untouched: Vec<_> = before
                .into_iter()
                .filter(|r| ["dup", "other", "stale"].contains(&r.0.0.as_str()))
                .collect();
            let write = table
                .upsert_with(
                    [batch(&[
                        row("a", "ny", 20, 101),
                        row("b", "ny", 20, 301),
                        row("stale", "ny", 5, 999),
                        row("new", "ny", 7, 70),
                    ])],
                    partial(&["city", "value"]),
                )
                .await
                .unwrap();
            assert_eq!((write.num_updates, write.num_inserts), (3, 1));
            let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let after = snapshot(&reopened).await;
            let survived: Vec<_> = after
                .iter()
                .filter(|r| ["dup", "other", "stale"].contains(&r.0.0.as_str()))
                .cloned()
                .collect();
            assert_eq!(survived, untouched);
            let mut expected = vec![
                row("a", "ny", 10, 101),
                row("b", "ny", 10, 301),
                row("stale", "sf", 10, 200),
                row("new", "ny", 7, 70),
                row("dup", "sf", 1, 10),
                row("dup", "sf", 2, 20),
                row("other", "ny", 1, 10),
                row("other", "ny", 2, 20),
            ];
            expected.sort();
            assert_eq!(rows(&reopened).await, expected);
        }
    }
}

fn nullable_batch(city: &str, ts: i64, note: Option<&str>, nullable: bool) -> RecordBatch {
    let base = batch(&[row("a", city, ts, 20)]);
    let mut fields = base.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("note", DataType::Utf8, nullable)));
    let mut columns = base.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![note])) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
#[tokio::test]
async fn test_partial_preserved_null_allows_nonnullable_input() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = create(dir.path(), 8, false).await;
    table
        .append([nullable_batch("sf", 1, None, true)])
        .await
        .unwrap();
    table
        .upsert_with(
            [nullable_batch("ny", 2, Some("ignored"), false)],
            partial(&["city", "value"]),
        )
        .await
        .unwrap();
    let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    let batches = reopened.read(&ReadOptions::new()).await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let nonempty = batches.iter().find(|b| b.num_rows() != 0).unwrap();
    assert!(nonempty.column_by_name("note").unwrap().is_null(0));
    assert_eq!(rows(&reopened).await, vec![row("a", "ny", 1, 20)]);
}

#[tokio::test]
async fn test_partial_mor_rejection_preserves_timeline_and_rows() {
    for version in [8, 9] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("mor")
            .with_table_type(TableTypeValue::MergeOnRead)
            .with_table_version(version)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_ordering_fields(["ts"])
            .create()
            .await
            .unwrap();
        table
            .append([batch(&[row("a", "sf", 10, 100)])])
            .await
            .unwrap();
        let before = snapshot(&table).await;
        let names = || {
            let mut n: Vec<_> = std::fs::read_dir(dir.path().join(".hoodie/timeline"))
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            n.sort();
            n
        };
        let before_names = names();
        let error = table
            .upsert_with(
                [batch(&[row("a", "ny", 20, 999)])],
                partial(&["city", "value"]),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, hudi_core::error::CoreError::Unsupported(_)));
        assert_eq!(names(), before_names);
        assert_eq!(snapshot(&table).await, before);
        table
            .upsert([batch(&[row("a", "ny", 5, 999)])])
            .await
            .unwrap();
        assert_eq!(rows(&table).await, vec![row("a", "sf", 10, 100)]);
        table
            .upsert([batch(&[row("a", "ny", 20, 999)])])
            .await
            .unwrap();
        assert_eq!(
            rows(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await,
            vec![row("a", "ny", 20, 999)]
        );
    }
}

fn composite_batch(country: &str, city: &str, ts: i64, note: Option<&str>) -> RecordBatch {
    let base = nullable_batch(city, ts, note, true);
    let mut fields = base.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("country", DataType::Utf8, false)));
    let mut columns = base.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![country])) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
#[tokio::test]
async fn test_partial_composite_partition_and_nullable_selected_field() {
    for metadata in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("composite")
            .with_table_version(9)
            .with_record_key_fields(["id"])
            .with_partition_fields(["country", "city"])
            .with_ordering_fields(["ts"])
            .with_metadata(metadata)
            .with_record_index(metadata)
            .with_option("hoodie.datasource.write.partitionpath.urlencode", "true")
            .create()
            .await
            .unwrap();
        table
            .append([composite_batch("us", "sf", 1, Some("old"))])
            .await
            .unwrap();
        table
            .upsert_with(
                [composite_batch("ca", "ny/a%", 2, None)],
                partial(&["city", "note"]),
            )
            .await
            .unwrap();
        let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        assert_location(&reopened, dir.path(), "country=us/city=ny%2Fa%25").await;
        let batches = reopened.read(&ReadOptions::new()).await.unwrap();
        let nonempty = batches.iter().find(|b| b.num_rows() != 0).unwrap();
        assert!(nonempty.column_by_name("note").unwrap().is_null(0));
        let country = nonempty
            .column_by_name("country")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(country.value(0), "us");
        assert_eq!(rows(&reopened).await, vec![row("a", "ny/a%", 1, 20)]);
    }
}

#[tokio::test]
async fn test_partial_empty_columns_preserve_matches_and_insert_full_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = create(dir.path(), 8, false).await;
    table
        .append([batch(&[row("a", "sf", 1, 10)])])
        .await
        .unwrap();
    table
        .upsert_with(
            [batch(&[row("a", "ny", 2, 20), row("new", "ny", 3, 30)])],
            partial(&[]),
        )
        .await
        .unwrap();
    assert_eq!(
        rows(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await,
        vec![row("a", "sf", 1, 10), row("new", "ny", 3, 30)]
    );
}

// Java key generators map nullable partition values to the same default path as
// empty strings. Reproduce that stored state without allowing null native input.
async fn replace_stored_city_with_null(table: &Table, root: &std::path::Path) {
    for slice in table.get_file_slices(&ReadOptions::new()).await.unwrap() {
        let path = root.join(slice.base_file_relative_path().unwrap().unwrap());
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&path).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let batches = reader.map(Result::unwrap).collect::<Vec<_>>();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            batches[0].schema(),
            None,
        )
        .unwrap();
        for batch in batches {
            let mut columns = batch.columns().to_vec();
            let city = batch.schema().index_of("city").unwrap();
            columns[city] = Arc::new(StringArray::from(vec![None::<&str>; batch.num_rows()]));
            writer
                .write(&RecordBatch::try_new(batch.schema(), columns).unwrap())
                .unwrap();
        }
        writer.close().unwrap();
    }
}

fn nullable_city_batch(city: Option<&str>, ts: i64, value: i64) -> RecordBatch {
    let base = batch(&[row("a", "", ts, value)]);
    let mut fields = base.schema().fields().to_vec();
    fields[1] = Arc::new(Field::new("city", DataType::Utf8, true));
    let mut columns = base.columns().to_vec();
    columns[1] = Arc::new(StringArray::from(vec![city]));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn test_partial_preserves_stored_java_null_partition() {
    for metadata in [false, true] {
        for composite in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = Table::create(dir.path().to_str().unwrap())
                .with_table_name("java_null_partition")
                .with_record_key_fields(["id"])
                .with_partition_fields(if composite {
                    vec!["country", "city"]
                } else {
                    vec!["city"]
                })
                .with_ordering_fields(["ts"])
                .with_metadata(metadata)
                .with_record_index(metadata)
                .with_option("hoodie.datasource.write.partitionpath.urlencode", "true")
                .create()
                .await
                .unwrap();
            let input = |city, ts, value, country| {
                let base = nullable_city_batch(city, ts, value);
                if !composite {
                    return base;
                }
                let mut fields = base.schema().fields().to_vec();
                fields.push(Arc::new(Field::new("country", DataType::Utf8, false)));
                let mut columns = base.columns().to_vec();
                columns.push(Arc::new(StringArray::from(vec![country])) as ArrayRef);
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
            };
            let selected = if composite {
                vec!["value", "country"]
            } else {
                vec!["value"]
            };
            table.append([input(Some(""), 1, 10, "us")]).await.unwrap();
            replace_stored_city_with_null(&table, dir.path()).await;
            let mut table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let before = table.read(&ReadOptions::new()).await.unwrap();
            assert!(before[0].column_by_name("city").unwrap().is_null(0));
            table
                .upsert_with([input(Some(""), 2, 20, "ca/x%")], partial(&selected))
                .await
                .unwrap();
            let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let partition = if composite {
                "country=ca%2Fx%25/city=__HIVE_DEFAULT_PARTITION__"
            } else {
                "city=__HIVE_DEFAULT_PARTITION__"
            };
            assert_location(&reopened, dir.path(), partition).await;
            let after = reopened.read(&ReadOptions::new()).await.unwrap();
            assert_eq!(after.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            let row = after.iter().find(|b| b.num_rows() != 0).unwrap();
            assert!(row.column_by_name("city").unwrap().is_null(0));
            let values = row
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.value(0), 20);
            let names = || {
                let mut files = std::fs::read_dir(dir.path().join(".hoodie/timeline"))
                    .unwrap()
                    .map(|entry| entry.unwrap().file_name())
                    .collect::<Vec<_>>();
                files.sort();
                files
            };
            let before_names = names();
            assert!(
                table
                    .upsert_with([input(None, 3, 30, "ca")], partial(&selected))
                    .await
                    .is_err()
            );
            assert_eq!(names(), before_names);
        }
    }
}

#[tokio::test]
async fn test_partial_reingestion_rejects_metadata_then_routes_user_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut table = create(&dir.path().join("source"), 8, false).await;
    table
        .append([batch(&[row("a", "sf", 1, 10)])])
        .await
        .unwrap();
    let old = table.read(&ReadOptions::new()).await.unwrap().remove(0);
    let output = dir.path().join("output");
    let schema = serde_json::json!({"type":"record", "name":"record", "fields":[
        {"name":"id", "type":"string"}, {"name":"city", "type":"string"},
        {"name":"ts", "type":"long"}, {"name":"value", "type":"long"}
    ]});
    let mut table = Table::create(output.to_str().unwrap())
        .with_table_name("prepared")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .with_metadata(false)
        .with_option("hoodie.table.create.schema", schema.to_string())
        .create()
        .await
        .unwrap();
    let mut columns = old.columns().to_vec();
    columns[old.schema().index_of("city").unwrap()] = Arc::new(StringArray::from(vec!["ny"]));
    let input = RecordBatch::try_new(old.schema(), columns).unwrap();
    let before = std::fs::read_dir(output.join(".hoodie/timeline"))
        .unwrap()
        .count();
    let error = table
        .upsert_with([input.clone()], partial(&["city", "value"]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("remove Hudi metadata columns"));
    assert_eq!(
        std::fs::read_dir(output.join(".hoodie/timeline"))
            .unwrap()
            .count(),
        before
    );
    let indices = ["id", "city", "ts", "value"]
        .iter()
        .map(|name| input.schema().index_of(name).unwrap())
        .collect::<Vec<_>>();
    table
        .upsert_with(
            [input.project(&indices).unwrap()],
            partial(&["city", "value"]),
        )
        .await
        .unwrap();
    let reopened = Table::new(output.to_str().unwrap()).await.unwrap();
    let request = HoodieKey {
        record_key: "a".into(),
        partition_path: "city=ny".into(),
    };
    let locations = hudi_core::index::for_table(&reopened)
        .tag_location(&reopened, std::slice::from_ref(&request))
        .await
        .unwrap();
    assert_eq!(
        locations[&request].as_ref().unwrap().partition_path,
        "city=ny"
    );
    assert_eq!(rows(&reopened).await, vec![row("a", "ny", 1, 10)]);
}
