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
//! Incidental rewrites must preserve the multiset of untouched appended rows.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
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

#[tokio::test]
async fn test_unrelated_packed_upsert_preserves_append_multiset() {
    let mut failures = Vec::new();
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            // No MDT, MDT with SimpleIndex, and MDT with RLI.
            for index in 0..3 {
                for event_time in [false, true] {
                    for partial in [false, true] {
                        if partial && table_type == TableTypeValue::MergeOnRead {
                            continue;
                        }
                        let mut variants = Vec::new();
                        for packing in [false, true] {
                            let dir = tempfile::tempdir().unwrap();
                            let path = dir.path().to_str().unwrap();
                            let mut builder = Table::create(path)
                                .with_table_name("multiset")
                                .with_table_type(table_type.clone())
                                .with_table_version(version)
                                .with_record_key_fields(["id"])
                                .with_partition_fields(["city"])
                                .with_metadata(index != 0)
                                .with_record_index(index == 2);
                            if event_time {
                                builder = builder.with_ordering_fields(["ts"]);
                            }
                            if !packing {
                                builder =
                                    builder.with_option("hoodie.parquet.small.file.limit", "0");
                            }
                            let mut table = builder.create().await.unwrap();
                            let initial = vec![
                                row("a", "sf", 1, 10),
                                row("a", "sf", 2, 20),
                                row("a", "sf", 1, 10),
                            ];
                            table.append([batch(&initial)]).await.unwrap();
                            let before = snapshot(&table).await;
                            assert_eq!(
                                rows(&table).await,
                                vec![initial[0].clone(), initial[2].clone(), initial[1].clone()]
                            );
                            let old_slice = table
                                .get_file_slices(&ReadOptions::new())
                                .await
                                .unwrap()
                                .remove(0);
                            table
                                .upsert_with(
                                    [batch(&[row("b", "sf", 3, 30)])],
                                    UpsertOptions {
                                        update_columns: partial.then(|| vec!["value".into()]),
                                    },
                                )
                                .await
                                .unwrap();
                            let slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
                            if packing {
                                assert_eq!(slices.len(), 1);
                                assert_eq!(slices[0].file_id(), old_slice.file_id());
                                if table_type == TableTypeValue::CopyOnWrite {
                                    assert_ne!(
                                        slices[0].base_file_relative_path().unwrap(),
                                        old_slice.base_file_relative_path().unwrap()
                                    );
                                } else {
                                    assert!(!slices[0].log_files.is_empty());
                                }
                            } else {
                                assert_eq!(slices.len(), 2);
                            }
                            let context = format!(
                                "{table_type:?} tv{version} index={index} event={event_time} partial={partial} packing={packing}"
                            );
                            let mut expected = initial;
                            expected.push(row("b", "sf", 3, 30));
                            expected.sort();
                            let actual = rows(&table).await;
                            if actual != expected {
                                failures.push(format!(
                                    "{context}: expected={expected:?}, actual={actual:?}"
                                ));
                            }
                            let surviving_old: Vec<_> = snapshot(&table)
                                .await
                                .into_iter()
                                .filter(|r| r.0.0 == "a")
                                .collect();
                            if surviving_old != before {
                                failures
                                    .push(format!("{context}: untouched payload/lineage changed"));
                            }
                            let reopened = Table::new(path).await.unwrap();
                            assert_eq!(rows(&reopened).await, actual, "{context}: reopen");
                            variants.push(actual);
                        }
                        if variants[0] != variants[1] {
                            failures.push(format!("{table_type:?} tv{version} index={index} event={event_time} partial={partial}: packing changes multiset"));
                        }
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn test_touched_cow_keys_keep_winner_rules_and_unmatched_duplicates() {
    for event_time in [false, true] {
        for partial in [false, true] {
            for incoming_ts in [4, 10, 11] {
                let dir = tempfile::tempdir().unwrap();
                let mut builder = Table::create(dir.path().to_str().unwrap())
                    .with_table_name("winners")
                    .with_record_key_fields(["id"])
                    .with_ordering_fields(["ts"])
                    .with_metadata(false);
                if !event_time {
                    builder =
                        builder.with_option("hoodie.record.merge.mode", "COMMIT_TIME_ORDERING");
                }
                let mut table = builder.create().await.unwrap();
                let untouched = vec![row("u", "sf", 1, 10), row("u", "sf", 2, 20)];
                // Partial updates exclude ts. Full-row event resolution also exercises
                // a maximum that is not the last existing row.
                let mut initial = if partial {
                    vec![row("a", "sf", 5, 50), row("a", "sf", 10, 100)]
                } else {
                    vec![row("a", "sf", 10, 100), row("a", "sf", 5, 50)]
                };
                initial.extend(untouched.clone());
                table.append([batch(&initial)]).await.unwrap();
                let before = snapshot(&table).await;
                table
                    .upsert_with(
                        [batch(&[row("a", "sf", incoming_ts, 999)])],
                        UpsertOptions {
                            update_columns: partial.then(|| vec!["value".into()]),
                        },
                    )
                    .await
                    .unwrap();
                let winner = if event_time && incoming_ts < 10 {
                    row("a", "sf", 10, 100)
                } else {
                    row("a", "sf", if partial { 10 } else { incoming_ts }, 999)
                };
                let mut expected = untouched;
                expected.push(winner.clone());
                expected.sort();
                assert_eq!(
                    rows(&table).await,
                    expected,
                    "event={event_time} partial={partial} ts={incoming_ts}"
                );
                let after = snapshot(&table).await;
                assert_eq!(
                    after.iter().filter(|r| r.0.0 == "u").collect::<Vec<_>>(),
                    before.iter().filter(|r| r.0.0 == "u").collect::<Vec<_>>()
                );
                if event_time && incoming_ts < 10 {
                    assert_eq!(
                        after.iter().find(|r| r.0 == winner),
                        before.iter().find(|r| r.0 == winner)
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn test_other_mutations_preserve_unrelated_duplicates() {
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("mutations")
            .with_table_type(table_type.clone())
            .with_record_key_fields(["id"])
            .with_metadata(false)
            .create()
            .await
            .unwrap();
        let duplicates = vec![row("a", "sf", 1, 10), row("a", "sf", 2, 20)];
        table.append([batch(&duplicates)]).await.unwrap();
        table
            .upsert([batch(&[row("b", "sf", 3, 30)])])
            .await
            .unwrap();
        let set = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![31]))],
        )
        .unwrap();
        table.update("value = 30", set).await.unwrap();
        let mut expected = duplicates.clone();
        expected.push(row("b", "sf", 3, 31));
        expected.sort();
        assert_eq!(rows(&table).await, expected);
        table.delete("value = 31").await.unwrap();
        assert_eq!(rows(&table).await, duplicates);
        if table_type == TableTypeValue::CopyOnWrite {
            table.delete("id = 'a'").await.unwrap();
            assert!(rows(&table).await.is_empty());
        }
    }
}

#[tokio::test]
async fn test_multigroup_packing_equivalent_across_parallelism_and_sizing() {
    let mut variants = Vec::new();
    for parallelism in ["1", "4"] {
        for packing in ["0", "104857600"] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = Table::create(dir.path().to_str().unwrap())
                .with_table_name("groups")
                .with_record_key_fields(["id"])
                .with_partition_fields(["city"])
                .with_metadata(false)
                .with_option("hoodie.parquet.small.file.limit", packing)
                .with_option("hoodie.write.task.parallelism", parallelism)
                .with_option("hoodie.copyonwrite.insert.auto.split", "false")
                .with_option("hoodie.copyonwrite.insert.split.size", "2")
                .create()
                .await
                .unwrap();
            let initial = vec![
                row("a", "sf", 1, 10),
                row("a", "sf", 2, 20),
                row("c", "ny", 1, 10),
                row("c", "ny", 2, 20),
            ];
            table.append([batch(&initial)]).await.unwrap();
            let inserted = vec![
                row("b", "sf", 3, 30),
                row("d", "ny", 3, 30),
                row("e", "la", 3, 30),
                row("f", "la", 3, 30),
                row("g", "la", 3, 30),
            ];
            table.upsert([batch(&inserted)]).await.unwrap();
            let mut expected = initial;
            expected.extend(inserted);
            expected.sort();
            let actual = rows(&table).await;
            assert_eq!(
                actual, expected,
                "parallelism={parallelism} packing={packing}"
            );
            variants.push(actual);
        }
    }
    assert!(variants.windows(2).all(|w| w[0] == w[1]));
}
