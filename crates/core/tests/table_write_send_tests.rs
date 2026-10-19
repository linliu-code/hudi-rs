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
//! Public write futures support ordinary multithreaded Tokio tasks.

use std::future::Future;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::error::{CoreError, Result};
use hudi_core::expr::filter::Filter;
use hudi_core::index::HoodieKey;
use hudi_core::table::{ReadOptions, Table, UpsertOptions};
use hudi_core::write;

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

fn set_value(value: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap()
}

async fn rows(table: &Table) -> Result<Vec<(String, String, i64, i64)>> {
    let mut result = Vec::new();
    for batch in table.read(&ReadOptions::new()).await? {
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
                strings("id").value(i).into(),
                strings("city").value(i).into(),
                ints("ts").value(i),
                ints("value").value(i),
            ));
        }
    }
    result.sort();
    Ok(result)
}

fn assert_send<F: Future + Send>(_: F) {}

#[tokio::test]
async fn test_all_public_mutation_futures_are_send() {
    let dir = tempfile::tempdir().unwrap();
    let unpolled_path = dir.path().join("unpolled");
    assert_send(
        Table::create(unpolled_path.to_str().unwrap())
            .with_table_name("unpolled")
            .create(),
    );
    assert!(!unpolled_path.exists());
    let mut table = Table::create(dir.path().join("table").to_str().unwrap())
        .with_table_name("send")
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    let input = batch(&[("a", "sf", 1, 10)]);
    let key = HoodieKey {
        record_key: "a".into(),
        partition_path: "city=sf".into(),
    };
    // Each unpolled future is dropped so the next call can borrow the table.
    assert_send(table.append([input.clone()]));
    assert_send(table.append_only(vec![input.clone()]));
    assert_send(table.upsert([input.clone()]));
    assert_send(table.upsert_with(vec![input.clone()], UpsertOptions::default()));
    assert_send(table.overwrite([input.clone()]));
    assert_send(table.dynamic_partition_overwrite(vec![input.clone()]));
    assert_send(table.delete("id = 'a'"));
    assert_send(table.delete_keys([key.clone()]));
    assert_send(table.update("id = 'a'", set_value(20)));

    let batches = [input];
    let keys = [key];
    let filter = || Filter::try_from(("id", "=", "a")).unwrap();
    assert_send(write::append_batches(&mut table, &batches));
    assert_send(write::append_batches_only(&mut table, &batches));
    assert_send(write::upsert_batches(
        &mut table,
        &batches,
        UpsertOptions::default(),
    ));
    assert_send(write::overwrite_batches(&mut table, &batches));
    assert_send(write::dynamic_partition_overwrite_batches(
        &mut table, &batches,
    ));
    assert_send(write::delete_filter(&mut table, filter()));
    assert_send(write::delete_keys(&mut table, &keys));
    assert_send(write::update_filter(&mut table, filter(), set_value(20)));
    assert!(table.get_timeline().completed_commits.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_spawned_mutations_with_metadata_and_error_retry() {
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap().to_string();
            let kind = kind.clone();
            let table = tokio::spawn(async move {
                let mut table = Table::create(&path)
                    .with_table_name("spawned")
                    .with_table_type(kind)
                    .with_table_version(version)
                    .with_record_key_fields(["id"])
                    .with_partition_fields(["city"])
                    .with_ordering_fields(["ts"])
                    .with_column_stats(true)
                    .with_partition_stats(true)
                    .create()
                    .await?;
                assert_eq!(
                    table
                        .append([batch(&[("a", "sf", 1, 10), ("b", "ny", 1, 20)])])
                        .await?
                        .num_rows,
                    2
                );
                let completed = table.get_timeline().completed_commits.clone();
                let error = table.upsert(Vec::<RecordBatch>::new()).await.unwrap_err();
                assert!(matches!(error, CoreError::Write(_)));
                assert_eq!(table.get_timeline().completed_commits, completed);
                let result = table
                    .upsert([batch(&[("a", "sf", 2, 30), ("c", "sf", 1, 40)])])
                    .await?;
                assert_eq!((result.num_updates, result.num_inserts), (1, 1));
                assert_eq!(
                    table
                        .upsert_with([batch(&[("c", "sf", 2, 41)])], UpsertOptions::default())
                        .await?
                        .num_updates,
                    1
                );
                table.update("id = 'a'", set_value(31)).await?;
                assert_eq!(
                    rows(&table).await?,
                    vec![
                        ("a".into(), "sf".into(), 2, 31),
                        ("b".into(), "ny".into(), 1, 20),
                        ("c".into(), "sf".into(), 2, 41)
                    ]
                );
                assert_eq!(table.delete("value = 20").await?.num_deletes, 1);
                assert_eq!(
                    table
                        .delete_keys([HoodieKey {
                            record_key: "c".into(),
                            partition_path: "city=sf".into()
                        }])
                        .await?
                        .num_deletes,
                    1
                );
                assert_eq!(rows(&table).await?, vec![("a".into(), "sf".into(), 2, 31)]);
                table
                    .dynamic_partition_overwrite([batch(&[("d", "sf", 3, 50)])])
                    .await?;
                assert_eq!(rows(&table).await?, vec![("d".into(), "sf".into(), 3, 50)]);
                table.overwrite([batch(&[("e", "ny", 4, 60)])]).await?;
                table.append([batch(&[("f", "la", 5, 70)])]).await?;
                for partition in ["column_stats", "partition_stats"] {
                    let logs = std::fs::read_dir(
                        std::path::Path::new(&path)
                            .join(".hoodie/metadata")
                            .join(partition),
                    )
                    .unwrap();
                    assert!(logs.into_iter().any(|entry| {
                        entry
                            .unwrap()
                            .file_name()
                            .to_str()
                            .unwrap()
                            .contains(".log.")
                    }));
                }
                Ok::<_, CoreError>(table)
            })
            .await
            .unwrap()
            .unwrap();
            let expected = vec![
                ("e".into(), "ny".into(), 4, 60),
                ("f".into(), "la".into(), 5, 70),
            ];
            assert_eq!(rows(&table).await.unwrap(), expected);
            let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            assert_eq!(rows(&reopened).await.unwrap(), expected);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_spawned_append_only_with_supported_configuration() {
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap().to_string();
            let kind = kind.clone();
            let table = tokio::spawn(async move {
                let mut table = Table::create(path)
                    .with_table_name("append_only")
                    .with_table_type(kind)
                    .with_table_version(version)
                    .with_record_key_fields(["id"])
                    .with_partition_fields(["city"])
                    .with_populates_meta_fields(false)
                    .with_metadata(false)
                    .create()
                    .await?;
                assert_eq!(
                    table
                        .append_only([batch(&[("a", "sf", 1, 10)])])
                        .await?
                        .num_rows,
                    1
                );
                Ok::<_, CoreError>(table)
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                rows(&table).await.unwrap(),
                vec![("a".into(), "sf".into(), 1, 10)]
            );
            assert_eq!(
                rows(&Table::new(dir.path().to_str().unwrap()).await.unwrap())
                    .await
                    .unwrap(),
                rows(&table).await.unwrap()
            );
        }
    }
}
