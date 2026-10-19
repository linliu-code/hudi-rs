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
//! First full-schema writes must not require a schema to index an empty table.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::{HudiTableConfig, TableTypeValue};
use hudi_core::error::CoreError;
use hudi_core::index::{HoodieIndex, HoodieKey, SimpleIndex};
use hudi_core::metadata::rollback::HoodieRollbackMetadata;
use hudi_core::table::{ReadOptions, Table};
use hudi_core::timeline::instant::Action;

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

async fn rows(table: &Table) -> Vec<(String, String, i64, i64)> {
    let mut rows = Vec::new();
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
            rows.push((
                strings("id").value(i).into(),
                strings("city").value(i).into(),
                ints("ts").value(i),
                ints("value").value(i),
            ));
        }
    }
    rows.sort();
    rows
}

async fn create(
    path: &Path,
    kind: TableTypeValue,
    version: isize,
    index: usize,
    partitioned: bool,
) -> Table {
    let mut builder = Table::create(path.to_str().unwrap())
        .with_table_name("first")
        .with_table_type(kind)
        .with_table_version(version)
        .with_record_key_fields(["id"])
        .with_ordering_fields(["ts"])
        .with_metadata(index != 0)
        .with_record_index(index == 2);
    if partitioned {
        builder = builder.with_partition_fields(["city"]);
    }
    builder.create().await.unwrap()
}

fn key(id: &str, partition: &str) -> HoodieKey {
    HoodieKey {
        record_key: id.into(),
        partition_path: partition.into(),
    }
}

fn timeline_files(path: &Path) -> Vec<String> {
    let mut files: Vec<_> = std::fs::read_dir(path.join(".hoodie/timeline"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    files.sort();
    files
}

fn assert_no_write_fences(path: &Path) {
    let files = timeline_files(path);
    // Completed writes can retain their same-instant fences until maintenance.
    // Only fences without a corresponding completion indicate a pending write.
    let completed: Vec<_> = files
        .iter()
        .filter(|name| {
            [".commit", ".deltacommit", ".replacecommit", ".rollback"]
                .iter()
                .any(|suffix| name.ends_with(suffix))
        })
        .map(|name| name.split('_').next().unwrap())
        .collect();
    for name in &files {
        if name.ends_with(".requested") || name.ends_with(".inflight") {
            assert!(
                completed.contains(&name.split('.').next().unwrap()),
                "unfinished fence: {name}"
            );
        }
    }
    let temp = path.join(".hoodie/.temp");
    if let Ok(instants) = std::fs::read_dir(temp) {
        for instant in instants {
            assert_eq!(
                std::fs::read_dir(instant.unwrap().path()).unwrap().count(),
                0
            );
        }
    }
}

#[tokio::test]
async fn test_first_full_upsert_and_indexed_continuation() {
    let mut failures = Vec::new();
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            for index in 0..3 {
                for partitioned in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path();
                    let mut table = create(path, kind.clone(), version, index, partitioned).await;
                    assert!(table.get_timeline().completed_commits.is_empty());
                    assert!(matches!(
                        table.get_schema().await,
                        Err(CoreError::SchemaNotFound(_))
                    ));
                    let context =
                        format!("{kind:?} v{version} index={index} partitioned={partitioned}");
                    let result = table.upsert([batch(&[("a", "sf", 1, 10)])]).await;
                    let result = match result {
                        Ok(result) => result,
                        Err(error) => {
                            failures.push(format!("{context}: {error:?}"));
                            continue;
                        }
                    };
                    assert_eq!(
                        (result.num_inserts, result.num_updates, result.num_writes),
                        (1, 0, 1),
                        "{context}"
                    );
                    assert_eq!(table.get_timeline().completed_commits.len(), 1);
                    assert_eq!(
                        table.get_timeline().completed_commits[0].action,
                        if kind == TableTypeValue::MergeOnRead {
                            Action::DeltaCommit
                        } else {
                            Action::Commit
                        }
                    );
                    assert_no_write_fences(path);
                    let mut table = Table::new(path.to_str().unwrap()).await.unwrap();
                    let schema = table.get_schema().await.unwrap();
                    let shape = |schema: &Schema| {
                        schema
                            .fields()
                            .iter()
                            .map(|f| (f.name().clone(), f.data_type().clone()))
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(shape(&schema), shape(&batch(&[]).schema()));
                    assert_eq!(
                        rows(&table).await,
                        vec![("a".into(), "sf".into(), 1, 10)],
                        "{context}"
                    );
                    let partition = if partitioned { "city=sf" } else { "" };
                    let a = key("a", partition);
                    let absent = key("missing", partition);
                    let locations = SimpleIndex
                        .tag_location(&table, &[a.clone(), absent.clone()])
                        .await
                        .unwrap();
                    assert_eq!(locations[&a].as_ref().unwrap().partition_path, partition);
                    assert!(locations[&absent].is_none());
                    let result = table
                        .upsert([batch(&[("a", "sf", 2, 20), ("b", "sf", 3, 30)])])
                        .await
                        .unwrap();
                    assert_eq!(
                        (result.num_inserts, result.num_updates),
                        (1, 1),
                        "{context}"
                    );
                    assert_eq!(
                        rows(&table).await,
                        vec![
                            ("a".into(), "sf".into(), 2, 20),
                            ("b".into(), "sf".into(), 3, 30)
                        ]
                    );
                    if kind == TableTypeValue::MergeOnRead {
                        assert!(
                            table
                                .get_file_slices(&ReadOptions::new())
                                .await
                                .unwrap()
                                .iter()
                                .any(|slice| !slice.log_files.is_empty())
                        );
                    }
                    assert_eq!(table.delete_keys([a]).await.unwrap().num_deletes, 1);
                    assert_eq!(rows(&table).await, vec![("b".into(), "sf".into(), 3, 30)]);
                    assert_eq!(
                        table
                            .delete_keys([key("b", partition)])
                            .await
                            .unwrap()
                            .num_deletes,
                        1
                    );
                    assert!(rows(&table).await.is_empty());
                    // An empty snapshot after committed deletes is not a schema-less table.
                    table.upsert([batch(&[("c", "sf", 4, 40)])]).await.unwrap();
                    let reopened = Table::new(path.to_str().unwrap()).await.unwrap();
                    assert_eq!(
                        rows(&reopened).await,
                        vec![("c".into(), "sf".into(), 4, 40)]
                    );
                    assert_no_write_fences(path);
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn test_simple_index_empty_lookup_retains_key_validation() {
    let mut failures = Vec::new();
    for version in [8, 9] {
        let dir = tempfile::tempdir().unwrap();
        let table = create(dir.path(), TableTypeValue::CopyOnWrite, version, 0, true).await;
        let requested = vec![
            key("a", "city=sf"),
            key("a", "city=ny"),
            key("b", "city=sf"),
        ];
        assert!(
            SimpleIndex
                .tag_location(&table, &[])
                .await
                .unwrap()
                .is_empty()
        );
        match SimpleIndex.tag_location(&table, &requested).await {
            Ok(locations) => {
                assert_eq!(locations.len(), 3);
                assert!(requested.iter().all(|k| locations[k].is_none()));
            }
            Err(error) => failures.push(format!("v{version}: {error:?}")),
        }
        for fields in [vec![], vec!["id", "city"]] {
            let dir = tempfile::tempdir().unwrap();
            let table = Table::create(dir.path().to_str().unwrap())
                .with_table_name("keys")
                .with_record_key_fields(fields)
                .with_metadata(false)
                .create()
                .await
                .unwrap();
            assert!(
                SimpleIndex
                    .tag_location(&table, &[])
                    .await
                    .unwrap()
                    .is_empty()
            );
            let error = SimpleIndex
                .tag_location(&table, &requested)
                .await
                .unwrap_err();
            assert!(matches!(error, CoreError::Unsupported(_)), "{error:?}");
            assert!(error.to_string().contains("exactly one record key"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn test_first_upsert_with_pending_or_rollback_only_history() {
    let mut failures = Vec::new();
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            for pending in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path();
                create(path, kind.clone(), version, 0, true).await;
                let timeline = path.join(".hoodie/timeline");
                let timestamp = "20200101000000000";
                let mut orphan = None;
                if pending {
                    let action = if kind == TableTypeValue::CopyOnWrite {
                        "commit"
                    } else {
                        "deltacommit"
                    };
                    std::fs::write(
                        timeline.join(format!("{timestamp}.{action}.requested")),
                        b"",
                    )
                    .unwrap();
                    let inflight = if kind == TableTypeValue::CopyOnWrite {
                        format!("{timestamp}.inflight")
                    } else {
                        format!("{timestamp}.deltacommit.inflight")
                    };
                    std::fs::write(timeline.join(inflight), b"").unwrap();
                    let name = if kind == TableTypeValue::CopyOnWrite {
                        format!("11111111-2222-3333-4444-555555555555-0_0-0-0_{timestamp}.parquet")
                    } else {
                        format!(".11111111-2222-3333-4444-555555555555-0_{timestamp}.log.1_0-0-0")
                    };
                    std::fs::create_dir_all(path.join("city=sf")).unwrap();
                    let file = path.join("city=sf").join(&name);
                    std::fs::write(&file, b"uncommitted invalid bytes").unwrap();
                    let markers = path.join(".hoodie/.temp").join(timestamp);
                    std::fs::create_dir_all(&markers).unwrap();
                    std::fs::write(markers.join("MARKERS.type"), b"TIMELINE_SERVER_BASED").unwrap();
                    std::fs::write(
                        markers.join("MARKERS0"),
                        format!("city=sf/{name}.marker.CREATE\n"),
                    )
                    .unwrap();
                    orphan = Some(file);
                } else {
                    let metadata = HoodieRollbackMetadata {
                        start_rollback_time: timestamp.into(),
                        version: Some(1),
                        ..Default::default()
                    };
                    let bytes = metadata.to_avro_bytes().unwrap();
                    assert_eq!(
                        HoodieRollbackMetadata::from_avro_bytes(&bytes)
                            .unwrap()
                            .start_rollback_time,
                        timestamp
                    );
                    std::fs::write(
                        timeline.join(format!("{timestamp}_{timestamp}.rollback")),
                        bytes,
                    )
                    .unwrap();
                }
                let mut table = Table::new(path.to_str().unwrap()).await.unwrap();
                assert!(table.get_timeline().completed_commits.is_empty());
                assert!(
                    table
                        .get_file_slices(&ReadOptions::new())
                        .await
                        .unwrap()
                        .is_empty()
                );
                let before = timeline_files(path);
                let requested = [key("a", "city=sf")];
                match SimpleIndex.tag_location(&table, &requested).await {
                    Ok(locations) => assert!(locations[&requested[0]].is_none()),
                    Err(error) => failures.push(format!(
                        "{kind:?} v{version} pending={pending} lookup: {error:?}"
                    )),
                }
                assert_eq!(
                    timeline_files(path),
                    before,
                    "lookup must not perform recovery"
                );
                if let Some(file) = &orphan {
                    assert_eq!(std::fs::read(file).unwrap(), b"uncommitted invalid bytes");
                }
                match table.upsert([batch(&[("a", "sf", 1, 10)])]).await {
                    Ok(_) => {
                        assert_eq!(
                            rows(&Table::new(path.to_str().unwrap()).await.unwrap()).await,
                            vec![("a".into(), "sf".into(), 1, 10)]
                        );
                        assert_no_write_fences(path);
                        if let Some(file) = orphan {
                            assert!(!file.exists());
                        }
                    }
                    Err(error) => failures.push(format!(
                        "{kind:?} v{version} pending={pending} upsert: {error:?}"
                    )),
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn replace_key(input: &RecordBatch, key: ArrayRef, datatype: DataType) -> RecordBatch {
    let mut fields = input.schema().fields().to_vec();
    fields[0] = Arc::new(Field::new("id", datatype, true));
    let mut columns = input.columns().to_vec();
    columns[0] = key;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn test_invalid_first_input_does_not_poison_valid_upsert() {
    let mut failures = Vec::new();
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path();
            let mut table = create(path, kind.clone(), version, 0, true).await;
            let input = batch(&[("a", "sf", 1, 10)]);
            let mut fields = input.schema().fields().to_vec();
            fields[2] = Arc::new(Field::new("ts", DataType::Utf8, false));
            let mut columns = input.columns().to_vec();
            columns[2] = Arc::new(StringArray::from(vec!["not an integer"]));
            let mismatched = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let cases = vec![
                vec![],
                vec![RecordBatch::new_empty(input.schema())],
                vec![input.project(&[1, 2, 3]).unwrap()],
                vec![input.project(&[0, 2, 3]).unwrap()],
                vec![replace_key(
                    &input,
                    Arc::new(StringArray::from(vec![None::<&str>])),
                    DataType::Utf8,
                )],
                vec![replace_key(
                    &input,
                    Arc::new(StringArray::from(vec![""])),
                    DataType::Utf8,
                )],
                vec![replace_key(
                    &input,
                    Arc::new(Int64Array::from(vec![1])),
                    DataType::Int64,
                )],
                vec![input.clone(), mismatched],
            ];
            let before = timeline_files(path);
            for (case, batches) in cases.into_iter().enumerate() {
                assert!(
                    table.upsert(batches).await.is_err(),
                    "{kind:?} v{version} invalid={case}"
                );
                assert_eq!(
                    timeline_files(path),
                    before,
                    "{kind:?} v{version} invalid={case} fenced invalid input"
                );
                assert!(table.get_timeline().completed_commits.is_empty());
                assert_no_write_fences(path);
            }
            match table.upsert([input]).await {
                Ok(_) => assert_eq!(rows(&table).await, vec![("a".into(), "sf".into(), 1, 10)]),
                Err(error) => failures.push(format!("{kind:?} v{version}: {error:?}")),
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn test_matching_create_schema_first_upsert_control() {
    let schema = serde_json::json!({"type":"record", "name":"record", "fields":[
        {"name":"id", "type":"string"}, {"name":"city", "type":"string"},
        {"name":"ts", "type":"long"}, {"name":"value", "type":"long"}
    ]});
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("declared")
            .with_table_type(kind)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .with_metadata(false)
            .with_option(HudiTableConfig::CreateSchema, schema.to_string())
            .create()
            .await
            .unwrap();
        assert!(table.get_schema().await.is_ok());
        table.upsert([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        assert_eq!(rows(&table).await, vec![("a".into(), "sf".into(), 1, 10)]);
        assert_no_write_fences(dir.path());
    }
}

#[tokio::test]
async fn test_simple_index_propagates_committed_file_errors() {
    for kind in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        let dir = tempfile::tempdir().unwrap();
        let mut table = create(dir.path(), kind, 9, 0, true).await;
        table.append([batch(&[("a", "sf", 1, 10)])]).await.unwrap();
        let slices = table.get_file_slices(&ReadOptions::new()).await.unwrap();
        let file = dir
            .path()
            .join(slices[0].base_file_relative_path().unwrap().unwrap());
        std::fs::write(file, b"corrupt committed parquet").unwrap();
        let before = timeline_files(dir.path());
        assert!(
            SimpleIndex
                .tag_location(&table, &[key("a", "city=sf")])
                .await
                .is_err()
        );
        assert_eq!(timeline_files(dir.path()), before);
    }
}
