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
//! Public write inputs must not publish caller-supplied stale Hudi identity or lineage.

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::table::TableTypeValue;
use hudi_core::index::{HoodieIndex, HoodieKey};
use hudi_core::table::{QueryType, ReadOptions, Table, UpsertOptions};
use std::sync::Arc;

type Row = (String, String, i64);

fn input() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["sf"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap()
}

async fn create(path: &std::path::Path, version: isize, mor: bool, metadata: bool) -> Table {
    let schema = serde_json::json!({"type":"record", "name":"record", "fields":[
        {"name":"id", "type":"string"}, {"name":"city", "type":"string"}, {"name":"ts", "type":"long"}
    ]});
    Table::create(path.to_str().unwrap())
        .with_table_name("input_metadata")
        .with_table_type(if mor {
            TableTypeValue::MergeOnRead
        } else {
            TableTypeValue::CopyOnWrite
        })
        .with_table_version(version)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["ts"])
        .with_metadata(metadata)
        .with_record_index(metadata)
        .with_option("hoodie.table.create.schema", schema.to_string())
        .create()
        .await
        .unwrap()
}

fn strings<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}
fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                strings(batch, "id").value(i).into(),
                strings(batch, "city").value(i).into(),
                ts.value(i),
            ));
        }
    }
    rows.sort();
    rows
}
async fn write(
    table: &mut Table,
    verb: &str,
    batch: RecordBatch,
) -> hudi_core::error::Result<String> {
    match verb {
        "append" => table.append([batch]).await.map(|r| r.instant),
        "upsert" => table.upsert([batch]).await.map(|r| r.instant),
        "partial" => table
            .upsert_with(
                [batch],
                UpsertOptions {
                    update_columns: Some(vec!["city".into()]),
                },
            )
            .await
            .map(|r| r.instant),
        "overwrite" => table.overwrite([batch]).await.map(|r| r.instant),
        "dynamic" => table
            .dynamic_partition_overwrite([batch])
            .await
            .map(|r| r.instant),
        _ => panic!("unknown test verb"),
    }
}

fn files(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        path: &std::path::Path,
        found: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, found);
            } else {
                found.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(&path).unwrap(),
                );
            }
        }
    }
    let mut found = std::collections::BTreeMap::new();
    visit(root, root, &mut found);
    found
}

fn assert_metadata_error(error: &hudi_core::error::CoreError, field: &str) {
    assert!(matches!(error, hudi_core::error::CoreError::Write(_)));
    let message = error.to_string();
    assert!(message.contains(field), "{message}");
    assert!(
        message.contains("remove Hudi metadata columns"),
        "{message}"
    );
}

fn data_only(batch: &RecordBatch) -> RecordBatch {
    let meta = hudi_core::metadata::meta_field::MetaField::field_names_with_operation();
    let indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| (!meta.contains(&f.name().as_str())).then_some(i))
        .collect::<Vec<_>>();
    batch.project(&indices).unwrap()
}

#[tokio::test]
async fn test_supplied_metadata_cannot_hide_new_write_from_incremental_read() {
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = create(source_dir.path(), 8, false, false).await;
    let source_instant = source.append([input()]).await.unwrap().instant;
    let prepared = source.read(&ReadOptions::new()).await.unwrap().remove(0);
    assert_eq!(
        strings(&prepared, "_hoodie_commit_time").value(0),
        source_instant
    );
    let expected = vec![("a".into(), "sf".into(), 1)];
    let mut failures = Vec::new();
    for version in [8, 9] {
        for mor in [false, true] {
            for metadata in [false, true] {
                for verb in ["append", "upsert", "partial", "overwrite", "dynamic"] {
                    if mor && verb == "partial" {
                        continue;
                    }
                    for supplied in [false, true] {
                        let dir = tempfile::tempdir().unwrap();
                        let mut table = create(dir.path(), version, mor, metadata).await;
                        let case = format!(
                            "tv={version} mor={mor} metadata={metadata} verb={verb} supplied={supplied}"
                        );
                        if supplied {
                            let before = files(dir.path());
                            let error =
                                write(&mut table, verb, prepared.clone()).await.unwrap_err();
                            assert_metadata_error(&error, "_hoodie_commit_time");
                            assert_eq!(files(dir.path()), before, "rejection mutated {case}");
                        }
                        let instant = write(&mut table, verb, input()).await.unwrap();
                        assert!(instant > source_instant);
                        let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                        let snapshot = reopened.read(&ReadOptions::new()).await.unwrap();
                        assert_eq!(rows(&snapshot), expected, "snapshot {case}");
                        let completion = reopened
                            .timeline
                            .completed_commits
                            .iter()
                            .find(|i| i.timestamp == instant)
                            .unwrap()
                            .completion_timestamp
                            .as_ref()
                            .unwrap();
                        let options = ReadOptions::new()
                            .with_query_type(QueryType::Incremental)
                            .with_start_timestamp(&instant)
                            .with_end_timestamp(completion);
                        let incremental = reopened.read(&options).await.unwrap();
                        let actual = rows(&incremental);
                        let nonempty = snapshot.iter().find(|b| b.num_rows() != 0).unwrap();
                        let stored_time = strings(nonempty, "_hoodie_commit_time").value(0);
                        let stored_seq = strings(nonempty, "_hoodie_commit_seqno").value(0);
                        println!(
                            "{case}: accepted={instant} completion={completion} source={source_instant} stored={stored_time} seq={stored_seq} snapshot={} incremental={}",
                            rows(&snapshot).len(),
                            actual.len()
                        );
                        if actual != expected || stored_time != instant {
                            failures.push(format!(
                                "{case}: new={instant} stored={stored_time}, incremental={actual:?}"
                            ));
                        }
                        if !supplied {
                            assert_eq!(actual, expected, "ordinary input control {case}");
                        }
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "accepted writes retained stale lineage: {failures:#?}"
    );
}

#[tokio::test]
async fn test_supplied_metadata_cannot_change_configured_delete_identity() {
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = create(source_dir.path(), 8, false, false).await;
    source.append([input()]).await.unwrap();
    let prepared = source.read(&ReadOptions::new()).await.unwrap().remove(0);
    let mut failures = Vec::new();
    for mor in [false, true] {
        for metadata in [false, true] {
            for field in ["id", "city"] {
                let dir = tempfile::tempdir().unwrap();
                let mut table = create(dir.path(), 8, mor, metadata).await;
                let mut columns = prepared.columns().to_vec();
                let changed = if field == "id" { "b" } else { "ny" };
                columns[prepared.schema().index_of(field).unwrap()] =
                    Arc::new(StringArray::from(vec![changed]));
                let changed_input = RecordBatch::try_new(prepared.schema(), columns).unwrap();
                let before_files = files(dir.path());
                let error = table.append([changed_input.clone()]).await.unwrap_err();
                assert_metadata_error(&error, "_hoodie_commit_time");
                assert_eq!(files(dir.path()), before_files);
                let instant = table
                    .append([data_only(&changed_input)])
                    .await
                    .unwrap()
                    .instant;
                let id = if field == "id" { "b" } else { "a" };
                let city = if field == "city" { "ny" } else { "sf" };
                let before = table.read(&ReadOptions::new()).await.unwrap();
                assert_eq!(rows(&before), vec![(id.into(), city.into(), 1)]);
                let key = HoodieKey {
                    record_key: id.into(),
                    partition_path: format!("city={city}"),
                };
                let locations = hudi_core::index::for_table(&table)
                    .tag_location(&table, std::slice::from_ref(&key))
                    .await
                    .unwrap();
                let located = locations[&key].is_some();
                let delete = table.delete_keys([key]).await.unwrap();
                let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                let remaining = rows(&reopened.read(&ReadOptions::new()).await.unwrap());
                let case = format!("mor={mor} metadata={metadata} changed={field}");
                println!(
                    "{case}: accepted={instant} configured=({id},{city}) stored_key={} stored_partition={} located={located} deleted={} remaining={remaining:?}",
                    strings(&before[0], "_hoodie_record_key").value(0),
                    strings(&before[0], "_hoodie_partition_path").value(0),
                    delete.num_deletes
                );
                if !remaining.is_empty() {
                    failures.push(format!("{case}: configured delete left {remaining:?}"));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "accepted input has incorrect mutation identity: {failures:#?}"
    );
}

fn with_metadata_fields(batch: &RecordBatch, names: &[&str]) -> RecordBatch {
    let mut fields = batch.schema().fields().to_vec();
    let mut columns = batch.columns().to_vec();
    for name in names {
        fields.push(Arc::new(Field::new(*name, DataType::Utf8, false)));
        columns
            .push(Arc::new(StringArray::from(vec!["caller-owned"; batch.num_rows()])) as ArrayRef);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

async fn write_batches(
    table: &mut Table,
    verb: &str,
    batches: Vec<RecordBatch>,
    direct: bool,
) -> hudi_core::error::Result<()> {
    let partial = UpsertOptions {
        update_columns: Some(vec!["city".into()]),
    };
    if direct {
        match verb {
            "append" => hudi_core::write::append_batches(table, &batches)
                .await
                .map(|_| ()),
            "append_only" => hudi_core::write::append_batches_only(table, &batches)
                .await
                .map(|_| ()),
            "upsert" => hudi_core::write::upsert_batches(table, &batches, UpsertOptions::default())
                .await
                .map(|_| ()),
            "partial" => hudi_core::write::upsert_batches(table, &batches, partial)
                .await
                .map(|_| ()),
            "overwrite" => hudi_core::write::overwrite_batches(table, &batches)
                .await
                .map(|_| ()),
            "dynamic" => hudi_core::write::dynamic_partition_overwrite_batches(table, &batches)
                .await
                .map(|_| ()),
            _ => panic!("unknown test verb"),
        }
    } else {
        match verb {
            "append" => table.append(batches).await.map(|_| ()),
            "append_only" => table.append_only(batches).await.map(|_| ()),
            "upsert" => table.upsert(batches).await.map(|_| ()),
            "partial" => table.upsert_with(batches, partial).await.map(|_| ()),
            "overwrite" => table.overwrite(batches).await.map(|_| ()),
            "dynamic" => table.dynamic_partition_overwrite(batches).await.map(|_| ()),
            _ => panic!("unknown test verb"),
        }
    }
}

#[tokio::test]
async fn test_protected_fields_rejected_at_every_complete_row_boundary() {
    let protected = hudi_core::metadata::meta_field::MetaField::field_names_with_operation();
    let mut field_sets = protected.iter().map(|name| vec![*name]).collect::<Vec<_>>();
    field_sets.push(vec!["_hoodie_record_key", "_hoodie_partition_path"]);
    field_sets.push(protected);
    for mor in [false, true] {
        for populate in [false, true] {
            for existing in [false, true] {
                for direct in [false, true] {
                    for verb in [
                        "append",
                        "append_only",
                        "upsert",
                        "partial",
                        "overwrite",
                        "dynamic",
                    ] {
                        let dir = tempfile::tempdir().unwrap();
                        let mut builder = Table::create(dir.path().to_str().unwrap())
                            .with_table_name("protected_fields")
                            .with_table_type(if mor {
                                TableTypeValue::MergeOnRead
                            } else {
                                TableTypeValue::CopyOnWrite
                            })
                            .with_table_version(9)
                            .with_record_key_fields(["id"])
                            .with_partition_fields(["city"])
                            .with_populates_meta_fields(populate)
                            .with_metadata(false);
                        if verb != "append_only" {
                            builder = builder.with_ordering_fields(["ts"]);
                        }
                        let mut table = builder.create().await.unwrap();
                        if existing {
                            table.append([input()]).await.unwrap();
                        }
                        let before = files(dir.path());
                        for fields in &field_sets {
                            let invalid = with_metadata_fields(&input(), fields);
                            let error =
                                write_batches(&mut table, verb, vec![invalid.clone()], direct)
                                    .await
                                    .unwrap_err();
                            assert_metadata_error(&error, fields[0]);
                            let error =
                                write_batches(&mut table, verb, vec![input(), invalid], direct)
                                    .await
                                    .unwrap_err();
                            assert_metadata_error(&error, fields[0]);
                            assert_eq!(
                                files(dir.path()),
                                before,
                                "mor={mor} populate={populate} existing={existing} direct={direct} verb={verb} fields={fields:?}"
                            );
                        }
                        // Even paths whose normal operation is unsupported must reject metadata
                        // before fencing; an ordinary append can still use the table afterward.
                        table.append([input()]).await.unwrap();
                        let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                        let actual = rows(&reopened.read(&ReadOptions::new()).await.unwrap());
                        assert_eq!(actual.len(), if existing { 2 } else { 1 });
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn test_predicate_set_ignores_metadata_and_preserves_untouched_lineage() {
    for mor in [false, true] {
        for metadata in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = create(dir.path(), 8, mor, metadata).await;
            let mut other = input().columns().to_vec();
            other[0] = Arc::new(StringArray::from(vec!["b"]));
            let other = RecordBatch::try_new(input().schema(), other).unwrap();
            table.append([input(), other]).await.unwrap();
            let before = table.read(&ReadOptions::new()).await.unwrap();
            let old = before.iter().find(|b| b.num_rows() != 0).unwrap();
            let old_time = strings(old, "_hoodie_commit_time").value(1).to_string();
            let old_sequence = strings(old, "_hoodie_commit_seqno").value(1).to_string();
            let updates = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![2]))],
            )
            .unwrap();
            let updates =
                with_metadata_fields(&updates, &["_hoodie_commit_time", "_hoodie_record_key"]);
            let result = table.update("id = 'a'", updates).await.unwrap();
            let reopened = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let after = reopened.read(&ReadOptions::new()).await.unwrap();
            assert_eq!(
                rows(&after),
                vec![("a".into(), "sf".into(), 2), ("b".into(), "sf".into(), 1)]
            );
            for batch in after {
                for i in 0..batch.num_rows() {
                    if strings(&batch, "id").value(i) == "a" {
                        assert_eq!(
                            strings(&batch, "_hoodie_commit_time").value(i),
                            result.instant
                        );
                        assert_eq!(strings(&batch, "_hoodie_record_key").value(i), "a");
                    } else {
                        assert_eq!(strings(&batch, "_hoodie_commit_time").value(i), old_time);
                        assert_eq!(
                            strings(&batch, "_hoodie_commit_seqno").value(i),
                            old_sequence
                        );
                    }
                }
            }
        }
    }
}
