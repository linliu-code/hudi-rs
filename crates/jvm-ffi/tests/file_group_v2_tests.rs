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

//! reader_v2-backed reads of a metadata-table (MDT) `record_index` file group,
//! exercised on the committed V8 fixture (whose MDT carries record-index HFiles
//! and log files across ten shards, `hoodie.table.base.file.format=HFILE`,
//! `hoodie.record.merge.mode=CUSTOM` with `HoodieMetadataPayload`).

use arrow::array::{Array, BooleanArray, Int32Array, RecordBatchReader, StringArray, StructArray};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use hudi::ffi_support::MAX_INSTANT_TIME;
use hudi::hfile::HFileReader;
use hudi_jvm_ffi::file_group_v2::{
    FileGroupRequest, export_file_group_stream_v2, read_file_group_v2,
};
use hudi_test::QuickstartTripsTable;
use std::collections::HashSet;

fn mdt_path() -> String {
    format!(
        "{}/.hoodie/metadata",
        QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro()
    )
}

/// (hfile base names, log file names) under `record_index`, both sorted.
fn record_index_files() -> (Vec<String>, Vec<String>) {
    let dir = format!("{}/record_index", mdt_path());
    let mut hfiles = Vec::new();
    let mut logs = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("record_index dir") {
        let name = entry
            .expect("dir entry")
            .file_name()
            .to_string_lossy()
            .to_string();
        // `._…` are macOS resource forks the zip carries alongside the real files.
        if name.starts_with("._") {
            continue;
        }
        if name.ends_with(".hfile") {
            hfiles.push(name);
        } else if name.starts_with(".record-index-") && name.contains(".log.") {
            logs.push(name);
        }
    }
    hfiles.sort();
    logs.sort();
    assert!(!hfiles.is_empty(), "fixture must carry record_index HFiles");
    (hfiles, logs)
}

/// The file id of an MDT file name: `record-index-0000-0_…` → `record-index-0000-0`;
/// `.record-index-0000-0_….log.1_…` → `record-index-0000-0`.
fn file_id(name: &str) -> String {
    let trimmed = name.trim_start_matches('.');
    trimmed.split('_').next().expect("file id").to_string()
}

fn keys_of(batch: &arrow::array::RecordBatch) -> Vec<String> {
    let col = batch
        .column_by_name("key")
        .expect("MDT records carry a `key` column");
    let keys = col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("key is utf8");
    (0..keys.len()).map(|i| keys.value(i).to_string()).collect()
}

/// Every shard, not just the first: a shard of this fixture holds one record or
/// none, so a single comparison would also pass for a reader that always
/// returned one row. Across the shards the counts differ, which they cannot for
/// a reader that is not actually reading the file.
#[test]
fn base_only_slice_returns_every_entry_of_the_hfile() {
    let (hfiles, _) = record_index_files();
    let mdt = mdt_path();
    let mut per_shard_rows = Vec::new();
    let mut total_rows = 0usize;
    let mut total_entries = 0u64;

    for base in &hfiles {
        let bytes = std::fs::read(format!("{mdt}/record_index/{base}")).expect("read hfile");
        let expected = HFileReader::new(bytes).expect("parse hfile").num_entries();

        let req = FileGroupRequest {
            table_path: &mdt,
            partition_path: "record_index",
            base_file_name: base,
            log_file_names: &[],
            latest_instant: MAX_INSTANT_TIME,
            data_schema_json: "",
            lookup_keys: None,
            lookup_keys_are_prefixes: false,
            valid_instants: &[],
        };
        let batch = read_file_group_v2(&req).expect("base-only read");

        let keys = keys_of(&batch);
        println!(
            "base_only rows={} hfile_entries={} base={} first_key={:?}",
            batch.num_rows(),
            expected,
            base,
            keys.first()
        );
        assert_eq!(
            batch.num_rows() as u64,
            expected,
            "every HFile entry of {base} must come out"
        );
        assert_eq!(
            keys.iter().collect::<HashSet<_>>().len(),
            keys.len(),
            "record keys are unique"
        );
        assert!(
            batch.column_by_name("recordIndexMetadata").is_some(),
            "RLI payload column present"
        );
        per_shard_rows.push(batch.num_rows());
        total_rows += batch.num_rows();
        total_entries += expected;
    }

    println!("base_only rows={total_rows} hfile_entries={total_entries} (all shards)");
    assert_eq!(total_rows as u64, total_entries);
    assert!(
        total_entries > 0,
        "the fixture must carry record-index entries"
    );
    assert!(
        per_shard_rows.iter().collect::<HashSet<_>>().len() > 1,
        "shards must not all return the same row count, or the comparison proves \
         little; got {per_shard_rows:?}"
    );
}

#[test]
fn base_plus_logs_slice_merges_without_error() {
    let (hfiles, logs) = record_index_files();
    // Prefer a shard that actually has log files; fall back to the first shard.
    let base = hfiles
        .iter()
        .find(|h| logs.iter().any(|l| file_id(l) == file_id(h)))
        .unwrap_or(&hfiles[0])
        .clone();
    let shard_logs: Vec<&str> = logs
        .iter()
        .filter(|l| file_id(l) == file_id(&base))
        .map(String::as_str)
        .collect();

    let mdt = mdt_path();
    let req = FileGroupRequest {
        table_path: &mdt,
        partition_path: "record_index",
        base_file_name: &base,
        log_file_names: &shard_logs,
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: "",
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    };
    let batch = read_file_group_v2(&req).expect("base+logs read");
    println!(
        "base_plus_logs base={} logs={} rows={}",
        base,
        shard_logs.len(),
        batch.num_rows()
    );
    assert!(batch.column_by_name("key").is_some());
    let keys = keys_of(&batch);
    assert_eq!(
        keys.iter().collect::<HashSet<_>>().len(),
        keys.len(),
        "merge keeps one row per key"
    );
}

#[test]
fn exported_stream_round_trips_through_the_arrow_c_stream_interface() {
    let (hfiles, _) = record_index_files();
    let mdt = mdt_path();
    let req = FileGroupRequest {
        table_path: &mdt,
        partition_path: "record_index",
        base_file_name: &hfiles[0],
        log_file_names: &[],
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: "",
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    };
    let direct = read_file_group_v2(&req).expect("direct read");

    // Caller-allocated struct, exactly what Java's ArrowArrayStream.allocateNew gives us.
    let mut out = FFI_ArrowArrayStream::empty();
    unsafe { export_file_group_stream_v2(&req, &mut out as *mut _) }.expect("export");
    let mut reader = ArrowArrayStreamReader::try_new(out).expect("import");
    assert_eq!(reader.schema(), direct.schema());
    let batches: Vec<_> = reader
        .by_ref()
        .collect::<Result<_, _>>()
        .expect("stream batches");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, direct.num_rows());
}

#[test]
fn a_missing_base_file_is_an_error_not_a_panic() {
    let mdt = mdt_path();
    let req = FileGroupRequest {
        table_path: &mdt,
        partition_path: "record_index",
        base_file_name: "record-index-9999-0_0-0-0_00000000000000000.hfile",
        log_file_names: &[],
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: "",
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    };
    let err = read_file_group_v2(&req).expect_err("must fail");
    assert!(!err.is_empty());
}

/// The Avro writer schema the MDT actually used, taken from the first
/// `record_index` HFile of the fixture. This is what Java hands the file-group
/// reader (`HoodieBackedTableMetadata` passes the `HoodieMetadataRecord`
/// schema); reading it back off a sibling HFile keeps the test honest about
/// what the writer really wrote.
fn mdt_record_schema_json() -> String {
    let (hfiles, _) = record_index_files();
    let path = format!("{}/record_index/{}", mdt_path(), hfiles[0]);
    let bytes = std::fs::read(&path).expect("read hfile");
    let reader = HFileReader::new(bytes).expect("parse hfile");
    let json = reader
        .avro_schema_json()
        .expect("read the hfile's avro schema")
        .expect("the MDT hfile carries a writer schema")
        .to_string();
    println!(
        "mdt_record_schema_json len={} from={}",
        json.len(),
        hfiles[0]
    );
    json
}

/// A metadata-table file group that has only its bootstrap log file (no base
/// file yet) must read the way Java's `HoodieFileGroupReader` reads it: an
/// empty result carrying the table schema, not an error.
#[test]
fn log_only_bootstrap_slice_reads_like_java_empty_result_with_table_schema() {
    let (hfiles, logs) = record_index_files();
    let log_only: Vec<&String> = logs
        .iter()
        .filter(|l| !hfiles.iter().any(|h| file_id(h) == file_id(l)))
        .collect();
    assert!(
        !log_only.is_empty(),
        "fixture must carry a log-only record_index shard"
    );
    let shard = file_id(log_only[0]);
    let shard_logs: Vec<&str> = logs
        .iter()
        .filter(|l| file_id(l) == shard)
        .map(String::as_str)
        .collect();

    let mdt = mdt_path();
    let schema_json = mdt_record_schema_json();
    let req = FileGroupRequest {
        table_path: &mdt,
        partition_path: "record_index",
        base_file_name: "",
        log_file_names: &shard_logs,
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: &schema_json,
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    };
    let batch = read_file_group_v2(&req).unwrap_or_else(|e| {
        panic!(
            "log-only shard {shard} ({} logs) must read: {e}",
            shard_logs.len()
        )
    });
    println!(
        "log_only shard={shard} logs={} rows={} columns={:?}",
        shard_logs.len(),
        batch.num_rows(),
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        batch.num_rows(),
        0,
        "the bootstrap log holds an empty delete block: no records"
    );
    assert!(
        batch.schema().column_with_name("key").is_some(),
        "the empty result must still carry the table schema"
    );
}

/// The no-schema path is unchanged, and its limitation is the documented one:
/// a log-only slice has nothing to infer an output schema from.
#[test]
fn log_only_slice_without_a_schema_still_fails_with_a_clear_error() {
    let (hfiles, logs) = record_index_files();
    let log_only: Vec<&String> = logs
        .iter()
        .filter(|l| !hfiles.iter().any(|h| file_id(h) == file_id(l)))
        .collect();
    assert!(
        !log_only.is_empty(),
        "fixture must carry a log-only record_index shard"
    );
    let shard = file_id(log_only[0]);
    let shard_logs: Vec<&str> = logs
        .iter()
        .filter(|l| file_id(l) == shard)
        .map(String::as_str)
        .collect();

    let mdt = mdt_path();
    let req = FileGroupRequest {
        table_path: &mdt,
        partition_path: "record_index",
        base_file_name: "",
        log_file_names: &shard_logs,
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: "",
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    };
    let err = read_file_group_v2(&req).expect_err("a log-only slice has no schema to infer");
    println!("log_only_no_schema shard={shard} err={err}");
    assert!(
        err.contains("No schema available for merge output"),
        "the failure must still name the missing merge schema, got {err:?}"
    );
}

/// Passing the schema must not change what a base-only slice returns: the
/// schema handler projects, it does not filter.
#[test]
fn base_only_slice_with_explicit_schema_returns_the_same_rows_as_without() {
    let (hfiles, _) = record_index_files();
    let mdt = mdt_path();
    let base = &hfiles[0];
    let schema_json = mdt_record_schema_json();

    let without = read_file_group_v2(&FileGroupRequest {
        table_path: &mdt,
        partition_path: "record_index",
        base_file_name: base,
        log_file_names: &[],
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: "",
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    })
    .expect("base-only read without a schema");
    let with = read_file_group_v2(&FileGroupRequest {
        table_path: &mdt,
        partition_path: "record_index",
        base_file_name: base,
        log_file_names: &[],
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: &schema_json,
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    })
    .expect("base-only read with a schema");

    let columns = |b: &arrow::array::RecordBatch| {
        b.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    };
    println!(
        "base_only_parity base={base} rows_without={} rows_with={} columns_without={:?} columns_with={:?}",
        without.num_rows(),
        with.num_rows(),
        columns(&without),
        columns(&with)
    );
    assert_eq!(
        without.num_rows(),
        with.num_rows(),
        "an explicit schema must not add or drop rows"
    );
    // The MDT's own writer schema is what the base file was written with, so
    // handing it back as the requested schema must reproduce the very schema the
    // no-schema read derives from that base file — meta fields included.
    assert_eq!(
        without.schema(),
        with.schema(),
        "the MDT writer schema must project to exactly the base-only schema"
    );
    let mut keys_without = keys_of(&without);
    let mut keys_with = keys_of(&with);
    keys_without.sort();
    keys_with.sort();
    assert_eq!(
        keys_without, keys_with,
        "an explicit schema must not change which records come out"
    );
}

/// The base+logs merge keeps working with an explicit schema.
#[test]
fn base_plus_logs_slice_with_explicit_schema_merges_without_error() {
    let (hfiles, logs) = record_index_files();
    let base = hfiles
        .iter()
        .find(|h| logs.iter().any(|l| file_id(l) == file_id(h)))
        .unwrap_or(&hfiles[0])
        .clone();
    let shard_logs: Vec<&str> = logs
        .iter()
        .filter(|l| file_id(l) == file_id(&base))
        .map(String::as_str)
        .collect();

    let mdt = mdt_path();
    let schema_json = mdt_record_schema_json();
    let req = FileGroupRequest {
        table_path: &mdt,
        partition_path: "record_index",
        base_file_name: &base,
        log_file_names: &shard_logs,
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: &schema_json,
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    };
    let batch = read_file_group_v2(&req).expect("base+logs read with a schema");
    println!(
        "base_plus_logs_with_schema base={} logs={} rows={}",
        base,
        shard_logs.len(),
        batch.num_rows()
    );
    assert!(batch.column_by_name("key").is_some());
    let keys = keys_of(&batch);
    assert_eq!(
        keys.iter().collect::<HashSet<_>>().len(),
        keys.len(),
        "merge keeps one row per key"
    );
}

/// A whole-slice request for `base` with the given logs; tests override the lookup fields.
fn request<'a>(
    mdt: &'a str,
    base: &'a str,
    logs: &'a [&'a str],
    schema: &'a str,
) -> FileGroupRequest<'a> {
    FileGroupRequest {
        table_path: mdt,
        partition_path: "record_index",
        base_file_name: base,
        log_file_names: logs,
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: schema,
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    }
}

/// The hfile with the most rows, read as a base-only slice, i.e. with no log
/// files handed to the reader — whether or not the shard also has logs on
/// disk (matching `base_only_slice_returns_every_entry_of_the_hfile`, which
/// reads every hfile the same way).
fn richest_hfile() -> (String, Vec<String>) {
    let (hfiles, _) = record_index_files();
    let mdt = mdt_path();
    let mut best: Option<(String, Vec<String>)> = None;
    for base in &hfiles {
        let batch = read_file_group_v2(&request(&mdt, base, &[], "")).expect("base-only read");
        let mut keys = keys_of(&batch);
        keys.sort();
        if best.as_ref().is_none_or(|(_, k)| keys.len() > k.len()) {
            best = Some((base.clone(), keys));
        }
    }
    let (base, keys) = best.expect("a record_index hfile");
    assert!(!keys.is_empty(), "shard {base} must hold at least one key");
    (base, keys)
}

/// A key that lives in a record_index hfile OTHER than `exclude_base`, read as
/// a base-only slice like `richest_hfile`. `None` only if no other hfile
/// yields any row when read base-only.
fn a_key_in_another_hfile(exclude_base: &str) -> Option<String> {
    let (hfiles, _) = record_index_files();
    let mdt = mdt_path();
    for base in &hfiles {
        if base == exclude_base {
            continue;
        }
        let batch = read_file_group_v2(&request(&mdt, base, &[], "")).expect("base-only read");
        if let Some(key) = keys_of(&batch).into_iter().next() {
            return Some(key);
        }
    }
    None
}

#[test]
fn a_keys_predicate_returns_exactly_the_asked_for_keys_that_exist() {
    let mdt = mdt_path();
    let (base, all_keys) = richest_hfile();
    let wanted = all_keys[0].clone();
    let lookup = [wanted.as_str(), "no-such-key-zzz"];
    let mut req = request(&mdt, &base, &[], "");
    req.lookup_keys = Some(lookup.as_slice());
    let batch = read_file_group_v2(&req).expect("keys lookup");
    let keys = keys_of(&batch);
    println!("keys_predicate base={base} asked={lookup:?} got={keys:?}");
    assert_eq!(keys, vec![wanted]);

    // Negative: a key that exists nowhere must return nothing. On this
    // fixture `richest_hfile` can return a 1-key shard, so an unfiltered read
    // already equals `[wanted]` above; these two negatives are what actually
    // proves the predicate filters rather than being ignored.
    let missing = ["no-such-key-zzz"];
    let mut req_missing = request(&mdt, &base, &[], "");
    req_missing.lookup_keys = Some(missing.as_slice());
    let missing_batch = read_file_group_v2(&req_missing).expect("missing-key lookup");
    assert_eq!(
        missing_batch.num_rows(),
        0,
        "a key that exists nowhere must yield no rows"
    );

    // Negative: a key that exists, but in a DIFFERENT hfile, must not leak
    // into this shard's result.
    let other_rows = a_key_in_another_hfile(&base).map(|other_key| {
        let lookup_other = [other_key.as_str()];
        let mut req_other = request(&mdt, &base, &[], "");
        req_other.lookup_keys = Some(lookup_other.as_slice());
        let other_batch = read_file_group_v2(&req_other).expect("other-shard-key lookup");
        other_batch.num_rows()
    });
    match other_rows {
        Some(n) => {
            println!(
                "keys_predicate negative_missing_rows={} negative_other_shard_rows={n}",
                missing_batch.num_rows()
            );
            assert_eq!(
                n, 0,
                "a key from a different shard must not appear in this shard's result"
            );
        }
        None => println!(
            "keys_predicate negative_missing_rows={} negative_other_shard_rows=<no other hfile holds a key, skipped>",
            missing_batch.num_rows()
        ),
    }
}

#[test]
fn a_prefixes_predicate_returns_only_keys_with_the_prefix() {
    let mdt = mdt_path();
    let (base, all_keys) = richest_hfile();
    let first = &all_keys[0];
    let prefix: String = first
        .chars()
        .take(std::cmp::max(1, first.chars().count() / 2))
        .collect();
    let expected: Vec<String> = all_keys
        .iter()
        .filter(|k| k.starts_with(&prefix))
        .cloned()
        .collect();
    let lookup = [prefix.as_str(), "zzz-no-such-prefix"];
    let mut req = request(&mdt, &base, &[], "");
    req.lookup_keys = Some(lookup.as_slice());
    req.lookup_keys_are_prefixes = true;
    let batch = read_file_group_v2(&req).expect("prefix lookup");
    let mut keys = keys_of(&batch);
    keys.sort();
    println!(
        "prefix_predicate base={base} prefix={prefix:?} expected={} got={}",
        expected.len(),
        keys.len()
    );
    assert_eq!(keys, expected);
    assert!(
        keys.len() < all_keys.len() || all_keys.len() == 1,
        "the prefix must actually narrow the read"
    );

    // Negative: an unmatched prefix, alone, must return nothing.
    let missing_prefix = ["zzz-no-such-prefix"];
    let mut req_missing = request(&mdt, &base, &[], "");
    req_missing.lookup_keys = Some(missing_prefix.as_slice());
    req_missing.lookup_keys_are_prefixes = true;
    let missing_batch = read_file_group_v2(&req_missing).expect("missing-prefix lookup");
    assert_eq!(
        missing_batch.num_rows(),
        0,
        "an unmatched prefix must yield no rows"
    );

    // Negative: the prefix of a DIFFERENT shard's key must not accidentally
    // also prefix this shard's key (checked up front, or the negative below
    // would be meaningless), and must not match this shard's row.
    let other_rows = a_key_in_another_hfile(&base).map(|other_key| {
        let other_prefix: String = other_key
            .chars()
            .take(std::cmp::max(1, other_key.chars().count() / 2))
            .collect();
        assert!(
            !first.starts_with(&other_prefix),
            "the other shard's prefix {other_prefix:?} must not also prefix this shard's key              {first:?}, or this negative check proves nothing"
        );
        let lookup_other = [other_prefix.as_str()];
        let mut req_other = request(&mdt, &base, &[], "");
        req_other.lookup_keys = Some(lookup_other.as_slice());
        req_other.lookup_keys_are_prefixes = true;
        let other_batch = read_file_group_v2(&req_other).expect("other-shard-prefix lookup");
        other_batch.num_rows()
    });
    match other_rows {
        Some(n) => {
            println!(
                "prefix_predicate negative_missing_rows={} negative_other_shard_rows={n}",
                missing_batch.num_rows()
            );
            assert_eq!(
                n, 0,
                "a different shard's key prefix must not match this shard's key"
            );
        }
        None => println!(
            "prefix_predicate negative_missing_rows={} negative_other_shard_rows=<no other hfile holds a key, skipped>",
            missing_batch.num_rows()
        ),
    }
}

#[test]
fn absent_lookup_keys_and_empty_valid_instants_read_the_whole_slice() {
    let mdt = mdt_path();
    let (base, all_keys) = richest_hfile();
    let req = request(&mdt, &base, &[], "");
    let batch = read_file_group_v2(&req).expect("whole slice");
    let mut keys = keys_of(&batch);
    keys.sort();
    assert_eq!(keys, all_keys);
    let bytes = std::fs::read(format!("{mdt}/record_index/{base}")).expect("read hfile");
    let expected = HFileReader::new(bytes).expect("parse hfile").num_entries();
    assert_eq!(
        batch.num_rows() as u64,
        expected,
        "an ABSENT predicate (None) and an empty instant set must read every entry of the HFile"
    );
}

/// A shard that has a base file AND log files: the log's records must come out
/// merged, and a keys predicate must still find a key that lives in the log.
fn base_plus_logs_shard() -> (String, Vec<String>) {
    let (hfiles, logs) = record_index_files();
    let base = hfiles
        .iter()
        .find(|h| logs.iter().any(|l| file_id(l) == file_id(h)))
        .expect("fixture must carry a record_index shard with base + logs")
        .clone();
    let shard_logs = logs
        .iter()
        .filter(|l| file_id(l) == file_id(&base))
        .cloned()
        .collect();
    (base, shard_logs)
}

#[test]
fn a_keys_predicate_on_base_plus_logs_returns_the_merged_row() {
    let mdt = mdt_path();
    let schema = mdt_record_schema_json();
    let (base, shard_logs) = base_plus_logs_shard();
    let logs: Vec<&str> = shard_logs.iter().map(String::as_str).collect();
    let merged = read_file_group_v2(&request(&mdt, &base, &logs, &schema)).expect("merged read");
    let merged_keys = keys_of(&merged);
    assert!(!merged_keys.is_empty(), "shard {base} must have rows");
    let base_only = read_file_group_v2(&request(&mdt, &base, &[], "")).expect("base read");
    let base_keys: HashSet<String> = keys_of(&base_only).into_iter().collect();
    // Prefer a key the logs add or change; fall back to any merged key.
    let target = merged_keys
        .iter()
        .find(|k| !base_keys.contains(*k))
        .unwrap_or(&merged_keys[0])
        .clone();
    println!(
        "base_plus_logs base={base} logs={} merged_rows={} target={target} in_base={}",
        logs.len(),
        merged.num_rows(),
        base_keys.contains(&target)
    );
    let lookup = [target.as_str()];
    let mut req = request(&mdt, &base, &logs, &schema);
    req.lookup_keys = Some(lookup.as_slice());
    let batch = read_file_group_v2(&req).expect("keys lookup on base+logs");
    assert_eq!(keys_of(&batch), vec![target]);

    // Negative: on this same base+logs shard, a key that exists nowhere must
    // return nothing, while the unfiltered merged read (asserted above to be
    // non-empty) shows the shard genuinely has rows to filter away.
    assert!(
        merged.num_rows() >= 1,
        "the unfiltered merged read must have rows for the negative check below to be meaningful"
    );
    let missing = ["no-such-key-zzz"];
    let mut req_missing = request(&mdt, &base, &logs, &schema);
    req_missing.lookup_keys = Some(missing.as_slice());
    let missing_batch = read_file_group_v2(&req_missing).expect("missing-key lookup on base+logs");
    println!("base_plus_logs negative_rows={}", missing_batch.num_rows());
    assert_eq!(
        missing_batch.num_rows(),
        0,
        "a key that exists nowhere must yield no rows on a base+logs slice"
    );
}

/// The completed instants of the fixture MDT's own timeline (`<mdt>/.hoodie/<instant>.deltacommit`,
/// also under `.hoodie/timeline/` on the newer layout). This is the set Java's
/// `getValidInstantTimestamps` would hand the reader for a fully committed table: log BLOCKS carry
/// these instants in their headers (a log FILE's name carries the slice's base instant, which is
/// not what the range filter tests).
fn mdt_completed_instants() -> Vec<String> {
    let mdt = mdt_path();
    let mut out = Vec::new();
    for dir in [format!("{mdt}/.hoodie"), format!("{mdt}/.hoodie/timeline")] {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let name = entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .to_string();
            let Some((stem, ext)) = name.rsplit_once('.') else {
                continue;
            };
            if !matches!(ext, "deltacommit" | "commit" | "compaction") {
                continue;
            }
            // `<instant>` or `<instant>_<completion>`; keep the leading digits.
            let instant: String = stem.chars().take_while(|c| c.is_ascii_digit()).collect();
            if !instant.is_empty() {
                out.push(instant);
            }
        }
    }
    out.sort();
    out.dedup();
    assert!(!out.is_empty(), "fixture MDT must have completed instants");
    out
}

#[test]
fn valid_instants_that_exclude_a_log_instant_drop_that_logs_rows() {
    let mdt = mdt_path();
    let schema = mdt_record_schema_json();
    let (base, shard_logs) = base_plus_logs_shard();
    let logs: Vec<&str> = shard_logs.iter().map(String::as_str).collect();
    let instants = mdt_completed_instants();
    println!("valid_instants base={base} completed_instants={instants:?}");

    let all = read_file_group_v2(&request(&mdt, &base, &logs, &schema)).expect("no-range read");
    let full: Vec<&str> = instants.iter().map(String::as_str).collect();
    let mut req_full = request(&mdt, &base, &logs, &schema);
    req_full.valid_instants = &full;
    let with_full = read_file_group_v2(&req_full).expect("full-set read");
    assert_eq!(
        keys_of(&with_full),
        keys_of(&all),
        "the full completed-instant set must keep every row"
    );

    let none: Vec<&str> = vec!["00000000000000000"];
    let mut req_none = request(&mdt, &base, &logs, &schema);
    req_none.valid_instants = &none;
    let without = read_file_group_v2(&req_none).expect("excluded read");
    let base_only = read_file_group_v2(&request(&mdt, &base, &[], "")).expect("base read");
    let mut got = keys_of(&without);
    got.sort();
    let mut expect = keys_of(&base_only);
    expect.sort();
    println!(
        "valid_instants all={} full_set={} excluded={} base_only={}",
        all.num_rows(),
        with_full.num_rows(),
        without.num_rows(),
        base_only.num_rows()
    );
    assert_ne!(
        all, base_only,
        "the shard's logs must change the base rows, or the exclusion check below proves nothing"
    );
    assert_eq!(
        got, expect,
        "with every log block's instant excluded only the base file's rows remain"
    );
    assert_eq!(
        without, base_only,
        "excluding every log instant must reproduce the base-only batch exactly"
    );
    assert!(
        all.num_rows() >= base_only.num_rows(),
        "sanity: merged never has fewer rows than the base alone"
    );
}

/// Minor: a key predicate on a LOG-ONLY slice (no base file — the same
/// bootstrap shard `log_only_bootstrap_slice_reads_like_java_empty_result_with_table_schema`
/// uses) must not break the log path. That shard's bootstrap log holds an
/// empty delete block, so the unfiltered read is already 0 rows; this proves
/// a predicate on a log-only slice still reads cleanly rather than erroring.
#[test]
fn a_keys_predicate_on_a_log_only_slice_reads_without_error() {
    let (hfiles, logs) = record_index_files();
    let log_only: Vec<&String> = logs
        .iter()
        .filter(|l| !hfiles.iter().any(|h| file_id(h) == file_id(l)))
        .collect();
    assert!(
        !log_only.is_empty(),
        "fixture must carry a log-only record_index shard"
    );
    let shard = file_id(log_only[0]);
    let shard_logs: Vec<&str> = logs
        .iter()
        .filter(|l| file_id(l) == shard)
        .map(String::as_str)
        .collect();

    let mdt = mdt_path();
    let schema_json = mdt_record_schema_json();
    let lookup = ["no-such-key-zzz"];
    let mut req = request(&mdt, "", &shard_logs, &schema_json);
    req.lookup_keys = Some(lookup.as_slice());
    let batch = read_file_group_v2(&req)
        .unwrap_or_else(|e| panic!("log-only shard {shard} with a key predicate must read: {e}"));
    println!("log_only_predicate shard={shard} rows={}", batch.num_rows());
    assert_eq!(
        batch.num_rows(),
        0,
        "a predicate on an already-empty log-only slice must still yield zero rows"
    );
}

/// Minor: `Some(&[])` on a LOG-ONLY slice (no base file to even attempt a
/// key-based seek against) must still take the match-nothing path (D-12)
/// rather than falling back to a full scan the way an absent predicate would.
#[test]
fn an_empty_keys_predicate_on_a_log_only_slice_reads_zero_rows() {
    let (hfiles, logs) = record_index_files();
    let log_only: Vec<&String> = logs
        .iter()
        .filter(|l| !hfiles.iter().any(|h| file_id(h) == file_id(l)))
        .collect();
    assert!(
        !log_only.is_empty(),
        "fixture must carry a log-only record_index shard"
    );
    let shard = file_id(log_only[0]);
    let shard_logs: Vec<&str> = logs
        .iter()
        .filter(|l| file_id(l) == shard)
        .map(String::as_str)
        .collect();

    let mdt = mdt_path();
    let schema_json = mdt_record_schema_json();
    let empty: [&str; 0] = [];
    let mut req = request(&mdt, "", &shard_logs, &schema_json);
    req.lookup_keys = Some(empty.as_slice());
    let batch = read_file_group_v2(&req)
        .unwrap_or_else(|e| panic!("log-only shard {shard} with an empty key set must read: {e}"));
    println!(
        "log_only_empty_keys shard={shard} rows={}",
        batch.num_rows()
    );
    assert_eq!(
        batch.num_rows(),
        0,
        "an empty key set on a log-only slice must yield zero rows"
    );
}

/// Java never has an "empty" latest instant: `readSliceWithFilter` passes the last
/// completed instant or SOLO_COMMIT_TIMESTAMP. The old native contract mapped "" to
/// MAX_INSTANT_TIME (read everything) — the opposite direction from Java's fallback
/// (read nothing). OI-16 / D-13: "" is refused before any I/O.
#[test]
fn an_empty_latest_instant_is_refused() {
    let mdt = mdt_path();
    let (base, _) = richest_hfile();
    let mut req = request(&mdt, &base, &[], "");
    req.latest_instant = "";
    let err = read_file_group_v2(&req).expect_err("an empty latest_instant must be refused");
    assert!(
        err.contains("latest_instant is empty"),
        "the error must name the argument, got: {err}"
    );
}

/// Java returns an EmptyIterator for zero keys before building any reader
/// (`readSliceAndFilterByKeysIntoList`: "If no keys to lookup, we must return early,
/// otherwise, the hfile lookup will return all records."). OI-14 / D-12: an EMPTY key
/// set (as opposed to an ABSENT one) reads nothing, on a base-only and a base+logs slice,
/// for exact keys and for prefixes.
#[test]
fn an_empty_key_set_reads_nothing() {
    let mdt = mdt_path();
    let (base, all_keys) = richest_hfile();
    assert!(!all_keys.is_empty(), "the control slice must hold rows");
    let empty: [&str; 0] = [];
    for prefixes in [false, true] {
        let mut req = request(&mdt, &base, &[], "");
        req.lookup_keys = Some(empty.as_slice());
        req.lookup_keys_are_prefixes = prefixes;
        let batch = read_file_group_v2(&req).expect("empty key set");
        assert_eq!(
            batch.num_rows(),
            0,
            "an empty key set (prefixes={prefixes}) must read nothing, got {:?}",
            keys_of(&batch)
        );
        let whole = read_file_group_v2(&request(&mdt, &base, &[], "")).expect("whole slice");
        assert_eq!(
            batch.schema(),
            whole.schema(),
            "the empty batch keeps the slice schema"
        );
    }
    let schema = mdt_record_schema_json();
    let (base, shard_logs) = base_plus_logs_shard();
    let logs: Vec<&str> = shard_logs.iter().map(String::as_str).collect();
    let mut req = request(&mdt, &base, &logs, &schema);
    req.lookup_keys = Some(empty.as_slice());
    let batch = read_file_group_v2(&req).expect("empty key set on base+logs");
    assert_eq!(batch.num_rows(), 0, "base+logs: got {:?}", keys_of(&batch));
}

/// `FileGroupRequest::default()` is all-empty and is refused on its first check, so
/// struct-update syntax can never produce a silently-wider read (OI-25 rs-c).
#[test]
fn default_request_is_all_empty_and_refused_on_table_path() {
    let req = FileGroupRequest::default();
    assert!(req.lookup_keys.is_none());
    assert!(req.log_file_names.is_empty() && req.valid_instants.is_empty());
    let err = read_file_group_v2(&req).expect_err("a default request has no table");
    assert!(err.contains("table_path is empty"), "got: {err}");
}

/// The DATA table (not its metadata table) of the same fixture: MOR with Avro log
/// blocks. `city=chennai` holds file id `6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0` with a
/// base parquet at instant 20251220210108078 and two later `.log.1_…` files, i.e. a
/// slice whose log blocks are AVRO_DATA_BLOCKs. With a key predicate the native read
/// must fail like Java's HoodieDataBlock.lookupEngineRecords does (OI-12 / D-14); the
/// same slice without a predicate reads fine (control).
#[test]
fn a_key_predicate_on_an_avro_data_block_is_refused_like_java() {
    let table = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
    let partition = "city=chennai";
    let dir = format!("{table}/{partition}");
    let mut base: Option<String> = None;
    let mut logs: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("partition dir") {
        let name = entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .to_string();
        if name.starts_with("._") {
            continue;
        }
        if name.ends_with(".parquet") && name.contains("_20251220210108078") {
            base = Some(name);
        } else if name.starts_with(".6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_")
            && name.contains(".log.")
        {
            logs.push(name);
        }
    }
    let base = base.expect("the fixture's first chennai base file");
    assert!(
        !logs.is_empty(),
        "the fixture's chennai slice must carry Avro log files"
    );
    logs.sort();
    let log_refs: Vec<&str> = logs.iter().map(String::as_str).collect();
    let control = FileGroupRequest {
        table_path: &table,
        partition_path: partition,
        base_file_name: &base,
        log_file_names: &log_refs,
        latest_instant: MAX_INSTANT_TIME,
        ..FileGroupRequest::default()
    };
    let batch =
        read_file_group_v2(&control).expect("the data-table slice reads without a predicate");
    assert!(batch.num_rows() > 0, "control read must return rows");

    let lookup = ["no-such-key"];
    let mut req = control;
    req.lookup_keys = Some(lookup.as_slice());
    let err =
        read_file_group_v2(&req).expect_err("a key predicate over Avro log blocks must be refused");
    assert!(
        err.contains("point lookups are not supported") && err.contains("AvroData"),
        "got: {err}"
    );
}

// ---- secondary_index ----------------------------------------------------
//
// The same fixture's `secondary_index_rider_idx` partition: keys are
// `<escapedSecKey>$<escapedRecKey>` (Java `SecondaryIndexKeyUtils`), Java
// always looks them up by PREFIX (`<escapedSecKey>$`), and the table's
// updates/delete wrote SI tombstones as delete-block keys. These tests prove the
// native reader serves that shape like Java's file group reader (spec §0/§1).

const SI_PARTITION: &str = "secondary_index_rider_idx";
/// Java `MetadataPartitionType.SECONDARY_INDEX.getRecordType()`.
const SI_RECORD_TYPE: i32 = 7;

/// (hfile base names, log file names) under the SI partition, both sorted.
fn secondary_index_files() -> (Vec<String>, Vec<String>) {
    let dir = format!("{}/{}", mdt_path(), SI_PARTITION);
    let mut hfiles = Vec::new();
    let mut logs = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("secondary_index dir") {
        let name = entry
            .expect("dir entry")
            .file_name()
            .to_string_lossy()
            .to_string();
        if name.starts_with("._") {
            continue;
        }
        if name.ends_with(".hfile") {
            hfiles.push(name);
        } else if name.starts_with(".secondary-index-") && name.contains(".log.") {
            logs.push(name);
        }
    }
    hfiles.sort();
    logs.sort();
    assert!(
        !hfiles.is_empty(),
        "fixture must carry secondary_index HFiles"
    );
    (hfiles, logs)
}

fn si_request<'a>(
    mdt: &'a str,
    base: &'a str,
    logs: &'a [&'a str],
    schema: &'a str,
) -> FileGroupRequest<'a> {
    FileGroupRequest {
        table_path: mdt,
        partition_path: SI_PARTITION,
        base_file_name: base,
        log_file_names: logs,
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: schema,
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    }
}

/// `<escapedSecKey>$` of an SI key: everything up to and including the first
/// `$` that is not escaped by a backslash (Java `getEscapedSecondaryKeyPrefixFromSecondaryKey`).
fn si_prefix_of(key: &str) -> String {
    let mut out = String::new();
    let mut escaped = false;
    for c in key.chars() {
        out.push(c);
        if escaped {
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == '$' {
            return out;
        }
    }
    panic!("SI key {key:?} has no unescaped `$` separator");
}

/// Every row of an SI read must be an SI record that is not a tombstone.
fn assert_si_rows(batch: &arrow::array::RecordBatch, where_: &str) {
    let types = batch
        .column_by_name("type")
        .expect("`type` column")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("type is int32");
    for i in 0..types.len() {
        assert_eq!(
            types.value(i),
            SI_RECORD_TYPE,
            "{where_}: row {i} is not an SI record"
        );
    }
    let si = batch
        .column_by_name("SecondaryIndexMetadata")
        .expect("`SecondaryIndexMetadata` column")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("SecondaryIndexMetadata is a struct");
    let deleted = si
        .column_by_name("isDeleted")
        .expect("isDeleted field")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("isDeleted is boolean");
    for i in 0..deleted.len() {
        assert!(
            si.is_null(i) || deleted.is_null(i) || !deleted.value(i),
            "{where_}: row {i} is a tombstone data record (Java writes SI tombstones as delete-block keys, never as data rows)"
        );
    }
}

/// The SI hfile with the most rows, read base-only, plus its sorted keys (the
/// independent oracle: an unfiltered read of the same file).
fn richest_si_hfile() -> (String, Vec<String>) {
    let (hfiles, _) = secondary_index_files();
    let mdt = mdt_path();
    let mut best: Option<(String, Vec<String>)> = None;
    for base in &hfiles {
        let batch =
            read_file_group_v2(&si_request(&mdt, base, &[], "")).expect("SI base-only read");
        let mut keys = keys_of(&batch);
        keys.sort();
        if best.as_ref().is_none_or(|(_, k)| keys.len() > k.len()) {
            best = Some((base.clone(), keys));
        }
    }
    let (base, keys) = best.expect("an SI hfile");
    assert!(
        !keys.is_empty(),
        "SI shard {base} must hold at least one key"
    );
    (base, keys)
}

#[test]
fn si_base_only_prefix_lookup_returns_exactly_the_keys_with_that_prefix() {
    let mdt = mdt_path();
    let (base, all_keys) = richest_si_hfile();
    let prefix = si_prefix_of(&all_keys[0]);
    let expected: Vec<String> = all_keys
        .iter()
        .filter(|k| k.starts_with(&prefix))
        .cloned()
        .collect();
    let lookup = [prefix.as_str()];
    let mut req = si_request(&mdt, &base, &[], "");
    req.lookup_keys = Some(lookup.as_slice());
    req.lookup_keys_are_prefixes = true;
    let batch = read_file_group_v2(&req).expect("SI prefix lookup");
    assert_si_rows(&batch, "si_base_only_prefix");
    let mut keys = keys_of(&batch);
    keys.sort();
    println!(
        "si_base_only_prefix base={base} prefix={prefix:?} expected={} got={} all={}",
        expected.len(),
        keys.len(),
        all_keys.len()
    );
    assert_eq!(keys, expected);
    for k in &keys {
        assert!(
            k.starts_with(&prefix),
            "returned key {k:?} lacks the prefix {prefix:?}"
        );
    }
    // Control: the unfiltered read of the same file is the oracle and is non-empty.
    assert!(!all_keys.is_empty());
}

/// One prefix's read, sorted: the independent per-prefix oracle a multi-prefix
/// read is checked against.
fn si_single_prefix_read(mdt: &str, base: &str, prefix: &str) -> Vec<String> {
    let lookup = [prefix];
    let mut req = si_request(mdt, base, &[], "");
    req.lookup_keys = Some(lookup.as_slice());
    req.lookup_keys_are_prefixes = true;
    let mut keys = keys_of(&read_file_group_v2(&req).expect("single-prefix read"));
    keys.sort();
    keys
}

#[test]
fn si_multi_prefix_lookup_is_the_union_and_a_stranger_prefix_adds_nothing() {
    let mdt = mdt_path();
    let (base, all_keys) = richest_si_hfile();
    let mut prefixes: Vec<String> = all_keys.iter().map(|k| si_prefix_of(k)).collect();
    prefixes.sort();
    prefixes.dedup();
    // Two distinct prefixes when the shard has them, else the one it has.
    let chosen: Vec<String> = prefixes.iter().take(2).cloned().collect();

    // The reader property under test: reading several prefixes at once must
    // equal the UNION of reading each of them alone -- not merely "equal to
    // the filtered oracle", which a reader that ignores the predicate and
    // returns every row would also satisfy whenever `chosen` happens to cover
    // every key (as it does on this fixture's 2-key richest shard).
    let single_reads: Vec<Vec<String>> = chosen
        .iter()
        .map(|p| si_single_prefix_read(&mdt, &base, p))
        .collect();
    let mut expected: Vec<String> = single_reads.iter().flatten().cloned().collect();
    expected.sort();
    expected.dedup();

    let mut lookup: Vec<&str> = chosen.iter().map(String::as_str).collect();
    lookup.push("zzz-no-such-secondary-key$");
    let mut req = si_request(&mdt, &base, &[], "");
    req.lookup_keys = Some(lookup.as_slice());
    req.lookup_keys_are_prefixes = true;
    let batch = read_file_group_v2(&req).expect("SI multi-prefix lookup");
    assert_si_rows(&batch, "si_multi_prefix");
    let mut keys = keys_of(&batch);
    keys.sort();
    println!(
        "si_multi_prefix base={base} prefixes={chosen:?} distinct_in_shard={} single_reads={:?} union={} got={}",
        prefixes.len(),
        single_reads.iter().map(Vec::len).collect::<Vec<_>>(),
        expected.len(),
        keys.len()
    );
    assert_eq!(
        keys, expected,
        "the multi-prefix read must equal the union of the single-prefix reads"
    );

    // A reader that ignores the predicate (and just returns every row) would
    // make each single-prefix read equal the multi-prefix read; with more
    // than one key in the shard, the real reader's per-prefix reads must each
    // be a STRICT subset of the union, or this test cannot tell the two apart.
    if all_keys.len() > 1 {
        for (p, single) in chosen.iter().zip(single_reads.iter()) {
            assert!(
                single.len() < keys.len(),
                "single-prefix read for {p:?} ({} keys) must be a strict subset of the \
                 multi-prefix read ({} keys); a reader that ignores the prefix predicate \
                 would also pass otherwise",
                single.len(),
                keys.len()
            );
            for k in single {
                assert!(
                    keys.contains(k),
                    "multi-prefix read must contain every key of the single-prefix read \
                     for {p:?}: missing {k:?}"
                );
            }
        }
    }

    let stranger = ["zzz-no-such-secondary-key$"];
    let mut req_none = si_request(&mdt, &base, &[], "");
    req_none.lookup_keys = Some(stranger.as_slice());
    req_none.lookup_keys_are_prefixes = true;
    assert_eq!(
        read_file_group_v2(&req_none)
            .expect("stranger prefix")
            .num_rows(),
        0
    );
}

/// A base-less SI shard (per `m1-t1-si-fixture-shape.txt`, shards
/// 0001/0003/0008/0009 never received a base HFile) is exactly what Java's V1
/// SI path hands the reader for every slice of the partition
/// (`parallelize(fileSlices)` scans them all): a log-only slice must read
/// without error under a prefix predicate, fail-loud meaning one erroring
/// slice would fail the whole lookup.
#[test]
fn si_log_only_shard_prefix_lookup_reads_without_error() {
    let mdt = mdt_path();
    let schema = mdt_record_schema_json();
    let (hfiles, logs) = secondary_index_files();
    let log_only: Vec<&String> = logs
        .iter()
        .filter(|l| !hfiles.iter().any(|h| file_id(h) == file_id(l)))
        .collect();
    assert!(
        !log_only.is_empty(),
        "fixture must carry a log-only secondary_index shard"
    );
    let shard = file_id(log_only[0]);
    let shard_logs: Vec<&str> = logs
        .iter()
        .filter(|l| file_id(l) == shard)
        .map(String::as_str)
        .collect();

    let (_, all_keys) = richest_si_hfile();
    let prefix = si_prefix_of(&all_keys[0]);
    let lookup = [prefix.as_str()];
    let mut req = si_request(&mdt, "", &shard_logs, &schema);
    req.lookup_keys = Some(lookup.as_slice());
    req.lookup_keys_are_prefixes = true;
    let batch = read_file_group_v2(&req).unwrap_or_else(|e| {
        panic!("log-only SI shard {shard} with a prefix predicate must read: {e}")
    });
    println!(
        "si_log_only shard={shard} logs={shard_logs:?} prefix={prefix:?} rows={}",
        batch.num_rows()
    );
    if batch.num_rows() > 0 {
        assert_si_rows(&batch, "si_log_only");
        for k in keys_of(&batch) {
            assert!(
                k.starts_with(&prefix),
                "returned key {k:?} lacks the prefix {prefix:?}"
            );
        }
    }
}

/// An SI shard whose logs change the base rows: (base, its logs, base-only keys,
/// merged keys with no instant range). Panics if no shard has logs.
///
/// `hfiles` can carry more than one base-file GENERATION per shard here (e.g.
/// `secondary-index-rider-idx-0000-0` has one from the table's initial bulk
/// insert at instant `00000000000000004` and one from a later compaction at
/// `20251220210130942`), and `file_id()` groups both generations of a shard
/// under the logs that belong to it. Iterating `hfiles` in sorted order (as
/// below) tries the OLDER generation of a shard before the newer one, and
/// that is deliberate, not an oversight: the newer generation is already the
/// compacted result of folding those same logs into the older base, so
/// reading it plus the very logs it was compacted from is a no-op (verified:
/// for shard 0000, `base_keys`/`merged` are `[]`/`[]` against the newer
/// generation vs. `["rider-J$…"]`/`[]` against the older one) and would make
/// this function loop past every shard without ever finding one whose logs
/// change anything. Pairing the OLDER generation with the shard's logs is
/// what actually exercises "log block changes a row `read_file_group_v2` got
/// from the base" -- the property this test exists to prove.
fn si_shard_whose_logs_matter() -> (String, Vec<String>, Vec<String>, Vec<String>) {
    let (hfiles, logs) = secondary_index_files();
    let mdt = mdt_path();
    let schema = mdt_record_schema_json();
    for base in &hfiles {
        let shard_logs: Vec<String> = logs
            .iter()
            .filter(|l| file_id(l) == file_id(base))
            .cloned()
            .collect();
        if shard_logs.is_empty() {
            continue;
        }
        let logs_ref: Vec<&str> = shard_logs.iter().map(String::as_str).collect();
        let mut base_keys =
            keys_of(&read_file_group_v2(&si_request(&mdt, base, &[], "")).expect("base"));
        base_keys.sort();
        let mut merged = keys_of(
            &read_file_group_v2(&si_request(&mdt, base, &logs_ref, &schema)).expect("merged"),
        );
        merged.sort();
        if merged != base_keys {
            return (base.clone(), shard_logs, base_keys, merged);
        }
    }
    panic!(
        "fixture must carry an SI shard whose logs change its rows (the table's 3 updates + 1 delete)"
    );
}

/// Java applies EVERY delete-block key whatever the key spec
/// (`KeyBasedFileGroupRecordBuffer.processDeleteBlock`), so an SI tombstone
/// written after a base row hides that row under a prefix lookup; excluding the
/// tombstone's instant (Java's `validInstantTimestamps`) brings it back.
#[test]
fn si_tombstone_in_a_delete_block_hides_the_base_row_under_a_prefix_lookup() {
    let mdt = mdt_path();
    let schema = mdt_record_schema_json();
    let (base, shard_logs, base_keys, merged) = si_shard_whose_logs_matter();
    let logs: Vec<&str> = shard_logs.iter().map(String::as_str).collect();
    let deleted: Vec<&String> = base_keys.iter().filter(|k| !merged.contains(k)).collect();
    println!(
        "si_tombstone base={base} logs={} base_keys={} merged={} deleted={deleted:?}",
        logs.len(),
        base_keys.len(),
        merged.len()
    );
    let Some(victim) = deleted.first() else {
        // The shard's logs only ADD rows: still prove the added rows are reachable by prefix.
        let added: Vec<&String> = merged.iter().filter(|k| !base_keys.contains(k)).collect();
        let prefix = si_prefix_of(added[0]);
        let lookup = [prefix.as_str()];
        let mut req = si_request(&mdt, &base, &logs, &schema);
        req.lookup_keys = Some(lookup.as_slice());
        req.lookup_keys_are_prefixes = true;
        let keys = keys_of(&read_file_group_v2(&req).expect("prefix over base+logs"));
        assert!(
            keys.contains(added[0]),
            "a log-added SI row must be reachable by its prefix"
        );
        println!(
            "si_tombstone: no tombstone in this shard; proved log-added row {:?} via prefix {prefix:?}",
            added[0]
        );
        return;
    };
    let prefix = si_prefix_of(victim);
    let lookup = [prefix.as_str()];

    // With every completed instant valid (or no range at all) the tombstone applies.
    let instants = mdt_completed_instants();
    let full: Vec<&str> = instants.iter().map(String::as_str).collect();
    let mut req = si_request(&mdt, &base, &logs, &schema);
    req.lookup_keys = Some(lookup.as_slice());
    req.lookup_keys_are_prefixes = true;
    req.valid_instants = &full;
    let batch = read_file_group_v2(&req).expect("prefix over base+logs");
    assert_si_rows(&batch, "si_tombstone");
    let keys = keys_of(&batch);
    assert!(
        !keys.contains(victim),
        "tombstoned key {victim:?} must not be returned under prefix {prefix:?}; got {keys:?}"
    );
    for k in &keys {
        assert!(k.starts_with(&prefix));
    }

    // Excluding every log instant reproduces the base row (Java would too: no valid log block).
    let none: Vec<&str> = vec!["00000000000000000"];
    let mut req_none = si_request(&mdt, &base, &logs, &schema);
    req_none.lookup_keys = Some(lookup.as_slice());
    req_none.lookup_keys_are_prefixes = true;
    req_none.valid_instants = &none;
    let batch_none = read_file_group_v2(&req_none).expect("prefix, logs excluded");
    assert_si_rows(&batch_none, "si_tombstone_logs_excluded");
    let keys_none = keys_of(&batch_none);
    assert!(
        keys_none.contains(victim),
        "with the tombstone's instant excluded the base row {victim:?} must come back; got {keys_none:?}"
    );
}

// ---- v6 (Hudi 0.14) record_index, read under the CURRENT schema -----------
//
// A table-version-6 MDT was written with a `HoodieMetadataRecord` that predates
// four additions: `SecondaryIndexMetadata`, `ColumnStatsMetadata.isTightBound`
// (non-nullable, `"default": false`), `ColumnStatsMetadata.valueType`,
// `recordIndexMetadata.position` — and two union branches appended to
// `ColumnStatsMetadata.minValue`/`.maxValue` (`LocalDateWrapper`,
// `ArrayWrapper`: 12 branches in the file against 14 in the reader schema).
//
// Java reads it with `GenericDatumReader(writerSchema, readerSchema)`
// (`HoodieNativeAvroHFileReader:453`), so Avro's resolver matches union branches
// by full name and fills reader-only fields from their declared defaults. The
// native path must do the same. Before it did, it decoded with the writer schema
// alone and then asked `arrow_cast` for a 12-branch → 14-branch union cast,
// which arrow-cast refuses — the CI failure of
// `TestUpgradeFromV6IndexTypes.testUpgradePreservesIndexFunctionality[6]`.
//
// Fixture: `crates/test/data/metadata_v6_record_index/`; its README carries the
// provenance and how to refresh the reader schema.

/// The file `TestUpgradeFromV6IndexTypes[6]` fails on, verbatim.
const V6_FAILING_HFILE: &str = "record-index-0005-0_4-1636-3849_20260505162917195001.hfile";
/// A v6 slice of the same partition whose log file carries an Avro DATA block,
/// so the log-block half of the resolution is exercised: the block is written in
/// the v6 schema and has to reach the current one.
const V6_BASE_WITH_LOG_HFILE: &str = "record-index-0002-0_3-1636-3848_20260505162917195001.hfile";
const V6_LOG_FILE: &str = ".record-index-0002-0_20260505162917195001.log.1_0-1654-3883";
/// A v6 slice whose log file is a DELETE block against the base row — the same
/// read with nothing to resolve, which must still come out empty rather than
/// erroring.
const V6_BASE_WITH_DELETE_LOG_HFILE: &str =
    "record-index-0004-0_5-1636-3850_20260505162917195001.hfile";
const V6_DELETE_LOG_FILE: &str = ".record-index-0004-0_20260505162917195001.log.1_0-1713-3978";

fn v6_fixture_file(relative: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR").replace("/jvm-ffi", "/test"))
        .join("data/metadata_v6_record_index")
        .join(relative)
}

/// The v6 metadata table's path, extracted from the committed fixture zip.
fn v6_mdt_path() -> String {
    hudi_test::extract_test_table(&v6_fixture_file("v6_record_index_014.zip"))
        .join("v6_record_index_014")
        .to_str()
        .expect("fixture path is utf8")
        .to_string()
}

/// (hfile base names, log file names) under the v6 `record_index`, both sorted.
fn v6_record_index_files() -> (Vec<String>, Vec<String>) {
    let dir = format!("{}/record_index", v6_mdt_path());
    let mut hfiles = Vec::new();
    let mut logs = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("v6 record_index dir") {
        let name = entry
            .expect("dir entry")
            .file_name()
            .to_string_lossy()
            .to_string();
        if name.starts_with("._") {
            continue;
        }
        if name.ends_with(".hfile") {
            hfiles.push(name);
        } else if name.starts_with(".record-index-") && name.contains(".log.") {
            logs.push(name);
        }
    }
    hfiles.sort();
    logs.sort();
    assert!(
        hfiles.iter().any(|h| h == V6_FAILING_HFILE),
        "the fixture must carry the file the Java test fails on"
    );
    (hfiles, logs)
}

/// The CURRENT `HoodieMetadataRecord` reader schema — Java's
/// `HoodieBackedTableMetadata.SCHEMA`, i.e.
/// `HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema())`,
/// derived from hudi-internal `hudi-common/src/main/avro/HoodieMetadata.avsc`.
/// See the fixture README for exactly how.
fn current_metadata_reader_schema_json() -> String {
    std::fs::read_to_string(v6_fixture_file(
        "HoodieMetadataRecord-with-meta-fields.avsc",
    ))
    .expect("read the current HoodieMetadataRecord schema")
    .trim()
    .to_string()
}

/// The writer schema of a v6 HFile, straight out of its own file info.
fn v6_writer_schema_json(base: &str) -> String {
    let path = format!("{}/record_index/{base}", v6_mdt_path());
    let bytes = std::fs::read(&path).expect("read v6 hfile");
    HFileReader::new(bytes)
        .expect("parse v6 hfile")
        .avro_schema_json()
        .expect("read the v6 hfile's avro schema")
        .expect("a v6 MDT hfile carries a writer schema")
        .to_string()
}

fn v6_request<'a>(
    mdt: &'a str,
    base: &'a str,
    logs: &'a [&'a str],
    schema: &'a str,
) -> FileGroupRequest<'a> {
    FileGroupRequest {
        table_path: mdt,
        partition_path: "record_index",
        base_file_name: base,
        log_file_names: logs,
        latest_instant: MAX_INSTANT_TIME,
        data_schema_json: schema,
        lookup_keys: None,
        lookup_keys_are_prefixes: false,
        valid_instants: &[],
    }
}

/// `(key, recordIndexMetadata.fileId, recordIndexMetadata.instantTime)` per row —
/// the RLI payload the Java caller reads off the record.
fn record_index_payload(batch: &arrow::array::RecordBatch) -> Vec<(String, String, i64)> {
    let keys = keys_of(batch);
    let rim = batch
        .column_by_name("recordIndexMetadata")
        .expect("`recordIndexMetadata` column")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("recordIndexMetadata is a struct");
    let file_id = rim
        .column_by_name("fileId")
        .expect("fileId field")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("fileId is utf8");
    let instant = rim
        .column_by_name("instantTime")
        .expect("instantTime field")
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("instantTime is int64");
    let mut out: Vec<(String, String, i64)> = (0..keys.len())
        .map(|i| {
            (
                keys[i].clone(),
                if file_id.is_null(i) {
                    String::new()
                } else {
                    file_id.value(i).to_string()
                },
                if instant.is_null(i) {
                    -1
                } else {
                    instant.value(i)
                },
            )
        })
        .collect();
    out.sort();
    out
}

/// The union branch names of `ColumnStatsMetadata.<field>` in a batch's schema.
fn union_branch_names(batch: &arrow::array::RecordBatch, field: &str) -> Vec<String> {
    let cs = batch
        .schema()
        .field_with_name("ColumnStatsMetadata")
        .expect("`ColumnStatsMetadata` column")
        .clone();
    let arrow::datatypes::DataType::Struct(children) = cs.data_type() else {
        panic!("ColumnStatsMetadata is not a struct: {:?}", cs.data_type());
    };
    let child = children
        .iter()
        .find(|f| f.name() == field)
        .unwrap_or_else(|| panic!("ColumnStatsMetadata has no `{field}` field"));
    match child.data_type() {
        arrow::datatypes::DataType::Union(fields, _) => {
            fields.iter().map(|(_, f)| f.name().clone()).collect()
        }
        other => panic!("ColumnStatsMetadata.{field} is not a union: {other:?}"),
    }
}

/// The child field names of a struct column of a batch.
fn struct_child_names(batch: &arrow::array::RecordBatch, column: &str) -> Vec<String> {
    let field = batch
        .schema()
        .field_with_name(column)
        .unwrap_or_else(|_| panic!("`{column}` column"))
        .clone();
    match field.data_type() {
        arrow::datatypes::DataType::Struct(children) => {
            children.iter().map(|f| f.name().clone()).collect()
        }
        other => panic!("{column} is not a struct: {other:?}"),
    }
}

/// The read the failing Java test performs: the v6 HFiles against the current
/// `HoodieMetadataRecord`. Everything the reader schema added must arrive from
/// its Avro default; everything the file carries must come out unchanged.
///
/// Every shard, not just the failing one: a shard here holds one record, so a
/// single comparison would also pass for a reader that always returned one row.
#[test]
fn v6_record_index_hfiles_read_under_the_current_metadata_schema() {
    let (hfiles, _) = v6_record_index_files();
    let mdt = v6_mdt_path();
    let reader_schema = current_metadata_reader_schema_json();

    let mut total_rows = 0usize;
    let mut failing_batch = None;
    for base in &hfiles {
        let writer_schema = v6_writer_schema_json(base);
        assert_ne!(
            writer_schema, reader_schema,
            "{base}: the fixture must actually be schema-evolved, or this test proves nothing"
        );

        // Independent oracle: the same file decoded with its OWN writer schema.
        // That read never needed resolution, so it is the truth about which rows
        // and which payload the file holds.
        let by_writer = read_file_group_v2(&v6_request(&mdt, base, &[], &writer_schema))
            .unwrap_or_else(|e| panic!("{base} reads under its own writer schema: {e}"));
        let batch = read_file_group_v2(&v6_request(&mdt, base, &[], &reader_schema))
            .unwrap_or_else(|e| panic!("{base} under the current schema must read: {e}"));

        println!(
            "v6_under_current base={base} rows={} writer_schema_rows={} payload={:?}",
            batch.num_rows(),
            by_writer.num_rows(),
            record_index_payload(&batch)
        );
        assert_eq!(
            batch.num_rows(),
            by_writer.num_rows(),
            "{base}: resolution must not add or drop records"
        );
        assert_eq!(
            record_index_payload(&batch),
            record_index_payload(&by_writer),
            "{base}: the RLI payload must be exactly what the writer-schema decode gives"
        );
        total_rows += batch.num_rows();
        if base == V6_FAILING_HFILE {
            failing_batch = Some(batch);
        }
    }
    assert!(
        total_rows >= hfiles.len(),
        "every v6 shard must hold at least one record; got {total_rows} over {} shards",
        hfiles.len()
    );

    // Schema assertions on the file the Java test actually fails on.
    let batch = failing_batch.expect("the failing shard was read");
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    println!("v6_under_current failing_file={V6_FAILING_HFILE} columns={names:?}");

    // The reader schema's twelve top-level fields, `SecondaryIndexMetadata`
    // included — the one the writer schema has no counterpart for.
    for expected in [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "key",
        "type",
        "filesystemMetadata",
        "BloomFilterMetadata",
        "ColumnStatsMetadata",
        "recordIndexMetadata",
        "SecondaryIndexMetadata",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "output schema must carry `{expected}`; got {names:?}"
        );
    }

    // The union resolved by branch NAME and widened to the reader's fourteen.
    for field in ["minValue", "maxValue"] {
        let branches = union_branch_names(&batch, field);
        assert_eq!(
            branches.len(),
            14,
            "ColumnStatsMetadata.{field} must carry the reader schema's 14 branches; got {branches:?}"
        );
        for tail in ["LocalDateWrapper", "ArrayWrapper"] {
            assert!(
                branches.iter().any(|b| b.ends_with(tail)),
                "the appended branch {tail} must be present in {field}; got {branches:?}"
            );
        }
    }

    // The fields Avro fills from their declared defaults.
    let cs_children = struct_child_names(&batch, "ColumnStatsMetadata");
    for expected in ["isTightBound", "valueType"] {
        assert!(
            cs_children.iter().any(|n| n == expected),
            "ColumnStatsMetadata.{expected} must be filled from its default; got {cs_children:?}"
        );
    }
    let rim_children = struct_child_names(&batch, "recordIndexMetadata");
    assert!(
        rim_children.iter().any(|n| n == "position"),
        "recordIndexMetadata.position must be filled from its default; got {rim_children:?}"
    );
}

/// The path the failing Java test actually takes: `readSliceWithFilter` with a
/// key set. Two keys the partition holds plus one it does not must return
/// exactly the two.
#[test]
fn v6_record_index_key_lookup_returns_only_the_keys_that_exist() {
    let (hfiles, _) = v6_record_index_files();
    let mdt = v6_mdt_path();
    let reader_schema = current_metadata_reader_schema_json();

    // A v6 shard here holds a single record, so the two present keys come from
    // two shards; each lookup is asserted against its own shard.
    let mut present: Vec<(String, String)> = Vec::new();
    for base in &hfiles {
        let all = read_file_group_v2(&v6_request(&mdt, base, &[], &v6_writer_schema_json(base)))
            .unwrap_or_else(|e| panic!("{base} writer-schema read: {e}"));
        for key in keys_of(&all) {
            present.push((base.clone(), key));
        }
    }
    present.sort();
    assert!(
        present.len() >= 2,
        "need two record keys across the v6 shards; got {present:?}"
    );
    println!("v6_lookup present={present:?}");

    let absent = "zzz-no-such-record-key";
    let mut matched = 0usize;
    for base in &hfiles {
        let mut lookup: Vec<&str> = present
            .iter()
            .filter(|(b, _)| b == base)
            .map(|(_, k)| k.as_str())
            .chain(std::iter::once(absent))
            .collect();
        // Sorted and deduplicated, as Java's caller hands them over.
        lookup.sort();
        lookup.dedup();
        let expected: Vec<String> = present
            .iter()
            .filter(|(b, _)| b == base)
            .map(|(_, k)| k.clone())
            .collect();

        let mut req = v6_request(&mdt, base, &[], &reader_schema);
        req.lookup_keys = Some(lookup.as_slice());
        let batch = read_file_group_v2(&req)
            .unwrap_or_else(|e| panic!("{base}: v6 key lookup under the current schema: {e}"));
        let mut got = keys_of(&batch);
        got.sort();
        println!("v6_lookup base={base} asked={lookup:?} got={got:?}");
        assert_eq!(
            got, expected,
            "{base}: exactly the keys that exist, and never the absent one"
        );
        matched += got.len();
    }
    assert_eq!(
        matched,
        present.len(),
        "every present key must be reachable by an exact-key lookup"
    );
}

/// The log-block half: a v6 slice with a base file AND a log file, read under
/// the current schema.
///
/// A log block carries its own writer schema, and Java reads it exactly as it
/// reads the base file — `HoodieAvroDataBlock:196` sees a reader record with
/// more fields than the writer's, takes the rewrite branch, and
/// `HoodieAvroUtils.rewriteRecordWithNewSchema` matches the union by branch name
/// and fills `isTightBound` from its `false` default. Nothing less than that
/// reads this block.
#[test]
fn v6_record_index_slice_with_a_log_file_reads_under_the_current_metadata_schema() {
    let (_, logs) = v6_record_index_files();
    for expected in [V6_LOG_FILE, V6_DELETE_LOG_FILE] {
        assert!(
            logs.iter().any(|l| l == expected),
            "the fixture must carry {expected}; got {logs:?}"
        );
    }
    let mdt = v6_mdt_path();
    let reader_schema = current_metadata_reader_schema_json();

    // A log file whose block holds records: the base row plus one the log adds.
    let log_names = [V6_LOG_FILE];
    let base_only = read_file_group_v2(&v6_request(
        &mdt,
        V6_BASE_WITH_LOG_HFILE,
        &[],
        &v6_writer_schema_json(V6_BASE_WITH_LOG_HFILE),
    ))
    .expect("writer-schema base-only read");
    let mut base_keys = keys_of(&base_only);
    base_keys.sort();

    let batch = read_file_group_v2(&v6_request(
        &mdt,
        V6_BASE_WITH_LOG_HFILE,
        &log_names,
        &reader_schema,
    ))
    .unwrap_or_else(|e| panic!("v6 base+log under the current schema must read: {e}"));

    let mut keys = keys_of(&batch);
    keys.sort();
    println!(
        "v6_base_plus_log base_keys={base_keys:?} merged_keys={keys:?} payload={:?}",
        record_index_payload(&batch)
    );
    assert!(
        keys.len() > base_keys.len(),
        "the log block must contribute records the base file does not have; \
         base={base_keys:?} merged={keys:?}"
    );
    for key in &base_keys {
        assert!(
            keys.contains(key),
            "the base row {key:?} must survive the merge; got {keys:?}"
        );
    }
    assert_eq!(
        keys.iter().collect::<HashSet<_>>().len(),
        keys.len(),
        "merge keeps one row per key"
    );

    // The log-contributed rows arrive in the reader schema, not the writer's.
    assert!(
        batch.column_by_name("SecondaryIndexMetadata").is_some(),
        "the merged output must carry the reader schema's added column"
    );
    for field in ["minValue", "maxValue"] {
        assert_eq!(
            union_branch_names(&batch, field).len(),
            14,
            "the merged output carries the reader schema's {field} union"
        );
    }
    assert!(
        struct_child_names(&batch, "ColumnStatsMetadata")
            .iter()
            .any(|n| n == "isTightBound"),
        "the merged output carries the default-filled `isTightBound`"
    );
    assert!(
        struct_child_names(&batch, "recordIndexMetadata")
            .iter()
            .any(|n| n == "position"),
        "the merged output carries the default-filled `position`"
    );

    // Every merged row is a real RLI record, not a shell the projection made.
    // `fileId` is empty here because this fixture stores the location UUID-encoded
    // (`fileIdEncoding = 0`, so the id lives in fileIdHighBits/LowBits/fileIndex),
    // which is exactly why `instantTime` is the field asserted on.
    for (key, file_id, instant) in record_index_payload(&batch) {
        assert!(
            instant > 0,
            "row {key:?} lost its RLI payload: fileId={file_id:?} instantTime={instant}"
        );
    }

    // The lookup shape the Java caller uses, on the merged slice: the two keys
    // the slice holds plus one it does not.
    assert!(keys.len() >= 2, "need two keys on the merged slice");
    let absent = "zzz-no-such-record-key";
    let mut lookup: Vec<&str> = keys
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(absent))
        .collect();
    lookup.sort();
    let mut req = v6_request(&mdt, V6_BASE_WITH_LOG_HFILE, &log_names, &reader_schema);
    req.lookup_keys = Some(lookup.as_slice());
    let looked_up = read_file_group_v2(&req)
        .unwrap_or_else(|e| panic!("v6 base+log key lookup under the current schema: {e}"));
    let mut got = keys_of(&looked_up);
    got.sort();
    println!("v6_base_plus_log lookup asked={lookup:?} got={got:?}");
    assert_eq!(got, keys, "exactly the keys the slice holds");

    // A log file that is a DELETE block against the base row: nothing to
    // resolve, and the row is gone rather than the read failing.
    let delete_logs = [V6_DELETE_LOG_FILE];
    let deleted = read_file_group_v2(&v6_request(
        &mdt,
        V6_BASE_WITH_DELETE_LOG_HFILE,
        &delete_logs,
        &reader_schema,
    ))
    .unwrap_or_else(|e| panic!("v6 base+delete-log under the current schema must read: {e}"));
    println!(
        "v6_delete_log base_rows={} merged_rows={}",
        read_file_group_v2(&v6_request(
            &mdt,
            V6_BASE_WITH_DELETE_LOG_HFILE,
            &[],
            &reader_schema
        ))
        .expect("base-only read")
        .num_rows(),
        deleted.num_rows()
    );
    assert_eq!(
        deleted.num_rows(),
        0,
        "the delete block removes the base row"
    );
    assert!(
        deleted
            .schema()
            .field_with_name("SecondaryIndexMetadata")
            .is_ok(),
        "even an empty result carries the reader schema"
    );
}

/// The other side of the contract: a slice whose writer schema already IS the
/// reader schema must come out exactly as it did before resolution was armed.
/// Read on the committed v8 fixture, every `record_index` shard, in this same
/// test run — with the file's own schema as the requested schema (the FFI path,
/// where resolution is now armed) and with none at all (the path that never had
/// it). The two batches must be equal, values and schema alike.
#[test]
fn v8_record_index_read_is_unchanged_when_the_writer_schema_is_the_reader_schema() {
    let (hfiles, _) = record_index_files();
    let mdt = mdt_path();
    let schema = mdt_record_schema_json();
    for base in &hfiles {
        let with_schema = read_file_group_v2(&FileGroupRequest {
            data_schema_json: &schema,
            ..v6_request(&mdt, base, &[], "")
        })
        .unwrap_or_else(|e| panic!("{base}: v8 read with its own schema requested: {e}"));
        let without_schema = read_file_group_v2(&v6_request(&mdt, base, &[], ""))
            .unwrap_or_else(|e| panic!("{base}: v8 read with no requested schema: {e}"));
        assert_eq!(
            with_schema, without_schema,
            "v8 shard {base}: arming reader-schema resolution must not change a byte \
             when the writer schema already equals the reader schema"
        );
    }
    println!(
        "v8_unchanged shards={} (identical with and without a requested schema)",
        hfiles.len()
    );
}
