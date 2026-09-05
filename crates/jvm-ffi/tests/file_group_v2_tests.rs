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

use arrow::array::{Array, RecordBatchReader, StringArray};
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
            lookup_keys: &[],
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
        lookup_keys: &[],
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
        lookup_keys: &[],
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
        lookup_keys: &[],
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
        lookup_keys: &[],
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
        lookup_keys: &[],
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
        lookup_keys: &[],
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
        lookup_keys: &[],
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
        lookup_keys: &[],
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
        lookup_keys: &[],
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
    req.lookup_keys = &lookup;
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
    req_missing.lookup_keys = &missing;
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
        req_other.lookup_keys = &lookup_other;
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
    req.lookup_keys = &lookup;
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
    req_missing.lookup_keys = &missing_prefix;
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
        req_other.lookup_keys = &lookup_other;
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
fn empty_lookup_keys_and_empty_valid_instants_read_the_whole_slice() {
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
        "an empty predicate and an empty instant set must read every entry of the HFile"
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
    req.lookup_keys = &lookup;
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
    req_missing.lookup_keys = &missing;
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
    req.lookup_keys = &lookup;
    let batch = read_file_group_v2(&req)
        .unwrap_or_else(|e| panic!("log-only shard {shard} with a key predicate must read: {e}"));
    println!("log_only_predicate shard={shard} rows={}", batch.num_rows());
    assert_eq!(
        batch.num_rows(),
        0,
        "a predicate on an already-empty log-only slice must still yield zero rows"
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
