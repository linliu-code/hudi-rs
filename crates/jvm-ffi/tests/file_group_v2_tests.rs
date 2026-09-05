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
            latest_instant: "",
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
        latest_instant: "",
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
        latest_instant: "",
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
        latest_instant: "",
    };
    let err = read_file_group_v2(&req).expect_err("must fail");
    assert!(!err.is_empty());
}
