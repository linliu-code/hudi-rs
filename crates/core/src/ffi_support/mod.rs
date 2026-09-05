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

//! Support surface for out-of-process FFI consumers (`cpp/`).
//!
//! Additive: everything here either re-exports an existing `reader_v2` type or
//! adds a helper that only an FFI consumer needs. Nothing in hudi-core depends
//! on this module, so it can be dropped without touching the rest of the crate.

use arrow_schema::SchemaRef;
use once_cell::sync::Lazy;

use crate::error::Result;
use crate::file_group::log_file::avro::{AvroBlockDecoder, RegisteredWriterSchema};

/// Batch size handed to the decoder built purely to read a schema off it.
/// Matches the HFile base-file reader's `DECODE_BATCH_SIZE`.
const SCHEMA_BATCH_SIZE: usize = 1024;

pub use crate::file_group::reader_v2::MAX_INSTANT_TIME;
pub use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
pub use crate::file_group::reader_v2::input_split::InputSplit;
pub use crate::file_group::reader_v2::merge_iterator::{
    FileGroupMergeStream, StreamReadStats, StreamStatsHandle,
};
pub use crate::file_group::reader_v2::reader_context::{CompletionGateInputs, ReaderContext};
pub use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
pub use crate::file_group::reader_v2::record_context::RecordContext;
pub use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
pub use crate::timeline::selector::InstantRange;

/// One long-lived multi-threaded runtime for every FFI-driven read.
///
/// hyper's connection dispatcher is spawned on the runtime that drives the
/// FIRST actual HTTP request, not the one active when the ObjectStore was
/// built. A per-file-group `current_thread` runtime therefore binds the
/// dispatcher to a runtime that is dropped as soon as `block_on` returns, and
/// every later read against the cached ObjectStore fails with `DispatchGone`.
/// Driving all reads on this one runtime gives the dispatcher and every
/// subsequent request a shared lifetime.
///
/// 8 workers: Velox executors run up to ~16 task slots concurrently through
/// here, and CPU-bound parquet decode lands here too. If that becomes a
/// bottleneck the fix is per-bucket subruntimes or a separate CPU pool -- NOT
/// per-task runtimes, which reintroduces `DispatchGone`.
///
/// Multi-thread (not `current_thread`) is load-bearing: the FFI adapter calls
/// `block_on` from inside `Iterator::next` while the same runtime drives IO.
pub static OBJECT_STORE_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .thread_name("hudi-rs-objstore")
        .build()
        .expect("Failed to build OBJECT_STORE_RUNTIME")
});

/// Borrow the reader's stage-timing sink.
///
/// The FFI must capture this **before** the reader is dropped: the returned
/// stream keeps writing into it as chunks drain, so `read_stats()` on the dead
/// reader would report zeros forever.
pub fn stream_stats_handle(reader: &HoodieFileGroupReader) -> StreamStatsHandle {
    reader.stream_stats_handle()
}

/// Arrow schema for an Avro JSON schema, built the way the HFile base-file
/// reader builds it (`RegisteredWriterSchema` + `AvroBlockDecoder::schema()`)
/// rather than through `avro_to_arrow`, which has no `AvroSchema::Ref` support
/// and panics on the metadata table's own record schema. Foreign callers
/// (JNI / C ABI) hand Java's data schema over through this.
///
/// The decoder is built only to be asked for its schema and is dropped
/// immediately, so the batch size it is given never bounds anything; it mirrors
/// `crates/core/src/file_group/base_file/hfile.rs`'s `DECODE_BATCH_SIZE` so the
/// two conversions cannot drift.
pub fn arrow_schema_from_avro_json(avro_schema_json: &str) -> Result<SchemaRef> {
    let registered = RegisteredWriterSchema::new(avro_schema_json)?;
    let decoder = AvroBlockDecoder::try_new_with_registered(&registered, None, SCHEMA_BATCH_SIZE)?;
    Ok(decoder.schema())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hudi_test::QuickstartTripsTable;

    /// The metadata table's own record schema is the case `avro_to_arrow` cannot
    /// convert: its `ColumnStatsMetadata.maxValue` union references the wrapper
    /// records that `minValue` defines, which parses as `AvroSchema::Ref`. The
    /// decoder path handles it, so the conversion must survive the real schema
    /// the fixture's MDT was written with.
    #[test]
    fn the_metadata_table_writer_schema_converts_through_the_decoder_path() {
        let mdt = format!(
            "{}/.hoodie/metadata/record_index",
            QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro()
        );
        let mut hfiles: Vec<String> = std::fs::read_dir(&mdt)
            .expect("record_index dir")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .to_string()
            })
            // `._…` are macOS resource forks the fixture zip carries along.
            .filter(|n| !n.starts_with("._") && n.ends_with(".hfile"))
            .collect();
        hfiles.sort();
        assert!(!hfiles.is_empty(), "fixture must carry record_index HFiles");

        let bytes = std::fs::read(format!("{mdt}/{}", hfiles[0])).expect("read hfile");
        let reader = crate::hfile::HFileReader::new(bytes).expect("parse hfile");
        let json = reader
            .avro_schema_json()
            .expect("read the hfile's avro schema")
            .expect("the MDT hfile carries a writer schema")
            .to_string();

        let schema = arrow_schema_from_avro_json(&json).expect("convert the MDT writer schema");
        println!(
            "arrow_schema_from_avro_json json_len={} fields={} columns={:?}",
            json.len(),
            schema.fields().len(),
            schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        assert!(
            schema.column_with_name("key").is_some(),
            "the MDT record schema has a `key` column"
        );
        assert!(
            schema.column_with_name("recordIndexMetadata").is_some(),
            "the MDT record schema has a `recordIndexMetadata` column"
        );
    }

    /// An unparseable schema is an error, not a panic.
    #[test]
    fn a_malformed_avro_schema_is_an_error() {
        let err = arrow_schema_from_avro_json("{not avro}").expect_err("must fail");
        assert!(!err.to_string().is_empty());
    }
}
