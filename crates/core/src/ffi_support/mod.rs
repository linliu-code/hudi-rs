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

use once_cell::sync::Lazy;

pub use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
pub use crate::file_group::reader_v2::input_split::InputSplit;
pub use crate::file_group::reader_v2::merge_iterator::{
    FileGroupMergeStream, StreamReadStats, StreamStatsHandle,
};
pub use crate::file_group::reader_v2::reader_context::{CompletionGateInputs, ReaderContext};
pub use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
pub use crate::file_group::reader_v2::record_context::RecordContext;
pub use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;

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
