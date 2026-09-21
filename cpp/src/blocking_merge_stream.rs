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

//! Synchronous [`RecordBatchReader`] over hudi-core's async merge stream.
//!
//! `FFI_ArrowArrayStream::get_next` is a synchronous C callback: C++ pulls one
//! batch at a time. hudi-core's OSS `FileGroupMergeStream` is async
//! (`next_chunk().await`). This adapter is the bridge, and it is the only
//! genuinely new logic in the OSS port.
//!
//! Each `next()` runs `OBJECT_STORE_RUNTIME.block_on(next_chunk())`, so the
//! parquet decode and hyper connection dispatch happen on the same
//! long-lived multi-threaded runtime that owns the connection pool. Same
//! shape as `ParquetSyncReader` in hudi-core's `storage` module.
//!
//! **Context requirement.** `block_on` panics if called from inside a tokio
//! runtime, and a panic unwinding across the FFI boundary is UB. Callers must
//! be plain native threads. `get_closable_iterator` enforces this with an
//! explicit `Handle::try_current()` check before constructing this adapter.

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use hudi_dep::ffi_support::{FileGroupMergeStream, OBJECT_STORE_RUNTIME};

/// Drives [`FileGroupMergeStream`] synchronously for the FFI.
pub struct BlockingMergeStream {
    inner: FileGroupMergeStream,
    /// Cached at construction: `RecordBatchReader::schema` is infallible and
    /// must answer before the first `next()`, since the FFI reads the schema
    /// when it creates the stream.
    schema: SchemaRef,
    /// Fuse latch: once set, `next()` returns `None` forever.
    ///
    /// RV-17. Set both on natural exhaustion and — the reason it exists — on
    /// the tokio re-entry refusal. The refusal yields `Some(Err(..))`, and
    /// `FFI_ArrowArrayStream` consumers routinely keep pulling after an error
    /// (arrow-rs's own `ArrowArrayStreamReader` does), so without the latch the
    /// same refusal is re-emitted on every subsequent pull: an infinite stream
    /// of identical errors instead of a terminated one. The condition cannot
    /// clear itself either — the calling thread does not stop being a tokio
    /// worker between two `get_next` calls — so re-checking it buys nothing.
    done: bool,
}

impl BlockingMergeStream {
    pub fn new(inner: FileGroupMergeStream) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            done: false,
        }
    }

    /// Current merge-map footprint, for the `hudi_reader_memory_bytes` metric.
    pub fn current_in_memory_bytes(&self) -> u64 {
        self.inner.current_in_memory_bytes()
    }
}

impl Iterator for BlockingMergeStream {
    type Item = Result<RecordBatch, ArrowError>;

    /// Fused: after the tokio re-entry refusal, and after the merge stream is
    /// exhausted, every subsequent call returns `None` (see [`Self::done`]).
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if tokio::runtime::Handle::try_current().is_ok() {
            self.done = true;
            return Some(Err(ArrowError::ExternalError(
                "BlockingMergeStream::next called from inside a tokio runtime; \
                 block_on would panic on re-entry (UB across FFI)"
                    .into(),
            )));
        }
        let chunk = OBJECT_STORE_RUNTIME
            .block_on(self.inner.next_chunk())
            .map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(e))));
        if chunk.is_none() {
            self.done = true;
        }
        chunk
    }
}

impl RecordBatchReader for BlockingMergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
