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

//! Injectable base-file data provider — an interface for serving base-file
//! data from an alternative source instead of the object-store read.
//!
//! hudi-rs defines this trait and calls it; it does **not** implement it and has
//! no dependency on any particular source. A downstream crate provides a
//! concrete [`BaseFileDataProvider`] and injects it via
//! [`HoodieFileGroupReaderBuilder::with_base_file_provider`](super::engine::HoodieFileGroupReaderBuilder::with_base_file_provider).
//! The composition root that constructs the provider (the native FFI bridge) is the only place
//! that names both hudi-rs and the concrete source — so this file, and hudi-core
//! as a whole, name no source at all.
//!
//! Scope: **base files only.** Log files are never served by a provider, and the
//! reader skips the provider entirely when position-based merge is active (which
//! needs the synthetic row-index column a provider does not produce). A provider
//! that returns `None` — or none being injected at all — falls straight through
//! to the normal object-store read.

use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::sync::Arc;

/// Source-agnostic, client-side counters for one base-file provider attempt.
///
/// hudi-core neither produces nor interprets these beyond summing them across
/// the base files of a read; a provider fills in whatever it tracks and leaves
/// the rest zero. Surfaced to backends (e.g. the Gluten JNI metrics bridge) via
/// [`HoodieReadStats::base_file_provider`](super::read_stats::HoodieReadStats::base_file_provider)
/// and, while a stream is still draining, through
/// [`HoodieFileGroupReader::base_file_provider_live_stats`](super::engine::HoodieFileGroupReader::base_file_provider_live_stats).
/// The field set is intentionally generic — no wire/protocol type of any
/// concrete provider leaks here.
///
/// ## When each field is populated
/// The provider is served **lazily** (see [`BaseFileDataProvider::try_base_file`]):
/// the batches stream through the merge loop rather than being materialized up
/// front. So the counters split by when they become known:
/// - **Setup counters** — `files_served`, `storage_fallbacks`, `local_served`,
///   `remote_served`, `discover_wall_nanos`, `connect_wall_nanos`,
///   `fetch_wall_nanos` — are known the moment the provider decides to serve, so
///   the provider fills them in the [`BaseFileProviderStats`] it returns from
///   `try_base_file`.
/// - **Drain counters** — `rows_served`, `bytes_materialized`, `batches_received`
///   — are only knowable once the stream has been consumed, which happens *after*
///   `try_base_file` returns. A streaming provider therefore leaves them zero;
///   **hudi-core** counts them as it pulls the served source and folds them into
///   the same slot, so a consumer that reads the stats after draining sees the
///   complete picture. A provider that fills them anyway would be double-counted
///   on the eager path, so leaving them zero is part of the contract (the C-ABI
///   adapter enforces it for C providers by zeroing them on the served path).
#[derive(Clone, Debug, Default)]
pub struct BaseFileProviderStats {
    /// Base files served by the provider.
    pub files_served: u64,
    /// Base files the provider could not serve (fell through to object storage).
    pub storage_fallbacks: u64,
    /// Subset of `files_served` served from a source local to this reader.
    pub local_served: u64,
    /// Subset of `files_served` served from a remote source.
    pub remote_served: u64,
    /// Rows served by the provider across all served files. Drain counter —
    /// filled by hudi-core while consuming the served source, not by the provider.
    pub rows_served: u64,
    /// In-memory Arrow footprint of the served batches, summed across all served
    /// files. Drain counter — filled by hudi-core while consuming the source.
    ///
    /// **Not a transfer volume.** This is measured *after* projection to the
    /// read's required schema, from `RecordBatch::get_array_memory_size()`, which
    /// reports *allocated buffer capacity* — so shared dictionary buffers and any
    /// over-allocated child buffer are counted in full, per batch. It can
    /// therefore exceed the bytes a provider actually transferred, sometimes by a
    /// large factor. Use it to reason about reader memory, never to reconcile
    /// against a provider-side byte counter; the two measure different things.
    /// Named `bytes_materialized` rather than `bytes_served` for exactly this
    /// reason.
    pub bytes_materialized: u64,
    /// Arrow record batches received from the provider across all served files.
    /// Drain counter — filled by hudi-core while consuming the source.
    pub batches_received: u64,
    /// Wall-clock nanoseconds spent discovering the serving endpoint.
    pub discover_wall_nanos: u64,
    /// Wall-clock nanoseconds spent connecting to the serving endpoint.
    pub connect_wall_nanos: u64,
    /// Wall-clock nanoseconds spent in the data fetch itself.
    pub fetch_wall_nanos: u64,
}

impl BaseFileProviderStats {
    /// Accumulate another set of counters into this one (per-base-file → per-read).
    pub fn merge(&mut self, other: &BaseFileProviderStats) {
        self.files_served += other.files_served;
        self.storage_fallbacks += other.storage_fallbacks;
        self.local_served += other.local_served;
        self.remote_served += other.remote_served;
        self.rows_served += other.rows_served;
        self.bytes_materialized += other.bytes_materialized;
        self.batches_received += other.batches_received;
        self.discover_wall_nanos += other.discover_wall_nanos;
        self.connect_wall_nanos += other.connect_wall_nanos;
        self.fetch_wall_nanos += other.fetch_wall_nanos;
    }
}

/// Everything a provider needs to serve one base file, expressed entirely in
/// hudi-core's own vocabulary. Deliberately carries **no** predicate or filter
/// type: the composition root decodes the predicate once and bakes any filters
/// into the concrete provider at construction, so no provider/query type
/// crosses this boundary.
pub struct BaseFileDataRequest<'a> {
    /// Absolute storage URI of the base file — the same URL the object-store read
    /// resolves to, and the identity a provider is expected to key the file by.
    pub file_uri: &'a str,
    /// The projected ("intersection") schema the read wants back.
    pub projected_schema: &'a SchemaRef,
    /// Whether it is safe to apply a pushed predicate **to this file**. A provider
    /// that pushes a predicate must honor it: when `false`, serve unfiltered so a
    /// post-merge filter can apply the predicate instead.
    ///
    /// This is a PER-FILE decision, not a table-level property, and it is exactly
    /// the decision the reader's own parquet `RowFilter` pushdown got for the same
    /// file. Two gates, both of which must pass:
    ///
    /// 1. the merge gate, `HoodieFileGroupReader::base_read_pushdown_is_safe` —
    ///    the split has no log files (nothing merges, so the base rows are final)
    ///    or the predicate is primary-key-safe; and
    /// 2. the repair gate — this file's footer does not label a predicate column
    ///    in a way the apache/hudi#18132 logical-type repair reinterprets on read.
    ///    Parquet evaluates a pushed predicate against the file's PHYSICAL values,
    ///    so a file that labels a tz-aware column micros while its stored i64 is
    ///    millis makes a millis-semantics literal read those rows as 1970 and drop
    ///    rows that match. Nothing downstream can restore them.
    ///
    /// Gate 2 is why this must not be read as "the merge gate": the same read can
    /// hand `true` for one base file and `false` for the next, and a provider that
    /// caches the answer per split is wrong.
    pub can_push_predicate: bool,
    /// Partition path of the split (e.g. `year=2024/month=01`), used to report
    /// partition-column metadata to the provider.
    pub partition_path: &'a str,
    /// Partition field names from `hoodie.table.partition.fields`, in order.
    pub partition_fields: &'a [String],
    /// The table's data schema, used to resolve partition-column types.
    pub data_schema: Option<&'a SchemaRef>,
}

/// A pluggable source that may serve a base file instead of the object-store
/// read.
///
/// Implemented downstream (never in hudi-core) and injected via
/// [`HoodieFileGroupReaderBuilder::with_base_file_provider`](super::engine::HoodieFileGroupReaderBuilder::with_base_file_provider).
/// Called once per base file **the read opens**, before the object-store read.
/// A base file the instant range excludes is settled before any read is set up
/// and is never offered, so a provider's `files_served + storage_fallbacks` is a
/// count of files read, not of files in the split. Implementations must map any
/// internal error to `None` — a provider failure is never allowed to fail the
/// read.
#[async_trait]
pub trait BaseFileDataProvider: Send + Sync {
    /// Try to serve `req.file_uri`. Returns `Some(reader)` — a **lazy**
    /// [`RecordBatchReader`] yielding batches at the request's
    /// `projected_schema`, exactly the shape the object-store projected read
    /// would have returned — or `None` when the provider cannot serve the file,
    /// in which case the caller reads from object storage as usual. Also returns
    /// the client-side setup counters for this attempt (a fallback still reports
    /// its timings; see [`BaseFileProviderStats`] for which counters the provider
    /// fills versus which hudi-core fills during drain).
    ///
    /// ## Streaming / threading contract
    /// The returned reader is consumed lazily rather than drained here, so the
    /// whole served file never needs to be resident at once. hudi-core moves it
    /// onto **one** `spawn_blocking` task that owns it for its whole life and
    /// feeds the merge over a depth-1 channel. Three consequences an
    /// implementation may rely on:
    ///
    /// - **`next()` may block.** It runs on tokio's blocking pool, never on a
    ///   runtime worker, so blocking on IO cannot stall the executor driving the
    ///   rest of the read.
    /// - **`next()` may call `block_on`** on the runtime driving the read. A
    ///   blocking-pool thread has that runtime's *handle* set (`Handle::enter`)
    ///   but is not "entered" in the sense `Handle::block_on` refuses, so nested
    ///   `block_on` there does not panic. (Do not rely on this from a runtime
    ///   worker — that is where it does panic, and a panic unwinding across the
    ///   FFI boundary is undefined behavior.)
    /// - **Every `next()` runs on the same thread**, so a thread-local arena or
    ///   a thread-bound connection is safe to hold across batches.
    ///
    /// The one thing to know in the other direction: the reader is pulled **up to
    /// two batches ahead** of the merge (one buffered, one blocked on send), so a
    /// read that ends early may have paid for two batches nobody consumed.
    /// `next()` must therefore be free of side effects the caller would not want
    /// on a cancelled read.
    ///
    /// The reader must be `Send` + `'static` so it can move from this async
    /// method onto that task.
    async fn try_base_file(
        &self,
        req: BaseFileDataRequest<'_>,
    ) -> (
        Option<Box<dyn RecordBatchReader + Send + 'static>>,
        BaseFileProviderStats,
    );
}

/// Shared handle to an injected provider.
pub type BaseFileDataProviderRef = Arc<dyn BaseFileDataProvider>;
