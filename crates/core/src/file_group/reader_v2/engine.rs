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

//! The merge-on-read file group reader.
//!
//! Mirrors Java's `org.apache.hudi.common.table.read.HoodieFileGroupReader`.
//! Reached from `file_group::reader::FileGroupReader` through [`super::adapter`].

use crate::Result;
use crate::config::table::{BaseFileFormatValue, HudiTableConfig};
use crate::error::CoreError;
use crate::file_group::base_file::hfile::HFileBaseFileReader;
use crate::file_group::base_file::reader::{
    BaseFileReadOptions, BaseFileReader, create_base_file_reader,
};
use crate::file_group::reader_v2::base_file_provider::{
    BaseFileDataProviderRef, BaseFileDataRequest, BaseFileProviderStats,
};
use crate::file_group::reader_v2::buffer::BufferType;
use crate::file_group::reader_v2::buffer::loader::{
    DefaultFileGroupRecordBufferLoader, FileGroupRecordBufferLoader,
};
use crate::file_group::reader_v2::buffer::record_positions::ROW_INDEX_TEMPORARY_COLUMN_NAME;
use crate::file_group::reader_v2::buffered_record_converter::BufferedRecordConverter;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::iterator_mode::IteratorMode;
use crate::file_group::reader_v2::merge_iterator::{
    BaseBatchStream, FileGroupMergeStream, StreamStatsHandle, new_stream_stats_handle,
};
use crate::file_group::reader_v2::output_converter::OutputConverter;
use crate::file_group::reader_v2::profiling::profile_once;
use crate::file_group::reader_v2::read_stats::HoodieReadStats;
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
use crate::storage::util::join_url_segments;
use crate::storage::{RowFilterBuilder, RowGroupSelector, Storage};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use std::str::FromStr;
use std::sync::Arc;
// Aliased because an unqualified `Mutex` in this neighbourhood reads as
// `tokio::sync::Mutex`. The provider-stats slot is deliberately the std lock: it
// is only ever held for a handful of field additions, never across an `.await`.
use std::sync::Mutex as StdMutex;

/// The top-level file group reader orchestrator.
///
/// Mirrors Java's `org.apache.hudi.common.table.read.HoodieFileGroupReader<T>`.
///
/// This is the main entry point for reading a file group. It:
/// 1. Accepts an `InputSplit` describing what to read (base file + log files)
/// 2. Creates the [`FileGroupReaderSchemaHandler`] from `data_schema` + `requested_schema`
/// 3. Creates base file iterators via storage
/// 4. Delegates log scanning + buffer creation to `FileGroupRecordBufferLoader`
/// 5. Merges base file records with log records via the buffer
/// 6. Projects output back to `requested_schema` via `OutputConverter`
///
/// ## Construction
///
/// Use [`HoodieFileGroupReader::builder()`] for the builder pattern, or construct
/// directly with [`HoodieFileGroupReader::new()`].
pub struct HoodieFileGroupReader {
    // ── Context (mirrors Java's HoodieReaderContext<T>) ────────────────
    /// Reader context carrying merge mode, instant range, and config maps.
    reader_context: Arc<ReaderContext>,

    /// Storage for reading base files and log files.
    storage: Arc<Storage>,

    // ── Input ──────────────────────────────────────────────────────────
    /// Describes what to read: base file, log files, partition path.
    input_split: InputSplit,

    // ── Configuration ──────────────────────────────────────────────────
    /// Reader flags: use_record_position, emit_delete, sort_output, etc.
    reader_parameters: ReaderParameters,

    /// The current iterator mode.
    #[allow(dead_code)]
    iterator_mode: IteratorMode,

    // ── Schema (mirrors Java's readerContext.getSchemaHandler()) ───────
    /// Schema handler created in the constructor from `data_schema` +
    /// `requested_schema`, exactly like Java lines 119-121.
    /// Owns the `required_schema` used for base file projection and the
    /// `output_converter` used for final projection.
    schema_handler: FileGroupReaderSchemaHandler,

    // ── Strategy ───────────────────────────────────────────────────────
    /// Buffer loader: selects buffer impl + triggers log scan.
    record_buffer_loader: DefaultFileGroupRecordBufferLoader,

    // ── Mutable state (populated during read) ──────────────────────────
    // NOTE: the record buffer and base-file batches are not stored on the
    // reader — they are local to `init_record_iterators` and owned by the
    // returned `FileGroupMergeStream` for the rest of the read.
    /// Optional converter for projecting/transforming output records.
    /// Mirrors Java's `Option<UnaryOperator<T>> outputConverter`.
    output_converter: Option<Box<dyn OutputConverter>>,

    /// Read statistics accumulator.
    read_stats: HoodieReadStats,

    /// Stage-timing sink shared with the [`FileGroupMergeStream`] returned by
    /// [`Self::open`]. The streaming iterator
    /// owns the buffer once `open()` returns, so the merge-phase timings
    /// (final_merge_us, output_build_us) and the update-processor
    /// insert/update/delete counts are accumulated through this handle during
    /// iteration and drained back into [`Self::read_stats`] by [`Self::read`]
    /// after the stream is exhausted. Wrapped in `Arc<Mutex<…>>` because the FFI
    /// path requires the iterator to be `Send` (it is boxed into an
    /// `FFI_ArrowArrayStream`); the lock is taken once per emitted chunk, so the
    /// cost is negligible against the per-chunk merge work.
    ///
    /// Both call shapes can read these back, by different routes.
    /// `read()`-based callers get them drained into [`Self::read_stats`]. A
    /// caller that streams through `open()` never calls `read()`, so that drain
    /// never runs for it — keeping the reader alive would not change that. A
    /// streaming caller that wants them clones the handle via
    /// [`Self::stream_stats_handle`] before it drops the reader and reads the
    /// shared sink after the stream drains, as the C++ FFI does; see
    /// [`crate::ffi_support::stream_stats_handle`] for that contract.
    stream_stats: StreamStatsHandle,

    /// Valid block instants from log scanning.
    valid_block_instants: Vec<String>,

    /// Converter for engine records to [`BufferedRecord`].
    /// Mirrors Java's `BufferedRecordConverter<T> bufferedRecordConverter`.
    buffered_record_converter: Option<Box<dyn BufferedRecordConverter>>,

    /// Optional injected base-file data provider (dependency injection).
    ///
    /// `None` on the default (OSS) path — the reader then reads every base file
    /// from object storage. When set via
    /// [`HoodieFileGroupReaderBuilder::with_base_file_provider`], the reader
    /// offers each base file to the provider before the object-store read.
    /// hudi-core implements no provider; see [`super::base_file_provider`].
    base_file_provider: Option<BaseFileDataProviderRef>,

    /// The one provider-stats sink for this reader.
    ///
    /// Shared rather than plain because the base source is **lazy**: only the
    /// setup counters (`files_served`, `storage_fallbacks`, the wall timings) are
    /// known when [`Self::base_file_source`] returns, while the drain counters
    /// (`rows_served`, `bytes_materialized`, `batches_received`) fill in as the
    /// caller pulls the merge stream — which, on the FFI path, happens after the
    /// reader itself has been dropped. So the slot is seeded at serve time and
    /// topped up in place by [`served_batch_stream`].
    ///
    /// [`Self::read`] snapshots it into
    /// [`HoodieReadStats::base_file_provider`](super::read_stats::HoodieReadStats::base_file_provider)
    /// once the stream is exhausted; a streaming caller reads it live through
    /// [`Self::base_file_provider_live_stats`]. All-zero when no provider is
    /// injected, so the seam costs the default path one `Arc` allocation and
    /// nothing else.
    base_file_provider_stats: Arc<StdMutex<BaseFileProviderStats>>,

    /// Guards the "exactly one provider attempt per reader" invariant that
    /// [`Self::record_provider_stats`] relies on to rule out double-counting.
    ///
    /// An atomic rather than a plain `bool` so recording stays a `&self`
    /// operation: the seam runs inside `base_file_source`, which is already
    /// holding an immutable borrow of `self` through its `read_options` closure.
    provider_stats_recorded: std::sync::atomic::AtomicBool,
    // NOTE: the optional parquet `RowFilter` builder lives on
    // `reader_context`, not this struct, so the same builder is
    // visible to (a) the base parquet read here, and (b) the parquet log
    // block decoder in `file_group::log_file::content::Decoder`. The gate
    // (CoW || mor_pk_safe) lives at the use sites; this file's gate is at
    // `base_file_source` below.
}

/// Rows per base batch handed to the merge, and therefore per merged chunk.
///
/// Load-bearing rather than cosmetic: merging a chunk is synchronous work on the
/// task that polls the stream, and its cost is linear in the chunk's rows. On
/// this machine, one merge of a 1024-row chunk against a 50k-key log map takes
/// 0.4-1.1 ms, and 5.7-6.1 ms once the merge map has spilled to disk; at 8192
/// rows those become 2.8 ms and ~40 ms. So the chunk size is what bounds how long
/// a single poll occupies its executor, and it is set here rather than inherited.
///
/// 1024 is what `parquet` already defaults to, so this pins today's behaviour
/// instead of changing it. Pinned because the bound is silent if it moves: a
/// larger default upstream would multiply the blocking above with nothing
/// failing. Measured by `spilled_merge_blocking_duration` (ignored; run with
/// `--release --ignored --nocapture`).
const MERGE_CHUNK_ROWS: usize = 1024;

/// [ENG-48159] How many base-file batches `base_file_source()` fetches **before**
/// it hands the stream back, on the object-store read path.
///
/// **The problem.** Velox prepares a split on its connector IO executor, ahead of
/// the driver (`TableScan::preload` -> `AsyncSource::prepare` -> `addSplit` ->
/// `HudiSplitReader::prepareSplit` -> `get_closable_iterator` ->
/// `FileGroupReader::open`). Making the base source lazy fixed a real bug — the
/// eager path concatenated a whole file into ONE arrow vector — but it moved the
/// fetch and decode onto the driver, where they serialise against query
/// execution: internal measured 51-198 ms per split across 1,823-2,337 splits per
/// scan node. Awaiting a bounded prefix inside `open()` puts that prefix back on
/// the preload thread, because `open()` is what the FFI drives with
/// `OBJECT_STORE_RUNTIME.block_on` from that thread.
///
/// **Why 2.** Internal's regressing splits carried 1-7 batches at the default
/// chunk size (50 / 1,684 / 14,110 / 27,727 rows per split on the four measured
/// scan nodes), so 2 fully covers the single-batch majority and gives the larger
/// ones a head start, at a cost of at most two buffered batches per in-flight
/// split. It is bounded, so it cannot become the eager path again, and it
/// preserves batch boundaries, so it cannot become the one-vector bug.
///
/// A `const` and not a config key deliberately: `hoodie.read.stream.batch_size`
/// shipped as a permanent knob and was then measured near-inert, so this stays
/// unconfigurable until a measurement shows the optimum is workload-dependent.
///
/// **0 disables the prefetch**, reproducing the pre-ENG-48159 behaviour exactly,
/// which is why there is no separate feature flag.
///
/// Applies to the **object-store** leg only. The provider leg already runs up to
/// two batches ahead by construction — see [`served_batch_stream`], whose depth-1
/// channel gives it the same bound and the same no-truncation guarantee.
const BASE_READ_INITIAL_PREFETCH_BATCHES: usize = 2;

/// Base-file read options carrying an optional pushdown predicate, the Avro
/// reader schema, and the row-position column when the merge is by position.
///
/// The three base reads below differ only in projection, so all of them are
/// attached in one place — a read that silently lost the filter would return
/// extra rows rather than fail, which is the hard kind of bug to notice; one
/// that lost the row-position column would fail in the buffer with the column
/// named but not the read that dropped it; and one that lost the reader schema
/// would decode an older file in its own writer schema and only fail later, in
/// the Arrow projector, with the two schemas printed and neither named.
fn base_read_options(
    row_filter: Option<RowFilterBuilder>,
    row_group_selector: Option<RowGroupSelector>,
    key_predicate: Option<crate::file_group::base_file::reader::KeyPredicate>,
    reader_schema_json: Option<String>,
    use_record_position: bool,
) -> BaseFileReadOptions {
    let mut options = BaseFileReadOptions::new();
    options = options.with_batch_size(MERGE_CHUNK_ROWS);
    if let Some(reader_schema_json) = reader_schema_json {
        options = options.with_reader_schema_json(reader_schema_json);
    }
    if let Some(row_filter) = row_filter {
        options = options.with_row_filter(row_filter);
    }
    if let Some(row_group_selector) = row_group_selector {
        options = options.with_row_group_selector(row_group_selector);
    }
    if let Some(key_predicate) = key_predicate {
        options = options.with_key_predicate(key_predicate);
    }
    if use_record_position {
        options = options.with_row_index_column(ROW_INDEX_TEMPORARY_COLUMN_NAME);
    }
    options
}

/// A base file as the merge consumes it: batches, plus the schema they carry.
///
/// The schema travels with the stream because a `Stream` has no `schema()` the
/// way a `RecordBatchReader` does, and the merge needs it before the first
/// batch arrives — to derive the merge schema, and to describe a base file that
/// yields no batches at all.
struct BaseSource {
    schema: SchemaRef,
    batches: crate::file_group::reader_v2::merge_iterator::BaseBatchStream,
}

impl BaseSource {
    /// A base file that contributes nothing: no base file at all, or one the
    /// instant range excludes.
    fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: futures::stream::empty().boxed(),
        }
    }
}

/// [ENG-48159] Fetch up to `batches` items from `stream` **now**, then return a
/// stream that serves those before pulling any more.
///
/// Called from `base_file_source()`, which is awaited inside
/// [`HoodieFileGroupReader::open`] — and `open()` is what the FFI drives with
/// `OBJECT_STORE_RUNTIME.block_on` on the thread Velox prepared the split on. So
/// the prefix is paid for there rather than on the driver. See
/// [`BASE_READ_INITIAL_PREFETCH_BATCHES`] for the measurement that motivates it.
///
/// **Awaited, never `block_on`-driven.** The caller is already inside
/// `OBJECT_STORE_RUNTIME.block_on(reader.open())`; a `block_on` here would be a
/// runtime re-entry panic, which across the cxx FFI boundary is UB. That is also
/// why this is a free `async fn` over the stream rather than N calls to some
/// synchronous adapter's `next()`.
///
/// **A short prefetch is never end-of-stream.** The buffer holds `Result`s, not
/// batches, so a read error keeps its own position in the sequence and is
/// delivered there. Two consequences, both load-bearing:
/// - an error stops the prefetch but does **not** mark the stream done, so
///   semantics past an error are unchanged from the unprefetched stream;
/// - the only `None` the consumer can observe is `stream`'s own exhaustion.
///
/// A prefetch that gave up early and reported success would be a silent short
/// read — every caller would see a valid, shorter table — which is the failure
/// mode this shape makes unrepresentable rather than merely unlikely.
///
/// When the prefetch loop consumes the stream to its end it drops it rather than
/// chaining it, so an exhausted stream is never polled again. Correctness does
/// not rest on that (the sources here are terminal and idempotent), but it makes
/// the contract local instead of inherited, and it is what
/// `initial_prefetch_does_not_repoll_a_completed_stream` pins.
///
/// `batches == 0` yields a stream that serves `stream` and polls it not at all
/// before the consumer asks, which is the intended way to disable this. (It is an
/// empty buffer chained onto `stream`, not `stream` itself — observationally the
/// same, and `initial_prefetch_depth_zero_matches_non_prefetched_stream` pins both
/// halves.)
async fn prefetch_initial_batches(mut stream: BaseBatchStream, batches: usize) -> BaseBatchStream {
    use futures::StreamExt;
    let mut prefetched: std::collections::VecDeque<Result<RecordBatch>> =
        std::collections::VecDeque::with_capacity(batches);
    let mut stream_done = false;
    for _ in 0..batches {
        match stream.next().await {
            Some(Ok(batch)) => prefetched.push_back(Ok(batch)),
            Some(Err(e)) => {
                prefetched.push_back(Err(e));
                break;
            }
            None => {
                stream_done = true;
                break;
            }
        }
    }
    if stream_done {
        futures::stream::iter(prefetched).boxed()
    } else {
        futures::stream::iter(prefetched).chain(stream).boxed()
    }
}

/// Adapt a provider's served base file into the stream the merge consumes.
///
/// **One blocking task owns the reader for its whole life.** That is what keeps
/// the two properties the provider contract rests on:
/// - *Thread affinity.* Every `next()` runs on the same OS thread, so a provider
///   may hold a thread-local arena or a thread-bound connection, and may call
///   `block_on` on the read's runtime — a blocking-pool thread has that runtime's
///   handle set but is not "entered", so nested `block_on` does not panic there.
///   Pulling each batch in its own `spawn_blocking` would move the reader between
///   pool threads and quietly withdraw both guarantees.
/// - *No runtime worker is stalled.* `next()` may block on IO for as long as it
///   likes without holding up the executor driving the rest of the merge.
///
/// Batches cross back over a depth-1 channel, so the file stays lazy — but the
/// producer is not lock-step with the consumer. With one batch delivered, one
/// more sits in the channel and a third is blocked in `blocking_send`, so the
/// reader runs **up to two batches ahead** and at most three are resident at
/// once. Bounded and small against `MERGE_CHUNK_ROWS`, and nothing like the whole
/// file — but it does mean a read that ends early may have paid for two batches
/// nobody consumed, so a provider's `next()` must be free of side effects it
/// would not want on a cancelled read. That is the whole deviation from strictly
/// demand-driven, and it is what buys the affinity above.
///
/// Per batch, in this order: the batch is evolved to `evolve_to` — the same
/// per-batch projection the object-store read applies, so a served file and a
/// read file are indistinguishable downstream — and then the **drain** counters
/// are tallied into `stats`, post-projection, which is what
/// [`BaseFileProviderStats::bytes_materialized`] documents itself as.
///
/// Errors are forwarded, never swallowed into end-of-stream: a mid-stream
/// provider failure must fail the read rather than silently truncate it into a
/// short success. The producer stops after the first error, so the reader is not
/// polled again.
///
/// The task is not tracked by a `JoinHandle` because the channel already bounds
/// it: dropping the returned stream drops the receiver, the next `blocking_send`
/// fails, and the producer returns. There is no path on which it outlives its
/// consumer by more than one batch OF WORK — but that is a bound on work, not on
/// time. A `next()` already in flight is not cancelled, so a provider blocked on
/// IO keeps the task, and the provider reference it holds, alive until that call
/// returns. `destroy(ctx)` is therefore deferred until then, however long after
/// the caller released both the reader handle and the stream.
///
/// **The provider outlives the reader it returned.** A clone of the provider is
/// moved onto the same task, in a `ServedReader` struct whose field order makes
/// the drop order structural, and is released only when the task ends. That is not
/// bookkeeping: a provider is free to return a reader that borrows its own state
/// — the C-ABI adapter returns an `ArrowArrayStream` whose `get_next`/`release`
/// callbacks point into the provider's `ctx`, and `CApiBaseFileDataProvider`'s
/// `Drop` calls `destroy(ctx)`. Without this, the last strong reference is the
/// one on the FFI reader handle, and that handle is a different object from the
/// stream the C++ caller is draining; nothing in the FFI ownership contract makes
/// the caller free them in an order that keeps `ctx` alive. Holding the clone
/// here makes "the provider outlives every stream it served" true by
/// construction, on every call path, at the cost of one `Arc` clone per served
/// file. Pinned by
/// `a_served_stream_keeps_its_provider_alive_after_the_reader_is_dropped`, which
/// fails if the provider field is removed from `ServedReader`.
///
/// It does occupy a blocking-pool slot for the whole served read rather than for
/// one batch, so the ceiling is concurrently-open file groups, not batches. That
/// is bounded by the caller's split concurrency (~16 for Velox) against tokio's
/// 512-thread default, and the thread is parked on `blocking_send` for most of
/// its life. A caller opening thousands of file groups at once against a serving
/// provider would need to raise `max_blocking_threads`.
fn served_batch_stream(
    reader: Box<dyn arrow_array::RecordBatchReader + Send>,
    evolve_to: SchemaRef,
    stats: Arc<StdMutex<BaseFileProviderStats>>,
    provider: BaseFileDataProviderRef,
) -> BaseBatchStream {
    /// Owns the served reader and the provider that produced it, in that order.
    ///
    /// The order is the point, and a struct is how it is made STRUCTURAL rather
    /// than a property of how rustc happens to order closure captures. Struct
    /// fields drop in declaration order, so `reader` — whose `release` callback
    /// may point into the provider's `ctx` — is always released before the
    /// provider whose `Drop` calls `destroy(ctx)`. That holds on every path,
    /// including the one no test can reach: a task queued and then discarded
    /// without ever running.
    struct ServedReader {
        reader: Box<dyn arrow_array::RecordBatchReader + Send>,
        _provider: BaseFileDataProviderRef,
    }
    let served = ServedReader {
        reader,
        _provider: provider,
    };
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(1);
    // A clone kept OUTSIDE the unwind-catching closure, so a panic still has a
    // channel to report itself on — see the `catch_unwind` note below.
    let panic_tx = tx.clone();
    tokio::task::spawn_blocking(move || {
        // A PANIC here must fail the read, not end it.
        //
        // This is a detached `spawn_blocking` whose `JoinHandle` is dropped, so
        // without this every panic in the producer is captured by tokio's task
        // harness, `tx` drops, and the consumer sees a clean end-of-stream: the
        // query returns a SHORT RESULT and reports success. Not hypothetical —
        // arrow-rs panics when importing a malformed `ArrowArray`/`ArrowSchema`,
        // which is precisely what a buggy C provider hands us, and
        // `project_batch_to_schema` indexes columns. Silent truncation of a query
        // is the worst failure mode this seam has (ISSUES OI-2).
        //
        // `AssertUnwindSafe` is sound here, but not for the reason it first looks:
        // state IS observed afterwards. The captured `stats` slot is the live one
        // `base_file_provider_live_stats()` hands out, and the reader's owner reads
        // it after the panic. It is safe because that slot is ADVISORY and is
        // consistent at every point a panic can occur — each update is a complete
        // `+=` under the guard with nothing fallible between them — and because
        // lock poison is already tolerated below. A future edit that puts a
        // fallible operation under that lock would invalidate this, and nothing
        // would catch it. `served` itself is dropped by the unwind, reader before
        // provider, because the field order still holds.
        let drained = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            // Capture the WHOLE struct, not one field. Since edition 2021 a closure
            // captures disjoint fields, so a body that only ever names `served.reader`
            // captures just that — and `served._provider` would be dropped when this
            // function returns, silently undoing the lifetime extension the struct
            // exists for. Naming the binding itself is what forces the whole value in.
            // `a_served_stream_keeps_its_provider_alive_after_the_reader_is_dropped`
            // catches the mistake; it caught this one.
            let mut served = served;
            for item in served.reader.by_ref() {
                let projected = match item {
                    Ok(batch) => {
                        crate::schema::batch_evolution::project_batch_to_schema(&batch, &evolve_to)
                    }
                    Err(e) => Err(CoreError::ReadFileSliceError(format!(
                        "base-file provider stream failed: {e}"
                    ))),
                };
                if let Ok(batch) = &projected {
                    // Ignore lock poison defensively: the counters are advisory and
                    // must never fail a read.
                    if let Ok(mut slot) = stats.lock() {
                        slot.batches_received += 1;
                        slot.rows_served += batch.num_rows() as u64;
                        slot.bytes_materialized += batch.get_array_memory_size() as u64;
                    }
                }
                let was_err = projected.is_err();
                // `Err` here means the consumer dropped the stream: stop pulling the
                // provider for a read nobody is reading.
                if tx.blocking_send(projected).is_err() || was_err {
                    return;
                }
            }
        }));
        if let Err(payload) = drained {
            // `blocking_send`, NOT `try_send`, and that is load-bearing.
            //
            // The channel has capacity ONE and the producer has usually just
            // filled it, so `try_send` returns `Full` on the common path, the
            // error is dropped, the channel closes and the consumer sees a clean
            // end-of-stream — OI-2 reinstated verbatim, on any read whose consumer
            // is a batch behind. That is not the rare case; it is the normal one
            // for a fast provider feeding a busy engine. Review round 9 built it
            // as mutation 36 and it survived the suite, because the only test
            // consuming this path was `collect()`, the fastest consumer possible.
            //
            // Blocking here parks one blocking-pool thread until the consumer
            // takes the error or goes away. Both terminate: the `Err` result
            // drops the sender either way. `let _` because a consumer that has
            // ALREADY gone away is the one case where losing the panic is
            // correct — nobody is left to tell.
            let _ = panic_tx.blocking_send(Err(CoreError::ReadFileSliceError(format!(
                "base-file provider stream PANICKED: {}. The read is failed rather \
                 than truncated — a panicking provider must not turn into a short \
                 result that reports success",
                panic_message(&*payload)
            ))));
        }
    });
    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed()
}

/// The human-readable half of a `catch_unwind` payload.
///
/// BOTH arms are load-bearing and each needs its own test. `panic!("literal")`
/// yields a `&'static str` — and so do `unwrap()`, `expect(..)`, `assert!(..)`
/// and arrow-rs's own panics in the FFI import path, i.e. exactly the payloads
/// this seam exists for. `panic!("{x}")` yields a `String`. Any other payload
/// type carries nothing renderable, and saying so beats an empty message.
///
/// Review round 9 deleted the `&'static str` arm as "two arms that do the same
/// thing" and the suite stayed green, because the only stub panicked with a
/// FORMATTED message. Every literal panic would have rendered as
/// `<non-string panic payload>` — caught, but not diagnosable.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Why a served stream's schema is not the one the provider was asked for, or
/// `None` if it matches.
///
/// Compares field NAMES and DATA TYPES positionally. Deliberately ignores
/// nullability and schema/field metadata: a provider that widens a non-null
/// column to nullable, or that carries extra key-value metadata through its own
/// transport, is still serving the right data, and declining it would cost a
/// re-read for nothing. A name, a position, a type or a count is the shape the
/// read is about to interpret the buffers as, so any of those differing means
/// the batches are not what was asked for.
///
/// That exemption is TOP-LEVEL ONLY, and the asymmetry is worth stating because
/// the code reads as though it were uniform. Only `name()` and `data_type()` are
/// compared here, so a top-level `nullable` or metadata difference cannot reach
/// the comparison at all. Inside a nested type it can: `DataType`'s own
/// `PartialEq` compares the child `Field`s, and `Field::eq` includes `nullable`
/// and `metadata`. So a `Struct` whose child differs only in nullability IS
/// declined, while the same difference on a flat column is served.
///
/// Left as it stands rather than normalised recursively: the strictness is
/// harmless (a conforming provider echoes the schema it was handed, children
/// included) and loosening it would mean walking every nested `DataType` arm to
/// rebuild it with children normalised, which is a real behaviour change on a
/// path whose whole job is to refuse anything it cannot vouch for. Pinned by
/// `the_shape_checks_nullability_exemption_is_top_level_only`.
///
/// Types are compared EXACTLY, dictionary encoding included: `Dictionary(Int32,
/// Utf8)` where `Utf8` was requested is a different physical layout, and the
/// caller asked for a schema rather than for something convertible to one.
///
/// The field COUNT is compared with `!=`, in BOTH directions, and the `<`
/// direction is the load-bearing one — which is not obvious here, so: `wanted` is
/// the read's `intersection`, and every field in it was read out of THIS file's
/// own footer. None of them can be legitimately absent from a serve of this file.
/// A provider returning FEWER fields is therefore not an old file missing a
/// column — the case `project_batch_to_schema`'s null-fill exists for — it is a
/// provider serving the wrong bytes, and null-filling it is exactly the silent
/// data loss this function prevents. Relaxing `!=` to `>` reads as a sensible
/// tightening and reopens that path; mutation 25 in `MATRIX.md`, killed by
/// `a_served_stream_that_drops_or_reorders_a_column_is_declined`.
///
/// The check is on the reader's DECLARED schema (`RecordBatchReader::schema()`),
/// not on the batches it yields. That is sufficient on the C-ABI path, where the
/// `ArrowArrayStream`'s declared schema governs the import and the batches cannot
/// disagree with it. A Rust-native provider could declare one schema and yield
/// another; nothing here catches that.
fn served_schema_mismatch(
    served: &arrow_schema::Schema,
    wanted: &arrow_schema::Schema,
) -> Option<String> {
    if served.fields().len() != wanted.fields().len() {
        return Some(format!(
            "served {} field(s), wanted {}",
            served.fields().len(),
            wanted.fields().len()
        ));
    }
    for (i, (got, want)) in served
        .fields()
        .iter()
        .zip(wanted.fields().iter())
        .enumerate()
    {
        if got.name() != want.name() {
            return Some(format!(
                "field {i} is named '{}', wanted '{}'",
                got.name(),
                want.name()
            ));
        }
        if got.data_type() != want.data_type() {
            return Some(format!(
                "field {i} ('{}') has type {:?}, wanted {:?}",
                want.name(),
                got.data_type(),
                want.data_type()
            ));
        }
    }
    None
}

/// `schema` without the internal row-position column.
///
/// The column belongs to the base read and the position buffer; it is not the
/// table's, so it must not reach a caller. Every schema derived from a base
/// source's own schema goes through here.
fn without_row_index(schema: SchemaRef) -> SchemaRef {
    if schema
        .column_with_name(ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .is_none()
    {
        return schema;
    }
    Arc::new(arrow_schema::Schema::new(
        schema
            .fields()
            .iter()
            .filter(|f| f.name() != ROW_INDEX_TEMPORARY_COLUMN_NAME)
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

impl HoodieFileGroupReader {
    /// Create a new file group reader.
    ///
    /// Mirrors Java's `HoodieFileGroupReader(readerContext, storage, tablePath,
    /// latestCommitTime, dataSchema, requestedSchema, ...)` constructor.
    ///
    /// The constructor:
    /// 1. Creates a [`FileGroupReaderSchemaHandler`] from `data_schema` +
    ///    `requested_schema` (Java lines 119-121)
    /// 2. Calls `prepare_required_schema()` to compute the `required_schema`
    ///    (Java: automatic in `FileGroupReaderSchemaHandler` constructor, line 105)
    /// 3. Obtains the `output_converter` from the schema handler (Java line 122)
    ///
    /// # Arguments
    /// * `reader_context` — Engine context with merge mode, ordering fields, table config.
    /// * `storage` — Storage layer for reading base files and log files.
    /// * `input_split` — Describes what to read (base file path, log file paths, partition).
    /// * `reader_parameters` — Reader flags (use_record_position, emit_delete, etc.).
    /// * `data_schema` — Full table schema (what columns exist in the files).
    ///   Maps to Java's `dataSchema` / `tableSchema` parameter.
    /// * `requested_schema` — Column projection requested by the caller.
    ///   Maps to Java's `requestedSchema` parameter. `None` means all columns.
    pub fn new(
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        input_split: InputSplit,
        reader_parameters: ReaderParameters,
        data_schema: Option<SchemaRef>,
        requested_schema: Option<SchemaRef>,
    ) -> Result<Self> {
        log::debug!(
            "HoodieFileGroupReader::new partition={} base_file={} log_files={} \
             ordering_fields={:?} latest_commit_time={} record_key_field={}",
            input_split.partition_path,
            input_split.base_file_path.as_deref().unwrap_or("<none>"),
            input_split.log_file_paths.len(),
            reader_context.ordering_field_names(),
            reader_context.latest_commit_time,
            reader_context.record_key_field(),
        );
        if log::log_enabled!(log::Level::Trace) {
            for (i, lf) in input_split.log_file_paths.iter().enumerate() {
                log::trace!("  log_file[{i}]: {lf}");
            }
        }

        // Mirrors Java lines 119-121:
        // readerContext.setSchemaHandler(
        //     new FileGroupReaderSchemaHandler(readerContext, dataSchema, requestedSchema, ...));
        //
        // When schemas are explicitly provided (direct construction / tests), create
        // a new handler. When they are not provided (FFI path via builder), use the
        // handler already on reader_context — which was populated by the FFI bridge
        // from the Avro JSON schemas passed through the Substrait proto.
        let mut schema_handler = if data_schema.is_some() || requested_schema.is_some() {
            let mut handler = FileGroupReaderSchemaHandler::new();
            if let Some(ds) = data_schema {
                handler = handler.with_table_schema(ds.clone()).with_data_schema(ds);
            }
            if let Some(rs) = requested_schema {
                handler = handler.with_requested_schema(rs);
            }
            handler
        } else {
            reader_context.schema_handler.clone()
        };

        // Mirrors Java FileGroupReaderSchemaHandler constructor line 105:
        // this.requiredSchema = prepareRequiredSchema(this.deleteContext);
        //
        // Uses record_key_fields() (all key fields) instead of record_key_field()
        // (single) to support composite record keys in virtual-key mode.
        // Mirrors Java's getMandatoryFieldsForMerging() lines 250-258.
        let has_instant_range = reader_context.instant_range.is_some();
        schema_handler.prepare_required_schema(
            input_split.has_log_files(),
            &reader_context.record_key_fields(),
            reader_context.ordering_field_names(),
            &reader_context.table_config,
            has_instant_range,
            &reader_context.merge_mode,
        )?;

        // Schema-on-read (InternalSchema) evolution is not supported in hudi-rs.
        // Java loads an InternalSchema from the `.schema` folder and
        // applies column renames / type changes through InternalSchema versioning
        // when `hoodie.schema.on.read.enable=true`. hudi-rs only implements
        // schema-on-write backward-compatible evolution, so silently honoring the
        // flag would risk misreading evolved data. Reject it loudly at the same
        // table-config chokepoint as the bootstrap gate below.
        if reader_context
            .table_config
            .get("hoodie.schema.on.read.enable")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            return Err(CoreError::Unsupported(format!(
                "schema-on-read (InternalSchema) is not supported in hudi-rs. \
                 Table at '{}' has hoodie.schema.on.read.enable=true, which requires \
                 InternalSchema-based evolution (column renames / type changes) that \
                 hudi-rs does not implement; only schema-on-write backward-compatible \
                 evolution is supported.",
                reader_context.table_path,
            )));
        }

        // Bootstrap merge reordering is not yet supported in hudi-rs.
        // Java's prepareRequiredSchema() (lines 280-288) partitions fields into
        // meta and data columns and reorders them for bootstrap tables. Until
        // that is implemented, reject bootstrap merge at construction time.
        if reader_context.needs_bootstrap_merge {
            // Reachable via table state (bootstrap base files), so this is a
            // loud error rather than a panic.
            return Err(CoreError::Unsupported(format!(
                "Bootstrap merge is not yet supported in hudi-rs. \
                 Table at '{}' has bootstrap base files that require \
                 meta/data column reordering.",
                reader_context.table_path,
            )));
        }

        // Composite virtual keys ARE supported. With `hoodie.populate.meta.fields=false`
        // and a multi-field recordkey, `RecordContext::record_key_array` reconstructs the
        // full `field:val,field:val` merge key per row (mirroring Java
        // `KeyGenerator.constructRecordKey`) on BOTH the base and log sides, so records
        // sharing the first field but differing on a later one do not collide. See
        // `RecordContext::build_composite_record_key_array`.

        // Multi-field (composite) precombine/ordering keys ARE supported.
        // `RecordContext::new` splits a comma-separated `hoodie.table.precombine.field`
        // / `hoodie.table.ordering.fields` into `ordering_field_names`, and
        // `get_ordering_values` builds one `OrderingValue::Composite` per row from
        // the per-field scalars (compared lexicographically field-by-field, mirroring
        // Java `OrderingValues`). A field absent from a batch, an unsupported field
        // type, or a null component falls back to natural order — matching the scalar
        // path — so there is no silent first-field-only degradation.

        // Mirrors Java line 122:
        // this.outputConverter = readerContext.getSchemaHandler().getOutputConverter();
        let output_converter = schema_handler.get_output_converter();

        // Propagate the prepared schema_handler back onto a new reader_context
        // so downstream consumers (record buffer, log scanner) see the canonical
        // schema_handler with its stored DeleteContext. Mirrors Java's
        // `readerContext.setSchemaHandler(...)` — in Java the reader context is
        // mutable; in Rust we create a new Arc with the updated handler.
        let reader_context = {
            let mut updated = (*reader_context).clone();
            updated.schema_handler = schema_handler.clone();
            Arc::new(updated)
        };

        Ok(Self {
            reader_context,
            storage,
            input_split,
            reader_parameters,
            iterator_mode: IteratorMode::EngineRecord,
            schema_handler,
            record_buffer_loader: DefaultFileGroupRecordBufferLoader::new(),
            output_converter,
            read_stats: HoodieReadStats::default(),
            stream_stats: new_stream_stats_handle(),
            valid_block_instants: Vec::new(),
            buffered_record_converter: None,
            base_file_provider: None,
            base_file_provider_stats: Arc::new(StdMutex::new(BaseFileProviderStats::default())),
            provider_stats_recorded: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Java-parity surface reached only by the test harness, which drives the engine
    /// the way an FFI caller would; `FileGroupReader` goes through `adapter`.
    #[allow(dead_code)]
    /// Create a builder for configuring the reader.
    pub fn builder() -> HoodieFileGroupReaderBuilder {
        HoodieFileGroupReaderBuilder::default()
    }

    // =========================================================================
    // Main read API (mirrors Java's getClosableIterator / getBufferedRecordIterator)
    // =========================================================================

    /// The reader for this slice's base file format.
    ///
    /// Built per call rather than held: the format comes from the reader
    /// context, and constructing one is cheap next to reading a file.
    fn base_file_reader(&self) -> Result<std::sync::Arc<dyn BaseFileReader>> {
        // An unset format means the caller did not say; parquet is the default
        // base file format, and is what every non-metadata table uses here.
        let format = if self.reader_context.base_file_format.is_empty() {
            BaseFileFormatValue::Parquet
        } else {
            BaseFileFormatValue::from_str(&self.reader_context.base_file_format)?
        };
        // The shared factory refuses HFile, which is what keeps the legacy
        // reader from serving it; this reader has its own.
        if matches!(format, BaseFileFormatValue::HFile) {
            return Ok(std::sync::Arc::new(HFileBaseFileReader::new(
                self.storage.clone(),
            )));
        }
        Ok(create_base_file_reader(&self.storage, &format)?)
    }

    /// Open the file group and return the merge stream itself.
    ///
    /// [`Self::open_stream`] erases this into a `BoxStream`, which drops
    /// [`FileGroupMergeStream::current_in_memory_bytes`]. The FFI path needs
    /// that value to publish `hudi_reader_memory_bytes`, so it takes this
    /// entry point instead.
    ///
    /// Single-use, like [`Self::open_stream`]: takes the output converter and,
    /// for MOR, moves the record buffer into the returned stream.
    pub async fn open(&mut self) -> Result<FileGroupMergeStream> {
        // Stage timing (perf harness): opening the base file, PLUS the decode of
        // the first BASE_READ_INITIAL_PREFETCH_BATCHES batches on the
        // object-store leg -- ENG-48159 moved that prefix into this span
        // deliberately. The rest of the per-row-group decode is still paid
        // lazily, inside the merge. Worth stating because a before/after
        // comparison across that change reads as the footer open regressing
        // when the work has only MOVED; see `read_stats.rs`.
        let base = profile_once!(self.read_stats.base_read_us, self.base_file_source().await)?;
        self.init_record_iterators(base).await
    }

    /// Stream the merged output.
    ///
    /// [`Self::read`] returns the whole file group as one batch, so peak memory
    /// tracks the base file. This reads the base file one bounded batch at a
    /// time instead (`MERGE_CHUNK_ROWS` rows), merging and emitting a chunk
    /// per batch.
    ///
    /// Demand-driven: nothing is merged until the consumer asks for it, so the
    /// memory this adds over the merge map is one chunk. The previous shape ran
    /// the merge on a blocking thread behind a depth-1 channel to get the same
    /// bound; a `Stream` has it by construction.
    ///
    /// Single-use: takes the output converter and, for MOR, moves the record
    /// buffer into the returned stream. Reading again needs a new reader.
    pub(crate) async fn open_stream(
        &mut self,
    ) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
        Ok(self.open().await?.into_stream())
    }

    /// Read the file group and return the merged output as a single
    /// `RecordBatch`.
    ///
    /// Same merge as [`Self::open_stream`], collected into one batch. Both
    /// entry points merge the base a batch at a time and therefore return
    /// the same row sequence; this one just concatenates the chunks.
    /// Single-use, like [`Self::open_stream`].
    pub async fn read(&mut self) -> Result<RecordBatch> {
        // Stage timing (perf harness): the open, same as `open_stream` — which
        // since ENG-48159 also carries the decode of the first
        // BASE_READ_INITIAL_PREFETCH_BATCHES batches (see `open`). The REST of
        // the decode still happens lazily while `collect_into_one_batch` drives
        // the stream, so it lands in the merge loop rather than in
        // `base_read_us`.
        let base = profile_once!(self.read_stats.base_read_us, self.base_file_source().await)?;
        let batch = self
            .init_record_iterators(base)
            .await?
            .collect_into_one_batch()
            .await?;
        // The merge accumulated the merge-phase timings + insert/update/delete
        // counts into the shared `stream_stats` while `collect_into_one_batch`
        // drove it to exhaustion. Drain them back into `self.read_stats` so
        // `read_stats()`-based callers (fg-bench, tests, reader_v1) observe them.
        self.drain_stream_stats();
        self.snapshot_provider_stats();
        Ok(batch)
    }

    /// Copy the accumulated streaming stage-stats into [`Self::read_stats`].
    /// Called by [`Self::read`] after the iterator is exhausted.
    fn drain_stream_stats(&mut self) {
        let s = self
            .stream_stats
            .lock()
            .expect("stream_stats mutex poisoned");
        self.read_stats.final_merge_us = s.final_merge_us;
        self.read_stats.output_build_us = s.output_build_us;
        self.read_stats.merge_map_peak_entries = s.merge_map_peak_entries;
        self.read_stats.num_inserts = s.num_inserts;
        self.read_stats.num_updates = s.num_updates;
        self.read_stats.num_deletes = s.num_deletes;
    }

    /// Initialize record iterators: read base file + scan/merge log files,
    /// hand state to a [`FileGroupMergeStream`].
    ///
    /// Mirrors Java's `HoodieFileGroupReader.initRecordIterators()`. The
    /// fast path (no log files = CoW / empty) returns an `Eager` iterator
    /// over the base file source; the MOR path returns a `Buffered`
    /// iterator that drives `buffer.has_next() / buffer.next()` in chunks.
    ///
    /// ```text
    /// initRecordIterators()
    ///   └─ recordBufferLoader.getRecordBuffer(...)
    ///        → FileGroupMergeStream::new_buffered(...)
    /// ```
    async fn init_record_iterators(&mut self, base: BaseSource) -> Result<FileGroupMergeStream> {
        log::trace!(
            "[HoodieFileGroupReader] initRecordIterators: partition={} base_file={} log_files={}",
            self.input_split.partition_path,
            self.input_split
                .base_file_path
                .as_deref()
                .unwrap_or("<none>"),
            self.input_split.log_file_paths.len(),
        );

        let BaseSource {
            schema: base_source_schema,
            batches: base_source,
        } = base;
        log::trace!(
            "[HoodieFileGroupReader] base file source: schema_cols={}",
            base_source_schema.fields().len(),
        );

        // The post-projection output schema is the same regardless of
        // CoW vs MOR — it is the schema every emitted chunk carries.
        let output_converter = self.output_converter.take();
        let post_projection_schema = output_converter.as_ref().map(|c| c.target_schema());

        // Step 2: If no records to merge (no log files), build an Eager
        // iterator that yields the base file batches directly.
        if self.input_split.is_base_only() {
            log::trace!("[HoodieFileGroupReader] no log files → Eager iterator");

            // The schema travels with the source, so it is known without
            // forcing a row-group decode. A log-only file group's source is
            // empty and carries the required schema.
            let merge_schema: SchemaRef = if let Some(rs) = &self.schema_handler.required_schema {
                rs.clone()
            } else {
                base_source_schema.clone()
            };

            let output_schema = post_projection_schema.unwrap_or(merge_schema);
            // Stage timing (perf harness): the Eager iterator accumulates
            // per-chunk output_build_us (concat is gone — each base batch flows
            // through the converter as its own chunk) into `stream_stats`, which
            // `read()` drains back into `self.read_stats`.
            return Ok(FileGroupMergeStream::new_eager(
                base_source,
                output_schema,
                output_converter,
                self.stream_stats.clone(),
            ));
        }

        // Step 3: MOR path — load record buffer (scan log files + create buffer).
        // Mirrors Java: this.recordBuffer = recordBufferLoader.getRecordBuffer(...).getLeft();
        log::trace!(
            "[HoodieFileGroupReader] scanning {} log file(s) with latest_commit_time={}",
            self.input_split.log_file_paths.len(),
            self.reader_context.latest_commit_time,
        );
        let load_result = self
            .record_buffer_loader
            .get_record_buffer(
                self.reader_context.clone(),
                self.storage.clone(),
                &self.input_split,
                &self.reader_parameters,
                &mut self.read_stats,
            )
            .await?;

        let record_buffer = load_result.record_buffer;
        self.valid_block_instants = load_result.valid_block_instants;

        // Anything this read expects that is quietly not done, said once, before
        // the rows come back looking unremarkable. Reported here rather than on
        // entry for two reasons: the scan has finished, so a position merge that
        // gave up partway through is visible (the buffer flips its own type when
        // it falls back, and nothing else records it); and every entry point goes
        // through here, so the streaming read is covered too — reporting from
        // `read()` alone left the streaming entry point silent.
        crate::file_group::reader_v2::gaps::report_for_read(
            &self.reader_context,
            &self.reader_parameters,
            self.use_record_position(),
            record_buffer.get_buffer_type() == BufferType::PositionBasedMerge,
        );

        log::debug!(
            "[HoodieFileGroupReader] log scan complete: buffer_size={} valid_instants={:?} \
             stats: log_blocks={} log_records={} corrupt={} rollbacks={}",
            record_buffer.size(),
            self.valid_block_instants,
            self.read_stats.total_log_blocks,
            self.read_stats.total_log_records,
            self.read_stats.total_corrupt_log_blocks,
            self.read_stats.total_rollback_blocks,
        );

        // Step 4: Determine merge_schema. The base source's schema travels
        // with it, so this needs no row-group decode.
        let merge_schema: SchemaRef = if let Some(rs) = &self.schema_handler.required_schema {
            rs.clone()
        } else if self.input_split.base_file_path.is_some() {
            // The base source's schema is the parquet schema after projection,
            // plus the row-position column when merging by position — which is
            // the reader's own and never an output column.
            without_row_index(base_source_schema.clone())
        } else {
            // Log-only file group: peek at any non-delete log record's batch
            // (HashMap order is non-deterministic, so we must search all
            // entries — the first record could be a delete).
            // Find the first non-delete record's schema (`get_record()` returns
            // `None` for a delete tombstone).
            let mut schema = None;
            for r in record_buffer.get_log_records().values() {
                if let Some(batch) = r.get_record() {
                    schema = Some(batch.schema());
                    break;
                }
            }
            schema.ok_or_else(|| {
                CoreError::ReadFileSliceError("No schema available for merge output".to_string())
            })?
        };

        let output_schema = post_projection_schema.unwrap_or_else(|| merge_schema.clone());

        // Step 5: return the streaming iterator, which owns both the buffer and
        // the base source from here on; the reader's role ends. The source is
        // the iterator's rather than the buffer's because only its holder can
        // say when the base is exhausted, and because the base file is the one
        // part of the merge that has to be read rather than computed.
        log::trace!("[HoodieFileGroupReader] returning Buffered iterator");

        // Step 6: Hand the buffer to a Buffered streaming iterator. The
        // iterator owns the buffer and drives `has_next/next` per chunk; it
        // accumulates final_merge_us + output_build_us and the update-processor
        // insert/update/delete counts into the shared `stream_stats`, which
        // `read()` drains back into `self.read_stats` after the stream is
        // exhausted (mirrors Java, where StandardUpdateProcessor increments
        // HoodieReadStats during iteration). merge_map_peak_entries was already
        // recorded during the log scan; the iterator reads it off the buffer up
        // front (the buffer is moved into the iterator here).
        self.stream_stats
            .lock()
            .expect("stream_stats mutex poisoned")
            .merge_map_peak_entries = record_buffer.merge_map_peak_entries();

        Ok(FileGroupMergeStream::new_buffered(
            record_buffer,
            base_source,
            merge_schema,
            output_schema,
            output_converter,
            self.stream_stats.clone(),
        ))
    }

    /// Is it safe to push the predicate into the BASE read of this split?
    ///
    /// Safe exactly when no log merge can change the predicate's outcome. Two
    /// ways that holds:
    ///   - the split carries no log files, so nothing merges and the base rows
    ///     are final; or
    ///   - the predicate references only record-key columns, which are immutable
    ///     across upserts, so the outcome survives the merge (`mor_pk_safe`).
    ///
    /// Mirrors Java's `SparkFileFormatInternalRowReaderContext
    /// .getSchemaAndFiltersForRead`, which branches on `getHasLogFiles()` and
    /// never on the table type: `allFilters` when there are no log files,
    /// `morFilters` when there are. The table type does not appear here either —
    /// a CoW slice has no log files, so it takes the first branch on its own.
    ///
    /// # Why the split and not `ReaderContext`
    ///
    /// [`ReaderContext::has_log_files`] is a different fact with a different
    /// source: it is set by whoever built the context, whereas
    /// [`InputSplit::log_file_paths`] is the split's own file list. Nothing
    /// derives one from the other, so gating on the context flag would rest a
    /// safety decision on a caller-supplied boolean. If it were ever false for a
    /// slice that does have logs, a non-PK predicate would reach the base read
    /// and drop rows the merge would have updated — silently, because a filter
    /// above the reader can only remove rows, never restore them.
    ///
    /// Using the split also keeps this in lock-step with
    /// [`Self::use_record_position`], which reads the same
    /// `input_split.has_log_files()`. The gate and the merge therefore cannot
    /// disagree about a split.
    ///
    /// # Why there is no bootstrap term, unlike Java
    ///
    /// Java has a third branch — `!getHasLogFiles && hasRowIndexField` selects
    /// `bootstrapSafeFilters` — because a bootstrap read pairs skeleton and data
    /// files by row position, and filters that physically drop records misalign
    /// that pairing. `has_bootstrap_base_file` reaches `ReaderContext` here and
    /// is consulted nowhere, so a bootstrap slice with
    /// `needs_bootstrap_merge == false` does arrive at this gate. It is still
    /// safe, for two independent reasons:
    ///
    /// 1. The positional mechanism here is the virtual `RowNumber` column, which
    ///    carries each row's TRUE physical position and stays correct under
    ///    row-group selection and `RowFilter` pushdown. Dropping rows cannot
    ///    shift it, which is exactly the failure Java's tier avoids by not
    ///    pushing.
    /// 2. The row-index column is requested only from
    ///    [`Self::use_record_position`], which returns false when the split has
    ///    no log files — so on the branch this gate widens, there is no
    ///    positional pairing to misalign at all.
    ///
    /// Anyone adding a bootstrap term should re-check both: lifting the
    /// `needs_bootstrap_merge` rejection in `new()` without revisiting this gate
    /// is how the Java hazard would arrive here.
    fn base_read_pushdown_is_safe(&self) -> bool {
        !self.input_split.has_log_files() || self.reader_context.mor_pk_safe
    }

    /// Whether this read should merge base + log records by base-file row
    /// position (rather than by record key). Mirrors Java
    /// `HoodieFileGroupReader`'s `setShouldMergeUseRecordPosition`:
    /// `useRecordPosition && !skipMerge && hasLogFiles && parquetBaseFile`.
    ///
    /// When true, the base file is read with a synthetic row-index column (see
    /// [`ROW_INDEX_TEMPORARY_COLUMN_NAME`]) so the position buffer can match
    /// base rows to log records by position.
    fn use_record_position(&self) -> bool {
        if !self.reader_parameters.use_record_position {
            return false;
        }
        if !self.input_split.has_log_files() || self.input_split.base_file_path.is_none() {
            return false;
        }
        // Position merge needs the base file's commit time to validate log-block
        // position headers. Without it the loader falls back to key-based, so the
        // base read must not attach the row-index column either (keep the two
        // decisions in lock-step).
        if self.input_split.base_file_commit_time.is_none() {
            return false;
        }
        let is_skip_merge = self
            .reader_context
            .hoodie_reader_config
            .get(crate::file_group::reader_v2::reader_context::CONFIG_MERGE_TYPE)
            .map(|v| v.eq_ignore_ascii_case("skip_merge"))
            .unwrap_or(false);
        if is_skip_merge {
            return false;
        }
        // hudi-rs only reads parquet base files; guard defensively when the
        // format is explicitly set to something else. Shared with the loader's
        // buffer-selection gate so the row-index attachment and the buffer
        // choice cannot diverge.
        crate::file_group::reader_v2::buffer::loader::base_file_is_parquet(
            &self.reader_context.base_file_format,
        )
    }

    /// Open the base file as a stream of batches, with the schema they carry.
    ///
    /// One shape for every caller: the base file is read asynchronously, one
    /// bounded batch at a time, and the whole file is never resident. A caller
    /// that needs it as a single batch collapses it afterwards (`read()` does,
    /// via `collect_into_one_batch`) — that is a choice about chunking, not
    /// about what is safe to call from where.
    ///
    /// ⚠️ **Not quite fully lazy, since ENG-48159.** On the object-store leg this
    /// function awaits the first [`BASE_READ_INITIAL_PREFETCH_BATCHES`] batches
    /// before it returns — see that constant for why. Residency is unaffected (two
    /// batches, not the file), but the timing is not: on the FFI path the prefix is
    /// paid on the thread Velox prepared the split on, which is the point. On every
    /// OTHER path — Rust `read()`/`open_stream()`, datafusion, python — there is no
    /// preload thread, so it is pure reordering, and an **abandoned** read (a
    /// `LIMIT`, an early `?`, or a MOR read whose log scan then fails in
    /// `init_record_iterators`) now pays for up to two batches it discards where it
    /// previously paid for none. Bounded and small, but it is a real behaviour
    /// change on the majority path and this is the function every caller reads.
    ///
    /// Returns an empty stream when the input split has no base file (log-only
    /// file group), and when the instant range excludes this base file: the
    /// range is a per-file decision, so it is settled here rather than by
    /// reading the file and discarding its rows.
    ///
    /// Mirrors Java's `HoodieFileGroupReader.makeBaseFileIterator()`.
    async fn base_file_source(&mut self) -> Result<BaseSource> {
        let Some(path) = self.input_split.base_file_path.clone() else {
            // Log-only file group — empty base. Use the required_schema
            // as the reported schema when available; otherwise an empty
            // schema (the buffer's reader_schema fallback handles schema
            // selection downstream).
            let schema = self
                .schema_handler
                .required_schema
                .clone()
                .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
            return Ok(BaseSource::empty(schema));
        };

        if self.buffered_record_converter.is_none() {
            log::trace!(
                "[HoodieFileGroupReader] base_file_source: no bufferedRecordConverter set \
                 (batch-level read does not require per-record conversion)"
            );
        }

        // gate parquet RowFilter pushdown on whether this read MERGES.
        //   No log files on this split: always safe (nothing merges). A CoW
        //        slice reaches the gate this way.
        //   Log files present: safe ONLY when every column referenced by the
        //        filter is a primary key (PKs are immutable across upserts, so
        //        the predicate outcome doesn't change post-merge —
        //        `reader_context.mor_pk_safe`, mirroring Java's
        //        `filterIsSafeForPrimaryKey`).
        //   Otherwise: drop the filter; the post-merge filter (Velox/Spark above
        //        the FG reader) evaluates the predicate after base+log merge.
        // ONE gate, bound once and shared by both mechanisms. Bound to a local
        // rather than called twice so the sharing is structural: an edit that
        // changes the condition for one can no longer leave the other behind,
        // and pruning is the one that must not be left behind — it drops rows
        // before the merge can see them.
        let pushdown_is_safe = self.base_read_pushdown_is_safe();
        let mut row_filter = if pushdown_is_safe {
            self.reader_context.row_filter_builder.clone()
        } else {
            if self.reader_context.row_filter_builder.is_some() {
                log::debug!(
                    "merging read with a non-PK predicate — skipping parquet \
                     RowFilter pushdown for base file '{path}' \
                     (post-merge filter still runs)"
                );
            }
            None
        };
        let mut row_group_selector = if pushdown_is_safe {
            self.reader_context.row_group_selector.clone()
        } else {
            // Record the suppression. The gate and the selector are each correct
            // alone; what does not compose is the observability.
            // `row_group_selector_calls` exists to separate "ran and found
            // nothing" from "never installed", and a selector the gate refuses
            // is a third state that also reads zero calls. Counting it here
            // keeps that counter answerable.
            if self.reader_context.row_group_selector.is_some() {
                self.storage.read_volume().record_selector_suppressed();
                log::debug!(
                    "merging read with a non-PK predicate — skipping row-group \
                     pruning for base file '{path}' (post-merge filter still runs)"
                );
            }
            None
        };

        // The key predicate needs no such gate. It narrows *which blocks are read*
        // and the reader filters the records it brings back, so it cannot change the
        // merge's outcome the way a non-primary-key row filter can — and a format
        // that cannot seek ignores it and returns every row.
        let key_predicate = self.reader_context.key_predicate.clone();

        // Position-based merge: ask the base read for a synthetic row-index
        // column carrying each row's TRUE physical base-file position (a parquet
        // virtual RowNumber column — correct even under RowFilter pushdown). It
        // is kept on the base source so the position buffer can match base rows
        // to log records, then dropped by the buffer when it reconciles each
        // batch to the merge schema. The column is NOT added to
        // `required_schema`/`merge_schema` — only to the base source's physical
        // schema (`base_read_schema` = required + row-index).
        let use_position = self.use_record_position();

        // No projection schema → fall back to the unprojected helper (rare; FFI
        // always supplies a required_schema). It reads the file as one batch,
        // because its schema is only known once the file has been read, so the
        // instant-range decision below cannot be made before reading it.
        //
        // This branch returns before the projected path's footer read, so it runs
        // the repair gate ITSELF, through the same three helpers — it used to run
        // it not at all, and a #18132-mislabelled file kept a pushdown here that
        // drops rows which match (ISSUES OI-11). It was latent rather than live:
        // no FFI caller reaches this path, because the FFI always supplies a
        // required schema. "Unreachable from the surface that ships today" is a
        // property of the callers, not of this function, and the guard is
        // cheaper than the argument.
        //
        // The footer read is gated on `repair_gate_is_armed`, which the common
        // scan answers `false` to — so the ordinary read pays nothing, and the
        // scan that does pay reads a footer the `read_data` below reads anyway
        // (and which `parquet_schema_cache` has usually already served).
        let Some(required_schema) = self.schema_handler.required_schema.clone() else {
            if self.repair_gate_is_armed(pushdown_is_safe) {
                let file_schema = self
                    .base_file_reader()?
                    .read_schema(
                        &path,
                        base_read_options(
                            row_filter.clone(),
                            row_group_selector.clone(),
                            key_predicate.clone(),
                            self.schema_handler.reader_schema_json.clone(),
                            use_position,
                        ),
                    )
                    .await
                    .map_err(|e| {
                        CoreError::ReadFileSliceError(format!(
                            "Failed to read base file footer schema '{path}': {e:?}"
                        ))
                    })?;
                let repair_conflict = self.repair_conflict_from(
                    &file_schema,
                    self.schema_handler.table_schema.as_deref(),
                    pushdown_is_safe,
                )?;
                self.withdraw_pushdown_for_repair(
                    &path,
                    &repair_conflict,
                    &mut row_filter,
                    &mut row_group_selector,
                );
            }
            let batch = self
                .base_file_reader()?
                .read_data(
                    &path,
                    base_read_options(
                        row_filter.clone(),
                        row_group_selector.clone(),
                        key_predicate.clone(),
                        self.schema_handler.reader_schema_json.clone(),
                        use_position,
                    ),
                )
                .await
                .map_err(|e| {
                    CoreError::ReadFileSliceError(format!(
                        "Failed to read base file '{path}': {e:?}"
                    ))
                })?;
            let schema = batch.schema();
            if !self.base_file_in_range()? {
                return Ok(BaseSource::empty(schema));
            }
            return Ok(BaseSource {
                schema: schema.clone(),
                batches: futures::stream::once(async move { Ok(batch) }).boxed(),
            });
        };

        // Schema-evolution intersection (Java parity:
        // HoodieParquetFileFormatHelper.buildImplicitSchemaChangeInfo):
        //   1. diff footer schema vs required by name;
        //   2. ask parquet only for the INTERSECTION (in the file's own types);
        //   3. project to required per batch: null-fill added columns, cast
        //      promotions (float→double string-mediated so it is value-exact).
        // Step 3 is applied PER ROW-GROUP, so every base batch the merge
        // interleaves is already in `required_schema`.

        // The options the read below will use: with an Avro reader schema the
        // HFile reader answers with the RESOLVED schema, which is what its
        // batches will carry, so the intersection is taken against that and not
        // against a writer schema the read never produces.
        // ENG-48206 / OSS #748 — `row_filter` and `row_group_selector` are WITHDRAWN
        // below, once this file's footer schema shows a value-reinterpreting repair.
        // They are therefore passed per call instead of captured: a closure that
        // captured them would borrow across that assignment (E0506) and, worse,
        // would have pinned the pre-withdrawal values for the stream read — i.e.
        // it would have pushed the very filter the gate just decided to withdraw.
        // Upstream has no closure here and calls `base_read_options` directly at
        // both sites; this keeps 145's de-duplication with upstream's semantics.
        let read_options =
            |row_filter: Option<RowFilterBuilder>, row_group_selector: Option<RowGroupSelector>| {
                base_read_options(
                    row_filter,
                    row_group_selector,
                    key_predicate.clone(),
                    self.schema_handler.reader_schema_json.clone(),
                    use_position,
                )
            };
        let file_schema = self
            .base_file_reader()?
            .read_schema(
                &path,
                read_options(row_filter.clone(), row_group_selector.clone()),
            )
            .await
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Failed to read base file footer schema '{path}': {e:?}"
                ))
            })?;
        // Intersection by *case-insensitive* name (Java/Spark resolve field names
        // case-insensitively). Project under the FILE's actual name+type so the
        // parquet reader finds the column; `project_batch_to_schema` (also
        // case-insensitive) then evolves each batch to `required_schema`. A
        // required column absent from the footer is skipped here and null-filled
        // downstream; an ambiguous footer case-collision errors loudly.
        let mut present: Vec<arrow_schema::FieldRef> =
            Vec::with_capacity(required_schema.fields().len());
        for rf in required_schema.fields() {
            if let Some(idx) = crate::schema::batch_evolution::index_of_ci(&file_schema, rf.name())?
            {
                present.push(file_schema.fields()[idx].clone());
            }
        }
        let present_len = present.len();
        let intersection: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(present));
        log::debug!(
            "[base-file-evolution] path={} file_cols={} required_cols={} intersect_cols={}",
            path,
            file_schema.fields().len(),
            required_schema.fields().len(),
            present_len
        );

        // Parquet evaluates a pushed predicate against the file's PHYSICAL values,
        // before `project_batch_to_schema` runs. Sound only while a physical value
        // means what its physical type says, which the apache/hudi#18132 repair
        // breaks: the file labels a tz-aware column micros while the stored i64 is
        // MILLIS, so a millis-semantics literal reads those rows as 1970 and the
        // filter drops rows that match. The post-scan filter cannot restore them.
        //
        // Two gates, cheapest first. `repair_risk_columns` was decided ONCE per scan
        // from the table schema and the predicate's own referenced columns, and is
        // empty unless the predicate touches a tz-aware millis column — so the
        // common scan never reaches the footer comparison below and never loses
        // pushdown. The footer schema itself is already fetched unconditionally
        // above, so gate 1 buys predicate scoping and the per-file name walk, not
        // avoided IO.
        //
        // The table side is `table_schema`, NOT `required_schema`: a filter column
        // absent from the projection is still decoded and still misread, because a
        // `RowFilter` builder derives its own `ProjectionMask` from the parquet
        // schema rather than from `intersection`.
        //
        // With no table schema, `required_schema` may stand in — but ONLY when it
        // carries every repair-risk column.
        //
        // It used to stand in unconditionally, which silently inverted the rule the
        // paragraph above states. `reinterpreted_columns` skips any candidate missing
        // from either side, so a risk column pruned out of the projection produced an
        // EMPTY conflict — the gate answering "nothing to repair" about a file it had
        // simply not been shown the column of, and the row filter surviving on exactly
        // the mislabelled file it exists to disarm.
        //
        // Withdrawing whenever the table schema is absent would close that, and is
        // what an earlier cut of this did, but it is too blunt: `cpp/` leaves
        // `table_schema` unset whenever `data_schema` is absent or unparseable, so
        // every such read would lose pushdown and full-scan even on an honestly
        // labelled file. The coverage test is the precise line — when the projection
        // carries all the risk columns it IS a sound table side for the only
        // comparison the gate makes, and when it does not, there is no answer and the
        // fail-safe arm is correct.
        //
        // Pinned in both directions by
        // `an_out_of_projection_filter_column_withdraws_pushdown_with_no_table_schema`
        // (missing risk column, must withdraw) and
        // `base_read_keeps_pushdown_when_the_file_is_honestly_labelled` (covered risk
        // column, must keep).
        let table_side = match self.schema_handler.table_schema.as_deref() {
            Some(table) => Some(table),
            None => {
                let mut covers_every_risk_column = true;
                for col in &self.reader_context.repair_risk_columns {
                    if crate::schema::batch_evolution::index_of_ci(&required_schema, col)?.is_none()
                    {
                        covers_every_risk_column = false;
                        break;
                    }
                }
                covers_every_risk_column.then(|| required_schema.as_ref())
            }
        };
        let repair_conflict =
            self.repair_conflict_from(&file_schema, table_side, pushdown_is_safe)?;

        // ONE verdict, THREE consumers — but reaching them by two mechanisms, and
        // the difference matters to anyone editing this.
        //
        // The injected provider's `can_push_predicate` reads this rebinding
        // DIRECTLY, so it is narrowed by construction. The row filter and the
        // row-group selector were bound from the un-narrowed value above and are
        // cleared imperatively by the block below — which cannot simply key on
        // `!pushdown_is_safe`. Not because the clearing would be wrong (it is
        // idempotent; they were already `None` from their binding) but because
        // the block also RECORDS `pushdown_suppressed_by_repair`. Keying it on
        // the narrowed value would fire that counter on every MERGE-gate refusal
        // too, and the counter exists precisely to separate the two causes:
        // `record_selector_suppressed` already speaks for the merge gate at its
        // own binding. Pinned by
        // `a_selector_the_gate_refuses_is_counted_not_silently_dropped`, which
        // asserts the repair counter stays at zero for a merge-gate refusal —
        // the only test in this file that fails under that mis-keying.
        //
        // The three verdicts agree; only one of them is structurally unable to be
        // left behind. A fourth consumer added later should read the binding,
        // not the block.
        //
        // The provider is the consumer that would otherwise be left behind, and
        // nothing would have said so: it arrived on a branch that forked BEFORE
        // this gate existed (`2ba0dbd`), so the merge that brought it in was
        // textually clean and touched no call site the repair work had ever seen.
        // A provider told it may push applies the predicate to the file's own
        // physical values and drops the same rows the in-process `RowFilter`
        // would have — rows the post-merge filter cannot restore. Withdrawing
        // only the in-process filter would leave the FFI reader exposed on
        // exactly the files this guard exists for.
        //
        // Rebound rather than folded into the `if` below because the value, not
        // the branch, is what the provider request reads: it is the same
        // narrowing internal `reader/mod.rs` applies, expressed for this tree.
        let pushdown_is_safe = pushdown_is_safe && repair_conflict.is_empty();
        self.withdraw_pushdown_for_repair(
            &path,
            &repair_conflict,
            &mut row_filter,
            &mut row_group_selector,
        );

        let base_read_schema: SchemaRef = if use_position {
            let mut fields: Vec<arrow_schema::FieldRef> =
                required_schema.fields().iter().cloned().collect();
            fields.push(Arc::new(arrow_schema::Field::new(
                ROW_INDEX_TEMPORARY_COLUMN_NAME,
                arrow_schema::DataType::Int64,
                false,
            )));
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            required_schema.clone()
        };

        // The instant range excludes whole base files, and the decision needs
        // only the file's commit instant, so it is made before opening rather
        // than by reading every row and dropping them.
        if !self.base_file_in_range()? {
            return Ok(BaseSource::empty(base_read_schema));
        }

        // ── Injected base-file data provider (base file only) ───────────────
        // Offer the base file to an injected provider before the object-store
        // read. Skipped under position-based merge, which needs the synthetic
        // row-index column a provider does not supply (there `base_read_schema`
        // = required + row-index, while a provider returns only projected data
        // columns). Served batches arrive at the `intersection` schema — the
        // same shape the object-store read below produces — and are evolved to
        // `base_read_schema` per batch, exactly like that read.
        //
        // Offered only after `base_file_in_range` has kept the file: a file the
        // instant range excludes contributes no rows either way, and asking a
        // provider for it would be a round-trip for nothing.
        //
        // `None`, or no provider injected, falls through to the unchanged read.
        //
        // `can_push_predicate` below is the REPAIR-NARROWED verdict, not the
        // merge gate: `pushdown_is_safe` was rebound above, after the footer read
        // and before this request, which is the only ordering on which the
        // provider can see the same decision the in-process `RowFilter` got for
        // this file.
        // A provider can only be served from inside a tokio runtime, because
        // `served_batch_stream` hands the reader to `spawn_blocking`. Both
        // `HoodieFileGroupReader` and `with_base_file_provider` are `pub`, so a
        // downstream crate can inject a provider and then drive this future on a
        // non-tokio executor — and `spawn_blocking` PANICS with no runtime, deep
        // inside a stream the caller did not write (ISSUES OI-22).
        //
        // Decline the provider instead. Falling back to the object-store read is
        // the same degradation an unimportable stream or a wrong schema gets and
        // produces the right answer — but note it is NOT counted: this check
        // short-circuits the whole provider block, so `record_provider_stats`
        // never runs and every counter stays zero, which is indistinguishable
        // from "no provider was injected". The `warn!` below is the only signal,
        // and `off_a_tokio_runtime_the_provider_is_declined_rather_than_panicking`
        // pins that (0, 0). Deliberate: a `storage_fallbacks` bump here would
        // read as "the provider declined this file" when in fact the provider was
        // never asked. Checked HERE rather than in
        // `served_batch_stream` because by then the provider has already done the
        // work of serving the file.
        //
        // Costs one `Handle::try_current()` per base file on the path that has a
        // provider at all; the common read has none and never evaluates it.
        if !use_position
            && let Some(provider) = self.base_file_provider.clone()
            && self.provider_is_usable_here(&path)
        {
            let file_uri = join_url_segments(&self.storage.base_url, &[path.as_str()])
                .map(|u| u.to_string())
                .unwrap_or_else(|_| path.clone());
            let partition_fields = self.partition_fields();
            let (outcome, mut stats) = provider
                .try_base_file(BaseFileDataRequest {
                    file_uri: &file_uri,
                    projected_schema: &intersection,
                    can_push_predicate: pushdown_is_safe,
                    partition_path: &self.input_split.partition_path,
                    partition_fields: &partition_fields,
                    data_schema: self.schema_handler.data_schema.as_ref(),
                })
                .await;

            // A served stream is TRUSTED for values and CHECKED for shape.
            //
            // `project_batch_to_schema` resolves the served batch against
            // `base_read_schema` BY NAME, and null-fills a name it does not find —
            // which is right for a column genuinely absent from an older file, and
            // is exactly what makes a wrong serve invisible. Every field in
            // `intersection` was read out of THIS file's footer, so all of them
            // are present; a provider that renames, reorders, retypes or drops one
            // is not serving an evolved file, it is serving the wrong bytes, and
            // without this check the read succeeds with a column of nulls where
            // the data was.
            //
            // Declining is the safe degradation and matches the unimportable-stream
            // path in `cpp/src/provider_abi.rs`: the attempt is reclassified from a
            // serve to a storage fallback and the object-store read below produces
            // the right answer. The cost of a false decline is one re-read; the
            // cost of a false accept is silent data loss.
            let outcome = match outcome {
                Some(served) => {
                    match served_schema_mismatch(served.schema().as_ref(), &intersection) {
                        None => Some(served),
                        Some(why) => {
                            log::error!(
                                "[HoodieFileGroupReader] base-file provider served \
                                 '{path}' at the WRONG schema and is being declined: \
                                 {why}. Falling back to the object-store read. A served \
                                 stream must match `projected_schema` field for field, \
                                 in order"
                            );
                            drop(served);
                            // The SAME reclassification `cpp/src/provider_abi.rs`
                            // performs when a served stream cannot be imported —
                            // including the drain counters, which that path zeroes
                            // and this one did not. The trait is public, so a
                            // Rust-native provider that fills them against contract
                            // would otherwise have them folded into the live slot
                            // by `record_provider_stats` below, attributed to a
                            // file nothing ever drained.
                            stats.files_served = stats.files_served.saturating_sub(1);
                            stats.storage_fallbacks += 1;
                            stats.rows_served = 0;
                            stats.bytes_materialized = 0;
                            stats.batches_received = 0;
                            None
                        }
                    }
                }
                None => None,
            };

            // Seed the shared slot with the setup counters on BOTH outcomes: a
            // fallback still reports its discover/connect timings, and
            // `storage_fallbacks` is the counter that makes a silent
            // fall-through visible.
            self.record_provider_stats(&stats);
            if let Some(served) = outcome {
                // trace, not debug: one line per base file per read, matching
                // the level the internal reader logs these at.
                log::trace!(
                    "[HoodieFileGroupReader] base-file provider served '{path}' \
                     ({} intersect cols, streaming)",
                    intersection.fields().len()
                );
                return Ok(BaseSource {
                    schema: base_read_schema.clone(),
                    batches: served_batch_stream(
                        served,
                        base_read_schema,
                        self.base_file_provider_stats.clone(),
                        provider.clone(),
                    ),
                });
            }
            log::trace!(
                "[HoodieFileGroupReader] base-file provider declined '{path}' — \
                 falling through to the object-store read"
            );
        }

        // Open the base file as a stream. The whole file never lives in memory;
        // one batch does. The gated RowFilter and row-group selector are both
        // threaded through the intersection read. Only the SELECTOR skips IO: a
        // RowFilter decides per row once the predicate columns are decoded. The
        // filter builder resolves predicate columns by name and returns None when
        // any referenced column is absent — safe even for evolved/added cols.
        let base_stream = self
            .base_file_reader()?
            .read_stream(
                &path,
                read_options(row_filter.clone(), row_group_selector.clone())
                    .with_projection(intersection.fields().iter().map(|f| f.name())),
            )
            .await
            .map_err(|e| {
                CoreError::ReadFileSliceError(format!(
                    "Failed to open base file stream '{path}': {e:?}"
                ))
            })?;

        let evolve_to = base_read_schema.clone();
        let evolved = futures::StreamExt::map(base_stream.into_stream(), move |b| match b {
            Ok(batch) => {
                crate::schema::batch_evolution::project_batch_to_schema(&batch, &evolve_to)
            }
            Err(e) => Err(CoreError::from(e)),
        });

        // ENG-48159 — pay for a bounded prefix HERE, while still on the thread
        // Velox prepared the split on. This body already runs inside
        // `OBJECT_STORE_RUNTIME.block_on(reader.open())`, so the `.await` is
        // mandatory: driving the prefetch through a synchronous adapter's
        // `next()` would be a runtime re-entry panic across the FFI boundary.
        // The provider leg above needs none of this — `served_batch_stream`
        // already runs up to two batches ahead over its depth-1 channel.
        let batches =
            prefetch_initial_batches(evolved.boxed(), BASE_READ_INITIAL_PREFETCH_BATCHES).await;

        Ok(BaseSource {
            schema: base_read_schema,
            batches,
        })
    }

    /// The table's partition field names, in `hoodie.table.partition.fields`
    /// order. Empty for a non-partitioned table, or when the config is absent.
    ///
    /// Only the provider seam needs these: they are how a provider maps the
    /// split's `partition_path` back onto typed partition columns.
    fn partition_fields(&self) -> Vec<String> {
        self.reader_context
            .table_config
            .get(HudiTableConfig::PartitionFields.as_ref())
            .map(|fields| {
                fields
                    .split(',')
                    .map(|field| field.trim().to_string())
                    .filter(|field| !field.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Can an injected provider actually be used on the executor polling us?
    ///
    /// `served_batch_stream` moves the served reader onto `spawn_blocking`, which
    /// panics outside a tokio runtime. Rather than let that panic escape from
    /// inside a stream, decline the provider and read from object storage — the
    /// same degradation a wrong schema or an unimportable stream gets.
    ///
    /// Warns on EVERY declined file, deliberately. There is no once-guard: the
    /// condition is a property of the caller's executor and cannot change within a
    /// read, so one reader emits at most one of these — but a scan of N file
    /// groups off-runtime emits N, which is the right volume for a misconfiguration
    /// that silently costs the provider seam its entire benefit.
    fn provider_is_usable_here(&self, path: &str) -> bool {
        if tokio::runtime::Handle::try_current().is_ok() {
            return true;
        }
        log::warn!(
            "[HoodieFileGroupReader] a base-file provider is injected but this read is \
             not being polled inside a tokio runtime, so a served stream could not be \
             driven. Declining the provider and reading '{path}' from object storage. \
             Poll the reader inside a tokio runtime to use the provider"
        );
        false
    }

    /// Is the #18132 repair gate capable of firing on this read at all?
    ///
    /// `repair_risk_columns` is decided ONCE per scan from the table schema and
    /// the predicate's own referenced columns, and is empty unless the predicate
    /// touches a tz-aware millis column — so the common scan answers `false` here
    /// and never reaches a footer on the gate's account. `pushdown_is_safe` is the
    /// merge gate: when it has already refused, there is no pushdown left to
    /// withdraw.
    ///
    /// One home, because both base-read paths ask it and they used to ask it
    /// differently — the unprojected one not at all (ISSUES OI-11).
    fn repair_gate_is_armed(&self, pushdown_is_safe: bool) -> bool {
        pushdown_is_safe && !self.reader_context.repair_risk_columns.is_empty()
    }

    /// The columns of THIS file whose declared type the apache/hudi#18132 repair
    /// reinterprets on read — the per-file half of the gate.
    ///
    /// `table_side` is `None` when the read has no table schema AND nothing sound
    /// to substitute. `required_schema` is only a legal substitute when it carries
    /// every `repair_risk_column`: it is the PROJECTION, so a risk column pruned
    /// out of it is absent from the table side while still being decoded and still
    /// being misread, and `reinterpreted_columns` skips any candidate absent from
    /// either schema — substituting it blindly answers "no conflict" for the one
    /// shape the gate exists to catch. The caller does that coverage test; by the
    /// time it reaches here, a `Some` is a table side sound for every column this
    /// function will look up.
    ///
    /// With no table side the question cannot be answered, and the only safe
    /// answer is "assume every candidate reinterprets": withdrawing pushdown
    /// costs a full scan, keeping it costs rows that match and cannot be
    /// recovered by the post-merge filter.
    fn repair_conflict_from(
        &self,
        file_schema: &arrow_schema::Schema,
        table_side: Option<&arrow_schema::Schema>,
        pushdown_is_safe: bool,
    ) -> Result<Vec<String>> {
        if !self.repair_gate_is_armed(pushdown_is_safe) {
            return Ok(Vec::new());
        }
        match table_side {
            Some(table) => crate::schema::batch_evolution::reinterpreted_columns(
                file_schema,
                table,
                &self.reader_context.repair_risk_columns,
            ),
            None => Ok(self.reader_context.repair_risk_columns.clone()),
        }
    }

    /// Withdraw a base read's pushdown because this file needs the repair, and
    /// say so in the counters. No-op when `repair_conflict` is empty.
    ///
    /// Deliberately keyed on the CONFLICT, never on the narrowed
    /// `pushdown_is_safe`: the clearing itself would be idempotent either way,
    /// but this also records `pushdown_suppressed_by_repair`, and keying that on
    /// the narrowed value fires it on every MERGE-gate refusal too — the counter
    /// exists precisely to separate the two causes. Pinned by
    /// `a_selector_the_gate_refuses_is_counted_not_silently_dropped`.
    fn withdraw_pushdown_for_repair(
        &self,
        path: &str,
        repair_conflict: &[String],
        row_filter: &mut Option<RowFilterBuilder>,
        row_group_selector: &mut Option<RowGroupSelector>,
    ) {
        if repair_conflict.is_empty() {
            return;
        }
        let volume = self.storage.read_volume();
        // Counted for every withdrawal; `row_group_selector_suppressed` can
        // only speak for a selector the caller actually installed.
        volume.record_pushdown_suppressed_by_repair();
        if row_group_selector.is_some() {
            volume.record_selector_suppressed();
        }
        log::debug!(
            "base file '{path}' needs a value-reinterpreting logical-type repair \
             on {repair_conflict:?} — skipping parquet RowFilter pushdown and \
             row-group pruning (post-merge filter still runs)"
        );
        *row_filter = None;
        *row_group_selector = None;
    }

    /// Fold one provider attempt's setup counters into the shared slot.
    ///
    /// The single writer, so "no counter is recorded twice" is a property of this
    /// function rather than an invariant spread across call sites. It holds
    /// because a reader offers exactly one base file to the provider, so this
    /// runs at most once per reader — asserted in debug builds via
    /// [`Self::provider_stats_recorded`]. The condition it catches is real on
    /// this lineage too: [`Self::base_file_source`] is reached from both
    /// [`Self::open`] and [`Self::read`], so driving one reader through both (or
    /// through `read()` twice) would silently double the setup counters.
    ///
    /// In release a second call accumulates rather than panicking: these are
    /// diagnostic counters and must never fail a read. The lock poison is
    /// swallowed for the same reason.
    fn record_provider_stats(&self, stats: &BaseFileProviderStats) {
        let already = self
            .provider_stats_recorded
            .swap(true, std::sync::atomic::Ordering::Relaxed);
        debug_assert!(
            !already,
            "base-file provider stats recorded twice on one reader — the \
             single-sink invariant that rules out double-counting is broken; \
             `base_file_source` was driven more than once"
        );
        if let Ok(mut slot) = self.base_file_provider_stats.lock() {
            slot.merge(stats);
        }
    }

    /// Copy the shared provider slot into [`Self::read_stats`].
    ///
    /// Called by [`Self::read`], where the stream is exhausted and the drain
    /// counters are therefore final. `None` stays `None` when no provider was
    /// INJECTED — which is what the guard below actually tests, and is weaker
    /// than "never ran": a provider injected on a position-merge split (skipped
    /// at the seam) or a log-only file group (which returns before the seam) is
    /// never offered a file, and this writes `Some(all zeroes)` for it. So
    /// all-zeroes distinguishes "no provider injected" from "a provider was
    /// injected", not from "a provider ran". A consumer that needs the stronger
    /// distinction should read `files_served + storage_fallbacks`, which is the
    /// count of files actually OFFERED. `base_file_provider: Some(all zeroes)`
    /// cannot be confused with
    /// "no provider injected".
    fn snapshot_provider_stats(&mut self) {
        if self.base_file_provider.is_none() {
            return;
        }
        if let Ok(slot) = self.base_file_provider_stats.lock() {
            self.read_stats.base_file_provider = Some(slot.clone());
        }
    }

    /// Live handle to this reader's provider counters.
    ///
    /// The setup counters are populated by the time [`Self::open`] returns; the
    /// drain counters (`rows_served` / `bytes_materialized` /
    /// `batches_received`) fill in as the caller pulls the merge stream, so read
    /// this **after** draining for the complete picture. All-zero when no
    /// provider served a base file.
    ///
    /// FFI consumers capture this before dropping the reader, for the same
    /// reason they capture [`Self::stream_stats_handle`].
    pub fn base_file_provider_live_stats(&self) -> Arc<StdMutex<BaseFileProviderStats>> {
        self.base_file_provider_stats.clone()
    }

    /// Whether this slice's base file is inside the read's instant range.
    ///
    /// A Hudi base file belongs to exactly one commit instant — encoded in its
    /// file name (`<fileId>_<writeToken>_<commit>.<ext>`) and surfaced as
    /// [`InputSplit::base_file_commit_time`]. So every row in the file shares
    /// that one instant, and the range test is a single per-file decision: keep
    /// the whole file or drop it.
    ///
    /// This mirrors the Java reader. `HoodieFileGroupReader` only applies
    /// `applyInstantRangeFilter` when `getInstantRange().isPresent()` (empty on a
    /// plain snapshot); inflight / rolled-back *base files* are otherwise excluded
    /// at the file-slice level by `HoodieTableFileSystemView`, never by a per-row
    /// `_hoodie_commit_time` test. The range here (set by the gluten adapter for a
    /// native snapshot read: instants <= latest completed) exists to exclude base
    /// files from inflight / rolled-back commits; log-block exclusion is handled
    /// separately in the log path via `valid_block_instants`, not here.
    ///
    /// Masking rows by the per-row `_hoodie_commit_time` *column* would be a
    /// fragile proxy: **virtual-key** tables
    /// (`hoodie.populate.meta.fields=false`) persist a NULL `_hoodie_commit_time`,
    /// so every base row would be masked out and the read would silently return
    /// 0 rows even though the file's own instant is in range.
    fn base_file_in_range(&self) -> Result<bool> {
        let Some(instant_range) = &self.reader_context.instant_range else {
            return Ok(true);
        };

        // Skip filtering for metadata table (mirrors Java line 356).
        if crate::util::path::is_metadata_table_path(&self.reader_context.table_path) {
            return Ok(true);
        }

        // Production: the FFI sets `base_file_commit_time`. Fall back to parsing it
        // from the base file name when unset (robustness / tests) so the per-file
        // decision still works.
        let file_commit_time = self.input_split.base_file_commit_time.clone().or_else(|| {
            self.input_split
                .base_file_path
                .as_deref()
                .and_then(Self::base_commit_time_from_path)
        });

        let timezone = self.reader_context.timezone();
        let keep = Self::base_file_in_instant_range(
            file_commit_time.as_deref(),
            instant_range,
            &timezone,
        )?;
        if !keep {
            log::debug!(
                "[HoodieFileGroupReader] base file commit {file_commit_time:?} outside the \
                 instant range — excluding the whole base file"
            );
        }
        Ok(keep)
    }

    // NOTE: the FileGroupMergeStream owns the OutputConverter and applies
    // it per emitted chunk in its `Iterator::next()`. The reader's
    // `output_converter` field only lives up to
    // `open()`, which takes ownership and hands it to the iterator.

    /// Best-effort parse of a base file's commit instant from its path
    /// (`…/<fileId>_<writeToken>_<commit>.<ext>`). Fallback for when
    /// [`InputSplit::base_file_commit_time`] is unset (the FFI normally sets it).
    fn base_commit_time_from_path(path: &str) -> Option<String> {
        let file_name = path.rsplit('/').next().unwrap_or(path);
        file_name
            .parse::<crate::file_group::base_file::BaseFile>()
            .ok()
            .map(|bf| bf.commit_timestamp)
    }

    /// Whether a base file's rows fall within `instant_range`, decided by the
    /// file's single commit instant.
    ///
    /// `None` (log-only slice, or an unparseable base-file name) → keep, matching
    /// the Java reader's default of not row-filtering a base read when it cannot
    /// be bounded.
    fn base_file_in_instant_range(
        base_file_commit_time: Option<&str>,
        instant_range: &crate::timeline::selector::InstantRange,
        timezone: &str,
    ) -> Result<bool> {
        match base_file_commit_time {
            // An unparseable commit instant (e.g. the short '001'-style instants some Hudi
            // write-path unit tests use) can't be datetime-bounded against the range. Fall
            // back to LEXICOGRAPHIC comparison, exactly matching the JVM reader -- which
            // compares instant strings (InstantComparison) and never parses. Hudi instants
            // are fixed-format numeric strings, so lexicographic order equals chronological
            // order; keeping the file unconditionally instead would admit rows Java excludes
            // (duplicates in incremental reads). Production commit instants always parse, so
            // this fallback is inert there.
            Some(commit_time) => match instant_range.is_in_range(commit_time, timezone) {
                Ok(in_range) => Ok(in_range),
                Err(e) => {
                    let in_range = instant_range.is_in_range_lexicographic(commit_time);
                    log::debug!(
                        "[HoodieFileGroupReader] base_file_in_instant_range: commit instant \
                         '{commit_time}' is not a parseable datetime ({e}); using lexicographic \
                         comparison (JVM InstantComparison parity) -> in_range={in_range}"
                    );
                    Ok(in_range)
                }
            },
            None => Ok(true),
        }
    }

    // =========================================================================
    // Setters (mirrors Java's mutable field assignments)
    // =========================================================================

    /// Set the output converter.
    /// Mirrors Java: `this.outputConverter = readerContext.getSchemaHandler().getOutputConverter()`.
    /// Set by the FFI/harness path before `open`; the adapter path installs neither.
    #[allow(dead_code)]
    pub fn set_output_converter(&mut self, converter: Box<dyn OutputConverter>) {
        self.output_converter = Some(converter);
    }

    /// Set the buffered record converter.
    /// Mirrors Java: `this.bufferedRecordConverter = BufferedRecordConverter.createConverter(...)`.
    /// Set by the FFI/harness path — see `set_output_converter`.
    #[allow(dead_code)]
    pub fn set_buffered_record_converter(&mut self, converter: Box<dyn BufferedRecordConverter>) {
        self.buffered_record_converter = Some(converter);
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Returns the read statistics collected during the read.
    /// Java-parity accessors; the adapter reads the stats it needs off the returned
    /// value.
    #[allow(dead_code)]
    /// The stats this read accumulated.
    ///
    /// Complete after [`Self::read`], which folds the merge-phase counters back
    /// in once the merge is exhausted. **After [`Self::open_stream`] the
    /// merge-phase counters read zero** - `final_merge_us`, `output_build_us`,
    /// `merge_map_peak_entries` and the insert/update/delete counts accumulate
    /// into the shared `stream_stats` handle as the stream is consumed, and
    /// nothing folds them back, because the caller owns the stream and the
    /// reader cannot know when it ended. The scan-phase counters (log blocks,
    /// log records, corrupt blocks, rollbacks, base read) are populated on both
    /// paths.
    ///
    /// Worth stating because the gap is silent and reads as data: a streaming
    /// read of a fixture with five deletes reports `num_deletes: 0` while
    /// returning exactly the same rows as the eager read that reports five.
    /// Nothing calls this accessor after a streaming open today - the test
    /// harness, the in-crate tests and the one production caller, the
    /// metadata-table read, all call it after [`Self::read`], where the values
    /// are complete - but a caller moved to a streaming open is precisely where
    /// a zero would be believed. The FFI's streaming route reads only
    /// `final_merge_us` and `output_build_us`, off the shared sink via
    /// [`Self::stream_stats_handle`]; the insert/update/delete counts have no
    /// production reader on either call shape.
    pub fn read_stats(&self) -> &HoodieReadStats {
        &self.read_stats
    }

    /// Clone the shared stage-timing sink.
    ///
    /// FFI consumers capture this before dropping the reader: the stream
    /// returned by [`Self::open`] keeps accumulating into it as chunks drain,
    /// so reading `read_stats()` off a dropped reader yields zeros.
    pub fn stream_stats_handle(&self) -> StreamStatsHandle {
        self.stream_stats.clone()
    }

    /// Returns the valid block instants from log scanning.
    /// See `read_stats`.
    #[allow(dead_code)]
    pub fn valid_block_instants(&self) -> &[String] {
        &self.valid_block_instants
    }
}

// =========================================================================
// Builder
// =========================================================================

/// Builder for `HoodieFileGroupReader`.
///
/// Reached only from the test harness today — `FileGroupReader` constructs the
/// engine directly through [`adapter`](super::adapter). Kept because the
/// harness is what drives the engine the way an FFI caller would, so it is the
/// only exercise of this construction path.
#[allow(dead_code)]
///
/// Mirrors Java's `HoodieFileGroupReader.Builder<T>`.
#[derive(Default)]
pub struct HoodieFileGroupReaderBuilder {
    reader_context: Option<Arc<ReaderContext>>,
    storage: Option<Arc<Storage>>,
    input_split: Option<InputSplit>,
    reader_parameters: ReaderParameters,
    data_schema: Option<SchemaRef>,
    requested_schema: Option<SchemaRef>,
    /// Set by `with_row_filter_builder`; copied onto a cloned reader_context
    /// at build time so the same builder is visible to base parquet reads
    /// (this file) and parquet log block decodes (`log_file::content`).
    row_filter_builder: Option<RowFilterBuilder>,
    /// Set by `with_row_group_selector`; copied onto a cloned reader_context in
    /// `build()`, exactly like `row_filter_builder`.
    row_group_selector: Option<RowGroupSelector>,
    /// Set by `with_mor_pk_safe`; copied onto the cloned reader_context.
    mor_pk_safe: Option<bool>,
    /// Set by `with_repair_risk_columns`; copied onto the cloned reader_context.
    /// Absent leaves the repair guard OFF.
    repair_risk_columns: Option<Vec<String>>,
    /// Set by `with_base_file_provider`; injected onto the reader at build time.
    base_file_provider: Option<BaseFileDataProviderRef>,
}

/// Reached only from the test harness — see the builder's own note.
#[allow(dead_code)]
impl HoodieFileGroupReaderBuilder {
    pub fn with_reader_context(mut self, ctx: Arc<ReaderContext>) -> Self {
        self.reader_context = Some(ctx);
        self
    }

    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn with_input_split(mut self, input_split: InputSplit) -> Self {
        self.input_split = Some(input_split);
        self
    }

    pub fn with_reader_parameters(mut self, params: ReaderParameters) -> Self {
        self.reader_parameters = params;
        self
    }

    /// Set the data schema (full table schema).
    /// Mirrors Java's `Builder.withDataSchema(Schema dataSchema)`.
    pub fn with_data_schema(mut self, schema: SchemaRef) -> Self {
        self.data_schema = Some(schema);
        self
    }

    /// Set the requested schema (column projection).
    /// Mirrors Java's `Builder.withRequestedSchema(Schema requestedSchema)`.
    pub fn with_requested_schema(mut self, schema: SchemaRef) -> Self {
        self.requested_schema = Some(schema);
        self
    }

    /// install a parquet `RowFilter` builder.
    ///
    /// Whether the builder is actually used at scan time is gated by
    /// `base_read_pushdown_is_safe()`:
    /// - CoW table → always pushed
    /// - MOR table → pushed only if `mor_pk_safe` is true (see
    ///   [`Self::with_mor_pk_safe`])
    ///
    /// The builder is also visible to the parquet log block decoder via the
    /// same `reader_context` channel.
    ///
    /// Pair this with [`Self::with_repair_risk_columns`], or the repair guard is
    /// off and a base file mislabelling a predicate column over-drops rows.
    pub fn with_row_filter_builder(mut self, b: RowFilterBuilder) -> Self {
        self.row_filter_builder = Some(b);
        self
    }

    /// Install a row-group selector, pruning base reads from footer statistics.
    ///
    /// Routed onto `reader_context` exactly like
    /// [`Self::with_row_filter_builder`], and gated at scan time by the same
    /// `base_read_pushdown_is_safe()`. Setting one without the other is
    /// supported: they are independent mechanisms over the same predicate, and
    /// only this one avoids IO.
    pub fn with_row_group_selector(mut self, selector: RowGroupSelector) -> Self {
        self.row_group_selector = Some(selector);
        self
    }

    /// mark the pushed predicate as safe for MOR (i.e. it
    /// references only primary-key columns). When true, the row filter
    /// pushes into both base parquet files and parquet log blocks on MOR
    /// tables. When false (default), the filter pushes only on CoW.
    ///
    /// Compute via [`crate::file_group::predicate::PushedFilter::references_only_primary_keys`]
    /// (lives in the cpp crate via FFI) and pass the result here.
    pub fn with_mor_pk_safe(mut self, mor_pk_safe: bool) -> Self {
        self.mor_pk_safe = Some(mor_pk_safe);
        self
    }

    /// Arm the value-reinterpreting repair guard with the predicate columns the
    /// apache/hudi#18132 logical-type repair could make a pushed filter misread.
    ///
    /// Required alongside [`Self::with_row_filter_builder`] whenever the table may
    /// hold legacy base files labelling a tz-aware column micros while the stored
    /// i64 is millis. Left unset the guard is OFF and such a file over-drops rows —
    /// this is not a perf knob. Compute via
    /// [`crate::schema::batch_evolution::repair_risk_columns`]; the empty vec is the
    /// explicit "no column is at risk".
    pub fn with_repair_risk_columns(mut self, columns: Vec<String>) -> Self {
        self.repair_risk_columns = Some(columns);
        self
    }

    /// Inject a base-file data provider (dependency injection).
    ///
    /// The built reader offers each base file to `provider` before the
    /// object-store read; a provider that returns `None` falls through to that
    /// read. hudi-core ships no provider — a downstream crate supplies one. See
    /// [`BaseFileDataProvider`](super::base_file_provider::BaseFileDataProvider).
    ///
    /// # Runtime precondition
    ///
    /// A served stream is driven on `tokio::task::spawn_blocking`, so the reader
    /// must be polled **inside a tokio runtime** for the provider to be used at
    /// all. Polled on any other executor, the provider is DECLINED per base file
    /// and every read goes to object storage — correct results, counted as
    /// storage fallbacks, with one warning naming the cause. It does not panic,
    /// and it is not silent, but the provider buys nothing.
    ///
    /// The stronger requirement is a MULTI-THREADED runtime: a provider whose
    /// reader itself calls `block_on` (the C-ABI adapter's does) needs the
    /// blocking pool to be a different thread from the runtime worker. See
    /// `served_batch_stream` for that argument.
    pub fn with_base_file_provider(mut self, provider: BaseFileDataProviderRef) -> Self {
        self.base_file_provider = Some(provider);
        self
    }

    pub fn build(self) -> Result<HoodieFileGroupReader> {
        let reader_context = self
            .reader_context
            .ok_or_else(|| CoreError::ReadFileSliceError("reader_context is required".into()))?;
        let storage = self
            .storage
            .ok_or_else(|| CoreError::ReadFileSliceError("storage is required".into()))?;
        let input_split = self
            .input_split
            .ok_or_else(|| CoreError::ReadFileSliceError("input_split is required".into()))?;

        // If the caller set a row_filter_builder or mor_pk_safe via the
        // builder API, copy them onto the reader_context. Clone-and-replace
        // mirrors the same pattern HoodieFileGroupReader::new() uses to
        // update the schema_handler on its reader_context.
        let reader_context = if self.row_filter_builder.is_some()
            || self.row_group_selector.is_some()
            || self.mor_pk_safe.is_some()
            || self.repair_risk_columns.is_some()
        {
            let mut updated = (*reader_context).clone();
            if let Some(b) = self.row_filter_builder {
                updated.row_filter_builder = Some(b);
            }
            if let Some(selector) = self.row_group_selector {
                updated.row_group_selector = Some(selector);
            }
            if let Some(s) = self.mor_pk_safe {
                updated.mor_pk_safe = s;
            }
            if let Some(cols) = self.repair_risk_columns {
                updated.repair_risk_columns = cols;
            }
            Arc::new(updated)
        } else {
            reader_context
        };

        let mut reader = HoodieFileGroupReader::new(
            reader_context,
            storage,
            input_split,
            self.reader_parameters,
            self.data_schema,
            self.requested_schema,
        )?;
        reader.base_file_provider = self.base_file_provider;

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::table::HudiTableConfig;
    use crate::storage::util::parse_uri;
    use arrow_array::Array;
    use std::collections::HashMap;

    use crate::timeline::selector::InstantRange;

    // the base-file instant-range decision is per-file (keyed on
    // `base_file_commit_time`), NOT per-row on the `_hoodie_commit_time` column —
    // so a virtual-key table (NULL commit-time column) is never wrongly dropped.
    #[test]
    fn base_file_in_instant_range_uses_file_commit_not_row_column() {
        // Range = up_to(latest) = (-INF, latest]  (the gluten snapshot cap).
        let latest = "20260710235017614";
        let range = InstantRange::up_to(latest, "UTC");

        // Valid base file at the latest completed commit → kept (inclusive end).
        // (Its rows' `_hoodie_commit_time` column is irrelevant here — could be NULL
        // for a virtual-key table; the decision is the file's own instant.)
        assert!(
            HoodieFileGroupReader::base_file_in_instant_range(Some(latest), &range, "UTC").unwrap(),
            "base file at the latest completed instant must be kept"
        );

        // A base file from a later (inflight / rolled-back) commit → excluded.
        assert!(
            !HoodieFileGroupReader::base_file_in_instant_range(
                Some("20260710235017615"),
                &range,
                "UTC"
            )
            .unwrap(),
            "base file newer than the range end must be excluded (C-PENDING-ROLLBACK)"
        );

        // No parseable base-file commit (log-only / unknown) → keep (Java default).
        assert!(
            HoodieFileGroupReader::base_file_in_instant_range(None, &range, "UTC").unwrap(),
            "unknown base-file commit must default to keep"
        );
    }

    // An unparseable base-file commit instant (e.g. the short '001'-style instants some Hudi
    // write-path unit tests use) must not fail the read — it falls back to LEXICOGRAPHIC
    // comparison, matching the JVM reader's InstantComparison (string compare, never parses).
    // The fallback must both KEEP in-range instants and EXCLUDE out-of-range ones; an
    // unconditional keep would admit rows Java excludes (dups in incremental reads).
    #[test]
    fn base_file_in_instant_range_unparseable_commit_uses_lexicographic() {
        // "001" <= end lexicographically -> kept (same outcome Java's string compare gives).
        let range = InstantRange::up_to("20260710235017614", "UTC");
        assert!(
            HoodieFileGroupReader::base_file_in_instant_range(Some("001"), &range, "UTC").unwrap(),
            "unparseable instant within the range must be kept, not error"
        );
        // "001" <= open start "100" lexicographically -> EXCLUDED, exactly as Java would.
        let range = InstantRange::within_open_closed("100", "20260710235017614", "UTC");
        assert!(
            !HoodieFileGroupReader::base_file_in_instant_range(Some("001"), &range, "UTC").unwrap(),
            "unparseable instant before the open start must be excluded (Java string-compare \
             parity), not kept unconditionally"
        );
    }

    #[test]
    fn base_file_in_instant_range_open_start_excludes_base_at_start() {
        // within_open_closed(base, log] — mirrors `instant_range_excludes_base`:
        // the base file's own instant (== open start) is excluded.
        let range =
            InstantRange::within_open_closed("20240101120000000", "20240101130000000", "UTC");
        assert!(
            !HoodieFileGroupReader::base_file_in_instant_range(
                Some("20240101120000000"),
                &range,
                "UTC"
            )
            .unwrap(),
            "open start must exclude a base file whose commit == start"
        );
        assert!(
            HoodieFileGroupReader::base_file_in_instant_range(
                Some("20240101123000000"),
                &range,
                "UTC"
            )
            .unwrap(),
            "a base file inside (start, end] must be kept"
        );
    }

    /// Write `batch` to `<dir>/<name>` as a parquet file. Minimal inline
    /// ArrowWriter helper (reader/mod.rs has no parquet-writing helper of its own).
    fn write_parquet_file(dir: &std::path::Path, name: &str, batch: &RecordBatch) {
        use parquet::arrow::ArrowWriter;
        let file = std::fs::File::create(dir.join(name)).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    /// As [`write_parquet_file`], but capping the row-group size so the file has
    /// several of them. A base file with one row group cannot tell a reader that
    /// keeps every group from one that keeps the first.
    fn write_parquet_file_in_row_groups(
        dir: &std::path::Path,
        name: &str,
        batch: &RecordBatch,
        rows_per_group: usize,
    ) {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows_per_group))
            .build();
        let file = std::fs::File::create(dir.join(name)).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    /// Build a `HoodieFileGroupReader` rooted at `dir`, with a base file at
    /// `base_name` and `required` set as the `required_schema` driving the read.
    async fn test_file_group_reader_for_base_file(
        dir: &std::path::Path,
        base_name: &str,
        required: SchemaRef,
    ) -> HoodieFileGroupReader {
        let base_path = dir.to_str().unwrap().to_string();
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref(),
            base_path,
        )]));
        let storage = Storage::new(Arc::new(HashMap::new()), hudi_configs).unwrap();

        let input_split =
            InputSplit::new(Some(base_name.to_string()), None, Vec::new(), String::new());

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        reader_context.rebuild_record_context(String::new());

        let mut reader = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            None,
            None,
        )
        .unwrap();

        // Drive base_file_source with the exact required schema under test,
        // bypassing prepare_required_schema's meta/key-field augmentation.
        reader.schema_handler.required_schema = Some(required);
        reader
    }

    /// Drain a base file source into one concatenated `RecordBatch`, under the
    /// schema the source reports.
    async fn drain_base_source(source: BaseSource) -> RecordBatch {
        let BaseSource { schema, batches } = source;
        let batches: Vec<RecordBatch> = batches.map(|r| r.unwrap()).collect().await;
        if batches.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            arrow::compute::concat_batches(&schema, &batches).unwrap()
        }
    }

    /// Base file written at s1 {meta..., id:int, price:float}; required schema at
    /// s2 {id:long, price:double, tag:string?}: missing column null-filled, int
    /// widened, float→double value-exact. Mirrors Java's HoodieParquetFileFormatHelper.
    ///
    /// Runs against BOTH base-file source modes —
    /// Runs against the base source as the merge sees it and against its
    /// collapsed single-batch form — the shape `read()` merges — to prove the
    /// evolution is applied per row group and does not depend on how the base is
    /// chunked. The two outputs must be byte-identical.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_base_file_source_schema_on_write_evolution() {
        use arrow_array::{Float32Array, Int32Array};
        // -- write a parquet base file with OLD schema --
        let tmp = tempfile::tempdir().unwrap();
        let file_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("price", arrow_schema::DataType::Float32, true),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["k1"])),
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(Float32Array::from(vec![0.1f32])),
            ],
        )
        .unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &batch);

        // -- required schema = NEW shape --
        let required = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("price", arrow_schema::DataType::Float64, true),
            arrow_schema::Field::new("tag", arrow_schema::DataType::Utf8, true),
        ]));

        // Assert the evolution invariants on a drained base-file source.
        let assert_evolved = |out: &RecordBatch| {
            assert_eq!(out.schema(), required);
            let id = out
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            assert_eq!(id.value(0), 7);
            let price = out
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap();
            assert_eq!(
                price.value(0),
                0.1f64,
                "float→double must be value-exact (gold C6)"
            );
            assert!(out.column(3).is_null(0), "added column null-filled");
        };

        // The evolution is applied per row group, so draining the source and
        // concatenating must give the same rows as reading it whole would - the
        // property `read()` relies on now that it collects the merged chunks
        // rather than collapsing the base first.
        let dir = tmp.path().to_path_buf();
        let mut reader =
            test_file_group_reader_for_base_file(&dir, base_name, required.clone()).await;
        let streamed = drain_base_source(reader.base_file_source().await.unwrap()).await;
        assert_evolved(&streamed);
    }

    /// A base source feeding a position-based merge carries the row-position
    /// column, and one feeding a key-based merge does not.
    ///
    /// This is the join between the base read and the position buffer: the
    /// buffer looks the column up by name and errors when it is absent, so a
    /// read whose base source omits it cannot merge by position at all. Asserted
    /// on both the eager and streaming sources, which open the parquet file
    /// through different calls and could disagree.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_base_file_source_carries_row_positions_for_position_merge() {
        use arrow_array::{Int32Array, Int64Array};

        let tmp = tempfile::tempdir().unwrap();
        let file_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["k1", "k2", "k3"])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap();
        let base_name = "f1-0_0-1-1_20240101120000000.parquet";
        write_parquet_file(tmp.path(), base_name, &batch);

        let required = file_schema.clone();
        let dir = tmp.path().to_path_buf();

        let build = |use_record_position: bool| {
            let dir = dir.clone();
            let required = required.clone();
            async move {
                let mut reader =
                    test_file_group_reader_for_base_file(&dir, base_name, required).await;
                // Position merge only applies to a slice that has log records to
                // merge, and only when the base file's instant is known.
                reader.input_split = InputSplit::new(
                    Some(base_name.to_string()),
                    Some("20240101120000000".to_string()),
                    vec![".f1-0_20240101130000000.log.1_0-1-1".to_string()],
                    String::new(),
                );
                reader.reader_parameters = ReaderParameters {
                    use_record_position,
                    ..Default::default()
                };
                reader
            }
        };

        let mut positional = build(true).await;
        let eager = drain_base_source(positional.base_file_source().await.unwrap()).await;
        let positions = eager
            .column_by_name(ROW_INDEX_TEMPORARY_COLUMN_NAME)
            .expect("position merge needs the row-position column on the base source")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row positions are Int64");
        assert_eq!(positions.values(), &[0, 1, 2]);

        let mut keyed = build(false).await;
        let without = drain_base_source(keyed.base_file_source().await.unwrap()).await;
        assert_eq!(
            without.schema(),
            required,
            "a key-based merge must not pay for the row-position column"
        );
    }

    #[tokio::test]
    async fn test_make_base_file_batches_case_insensitive_column_match() {
        use arrow_array::Int32Array;
        // Base file written with `ID` (uppercase); required schema asks for
        // `id`. A case-sensitive intersection drops `ID` from the parquet
        // projection, so the column is never read and `project_batch_to_schema`
        // null-fills `id` — silently discarding the real values. The whole base
        // read path must match names case-insensitively (gold/Spark behavior).
        let tmp = tempfile::tempdir().unwrap();
        let file_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("ID", arrow_schema::DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["k1"])),
                Arc::new(Int32Array::from(vec![7])),
            ],
        )
        .unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &batch);

        let required = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
        ]));

        let mut reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, required.clone()).await;
        let source = reader.base_file_source().await.unwrap();
        let out = drain_base_source(source).await;
        assert_eq!(out.schema(), required);
        let id = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(
            !id.is_null(0),
            "case-differing column must not be silently null-filled"
        );
        assert_eq!(
            id.value(0),
            7,
            "real value must survive case-insensitive column match"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // builder routes `with_row_filter_builder` and
    // `with_mor_pk_safe` onto the shared `reader_context` so both the base
    // parquet read site (this file's `base_file_source`) and the
    // parquet log block decoder (`file_group::log_file::content::Decoder`)
    // see the same gating decision.
    //
    // Pure builder-state tests — they exercise the builder plumbing without
    // actually executing a read. End-to-end integration is covered by the
    // FFI-level tests + the lake-loader functional benchmark.
    // ════════════════════════════════════════════════════════════════════

    fn dummy_reader_context(table_type: &str) -> Arc<ReaderContext> {
        let mut ctx = ReaderContext::empty();
        ctx.table_config
            .insert("hoodie.table.type".to_string(), table_type.to_string());
        Arc::new(ctx)
    }

    fn dummy_input_split() -> InputSplit {
        // Bare split: no base file, no log files. Sufficient for builder
        // plumbing assertions — we never call read().
        InputSplit::new(None, None, vec![], "p1".to_string())
    }

    /// A split that merges: one base file and one log file. The gate reduces to
    /// `mor_pk_safe` only on a split like this — with no log files it is open
    /// whatever `mor_pk_safe` says, so a PK-safety assertion made on a bare
    /// split would pass without testing anything.
    fn merging_input_split() -> InputSplit {
        InputSplit::new(
            Some("base.parquet".to_string()),
            None,
            vec![".log.1".to_string()],
            "p1".to_string(),
        )
    }

    fn make_row_filter_builder() -> RowFilterBuilder {
        // Closure that always returns None — we only care that the builder
        // was installed, not what it produces.
        std::sync::Arc::new(|_parquet_schema, _projected_schema| None)
    }

    #[test]
    fn builder_routes_row_filter_builder_into_reader_context() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            .build()
            .unwrap();
        assert!(
            reader.reader_context.row_filter_builder.is_some(),
            "with_row_filter_builder should land on reader_context"
        );
    }

    #[test]
    fn builder_mor_pk_safe_true_unlocks_pushdown_on_mor() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(merging_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            .with_mor_pk_safe(true)
            .build()
            .unwrap();
        assert!(reader.reader_context.mor_pk_safe);
        assert!(
            reader.base_read_pushdown_is_safe(),
            "MOR + mor_pk_safe=true must push"
        );
    }

    #[test]
    fn builder_mor_pk_safe_false_blocks_pushdown_on_mor() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(merging_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            // mor_pk_safe defaults to false
            .build()
            .unwrap();
        assert!(!reader.reader_context.mor_pk_safe);
        assert!(
            !reader.base_read_pushdown_is_safe(),
            "MOR without PK-safety must NOT push (mirrors Java's morFilters gate)"
        );
    }

    /// A MOR slice with no log files does not merge, so the predicate is safe to
    /// push whatever `mor_pk_safe` says. Parameterized over both values so the
    /// "does it merge" rule is shown to be independent of PK safety.
    #[test]
    fn base_only_mor_slice_allows_pushdown_regardless_of_pk_safety() {
        for mor_pk_safe in [false, true] {
            let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
            let reader = HoodieFileGroupReader::builder()
                .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
                .with_storage(storage)
                .with_input_split(InputSplit::new(
                    Some("base.parquet".to_string()),
                    None,
                    // No log files => no merge => nothing can flip the predicate.
                    vec![],
                    "p1".to_string(),
                ))
                .with_row_filter_builder(make_row_filter_builder())
                .with_mor_pk_safe(mor_pk_safe)
                .build()
                .unwrap();
            assert!(
                reader.base_read_pushdown_is_safe(),
                "base-only slice must push regardless of mor_pk_safe ({mor_pk_safe})"
            );
        }
    }

    /// The split rule must not weaken the real MOR case: with log files present
    /// the merge can supersede or delete a base row, so a non-PK-safe predicate
    /// still may not be pushed.
    #[test]
    fn mor_slice_with_log_files_still_blocks_pushdown_when_not_pk_safe() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(InputSplit::new(
                Some("base.parquet".to_string()),
                None,
                vec![".log.1".to_string()],
                "p1".to_string(),
            ))
            .with_row_filter_builder(make_row_filter_builder())
            // mor_pk_safe defaults to false
            .build()
            .unwrap();
        assert!(reader.input_split.has_log_files());
        assert!(
            !reader.base_read_pushdown_is_safe(),
            "MOR with log files and no PK safety must NOT push"
        );
    }

    #[test]
    fn builder_routes_row_group_selector_into_reader_context() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_row_group_selector(std::sync::Arc::new(|_| None))
            .build()
            .unwrap();
        assert!(
            reader.reader_context.row_group_selector.is_some(),
            "with_row_group_selector should land on reader_context"
        );
        assert!(
            reader.reader_context.row_filter_builder.is_none(),
            "the two mechanisms are independent: one may be set without the other"
        );
    }

    /// Three rows, one per row group. A selector keeping only the first must
    /// leave the read with that row group's row and no other -- and the volume
    /// counters must show that the other two were never scanned, which is the
    /// difference between pruning and filtering.
    #[tokio::test]
    async fn a_selector_prunes_row_groups_when_the_read_does_not_merge() {
        use std::sync::atomic::Ordering::Relaxed;

        let (tmp, base_name, schema) = three_row_groups();
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), &base_name, schema).await;
        let volume = reader.storage.read_volume();
        install_selector(&mut reader, |_| Some(vec![0]), false);

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(out.num_rows(), 1, "only the kept row group was read");
        assert_eq!(volume.row_group_selector_calls.load(Relaxed), 1);
        assert_eq!(volume.row_group_selector_suppressed.load(Relaxed), 0);
        assert_eq!(volume.file_row_groups.load(Relaxed), 3);
        assert_eq!(
            volume.row_groups_read.load(Relaxed),
            1,
            "the other two row groups were never fetched"
        );
    }

    /// The same selector on a slice that merges, with a predicate that is not
    /// primary-key-safe. Pruning would drop base rows before the merge could
    /// update them into a match, so the gate refuses it -- and counts the
    /// refusal, because a suppressed selector otherwise reads as "no caller ever
    /// The third state of `row_group_selector_calls`: never installed.
    ///
    /// The counter exists to separate "ran and pruned nothing" from "never ran",
    /// and a gate-refused selector is a third case that also reads zero — which
    /// is why `row_group_selector_suppressed` was added beside it. Both of those
    /// are asserted below. This pins the BASELINE they are read against: with no
    /// selector at all, calls AND suppressions are both zero. Without it, a
    /// regression that incremented `calls` unconditionally would still satisfy
    /// every other selector test, and the counter would stop answering the
    /// question it was added for.
    #[tokio::test]
    async fn no_selector_installed_counts_neither_a_call_nor_a_suppression() {
        use std::sync::atomic::Ordering::Relaxed;

        let (tmp, base_name, schema) = three_row_groups();
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), &base_name, schema).await;
        let volume = reader.storage.read_volume();

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(out.num_rows(), 3);
        assert_eq!(
            volume.row_group_selector_calls.load(Relaxed),
            0,
            "no selector was installed, so nothing can have called one"
        );
        assert_eq!(
            volume.row_group_selector_suppressed.load(Relaxed),
            0,
            "and nothing was suppressed — there was nothing to suppress"
        );
        assert_eq!(
            volume.row_groups_read.load(Relaxed),
            3,
            "every row group is read when no selector prunes"
        );
    }

    /// installed one": both are zero calls.
    #[tokio::test]
    async fn a_selector_the_gate_refuses_is_counted_not_silently_dropped() {
        use std::sync::atomic::Ordering::Relaxed;

        let (tmp, base_name, schema) = three_row_groups();
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), &base_name, schema).await;
        let volume = reader.storage.read_volume();
        reader.input_split = InputSplit::new(
            Some(base_name.clone()),
            Some("20240101120000000".to_string()),
            vec![".f1-0_20240101130000000.log.1_0-1-1".to_string()],
            String::new(),
        );
        install_selector(&mut reader, |_| Some(vec![0]), false);

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(out.num_rows(), 3, "every base row still reaches the merge");
        assert_eq!(
            volume.row_group_selector_calls.load(Relaxed),
            0,
            "the selector never ran"
        );
        assert_eq!(
            volume.row_group_selector_suppressed.load(Relaxed),
            1,
            "and the reason it never ran is on the record"
        );
        assert_eq!(volume.row_groups_read.load(Relaxed), 3);
        // The two causes must stay separable. This is a MERGE-gate refusal, so
        // the repair counter must not move — and it is what stops the withdrawal
        // block in `base_file_source` from being keyed on the narrowed
        // `pushdown_is_safe`, which would attribute every merge-gate refusal to
        // the repair gate. Without this assertion that mis-keying passes the
        // whole file.
        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            0,
            "a merge-gate refusal is not a repair withdrawal, and the counters \
             exist to tell them apart"
        );
    }

    /// A merge-gate refusal must not ALSO arm the repair gate.
    ///
    /// `repair_gate_is_armed` is `pushdown_is_safe && !repair_risk_columns
    /// .is_empty()`. Dropping the first conjunct survived the whole suite,
    /// because every fixture that refuses on the merge gate leaves
    /// `repair_risk_columns` empty — so the second conjunct was doing all the
    /// work and the first was pinned by nothing.
    ///
    /// This fixture arms BOTH: a merging split with a non-PK predicate (the merge
    /// gate refuses) over a file that genuinely carries the #18132 mislabel (so
    /// the repair gate would find a real conflict if it were armed). With the
    /// conjunct the gate stays disarmed and `pushdown_suppressed_by_repair` stays
    /// zero; without it the gate fires, finds the conflict, and attributes a
    /// merge-gate refusal to the repair — which is exactly the mis-keying the
    /// two counters exist to keep apart.
    #[tokio::test]
    async fn a_merge_gate_refusal_does_not_also_arm_the_repair_gate() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        // THE LIE: micros declared, millis stored.
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Microsecond);

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());
        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            None,
            &["ts"],
        )
        .await;
        reader.schema_handler.table_schema = Some(straddling_table_schema());
        // A log file on the split with a non-PK predicate: the MERGE gate refuses
        // before the repair gate is ever consulted.
        reader.input_split = InputSplit::new(
            Some(base_name.to_string()),
            Some("20240101120000000".to_string()),
            vec![".f1-0_20240101130000000.log.1_0-1-1".to_string()],
            String::new(),
        );

        let volume = reader.storage.read_volume();
        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            invocations.load(Relaxed),
            0,
            "fixture check: the merge gate must have refused the pushdown, or this \
             test is not exercising the conjunct at all"
        );
        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            0,
            "a merge-gate refusal is not a repair withdrawal — the repair gate must \
             not arm once pushdown is already gone"
        );
    }

    /// The same merging slice with a primary-key-safe predicate: the gate opens,
    /// so the selector runs. Pairs with the case above -- same file, same
    /// selector, opposite outcome from `mor_pk_safe` alone.
    #[tokio::test]
    async fn a_pk_safe_predicate_lets_the_selector_run_on_a_merging_slice() {
        use std::sync::atomic::Ordering::Relaxed;

        let (tmp, base_name, schema) = three_row_groups();
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), &base_name, schema).await;
        let volume = reader.storage.read_volume();
        reader.input_split = InputSplit::new(
            Some(base_name.clone()),
            Some("20240101120000000".to_string()),
            vec![".f1-0_20240101130000000.log.1_0-1-1".to_string()],
            String::new(),
        );
        install_selector(&mut reader, |_| Some(vec![0]), true);

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(out.num_rows(), 1);
        assert_eq!(volume.row_group_selector_calls.load(Relaxed), 1);
        assert_eq!(volume.row_group_selector_suppressed.load(Relaxed), 0);
    }

    /// A selector with no opinion reads the whole file -- but the call is still
    /// counted, which is what separates "ran and found nothing" from "never
    /// installed".
    #[tokio::test]
    async fn a_selector_that_declines_reads_every_row_group_and_still_counts() {
        use std::sync::atomic::Ordering::Relaxed;

        let (tmp, base_name, schema) = three_row_groups();
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), &base_name, schema).await;
        let volume = reader.storage.read_volume();
        install_selector(&mut reader, |_| None, false);

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(out.num_rows(), 3);
        assert_eq!(volume.row_group_selector_calls.load(Relaxed), 1);
        assert_eq!(
            volume.row_groups_read.load(Relaxed),
            volume.file_row_groups.load(Relaxed),
            "declining prunes nothing"
        );
    }

    /// A three-row base file written one row per row group, so a selector has
    /// something to choose between.
    fn three_row_groups() -> (tempfile::TempDir, String, SchemaRef) {
        use arrow_array::Int32Array;

        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![7, 8, 9]))],
        )
        .unwrap();
        let base_name = "f1-0_0-1-1_20240101120000000.parquet".to_string();
        write_parquet_file_in_row_groups(tmp.path(), &base_name, &batch, 1);
        (tmp, base_name, schema)
    }

    /// Put a selector and a PK-safety verdict on a reader that was already built.
    fn install_selector(
        reader: &mut HoodieFileGroupReader,
        select: fn(&parquet::file::metadata::ParquetMetaData) -> Option<Vec<usize>>,
        mor_pk_safe: bool,
    ) {
        let mut context = (*reader.reader_context).clone();
        context.row_group_selector = Some(std::sync::Arc::new(select));
        context.mor_pk_safe = mor_pk_safe;
        reader.reader_context = Arc::new(context);
    }

    // Bootstrap base files are rejected loudly at reader construction.
    // `needs_bootstrap_merge = true` (set when the table has bootstrap base files
    // requiring meta/data column reordering) must surface as CoreError::Unsupported
    // from HoodieFileGroupReader::new, not a silent wrong-data read or a panic.
    // The gate lives just after prepare_required_schema (this file, ~line 264).
    #[tokio::test]
    async fn test_bootstrap_merge_rejected_at_construction() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Trigger condition: table has bootstrap base files (skeleton/data split).
        reader_context.needs_bootstrap_merge = true;
        reader_context.rebuild_record_context(String::new());

        // A minimal data schema lets prepare_required_schema run so the bootstrap
        // gate (which fires immediately after) is the failing point.
        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        ]));

        let input_split = InputSplit::new(
            Some("f1-0_0-1-1_001.parquet".to_string()),
            None,
            Vec::new(),
            String::new(),
        );

        let result = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            Some(data_schema.clone()),
            Some(data_schema),
        );

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("bootstrap merge must be rejected at construction"),
        };
        assert!(
            matches!(err, CoreError::Unsupported(_)),
            "expected CoreError::Unsupported, got {err:?}"
        );
        assert!(
            err.to_string().contains("Bootstrap merge"),
            "error should mention Bootstrap merge, got: {err}"
        );
    }

    // Schema-on-read (InternalSchema) is rejected loudly at reader
    // construction. `hoodie.schema.on.read.enable=true` in table_config must
    // surface as CoreError::Unsupported rather than being silently ignored
    // (silent-wrong-data risk: InternalSchema evolution would be misread).
    // The gate lives just after prepare_required_schema (this file, ~line 264),
    // alongside the bootstrap gate. The FgReaderCase harness has no table_config
    // injection field, so this is asserted as a unit test at the gate's layer.
    #[tokio::test]
    async fn test_schema_on_read_rejected_at_construction() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Trigger condition: table opts into schema-on-read / InternalSchema.
        reader_context.table_config.insert(
            "hoodie.schema.on.read.enable".to_string(),
            "true".to_string(),
        );
        reader_context.rebuild_record_context(String::new());

        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        ]));

        let input_split = InputSplit::new(
            Some("f1-0_0-1-1_001.parquet".to_string()),
            None,
            Vec::new(),
            String::new(),
        );

        let result = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            Some(data_schema.clone()),
            Some(data_schema),
        );

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("schema-on-read must be rejected at construction"),
        };
        assert!(
            matches!(err, CoreError::Unsupported(_)),
            "expected CoreError::Unsupported, got {err:?}"
        );
        assert!(
            err.to_string().contains("schema-on-read"),
            "error should mention schema-on-read, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_composite_virtual_keys_accepted_at_construction() {
        // Composite virtual keys (virtual keys + a multi-field record key)
        // are supported — `RecordContext::record_key_array` reconstructs the full
        // `field:val,field:val` merge key per row on both sides, so construction
        // must succeed rather than erroring `CoreError::Unsupported`.
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Trigger: virtual keys (no meta fields) + a multi-field record key.
        reader_context.table_config.insert(
            "hoodie.populate.meta.fields".to_string(),
            "false".to_string(),
        );
        reader_context.table_config.insert(
            "hoodie.table.recordkey.fields".to_string(),
            "pk1,pk2".to_string(),
        );
        reader_context.rebuild_record_context(String::new());
        // The full record-key field list is retained (not just the first field).
        assert_eq!(
            reader_context.get_record_context().record_key_fields,
            vec!["pk1", "pk2"],
        );

        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("pk1", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("pk2", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("v", arrow_schema::DataType::Int64, true),
        ]));

        let input_split = InputSplit::new(
            Some("f1-0_0-1-1_001.parquet".to_string()),
            None,
            Vec::new(),
            String::new(),
        );

        let result = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            Some(data_schema.clone()),
            Some(data_schema),
        );

        assert!(
            result.is_ok(),
            "composite virtual keys must be accepted at construction, got {:?}",
            result.err(),
        );
    }

    #[tokio::test]
    async fn test_composite_precombine_accepted_at_construction() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Multi-field (comma-separated) precombine is supported: RecordContext splits
        // it into ordering_field_names and get_ordering_values builds a composite
        // ordering value per row — no silent first-field-only degradation, so
        // construction must accept it.
        reader_context.table_config.insert(
            "hoodie.table.precombine.field".to_string(),
            "ts,seq".to_string(),
        );
        reader_context.rebuild_record_context(String::new());
        // Both ordering fields are parsed (not just the first).
        assert_eq!(
            reader_context.record_context.ordering_field_names,
            vec!["ts".to_string(), "seq".to_string()],
            "comma-separated precombine must split into all ordering fields"
        );

        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("pk", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("ts", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("seq", arrow_schema::DataType::Int64, true),
        ]));

        let input_split = InputSplit::new(
            Some("f1-0_0-1-1_001.parquet".to_string()),
            None,
            Vec::new(),
            String::new(),
        );

        let result = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            Some(data_schema.clone()),
            Some(data_schema),
        );

        assert!(
            result.is_ok(),
            "multi-field precombine must be accepted at construction, got {:?}",
            result.err()
        );
    }

    /// A CoW slice never carries log files, so it reaches the gate through the
    /// "nothing merges" branch rather than through a table-type test.
    #[test]
    fn builder_cow_always_pushes_regardless_of_mor_pk_safe() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("COPY_ON_WRITE"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            // mor_pk_safe stays default false — irrelevant for CoW.
            .build()
            .unwrap();
        assert!(
            reader.base_read_pushdown_is_safe(),
            "CoW path always pushes regardless of mor_pk_safe"
        );
    }

    /// A merged chunk is bounded, and the bound is the reader's, not the base
    /// file's layout.
    ///
    /// Merging a chunk is synchronous work on the task that polls the stream and
    /// its cost is linear in the chunk's rows, so an unbounded chunk is an
    /// unbounded poll. The fixture puts 5000 rows in a single row group: if the
    /// chunk followed the file's layout, one chunk would carry all 5000 and one
    /// poll would do five times the work `MERGE_CHUNK_ROWS` allows for.
    ///
    /// The direct assertion on the option is deliberate. The bound currently
    /// agrees with what `parquet` defaults to, so no output-level test can tell
    /// the pin from the default — but a caller that passed a larger batch size
    /// through here (making `hoodie.read.stream.batch_size` effective on the
    /// merge path, say) would multiply every poll's cost, and this is what says
    /// so out loud.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_merged_chunk_is_bounded_by_the_readers_own_batch_size() {
        // Every argument combination must carry the pin: the position-merge
        // read (row-index column attached) and the filtered read bound their
        // polls by the same argument as the plain one, so a refactor that
        // branched the builder per arm must not lose it on any branch.
        for use_position in [false, true] {
            for filter in [None, Some(make_row_filter_builder())] {
                assert_eq!(
                    base_read_options(filter, None, None, None, use_position).batch_size,
                    Some(MERGE_CHUNK_ROWS),
                    "the base read must ask for the merge's chunk bound rather than \
                     inherit one (use_position={use_position})"
                );
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            true,
        )]));
        let rows = 5_000;
        let ids: Vec<i32> = (0..rows).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(ids.clone()))],
        )
        .unwrap();
        let base_name = "one-big-group.parquet";
        // One row group holding every row, so the file's layout cannot be what
        // bounds the chunk.
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, rows as usize);

        let mut reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let mut stream = reader.open_stream().await.unwrap();
        let mut sizes: Vec<usize> = Vec::new();
        let mut total = 0usize;
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            sizes.push(b.num_rows());
            total += b.num_rows();
        }
        assert_eq!(total, rows as usize, "every row must still come back");
        assert!(
            sizes.iter().all(|n| *n <= MERGE_CHUNK_ROWS),
            "every chunk must respect the bound, got {sizes:?}"
        );
        assert!(
            sizes.len() > 1,
            "5000 rows cannot arrive in one chunk under a {MERGE_CHUNK_ROWS}-row bound"
        );
    }

    /// Build a reader over a base file plus one real log file, so the read
    /// takes the Buffered (merge) path rather than the Eager one. The reader
    /// schema is the single `_hoodie_record_key` column, which is enough to
    /// decode the shipped log fixtures and extract keys on both sides.
    async fn test_file_group_reader_for_merged_slice(
        dir: &std::path::Path,
        base_name: &str,
        log_name: &str,
        required: SchemaRef,
    ) -> HoodieFileGroupReader {
        let base_path = dir.to_str().unwrap().to_string();
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref(),
            base_path,
        )]));
        let storage = Storage::new(Arc::new(HashMap::new()), hudi_configs).unwrap();

        let input_split = InputSplit::new(
            Some(base_name.to_string()),
            None,
            vec![log_name.to_string()],
            String::new(),
        );

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        // Set on purpose: the knob is documented to size base-file-only slices
        // and to have NO effect on a merged slice. The chunk assertions in the
        // test below are what hold that, rather than a one-off measurement.
        reader_context.hoodie_reader_config.insert(
            crate::config::read::HudiReadConfig::StreamBatchSize
                .as_ref()
                .to_string(),
            "8192".to_string(),
        );
        reader_context.rebuild_record_context(String::new());
        // The log scan decodes blocks and builds the delete context through the
        // context's own schema handler, so it needs the prepared one.
        let mut handler =
            crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler::new()
                .with_table_schema(required.clone())
                .with_data_schema(required.clone());
        handler
            .prepare_required_schema(
                true,
                &["_hoodie_record_key".to_string()],
                &[],
                &reader_context.table_config,
                false,
                "COMMIT_TIME_ORDERING",
            )
            .unwrap();
        reader_context.schema_handler = handler;

        let mut reader = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            ReaderParameters::default(),
            None,
            None,
        )
        .unwrap();
        reader.schema_handler.required_schema = Some(required);
        reader
    }

    /// The chunk bound on the path it exists for: a slice WITH log files.
    ///
    /// `test_a_merged_chunk_is_bounded_by_the_readers_own_batch_size` above
    /// drives the Eager (base-only) arm, so it pins the option and the base
    /// read but never the Buffered state machine. This one merges a 5000-row
    /// single-row-group base against a real delete-block log file, so every
    /// chunk it observes came out of `merge_base_batch`: a state machine that
    /// coalesced source batches, or a base read that lost the bound only on
    /// the merge route, fails here and nowhere else.
    ///
    /// The fixture's delete keys are trips UUIDs and the base keys are
    /// synthetic, so nothing matches: every base row survives, the drain has
    /// nothing to emit, and the chunk cadence is exactly what the machine
    /// produced. `hoodie.read.stream.batch_size=8192` is set in the reader
    /// config on purpose — the knob must have no effect on a merged slice.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_the_chunk_bound_holds_on_a_slice_with_log_files() {
        let tmp = tempfile::tempdir().unwrap();
        let log_name = ".6d3d1d6e-2298-4080-a0c1-494877d6f40a-0_20250618054711154.log.1_0-26-85";
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/log_files/valid_log_delete")
            .join(log_name);
        std::fs::copy(&fixture, tmp.path().join(log_name)).unwrap();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "_hoodie_record_key",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let base_rows: usize = 5_000;
        let keys: Vec<String> = (0..base_rows).map(|i| format!("base-{i:05}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(
                keys.iter().map(String::as_str).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let base_name = "one-big-group.parquet";
        // One row group holding every row, so the file's layout cannot be what
        // bounds the chunk.
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, base_rows);

        let mut eager = test_file_group_reader_for_merged_slice(
            tmp.path(),
            base_name,
            log_name,
            schema.clone(),
        )
        .await;
        let expected_total = eager.read().await.unwrap().num_rows();
        assert_eq!(
            expected_total, base_rows,
            "no fixture delete key may collide with a synthetic base key"
        );

        let mut reader = test_file_group_reader_for_merged_slice(
            tmp.path(),
            base_name,
            log_name,
            schema.clone(),
        )
        .await;
        let mut stream = reader.open_stream().await.unwrap();
        let mut sizes: Vec<usize> = Vec::new();
        while let Some(b) = stream.next().await {
            sizes.push(b.unwrap().num_rows());
        }
        assert_eq!(
            sizes.iter().sum::<usize>(),
            expected_total,
            "the streamed merge must return the same rows as the eager read"
        );
        assert!(
            sizes.iter().all(|n| *n <= MERGE_CHUNK_ROWS),
            "every merged chunk must respect the bound, got {sizes:?}"
        );
        assert!(
            sizes.len() >= base_rows / MERGE_CHUNK_ROWS,
            "{base_rows} base rows cannot arrive in {} chunk(s) under a \
             {MERGE_CHUNK_ROWS}-row bound: {sizes:?}",
            sizes.len()
        );
    }

    #[tokio::test]
    async fn open_and_open_stream_produce_the_same_rows() {
        use futures::StreamExt;
        // Two readers over the same file group: one drained via open() +
        // next_chunk(), one via open_stream(). The split in open_stream must be a
        // pure refactor, so the row sets must be identical.
        let tmp = tempfile::tempdir().unwrap();
        let log_name = ".6d3d1d6e-2298-4080-a0c1-494877d6f40a-0_20250618054711154.log.1_0-26-85";
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/log_files/valid_log_delete")
            .join(log_name);
        std::fs::copy(&fixture, tmp.path().join(log_name)).unwrap();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "_hoodie_record_key",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let base_rows: usize = 10;
        let keys: Vec<String> = (0..base_rows).map(|i| format!("base-{i:05}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(
                keys.iter().map(String::as_str).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let base_name = "mor-base.parquet";
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, base_rows);

        let mut a = test_file_group_reader_for_merged_slice(
            tmp.path(),
            base_name,
            log_name,
            schema.clone(),
        )
        .await;
        let mut b = test_file_group_reader_for_merged_slice(
            tmp.path(),
            base_name,
            log_name,
            schema.clone(),
        )
        .await;

        let mut via_open = Vec::new();
        let mut stream = a.open().await.unwrap();
        while let Some(chunk) = stream.next_chunk().await {
            via_open.push(chunk.unwrap());
        }

        let mut via_open_stream = Vec::new();
        let mut boxed = b.open_stream().await.unwrap();
        while let Some(chunk) = boxed.next().await {
            via_open_stream.push(chunk.unwrap());
        }

        let rows_open: usize = via_open.iter().map(|b| b.num_rows()).sum();
        let rows_stream: usize = via_open_stream.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows_open, rows_stream,
            "open() and open_stream() disagree on row count"
        );
        assert!(
            rows_open > 0,
            "fixture produced no rows; the test proves nothing"
        );
    }

    #[tokio::test]
    async fn open_exposes_the_in_memory_footprint() {
        // The reason open() exists: BoxStream hides current_in_memory_bytes(),
        // which the FFI publishes as hudi_reader_memory_bytes.
        let tmp = tempfile::tempdir().unwrap();
        let log_name = ".6d3d1d6e-2298-4080-a0c1-494877d6f40a-0_20250618054711154.log.1_0-26-85";
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/log_files/valid_log_delete")
            .join(log_name);
        std::fs::copy(&fixture, tmp.path().join(log_name)).unwrap();

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "_hoodie_record_key",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let base_rows: usize = 10;
        let keys: Vec<String> = (0..base_rows).map(|i| format!("base-{i:05}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(
                keys.iter().map(String::as_str).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let base_name = "mor-base.parquet";
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, base_rows);

        let mut r = test_file_group_reader_for_merged_slice(
            tmp.path(),
            base_name,
            log_name,
            schema.clone(),
        )
        .await;
        let stream = r.open().await.unwrap();
        assert!(
            stream.current_in_memory_bytes() > 0,
            "MOR slice with a log file must have a non-empty merge map after open()"
        );
    }

    /// Every row group of the base file reaches the output, on both entry
    /// points.
    ///
    /// `read()` collapses the base to one batch before merging, and a collapse
    /// that kept only the first row group would return fewer rows and raise
    /// nothing — the exact shape of silent data loss this path must not have.
    /// No other test can see it: every base file elsewhere in the suite fits in
    /// a single row group, so keeping one group and keeping all of them look
    /// identical. The stream side asserts more than one chunk, which is what
    /// proves the fixture really has several groups.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_every_base_row_group_reaches_the_output() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            true,
        )]));
        let ids: Vec<i32> = (0..40).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(ids.clone()))],
        )
        .unwrap();
        let base_name = "many-groups.parquet";
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, 10);

        let read_ids = |b: &RecordBatch| -> Vec<i32> {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        };

        let mut eager =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let one = eager.read().await.unwrap();
        assert_eq!(
            read_ids(&one),
            ids,
            "read() must return every row of every row group"
        );

        let mut streamed =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let mut stream = streamed.open_stream().await.unwrap();
        let mut chunks = 0usize;
        let mut got: Vec<i32> = Vec::new();
        while let Some(b) = stream.next().await {
            chunks += 1;
            got.extend(read_ids(&b.unwrap()));
        }
        assert!(
            chunks > 1,
            "the fixture must span several row groups for this test to mean anything, got {chunks}"
        );
        assert_eq!(got, ids, "the streamed read must return every row too");
    }

    /// The streaming path must return exactly what the single-batch one does.
    /// It merges the base file a row group at a time instead of whole, which is
    /// a memory and chunking difference, not a data one — so any divergence in
    /// the rows is a bug rather than a tradeoff.
    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_and_eager_reads_agree() {
        use futures::StreamExt;

        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        let base_name = "base.parquet";
        let file = std::fs::File::create(tmp.path().join(base_name)).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let mut eager_reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let eager = eager_reader.read().await.unwrap();

        let mut stream_reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let mut stream = stream_reader.open_stream().await.unwrap();
        let mut streamed: Vec<RecordBatch> = Vec::new();
        while let Some(b) = stream.next().await {
            streamed.push(b.unwrap());
        }
        assert!(
            !streamed.is_empty(),
            "the stream yielded nothing; it should emit at least one batch"
        );

        // Row content, not just a count. Counting alone passes for a stream that
        // returns the right number of wrong rows, which is the failure a merge
        // rewrite actually produces. Sorted, because the two entry points chunk
        // the base differently and Hudi promises no row order.
        let render = |batches: &[RecordBatch]| -> Vec<String> {
            let mut out: Vec<String> = batches
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        (0..b.num_columns())
                            .map(|c| {
                                format!(
                                    "{:?}",
                                    arrow::util::display::array_value_to_string(b.column(c), r)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .collect();
            out.sort();
            out
        };
        assert_eq!(
            render(&streamed),
            render(std::slice::from_ref(&eager)),
            "the streamed read must return the same rows as the single-batch read"
        );
    }

    // ── pushdown vs. the apache/hudi#18132 logical-type repair ────────────────

    /// A `ts > threshold` row filter that normalises the column to NANOSECONDS
    /// from its own DECLARED unit — the shape an engine's timestamp comparison
    /// takes when it reconciles a literal against the column type parquet reports.
    /// Counts its own invocations, so a test can assert the filter was never even
    /// built rather than inferring it from rows.
    fn nanos_gt_filter_builder(
        column: &'static str,
        threshold_nanos: i64,
        invocations: Arc<std::sync::atomic::AtomicUsize>,
    ) -> RowFilterBuilder {
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
        use std::sync::atomic::Ordering::Relaxed;
        Arc::new(move |parquet_schema, _projected_schema| {
            invocations.fetch_add(1, Relaxed);
            let root = parquet_schema.root_schema();
            let idx = root.get_fields().iter().position(|f| f.name() == column)?;
            let mask = ProjectionMask::roots(parquet_schema, [idx]);
            let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                use arrow_array::cast::AsArray;
                use arrow_array::types::{TimestampMicrosecondType, TimestampMillisecondType};
                let col = batch.column_by_name(column).ok_or_else(|| {
                    arrow_schema::ArrowError::ComputeError(format!(
                        "predicate column '{column}' missing from the predicate batch"
                    ))
                })?;
                // Scale the raw i64 to nanos using the unit the column DECLARES.
                // That declaration is precisely what a mislabelled file gets wrong,
                // so the scaling inherits the lie.
                let (values, per_unit): (Vec<i64>, i64) = match col.data_type() {
                    arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                        let a = col.as_primitive::<TimestampMicrosecondType>();
                        ((0..a.len()).map(|i| a.value(i)).collect(), 1_000)
                    }
                    arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                        let a = col.as_primitive::<TimestampMillisecondType>();
                        ((0..a.len()).map(|i| a.value(i)).collect(), 1_000_000)
                    }
                    other => {
                        return Err(arrow_schema::ArrowError::ComputeError(format!(
                            "unsupported predicate column type {other}"
                        )));
                    }
                };
                Ok(arrow_array::BooleanArray::from_iter(
                    values.iter().map(|v| Some(v * per_unit > threshold_nanos)),
                ))
            });
            Some(RowFilter::new(vec![Box::new(predicate)]))
        })
    }

    /// Like [`test_file_group_reader_for_base_file`], but also installs a row
    /// filter builder so the base read exercises the pushdown path.
    async fn test_file_group_reader_with_row_filter(
        dir: &std::path::Path,
        base_name: &str,
        required: SchemaRef,
        row_filter_builder: RowFilterBuilder,
        row_group_selector: Option<RowGroupSelector>,
        repair_risk_columns: &[&str],
    ) -> HoodieFileGroupReader {
        let mut reader = test_file_group_reader_for_base_file(dir, base_name, required).await;
        let mut context = (*reader.reader_context).clone();
        context.row_filter_builder = Some(row_filter_builder);
        context.row_group_selector = row_group_selector;
        // What `batch_evolution::repair_risk_columns` would have produced for this
        // predicate against this table schema — the gate that arms the per-file check.
        context.repair_risk_columns = repair_risk_columns.iter().map(|c| c.to_string()).collect();
        reader.reader_context = Arc::new(context);
        reader
    }

    /// 2020-01-01T00:00:00Z — the threshold the failing fixtures straddle.
    const THRESHOLD_NANOS: i64 = 1_577_836_800_000_000_000;
    /// 2020-01-01T00:00:00.001Z as MILLIS — above the threshold.
    const ABOVE_MS: i64 = 1_577_836_800_001;
    /// 2019-12-31T23:59:59.999Z as MILLIS — below it.
    const BELOW_MS: i64 = 1_577_836_799_999;

    fn ts_field(name: &str, unit: arrow_schema::TimeUnit) -> arrow_schema::Field {
        arrow_schema::Field::new(
            name,
            arrow_schema::DataType::Timestamp(unit, Some("UTC".into())),
            true,
        )
    }

    /// The table's view of the straddling file: `ts` is tz-aware MILLIS, which is
    /// what the stored i64s have always been.
    fn straddling_table_schema() -> SchemaRef {
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            ts_field("ts", arrow_schema::TimeUnit::Millisecond),
        ]))
    }

    /// Write a two-row base file whose `ts` column is DECLARED with `declared_unit`
    /// while its values are always the millisecond counts above. When
    /// `declared_unit` is micros this is the apache/hudi#18132 shape: the label is
    /// a lie and the repair has to reinterpret it on read.
    fn write_straddling_base_file(
        dir: &std::path::Path,
        name: &str,
        declared_unit: arrow_schema::TimeUnit,
    ) {
        let file_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            ts_field("ts", declared_unit),
        ]));
        let ts: arrow_array::ArrayRef = match declared_unit {
            arrow_schema::TimeUnit::Microsecond => Arc::new(
                arrow_array::TimestampMicrosecondArray::from(vec![ABOVE_MS, BELOW_MS])
                    .with_timezone("UTC"),
            ),
            _ => Arc::new(
                arrow_array::TimestampMillisecondArray::from(vec![ABOVE_MS, BELOW_MS])
                    .with_timezone("UTC"),
            ),
        };
        let batch = RecordBatch::try_new(
            file_schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["k1", "k2"])),
                ts,
            ],
        )
        .unwrap();
        write_parquet_file(dir, name, &batch);
    }

    /// THE REGRESSION. The file declares `ts` as tz-aware micros while the stored
    /// i64s are MILLIS, so a nanos-normalised predicate reads them as 1970 and
    /// `ts > 2020-01-01` matches nothing. The post-scan filter cannot restore the
    /// rows the scan already dropped.
    #[tokio::test]
    async fn base_read_declines_pushdown_when_the_file_needs_a_reinterpreting_repair() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(
            tmp.path(),
            base_name,
            arrow_schema::TimeUnit::Microsecond, // the LIE
        );

        let required = straddling_table_schema();
        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            required.clone(),
            builder,
            None,
            &["ts"],
        )
        .await;
        // The existing merge gate is satisfied: no log files, so nothing merges.
        assert!(
            reader.base_read_pushdown_is_safe(),
            "a slice with no log files clears the merge gate; the repair check \
             is what must decline this read"
        );
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            invocations.load(Relaxed),
            0,
            "the row filter must never even be BUILT for a file whose physical \
             timestamp labelling is repaired on read"
        );
        assert_eq!(out.schema(), required);
        assert_eq!(
            out.num_rows(),
            2,
            "both rows must reach the post-scan filter; dropping one inside the \
             scan is unrecoverable"
        );
        let ts = out
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            .expect("the repair must relabel the column to millis");
        assert_eq!(
            (ts.value(0), ts.value(1)),
            (ABOVE_MS, BELOW_MS),
            "and it must relabel the i64, not rescale it"
        );
    }

    /// The other half of the rule. Same values and predicate, but the file declares
    /// the unit it actually uses, so no repair applies and pushdown must survive —
    /// otherwise the guard is a blanket regression on every well-formed table.
    #[tokio::test]
    async fn base_read_keeps_pushdown_when_the_file_is_honestly_labelled() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Millisecond);

        let required = straddling_table_schema();
        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            required.clone(),
            builder,
            None,
            &["ts"],
        )
        .await;
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            invocations.load(Relaxed),
            1,
            "an honestly labelled file must keep its pushdown — the guard keys on \
             the FILE's own schema, not on the table's"
        );
        assert_eq!(
            out.num_rows(),
            1,
            "the pushed predicate keeps only the row above the threshold"
        );
        // The row count alone does not say WHICH row survived. A predicate pushed
        // against a rescaled column keeps exactly one row too -- the wrong one.
        let ts = out
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            .expect("an honest file keeps its declared millisecond unit");
        assert_eq!(
            ts.value(0),
            ABOVE_MS,
            "and the survivor must be the row above the threshold, by value"
        );
    }

    /// The narrowing. A file mislabels `ts`, but the predicate reads `other`, so
    /// nothing the predicate touches is misread and pushdown must be kept. Without
    /// the per-column scoping this file would lose pushdown for a predicate the
    /// repair cannot affect.
    #[tokio::test]
    async fn base_read_keeps_pushdown_for_a_predicate_on_an_unaffected_column() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        // `ts` mislabelled micros; `other` is honestly labelled millis.
        let file_schema = Arc::new(arrow_schema::Schema::new(vec![
            ts_field("ts", arrow_schema::TimeUnit::Microsecond),
            ts_field("other", arrow_schema::TimeUnit::Millisecond),
        ]));
        let batch = RecordBatch::try_new(
            file_schema,
            vec![
                Arc::new(
                    arrow_array::TimestampMicrosecondArray::from(vec![ABOVE_MS, BELOW_MS])
                        .with_timezone("UTC"),
                ),
                Arc::new(
                    arrow_array::TimestampMillisecondArray::from(vec![ABOVE_MS, BELOW_MS])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        write_parquet_file(tmp.path(), base_name, &batch);

        let required: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            ts_field("ts", arrow_schema::TimeUnit::Millisecond),
            ts_field("other", arrow_schema::TimeUnit::Millisecond),
        ]));
        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("other", THRESHOLD_NANOS, invocations.clone());

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            required,
            builder,
            None,
            // Gate 1 saw only `other`: it is the sole column the predicate reads.
            &["other"],
        )
        .await;
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            invocations.load(Relaxed),
            1,
            "a mislabelled column the predicate never reads must not cost pushdown"
        );
        assert_eq!(out.num_rows(), 1);
    }

    /// The unarmed gate. Same mislabelled file and same predicate column, but gate 1
    /// reported nothing at risk — the case of every table Spark wrote with micros.
    /// The per-file check must not run at all, so pushdown survives.
    #[tokio::test]
    async fn base_read_keeps_pushdown_when_no_predicate_column_is_at_risk() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Microsecond);

        let required = straddling_table_schema();
        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            required,
            builder,
            None,
            &[], // gate 1 disarmed
        )
        .await;
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            invocations.load(Relaxed),
            1,
            "an empty repair_risk_columns must skip the per-file check entirely"
        );
        // ...and the filter it installed must actually RUN. Counting the build alone
        // passes for a filter that was constructed and then never applied.
        //
        // The count is 0 rather than internal's 1 because the fixtures differ, and
        // deliberately: this file DECLARES micros while holding millisecond counts,
        // and with gate 1 disarmed nothing relabels it, so the filter normalises both
        // values from the declared unit and neither clears the threshold. The
        // honest-file case, where the survivor is the one row above it, is the
        // neighbouring `base_read_keeps_pushdown_when_the_file_is_honestly_labelled`.
        //
        // 0 is not this counter's uninformative initial value. The fixture holds two
        // rows, and the neighbouring
        // `base_read_declines_pushdown_when_the_file_needs_a_reinterpreting_repair`
        // asserts that both of them reach the output when this same file is read with
        // the repair ARMED (gate 1 on, so no filter is pushed). That test is not this
        // read minus the filter -- it passes `&["ts"]` where this passes `&[]`, so the
        // repair fires and relabels micros to millis -- but it does establish the row
        // count of the fixture. So a filter that is built and then never applied
        // returns 2 here and fails this assertion.
        assert_eq!(
            out.num_rows(),
            0,
            "the installed filter must be applied during the scan, not merely built"
        );
    }

    /// The table side of gate 2 is the TABLE schema, not the projection. A pushed
    /// predicate reads its columns whether or not they were projected, because the
    /// `RowFilter` builder derives its own `ProjectionMask` from the parquet schema.
    /// Here `ts` is mislabelled and absent from `required_schema`; reading the
    /// projection instead of the table schema would find nothing and push anyway.
    #[tokio::test]
    async fn base_read_declines_pushdown_for_an_unprojected_predicate_column() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Microsecond);

        // Projection keeps only the key; `ts` is filtered on but never returned.
        let required: SchemaRef =
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "_hoodie_record_key",
                arrow_schema::DataType::Utf8,
                true,
            )]));
        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            required,
            builder,
            None,
            &["ts"],
        )
        .await;
        reader.schema_handler.table_schema = Some(straddling_table_schema());
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            invocations.load(Relaxed),
            0,
            "the guard must consult the TABLE schema; a filter column outside the \
             projection is still decoded and still misread"
        );
        assert_eq!(out.num_rows(), 2);
    }

    /// The two conditions the tests above hold apart, crossed: a MISLABELLED file,
    /// a filter column pruned out of the projection, AND no table schema.
    ///
    /// `base_read_declines_pushdown_for_an_unprojected_predicate_column` supplies a
    /// table schema, and `with_no_table_schema_the_unprojected_path_withdraws_pushdown_anyway`
    /// drops the table schema but keeps `ts` inside the schema it passes as
    /// `required`. Neither reaches the shape where the projected path has no table
    /// side AND the risk column is outside the projection — which is exactly the
    /// shape `cpp/` produces whenever `fgrc.data_schema` is absent or fails to
    /// parse while `requested_schema` still does, since `repair_risk_columns` is
    /// computed fail-closed from the predicate and arrives populated anyway.
    ///
    /// Falling back to `required_schema` for the table side answers "no conflict"
    /// here — `reinterpreted_columns` skips `ts` because it is missing from that
    /// side — so the `RowFilter` is pushed against micros-labelled millis, sees
    /// 1970, and drops `k1`. The read returns 1 row and reports success.
    #[tokio::test]
    async fn an_out_of_projection_filter_column_withdraws_pushdown_with_no_table_schema() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        // THE LIE: micros declared, millis stored.
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Microsecond);

        // Projection keeps only the key; `ts` is filtered on but never returned.
        let required: SchemaRef =
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "_hoodie_record_key",
                arrow_schema::DataType::Utf8,
                true,
            )]));
        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            required,
            builder,
            None,
            &["ts"],
        )
        .await;
        // The crossing condition: no table side at all on the PROJECTED path.
        reader.schema_handler.table_schema = None;

        let volume = reader.storage.read_volume();
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            1,
            "with no table side the projected path must withdraw, not fall back to \
             the projection and conclude there is nothing to repair"
        );
        assert_eq!(
            invocations.load(Relaxed),
            0,
            "the filter must not have been pushed against a mislabelled column"
        );
        assert_eq!(
            out.num_rows(),
            2,
            "both rows survive; pushing here would have dropped the ABOVE_MS row"
        );
    }

    /// A withdrawal takes the row-group selector with it, and is counted on both
    /// counters: `row_group_selector_suppressed` so the existing "installed but
    /// never passed down" question stays answerable, and
    /// `pushdown_suppressed_by_repair` so its cause is separable from a
    /// merge-gate refusal.
    #[tokio::test]
    async fn repair_suppression_counts_the_row_group_selector() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Microsecond);

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());
        let selector_calls = Arc::new(AtomicUsize::new(0));
        let seen = selector_calls.clone();
        let selector: RowGroupSelector = Arc::new(move |_| {
            seen.fetch_add(1, Relaxed);
            Some(vec![0])
        });

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            Some(selector),
            &["ts"],
        )
        .await;
        let volume = reader.storage.read_volume();
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(out.num_rows(), 2);
        assert_eq!(selector_calls.load(Relaxed), 0, "the selector never ran");
        assert_eq!(volume.row_group_selector_calls.load(Relaxed), 0);
        assert_eq!(volume.row_group_selector_suppressed.load(Relaxed), 1);
        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            1,
            "the cause must be separable from a merge-gate refusal"
        );
    }

    /// THE UNPROJECTED PATH runs the repair gate too.
    ///
    /// `base_file_source` has an early return for `required_schema == None` that
    /// read with the row filter and selector straight from the MERGE gate and
    /// returned before the projected path's footer read — so the repair gate
    /// never ran there, and a #18132-mislabelled file kept a pushdown that drops
    /// rows which match, with the post-merge filter unable to restore them
    /// (ISSUES OI-11).
    ///
    /// It was latent rather than live: no FFI caller reaches this path, because
    /// the FFI always supplies a required schema. "Unreachable from the surface
    /// that ships today" is a property of the callers, not of this function.
    ///
    /// The filter-builder invocation count is the load-bearing assertion — the
    /// builder runs only if the filter was actually installed on the read, so
    /// `0` means withdrawn rather than merely "a counter moved".
    #[tokio::test(flavor = "multi_thread")]
    async fn the_unprojected_path_withdraws_pushdown_for_a_repair_conflict_too() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Microsecond);

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());
        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            None,
            &["ts"],
        )
        .await;
        // The table's view has to be reachable, because on this path there is no
        // `required_schema` to fall back to as the comparison side.
        reader.schema_handler.table_schema = Some(straddling_table_schema());
        // THE POINT OF THIS TEST: no projection schema, so `base_file_source`
        // takes the early return.
        reader.schema_handler.required_schema = None;
        assert!(
            reader.base_read_pushdown_is_safe(),
            "fixture check: the merge gate must PASS, or the repair gate is not \
             what withdrew anything"
        );

        let volume = reader.storage.read_volume();
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            out.num_rows(),
            2,
            "both rows survive — the mislabelled file is read unfiltered"
        );
        assert_eq!(
            invocations.load(Relaxed),
            0,
            "the row filter must never have been built, i.e. never pushed: a \
             predicate evaluated against this file's PHYSICAL values drops the \
             row that matches, and the post-merge filter cannot bring it back"
        );
        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            1,
            "and the withdrawal must be COUNTED on this path, with the same \
             counter the projected path uses — one gate, one telemetry"
        );
    }

    /// The control for the test above: on the same unprojected path, an HONESTLY
    /// labelled file KEEPS its pushdown.
    ///
    /// Without it, an early return hard-wired to withdraw — or a gate that simply
    /// refuses whenever `repair_risk_columns` is non-empty — passes every
    /// assertion above while disabling pushdown for every unprojected read.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_unprojected_path_keeps_pushdown_for_an_honest_file() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        // MILLIS declared and millis stored — the same fixture, telling the truth.
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Millisecond);

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());
        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            None,
            &["ts"],
        )
        .await;
        reader.schema_handler.table_schema = Some(straddling_table_schema());
        reader.schema_handler.required_schema = None;

        let volume = reader.storage.read_volume();
        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            0,
            "an honestly labelled file must not have its pushdown withdrawn"
        );
        assert!(
            invocations.load(Relaxed) > 0,
            "and the filter must actually have been pushed, or this control \
             passes for a file that was never filtered either way"
        );
    }

    /// With NO table schema to compare the footer against, the gate withdraws
    /// anyway — the conservative branch, and the only reachable case where
    /// pushdown is lost on an HONESTLY labelled file.
    ///
    /// Its pair is `the_unprojected_path_keeps_pushdown_for_an_honest_file`: same
    /// honest file, same predicate, same path — and pushdown is KEPT there,
    /// because a table schema is available. The only difference between the two is
    /// whether the question can be answered, so together they pin the fallback to
    /// "assume it reinterprets" rather than to "assume it is fine".
    ///
    /// That direction is the load-bearing one. Withdrawing when we cannot tell
    /// costs a full scan; keeping costs rows that match, which the post-merge
    /// filter cannot restore. A `None` branch returning `Vec::new()` looks
    /// obviously right and is the unsafe answer.
    #[tokio::test(flavor = "multi_thread")]
    async fn with_no_table_schema_the_unprojected_path_withdraws_pushdown_anyway() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        // HONEST: millis declared, millis stored. Nothing to repair.
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Millisecond);

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());
        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            None,
            &["ts"],
        )
        .await;
        // Neither side of the comparison is available.
        reader.schema_handler.table_schema = None;
        reader.schema_handler.required_schema = None;

        let volume = reader.storage.read_volume();
        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            1,
            "with no table schema the question cannot be answered, and the safe \
             answer is to withdraw"
        );
        assert_eq!(
            invocations.load(Relaxed),
            0,
            "and the filter must not have been pushed"
        );
    }

    /// The row-filter-only case, which `row_group_selector_suppressed` structurally
    /// cannot see: no selector was ever installed, so that counter stays zero while
    /// pushdown was still withdrawn.
    #[tokio::test]
    async fn repair_suppression_is_counted_without_a_row_group_selector() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Microsecond);

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            None,
            &["ts"],
        )
        .await;
        let volume = reader.storage.read_volume();
        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            volume.row_group_selector_suppressed.load(Relaxed),
            0,
            "no selector was installed, so that counter cannot speak for this case"
        );
        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            1,
            "the withdrawal must be counted for the row-filter side too"
        );
        // The counter alone cannot tell "a file was withdrawn" from "the scan ran":
        // the consequence of the withdrawal is that the filter is never even BUILT.
        assert_eq!(invocations.load(Relaxed), 0);
    }

    /// And it must stay at zero when pushdown survives, or the counter cannot
    /// distinguish "a file was withdrawn" from "the scan ran".
    #[tokio::test]
    async fn repair_suppression_is_not_counted_when_pushdown_survives() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::Relaxed;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Millisecond);

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());

        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            None,
            &["ts"],
        )
        .await;
        let volume = reader.storage.read_volume();
        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            volume.pushdown_suppressed_by_repair.load(Relaxed),
            0,
            "nothing was withdrawn on an honest file"
        );
        // `pushdown_suppressed_by_repair == 0` is that counter's INITIAL value, so it
        // holds whenever the repair path did not fire -- including when the row filter
        // was never built at all. Without this second assertion the test is named after
        // pushdown surviving and pins nothing about it.
        assert_eq!(
            invocations.load(Relaxed),
            1,
            "and the row filter was actually installed"
        );
    }

    // ── the repair gate vs. the INJECTED PROVIDER ────────────────────────────
    //
    // Ported from internal `file_group/reader/mod.rs`
    // (`repair_conflict_also_withdraws_provider_pushdown`) and re-expressed for
    // this tree: different file, different reader, different provider trait, and
    // a stub that lives in this module rather than that one — so this is a port
    // from tree, not a cherry-pick.
    //
    // The pair matters more than either half. `..._withdraws_provider_pushdown`
    // alone would pass against a seam hard-wired to `false`; `..._keeps_...`
    // alone would pass against the un-narrowed merge gate, which is exactly the
    // defect. Only both together pin the verdict to the per-file decision.

    /// THE PROVIDER PATH. `can_push_predicate` must carry the same verdict the
    /// in-process `RowFilter` got for this file.
    ///
    /// An injected provider told it may push applies the predicate to the file's
    /// own physical values and drops the same rows the parquet `RowFilter` would
    /// have — and the post-merge filter cannot restore them. So a guard that
    /// clears only `row_filter`/`row_group_selector` leaves the FFI reader
    /// exposed on precisely the files it exists for.
    ///
    /// Mutation proof: revert the `let pushdown_is_safe = pushdown_is_safe &&
    /// repair_conflict.is_empty();` rebinding in `base_file_source` and this test
    /// fails on the `!can_push` assertion, while every other test in this file
    /// still passes.
    #[tokio::test(flavor = "multi_thread")]
    async fn repair_conflict_also_withdraws_provider_pushdown() {
        use std::sync::atomic::AtomicUsize;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(
            tmp.path(),
            base_name,
            arrow_schema::TimeUnit::Microsecond, // the LIE
        );

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());
        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            None,
            &["ts"],
        )
        .await;
        // Declines to serve, so the read falls through to storage and the only
        // thing under test is the request the provider was handed.
        let provider = StubDataProvider::not_serving();
        reader.base_file_provider = Some(provider.clone());
        assert!(
            reader.base_read_pushdown_is_safe(),
            "the merge gate passes on a slice with no log files; the repair \
             conflict is what must withdraw the provider's pushdown"
        );

        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        let seen = provider
            .seen()
            .expect("the provider must have been offered the file");
        assert!(
            !seen.can_push_predicate,
            "a file needing a value-reinterpreting repair must withdraw PROVIDER \
             pushdown too, not only the in-process row filter"
        );
    }

    /// THE MERGE-GATE HALF. A merging split that has made no primary-key-safety
    /// claim must withdraw the provider's pushdown too, with no repair conflict
    /// anywhere in sight.
    ///
    /// Note what the gate actually reads: `base_read_pushdown_is_safe()` is
    /// `!has_log_files() || mor_pk_safe` and consults no predicate at all. So the
    /// fixture installs none — what makes the gate refuse is a log file on the
    /// split plus `mor_pk_safe == false`.
    ///
    /// `can_push_predicate` is a CONJUNCTION and each conjunct needs its own
    /// mutation to kill it. Dropping the repair conjunct is caught by
    /// [`repair_conflict_also_withdraws_provider_pushdown`]; dropping the MERGE
    /// conjunct — `let pushdown_is_safe = repair_conflict.is_empty();` — is caught
    /// only here, and would otherwise pass the whole file, because every other
    /// provider fixture builds a split with no log files and so clears the merge
    /// gate unconditionally. That mutation tells a provider it may push on exactly
    /// the MOR split whose log records the predicate has not seen yet, which is
    /// the same over-drop the repair narrowing exists to stop.
    ///
    /// Nothing here arms `repair_risk_columns`, so the repair conjunct is
    /// vacuously true and the merge gate is the only thing that can produce the
    /// `false`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_merging_split_withdraws_provider_pushdown_too() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::not_serving();
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema, provider.clone()).await;
        // A log file on the split and no PK-safety claim: the merge gate refuses.
        reader.input_split = InputSplit::new(
            Some(base_name.to_string()),
            Some("20240101120000000".to_string()),
            vec![".f1-0_20240101130000000.log.1_0-1-1".to_string()],
            String::new(),
        );
        assert!(
            !reader.base_read_pushdown_is_safe(),
            "fixture check: the merge gate must REFUSE here, or this test pins \
             nothing"
        );
        assert!(
            reader.reader_context.repair_risk_columns.is_empty(),
            "fixture check: the repair guard must be DISARMED here, or the \
             withdrawal below could come from the wrong conjunct"
        );

        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        let seen = provider
            .seen()
            .expect("the provider must have been offered the file");
        assert!(
            !seen.can_push_predicate,
            "a merging split with no primary-key-safety claim must withdraw \
             PROVIDER pushdown, exactly as it withdraws the in-process row filter"
        );
    }

    /// And the merge gate's OTHER branch: a merging split that DOES claim
    /// primary-key safety must keep provider pushdown.
    ///
    /// Without this, `can_push_predicate: pushdown_is_safe && !has_log_files()`
    /// — which silently drops the `mor_pk_safe` widening and costs every
    /// PK-safe MOR split its provider-side pushdown — survives all three of the
    /// tests above, because none of them has both a log file and `mor_pk_safe`.
    /// Strictly narrower than the delivered verdict, so it cannot over-drop rows;
    /// it is a performance regression rather than a correctness one, and this is
    /// what keeps it from being silent.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_pk_safe_merging_split_keeps_provider_pushdown() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::not_serving();
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema, provider.clone()).await;
        reader.input_split = InputSplit::new(
            Some(base_name.to_string()),
            Some("20240101120000000".to_string()),
            vec![".f1-0_20240101130000000.log.1_0-1-1".to_string()],
            String::new(),
        );
        {
            let ctx = Arc::get_mut(&mut reader.reader_context).expect("sole owner in test");
            ctx.mor_pk_safe = true;
        }
        assert!(
            reader.input_split.has_log_files(),
            "fixture check: the split must MERGE, or this is the same case as the \
             no-log-files fixtures"
        );
        assert!(
            reader.base_read_pushdown_is_safe(),
            "fixture check: mor_pk_safe must open the gate, or the assertion below \
             passes for the wrong reason"
        );

        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        let seen = provider
            .seen()
            .expect("the provider must have been offered the file");
        assert!(
            seen.can_push_predicate,
            "a primary-key-safe predicate keeps pushdown on a merging split, and \
             the provider must be told so"
        );
    }

    /// THE VALUES CONTRACT. A provider serves the file's PHYSICAL values and
    /// hudi-core applies the #18132 repair to them, exactly as it does to a file
    /// read from object storage.
    ///
    /// The provider is handed `intersection`, which carries the FILE's schema —
    /// so on a mislabelled file it is told micros while the stored i64s are
    /// millis. It serves them unaltered; `served_batch_stream`'s
    /// `project_batch_to_schema` then RELABELS the column to the table's millis
    /// without touching the value.
    ///
    /// Without this test the values contract on `try_base_file` is documentation
    /// with nothing behind it: deleting the projection from the served path is a
    /// silent data-corruption change on the production FFI path, and every other
    /// provider test uses an int32 column that the repair never touches.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_served_batch_gets_the_same_logical_type_repair_as_a_read_one() {
        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        // On disk: the #18132 shape. The provider will serve DIFFERENT values in
        // the same mislabelled shape, so the assertion can only pass if the
        // PROVIDER's batch is what reached the caller.
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Microsecond);

        // What the provider serves: the file's own (lying) schema, stored millis.
        let served_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_hoodie_record_key", arrow_schema::DataType::Utf8, true),
            ts_field("ts", arrow_schema::TimeUnit::Microsecond),
        ]));
        let served = RecordBatch::try_new(
            served_schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["p1", "p2"])),
                Arc::new(
                    arrow_array::TimestampMicrosecondArray::from(vec![ABOVE_MS + 7, BELOW_MS + 7])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();

        let provider = StubDataProvider::serving(vec![served]);
        let mut reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, straddling_table_schema())
                .await;
        reader.base_file_provider = Some(provider.clone());

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        // THE REQUEST SIDE of the same contract, and the only place in this file
        // that can pin it: every other provider fixture uses an `id: int32`
        // column, where `intersection` and `required_schema` are equal and the
        // two are indistinguishable.
        //
        // `projected_schema` must be the INTERSECTION — the file's own footer,
        // lie included — not the table's `required_schema`. Handing over the
        // table's schema tells a provider "millis" about a file whose footer says
        // "micros"; a conforming provider casts on read, dividing by 1000, and
        // `project_batch_to_schema` then sees millis→millis and applies NO repair.
        // Corruption with the repair arm bypassed, on the exact #18132 path this
        // milestone exists for — and it type-checks.
        let seen = provider
            .seen()
            .expect("the provider must have been offered the file");
        assert_eq!(
            seen.projected_schema.field(1).data_type(),
            &arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("UTC".into())
            ),
            "the provider is handed the FILE's schema, lie included — not the \
             table's. Serving raw values is only correct if the request says what \
             the file claims they are"
        );

        let ts = out
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            .expect("the repair must RELABEL the served column to the table's millis");
        assert_eq!(
            (ts.value(0), ts.value(1)),
            (ABOVE_MS + 7, BELOW_MS + 7),
            "and must not rescale: the served i64s must arrive byte-for-byte, and \
             they must be the PROVIDER's values, not the file's"
        );
    }

    /// The other half, and the reason the assertion above is not vacuous: the
    /// same fixture with an honestly labelled file must still hand the provider
    /// `true`.
    ///
    /// Without this, `can_push_predicate: false` — a blanket regression that
    /// costs every well-formed table its provider-side pushdown — passes the test
    /// above.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_honestly_labelled_file_keeps_provider_pushdown() {
        use std::sync::atomic::AtomicUsize;

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_straddling_base_file(tmp.path(), base_name, arrow_schema::TimeUnit::Millisecond);

        let invocations = Arc::new(AtomicUsize::new(0));
        let builder = nanos_gt_filter_builder("ts", THRESHOLD_NANOS, invocations.clone());
        let mut reader = test_file_group_reader_with_row_filter(
            tmp.path(),
            base_name,
            straddling_table_schema(),
            builder,
            None,
            &["ts"], // the guard is ARMED; the file is simply honest
        )
        .await;
        let provider = StubDataProvider::not_serving();
        reader.base_file_provider = Some(provider.clone());

        let _ = drain_base_source(reader.base_file_source().await.unwrap()).await;

        let seen = provider
            .seen()
            .expect("the provider must have been offered the file");
        assert!(
            seen.can_push_predicate,
            "an armed guard over an honestly labelled file must leave provider \
             pushdown intact"
        );
    }

    #[test]
    fn builder_routes_repair_risk_columns_into_reader_context() {
        // NO `with_row_filter_builder` here, deliberately. `build()`'s decision to
        // clone the context is a disjunction, and a sibling call would satisfy it
        // independently — leaving the `repair_risk_columns` disjunct pinned by
        // nothing, so deleting it from that condition would pass this test.
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_repair_risk_columns(vec!["ts".to_string()])
            .build()
            .unwrap();
        assert_eq!(
            reader.reader_context.repair_risk_columns,
            vec!["ts".to_string()],
            "with_repair_risk_columns should land on reader_context"
        );
    }

    #[test]
    fn builder_leaves_repair_risk_columns_empty_by_default() {
        let storage = Storage::new_with_base_url(parse_uri("file:///tmp").unwrap()).unwrap();
        let reader = HoodieFileGroupReader::builder()
            .with_reader_context(dummy_reader_context("MERGE_ON_READ"))
            .with_storage(storage)
            .with_input_split(dummy_input_split())
            .with_row_filter_builder(make_row_filter_builder())
            .build()
            .unwrap();
        assert!(
            reader.reader_context.repair_risk_columns.is_empty(),
            "unset must leave the guard disarmed, not populated by accident"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // Injected base-file data provider (`base_file_provider`).
    //
    // hudi-core defines the trait and never implements it, so these tests
    // supply their own in-module stub. That is the point: the whole contract —
    // served data is used, `None` falls through, counters land in the shared
    // slot and in `HoodieReadStats::base_file_provider` — is verifiable with no
    // concrete provider in the picture.
    // ════════════════════════════════════════════════════════════════════

    /// Test-only [`BaseFileDataProvider`] returning a canned outcome and
    /// recording the request it was handed.
    struct StubDataProvider {
        /// Batches to serve; `None` reports a storage fallback.
        serve: Option<Vec<RecordBatch>>,
        /// When set, the served reader yields `serve`'s batches and *then* an
        /// error — a provider that reports SERVED and dies mid-stream, the shape
        /// the streaming contract makes query-visible.
        fail_after_serving: bool,
        /// When set, the served reader yields `serve`'s batches and then PANICS,
        /// which is what arrow-rs does on a malformed `ArrowArray`/`ArrowSchema`
        /// — i.e. what a buggy C provider actually produces.
        panic_after_serving: bool,
        /// Which PAYLOAD the panic carries: a bare literal (`&'static str`) or an
        /// interpolated message (`String`). `panic_message` has an arm for each
        /// and only one of them used to be reachable from a test.
        panic_is_literal: bool,
        /// When set, the SERVED stats carry non-zero drain counters — a provider
        /// filling them against contract, which is the only shape that can tell
        /// whether the decline path zeroes them. Every other fixture leaves all
        /// three at `Default::default()`, so an assertion that they are zero on a
        /// decline is satisfied by the fixture rather than by the code.
        serve_with_drain_counters: bool,
        /// The request fields of the last call.
        seen: StdMutex<Option<SeenRequest>>,
    }

    /// What [`StubDataProvider`] records off a [`BaseFileDataRequest`]. A struct
    /// rather than a tuple so an assertion names the field it is checking.
    #[derive(Clone, Debug, PartialEq)]
    struct SeenRequest {
        file_uri: String,
        projected_schema: SchemaRef,
        can_push_predicate: bool,
        partition_path: String,
        partition_fields: Vec<String>,
        data_schema: Option<SchemaRef>,
    }

    impl StubDataProvider {
        fn serving(batches: Vec<RecordBatch>) -> Arc<Self> {
            Arc::new(Self {
                serve: Some(batches),
                fail_after_serving: false,
                panic_after_serving: false,
                panic_is_literal: false,
                serve_with_drain_counters: false,
                seen: StdMutex::new(None),
            })
        }

        /// Serves `batches`, then errors instead of ending the stream cleanly.
        fn serving_then_failing(batches: Vec<RecordBatch>) -> Arc<Self> {
            Arc::new(Self {
                serve: Some(batches),
                fail_after_serving: true,
                panic_after_serving: false,
                panic_is_literal: false,
                serve_with_drain_counters: false,
                seen: StdMutex::new(None),
            })
        }

        /// Serves `batches`, then PANICS with an interpolated message (a `String`
        /// payload) instead of ending the stream cleanly.
        fn serving_then_panicking(batches: Vec<RecordBatch>) -> Arc<Self> {
            Arc::new(Self {
                serve: Some(batches),
                fail_after_serving: false,
                panic_after_serving: true,
                panic_is_literal: false,
                serve_with_drain_counters: false,
                seen: StdMutex::new(None),
            })
        }

        /// As [`Self::serving_then_panicking`], but the panic is a bare LITERAL —
        /// a `&'static str` payload, which is what `unwrap`, `expect`, `assert!`
        /// and arrow-rs's FFI-import panics all produce.
        fn serving_then_panicking_with_a_literal(batches: Vec<RecordBatch>) -> Arc<Self> {
            Arc::new(Self {
                serve: Some(batches),
                fail_after_serving: false,
                panic_after_serving: true,
                panic_is_literal: true,
                serve_with_drain_counters: false,
                seen: StdMutex::new(None),
            })
        }

        /// Serves `batches` AND reports drain counters, which a conforming
        /// provider must not do — those are hudi-rs's to count as it drains.
        /// Used to prove the decline path zeroes them rather than folding a
        /// provider's numbers into the live slot for a file nothing drained.
        fn serving_with_drain_counters(batches: Vec<RecordBatch>) -> Arc<Self> {
            Arc::new(Self {
                serve: Some(batches),
                fail_after_serving: false,
                panic_after_serving: false,
                panic_is_literal: false,
                serve_with_drain_counters: true,
                seen: StdMutex::new(None),
            })
        }

        fn not_serving() -> Arc<Self> {
            Arc::new(Self {
                serve: None,
                fail_after_serving: false,
                panic_after_serving: false,
                panic_is_literal: false,
                serve_with_drain_counters: false,
                seen: StdMutex::new(None),
            })
        }

        fn seen(&self) -> Option<SeenRequest> {
            self.seen.lock().unwrap().clone()
        }
    }

    /// The error a `serving_then_failing` stub injects. Matched on by the
    /// mid-stream tests so they cannot pass on some unrelated failure.
    const STUB_MID_STREAM_ERROR: &str = "stub provider died mid-stream";

    /// The panic message a `serving_then_panicking` stub raises. Matched on so
    /// the test cannot pass on some unrelated panic.
    const STUB_MID_STREAM_PANIC: &str = "stub provider panicked mid-stream";

    /// The message the LITERAL-payload variant raises. A separate constant
    /// because the point is the payload TYPE: this one is raised as
    /// `panic!("<literal>")`, which yields `&'static str`, where
    /// [`STUB_MID_STREAM_PANIC`] is interpolated and yields `String`. The two
    /// take different arms of `panic_message`, and one arm was pinned by nothing.
    const STUB_LITERAL_PANIC: &str = "stub provider panicked with a literal";

    /// A `RecordBatchReader` that yields `items` and then panics.
    ///
    /// `RecordBatchIterator` cannot express this — its item type is a `Result`,
    /// and a panic is neither variant. Which is the point: a panic is not a value
    /// the stream contract can carry, and before the `catch_unwind` in
    /// `served_batch_stream` it silently became end-of-stream (ISSUES OI-2).
    struct PanickingReader {
        items: std::vec::IntoIter<RecordBatch>,
        schema: SchemaRef,
        /// `true` raises a bare literal (`&'static str` payload), `false` an
        /// interpolated message (`String`). The two exercise different arms of
        /// `panic_message`.
        literal: bool,
    }

    impl Iterator for PanickingReader {
        type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
        fn next(&mut self) -> Option<Self::Item> {
            match self.items.next() {
                Some(b) => Some(Ok(b)),
                // Deliberately NOT `panic!("{STUB_LITERAL_PANIC}")` — that would
                // interpolate and yield a `String`, which is the other arm.
                None if self.literal => panic!("stub provider panicked with a literal"),
                None => panic!("{STUB_MID_STREAM_PANIC}"),
            }
        }
    }

    impl arrow_array::RecordBatchReader for PanickingReader {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::file_group::reader_v2::base_file_provider::BaseFileDataProvider for StubDataProvider {
        async fn try_base_file(
            &self,
            req: BaseFileDataRequest<'_>,
        ) -> (
            Option<Box<dyn arrow_array::RecordBatchReader + Send + 'static>>,
            BaseFileProviderStats,
        ) {
            *self.seen.lock().unwrap() = Some(SeenRequest {
                file_uri: req.file_uri.to_string(),
                projected_schema: req.projected_schema.clone(),
                can_push_predicate: req.can_push_predicate,
                partition_path: req.partition_path.to_string(),
                partition_fields: req.partition_fields.to_vec(),
                data_schema: req.data_schema.cloned(),
            });
            match &self.serve {
                Some(batches) => {
                    // Serve the batches as a lazy reader, matching the real
                    // provider's streaming contract.
                    let schema = batches
                        .first()
                        .map(|b| b.schema())
                        .unwrap_or_else(|| req.projected_schema.clone());
                    let mut items: Vec<std::result::Result<RecordBatch, arrow_schema::ArrowError>> =
                        batches.clone().into_iter().map(Ok).collect();
                    if self.fail_after_serving {
                        items.push(Err(arrow_schema::ArrowError::ExternalError(Box::new(
                            std::io::Error::other(STUB_MID_STREAM_ERROR),
                        ))));
                    }
                    let reader: Box<dyn arrow_array::RecordBatchReader + Send + 'static> =
                        if self.panic_after_serving {
                            Box::new(PanickingReader {
                                items: batches.clone().into_iter(),
                                schema,
                                literal: self.panic_is_literal,
                            })
                        } else {
                            Box::new(arrow_array::RecordBatchIterator::new(
                                items.into_iter(),
                                schema,
                            ))
                        };
                    (
                        Some(reader),
                        BaseFileProviderStats {
                            files_served: 1,
                            // Distinct non-zero values so a single surviving
                            // assignment is identifiable, not just "non-zero".
                            rows_served: if self.serve_with_drain_counters {
                                11
                            } else {
                                0
                            },
                            bytes_materialized: if self.serve_with_drain_counters {
                                22
                            } else {
                                0
                            },
                            batches_received: if self.serve_with_drain_counters {
                                33
                            } else {
                                0
                            },
                            ..Default::default()
                        },
                    )
                }
                None => (
                    None,
                    BaseFileProviderStats {
                        storage_fallbacks: 1,
                        ..Default::default()
                    },
                ),
            }
        }
    }

    /// One-column `{id: int32}` batch — the smallest thing both the on-disk base
    /// file and the stubbed provider can carry, so a value difference proves
    /// which source the reader actually used.
    fn id_batch(values: Vec<i32>) -> (SchemaRef, RecordBatch) {
        use arrow_array::Int32Array;
        let schema: SchemaRef =
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int32,
                true,
            )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap();
        (schema, batch)
    }

    /// A TWO-column fixture, because a one-column one cannot express the cases
    /// that matter most.
    ///
    /// `id_batch` is one `id: int32`, and every provider test was built on it.
    /// That makes three shape deviations inexpressible: a DROPPED field (dropping
    /// the only column leaves a zero-field batch), a MIS-ORDERED pair, and any
    /// mismatch at an index past 0 — so the comparison loop's body was only ever
    /// entered at `i == 0`. Review round 8 built mutation 25 out of exactly that
    /// gap: narrowing `served.fields().len() != wanted.fields().len()` to `>`
    /// accepts a provider that drops a column, which is then null-filled, and all
    /// 62 engine tests stayed green.
    fn id_amount_batch(ids: Vec<i32>, amounts: Vec<i32>) -> (SchemaRef, RecordBatch) {
        use arrow_array::Int32Array;
        let schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("amount", arrow_schema::DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(amounts)),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    /// Column `idx` of a batch, as i32s, with nulls surfaced as `None` — because
    /// "the provider's values" and "null-filled" have to be distinguishable, and
    /// that distinction IS the finding.
    fn i32_col(batch: &RecordBatch, idx: usize) -> Vec<Option<i32>> {
        use arrow_array::Int32Array;
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..col.len())
            .map(|i| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            })
            .collect()
    }

    fn id_values(batch: &RecordBatch) -> Vec<i32> {
        use arrow_array::Int32Array;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..col.len()).map(|i| col.value(i)).collect()
    }

    /// A reader over `base_name` with `provider` injected through the same
    /// builder field the FFI bridge sets.
    async fn reader_with_provider(
        dir: &std::path::Path,
        base_name: &str,
        required: SchemaRef,
        provider: Arc<StubDataProvider>,
    ) -> HoodieFileGroupReader {
        let mut reader = test_file_group_reader_for_base_file(dir, base_name, required).await;
        reader.base_file_provider = Some(provider);
        reader
    }

    /// Served data is used instead of the object-store read, and is counted.
    ///
    /// The provider's batch deliberately holds different values from the parquet
    /// file on disk, so the assertion can only pass if the provider's data — not
    /// the file's — reached the caller.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_data_is_served_instead_of_the_base_file() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let (_, served) = id_batch(vec![7, 8]);
        let provider = StubDataProvider::serving(vec![served]);
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider).await;

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;
        assert_eq!(
            id_values(&out),
            vec![7, 8],
            "the provider's batch must be served, not the file on disk"
        );
        let stats = reader.base_file_provider_live_stats();
        let stats = stats.lock().unwrap();
        assert_eq!(stats.files_served, 1, "the served file is counted");
        assert_eq!(stats.storage_fallbacks, 0);
    }

    /// The served source is **lazy**: its drain counters (rows/bytes/batches)
    /// fill the FFI-observable slot only as batches are pulled, not up front.
    /// That is the memory contract — the whole served file is never resident at
    /// serve time — and a counter populated early is the symptom of losing it.
    ///
    /// The pre-drain bound is `<= 2`, not `== 0`, and the difference is a real
    /// flake this milestone had to fix rather than a weakening. `== 0` assumes
    /// the producer has not been scheduled yet, which nothing guarantees: it runs
    /// concurrently and, by the time this assertion reads the slot, can
    /// legitimately have counted TWO — one batch occupying the depth-1 channel's
    /// single permit and one parked in `blocking_send`. Nothing has been
    /// DELIVERED at this point, because the receiver has not been polled; the
    /// third batch in `a_served_reader_runs_two_batches_ahead_and_no_further`'s
    /// bound is the one the merge has already taken, and that test pulls a batch
    /// first. Two is also what `try_base_file`'s own contract says ("pulled up to
    /// two batches ahead"). Under full-suite load the inherited `== 0` failed on
    /// exactly this — intermittently, which is worse than failing.
    ///
    /// FIVE served batches rather than two, so the bound is meaningful: with two,
    /// "at most two" is vacuous. What the test now says is the contract — the
    /// source is not drained up front — instead of a timing accident.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_served_source_is_lazy_and_counts_on_drain() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        // Five served batches, so "at most three pulled before a drain" is a real
        // bound rather than a restatement of the batch count.
        let served: Vec<RecordBatch> = [vec![7, 8], vec![9], vec![10], vec![11], vec![12]]
            .into_iter()
            .map(|v| id_batch(v).1)
            .collect();
        let provider = StubDataProvider::serving(served);
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider).await;

        let live = reader.base_file_provider_live_stats();
        let source = reader.base_file_source().await.unwrap();

        // Setup counter seeded at serve time; the drain counters may have moved,
        // but only as far as the read-ahead allows.
        {
            let s = live.lock().unwrap();
            assert_eq!(s.files_served, 1, "setup counter seeded before drain");
            assert!(
                s.batches_received <= 2,
                "the source must not be drained up front — the receiver has not \
                 been polled, so at most one batch occupies the depth-1 channel \
                 and one is blocked in send, saw {}",
                s.batches_received
            );
            assert!(
                s.rows_served <= 3,
                "and the rows with them (the first two batches hold 2+1), saw {}",
                s.rows_served
            );
        }

        // Draining the lazy source fills the drain counters in place.
        let out = drain_base_source(source).await;
        assert_eq!(
            id_values(&out),
            vec![7, 8, 9, 10, 11, 12],
            "served data, streamed"
        );
        let s = live.lock().unwrap();
        assert_eq!(s.batches_received, 5, "every served batch counted on drain");
        assert_eq!(s.rows_served, 6, "all served rows counted on drain");
        // Bound it rather than just `> 0`: six i32 values plus validity cannot
        // plausibly need a megabyte, and an unbounded assertion would pass on a
        // counter that had accumulated garbage.
        assert!(
            (1..1_048_576).contains(&s.bytes_materialized),
            "materialized bytes counted on drain, within a sane range: {}",
            s.bytes_materialized
        );
    }

    /// A served stream whose schema is not the one the provider was asked for is
    /// DECLINED, and the read falls back to object storage.
    ///
    /// This is the one provider mistake that is otherwise invisible.
    /// `project_batch_to_schema` resolves by NAME and null-fills a name it cannot
    /// find, because that is the correct behaviour for a column genuinely absent
    /// from an older base file. A provider that renames, reorders, retypes or
    /// drops a column therefore does not fail — it produces a successful read with
    /// nulls where the data was, on the production FFI path, with no error and no
    /// counter moving.
    ///
    /// Each case is driven end to end rather than against
    /// `served_schema_mismatch` directly: a unit test of the comparator would
    /// still pass with the call site deleted, which is the mutation that matters.
    /// The parquet file holds 1, 2 and every stub serves 7, 8, so "fell back"
    /// and "was served" are distinguishable in the OUTPUT, not just in a counter.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_served_stream_at_the_wrong_schema_is_declined_not_null_filled() {
        use arrow_array::{Int32Array, Int64Array};
        use arrow_schema::{DataType, Field, Schema};

        let wrong: Vec<(&str, RecordBatch)> = vec![
            (
                "renamed column",
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "ident",
                        DataType::Int32,
                        true,
                    )])),
                    vec![Arc::new(Int32Array::from(vec![7, 8]))],
                )
                .unwrap(),
            ),
            (
                "retyped column",
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
                    vec![Arc::new(Int64Array::from(vec![7i64, 8]))],
                )
                .unwrap(),
            ),
            (
                "extra column",
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int32, true),
                        Field::new("extra", DataType::Int32, true),
                    ])),
                    vec![
                        Arc::new(Int32Array::from(vec![7, 8])),
                        Arc::new(Int32Array::from(vec![70, 80])),
                    ],
                )
                .unwrap(),
            ),
        ];

        for (case, served) in wrong {
            let tmp = tempfile::tempdir().unwrap();
            let (schema, on_disk) = id_batch(vec![1, 2]);
            let base_name = "f1-0_0-1-1_001.parquet";
            write_parquet_file(tmp.path(), base_name, &on_disk);

            // Reports drain counters it has no business reporting. A conforming
            // provider leaves them at zero, which is exactly why the plain
            // `serving` stub cannot test the zeroing on the decline path: with the
            // fixture at `Default::default()`, deleting all three assignments
            // leaves any "they are zero" assertion passing.
            let provider = StubDataProvider::serving_with_drain_counters(vec![served]);
            let mut reader =
                reader_with_provider(tmp.path(), base_name, schema.clone(), provider.clone()).await;
            let live = reader.base_file_provider_live_stats();

            let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

            // Fixture check, not a backstop: if the provider were never offered
            // the file, `storage_fallbacks == 1` below would fail first. It is here
            // so THAT failure is diagnosable.
            assert!(
                provider.seen().is_some(),
                "{case}: fixture check — the provider must have been offered the file"
            );
            assert_eq!(
                id_values(&out),
                vec![1, 2],
                "{case}: the FILE's rows must reach the caller. Seeing [7, 8] means the \
                 wrong-shaped serve was accepted; seeing nulls means it was accepted and \
                 null-filled"
            );
            let s = live.lock().unwrap();
            assert_eq!(
                s.files_served, 0,
                "{case}: a declined serve must not stay counted as a served file"
            );
            assert_eq!(
                s.storage_fallbacks, 1,
                "{case}: and must be counted as the storage fallback it became, or the \
                 decline is silent to an operator"
            );
            assert_eq!(
                (s.rows_served, s.bytes_materialized, s.batches_received),
                (0, 0, 0),
                "{case}: nothing was drained from the declined stream, so the \
                 provider's own (11, 22, 33) must not survive into the live slot"
            );
        }
    }

    /// A served stream that DROPS a column, or REORDERS two, is declined — the
    /// two cases the one-column fixture could not express.
    ///
    /// Separate from its sibling above because it needs a two-column file, and
    /// that is the whole point. Review round 8 built mutation 25 here: narrowing
    /// the field-count guard from `!=` to `>` is one token, reads as a deliberate
    /// relaxation ("serving FEWER fields is the benign case null-fill was designed
    /// for"), and reopens the identical silent-data-loss path row 22 closed —
    /// invisibly, because dropping the only column of a one-column fixture leaves
    /// a zero-field batch, so the `served < wanted` direction was not merely
    /// untested but unrepresentable.
    ///
    /// The argument the mutation misses is at the CALL SITE, not at the
    /// comparator: every field in `intersection` was read from THIS file's footer,
    /// so none of them can be legitimately absent from a serve of this file. A
    /// provider that omits one is not serving an evolved file; it is serving the
    /// wrong bytes. The comparator's own doc now says so.
    ///
    /// The reorder case also drives the comparison loop past `i == 0` for the
    /// first time — every earlier case is detected at field 0.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_served_stream_that_drops_or_reorders_a_column_is_declined() {
        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field, Schema};

        let dropped = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![7, 8]))],
        )
        .unwrap();
        // Both names present, both types right, the COUNT right — only the order
        // differs. Nothing but a positional comparison catches this one.
        let reordered = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Int32, true),
                Field::new("id", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![700, 800])),
                Arc::new(Int32Array::from(vec![7, 8])),
            ],
        )
        .unwrap();

        for (case, served) in [("dropped column", dropped), ("reordered pair", reordered)] {
            let tmp = tempfile::tempdir().unwrap();
            let (schema, on_disk) = id_amount_batch(vec![1, 2], vec![100, 200]);
            let base_name = "f1-0_0-1-1_001.parquet";
            write_parquet_file(tmp.path(), base_name, &on_disk);

            // Reports drain counters it has no business reporting. A conforming
            // provider leaves them at zero, which is exactly why the plain
            // `serving` stub cannot test the zeroing on the decline path: with the
            // fixture at `Default::default()`, deleting all three assignments
            // leaves any "they are zero" assertion passing.
            let provider = StubDataProvider::serving_with_drain_counters(vec![served]);
            let mut reader =
                reader_with_provider(tmp.path(), base_name, schema.clone(), provider.clone()).await;
            let live = reader.base_file_provider_live_stats();

            let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

            assert!(
                provider.seen().is_some(),
                "{case}: fixture check — the provider must have been offered the file"
            );
            assert_eq!(
                i32_col(&out, 0),
                vec![Some(1), Some(2)],
                "{case}: the FILE's `id` must reach the caller, not the provider's [7, 8]"
            );
            // The column the provider omitted is the one that goes silently to
            // nulls when the count guard is relaxed. Assert its VALUES, not just
            // that a column exists.
            assert_eq!(
                i32_col(&out, 1),
                vec![Some(100), Some(200)],
                "{case}: the FILE's `amount` must reach the caller. [None, None] means the \
                 wrong-shaped serve was accepted and the missing column null-filled — the \
                 silent data loss this whole check exists to prevent"
            );
            let s = live.lock().unwrap();
            assert_eq!(
                (s.files_served, s.storage_fallbacks),
                (0, 1),
                "{case}: a declined serve is reclassified as the storage fallback it became"
            );
            assert_eq!(
                (s.rows_served, s.bytes_materialized, s.batches_received),
                (0, 0, 0),
                "{case}: and the provider's own (11, 22, 33) must not survive the \
                 decline — nothing was drained from that stream"
            );
        }
    }

    /// A provider whose reader PANICS mid-stream fails the read — it does not
    /// truncate it into a short success.
    ///
    /// `served_batch_stream` runs a detached `spawn_blocking` and drops the
    /// `JoinHandle`, so without the `catch_unwind` tokio's task harness swallows
    /// the panic, `tx` drops, and the consumer sees a clean end-of-stream: the
    /// query returns FEWER ROWS and reports success. That is the worst failure
    /// mode this seam has, and it directly contradicted the doc four lines above
    /// the loop, which promised errors are "forwarded, never swallowed into
    /// end-of-stream" (ISSUES OI-2).
    ///
    /// Not hypothetical: arrow-rs panics on importing a malformed
    /// `ArrowArray`/`ArrowSchema`, which is exactly what a buggy C provider hands
    /// across the ABI.
    ///
    /// The assertion is on BOTH halves — the batches that did arrive are kept
    /// (a panic must not discard work already sent) AND the stream ends in an
    /// `Err` naming the panic, not in `None`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_provider_stream_fails_the_read_rather_than_truncating_it() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::serving_then_panicking(vec![id_batch(vec![7, 8]).1]);
        let mut reader = reader_with_provider(tmp.path(), base_name, schema, provider).await;

        let BaseSource { batches, .. } = reader.base_file_source().await.unwrap();
        let items: Vec<Result<RecordBatch>> = batches.collect().await;

        // The panic hook prints to stderr during this test. That is expected and
        // is not a failure; the assertion is on what the CONSUMER observes.
        let (ok, err): (Vec<_>, Vec<_>) = items.iter().partition(|r| r.is_ok());
        assert_eq!(
            ok.len(),
            1,
            "the batch the provider did serve before panicking must still arrive"
        );
        assert_eq!(
            err.len(),
            1,
            "the panic must reach the consumer as an Err — a stream that simply \
             ENDS here is a short result reporting success, which is the bug"
        );
        let msg = format!("{}", err[0].as_ref().unwrap_err());
        assert!(
            msg.contains(STUB_MID_STREAM_PANIC),
            "the error must name the panic it came from, so it is diagnosable \
             rather than merely present: {msg}"
        );
        assert!(
            msg.contains("PANICKED"),
            "and must be distinguishable from a mid-stream Err, which is a \
             different provider defect: {msg}"
        );
    }

    /// The panic reaches a SLOW consumer — one that is a batch behind when the
    /// panic fires.
    ///
    /// The sibling test above consumes with `collect()`, the fastest consumer
    /// possible, so the depth-1 channel is always empty by the time the producer
    /// reports its panic. That makes the DELIVERY of the report untested: review
    /// round 9 changed `blocking_send` to `try_send` — one token, and the obvious
    /// edit for anyone who dislikes parking a blocking-pool thread on the way out
    /// of a panic — and the whole suite stayed green. With the buffer full,
    /// `try_send` returns `Full`, the error is dropped, the channel closes, and
    /// the consumer sees a clean end-of-stream: OI-2 reinstated verbatim, on
    /// every read whose consumer is a batch behind.
    ///
    /// So this consumer deliberately does not poll until the producer has had
    /// time to fill the buffer, and then drains slowly.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_consumer_still_receives_the_panic() {
        use futures::StreamExt;

        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        // Several batches, so the producer is well ahead of this consumer.
        let served: Vec<RecordBatch> = [vec![7], vec![8], vec![9]]
            .into_iter()
            .map(|v| id_batch(v).1)
            .collect();
        let provider = StubDataProvider::serving_then_panicking(served);
        let mut reader = reader_with_provider(tmp.path(), base_name, schema, provider).await;

        let BaseSource { mut batches, .. } = reader.base_file_source().await.unwrap();

        // Do not poll yet: let the producer fill the depth-1 channel, block on
        // the next send, run out of batches and panic with the buffer FULL.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let mut items: Vec<Result<RecordBatch>> = Vec::new();
        while let Some(item) = batches.next().await {
            items.push(item);
            // Stay behind the producer for the whole drain.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let errs: Vec<&CoreError> = items.iter().filter_map(|r| r.as_ref().err()).collect();
        assert_eq!(
            errs.len(),
            1,
            "the panic must reach even a consumer that is a batch behind — a \
             stream that simply ENDS here is a short result reporting success, \
             and the consumer has no way to know it lost rows"
        );
        assert!(
            format!("{}", errs[0]).contains(STUB_MID_STREAM_PANIC),
            "and must still name the panic: {}",
            errs[0]
        );
    }

    /// A panic carrying a LITERAL payload is rendered, not swallowed into
    /// `<non-string panic payload>`.
    ///
    /// `panic_message` has two arms. `panic!("{x}")` yields a `String`; a bare
    /// `panic!("literal")` yields a `&'static str` — and so do `unwrap()`,
    /// `expect(..)`, `assert!(..)` and arrow-rs's own panics on importing a
    /// malformed `ArrowArray`, which is the case this whole seam exists for.
    ///
    /// Only the `String` arm was reachable from a test, because the one stub
    /// interpolated its message. Review round 9 deleted the `&'static str` arm as
    /// redundant and the suite stayed green — every literal panic would have
    /// arrived caught but unreadable, which is half the fix.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_literal_panic_payload_is_still_rendered() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider =
            StubDataProvider::serving_then_panicking_with_a_literal(vec![id_batch(vec![7]).1]);
        let mut reader = reader_with_provider(tmp.path(), base_name, schema, provider).await;

        let BaseSource { batches, .. } = reader.base_file_source().await.unwrap();
        let items: Vec<Result<RecordBatch>> = batches.collect().await;

        let errs: Vec<&CoreError> = items.iter().filter_map(|r| r.as_ref().err()).collect();
        assert_eq!(errs.len(), 1, "the literal panic must still fail the read");
        let msg = format!("{}", errs[0]);
        assert!(
            msg.contains(STUB_LITERAL_PANIC),
            "a `&'static str` payload must be RENDERED, not reported as an \
             unrecognised payload — a caught panic nobody can read is half a \
             fix: {msg}"
        );
    }

    /// The control: the same fixture WITHOUT the panic ends cleanly, with no
    /// spurious `Err`.
    ///
    /// Without it, a `served_batch_stream` that appended an error to every stream
    /// would pass the test above.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_well_behaved_provider_stream_ends_without_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::serving(vec![id_batch(vec![7, 8]).1]);
        let mut reader = reader_with_provider(tmp.path(), base_name, schema, provider).await;

        let BaseSource { batches, .. } = reader.base_file_source().await.unwrap();
        let items: Vec<Result<RecordBatch>> = batches.collect().await;

        assert_eq!(items.len(), 1, "one batch, and nothing after it");
        assert!(
            items[0].is_ok(),
            "a clean stream must not acquire an error on the way out"
        );
    }

    /// Off a tokio runtime, the provider is DECLINED rather than allowed to
    /// panic inside `spawn_blocking`.
    ///
    /// `HoodieFileGroupReader` and `with_base_file_provider` are both `pub`, so a
    /// downstream crate can inject a provider and drive the read on any executor.
    /// `served_batch_stream` hands the served reader to `spawn_blocking`, which
    /// panics with no runtime — a panic from deep inside a stream the caller did
    /// not write, on a path nothing documented (ISSUES OI-22).
    ///
    /// Declining is the same degradation a wrong schema or an unimportable stream
    /// gets: correct results from object storage, counted as a fallback.
    ///
    /// Driven with `futures::executor::block_on`, which is NOT a tokio runtime —
    /// the whole point. Note this test is deliberately not `#[tokio::test]`.
    #[test]
    fn off_a_tokio_runtime_the_provider_is_declined_rather_than_panicking() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        // The stub WOULD serve [7, 8]; seeing [1, 2] proves it was declined.
        let provider = StubDataProvider::serving(vec![id_batch(vec![7, 8]).1]);

        let out = futures::executor::block_on(async {
            let mut reader =
                reader_with_provider(tmp.path(), base_name, schema, provider.clone()).await;
            let live = reader.base_file_provider_live_stats();
            let source = reader.base_file_source().await.unwrap();
            let batch = drain_base_source(source).await;
            let s = live.lock().unwrap();
            (batch, s.files_served, s.storage_fallbacks)
        });
        let (batch, files_served, storage_fallbacks) = out;

        assert_eq!(
            id_values(&batch),
            vec![1, 2],
            "the FILE's rows, from object storage — [7, 8] would mean the provider \
             was used on an executor that cannot drive its stream"
        );
        assert!(
            provider.seen().is_none(),
            "and the provider must not even be OFFERED the file: by the time it \
             has served one, the work is already done and the panic unavoidable"
        );
        assert_eq!(
            (files_served, storage_fallbacks),
            (0, 0),
            "nothing was served and nothing was attempted, so neither counter moves \
             — a provider that was never asked did not 'fall back'"
        );
    }

    /// The control: the SAME fixture on a tokio runtime IS served.
    ///
    /// Without it, `provider_is_usable_here` hard-wired to `false` — which
    /// disables the provider seam entirely — passes every assertion above.
    #[tokio::test(flavor = "multi_thread")]
    async fn on_a_tokio_runtime_the_same_fixture_is_served() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::serving(vec![id_batch(vec![7, 8]).1]);
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema, provider.clone()).await;
        let live = reader.base_file_provider_live_stats();

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(id_values(&out), vec![7, 8], "served on a tokio runtime");
        assert!(
            provider.seen().is_some(),
            "and the provider was offered the file"
        );
        assert_eq!(live.lock().unwrap().files_served, 1);
    }

    /// The three dimensions `served_schema_mismatch` DOCUMENTS and no fixture
    /// could express: nullability and metadata are ignored, types are exact.
    ///
    /// Every served-shape fixture is built with `nullable: true`, no field or
    /// schema metadata, and no dictionary types — so all three of the
    /// comparator's stated contract claims were pinned by nothing. Review round
    /// 10 built two mutations out of that and both survived the WHOLE workspace
    /// suite:
    ///
    /// - adding `if got.is_nullable() != want.is_nullable() { … }`, which
    ///   contradicts the doc and declines a provider that widens a non-null
    ///   column — costing a full re-read of every such file for nothing;
    /// - comparing a dictionary's VALUE type instead of the type exactly, which
    ///   accepts `Dictionary(Int32, Utf8)` where `Utf8` was asked for. That one is
    ///   in the wrong-results class: the doc calls it "a different physical
    ///   layout", and it is the buffers `project_batch_to_schema` then
    ///   reinterprets.
    ///
    /// This is row 25's lesson at one more remove, and round 9's again: the defect
    /// is in what the fixture does not VARY, which is invisible in the assertion.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_shape_check_ignores_nullability_and_metadata_but_not_dictionary_encoding() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        // On disk: `id` is NOT NULL and `tag` is plain Utf8, so the footer — and
        // therefore `intersection`, which the provider is handed — says so.
        let on_disk_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tag", DataType::Utf8, true),
        ]));
        let on_disk = RecordBatch::try_new(
            on_disk_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        // ACCEPTED: `id` widened to nullable, and `tag` carrying field metadata
        // the read never asked for. Both are explicitly ignored, so the provider's
        // rows must reach the caller.
        let mut with_meta = Field::new("tag", DataType::Utf8, true);
        with_meta.set_metadata(
            [("provider".to_string(), "irrelevant".to_string())]
                .into_iter()
                .collect(),
        );
        let widened = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                with_meta,
            ])),
            vec![
                Arc::new(Int32Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        // DECLINED: `tag` dictionary-encoded where plain `Utf8` was asked for.
        let dict: arrow_array::DictionaryArray<arrow_array::types::Int32Type> =
            vec!["x", "y"].into_iter().collect();
        let dictionary_encoded = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new(
                    "tag",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                ),
            ])),
            vec![Arc::new(Int32Array::from(vec![7, 8])), Arc::new(dict)],
        )
        .unwrap();

        // ACCEPTED: metadata on the SCHEMA itself rather than on a field. The doc
        // says "schema/field metadata", but every fixture here varied only the
        // FIELD half — so bolting a `served.metadata() != wanted.metadata()` check
        // onto the comparator passed this test, and the whole workspace with it.
        let schema_level_metadata = RecordBatch::try_new(
            Arc::new(
                Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("tag", DataType::Utf8, true),
                ])
                .with_metadata(
                    [("provider".to_string(), "irrelevant".to_string())]
                        .into_iter()
                        .collect(),
                ),
            ),
            vec![
                Arc::new(Int32Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        for (case, served, expect_served) in [
            ("widened nullability + extra metadata", widened, true),
            (
                "metadata on the schema itself, not on a field",
                schema_level_metadata,
                true,
            ),
            (
                "dictionary-encoded where plain was asked for",
                dictionary_encoded,
                false,
            ),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let base_name = "f1-0_0-1-1_001.parquet";
            write_parquet_file(tmp.path(), base_name, &on_disk);

            let provider = StubDataProvider::serving(vec![served]);
            let mut reader = reader_with_provider(
                tmp.path(),
                base_name,
                on_disk_schema.clone(),
                provider.clone(),
            )
            .await;
            let live = reader.base_file_provider_live_stats();

            let out = drain_base_source(reader.base_file_source().await.unwrap()).await;
            let ids = i32_col(&out, 0);
            let s = live.lock().unwrap();

            if expect_served {
                assert_eq!(
                    ids,
                    vec![Some(7), Some(8)],
                    "{case}: must be SERVED — the comparator documents both as \
                     ignored, and declining costs a full re-read of every such \
                     file for nothing"
                );
                assert_eq!((s.files_served, s.storage_fallbacks), (1, 0), "{case}");
            } else {
                assert_eq!(
                    ids,
                    vec![Some(1), Some(2)],
                    "{case}: must be DECLINED — a dictionary is a different \
                     PHYSICAL layout from the one the read asked for, and it is \
                     the buffers that get reinterpreted"
                );
                assert_eq!((s.files_served, s.storage_fallbacks), (0, 1), "{case}");
            }
        }
    }

    /// The nullability exemption is TOP-LEVEL ONLY — pinned, not fixed.
    ///
    /// `served_schema_mismatch` compares `got.data_type() != want.data_type()`.
    /// On a flat column that cannot see `nullable` at all, which is what makes the
    /// documented exemption true. On a nested one it delegates to `DataType`'s
    /// `PartialEq`, which compares the child `Field`s — and `Field::eq` includes
    /// `nullable` and `metadata`. So the exact difference the test above proves is
    /// IGNORED on `id` is ENFORCED one level down inside a `Struct`.
    ///
    /// No fixture in this file used a nested column, so nothing expressed that
    /// asymmetry and the doc read as though the exemption were uniform. This pins
    /// the behaviour the code actually has; the doc now says the same thing.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_shape_checks_nullability_exemption_is_top_level_only() {
        use arrow_array::{ArrayRef, Int32Array, StructArray};
        use arrow_schema::{DataType, Field, Fields, Schema};

        let child_non_null = Arc::new(Field::new("inner", DataType::Int32, false));
        let child_nullable = Arc::new(Field::new("inner", DataType::Int32, true));

        let on_disk_fields = Fields::from(vec![child_non_null.clone()]);
        let on_disk_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(on_disk_fields.clone()),
            true,
        )]));
        let on_disk = RecordBatch::try_new(
            on_disk_schema.clone(),
            vec![Arc::new(StructArray::new(
                on_disk_fields,
                vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
                None,
            ))],
        )
        .unwrap();

        // The CHILD widened to nullable — the same widening the flat `id` column
        // is served for two tests above.
        let served_fields = Fields::from(vec![child_nullable.clone()]);
        let served = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                DataType::Struct(served_fields.clone()),
                true,
            )])),
            vec![Arc::new(StructArray::new(
                served_fields,
                vec![Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef],
                None,
            ))],
        )
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::serving(vec![served]);
        let mut reader = reader_with_provider(
            tmp.path(),
            base_name,
            on_disk_schema.clone(),
            provider.clone(),
        )
        .await;
        let live = reader.base_file_provider_live_stats();

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert!(
            provider.seen().is_some(),
            "fixture check — the provider must have been offered the file"
        );
        assert_eq!(
            out.num_rows(),
            2,
            "the file's two rows must reach the caller either way"
        );
        let s = live.lock().unwrap();
        assert_eq!(
            (s.files_served, s.storage_fallbacks),
            (0, 1),
            "a child-only nullability widening IS declined: the exemption the doc              promises is top-level only, because `Field::eq` inside a nested              `DataType` compares `nullable`"
        );
    }

    /// The control for the test above: the SAME fixture, with the schema the
    /// provider was actually asked for, is served.
    ///
    /// Without it, `served_schema_mismatch` returning `Some` unconditionally — or
    /// the call site declining every serve — passes every assertion above while
    /// disabling the provider path entirely.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_served_stream_at_the_right_schema_is_still_served() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::serving(vec![id_batch(vec![7, 8]).1]);
        let mut reader = reader_with_provider(tmp.path(), base_name, schema, provider).await;
        let live = reader.base_file_provider_live_stats();

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;

        assert_eq!(
            id_values(&out),
            vec![7, 8],
            "a matching schema is served — the PROVIDER's rows reach the caller"
        );
        let s = live.lock().unwrap();
        assert_eq!(s.files_served, 1);
        assert_eq!(s.storage_fallbacks, 0);
    }

    /// `read()` snapshots the shared slot into `read_stats` once the merge has
    /// drained it, so a `read_stats()`-based caller sees the complete picture —
    /// setup counters *and* drain counters — without knowing the slot exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_read_snapshots_the_slot_into_read_stats() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let (_, b1) = id_batch(vec![7, 8]);
        let (_, b2) = id_batch(vec![9]);
        let provider = StubDataProvider::serving(vec![b1, b2]);
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider).await;

        let batch = reader.read().await.unwrap();
        assert_eq!(
            id_values(&batch),
            vec![7, 8, 9],
            "served data reached read()"
        );

        let recorded = reader
            .read_stats()
            .base_file_provider
            .as_ref()
            .expect("read() snapshots the provider stats");
        assert_eq!(recorded.files_served, 1, "setup counter preserved");
        assert_eq!(recorded.batches_received, 2, "both batches counted");
        assert_eq!(recorded.rows_served, 3, "all rows counted");
        assert!(
            recorded.bytes_materialized > 0,
            "materialized bytes counted by the time read() returns"
        );
    }

    /// **Anti-truncation guard for the served source.**
    ///
    /// A lazy source means a provider error is no longer classified before any
    /// data is handed downstream: it has to surface *mid-iteration*. The
    /// regression this catches is an adapter that maps that error to
    /// end-of-stream, because then a provider dying halfway through returns a
    /// short, *successful* read and silently drops the remaining base rows.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_mid_stream_error_surfaces_and_never_ends_the_stream() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        // One good batch, then the provider dies.
        let (_, good) = id_batch(vec![7, 8]);
        let provider = StubDataProvider::serving_then_failing(vec![good]);
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider).await;

        let source = reader
            .base_file_source()
            .await
            .expect("serving succeeds — the failure is mid-stream, not at setup");
        let mut batches = source.batches;

        // Batch 1 arrives normally.
        let first = batches
            .next()
            .await
            .expect("a first item")
            .expect("first batch is good");
        assert_eq!(id_values(&first), vec![7, 8]);

        // Batch 2 is the injected failure. It MUST be `Some(Err(_))`, never
        // `None` — `None` here is the silent-truncation bug.
        let second = batches
            .next()
            .await
            .expect("mid-stream failure must be reported, not silently end the stream");
        let err = second.expect_err("second item must be the provider's error");
        assert!(
            err.to_string().contains(STUB_MID_STREAM_ERROR),
            "the provider's own error must reach the consumer, not be replaced: {err}"
        );
    }

    /// The same anti-truncation guarantee, one layer up: an error from the base
    /// source must come out of the **merge** as an error, not as a clean end of
    /// iteration. This is the level a query actually observes, and it covers the
    /// no-log-file (CoW / MOR `_ro`) shape, which takes the eager merge iterator
    /// rather than the record buffer.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_mid_stream_error_fails_the_merge_not_truncates_it() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let (_, good) = id_batch(vec![7, 8]);
        let provider = StubDataProvider::serving_then_failing(vec![good]);
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider).await;

        let mut merged = reader.open_stream().await.expect("open succeeds");

        // Drive the merge to completion, recording whether an error appeared.
        let mut saw_error = None;
        let mut ok_rows = 0usize;
        while let Some(item) = merged.next().await {
            match item {
                Ok(batch) => ok_rows += batch.num_rows(),
                Err(e) => {
                    saw_error = Some(e.to_string());
                    break;
                }
            }
        }
        let err = saw_error.expect(
            "the merge must surface the provider's mid-stream error; ending cleanly here \
             would be a short successful read — silent data loss",
        );
        assert!(
            err.contains(STUB_MID_STREAM_ERROR),
            "the underlying provider error must be preserved through the merge: {err}"
        );
        // Whatever made it through before the failure is fine to have emitted —
        // the point is that the read did not *complete* on it.
        assert!(
            ok_rows <= 2,
            "only the pre-failure batch could have been emitted, got {ok_rows} rows"
        );
    }

    /// A provider that returns `None` falls through to the object-store read,
    /// and is still counted — the failure mode this guards is an unserved file
    /// quietly emptying a read.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_none_falls_through_to_the_base_file() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::not_serving();
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider).await;

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;
        assert_eq!(
            id_values(&out),
            vec![1, 2],
            "a fallback must read the base file exactly as if no provider were injected"
        );
        let stats = reader.base_file_provider_live_stats();
        let stats = stats.lock().unwrap();
        assert_eq!(stats.storage_fallbacks, 1, "a fallback reports its attempt");
        assert_eq!(stats.files_served, 0);
        assert_eq!(stats.rows_served, 0, "a fallback serves no rows");
    }

    /// With no provider injected the read is unchanged and `base_file_provider`
    /// stays `None`, so the provider concept costs the default path nothing —
    /// and `Some(all-zeroes)` can never be confused with "no provider injected".
    #[tokio::test(flavor = "multi_thread")]
    async fn no_base_file_provider_leaves_read_and_stats_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let mut reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;
        let batch = reader.read().await.unwrap();
        assert_eq!(id_values(&batch), vec![1, 2]);
        assert!(
            reader.read_stats().base_file_provider.is_none(),
            "no provider injected → no provider stats section at all"
        );
    }

    /// The provider is handed the base file's absolute URI, the projected schema
    /// the read wants back, the reader's pushdown decision and the partition
    /// coordinates — the inputs an implementation cannot be correct without.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_request_carries_uri_schema_and_pushdown_gate() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider = StubDataProvider::not_serving();
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider.clone()).await;
        // Partition fields are read off the table config, so set one and prove it
        // is parsed rather than passed through verbatim (note the stray space).
        //
        // The partition PATH is set non-empty on purpose. It used to be `""`, and
        // the assertion below then read `partition_path == ""` — which
        // `partition_path: ""` hard-wired at the seam satisfies exactly as well.
        // The path is the only input a provider has for reconstructing partition
        // columns, so a provider handed an empty one emits null partition values
        // into batches presented at the projected schema.
        {
            let ctx = Arc::get_mut(&mut reader.reader_context).expect("sole owner in test");
            ctx.table_config.insert(
                HudiTableConfig::PartitionFields.as_ref().to_string(),
                "city, ts".to_string(),
            );
        }
        reader.input_split = InputSplit::new(
            Some(base_name.to_string()),
            None,
            Vec::new(),
            "city=sf/ts=2024".to_string(),
        );
        // A data schema DISTINCT from the projected one, so the assertion below
        // cannot pass by both being `None` — which is how the fixture stood when
        // the assertion was first written, making it vacuous.
        let data_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("city", arrow_schema::DataType::Utf8, true),
        ]));
        reader.schema_handler.data_schema = Some(data_schema.clone());
        let expected_gate = reader.base_read_pushdown_is_safe();
        let expected_uri = join_url_segments(&reader.storage.base_url, &[base_name])
            .map(|u| u.to_string())
            .expect("the fixture's base url joins");
        let _ = reader.base_file_source().await.unwrap();

        let seen = provider.seen().expect("provider called");
        assert_eq!(
            seen.file_uri, expected_uri,
            "request must carry the base file's ABSOLUTE URI. `ends_with(base_name)` \
             was the old assertion and a table-RELATIVE path satisfies it just as \
             well — but a provider keys the file by this string"
        );
        assert_eq!(
            seen.projected_schema, schema,
            "request must carry the projected schema the read wants back"
        );
        assert_eq!(
            seen.can_push_predicate, expected_gate,
            "request must carry the reader's pushdown decision for this file. \
             This fixture arms no repair-risk column, so the repair gate cannot \
             narrow anything and the decision coincides with the merge gate; the \
             case where it does NOT is \
             `repair_conflict_also_withdraws_provider_pushdown`"
        );
        assert_eq!(
            seen.partition_fields,
            vec!["city".to_string(), "ts".to_string()],
            "partition fields must be split and trimmed"
        );
        assert_eq!(
            seen.partition_path, "city=sf/ts=2024",
            "the split's partition path is passed through as-is — it is the only \
             input a provider has for reconstructing partition columns"
        );
        assert_eq!(
            seen.data_schema.as_ref(),
            Some(&data_schema),
            "and the table's data schema, which is how a provider resolves those \
             partition columns' TYPES. Asserted against the fixture's own schema, \
             not against the reader's field — comparing the reader to itself would \
             pass under `data_schema: None` at the seam"
        );
    }

    /// A base file the instant range excludes is never offered to the provider.
    ///
    /// The range is a per-file decision settled before the read opens, so asking
    /// a provider to serve a file whose rows are all going to be dropped would be
    /// a round-trip for nothing — and, if a provider ever forgot the range, a way
    /// for excluded rows to reach an incremental query.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_is_not_offered_a_file_outside_the_instant_range() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        // The base file's own commit instant is "001" (…_001.parquet).
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);
        let (_, served) = id_batch(vec![7, 8, 9]);

        // Range that EXCLUDES the base file's instant: open start "100" > "001".
        let provider = StubDataProvider::serving(vec![served.clone()]);
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider.clone()).await;
        {
            let ctx = Arc::get_mut(&mut reader.reader_context).expect("sole owner in test");
            ctx.instant_range = Some(InstantRange::within_open_closed("100", "999", "UTC"));
        }
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;
        assert_eq!(
            out.num_rows(),
            0,
            "a base file outside the instant range contributes no rows"
        );
        assert!(
            provider.seen().is_none(),
            "an excluded base file must not be offered to the provider at all"
        );

        // Control: a range that INCLUDES "001" reaches the provider and keeps the
        // served rows, proving the zero above came from the range and not from
        // the seam being unreachable.
        let provider = StubDataProvider::serving(vec![served]);
        let mut reader =
            reader_with_provider(tmp.path(), base_name, schema.clone(), provider.clone()).await;
        {
            let ctx = Arc::get_mut(&mut reader.reader_context).expect("sole owner in test");
            ctx.instant_range = Some(InstantRange::up_to("999", "UTC"));
        }
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;
        assert_eq!(
            id_values(&out),
            vec![7, 8, 9],
            "served rows inside the instant range survive"
        );
        assert!(provider.seen().is_some(), "an in-range file is offered");
    }

    /// Position-based merge skips the provider: the base read carries a
    /// synthetic row-index column the position buffer matches log records
    /// against, and a provider returns only projected data columns. Serving one
    /// would drop the column and break the merge.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_provider_is_skipped_under_position_based_merge() {
        let (tmp, base_name, log_name, schema) = position_merge_slice();
        let provider = StubDataProvider::serving(vec![id_batch(vec![7, 8]).1]);

        let base_path = tmp.path().to_str().unwrap().to_string();
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref(),
            base_path,
        )]));
        let storage = Storage::new(Arc::new(HashMap::new()), hudi_configs).unwrap();
        let input_split = InputSplit::new(
            Some(base_name),
            Some("001".to_string()),
            vec![log_name],
            String::new(),
        );
        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time =
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        reader_context.rebuild_record_context(String::new());
        let params = ReaderParameters {
            use_record_position: true,
            ..Default::default()
        };

        let mut reader = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            input_split,
            params,
            None,
            None,
        )
        .unwrap();
        reader.schema_handler.required_schema = Some(schema);
        reader.base_file_provider = Some(provider.clone());

        assert!(
            reader.use_record_position(),
            "the fixture must actually take the position-merge path"
        );
        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;
        assert!(
            provider.seen().is_none(),
            "position-based merge must not offer the base file to a provider"
        );
        assert_eq!(
            id_values(&out.project(&[0]).unwrap()),
            vec![1, 2],
            "the base file itself is read, row-index column and all"
        );
        assert!(
            out.schema()
                .column_with_name(ROW_INDEX_TEMPORARY_COLUMN_NAME)
                .is_some(),
            "the row-index column the provider cannot supply is still present"
        );
    }

    /// A base file plus one (empty) log file, so `use_record_position` is
    /// satisfied: it needs log files, a parquet base file and a base commit time.
    fn position_merge_slice() -> (tempfile::TempDir, String, String, SchemaRef) {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);
        let log_name = ".f1-0_20240101120000000.log.1_0-0-0";
        std::fs::write(tmp.path().join(log_name), b"").unwrap();
        (tmp, base_name.to_string(), log_name.to_string(), schema)
    }

    /// Counters accumulate per base file into one per-read total.
    #[test]
    fn base_file_provider_stats_merge_sums_counters() {
        let mut total = BaseFileProviderStats {
            files_served: 1,
            local_served: 1,
            rows_served: 10,
            ..Default::default()
        };
        total.merge(&BaseFileProviderStats {
            storage_fallbacks: 2,
            remote_served: 1,
            rows_served: 5,
            fetch_wall_nanos: 7,
            ..Default::default()
        });
        assert_eq!(total.files_served, 1);
        assert_eq!(total.storage_fallbacks, 2);
        assert_eq!(total.local_served, 1);
        assert_eq!(total.remote_served, 1);
        assert_eq!(total.rows_served, 15, "summed, not replaced");
        assert_eq!(total.fetch_wall_nanos, 7);
    }
    /// A provider reader that records which thread each `next()` ran on, and
    /// optionally drives an async fetch with `block_on` while it is there.
    struct ProbeReader {
        remaining: usize,
        schema: SchemaRef,
        threads: Arc<StdMutex<Vec<std::thread::ThreadId>>>,
        block_on_each_next: bool,
    }

    impl Iterator for ProbeReader {
        type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.remaining == 0 {
                return None;
            }
            self.remaining -= 1;
            self.threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            if self.block_on_each_next {
                // The contract this pins: a served reader may drive async work
                // with `block_on` on the very runtime driving the read. A
                // blocking-pool thread has that runtime's handle set but is not
                // "entered", so this does not panic — which is the whole reason
                // the reader is moved onto the blocking pool rather than pulled
                // from a runtime worker.
                let handle = tokio::runtime::Handle::current();
                handle.block_on(async { tokio::task::yield_now().await });
            }
            Some(Ok(RecordBatch::new_empty(self.schema.clone())))
        }
    }

    impl arrow_array::RecordBatchReader for ProbeReader {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    /// A provider serving `batches` empty batches through a [`ProbeReader`].
    struct ProbeProvider {
        batches: usize,
        threads: Arc<StdMutex<Vec<std::thread::ThreadId>>>,
        block_on_each_next: bool,
    }

    #[async_trait::async_trait]
    impl crate::file_group::reader_v2::base_file_provider::BaseFileDataProvider for ProbeProvider {
        async fn try_base_file(
            &self,
            req: BaseFileDataRequest<'_>,
        ) -> (
            Option<Box<dyn arrow_array::RecordBatchReader + Send + 'static>>,
            BaseFileProviderStats,
        ) {
            (
                Some(Box::new(ProbeReader {
                    remaining: self.batches,
                    schema: req.projected_schema.clone(),
                    threads: self.threads.clone(),
                    block_on_each_next: self.block_on_each_next,
                })),
                BaseFileProviderStats {
                    files_served: 1,
                    ..Default::default()
                },
            )
        }
    }

    async fn drive_probe(batches: usize, block_on_each_next: bool) -> Vec<std::thread::ThreadId> {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let threads = Arc::new(StdMutex::new(Vec::new()));
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), base_name, schema).await;
        reader.base_file_provider = Some(Arc::new(ProbeProvider {
            batches,
            threads: threads.clone(),
            block_on_each_next,
        }));

        let out = drain_base_source(reader.base_file_source().await.unwrap()).await;
        assert_eq!(out.num_rows(), 0, "the probe serves empty batches");
        let seen = threads.lock().unwrap().clone();
        assert_eq!(seen.len(), batches, "every batch must have been pulled");
        seen
    }

    /// Every `next()` on a served reader runs on one thread, and not on the
    /// thread driving the merge.
    ///
    /// What this pins is the second half: a blocking pull from a runtime worker
    /// would stall the executor, and that this catches. It is **weak evidence for
    /// the first half** — tokio hands sequential `spawn_blocking` calls back to
    /// the same idle pool thread, so a per-`next()` implementation passes this
    /// too (verified, not assumed). The structural guarantee that one task owns
    /// the reader is pinned by
    /// [`a_served_reader_runs_two_batches_ahead_and_no_further`] instead, which
    /// that implementation cannot satisfy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_served_reader_is_pulled_from_one_thread_throughout() {
        let driver = std::thread::current().id();
        let seen = drive_probe(6, false).await;

        let first = seen[0];
        assert!(
            seen.iter().all(|t| *t == first),
            "every next() must run on one thread, saw {seen:?}"
        );
        assert_ne!(
            first, driver,
            "and not on the thread driving the merge — a blocking pull there \
             would stall the executor"
        );
    }

    /// The served reader runs **two batches ahead** of the merge, and no further.
    ///
    /// Both bounds are the contract, and the exact number is worth pinning
    /// because it is the resident-memory bound: with one batch delivered, one
    /// waits in the depth-1 channel and one is blocked in `blocking_send`, so
    /// three exist at once.
    ///
    /// *Runs ahead at all* is the observable signature of one blocking task
    /// owning the reader — a per-`next()` `spawn_blocking` produces nothing until
    /// asked, so it stalls at 1 and fails here. *No further* is the memory
    /// contract: an adapter that drained the source up front would reach 4.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_served_reader_runs_two_batches_ahead_and_no_further() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let threads = Arc::new(StdMutex::new(Vec::new()));
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), base_name, schema).await;
        reader.base_file_provider = Some(Arc::new(ProbeProvider {
            batches: 4,
            threads: threads.clone(),
            block_on_each_next: false,
        }));

        let mut batches = reader.base_file_source().await.unwrap().batches;
        let _first = batches.next().await.expect("a first batch").expect("ok");

        // The producer runs concurrently, so poll for the read-ahead rather than
        // sleeping a guessed interval. A per-next() implementation never gets
        // past 1 and this loop runs out.
        let produced = || threads.lock().unwrap().len();
        for _ in 0..200 {
            if produced() >= 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            produced(),
            3,
            "one delivered + one buffered + one blocked in send (a per-pull \
             adapter stalls at 1)"
        );

        // And it stays there: the producer is blocked, not merely slow. Without
        // this an eager drain would pass the assertion above on its way to 4.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            produced(),
            3,
            "the producer must stay blocked until the merge takes another batch \
             — reaching 4 means the served file is being drained up front"
        );
    }

    /// A provider must outlive every reader it handed back.
    ///
    /// A provider is free to return a reader that borrows its own state — the
    /// C-ABI adapter returns an `ArrowArrayStream` whose callbacks point into the
    /// provider's `ctx`, and dropping `CApiBaseFileDataProvider` calls
    /// `destroy(ctx)`. On the FFI path the core reader is a local of
    /// `get_closable_iterator` and dies when that function returns, while the
    /// stream it produced is handed to C++ and drained afterwards. So "the caller
    /// keeps the provider alive" cannot be assumed: the FFI reader handle and the
    /// stream are separate objects with no ordering between their frees.
    ///
    /// This pins the property that makes that safe — the served stream's own task
    /// holds a strong reference — by dropping EVERY other reference and checking
    /// the provider is still alive. Removing the `_provider` field from
    /// `ServedReader` fails it, and so does capturing `served.reader` instead of
    /// `served` — which is how it actually broke once, and is the shape the RED
    /// proof reproduces. The field looks like dead code and is not.
    ///
    /// What this test does NOT pin is the ORDER the two drop in;
    /// [`a_served_readers_release_runs_before_the_providers_destroy`] owns that,
    /// and without it swapping `ServedReader`'s two fields passes every test here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_served_stream_keeps_its_provider_alive_after_the_reader_is_dropped() {
        use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

        /// Flips `0` when the provider holding it is dropped.
        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Relaxed);
            }
        }
        struct DropTrackingProvider {
            batches: usize,
            _flag: DropFlag,
        }
        #[async_trait::async_trait]
        impl crate::file_group::reader_v2::base_file_provider::BaseFileDataProvider
            for DropTrackingProvider
        {
            async fn try_base_file(
                &self,
                req: BaseFileDataRequest<'_>,
            ) -> (
                Option<Box<dyn arrow_array::RecordBatchReader + Send + 'static>>,
                BaseFileProviderStats,
            ) {
                (
                    Some(Box::new(ProbeReader {
                        remaining: self.batches,
                        schema: req.projected_schema.clone(),
                        threads: Arc::new(StdMutex::new(Vec::new())),
                        block_on_each_next: false,
                    })),
                    BaseFileProviderStats {
                        files_served: 1,
                        ..Default::default()
                    },
                )
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let dropped = Arc::new(AtomicBool::new(false));
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), base_name, schema).await;
        // Constructed inline: the test deliberately keeps NO reference of its own,
        // so after the drop below the only strong reference left is the one the
        // served stream's task holds.
        reader.base_file_provider = Some(Arc::new(DropTrackingProvider {
            batches: 4,
            _flag: DropFlag(dropped.clone()),
        }));

        let source = reader.base_file_source().await.unwrap();
        drop(reader);

        assert!(
            !dropped.load(Relaxed),
            "the provider was dropped while its served stream was still live — a \
             provider whose reader borrows its own state is now reading freed \
             memory"
        );

        // And it is released once the stream is done, so this is a lifetime
        // extension and not a leak. The producer task ends after the last batch
        // is taken, so poll rather than sleep a guessed interval.
        let out = drain_base_source(source).await;
        assert_eq!(out.num_rows(), 0, "the probe serves empty batches");
        for _ in 0..200 {
            if dropped.load(Relaxed) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            dropped.load(Relaxed),
            "and it must be released once the stream is exhausted, or the \
             reference is a leak rather than a lifetime extension"
        );
    }

    /// **The producer STOPS when the consumer drops the stream.**
    ///
    /// `served_batch_stream`'s doc rests on this in three places — "dropping the
    /// returned stream drops the receiver, the next `blocking_send` fails, and
    /// the producer returns", the bound on how far ahead it may run, and the
    /// claim that a cancelled read costs at most two wasted batches. Nothing
    /// pinned it: `a_served_reader_runs_two_batches_ahead_and_no_further` drops
    /// the stream and then asserts nothing about what happens next.
    ///
    /// So `if tx.blocking_send(projected).is_err() || was_err` → `&& was_err`
    /// compiled, tripped no lint, and passed every test — while turning a
    /// cancelled query into a hot loop that pulls the provider to exhaustion
    /// (the whole base file, for a read nobody is reading) and re-polls a reader
    /// that has already returned an error, which for an FFI `ArrowArrayStream` is
    /// use-after-error on a C object.
    ///
    /// The probe serves far more batches than the read-ahead, so "it stopped" and
    /// "it ran out" are distinguishable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dropped_stream_stops_the_producer() {
        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let threads = Arc::new(StdMutex::new(Vec::new()));
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), base_name, schema).await;
        reader.base_file_provider = Some(Arc::new(ProbeProvider {
            batches: 500,
            threads: threads.clone(),
            block_on_each_next: false,
        }));

        let mut batches = reader.base_file_source().await.unwrap().batches;
        let _first = batches.next().await.expect("a first batch").expect("ok");
        drop(batches);

        // Let the producer notice. It is parked in `blocking_send`, so the drop
        // of the receiver is what wakes it.
        // THREE consecutive stable samples, not one. The producer calls `next()`
        // before `blocking_send`, so a blocking-pool thread starved for a single
        // 10ms window would look settled and then legitimately pull once more —
        // a false FAILURE under load. (Not a false pass: the `< 500` assertion
        // below is what excludes "it finished".)
        let pulled = || threads.lock().unwrap().len();
        let mut settled = pulled();
        let mut stable = 0;
        for _ in 0..200 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let now = pulled();
            if now == settled {
                stable += 1;
                if stable >= 3 {
                    break;
                }
            } else {
                stable = 0;
                settled = now;
            }
        }
        let after_settling = pulled();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        assert_eq!(
            pulled(),
            after_settling,
            "the producer must STOP once the consumer drops the stream — it is \
             still pulling, so a cancelled read is draining the whole served file"
        );
        assert!(
            after_settling < 500,
            "and it must stop EARLY, not merely finish: {after_settling} of 500 \
             batches pulled for a read that took one"
        );
    }

    /// **The served reader is released BEFORE the provider's `destroy(ctx)`.**
    ///
    /// `a_served_stream_keeps_its_provider_alive_after_the_reader_is_dropped`
    /// pins that the provider outlives the stream. It does NOT pin the order the
    /// two are released in, and the order is the half that matters to the C ABI:
    /// the served `ArrowArrayStream`'s `release` callback points into the
    /// provider's `ctx`, so releasing the provider first is the same
    /// use-after-free `OI-1` was raised to close — just reached from the other
    /// end.
    ///
    /// That order is supplied by `ServedReader`'s FIELD DECLARATION ORDER, and
    /// swapping two fields is an entirely plausible tidy-up that compiles and
    /// (without this test) passes everything: the liveness test still sees the
    /// provider alive during the drain and released after it. Three separate doc
    /// comments assert the order is "structural"; this is what makes that true
    /// rather than asserted.
    ///
    /// The probe is the reader itself: its `Drop` reads the provider's flag and
    /// records whether the provider had already gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_served_readers_release_runs_before_the_providers_destroy() {
        use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

        /// Set when the provider is dropped — i.e. when `destroy(ctx)` would run.
        ///
        /// `SeqCst` throughout, not `Relaxed`: the assertion below reads one flag
        /// having polled on another, and under the mutation this test exists to
        /// kill the two stores happen in the opposite order. With `Relaxed` the
        /// read can land between them and report GREEN on the mutation.
        #[derive(Clone)]
        struct ProviderGone(Arc<AtomicBool>);
        impl Drop for ProviderGone {
            fn drop(&mut self) {
                self.0.store(true, SeqCst);
            }
        }

        /// Reads `ProviderGone` in its own `Drop`, which is exactly what a C
        /// stream's `release` callback does when it touches `ctx`.
        struct ReleaseProbe {
            schema: SchemaRef,
            remaining: usize,
            provider_gone: Arc<AtomicBool>,
            saw_provider_gone: Arc<AtomicBool>,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for ReleaseProbe {
            fn drop(&mut self) {
                // Record the observation BEFORE announcing that we ran, so a
                // waiter that sees `dropped` is guaranteed to see the value too.
                self.saw_provider_gone
                    .store(self.provider_gone.load(SeqCst), SeqCst);
                self.dropped.store(true, SeqCst);
            }
        }
        impl Iterator for ReleaseProbe {
            type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
            fn next(&mut self) -> Option<Self::Item> {
                if self.remaining == 0 {
                    return None;
                }
                self.remaining -= 1;
                Some(Ok(RecordBatch::new_empty(self.schema.clone())))
            }
        }
        impl arrow_array::RecordBatchReader for ReleaseProbe {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }

        struct ReleaseOrderProvider {
            gone: ProviderGone,
            saw_provider_gone: Arc<AtomicBool>,
            probe_dropped: Arc<AtomicBool>,
        }
        #[async_trait::async_trait]
        impl crate::file_group::reader_v2::base_file_provider::BaseFileDataProvider
            for ReleaseOrderProvider
        {
            async fn try_base_file(
                &self,
                req: BaseFileDataRequest<'_>,
            ) -> (
                Option<Box<dyn arrow_array::RecordBatchReader + Send + 'static>>,
                BaseFileProviderStats,
            ) {
                (
                    Some(Box::new(ReleaseProbe {
                        schema: req.projected_schema.clone(),
                        remaining: 2,
                        provider_gone: self.gone.0.clone(),
                        saw_provider_gone: self.saw_provider_gone.clone(),
                        dropped: self.probe_dropped.clone(),
                    })),
                    BaseFileProviderStats {
                        files_served: 1,
                        ..Default::default()
                    },
                )
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let (schema, on_disk) = id_batch(vec![1, 2]);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file(tmp.path(), base_name, &on_disk);

        let provider_gone = Arc::new(AtomicBool::new(false));
        let saw_provider_gone = Arc::new(AtomicBool::new(false));
        let probe_dropped = Arc::new(AtomicBool::new(false));
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), base_name, schema).await;
        reader.base_file_provider = Some(Arc::new(ReleaseOrderProvider {
            gone: ProviderGone(provider_gone.clone()),
            saw_provider_gone: saw_provider_gone.clone(),
            probe_dropped: probe_dropped.clone(),
        }));

        let source = reader.base_file_source().await.unwrap();
        drop(reader);
        let out = drain_base_source(source).await;
        assert_eq!(out.num_rows(), 0, "the probe serves empty batches");

        // Poll on the PROBE's flag, not the provider's. Under the mutation this
        // test exists to kill, the provider is released FIRST, so waiting on
        // `provider_gone` would let the assertion run before the probe had made
        // its observation — and read a default `false`, i.e. report GREEN on the
        // mutation. The probe's flag is set after its observation, so seeing it
        // guarantees the value is there.
        for _ in 0..200 {
            if probe_dropped.load(SeqCst) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            probe_dropped.load(SeqCst),
            "fixture check: the served reader must have been released by now, or \
             the assertion below has not been exercised at all"
        );
        assert!(
            provider_gone.load(SeqCst),
            "fixture check: and the provider too, or the ordering below is not \
             the one this test is about"
        );
        assert!(
            !saw_provider_gone.load(SeqCst),
            "the served reader was released AFTER the provider — a C stream's \
             release callback would be reading a ctx that destroy() already \
             freed. ServedReader's field order is what prevents this; check that \
             `reader` is still declared before `_provider`"
        );
    }

    /// **`block_on` inside `next()` does not panic.** The reader runs on a
    /// blocking-pool thread, which carries the runtime's handle but is not
    /// "entered", so a provider may drive an async fetch inline.
    ///
    /// Worth a test because the opposite is true one thread over: the same call
    /// from a runtime worker panics, and a panic unwinding across the FFI
    /// boundary is undefined behavior. If the adapter ever stopped moving the
    /// reader onto the blocking pool, this test would panic rather than fail
    /// quietly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_served_reader_may_block_on_the_reads_own_runtime() {
        let seen = drive_probe(3, true).await;
        assert_eq!(seen.len(), 3, "all three batches survived the block_on");
    }

    // ------------------------------------------------------------------
    // ENG-48159 — the bounded initial prefetch (T-1).
    //
    // These six tests are `ROW-S17(c)`'s `initial_prefetch_*` family, re-expressed
    // against this tree's `BoxStream` shape. They could not be carried as code:
    // internal's originals reach into `ParquetFileStream`'s private fields to
    // inject a read error at a chosen position, and this tree has no
    // `ParquetSyncReader` for them to hang off. What is carried is the
    // specification — the behaviour, not the line range.
    // ------------------------------------------------------------------

    /// One `id: Int32` batch holding `start..start + n`.
    fn initial_prefetch_batch(start: i32, n: i32) -> (SchemaRef, RecordBatch) {
        let schema: SchemaRef =
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int32,
                false,
            )]));
        let col = Arc::new(arrow_array::Int32Array::from(
            (start..start + n).collect::<Vec<i32>>(),
        ));
        (
            schema.clone(),
            RecordBatch::try_new(schema, vec![col]).unwrap(),
        )
    }

    /// A `BaseBatchStream` over caller-supplied items, counting **polls** of the
    /// underlying stream.
    ///
    /// Counting `poll_next` CALLS rather than items is the point.
    /// `StreamExt::inspect` fires per item, so it cannot see the poll that
    /// returns `None` — which is exactly the poll the fuse exists to prevent,
    /// leaving the fuse untestable. Internal found that by mutation: deleting the
    /// fuse left an inspect-based helper green.
    fn counted_source(
        items: Vec<Result<RecordBatch>>,
        polls: Arc<std::sync::atomic::AtomicU64>,
    ) -> BaseBatchStream {
        struct PollCounted {
            inner: futures::stream::Iter<std::vec::IntoIter<Result<RecordBatch>>>,
            polls: Arc<std::sync::atomic::AtomicU64>,
        }
        impl futures::Stream for PollCounted {
            type Item = Result<RecordBatch>;
            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                self.polls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                std::pin::Pin::new(&mut self.inner).poll_next(cx)
            }
        }
        PollCounted {
            inner: futures::stream::iter(items),
            polls,
        }
        .boxed()
    }

    fn counted(
        items: Vec<Result<RecordBatch>>,
    ) -> (BaseBatchStream, Arc<std::sync::atomic::AtomicU64>) {
        let polls = Arc::new(std::sync::atomic::AtomicU64::new(0));
        (counted_source(items, polls.clone()), polls)
    }

    async fn drain_sizes(s: BaseBatchStream) -> Vec<std::result::Result<usize, String>> {
        s.map(|item| item.map(|b| b.num_rows()).map_err(|e| e.to_string()))
            .collect::<Vec<_>>()
            .await
    }

    fn sized_items(sizes: &[i32]) -> Vec<Result<RecordBatch>> {
        let mut start = 0;
        sizes
            .iter()
            .map(|&n| {
                let (_, b) = initial_prefetch_batch(start, n);
                start += n;
                Ok(b)
            })
            .collect()
    }

    /// T1 — a file with MORE batches than the prefetch depth comes out WHOLE and
    /// UNALTERED: same batch sequence, same schema, same values.
    ///
    /// Discriminating input: five batches of distinct, non-uniform row counts
    /// (3,1,4,1,5) against a prefetch of 2, compared against the unprefetched
    /// stream. Full `RecordBatch` equality rather than row counts is what makes
    /// this discriminate, twice over: a concatenation — the exact bug ENG-48159's
    /// laziness fix exists to avoid — preserves the row TOTAL and destroys the
    /// sequence, so a total-only assertion would pass it; and batch equality
    /// covers schema and every column value, so the prefetch's column-agnosticism
    /// is pinned in place too.
    #[tokio::test]
    async fn initial_prefetch_preserves_batches_exactly_when_file_exceeds_prefetch_depth() {
        let sizes = [3, 1, 4, 1, 5];
        let prefetched: Vec<RecordBatch> =
            prefetch_initial_batches(counted(sized_items(&sizes)).0, 2)
                .await
                .map(|r| r.unwrap())
                .collect()
                .await;
        let plain: Vec<RecordBatch> = counted(sized_items(&sizes))
            .0
            .map(|r| r.unwrap())
            .collect()
            .await;
        assert_eq!(
            prefetched, plain,
            "a prefetched source must yield the identical batch sequence"
        );
        assert_eq!(
            prefetched.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![3, 1, 4, 1, 5],
            "batch boundaries must survive the prefetch"
        );
    }

    /// T2 — a file SHORTER than the prefetch depth is not truncated and does not
    /// grow. Three batches, depth 5: the prefetch loop hits the real end of the
    /// stream while still buffering.
    #[tokio::test]
    async fn initial_prefetch_handles_file_shorter_than_prefetch_depth() {
        let (src, _) = counted(sized_items(&[2, 2, 2]));
        assert_eq!(
            drain_sizes(prefetch_initial_batches(src, 5).await).await,
            vec![Ok(2), Ok(2), Ok(2)]
        );
    }

    /// T3 — depth 0 is the documented off switch and must be byte-for-byte the
    /// unprefetched stream, *including* touching the underlying stream not at all
    /// before the consumer asks.
    #[tokio::test]
    async fn initial_prefetch_depth_zero_matches_non_prefetched_stream() {
        let (src, polls) = counted(sized_items(&[3, 1, 4]));
        let out = prefetch_initial_batches(src, 0).await;
        assert_eq!(
            polls.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "depth 0 must not poll the stream during construction"
        );
        assert_eq!(drain_sizes(out).await, vec![Ok(3), Ok(1), Ok(4)]);
    }

    /// T4 — **the invariant that matters most.** A read error inside the
    /// prefetched prefix is returned AT ITS OWN POSITION, and never collapses
    /// into end-of-stream. A prefetch that gave up early and reported success is
    /// a silent short read: the rows are simply gone, and every caller sees a
    /// valid, shorter table.
    ///
    /// Items are `Ok, Err, Ok` against a depth of 3, so the error is *inside* the
    /// prefetched prefix rather than after it.
    #[tokio::test]
    async fn initial_prefetch_read_error_surfaces_in_order_and_never_as_end_of_stream() {
        let (_, b0) = initial_prefetch_batch(0, 3);
        let (_, b2) = initial_prefetch_batch(10, 5);
        let items: Vec<Result<RecordBatch>> = vec![
            Ok(b0),
            Err(CoreError::ReadFileSliceError("injected".to_string())),
            Ok(b2),
        ];
        let out = drain_sizes(prefetch_initial_batches(counted(items).0, 3).await).await;
        assert_eq!(out.len(), 3, "the error must not truncate the stream");
        assert_eq!(out[0], Ok(3));
        assert!(
            matches!(&out[1], Err(e) if e.contains("injected")),
            "the error must surface at its own position, got {:?}",
            out[1]
        );
        assert_eq!(
            out[2],
            Ok(5),
            "items after the error must still be delivered"
        );
    }

    /// T5 — an exhausted stream is never polled again. The prefetch loop drains a
    /// 1-batch stream at depth 3, which means it has already seen the `None`; the
    /// returned stream must not ask for another.
    ///
    /// Polls are counted rather than items because `futures::stream::iter` yields
    /// `None` forever, so an item-counting assertion cannot fail however many
    /// times the stream is re-polled.
    #[tokio::test]
    async fn initial_prefetch_does_not_repoll_a_completed_stream() {
        let (src, polls) = counted(sized_items(&[7]));
        let out = prefetch_initial_batches(src, 3).await;
        let seen = polls.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            seen, 2,
            "depth 3 over a 1-batch stream must poll exactly twice: the batch, then the end"
        );
        assert_eq!(drain_sizes(out).await, vec![Ok(7)]);
        assert_eq!(
            polls.load(std::sync::atomic::Ordering::Relaxed),
            seen,
            "draining the prefetched stream must not re-poll the exhausted source"
        );
    }

    /// T6 — an empty file. Depth 2 over zero batches must yield nothing and must
    /// not treat the immediate `None` as anything other than a real end.
    #[tokio::test]
    async fn initial_prefetch_handles_a_stream_with_no_batches() {
        let (src, polls) = counted(vec![]);
        let out = prefetch_initial_batches(src, 2).await;
        assert_eq!(
            polls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the prefetch must stop at the first None rather than keep asking"
        );
        assert!(drain_sizes(out).await.is_empty());
    }

    /// The prefetch is bounded — it must NOT drain the file. Ten batches at depth
    /// 2: exactly two are pulled before the consumer asks for anything.
    ///
    /// This is what separates ENG-48159's remedy from the eager path it replaced.
    #[tokio::test]
    async fn initial_prefetch_is_bounded_and_does_not_drain_the_file() {
        let (src, polls) = counted(sized_items(&[1; 10]));
        let out = prefetch_initial_batches(src, 2).await;
        assert_eq!(
            polls.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "exactly `depth` polls before the consumer asks"
        );
        assert_eq!(drain_sizes(out).await.len(), 10);
    }

    /// **The prefetch is actually WIRED INTO `base_file_source()`.**
    ///
    /// Every other test in this family calls `prefetch_initial_batches` directly,
    /// which means deleting the single call site — i.e. deleting the feature from
    /// the shipped read — leaves all of them green. That is the charter's trap 3
    /// and `I-30` exactly: a surviving mutation hiding in the one dimension the
    /// fixture holds constant. This test is the one that observes the call site.
    ///
    /// `rows_out` is incremented per batch inside the parquet stream's map
    /// (`base_file/parquet.rs`), so it counts batches that were actually pulled.
    /// The fixture is three row groups of one row, so at depth 2 exactly two rows
    /// have been paid for before the consumer asks for anything — and **0** if the
    /// call site is removed, because the object-store stream is lazy without it.
    #[tokio::test(flavor = "multi_thread")]
    async fn base_file_source_pays_for_the_prefix_before_it_returns() {
        use std::sync::atomic::Ordering::Relaxed;

        const FIXTURE_ROWS: u64 = 3;
        assert!(
            (BASE_READ_INITIAL_PREFETCH_BATCHES as u64) < FIXTURE_ROWS,
            "this test only discriminates while the prefetch depth is BELOW the \
             fixture's row-group count: at or above it the whole file is prefetched \
             and 'before it returns' asserts nothing. Widen three_row_groups() if \
             the constant grows."
        );

        let (tmp, base_name, schema) = three_row_groups();
        let mut reader = test_file_group_reader_for_base_file(tmp.path(), &base_name, schema).await;
        let volume = reader.storage.read_volume();
        assert_eq!(volume.rows_out.load(Relaxed), 0, "nothing read before open");

        let source = reader.base_file_source().await.unwrap();

        assert_eq!(
            volume.rows_out.load(Relaxed),
            BASE_READ_INITIAL_PREFETCH_BATCHES as u64,
            "base_file_source must pay for BASE_READ_INITIAL_PREFETCH_BATCHES batches \
             BEFORE handing the stream back — 0 here means the prefetch is not wired in"
        );

        // ...and the rest is still lazy and still complete: the prefix is served
        // first and the remaining row group follows, three rows in file order.
        let out = drain_base_source(source).await;
        assert_eq!(out.num_rows(), 3, "the whole file still arrives");
        assert_eq!(
            id_values(&out),
            vec![7, 8, 9],
            "and in file order — the prefetched prefix is not reordered"
        );
        assert_eq!(
            volume.rows_out.load(Relaxed),
            3,
            "exactly the file's rows were read, so the prefetch did not double-read"
        );
    }

    /// The constant this tree actually uses is a real, small depth. Pinned because
    /// `0` silently disables the feature and nothing else would fail.
    ///
    /// `const` blocks, so this is a COMPILE error rather than a test failure — the
    /// value is known at compile time and `clippy::assertions_on_constants` (which
    /// CI runs with `--all-targets -D warnings`) rejects a runtime `assert!` on it.
    /// Compile-time is the better gate anyway.
    #[test]
    fn base_read_initial_prefetch_batches_is_enabled_and_bounded() {
        const {
            assert!(
                BASE_READ_INITIAL_PREFETCH_BATCHES > 0,
                "0 disables ENG-48159's prefetch entirely"
            );
        }
        const {
            assert!(
                BASE_READ_INITIAL_PREFETCH_BATCHES <= 4,
                "the prefetch must stay bounded and small, or it becomes the eager path"
            );
        }
    }

    // ------------------------------------------------------------------
    // ENG-48159 — the RE-MEASUREMENT required by m22's AC-2.
    //
    //   cargo test -p hudi-core --release --lib eng_48159_prefetch_bench \
    //       -- --ignored --nocapture
    //
    // Release only; a debug build says nothing about production cost.
    //
    // ⚠️ READ THIS BEFORE QUOTING ANY NUMBER FROM IT.
    //
    // Internal's 1.82x is a TPC-DS 10 TB q88+q76 figure from a Velox cluster,
    // measured against internal's SYNC-READER shape. It is not reproducible here
    // and this bench does not try: there is no Velox driver, no preload thread, no
    // TPC-DS data and no cluster. Quoting 1.82x for this tree would be carrying a
    // performance claim across a different execution shape, which is what m22's
    // AC-2 exists to stop.
    //
    // What IS measurable here is the MECHANISM the 1.82x is attributed to: the
    // prefetch moves the cost of the first N batches off whoever consumes the
    // stream and onto whoever awaits `base_file_source()`. On the FFI path those
    // are different threads — `open()` is driven by
    // `OBJECT_STORE_RUNTIME.block_on` on Velox's preload thread, and the stream is
    // drained by the driver — so cost moved here is cost taken off the driver.
    // Whether that buys 1.82x on a cluster is a cluster question.
    //
    // Both arms read the SAME real parquet file through the SAME object-store
    // reader; the only difference is the prefetch depth.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "benchmark; run explicitly with --release --ignored --nocapture"]
    async fn eng_48159_prefetch_bench() {
        use std::time::Instant;

        const ROWS: i32 = 400_000;
        const ROWS_PER_GROUP: usize = 4_096;
        const REPS: usize = 7;

        let tmp = tempfile::tempdir().unwrap();
        let (schema, batch) = initial_prefetch_batch(0, ROWS);
        let base_name = "f1-0_0-1-1_001.parquet";
        write_parquet_file_in_row_groups(tmp.path(), base_name, &batch, ROWS_PER_GROUP);
        let bytes = std::fs::metadata(tmp.path().join(base_name)).unwrap().len();

        // Rebuild the object-store leg of `base_file_source()` exactly: the same
        // reader, the same options, the same per-batch evolution map. Only the
        // prefetch depth differs between the arms.
        async fn raw_stream(
            reader: &HoodieFileGroupReader,
            base_name: &str,
            evolve_to: SchemaRef,
        ) -> BaseBatchStream {
            let s = reader
                .base_file_reader()
                .unwrap()
                .read_stream(
                    base_name,
                    base_read_options(None, None, None, None, false)
                        .with_projection(evolve_to.fields().iter().map(|f| f.name())),
                )
                .await
                .unwrap();
            futures::StreamExt::map(s.into_stream(), move |b| match b {
                Ok(batch) => {
                    crate::schema::batch_evolution::project_batch_to_schema(&batch, &evolve_to)
                }
                Err(e) => Err(CoreError::from(e)),
            })
            .boxed()
        }

        let reader =
            test_file_group_reader_for_base_file(tmp.path(), base_name, schema.clone()).await;

        /// One timed pass at `depth`: (open us, time-to-first us, total us).
        async fn one_pass(
            reader: &HoodieFileGroupReader,
            base_name: &str,
            schema: SchemaRef,
            depth: usize,
        ) -> (f64, f64, f64) {
            let t0 = Instant::now();
            let s = raw_stream(reader, base_name, schema).await;
            let mut s = prefetch_initial_batches(s, depth).await;
            let open = t0.elapsed();

            let t1 = Instant::now();
            let first = futures::StreamExt::next(&mut s).await;
            let ttfb = t1.elapsed();
            assert!(first.is_some() && first.unwrap().is_ok());

            let t2 = Instant::now();
            let mut n = 1usize;
            while let Some(item) = futures::StreamExt::next(&mut s).await {
                item.unwrap();
                n += 1;
            }
            let rest = t2.elapsed();
            assert!(n > 1, "the fixture must have several batches, got {n}");
            (
                open.as_micros() as f64,
                ttfb.as_micros() as f64,
                (open + ttfb + rest).as_micros() as f64,
            )
        }

        let depths = [0usize, BASE_READ_INITIAL_PREFETCH_BATCHES];

        // ⚠️ WARM-UP, then INTERLEAVE. Both are load-bearing. Blocked arms — all
        // reps of one depth, then all of the other — let the second arm read a
        // page cache the first just warmed, which is enough to invert the sign of
        // every figure this bench prints, including making `open()` look FASTER
        // with a prefetch that does strictly more work. Two warm-up passes per
        // arm, then alternate, so neither arm owns the cold cache.
        for d in depths {
            for _ in 0..2 {
                one_pass(&reader, base_name, schema.clone(), d).await;
            }
        }

        let mut samples: Vec<(Vec<f64>, Vec<f64>, Vec<f64>)> =
            depths.iter().map(|_| (vec![], vec![], vec![])).collect();
        for rep in 0..REPS {
            // Alternate which arm goes first each rep, so any residual ordering
            // effect cancels instead of accumulating into one arm.
            let order: Vec<usize> = if rep % 2 == 0 { vec![0, 1] } else { vec![1, 0] };
            for i in order {
                let (o, f, t) = one_pass(&reader, base_name, schema.clone(), depths[i]).await;
                samples[i].0.push(o);
                samples[i].1.push(f);
                samples[i].2.push(t);
            }
        }

        // Report SPREAD, not just a median. Seven medians with no dispersion let a
        // reader take a 31 us delta on a 136 us baseline as a result when it may be
        // noise; that is the reading this bench's first artifact invited.
        /// median, min, max of one arm's samples for one quantity.
        struct Spread {
            med: f64,
            min: f64,
            max: f64,
        }
        /// One arm's three quantities.
        struct Arm {
            depth: usize,
            open: Spread,
            first: Spread,
            total: Spread,
        }
        let stat = |mut v: Vec<f64>| -> Spread {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            Spread {
                med: v[v.len() / 2],
                min: v[0],
                max: v[v.len() - 1],
            }
        };
        let rows: Vec<Arm> = depths
            .iter()
            .zip(samples)
            .map(|(d, (o, f, t))| Arm {
                depth: *d,
                open: stat(o),
                first: stat(f),
                total: stat(t),
            })
            .collect();

        println!("\n=== ENG-48159 re-measurement on this tree's BoxStream shape ===");
        println!(
            "fixture: {ROWS} rows, {ROWS_PER_GROUP} rows/row-group, {bytes} bytes on disk, \
             median of {REPS} reps, local object store"
        );
        println!(
            "\n{:<8} {:>24} {:>24} {:>26}",
            "depth",
            "open() us med[min-max]",
            "t-to-first us med[min-max]",
            "total us med[min-max]"
        );
        for a in &rows {
            println!(
                "{:<8} {:>12.0} [{:.0}-{:.0}] {:>12.0} [{:.0}-{:.0}] {:>13.0} [{:.0}-{:.0}]",
                a.depth,
                a.open.med,
                a.open.min,
                a.open.max,
                a.first.med,
                a.first.min,
                a.first.max,
                a.total.med,
                a.total.min,
                a.total.max
            );
        }
        let (o0, f0, t0) = (&rows[0].open, &rows[0].first, &rows[0].total);
        let (d1, o1, f1, t1) = (rows[1].depth, &rows[1].open, &rows[1].first, &rows[1].total);
        let pct = |a: f64, b: f64| {
            if b == 0.0 {
                f64::NAN
            } else {
                100.0 * (a - b) / b
            }
        };
        println!(
            "\ndepth {d1} vs depth 0, on the MEDIANS:\n  open()           {:+.0} us  ({:+.1}%)\n  \
             time-to-first    {:+.0} us  ({:+.1}%)\n  total            {:+.0} us  ({:+.1}%)",
            o1.med - o0.med,
            pct(o1.med, o0.med),
            f1.med - f0.med,
            pct(f1.med, f0.med),
            t1.med - t0.med,
            pct(t1.med, t0.med)
        );
        println!(
            "\nWHAT THIS DOES AND DOES NOT SHOW -- read the spreads above before quoting a delta.\n\
             \n\
             Supported: work MOVES into `open()`. `open()` rises; the consumer's first `next()`\n\
             falls to a VecDeque pop. It is NOT a throughput win -- `total` is a near-wash --\n\
             and on the FFI path the two sides are different threads, which is where the\n\
             cluster-level win came from. That part is NOT measured here.\n\
             \n\
             NOT supported, and do not write it down:\n\
             * the depth-2 time-to-first figure is a RESOLUTION FLOOR, not a measurement --\n\
               popping a VecDeque is below `as_micros()` granularity, so it reads 0 and the\n\
               percentage is a division artifact;\n\
             * the two deltas do NOT have to cancel. The prefetch pulls `depth` batches into\n\
               `open()` and removes only the FIRST from `next()`, so `open()` should rise by\n\
               roughly depth x one batch while first-next falls by one. Any arithmetic that\n\
               makes them balance is coincidence at this sample size;\n\
             * the residue in `total` is per-poll `chain` overhead across every row group,\n\
               not a once-per-file allocation;\n\
             * both arms read a page-cache-warm LOCAL file, so what moved here is DECODE.\n\
               The object-store fetch this feature exists to move is not exercised."
        );
    }
}
