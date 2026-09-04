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
pub mod base_file_provider;
pub mod blocking_merge_stream;
pub mod context;
pub mod provider_abi;
mod util;

/// Re-export core types for integration tests and downstream consumers.
pub use hudi_dep as hudi_core;

use crate::base_file_provider::{BaseFileDataProviderRef, BaseFileProviderStats};
use crate::blocking_merge_stream::BlockingMergeStream;
use crate::context::FileGroupReaderContext;
use crate::util::{create_raw_pointer_for_record_batch_reader, free_arrow_stream};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;
use hudi_dep::config::HudiConfigs;
use hudi_dep::config::table::HudiTableConfig;
use hudi_dep::config::util::split_hudi_options_from_others;
use hudi_dep::ffi_support::InstantRange;
use hudi_dep::ffi_support::OBJECT_STORE_RUNTIME;
use hudi_dep::ffi_support::{
    CompletionGateInputs, FileGroupReaderSchemaHandler,
    HoodieFileGroupReader as CoreFileGroupReader, InputSplit, ReaderContext, ReaderParameters,
    RecordContext,
};
use hudi_dep::file_group::base_file::BaseFile;
use hudi_dep::storage::Storage;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

mod predicate;
use predicate::PushedFilter;

/// Log the result of the ENG-40156 post-merge row filter at debug level. Shared
/// by the eager (`read_record_batch`) and streaming (`get_closable_iterator`)
/// filter sites so the single format string has one home. A macro (not a fn/const)
/// because `log::debug!` requires a string-literal format; expands to the `debug!`
/// call so filtered-out logging stays lazy. Fires once per batch/chunk (CLAUDE.md §10).
macro_rules! log_post_merge_filter {
    ($pre_rows:expr, $filtered:expr, $filter:expr) => {
        log::debug!(
            "[ENG-40156] post-merge filter: {} -> {} rows (cols={:?})",
            $pre_rows,
            $filtered.num_rows(),
            $filter.columns()
        )
    };
}

/// Warn the first time a call site fires in this process, then drop to debug.
///
/// For deterministic failures on per-batch or per-file paths: the condition that
/// makes the first call warn is still true on every later call, so a plain `warn!`
/// there emits one line per batch at the default `info` filter. The first line
/// keeps the failure visible; the rest stay available under `RUST_LOG=debug`.
/// A macro, not a fn, so the format arguments stay lazy and each expansion gets
/// its own flag.
macro_rules! warn_once {
    ($($arg:tt)*) => {{
        static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            log::debug!($($arg)*);
        } else {
            log::warn!($($arg)*);
        }
    }};
}
pub(crate) use warn_once;

static LOGGER: OnceLock<()> = OnceLock::new();

/// Initialize env_logger exactly once for the lifetime of the loaded shared library.
///
/// Uses an env_logger `Builder` with an explicit default filter instead of
/// mutating `RUST_LOG` via `std::env::set_var` (review C1). `set_var` is
/// `unsafe` and unsound in a multi-threaded process — the env is process-global
/// and another thread reading `getenv` concurrently is UB. The Builder honors
/// `RUST_LOG` when set and falls back to `info` only when it is absent, with no
/// environment mutation.
fn init_logger() {
    LOGGER.get_or_init(|| {
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .try_init();
    });
}

#[cxx::bridge]
mod ffi {
    // ════════════════════════════════════════════════════════════════════════
    // ENG-40156 — predicate pushdown wire format
    // ════════════════════════════════════════════════════════════════════════
    //
    // Velox HudiSplitReader forwards the Substrait filter that Gluten emitted
    // for this ReadRel, wrapped in a substrait::proto::ExtendedExpression so
    // the hudi-rs side has everything it needs in one blob:
    //   - the filter Expression itself
    //   - a NamedStruct base_schema (so substrait field indices resolve to
    //     hudi-rs column names)
    //   - SimpleExtensionDeclaration entries that map function_reference ints
    //     to URI-qualified names like "lt:any_any"
    //
    // The bytes are produced inside Gluten's SubstraitToVeloxPlan converter,
    // stashed on HiveTableHandle, and pulled back out by HudiSplitReader.
    // Empty bytes mean the scan has no pushable predicate; hudi-rs returns
    // every row and Velox's existing post-scan filter does all the work.

    /// Mirrors `HudiReadOptions.LogFile` proto (CXX-safe, used in `Vec`).
    #[derive(Default)]
    struct FfiLogFile {
        path_str: String,
        file_id: String,
        delta_commit_time: String,
        log_version: i32,
        log_write_token: String,
        file_extension: String,
        suffix: String,
        file_size: i64,
    }

    /// FFI-safe flat representation of `HudiReadOptions.HoodieFileGroupReaderContext`.
    ///
    /// Contains everything the file group reader needs:
    ///   - Table path (used as `hoodie.base.path` for Storage)
    ///   - Partition, commit time, reader flags
    ///   - Config maps (table_config, props, hoodie_reader_config)
    ///   - Base file / log file details
    ///   - Schemas, merge mode, instant range
    ///   - Split-level transport fields (base_file_name, log_file_names)
    #[derive(Default)]
    struct FfiReaderContext {
        // ── outer primitives (HoodieFileGroupReaderContext fields 1–9) ──
        table_path: String,
        partition_path: String,
        latest_commit_time: String,
        start: i64,
        length: i64,
        should_use_record_position: bool,
        allow_inflight_instants: bool,
        emit_delete: bool,
        sort_output: bool,

        // ── props (proto field 10) ───────────────────────────────────────
        props_keys: Vec<String>,
        props_values: Vec<String>,

        // ── BaseFile (proto field 11, flattened) ─────────────────────────
        has_base_file: bool,
        base_file_path: String,
        base_file_file_name: String,
        base_file_file_size: i64,
        base_file_file_id: String,
        base_file_commit_time: String,
        base_file_has_bootstrap: bool,
        base_file_bootstrap_path: String,
        base_file_bootstrap_file_name: String,
        base_file_bootstrap_file_size: i64,
        base_file_bootstrap_file_id: String,
        base_file_bootstrap_commit_time: String,

        // ── LogFile[] (proto field 12) ────────────────────────────────────
        log_file_details: Vec<FfiLogFile>,

        // ── HoodieSchema (proto fields 13–14, inlined) ───────────────────
        data_schema_json: String,
        requested_schema_json: String,

        // ── ReaderContext (proto field 15, flattened) ─────────────────────
        base_file_format: String,
        has_log_files: bool,
        has_bootstrap_base_file: bool,
        needs_bootstrap_merge: bool,
        should_merge_use_record_position: bool,
        enable_logical_timestamp_field_repair: bool,
        iterator_mode: String,
        merge_mode: String,
        merge_strategy_id: String,
        has_instant_range: bool,
        instant_range_start: String,
        instant_range_end: String,
        instant_range_type: String,
        // ── C-INFLIGHT-DELTA completion gate (proto fields 15–18) ─────────
        // Active-timeline completed/inflight instant times + archived boundary.
        // Consumed only when apply_completion_gate is true (table version < 8
        // snapshot reads); otherwise hudi-rs uses None (prior behavior).
        completed_instants: Vec<String>,
        inflight_instants: Vec<String>,
        // Empty string means None (no archived-boundary fallback).
        archived_boundary: String,
        apply_completion_gate: bool,
        table_config_keys: Vec<String>,
        table_config_values: Vec<String>,
        hoodie_reader_config_keys: Vec<String>,
        hoodie_reader_config_values: Vec<String>,

        // ── file-slice split fields (not in proto — transport only) ──────
        base_file_name: String,
        log_file_names: Vec<String>,

        // ── predicate pushdown (ENG-40156) ────────────────────────────────
        // prost-serialized substrait::proto::ExtendedExpression. Empty when
        // the caller pushed no predicate (e.g. unfiltered scan, or Gluten
        // didn't run the Hudi pushdown path for this query).
        substrait_filter_bytes: Vec<u8>,

        // ── base-file data provider (C ABI) ──────────────────────────────
        // Opaque handle from `hudi_base_file_data_provider_new`, or 0 for no provider.
        // The composition root builds the concrete provider, wraps it into a
        // handle, and stashes it here; `new_file_group_reader_with_context`
        // consumes it into the injected provider. Carried as an integer because
        // C function pointers cannot live in a cxx shared struct; see
        // `provider_abi` for the ownership contract.
        base_file_provider_handle: u64,
    }

    /// Base-file provider read counters for one file group, returned to the
    /// C++ consumer (Velox `HudiSplitReader`) after the merged stream is drained.
    /// Maps onto `crate::base_file_provider::BaseFileProviderStats`
    /// field-for-field (all `u64`, FFI-safe); the consumer maps these onto its
    /// operator stats keys.
    #[derive(Default)]
    struct FfiBaseFileProviderStats {
        discover_wall_nanos: u64,
        connect_wall_nanos: u64,
        fetch_wall_nanos: u64,
        batches_received: u64,
        bytes_served: u64,
        rows_served: u64,
        files_served: u64,
        storage_fallbacks: u64,
        local_served: u64,
        remote_served: u64,
    }

    unsafe extern "C++" {
        include!("arrow/c/abi.h");

        type ArrowArrayStream;
    }

    extern "Rust" {
        type HoodieFileGroupReader;

        /// Create a file group reader from the full context.
        /// `table_path` inside `ctx` is used as the storage base URI.
        fn new_file_group_reader_with_context(
            ctx: FfiReaderContext,
        ) -> Result<Box<HoodieFileGroupReader>>;

        /// Read the file group and return merged results as an `ArrowArrayStream`.
        ///
        /// OWNERSHIP CONTRACT (review C2): the returned `*mut ArrowArrayStream`
        /// is a Rust-heap-allocated, leaked pointer. The C++ caller takes
        /// ownership and MUST free EVERY successfully-returned stream via
        /// `hudi_free_arrow_stream` EXACTLY ONCE — including on exception/early-
        /// return paths after the call returns. Failing to free leaks the stream
        /// and its Arrow buffers; freeing twice is a double-free (UB).
        ///
        /// C++ side: wrap the returned pointer in an RAII guard
        /// (e.g. `std::unique_ptr<ArrowArrayStream, &hudi_free_arrow_stream>`)
        /// at the call site so the free runs on every code path. The C++ guard
        /// itself lands with the gluten consumer (M2 docket); this contract is
        /// the Rust-side half.
        fn get_closable_iterator(self: &HoodieFileGroupReader) -> Result<*mut ArrowArrayStream>;

        /// The reader's PEAK in-memory native footprint for this file group, in
        /// bytes (ENG-44436).
        ///
        /// Returns the tracked footprint of the merge map hudi-rs holds (see
        /// `SpillableRecordMap::current_in_memory_bytes` — the pinned source
        /// batches + owned/key/overhead bytes; NOT the RocksDB spill tier or the
        /// produced Arrow output batch). It is published ONCE, at
        /// `get_closable_iterator` time, as the post-`open()` value — which is the
        /// true maximum for the read: `open()` loads every log record into the
        /// merge map during the log scan, and from then on the base file is
        /// *streamed* through the merge (base rows are never accumulated into the
        /// map), so the footprint only decreases as chunks drain. It is therefore
        /// the high-water mark a host memory manager should reserve for the whole
        /// read. Returns 0 before `get_closable_iterator` and for a base-only
        /// (no-merge) slice.
        ///
        /// Intended for a host memory manager (velox's `MemoryPool`) to RESERVE
        /// against hudi-rs's native usage so velox's arbitration reflects it. Cheap
        /// (a single relaxed atomic load).
        ///
        /// Thread-safety: the publish happens on the IO thread inside
        /// `get_closable_iterator` (Velox's split preload) and the read happens on
        /// the driver thread in `next()`. The happens-before that makes the
        /// published value visible is Velox's prepareSplit→next() split handoff,
        /// NOT the `Relaxed` atomic ordering — the atomic only guarantees the load
        /// is not torn.
        fn hudi_reader_memory_bytes(self: &HoodieFileGroupReader) -> u64;

        /// Base-file provider read counters for the last `get_closable_iterator`
        /// call. Because the provider is now STREAMED, the counters split by when
        /// they are final:
        ///   - **setup counters** (files_served, storage_fallbacks, local/remote,
        ///     discover/connect/fetch wall-nanos) are populated by `open()`, before
        ///     any batch is streamed — read them any time after
        ///     `get_closable_iterator`;
        ///   - **drain counters** (rows_served, bytes_served, batches_received) are
        ///     tallied as the consumer drains the returned stream, so they are only
        ///     final AFTER the stream is fully drained. Read this after draining to
        ///     capture them.
        /// Reads a shared live slot, so it does not depend on the stream still being
        /// alive (safe to call after the stream is freed). Returns all-zero when no
        /// provider was injected (the default) or `get_closable_iterator` has not
        /// been called on this reader.
        fn base_file_provider_stats(self: &HoodieFileGroupReader) -> FfiBaseFileProviderStats;
        /// [ENG-47483] Bytes fetched from storage by this reader, summed over every
        /// range read, counted at the `AsyncFileReader` boundary.
        ///
        /// Exact and page-cache independent: a warm re-read reports the same number
        /// as a cold one, which is what makes it comparable across runs where wall
        /// time is not. NOT final until the split has drained.
        ///
        /// This is the metric that separates pushdown that WORKS from pushdown that
        /// HELPS. `hudi_pushdown_row_filters_installed` can read healthy on every
        /// slice while this number is unchanged, because nothing on the FFI path
        /// prunes row groups — the predicate columns are still fetched in full.
        fn hudi_bytes_read(self: &HoodieFileGroupReader) -> u64;

        /// [ENG-47483] Storage round trips (`get_bytes` / `get_byte_ranges` calls).
        /// A two-pass read — predicate columns first, then the selected rows — shows
        /// up as roughly double the calls of a single-pass read over the same file,
        /// which is how the cost of installing a row filter becomes measured rather
        /// than inferred.
        fn hudi_io_calls(self: &HoodieFileGroupReader) -> u64;

        /// [ENG-47483] Rows this reader's stream yielded, after any parquet row
        /// filter. Read against `hudi_file_rows` for selectivity, and against
        /// `hudi_bytes_read` for what that selectivity cost.
        fn hudi_rows_out(self: &HoodieFileGroupReader) -> u64;

        /// [ENG-47483] Rows the base file contains, from parquet footer metadata —
        /// no extra IO, the footer is already read. Denominator for
        /// `hudi_rows_out`.
        fn hudi_file_rows(self: &HoodieFileGroupReader) -> u64;

        /// [ENG-47483] Row groups this reader actually read. Equals
        /// `hudi_file_row_groups` when nothing prunes; the GAP between them is
        /// the row-group pruning win, and is the only way to see that
        /// `with_row_groups` did anything. Velox's own "skipped row groups"
        /// counter reads 0 on this path because Velox is not doing the reading.
        fn hudi_row_groups_read(self: &HoodieFileGroupReader) -> u64;

        /// [ENG-47483] Row groups the base files contain, from footer metadata.
        /// Denominator for `hudi_row_groups_read`.
        fn hudi_file_row_groups(self: &HoodieFileGroupReader) -> u64;

        /// [ENG-47483] Times the row-group selector closure ran.
        ///
        /// Needed because the selector returns None when it cannot prune, so
        /// `hudi_row_groups_read == hudi_file_row_groups` is identical whether the
        /// selector ran and found nothing or was never installed. Without this,
        /// "pruning is correct and this data is unprunable" and "pruning never
        /// executed" are the same reading.
        fn hudi_row_group_selector_calls(self: &HoodieFileGroupReader) -> u64;

        /// Cumulative wall ms the merge spent producing output, across every
        /// chunk of this reader's stream. NOT final until the stream drains.
        ///
        /// 0 on a CoW read by construction: with no log files the iterator uses
        /// the Eager (no-merge) source, which has no merge work. That zero is
        /// informative next to the Velox-side `hudiRsReaderWallNanos` -- it says
        /// the FFI time went to the lazy parquet decode, not to merging.
        fn hudi_final_merge_ms(self: &HoodieFileGroupReader) -> u64;

        /// Cumulative wall ms spent turning merged records into output batches
        /// (`records_to_batch` plus the per-chunk output projection), across
        /// every chunk. NOT final until the stream drains. On a CoW read this is
        /// the output converter only.
        fn hudi_output_build_ms(self: &HoodieFileGroupReader) -> u64;

        /// [ENG-40156] 1 if the Substrait predicate Velox handed to
        /// `new_file_group_reader_with_context` DECODED into an evaluable
        /// filter, 0 otherwise (no bytes were passed, or `PushedFilter::decode`
        /// dropped them — malformed protobuf, missing base_schema, or a function
        /// outside the evaluator's inventory).
        ///
        /// Final immediately after reader construction; safe to read any time.
        ///
        /// Pair with `hudi_pushdown_row_filters_installed` and the caller's own
        /// "were bytes offered" count to separate the two causes of lost
        /// pushdown, which have opposite meanings: bytes offered but not decoded
        /// is a gap or a bug, decoded but not installed is a deliberate gate.
        fn hudi_pushdown_decoded(self: &HoodieFileGroupReader) -> u64;

        /// [ENG-40156] How many parquet `RowFilter`s this reader actually
        /// installed — the number of base files read with EARLY filtering.
        ///
        /// NOT final until the split has drained. `open()` holds the base file
        /// as a lazy `ParquetSyncReader` (see `FileGroupReader::open`), so the
        /// builder that produces the RowFilter runs on the first batch pull, not
        /// during `get_closable_iterator`. Reading this at prepareSplit time
        /// always yields 0 and would look like "pushdown broken" on every split.
        ///
        /// 0 with `hudi_pushdown_decoded() == 1` means the predicate decoded but
        /// no RowFilter was installed: the base-read pushdown gate (merging slice
        /// without a PK-only predicate), the ENG-42276 selectivity gate (no
        /// comparison op for parquet stats to prune on), or a referenced column
        /// missing from the parquet schema. The reason is logged at info by
        /// hudi-rs; only the count crosses this boundary.
        fn hudi_pushdown_row_filters_installed(self: &HoodieFileGroupReader) -> u64;

        /// Free an `ArrowArrayStream` that was returned by `get_closable_iterator`.
        ///
        /// This drops the stream (invoking the Arrow release callback to free
        /// internal buffers) and deallocates the struct itself through Rust's
        /// allocator. The caller must not use `ptr` after this call.
        ///
        /// Contract: call EXACTLY ONCE per stream returned by
        /// `get_closable_iterator` (see its ownership contract above). Calling it
        /// twice is a double-free; never calling it leaks the stream.
        unsafe fn hudi_free_arrow_stream(ptr: *mut ArrowArrayStream);
    }
}

pub struct HoodieFileGroupReader {
    // ── 1:1 with Java HoodieFileGroupReader fields ─────────────────
    reader_context: Arc<ReaderContext>,
    storage: Arc<Storage>,
    // Kept for 1:1 parity with Java HoodieFileGroupReader; merged props are
    // consumed via HudiConfigs at construction, so the field itself is unread.
    #[allow(dead_code)]
    props: HashMap<String, String>,
    reader_parameters: ReaderParameters,
    input_split: InputSplit,
    // Kept for 1:1 parity with Java HoodieFileGroupReader; partition values are
    // resolved from the input split, so this field is currently unread.
    #[allow(dead_code)]
    partition_path_fields: Option<Vec<String>>,

    // ── ENG-40156 predicate pushdown ────────────────────────────────
    // Decoded substrait predicate from `FfiReaderContext.substrait_filter_bytes`.
    // None when no predicate was pushed (or when the substrait expression
    // referenced functions outside the hudi-rs evaluator's inventory — in
    // that case PushedFilter::decode dropped the filter and Velox's
    // post-scan filter does all the work).
    //
    // Applied post-merge in read_record_batch — semantically safe for any
    // MOR shape because the merge has resolved log updates before we filter.
    pushed_filter: Option<PushedFilter>,
    // ── ENG-42276 v4.3 ────────────────────────────────────────────
    // No per-file-group tokio runtime any more — `reader.read()` is
    // driven on the long-lived `OBJECT_STORE_RUNTIME` so hyper's
    // connection dispatcher (spawned by the cached ObjectStore) shares
    // a lifetime with the requests. See storage/mod.rs OBJECT_STORE_RUNTIME
    // docs for the DispatchGone failure mode this prevents.

    // ── ENG-44436 native-memory accounting ─────────────────────────
    // The reader's PEAK merge-map footprint (bytes), published once at
    // `get_closable_iterator` time and read by `hudi_reader_memory_bytes`.
    // `AtomicU64` for interior mutability + a non-torn cross-thread read (the
    // publish/read happen-before comes from Velox's prepareSplit→next() handoff,
    // see the FFI doc). Starts at 0. The post-`open()` value is the true max
    // because the base file is streamed through the merge, not accumulated, so
    // the footprint never grows after `open()` — hence a single publish, not a
    // per-chunk refresh.
    reader_memory_bytes: AtomicU64,

    // ── base-file data provider (composition-root-injected via C ABI) ─────
    // The concrete provider, presented as hudi-core's trait object. `None`
    // when no provider handle was supplied on the FFI context (the common
    // path).
    //
    // Phase 1: held but never read. OSS core has no provider seam, so nothing
    // is injected into the reader builder (see `get_closable_iterator`). The
    // field still exists so `take_provider_from_handle` (which reclaims the
    // `Box` behind the FFI handle into this `Arc`) has somewhere to put its
    // result -- this field ends up owning the last strong reference, so
    // `destroy(ctx)` (`impl Drop for CApiBaseFileDataProvider`) runs exactly
    // when this FFI reader drops. Removing the `take_provider_from_handle`
    // call would leak the provider `ctx` (the boxed handle is never
    // reclaimed into a value that can drop). Dropping this field instead of
    // holding it for the reader's lifetime would free the provider -- and run
    // `destroy(ctx)` -- too early, while the reader (a Phase-2 consumer of
    // the provider) may still be in scope.
    #[allow(dead_code)]
    base_file_provider: Option<BaseFileDataProviderRef>,

    // ── base-file provider read counters ──────────────────────────────────────
    // Handle to the core reader's LIVE provider-stats slot, captured at
    // `get_closable_iterator` time and read back by `base_file_provider_stats()`.
    // Empty until a reader is opened. The slot is shared with the streaming
    // counting adapter, so the drain counters (rows/bytes/batches) fill in as the
    // C++ consumer drains the stream — read this AFTER draining for the complete
    // picture. All-zero when no provider served a base file.
    //
    // `OnceLock` rather than `Mutex<Option<…>>`: this is written exactly once, by
    // the first `get_closable_iterator`, and read many times thereafter — which is
    // precisely `OnceLock`'s contract. It also drops a lock layer and the
    // poison-recovery branch the outer `Mutex` needed. A re-open (`set` on an
    // already-initialised cell) keeps the first slot, which is the right outcome:
    // the returned stream from the first open may still be draining into it.
    base_file_provider_stats: OnceLock<Arc<Mutex<BaseFileProviderStats>>>,
    // ── ENG-40156 pushdown observability ───────────────────────────
    // How many times the `row_filter_builder` closure actually produced a
    // parquet `RowFilter` for this reader — i.e. how many base files were read
    // WITH early filtering. Incremented at the one place a RowFilter is handed
    // back to the parquet builder, never inferred from `pushed_filter.is_some()`:
    // a predicate can decode cleanly and still not be installed, via the
    // base-read pushdown gate, the ENG-42276 no-prunable-comparison gate, or
    // a column absent from the parquet schema. Distinguishing "decoded" from
    // "installed" is the whole point of the counter, so the two must not share
    // a source.
    //
    // `Arc<AtomicU64>` rather than a bare `AtomicU64`: the builder is an
    // `Arc<dyn Fn ... + Send + Sync>` that the parquet stream may evaluate on
    // any worker thread, so the counter must be shared with the closure and
    // outlive this struct's borrow. `Relaxed` matches `reader_memory_bytes` —
    // the FFI consumer reads it after the split has drained, so the
    // happens-before comes from Velox's split lifecycle, not this ordering.
    row_filters_installed: Arc<AtomicU64>,

    // ── [ENG-47483] read-volume counters ────────────────────────────
    // Clone of this reader's Storage-scoped ReadVolume, captured at construction so
    // it stays readable after the read without holding the Storage.
    //
    // These exist because pushdown DISPOSITION is not pushdown VALUE: the
    // offered/decoded/installed counters can all read healthy while every byte of
    // the file is still fetched, because nothing on this path prunes row groups.
    // bytes_read is counted at the AsyncFileReader boundary, so unlike wall time it
    // is exact and page-cache independent, and therefore comparable across runs.
    read_volume: Arc<hudi_dep::storage::ReadVolume>,

    // ── Iteration-phase timings (a) ─────────────────────────────────
    // Clone of the core reader's shared `StreamReadStats` sink, captured in
    // `get_closable_iterator` before the core reader is dropped. `OnceLock`
    // because that method takes `&self` and the handle exists only from the
    // first `open()` onward; set at most once per FFI reader.
    //
    // Read AFTER the stream drains -- the values accumulate per emitted chunk.
    //
    // Scope, stated because it is easy to over-read these numbers: on a CoW
    // read the file group has no log files, so the iterator takes the Eager
    // (no-merge) source where `final_merge_ms` is 0 BY CONSTRUCTION and
    // `output_build_ms` covers only the output converter. Neither includes the
    // lazy per-row-group parquet decode, which is the bulk of CoW read cost and
    // is timed on the Velox side (`hudiRsReaderWallNanos`). A near-zero pair
    // here beside a large FFI time is therefore the useful signal: it localises
    // the cost to decode rather than to merge or convert.
    stream_stats: OnceLock<hudi_dep::ffi_support::StreamStatsHandle>,
}

/// [ENG-40156] Wrap `PushedFilter::build_row_filter` in a `RowFilterBuilder`
/// that counts the times it actually produced a parquet `RowFilter`.
///
/// The count is taken from `build_row_filter`'s own return value, which is the
/// only signal that distinguishes "a predicate exists" from "early filtering is
/// really happening": `build_row_filter` has three skip paths (no referenced
/// fields, the ENG-42276 no-prunable-comparison gate, a column absent from the
/// parquet schema) and every one of them returns `None` invisibly to callers.
/// Counting anywhere else — at decode, or at builder construction — would
/// report pushdown that never occurred.
///
/// A standalone function rather than an inline closure so the counting contract
/// is reachable from a test without reconstructing an FFI context; the
/// production path in `new_file_group_reader_with_context` calls this same
/// function.
///
/// Invoked once per parquet file opened (so once per split on CoW, where a file
/// slice has a single base file), on whichever worker thread the parquet stream
/// runs on — hence the `Arc<AtomicU64>`.
pub(crate) fn counting_row_filter_builder(
    pushed_filter: &PushedFilter,
    installed_counter: Arc<AtomicU64>,
) -> hudi_dep::storage::RowFilterBuilder {
    let pf_for_closure = pushed_filter.clone();
    Arc::new(move |parquet_schema, _projected_schema| {
        let row_filter = pf_for_closure.build_row_filter(parquet_schema);
        if row_filter.is_some() {
            installed_counter.fetch_add(1, Ordering::Relaxed);
        }
        row_filter
    })
}

/// Creates a `HoodieFileGroupReader` from a full `FfiReaderContext`.
pub fn new_file_group_reader_with_context(
    ctx: ffi::FfiReaderContext,
) -> std::result::Result<Box<HoodieFileGroupReader>, String> {
    init_logger();

    // Capture transport-only file names before ctx is consumed by .into().
    let base_file_name = ctx.base_file_name.clone();
    let log_file_names: Vec<String> = ctx.log_file_names.to_vec();
    // ENG-40156 — snapshot substrait filter bytes before `ctx.into()` consumes ctx.
    let substrait_filter_bytes = ctx.substrait_filter_bytes.clone();
    // Snapshot the provider handle before `ctx.into()` consumes ctx.
    let base_file_provider_handle = ctx.base_file_provider_handle;

    // ENG-40156 v4.5 diagnostic — capture before ctx.into() consumes ctx.
    let use_record_position = ctx.should_use_record_position;

    // One line per process at the default `info` filter. Operators need on-by-default
    // evidence that dispatch reached hudi-rs at all rather than falling back to Velox's
    // own reader; the per-file-group detail below would be one line per split, so it is
    // debug!. Together they answer "did we engage" cheaply and "with what" on demand.
    static ENGAGED: std::sync::Once = std::sync::Once::new();
    ENGAGED.call_once(|| {
        log::info!(
            "[hudi-rs-reader] engaged: serving file group reads in this process \
             (set RUST_LOG=debug for per-file-group detail)"
        );
    });

    // Beacon: confirms hudi-rs reader was actually entered for this file group.
    // Run the executor with `RUST_LOG=debug` and grep stderr for `[hudi-rs-reader]`
    // to verify dispatch routed here. debug!, not info!: this fires once per file
    // group, so at the default `info` filter a wide scan would emit one line per
    // split (CLAUDE.md §10 — info! is for significant lifecycle events).
    // Fields chosen to also distinguish CoW (log_files_count=0) vs MOR (>0),
    // whether v4/v4.1 predicate pushdown was attempted (substrait_filter_bytes>0),
    // and whether position-based merge was requested (use_record_position) —
    // the latter is the flag that decides between the unimplemented
    // PositionBasedFileGroupRecordBuffer and the working
    // KeyBasedFileGroupRecordBuffer in DefaultFileGroupRecordBufferLoader.
    log::debug!(
        "[hudi-rs-reader] new_file_group_reader_with_context entered: \
         base_file_name={} log_files_count={} substrait_filter_bytes={} \
         use_record_position={}",
        base_file_name,
        log_file_names.len(),
        substrait_filter_bytes.len(),
        use_record_position,
    );

    // Convert flat FFI struct → nested Rust types (intermediate, not stored).
    let fgrc: FileGroupReaderContext = ctx.into();

    log::debug!(
        "new_file_group_reader_with_context: \
         table_path={table_path} partition_path={partition_path} \
         latest_commit_time={latest_commit_time} start={start} length={length} \
         should_use_record_position={surp} allow_inflight_instants={aii} \
         emit_delete={ed} sort_output={so} \
         props_count={props_count} base_file={base_file} log_files_count={log_files_count} \
         has_data_schema={has_data_schema} has_requested_schema={has_req_schema}",
        table_path = fgrc.table_path,
        partition_path = fgrc.partition_path,
        latest_commit_time = fgrc.latest_commit_time,
        start = fgrc.start,
        length = fgrc.length,
        surp = fgrc.should_use_record_position,
        aii = fgrc.allow_inflight_instants,
        ed = fgrc.emit_delete,
        so = fgrc.sort_output,
        props_count = fgrc.props.len(),
        base_file = fgrc
            .base_file
            .as_ref()
            .map(|bf| bf.file_name.as_str())
            .unwrap_or("<none>"),
        log_files_count = fgrc.log_files.len(),
        has_data_schema = fgrc.data_schema.is_some(),
        has_req_schema = fgrc.requested_schema.is_some(),
    );
    {
        let rc = &fgrc.reader_context;
        log::debug!(
            "new_file_group_reader_with_context: reader_context \
             table_path={table_path} latest_commit_time={latest_commit_time} \
             base_file_format={base_file_format} has_log_files={has_log_files} \
             needs_bootstrap_merge={nbm} should_merge_use_record_position={smurp} \
             iterator_mode={iterator_mode} merge_mode={merge_mode} \
             merge_strategy_id={merge_strategy_id} has_instant_range={has_instant_range} \
             table_config_count={table_config_count} hoodie_reader_config_count={hrc_count}",
            table_path = rc.table_path,
            latest_commit_time = rc.latest_commit_time,
            base_file_format = rc.base_file_format,
            has_log_files = rc.has_log_files,
            nbm = rc.needs_bootstrap_merge,
            smurp = rc.should_merge_use_record_position,
            iterator_mode = rc.iterator_mode,
            merge_mode = rc.merge_mode,
            merge_strategy_id = rc.merge_strategy_id,
            has_instant_range = rc.instant_range.is_some(),
            table_config_count = rc.table_config.len(),
            hrc_count = rc.hoodie_reader_config.len(),
        );
    }
    log::debug!(
        "new_file_group_reader_with_context: split base_file_name={base_file_name} \
         log_file_names={log_file_names:?}",
    );

    // ── 1. Build merged props ───────────────────────────────────────
    // Order: hoodie.base.path + table_config < props < hoodie_reader_config
    let mut options: Vec<(String, String)> = Vec::new();
    options.push((
        HudiTableConfig::BasePath.as_ref().to_string(),
        fgrc.table_path.clone(),
    ));
    for (k, v) in &fgrc.reader_context.table_config {
        options.push((k.clone(), v.clone()));
    }
    for (k, v) in &fgrc.props {
        options.push((k.clone(), v.clone()));
    }
    for (k, v) in &fgrc.reader_context.hoodie_reader_config {
        options.push((k.clone(), v.clone()));
    }

    // ── 2. Create Storage (needs temporary HudiConfigs) ─────────────
    let (hudi_opts, storage_opts) = split_hudi_options_from_others(options);
    let props: HashMap<String, String> = hudi_opts;
    let hudi_configs = Arc::new(HudiConfigs::new(props.clone()));
    let storage = Storage::new(Arc::new(storage_opts), hudi_configs)
        .map_err(|e| format!("Failed to create Storage: {e}"))?;

    // ── 3. Build ReaderParameters ───────────────────────────────────
    let reader_parameters = ReaderParameters {
        use_record_position: fgrc.should_use_record_position,
        emit_delete: fgrc.emit_delete,
        sort_output: fgrc.sort_output,
        allow_inflight_instants: fgrc.allow_inflight_instants,
    };

    // ── 4. Build InputSplit ─────────────────────────────────────────
    // Position-based merge validates a log block's positions against the base
    // file's commit time, so derive it from the base file name (format
    // `<fileId>_<writeToken>_<commitTime>.<ext>`). Best-effort: a parse failure
    // (or a log-only slice) leaves it None — position merge then falls back to
    // key-based merge, which needs no commit time.
    let base_file_commit_time = if base_file_name.is_empty() {
        None
    } else {
        match base_file_name.parse::<BaseFile>() {
            Ok(bf) => Some(bf.commit_timestamp),
            Err(e) => {
                log::warn!(
                    "new_file_group_reader_with_context: could not parse base-file commit time \
                     from '{base_file_name}': {e:?} (position-based merge will fall back to \
                     key-based if requested)"
                );
                None
            }
        }
    };
    let base_file_path = if base_file_name.is_empty() {
        None
    } else if fgrc.partition_path.is_empty() {
        Some(base_file_name)
    } else {
        Some(format!("{}/{}", fgrc.partition_path, base_file_name))
    };
    let log_file_paths: Vec<String> = log_file_names
        .into_iter()
        .map(|name| {
            if fgrc.partition_path.is_empty() {
                name
            } else {
                format!("{}/{}", fgrc.partition_path, name)
            }
        })
        .collect();
    let input_split = InputSplit::new(
        base_file_path,
        base_file_commit_time,
        log_file_paths,
        fgrc.partition_path,
    );

    // ── 5. Convert FFI ReaderContext → core ReaderContext ────────────
    let ffi_rc = fgrc.reader_context;
    let instant_range = ffi_rc.instant_range.map(|ir| {
        let timezone = ffi_rc
            .table_config
            .get(HudiTableConfig::TimelineTimezone.as_ref())
            .cloned()
            .unwrap_or_else(|| "utc".to_string());
        let (start_inclusive, end_inclusive) = match ir.range_type.as_str() {
            "CLOSED_CLOSED" => (true, true),
            "OPEN_CLOSED" => (false, true),
            "CLOSED_OPEN" => (true, false),
            _ => (false, true), // default
        };
        let start = if ir.start_instant.is_empty() {
            None
        } else {
            Some(ir.start_instant)
        };
        let end = if ir.end_instant.is_empty() {
            None
        } else {
            Some(ir.end_instant)
        };
        InstantRange::new(timezone, start, end, start_inclusive, end_inclusive)
    });
    // RecordContext is constructed from table_config, matching Java's
    // RecordContext(tableConfig, typeConverter) pattern. The table_config
    // carries hoodie.populate.meta.fields, hoodie.table.precombine.field,
    // and hoodie.table.recordkey.fields — RecordContext derives everything
    // from these.
    let partition_path = input_split.partition_path.clone();
    let record_context = RecordContext::new(&ffi_rc.table_config, partition_path);

    // ── Extract partition path fields from table config ─────────────
    // Mirrors Java's: tableConfig.getPartitionFields() which reads
    // "hoodie.table.partition.fields", splits on ",", and strips
    // custom key-generator partition type suffixes (split on ":").
    // e.g. "date:TIMESTAMP,region:SIMPLE" → ["date", "region"]
    let partition_path_fields: Option<Vec<String>> = ffi_rc
        .table_config
        .get("hoodie.table.partition.fields")
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().split(':').next().unwrap_or("").to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });

    // ── 6. Build schema handler from Avro schemas passed via FFI ──
    // Set on ReaderContext to match Java's HoodieReaderContext.schemaHandler.
    //
    // The `data_schema` from the Scala planning layer is the pruned table
    // data schema (full table columns minus "op" and partition columns).
    // This serves as both `table_schema` (field lookup source for mandatory
    // fields) and `data_schema` (base file reading schema).
    let schema_handler = {
        let mut handler = FileGroupReaderSchemaHandler::new();
        if let Some(hs) = fgrc.data_schema.as_ref()
            && let Ok(arrow_schema) = avro_json_to_arrow_schema(&hs.avro_schema_json)
        {
            let schema_ref = Arc::new(arrow_schema);
            handler = handler
                .with_table_schema(schema_ref.clone())
                .with_data_schema(schema_ref)
                .with_data_schema_json(hs.avro_schema_json.clone());
        }
        if let Some(hs) = fgrc.requested_schema.as_ref()
            && let Ok(arrow_schema) = avro_json_to_arrow_schema(&hs.avro_schema_json)
        {
            handler = handler
                .with_requested_schema(Arc::new(arrow_schema))
                .with_requested_schema_json(hs.avro_schema_json.clone());
        }
        handler
    };

    // ── 7. Decode ENG-40156 substrait predicate ──────────────────────
    // Predicates referring to functions we don't recognise are dropped (decode
    // returns Ok(None)). A decode Err (malformed protobuf bytes) is also
    // downgraded to a dropped filter rather than failing the read: Velox's
    // post-scan filter still evaluates the original predicate, so dropping is
    // always correct (we only lose the early-filtering perf benefit). Failing
    // the read on malformed bytes would be an unnecessary hard-fail.
    let pushed_filter = match PushedFilter::decode(&substrait_filter_bytes) {
        Ok(pf) => pf,
        Err(e) => {
            log::warn!(
                "[ENG-40156] could not decode pushed filter ({e}); dropping it \
                 and relying on Velox's post-scan filter to apply the original \
                 predicate"
            );
            None
        }
    };
    if let Some(ref pf) = pushed_filter {
        log::debug!(
            "[ENG-40156] decoded substrait predicate over columns {:?}",
            pf.columns()
        );
    }

    // ── 7a. ENG-42866 — compute MOR pushdown safety ──────────────────
    // Java's `SparkFileFormatInternalRowReaderContext.filterIsSafeForPrimaryKey`
    // pushes a filter into MOR base + log readers iff every column it
    // references is in the primary-key field set (or is the
    // _hoodie_record_key meta column). Primary keys are immutable across
    // upserts, so a PK-only predicate has the same outcome pre- and post-
    // merge.
    //
    // We compute the flag here (where we have both the decoded filter and
    // the table config) and stash it on the reader_context so the FG
    // reader gate at `make_base_file_batches` and the parquet log block
    // decoder in `Decoder::decode_parquet_record_content` see the same
    // decision.
    let record_key_fields = ReaderContext::record_key_fields_from(&ffi_rc.table_config);
    let mor_pk_safe = pushed_filter
        .as_ref()
        .map(|pf| pf.references_only_primary_keys(&record_key_fields))
        .unwrap_or(false);
    if let Some(ref pf) = pushed_filter {
        log::debug!(
            "[ENG-42866] mor_pk_safe={mor_pk_safe} (predicate columns={:?}, \
             primary keys={:?})",
            pf.columns(),
            record_key_fields,
        );
    }

    // Build the RowFilterBuilder closure once. It's installed onto the
    // reader_context only if there is a pushed filter; the actual
    // installation at scan time is then gated by
    // `HoodieFileGroupReader::base_read_pushdown_is_safe()`
    // (no log files on the slice, or mor_pk_safe) -- ENG-47506.
    // ENG-40156 pushdown observability — see the `row_filters_installed` field
    // doc. Counted HERE, wrapping `build_row_filter`'s own return value, because
    // this is the only place that knows whether a RowFilter was really produced:
    // the three skip paths inside `build_row_filter` all return None and are
    // invisible to every caller above.
    let row_filters_installed = Arc::new(AtomicU64::new(0));
    let row_filter_builder: Option<hudi_dep::storage::RowFilterBuilder> = pushed_filter
        .as_ref()
        .map(|pf| counting_row_filter_builder(pf, Arc::clone(&row_filters_installed)));

    // ENG-47483 — row-group pruning from footer statistics. Built from the same
    // decoded predicate as the row filter, but it is a different mechanism with
    // a different payoff: the filter saves decode, this saves IO. Both ride on
    // ReaderContext and share the base-read pushdown gate.
    let row_group_selector: Option<hudi_dep::storage::RowGroupSelector> =
        pushed_filter.as_ref().map(|pf| {
            let pf = pf.clone();
            Arc::new(move |md: &parquet::file::metadata::ParquetMetaData| pf.select_row_groups(md))
                as hudi_dep::storage::RowGroupSelector
        });

    // ── 7b. C-INFLIGHT-DELTA completion gate (proto fields 15–18) ─────
    // Build the Gate-3 inputs only when the planner enabled the gate
    // (apply_completion_gate == true, i.e. table version < 8 snapshot read).
    // Otherwise leave None so Gate 3 stays a no-op (v8+/incremental preserve
    // prior behavior). Mirrors how `instant_range` rides on ReaderContext.
    let completion_gate_inputs = if ffi_rc.apply_completion_gate {
        Some(Arc::new(CompletionGateInputs {
            completed_instants: ffi_rc.completed_instants.into_iter().collect(),
            inflight_instants: ffi_rc.inflight_instants.into_iter().collect(),
            archived_boundary: if ffi_rc.archived_boundary.is_empty() {
                None
            } else {
                Some(ffi_rc.archived_boundary)
            },
        }))
    } else {
        None
    };

    let core_reader_context = Arc::new(ReaderContext {
        table_path: ffi_rc.table_path,
        latest_commit_time: ffi_rc.latest_commit_time,
        base_file_format: ffi_rc.base_file_format,
        has_log_files: ffi_rc.has_log_files,
        has_bootstrap_base_file: ffi_rc.has_bootstrap_base_file,
        needs_bootstrap_merge: ffi_rc.needs_bootstrap_merge,
        should_merge_use_record_position: ffi_rc.should_merge_use_record_position,
        enable_logical_timestamp_field_repair: ffi_rc.enable_logical_timestamp_field_repair,
        iterator_mode: ffi_rc.iterator_mode,
        merge_mode: ffi_rc.merge_mode,
        merge_strategy_id: ffi_rc.merge_strategy_id,
        instant_range,
        record_context,
        schema_handler,
        table_config: ffi_rc.table_config,
        hoodie_reader_config: ffi_rc.hoodie_reader_config,
        row_filter_builder,
        row_group_selector,
        mor_pk_safe,
        key_predicate: None,
        completion_gate_inputs,
    });

    // Base-file data provider: consume the ABI handle into an owning provider.
    // Done here (after all fallible setup) so an early return above leaves the
    // handle for the caller to free; from this point the reader owns it and its
    // `destroy` runs on drop. `None` when no handle was supplied.
    // SAFETY: `base_file_provider_handle` is 0, or a handle from
    // `hudi_base_file_data_provider_new` not yet consumed or freed.
    let base_file_provider =
        unsafe { provider_abi::take_provider_from_handle(base_file_provider_handle) };

    // [ENG-47483] Capture the read-volume handle before `storage` moves into the
    // struct below.
    let read_volume = storage.read_volume();

    // ENG-42276 v4.3 — no per-fg tokio runtime; read() drives on
    // OBJECT_STORE_RUNTIME (see HoodieFileGroupReader doc comment).
    Ok(Box::new(HoodieFileGroupReader {
        reader_context: core_reader_context.clone(),
        storage,
        props,
        reader_parameters,
        input_split,
        partition_path_fields,
        pushed_filter,
        reader_memory_bytes: AtomicU64::new(0),
        base_file_provider,
        base_file_provider_stats: OnceLock::new(),
        row_filters_installed,
        stream_stats: OnceLock::new(),
        read_volume,
    }))
}

impl HoodieFileGroupReader {
    /// Construct a `HoodieFileGroupReader` directly (for testing without FFI).
    pub fn new(
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        props: HashMap<String, String>,
        reader_parameters: ReaderParameters,
        input_split: InputSplit,
        partition_path_fields: Option<Vec<String>>,
    ) -> std::result::Result<Self, String> {
        // ENG-42276 v4.3 — no per-fg tokio runtime (see struct doc).
        // [ENG-47483] capture before `storage` moves into the literal.
        let read_volume = storage.read_volume();
        Ok(Self {
            reader_context,
            storage,
            props,
            reader_parameters,
            input_split,
            partition_path_fields,
            pushed_filter: None,
            reader_memory_bytes: AtomicU64::new(0),
            base_file_provider: None,
            base_file_provider_stats: OnceLock::new(),
            // No pushed filter on this path, so no builder and nothing to
            // count; stays 0 for the reader's lifetime.
            row_filters_installed: Arc::new(AtomicU64::new(0)),
            stream_stats: OnceLock::new(),
            read_volume,
        })
    }

    /// Runs the full 3-phase merge and returns the resulting `RecordBatch` and its schema.
    pub fn read_record_batch(
        &self,
    ) -> std::result::Result<(arrow_array::RecordBatch, arrow_schema::SchemaRef), String> {
        log::debug!(
            "read_record_batch: partition={} base_file={:?} log_files={} \
             latest_instant_time={} ordering_fields={:?} merge_mode={}",
            self.input_split.partition_path,
            self.input_split.base_file_path,
            self.input_split.log_file_paths.len(),
            self.reader_context.latest_commit_time,
            self.reader_context.ordering_field_names(),
            self.reader_context.merge_mode.as_str(),
        );

        // ENG-42276 / ENG-42866 — the row_filter_builder + mor_pk_safe live
        // on reader_context (set at FFI entry, see new_file_group_reader_with_context).
        // The FG reader gate at make_base_file_batches and the parquet log
        // block decoder in `Decoder::decode_parquet_record_content` both
        // consult base_read_pushdown_is_safe() to decide whether to
        // install the filter. The post-merge filter below still runs
        // unconditionally for non-pushed-down predicates.
        // Phase 1: the provider handle stays in the cxx ABI so velox needs no
        // change, but OSS core has no provider seam, so nothing is injected.
        // Base files take core's own object-store read -- the same fallback
        // core takes for `None`. Provider metrics therefore read zero; see
        // `base_file_provider_stats`.
        let mut reader = CoreFileGroupReader::builder()
            .with_reader_context(self.reader_context.clone())
            .with_storage(self.storage.clone())
            .with_input_split(self.input_split.clone())
            .with_reader_parameters(self.reader_parameters.clone())
            .build()
            .map_err(|e| format!("Failed to build file group reader: {e}"))?;

        // C3 — tokio re-entry guard. `block_on` panics if called from within a
        // tokio runtime thread, and a panic unwinding across the FFI boundary is
        // UB. This entry point is meant to be called from a non-async C++ thread;
        // if a caller ever drives it from inside a tokio runtime, surface a loud
        // error here instead of letting `block_on` panic across FFI.
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(
                "read_record_batch must not be called from within a tokio runtime: \
                 it uses block_on on OBJECT_STORE_RUNTIME, which panics on re-entry \
                 (call it from a plain C++/native thread instead)"
                    .to_string(),
            );
        }

        // ENG-42276 v4.3 — drive the entire read on OBJECT_STORE_RUNTIME so
        // the hyper connection dispatcher (spawned the first time the cached
        // ObjectStore opens a connection) lives in the same runtime that
        // subsequent file-group reads will dispatch through. A per-file-group
        // current_thread runtime would bind the dispatcher to itself, drop it
        // on block_on return, and break the next file group with DispatchGone.
        let record_batch = OBJECT_STORE_RUNTIME
            .block_on(reader.read())
            .map_err(|e| format!("Failed to read file group: {e}"))?;

        log::debug!(
            "read_record_batch: merge complete, {} rows, {} cols",
            record_batch.num_rows(),
            record_batch.num_columns(),
        );

        // ── ENG-40156 — post-merge predicate evaluation ───────────────
        // Safe for all MOR shapes: the merge has already resolved log
        // updates so predicate values reflect the snapshot the caller
        // would see.
        let pre_rows = record_batch.num_rows();
        let record_batch = match &self.pushed_filter {
            None => record_batch,
            Some(filter) => {
                // Graceful fallback: if the evaluator hits a shape it doesn't
                // support (nested struct/list/map access, unusual column type,
                // type mismatch, etc.), log a warning and return the batch
                // unfiltered.  Velox's existing post-scan filter machinery
                // (Option A) will still evaluate the full original predicate
                // on the returned rows, so correctness is preserved — only the
                // perf benefit of early filtering is lost for that shape.
                match predicate::filter_batch(&record_batch, filter) {
                    Ok(filtered) => {
                        // debug, not info: fires once per batch (CLAUDE.md §10).
                        log_post_merge_filter!(pre_rows, filtered, filter);
                        filtered
                    }
                    Err(e) => {
                        log::warn!(
                            "[ENG-40156] filter eval failed; falling back to \
                             Velox post-scan filter: {e}; cols={:?}",
                            filter.columns()
                        );
                        record_batch
                    }
                }
            }
        };
        let schema = record_batch.schema();

        Ok((record_batch, schema))
    }

    /// Build a streaming `ArrowArrayStream` over the merged file-group
    /// output. The C++ side consumes it batch-by-batch via
    /// `ArrowArrayStream::get_next` until the release callback signals
    /// end-of-stream.
    ///
    /// ENG-42991 — true streaming output. Async work
    /// (base file decode + log scan + buffer population) still runs
    /// up-front via `block_on(reader.open())` on `OBJECT_STORE_RUNTIME`;
    /// from then on the iterator is pure synchronous in-memory work
    /// (HashMap lookups + Arrow slicing), which matches the
    /// `FFI_ArrowArrayStream` synchronous contract.
    ///
    /// ENG-40156 post-merge predicate evaluation is layered on top via
    /// [`PostMergePredicateFilter`] — it runs per chunk, preserving the
    /// fallback-to-Velox semantics for unsupported predicate shapes.
    ///
    /// Ownership: returns a leaked, Rust-heap-allocated `ArrowArrayStream`. The
    /// caller owns it and MUST free it via [`hudi_free_arrow_stream`] exactly
    /// once on every path (including exceptions). See the cxx bridge doc on
    /// `get_closable_iterator` for the full contract and the C++ RAII-guard
    /// recommendation.
    pub fn get_closable_iterator(&self) -> std::result::Result<*mut ffi::ArrowArrayStream, String> {
        log::debug!(
            "get_closable_iterator: partition={} base_file={:?} log_files={} \
             latest_instant_time={} ordering_fields={:?} merge_mode={}",
            self.input_split.partition_path,
            self.input_split.base_file_path,
            self.input_split.log_file_paths.len(),
            self.reader_context.latest_commit_time,
            self.reader_context.ordering_field_names(),
            self.reader_context.merge_mode.as_str(),
        );

        // Same construction as read_record_batch — see ENG-42276 / ENG-42866
        // doc comments there for why row_filter_builder + mor_pk_safe live on
        // the reader_context.
        // Phase 1: the provider handle stays in the cxx ABI so velox needs no
        // change, but OSS core has no provider seam, so nothing is injected.
        // Base files take core's own object-store read -- the same fallback
        // core takes for `None`. Provider metrics therefore read zero; see
        // `base_file_provider_stats`.
        let mut reader = CoreFileGroupReader::builder()
            .with_reader_context(self.reader_context.clone())
            .with_storage(self.storage.clone())
            .with_input_split(self.input_split.clone())
            .with_reader_parameters(self.reader_parameters.clone())
            .build()
            .map_err(|e| format!("Failed to build file group reader: {e}"))?;

        // C3 — tokio re-entry guard (preserved from read_record_batch). The
        // async setup below uses block_on on OBJECT_STORE_RUNTIME; block_on
        // panics if invoked from within a tokio runtime thread, and a panic
        // unwinding across the FFI boundary is UB. This entry point is meant to
        // be called from a non-async C++ thread; surface a loud error instead of
        // letting block_on panic across FFI.
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(
                "get_closable_iterator must not be called from within a tokio runtime: \
                 it uses block_on on OBJECT_STORE_RUNTIME, which panics on re-entry \
                 (call it from a plain C++/native thread instead)"
                    .to_string(),
            );
        }

        // Capture the iteration-phase stats sink BEFORE `reader` goes out of
        // scope at the end of this function. The iterator we return keeps
        // writing into it; `read_stats` would be unreachable (see
        // `stream_stats_handle`).
        let _ = self
            .stream_stats
            .set(hudi_dep::ffi_support::stream_stats_handle(&reader));

        // ENG-42276 v4.3 — async setup runs on OBJECT_STORE_RUNTIME so the
        // hyper dispatcher survives across file groups. The merge iteration
        // itself runs synchronously from FFI get_next calls, via
        // `BlockingMergeStream` — see that module for why the guard above is
        // what makes its `block_on` sound.
        let merge_stream = OBJECT_STORE_RUNTIME
            .block_on(reader.open())
            .map_err(|e| format!("Failed to open file group: {e}"))?;

        let merge_iter = BlockingMergeStream::new(merge_stream);

        // ENG-44436 — publish the reader's PEAK native footprint ONCE, here.
        // `open()` has fully populated the merge map during the log scan, and the
        // base file is streamed (not accumulated) through the merge from here on,
        // so the footprint only decreases as chunks drain — this post-`open()`
        // value is the true maximum for the read and the number the FFI consumer
        // reserves against. See `hudi_reader_memory_bytes`.
        self.reader_memory_bytes
            .store(merge_iter.current_in_memory_bytes(), Ordering::Relaxed);

        // Phase 1: no provider counters to capture. Nothing was injected into
        // core, so there is no live stats slot to fold `read_stats` into and
        // `base_file_provider_stats` stays unset -- its `None` arm then reports
        // all zeros, which is the honest answer for a read core served itself.

        // Wrap with the per-chunk ENG-40156 post-merge filter (no-op when
        // no predicate was pushed). Schema is unchanged by filtering.
        let filtered = PostMergePredicateFilter::new(merge_iter, self.pushed_filter.clone());

        Ok(create_raw_pointer_for_record_batch_reader(filtered))
    }

    /// See the cxx-bridge doc on `hudi_reader_memory_bytes`.
    fn hudi_reader_memory_bytes(&self) -> u64 {
        self.reader_memory_bytes.load(Ordering::Relaxed)
    }

    /// The base-file provider read counters captured during the last
    /// `get_closable_iterator` call. See the bridge declaration for the full
    /// contract. Returns all-zero when no provider was injected or the
    /// reader was never opened.
    fn base_file_provider_stats(&self) -> ffi::FfiBaseFileProviderStats {
        // Reads the live slot, so a consumer that calls this after draining the
        // stream observes the streaming drain counters too. Recover from a
        // poisoned lock into the default (all-zero) stats rather than panic across
        // the FFI boundary — these are diagnostic counters.
        let stats = match self.base_file_provider_stats.get() {
            Some(slot) => slot.lock().map(|g| g.clone()).unwrap_or_default(),
            None => BaseFileProviderStats::default(),
        };
        to_ffi_base_file_provider_stats(&stats)
    }
    /// Read one field out of the shared iteration-phase stats sink.
    ///
    /// 0 when no stream has been opened yet (the sink does not exist), which is
    /// indistinguishable from a genuine 0 -- acceptable because callers read
    /// these only after draining a stream they opened.
    fn stream_stat<T>(&self, pick: impl Fn(&hudi_dep::ffi_support::StreamReadStats) -> T) -> T
    where
        T: Default,
    {
        match self.stream_stats.get() {
            Some(handle) => match handle.lock() {
                Ok(guard) => pick(&guard),
                // A poisoned mutex means a chunk-emitting thread panicked. The
                // read is advisory instrumentation; report the default rather
                // than propagate a panic across the FFI boundary (UB).
                Err(_) => T::default(),
            },
            None => T::default(),
        }
    }

    /// See the cxx-bridge doc on `hudi_bytes_read`.
    fn hudi_bytes_read(&self) -> u64 {
        self.read_volume
            .bytes_read
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// See the cxx-bridge doc on `hudi_io_calls`.
    fn hudi_io_calls(&self) -> u64 {
        self.read_volume
            .io_calls
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// See the cxx-bridge doc on `hudi_rows_out`.
    fn hudi_rows_out(&self) -> u64 {
        self.read_volume
            .rows_out
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// See the cxx-bridge doc on `hudi_file_rows`.
    fn hudi_file_rows(&self) -> u64 {
        self.read_volume
            .file_rows
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// See the cxx-bridge doc on `hudi_row_group_selector_calls`.
    fn hudi_row_group_selector_calls(&self) -> u64 {
        self.read_volume
            .row_group_selector_calls
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// See the cxx-bridge doc on `hudi_row_groups_read`.
    fn hudi_row_groups_read(&self) -> u64 {
        self.read_volume
            .row_groups_read
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// See the cxx-bridge doc on `hudi_file_row_groups`.
    fn hudi_file_row_groups(&self) -> u64 {
        self.read_volume
            .file_row_groups
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// See the cxx-bridge doc on `hudi_final_merge_ms`.
    fn hudi_final_merge_ms(&self) -> u64 {
        // OSS measures µs; the ABI promises ms.
        self.stream_stat(|s| s.final_merge_us / 1000)
    }

    /// See the cxx-bridge doc on `hudi_output_build_ms`.
    fn hudi_output_build_ms(&self) -> u64 {
        // OSS measures µs; the ABI promises ms.
        self.stream_stat(|s| s.output_build_us / 1000)
    }

    /// See the cxx-bridge doc on `hudi_pushdown_decoded`.
    fn hudi_pushdown_decoded(&self) -> u64 {
        u64::from(self.pushed_filter.is_some())
    }

    /// See the cxx-bridge doc on `hudi_pushdown_row_filters_installed`.
    fn hudi_pushdown_row_filters_installed(&self) -> u64 {
        self.row_filters_installed.load(Ordering::Relaxed)
    }
}

/// Copy `BaseFileProviderStats` into the FFI shared struct field-for-field.
/// Free function (not a method) so the mapping is unit-testable without
/// building a full `HoodieFileGroupReader`.
fn to_ffi_base_file_provider_stats(stats: &BaseFileProviderStats) -> ffi::FfiBaseFileProviderStats {
    ffi::FfiBaseFileProviderStats {
        discover_wall_nanos: stats.discover_wall_nanos,
        connect_wall_nanos: stats.connect_wall_nanos,
        fetch_wall_nanos: stats.fetch_wall_nanos,
        batches_received: stats.batches_received,
        // The FFI/C-ABI field keeps the name `bytes_served` — it is frozen and its
        // consumers are already built against it — while hudi-core calls the same
        // number `bytes_materialized`, which is what it actually measures on the
        // served path (post-projection, allocated-capacity Arrow footprint, not a
        // transfer volume). Deliberate name difference; see the core field's docs.
        bytes_served: stats.bytes_materialized,
        rows_served: stats.rows_served,
        files_served: stats.files_served,
        storage_fallbacks: stats.storage_fallbacks,
        local_served: stats.local_served,
        remote_served: stats.remote_served,
    }
}

/// [ENG-40156 + ENG-42991] Per-chunk post-merge predicate evaluation
/// adapter.
///
/// Wraps a [`BlockingMergeStream`] and applies the pushed Substrait
/// predicate to each emitted `RecordBatch` before forwarding it on. When
/// no predicate was pushed (`filter == None`), it is a transparent
/// pass-through.
///
/// Failure handling mirrors the sibling `read_record_batch` filter path:
/// on filter-eval error the batch is forwarded UNFILTERED and Velox's
/// existing post-scan filter machinery (Option A) evaluates the original
/// predicate on the returned rows. Correctness is preserved; only the
/// hudi-rs-side perf benefit is forfeited for the offending shape.
struct PostMergePredicateFilter {
    inner: BlockingMergeStream,
    filter: Option<PushedFilter>,
}

impl PostMergePredicateFilter {
    fn new(inner: BlockingMergeStream, filter: Option<PushedFilter>) -> Self {
        Self { inner, filter }
    }
}

impl Iterator for PostMergePredicateFilter {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = match self.inner.next()? {
            Ok(b) => b,
            err @ Err(_) => return Some(err),
        };
        let Some(filter) = &self.filter else {
            return Some(Ok(batch));
        };
        let pre_rows = batch.num_rows();
        match predicate::filter_batch(&batch, filter) {
            Ok(filtered) => {
                // debug, not info: fires once per emitted chunk on the streaming
                // path (CLAUDE.md §10 — never info! per record/chunk).
                log_post_merge_filter!(pre_rows, filtered, filter);
                Some(Ok(filtered))
            }
            Err(e) => {
                // Deterministic for a given predicate + schema, so it would
                // otherwise repeat on every emitted chunk.
                warn_once!(
                    "[ENG-40156] filter eval failed; falling back to \
                     Velox post-scan filter: {e}; cols={:?}",
                    filter.columns()
                );
                Some(Ok(batch))
            }
        }
    }
}

impl RecordBatchReader for PostMergePredicateFilter {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }
}

/// Free an `ArrowArrayStream` returned by [`HoodieFileGroupReader::get_closable_iterator`].
///
/// # Safety
/// `ptr` must have been returned by `get_closable_iterator` and must not be
/// used after this call.
unsafe fn hudi_free_arrow_stream(ptr: *mut ffi::ArrowArrayStream) {
    unsafe { free_arrow_stream(ptr) };
}

/// Convert an Avro schema JSON string to an Arrow Schema.
///
/// Delegates to the core crate's single conversion point so FFI-passed
/// schemas agree with every other Avro-derived schema in the reader.
fn avro_json_to_arrow_schema(avro_json: &str) -> std::result::Result<arrow_schema::Schema, String> {
    hudi_dep::schema::resolver::avro_json_to_arrow_schema(avro_json)
        .map_err(|e| format!("Failed to convert Avro→Arrow: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The FFI mapping copies every `BaseFileProviderStats` field onto the FFI
    /// names. Values are all distinct so a transposed pair (e.g. local/remote,
    /// served/fallbacks) fails instead of silently passing.
    ///
    /// The one field whose name differs is `bytes_materialized` → `bytes_served`:
    /// the C ABI name is frozen, while the core name says what the number really
    /// measures. This test is what keeps that deliberate mismatch wired up.
    #[test]
    fn base_file_provider_stats_maps_all_fields() {
        let core = BaseFileProviderStats {
            files_served: 1,
            storage_fallbacks: 2,
            local_served: 3,
            remote_served: 4,
            rows_served: 66,
            bytes_materialized: 555,
            batches_received: 7,
            discover_wall_nanos: 11,
            connect_wall_nanos: 22,
            fetch_wall_nanos: 33,
        };
        let mapped = to_ffi_base_file_provider_stats(&core);
        assert_eq!(mapped.discover_wall_nanos, 11);
        assert_eq!(mapped.connect_wall_nanos, 22);
        assert_eq!(mapped.fetch_wall_nanos, 33);
        assert_eq!(mapped.batches_received, 7);
        assert_eq!(
            mapped.bytes_served, 555,
            "core `bytes_materialized` maps onto the frozen ABI name `bytes_served`"
        );
        assert_eq!(mapped.rows_served, 66);
        assert_eq!(mapped.files_served, 1);
        assert_eq!(mapped.storage_fallbacks, 2);
        assert_eq!(mapped.local_served, 3);
        assert_eq!(mapped.remote_served, 4);
    }

    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
    use hudi_dep::ffi_support::MAX_INSTANT_TIME;
    use hudi_dep::ffi_support::OBJECT_STORE_RUNTIME;
    use hudi_dep::table::builder::OptionResolver;
    use hudi_test::QuickstartTripsTable;

    /// Build a reader over the sf partition of the V9Mor8I4UCommitTime fixture
    /// (base + one avro update log; merged result = Alice-V2/31, Bob/25).
    fn build_test_reader() -> HoodieFileGroupReader {
        build_test_reader_with_selector(None)
    }

    fn build_test_reader_with_selector(
        selector: Option<hudi_dep::storage::RowGroupSelector>,
    ) -> HoodieFileGroupReader {
        build_test_reader_gated(selector, false)
    }

    fn build_test_reader_gated(
        selector: Option<hudi_dep::storage::RowGroupSelector>,
        mor_pk_safe: bool,
    ) -> HoodieFileGroupReader {
        let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
        let empty_opts: Vec<(&str, &str)> = vec![];
        let mut resolver = OptionResolver::new_with_options(&table_path, empty_opts);
        OBJECT_STORE_RUNTIME
            .block_on(resolver.resolve_options())
            .expect("resolve table options");
        let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options));
        let storage =
            Storage::new(Arc::new(resolver.storage_options), hudi_configs).expect("storage");

        let mut reader_context = ReaderContext::empty();
        reader_context.latest_commit_time = MAX_INSTANT_TIME.to_string();
        reader_context.merge_mode = "COMMIT_TIME_ORDERING".to_string();
        reader_context.table_config.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        reader_context.rebuild_record_context("city=sf".to_string());
        reader_context.has_log_files = true;
        reader_context.row_group_selector = selector;
        reader_context.mor_pk_safe = mor_pk_safe;

        let input_split = InputSplit::new(
            Some(
                "city=sf/fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet"
                    .to_string(),
            ),
            None,
            vec![
                "city=sf/.fee86b18-67b1-4479-b517-075683aeb2d1-0_20260408053037787.log.1_0-27-73"
                    .to_string(),
            ],
            "city=sf".to_string(),
        );

        HoodieFileGroupReader::new(
            Arc::new(reader_context),
            storage,
            HashMap::new(),
            ReaderParameters::default(),
            input_split,
            None,
        )
        .expect("reader")
    }

    /// The documented consumer flow (cxx bridge doc on `get_closable_iterator`):
    /// the C++ side imports the stream as owner (which moves the stream content
    /// out, nulling its release callback) and STILL calls
    /// `hudi_free_arrow_stream` to reclaim the heap allocation. Both steps must
    /// be safe in that order, and the data must round-trip intact.
    #[test]
    fn test_closable_iterator_import_then_free() {
        let reader = build_test_reader();
        let ptr = reader.get_closable_iterator().expect("stream");
        assert!(!ptr.is_null());

        let imported =
            unsafe { ArrowArrayStreamReader::from_raw(ptr as *mut FFI_ArrowArrayStream) }
                .expect("import stream");
        let batches: Vec<arrow_array::RecordBatch> = imported
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("read all batches");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "sf merge yields Alice-V2 and Bob");

        // Validate actual data, not just counts: names must reflect the merged
        // state (id=1 updated by the log block, id=2 untouched).
        let mut names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col = b.column_by_name("name").expect("name column");
                let arr = col.as_string::<i32>();
                (0..arr.len())
                    .map(|i| arr.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice-V2".to_string(), "Bob".to_string()]);

        // The import moved the stream out and left an empty struct (release =
        // None); freeing the allocation afterwards must not double-release.
        unsafe { hudi_free_arrow_stream(ptr) };
    }

    /// Freeing an unconsumed stream must run the Arrow release callback and
    /// reclaim the allocation without a crash (the exception-path contract).
    #[test]
    fn test_closable_iterator_free_unconsumed() {
        let reader = build_test_reader();
        let ptr = reader.get_closable_iterator().expect("stream");
        assert!(!ptr.is_null());
        unsafe { hudi_free_arrow_stream(ptr) };
    }

    /// `hudi_free_arrow_stream(nullptr)` is documented as a no-op.
    #[test]
    fn test_free_null_stream_is_noop() {
        unsafe { hudi_free_arrow_stream(std::ptr::null_mut()) };
    }

    /// ENG-44436 — `hudi_reader_memory_bytes` publishes the reader's PEAK native
    /// footprint: 0 before the file group is opened, a plausible non-zero value
    /// once `get_closable_iterator` has opened it (the merge map is fully
    /// populated during the log scan — the true maximum, since the base file is
    /// streamed, not accumulated), and stable at that peak thereafter (published
    /// once, no per-chunk refresh). This is the value velox reserves against its
    /// `MemoryPool`.
    #[test]
    fn test_reader_memory_bytes_publishes_peak_footprint() {
        let reader = build_test_reader();
        assert_eq!(
            reader.hudi_reader_memory_bytes(),
            0,
            "footprint is 0 before the file group is opened"
        );

        let ptr = reader.get_closable_iterator().expect("stream");
        assert!(!ptr.is_null());
        let peak = reader.hudi_reader_memory_bytes();
        assert!(
            peak > 0,
            "merge-map footprint must be a plausible non-zero value right after \
             open() (the post-open peak), got {peak}"
        );

        // Draining the stream must not change the published peak (single publish,
        // no downward refresh — velox holds the high-water reservation for the
        // whole read and releases it wholesale at EOF).
        let imported =
            unsafe { ArrowArrayStreamReader::from_raw(ptr as *mut FFI_ArrowArrayStream) }
                .expect("import stream");
        let batches: Vec<arrow_array::RecordBatch> = imported
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("read all batches");
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            2,
            "sf merge yields Alice-V2 and Bob"
        );
        assert_eq!(
            reader.hudi_reader_memory_bytes(),
            peak,
            "the published peak is stable across the read (single publish at open)"
        );

        unsafe { hudi_free_arrow_stream(ptr) };
    }

    // ─── ENG-40156 pushdown counters ────────────────────────────────────────
    //
    // These pin the counter's defining property: it reports what was INSTALLED,
    // never what was intended. A future "simplification" of
    // `counting_row_filter_builder` to `pushed_filter.is_some()` compiles, keeps
    // every other test green, and is caught only by
    // `counting_builder_does_not_count_when_row_filter_is_skipped`.

    /// A parquet `SchemaDescriptor` with INT64 columns of the given names.
    /// Physical type is irrelevant — `build_row_filter` resolves columns by
    /// name. Mirrors the helper in `predicate.rs`'s tests; duplicated rather
    /// than shared because it is test scaffolding, not a shared rule.
    fn pushdown_parquet_schema(field_names: &[&str]) -> parquet::schema::types::SchemaDescriptor {
        use parquet::basic::Type as ParquetPhysicalType;
        use parquet::schema::types::Type as ParquetType;
        let fields: Vec<Arc<ParquetType>> = field_names
            .iter()
            .map(|n| {
                Arc::new(
                    ParquetType::primitive_type_builder(n, ParquetPhysicalType::INT64)
                        .build()
                        .unwrap(),
                )
            })
            .collect();
        let root = ParquetType::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();
        parquet::schema::types::SchemaDescriptor::new(Arc::new(root))
    }

    /// Serialized `ExtendedExpression` for `<names[0]> > 100` — a prunable
    /// comparison, so it clears the ENG-42276 selectivity gate and the only
    /// remaining reason to skip is column resolution.
    fn pushdown_gt_filter_bytes(names: &[&str]) -> Vec<u8> {
        use prost::Message;
        use substrait::proto::{
            Expression, ExtendedExpression, FunctionArgument, NamedStruct,
            expression::{
                FieldReference, Literal, ReferenceSegment, ScalarFunction, field_reference,
                literal::LiteralType, reference_segment,
            },
            expression_reference,
            extensions::{
                SimpleExtensionDeclaration, SimpleExtensionUri, simple_extension_declaration,
            },
            function_argument, r#type,
        };
        let col = Expression {
            rex_type: Some(substrait::proto::expression::RexType::Selection(Box::new(
                FieldReference {
                    reference_type: Some(field_reference::ReferenceType::DirectReference(
                        ReferenceSegment {
                            reference_type: Some(reference_segment::ReferenceType::StructField(
                                Box::new(reference_segment::StructField {
                                    field: 0,
                                    child: None,
                                }),
                            )),
                        },
                    )),
                    root_type: None,
                },
            ))),
        };
        let lit = Expression {
            rex_type: Some(substrait::proto::expression::RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I64(100)),
            })),
        };
        let gt = Expression {
            rex_type: Some(substrait::proto::expression::RexType::ScalarFunction(
                ScalarFunction {
                    function_reference: 7,
                    arguments: vec![col, lit]
                        .into_iter()
                        .map(|e| FunctionArgument {
                            arg_type: Some(function_argument::ArgType::Value(e)),
                        })
                        .collect(),
                    output_type: None,
                    ..Default::default()
                },
            )),
        };
        let ext = ExtendedExpression {
            version: None,
            extension_uris: vec![SimpleExtensionUri {
                extension_uri_anchor: 1,
                uri: "/functions_comparison.yaml".to_string(),
            }],
            extensions: vec![SimpleExtensionDeclaration {
                mapping_type: Some(
                    simple_extension_declaration::MappingType::ExtensionFunction(
                        simple_extension_declaration::ExtensionFunction {
                            extension_uri_reference: 1,
                            function_anchor: 7,
                            name: "gt:any_any".to_string(),
                        },
                    ),
                ),
            }],
            referred_expr: vec![substrait::proto::ExpressionReference {
                output_names: vec!["filter".to_string()],
                expr_type: Some(expression_reference::ExprType::Expression(gt)),
            }],
            base_schema: Some(NamedStruct {
                names: names.iter().map(|s| s.to_string()).collect(),
                r#struct: Some(r#type::Struct::default()),
            }),
            advanced_extensions: None,
            expected_type_urls: vec![],
        };
        let mut buf = Vec::new();
        ext.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn pushdown_decoded_is_zero_when_no_filter_was_pushed() {
        let reader = build_test_reader();
        assert_eq!(
            reader.hudi_pushdown_decoded(),
            0,
            "no substrait bytes were supplied, so nothing decoded"
        );
        assert_eq!(
            reader.hudi_pushdown_row_filters_installed(),
            0,
            "no builder exists, so no RowFilter can be installed"
        );
    }

    #[test]
    fn counting_builder_counts_each_installed_row_filter() {
        let bytes = pushdown_gt_filter_bytes(&["cs_net_paid", "cs_quantity"]);
        let pf = PushedFilter::decode(&bytes)
            .expect("well-formed ExtendedExpression")
            .expect("gt is a known function, so it must decode");

        let counter = Arc::new(AtomicU64::new(0));
        let builder = counting_row_filter_builder(&pf, Arc::clone(&counter));

        // Column IS present in the parquet schema -> build_row_filter returns Some.
        let schema = pushdown_parquet_schema(&["cs_net_paid", "cs_quantity"]);
        let projected = arrow_schema::Schema::empty();

        assert!(
            builder(&schema, &projected).is_some(),
            "a resolvable prunable comparison must install a RowFilter"
        );
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        // Invoked once per parquet file, so a second file counts again.
        assert!(builder(&schema, &projected).is_some());
        assert_eq!(
            counter.load(Ordering::Relaxed),
            2,
            "counter is per installed RowFilter, not per reader"
        );
    }

    #[test]
    fn counting_builder_does_not_count_when_row_filter_is_skipped() {
        // Predicate references `cs_net_paid`; the parquet file does not have it,
        // so build_row_filter takes its column-not-found skip path and returns
        // None. The predicate still DECODED fine -- which is exactly the state a
        // counter derived from `pushed_filter.is_some()` would misreport as
        // pushdown having happened.
        let bytes = pushdown_gt_filter_bytes(&["cs_net_paid"]);
        let pf = PushedFilter::decode(&bytes)
            .expect("well-formed ExtendedExpression")
            .expect("gt is a known function, so it must decode");

        let counter = Arc::new(AtomicU64::new(0));
        let builder = counting_row_filter_builder(&pf, Arc::clone(&counter));

        let schema = pushdown_parquet_schema(&["some_other_column"]);
        let projected = arrow_schema::Schema::empty();

        assert!(
            builder(&schema, &projected).is_none(),
            "column absent from the parquet schema must skip the RowFilter"
        );
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "a skipped RowFilter must NOT be counted as installed"
        );
    }

    #[test]
    fn stream_stats_handle_survives_the_core_reader_drop() {
        // The plumbing risk in (a) is lifetime, not arithmetic: the core
        // FileGroupReader is a LOCAL in get_closable_iterator and is dropped the
        // moment it returns, so the only reason these numbers are observable at
        // all is that the Arc sink was cloned out before that drop. If someone
        // later moves the `stream_stats.set(...)` after the drop, or captures
        // read_stats instead, the getters silently return 0 forever -- a
        // permanently-quiet metric, which is the exact failure mode the whole
        // pushdown-counter exercise exists to eliminate.
        let reader = build_test_reader();
        assert!(
            reader.stream_stats.get().is_none(),
            "no stream opened yet, so no sink should be captured"
        );
        assert_eq!(reader.hudi_final_merge_ms(), 0, "no sink -> 0");
        assert_eq!(reader.hudi_output_build_ms(), 0, "no sink -> 0");

        let ptr = reader.get_closable_iterator().expect("stream");
        assert!(!ptr.is_null());
        // The core reader is already dropped here -- get_closable_iterator has
        // returned. The sink must still be reachable.
        assert!(
            reader.stream_stats.get().is_some(),
            "the stats sink must outlive the core reader that created it"
        );

        let imported =
            unsafe { ArrowArrayStreamReader::from_raw(ptr as *mut FFI_ArrowArrayStream) }
                .expect("import stream");
        let batches: Vec<arrow_array::RecordBatch> = imported
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("read all batches");
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            2,
            "sf merge yields Alice-V2 and Bob"
        );

        // Deliberately NOT asserting a positive duration: this fixture is a
        // two-row MOR merge that completes well inside a millisecond, so a
        // `> 0` assertion would be flaky on fast hardware. What is asserted is
        // that the read path reached the sink at all -- the Arc is live and the
        // lock is not poisoned -- which is the claim that can actually break.
        let merge_ms = reader.hudi_final_merge_ms();
        let build_ms = reader.hudi_output_build_ms();
        assert!(
            merge_ms < 60_000 && build_ms < 60_000,
            "timings must be readable and sane, got merge={merge_ms}ms build={build_ms}ms"
        );

        unsafe { hudi_free_arrow_stream(ptr) };
    }

    #[test]
    fn read_volume_counts_bytes_io_and_rows_over_a_real_read() {
        // ENG-47483. Pins the counters against an actual parquet read rather than a
        // hand-built fixture, because the thing that can break here is the wiring —
        // a CountingReader that wraps the wrong reader, or a Storage handle captured
        // from a different Storage than the one that does the reading — and neither
        // is visible without real IO.
        let reader = build_test_reader();
        assert_eq!(reader.hudi_bytes_read(), 0, "nothing read before open");
        assert_eq!(reader.hudi_io_calls(), 0);
        assert_eq!(reader.hudi_rows_out(), 0);

        let ptr = reader.get_closable_iterator().expect("stream");
        let imported =
            unsafe { ArrowArrayStreamReader::from_raw(ptr as *mut FFI_ArrowArrayStream) }
                .expect("import stream");
        let batches: Vec<arrow_array::RecordBatch> = imported
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("read all batches");
        let merged: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(merged, 2, "sf merge yields Alice-V2 and Bob");

        let bytes = reader.hudi_bytes_read();
        let calls = reader.hudi_io_calls();
        let rows_out = reader.hudi_rows_out();
        let file_rows = reader.hudi_file_rows();

        // Bytes and calls must be strictly positive: the base file was read, so a
        // zero here means the CountingReader is not in the path at all — the exact
        // wiring failure this test exists for.
        assert!(bytes > 0, "base file was read, so bytes_read must be > 0");
        assert!(calls > 0, "reading implies at least one storage round trip");

        // file_rows comes from footer metadata and must cover what the base file
        // holds. rows_out is the BASE row count, not the merged count: the merge and
        // the post-merge filter run above this layer, so rows_out >= merged.
        assert!(file_rows > 0, "footer reports a row count");
        assert!(
            rows_out >= merged as u64,
            "rows_out ({rows_out}) counts base rows yielded, so it cannot be below \
             the merged output ({merged})"
        );
        assert!(
            rows_out <= file_rows,
            "rows_out ({rows_out}) cannot exceed the rows the file contains ({file_rows})"
        );

        // Sanity on magnitude: a tiny fixture must not report an absurd byte count,
        // which is what a double-count (wrapping both get_bytes and get_byte_ranges
        // over the same data) would look like.
        assert!(
            bytes < 100 * 1024 * 1024,
            "bytes_read ({bytes}) is implausible for a 2-row fixture — likely double counted"
        );

        unsafe { hudi_free_arrow_stream(ptr) };
    }
}
