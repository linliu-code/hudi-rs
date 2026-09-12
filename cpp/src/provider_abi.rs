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

//! C ABI for injecting a base-file data provider, and the Rust adapter that
//! presents it as a [`BaseFileDataProvider`].
//!
//! hudi-core defines the source-agnostic [`BaseFileDataProvider`] trait but
//! implements no source. The composition root that constructs a concrete
//! provider lives **outside** hudi-rs, and hudi-rs makes no assumption about
//! what backs it. That provider is handed to hudi-rs across a plain
//! C ABI — a small vtable of function pointers — so `libhudi.so` never links or
//! names any provider implementation.
//!
//! Flow:
//! 1. The provider's cdylib produces a [`HudiBaseFileDataProviderVTable`] + an
//!    opaque `ctx`.
//! 2. The composition root calls [`hudi_base_file_data_provider_new`] to wrap them
//!    into an owning handle and stashes the handle on
//!    `FfiReaderContext.base_file_provider_handle`.
//! 3. `new_file_group_reader_with_context` consumes the handle into a
//!    [`CApiBaseFileDataProvider`] and injects it via `with_base_file_provider`.
//!
//! The matching C declarations are shipped in `cpp/include/hudi_base_file_data_provider.h`;
//! the two must stay layout-identical. The [`HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION`]
//! marker guards against silent layout drift once the two sides are built apart.
//!
//! **Async boundary.** The trait method is `async`, but the vtable call is a
//! synchronous C function that itself blocks (a provider typically drives
//! discovery and data-fetch calls behind its own runtime). hudi-core drives readers on the
//! multi-thread `OBJECT_STORE_RUNTIME`, and calling `block_on` from a runtime
//! worker thread panics — so the adapter runs the C call inside `spawn_blocking`,
//! moving it onto a blocking-pool thread that carries no runtime context.

use std::os::raw::{c_int, c_void};

use arrow_array::RecordBatchReader;
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_schema::ffi::FFI_ArrowSchema;
use async_trait::async_trait;

use hudi_dep::ffi_support::{
    BaseFileDataProvider, BaseFileDataProviderRef, BaseFileDataRequest, BaseFileProviderStats,
};

/// Layout/behaviour version of the base-file provider C ABI.
///
/// Bump on any change to the vtable, request, result, or stats struct layout.
/// [`CApiBaseFileDataProvider::from_raw`] rejects a vtable whose `abi_version` does not
/// match, degrading to "no provider" rather than reading an unknown layout.
pub const HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION: u32 = 1;

/// Return code: the provider served the base file (a valid stream is in `stream`).
pub const HUDI_PROVIDER_OUTCOME_SERVED: c_int = 1;
/// Return code: the provider did not serve the base file (read from object storage).
pub const HUDI_PROVIDER_OUTCOME_NOT_SERVED: c_int = 0;

/// A borrowed UTF-8 string slice across the ABI: pointer + byte length, no NUL.
///
/// The pointer is valid only for the duration of the `try_base_file` call.
#[repr(C)]
pub struct HudiStrSlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl HudiStrSlice {
    fn from_str(s: &str) -> Self {
        Self {
            ptr: s.as_ptr(),
            len: s.len(),
        }
    }
}

/// Everything the provider needs to try one base file. Mirrors
/// [`BaseFileDataRequest`] field for field; all pointers are borrowed for the
/// duration of the call only.
#[repr(C)]
pub struct HudiBaseFileDataRequest {
    /// Absolute storage URI of the base file (the identity a provider keys by).
    pub file_uri: HudiStrSlice,
    /// Projected ("intersection") schema the read wants back, as an Arrow C
    /// schema. Never null.
    pub projected_schema: *const FFI_ArrowSchema,
    /// Whether a pushed predicate may be applied to this file. When false, the
    /// provider must serve unfiltered and let the post-merge filter apply it.
    ///
    /// PER FILE, not per split, and not a table property — it is exactly the
    /// decision hudi-rs's own parquet `RowFilter` pushdown got for this same file,
    /// and TWO independent gates must both pass:
    ///
    /// 1. the merge-safety gate — no log files on the split, or a
    ///    primary-key-safe predicate (ENG-47506; was a table-type check, so this
    ///    is now true for a MOR slice with no log files where it previously was
    ///    not); and
    /// 2. the repair gate — this file's footer does not label a predicate column
    ///    in a way the apache/hudi#18132 logical-type repair reinterprets on read.
    ///
    /// Gate 2 is decided from the file's own footer, so the SAME read can hand
    /// true for one base file and false for the next. A provider that caches the
    /// answer across the files of a split is wrong, and drops rows that match.
    ///
    /// This doc must stay in step with
    /// `cpp/include/hudi_base_file_data_provider.h` and with
    /// [`BaseFileDataRequest::can_push_predicate`] — three mirrors of one
    /// contract, and this one was the last to be updated.
    pub can_push_predicate: bool,
    /// Partition path of the split (e.g. `year=2024/month=01`).
    pub partition_path: HudiStrSlice,
    /// Partition field names, in order; `partition_fields_len` entries.
    pub partition_fields: *const HudiStrSlice,
    pub partition_fields_len: usize,
    /// Table data schema (Arrow C schema), or null if unavailable.
    pub data_schema: *const FFI_ArrowSchema,
}

/// Client-side counters for one base-file provider attempt. Mirrors hudi-core's
/// source-agnostic [`BaseFileProviderStats`] field for field (all `u64`,
/// FFI-safe).
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct HudiBaseFileProviderStats {
    pub files_served: u64,
    pub storage_fallbacks: u64,
    pub local_served: u64,
    pub remote_served: u64,
    pub rows_served: u64,
    pub bytes_served: u64,
    pub batches_received: u64,
    pub discover_wall_nanos: u64,
    pub connect_wall_nanos: u64,
    pub fetch_wall_nanos: u64,
}

impl From<HudiBaseFileProviderStats> for BaseFileProviderStats {
    fn from(s: HudiBaseFileProviderStats) -> Self {
        BaseFileProviderStats {
            files_served: s.files_served,
            storage_fallbacks: s.storage_fallbacks,
            local_served: s.local_served,
            remote_served: s.remote_served,
            rows_served: s.rows_served,
            // Wire field keeps the name `bytes_served` (it is part of the frozen
            // C ABI and its consumers); hudi-core's field is `bytes_materialized`
            // because that is what the number actually measures on the served
            // path. Deliberate name difference, not an oversight.
            bytes_materialized: s.bytes_served,
            batches_received: s.batches_received,
            discover_wall_nanos: s.discover_wall_nanos,
            connect_wall_nanos: s.connect_wall_nanos,
            fetch_wall_nanos: s.fetch_wall_nanos,
        }
    }
}

/// Out-parameter the provider fills.
///
/// ⚠️ **The FUNCTION'S RETURN VALUE is authoritative, not this `outcome` field.**
/// hudi-rs branches on what `try_base_file` returns and treats `out->outcome` as
/// advisory only; the field exists because the ABI is frozen at version 1.
///
/// That distinction bites in one specific way, which is why it is stated here
/// rather than left implicit. `HUDI_PROVIDER_OUTCOME_NOT_SERVED` is `0`, and
/// "return 0 on success" is the dominant C idiom — so a provider that fills
/// `out->stream`, sets `out->outcome = HUDI_PROVIDER_OUTCOME_SERVED`, and then
/// returns `0` is DECLINED on every file, silently, while its own counters say
/// it served them. Return `HUDI_PROVIDER_OUTCOME_SERVED` (`1`).
///
/// hudi-rs logs loudly when the two disagree, so the mistake is diagnosable from
/// one read's logs rather than from a fallback rate nobody can explain.
///
/// When served, `stream` holds a valid Arrow C stream of the projected batches;
/// otherwise `stream` is left empty.
/// `stats` is filled either way (a file that was not served still reports its
/// timings).
#[repr(C)]
pub struct HudiBaseFileDataResult {
    pub outcome: c_int,
    pub stream: FFI_ArrowArrayStream,
    pub stats: HudiBaseFileProviderStats,
}

impl HudiBaseFileDataResult {
    fn empty() -> Self {
        Self {
            outcome: HUDI_PROVIDER_OUTCOME_NOT_SERVED,
            stream: FFI_ArrowArrayStream::empty(),
            stats: HudiBaseFileProviderStats::default(),
        }
    }
}

/// The provider's function-pointer table. Copied by value into the adapter, so the
/// storage backing this struct need not outlive [`hudi_base_file_data_provider_new`].
///
/// **Concurrency contract (must hold for the C implementor):** `try_base_file`
/// MAY be called concurrently from several threads on the same `ctx`. The
/// implementation must be `Send + Sync`-equivalent.
///
/// **Lifetime contract (guaranteed to the C implementor):** `destroy` is called
/// exactly once, after the last `try_base_file` has returned AND after every
/// `ArrowArrayStream` this provider served has been fully drained or released. So
/// a served stream's `get_next`/`release` callbacks may point into `ctx`.
///
/// "after the last `try_base_file` has returned" alone would NOT be enough, and
/// used to be all this said. The reader that calls `try_base_file` is a local of
/// [`HoodieFileGroupReader::get_closable_iterator`](crate::HoodieFileGroupReader)
/// and dies when that function returns, while the stream it produced is handed to
/// C++ and drained afterwards — two objects with no ordering between their frees.
/// The guarantee is now structural rather than a rule the caller must remember:
/// `hudi-core`'s `served_batch_stream` moves a strong reference to the provider
/// onto the task that owns the served reader, so the provider cannot outlive
/// its own streams by construction. Pinned by
/// `a_served_stream_keeps_its_provider_alive_after_the_reader_is_dropped`.
///
/// ⚠️ **`destroy` is consequently NOT guaranteed to run on the C++ thread that
/// frees the reader handle, and is not ordered against that free.** Whichever of
/// {the reader handle, the last served stream} is released second drops the last
/// reference, and when that is the stream's producer task, `destroy` runs on a
/// tokio BLOCKING-POOL thread hudi-rs owns, at a moment the caller does not
/// choose. So `destroy` must be thread-agnostic and self-sufficient: it may not
/// assume a thread-local the caller set up, and for a JNI-backed `ctx` it must
/// attach to the JVM itself rather than assume an attached thread. This was not
/// true before the reference above was added — `destroy` then always ran on the
/// freeing thread — so it is a real change for an existing implementor.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HudiBaseFileDataProviderVTable {
    /// Must equal [`HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION`].
    pub abi_version: u32,
    /// Try to serve one base file. Returns `HUDI_PROVIDER_OUTCOME_SERVED` or
    /// `HUDI_PROVIDER_OUTCOME_NOT_SERVED`. Any internal failure must be reported
    /// as not-served, never as an error that fails the read.
    pub try_base_file: extern "C" fn(
        ctx: *mut c_void,
        req: *const HudiBaseFileDataRequest,
        out: *mut HudiBaseFileDataResult,
    ) -> c_int,
    /// Release `ctx`. Called exactly once, once the owning reader AND every
    /// stream that reader served have been released — see the lifetime contract
    /// on this struct.
    pub destroy: extern "C" fn(ctx: *mut c_void),
}

/// Owns the provider `ctx` and forwards trait calls through the vtable.
///
/// Holds the vtable by value (it is `Copy`) and the opaque `ctx`. The `ctx` is
/// released via `destroy` exactly once, on drop.
pub struct CApiBaseFileDataProvider {
    vtable: HudiBaseFileDataProviderVTable,
    ctx: *mut c_void,
}

// SAFETY: the ABI concurrency contract (documented on `HudiBaseFileDataProviderVTable`)
// requires the C implementation to tolerate concurrent `try_base_file` calls on
// the same `ctx`; the raw pointer is only ever passed back to the vtable, never
// dereferenced on the Rust side. `destroy` runs once, on drop, after all calls.
unsafe impl Send for CApiBaseFileDataProvider {}
unsafe impl Sync for CApiBaseFileDataProvider {}

impl CApiBaseFileDataProvider {
    /// Build an adapter from a raw vtable pointer and an owned `ctx`.
    ///
    /// Returns `None` (and does **not** take ownership of `ctx`) if the vtable is
    /// null or its `abi_version` is incompatible — the caller then falls back to
    /// "no provider". On `Some`, ownership of `ctx` transfers to the returned value,
    /// which releases it via `destroy` on drop.
    ///
    /// # Safety
    /// `vtable` must point to a valid [`HudiBaseFileDataProviderVTable`] for the duration
    /// of this call, and `ctx` must be the matching context the vtable expects.
    unsafe fn from_raw(
        vtable: *const HudiBaseFileDataProviderVTable,
        ctx: *mut c_void,
    ) -> Option<Self> {
        if vtable.is_null() {
            log::error!("[hudi-provider-abi] null vtable; falling back to no provider");
            return None;
        }
        // Copy the vtable by value; storage behind `vtable` need not outlive us.
        let vtable = unsafe { *vtable };
        if vtable.abi_version != HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION {
            // Do not touch `ctx` or call `destroy` — a mismatched vtable's
            // `destroy` cannot be trusted. Degrade to no provider and leak `ctx`
            // (rare, and safer than calling an unknown-layout function pointer).
            log::error!(
                "[hudi-provider-abi] vtable abi_version {} != expected {}; falling back to no provider",
                vtable.abi_version,
                HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION
            );
            return None;
        }
        Some(Self { vtable, ctx })
    }

    /// Adapt a served result's Arrow C stream into a **lazy**
    /// [`RecordBatchReader`], or `None` if the stream cannot be imported (which
    /// the caller maps to "not served" so the read falls back to object storage).
    ///
    /// The reader is pulled downstream inside the merge loop, one batch at a
    /// time, so the served file is never fully materialized here. This is the
    /// R3 memory fix for the provider path.
    ///
    /// **Fallback tradeoff (accepted SLA change).** Import failure *before*
    /// streaming starts still degrades to a storage read (returns `None`). An
    /// error surfaced *mid-stream* cannot: the merge loop is already consuming, so
    /// it propagates as a read error rather than silently re-reading the whole
    /// file from storage. This is a deliberate resilience/availability change from
    /// the old materialize-then-classify path (where any per-batch error fell back
    /// transparently): a base file provider dying mid-stream now fails the query
    /// instead of degrading. It is **safe** (no partial or duplicated data — the
    /// merge never restarts), and the peak-memory win is the point. A provider
    /// must therefore only report `SERVED` when confident it can deliver the whole
    /// stream (source-availability failures classified as not-served *before*
    /// returning), and operators should monitor the provider's mid-stream error
    /// rate since it is now query-visible.
    fn served_reader(
        result: &mut HudiBaseFileDataResult,
    ) -> Option<Box<dyn RecordBatchReader + Send>> {
        // SAFETY: on a served outcome the provider filled `stream` with a valid Arrow C stream;
        // `from_raw` moves it out and marks the source released, so `result`
        // dropping afterwards is a no-op (no double free).
        match unsafe { ArrowArrayStreamReader::from_raw(&mut result.stream) } {
            Ok(reader) => Some(Box::new(reader)),
            Err(e) => {
                log::warn!(
                    "[hudi-provider-abi] served stream import failed; falling back to storage read: {e}"
                );
                None
            }
        }
    }
}

impl Drop for CApiBaseFileDataProvider {
    fn drop(&mut self) {
        (self.vtable.destroy)(self.ctx);
    }
}

#[async_trait]
impl BaseFileDataProvider for CApiBaseFileDataProvider {
    async fn try_base_file(
        &self,
        req: BaseFileDataRequest<'_>,
    ) -> (
        Option<Box<dyn RecordBatchReader + Send>>,
        BaseFileProviderStats,
    ) {
        // ── Marshal the request into OWNED, Send data. The Arrow C schemas are
        //    Send; the strings become owned copies. This data is moved into the
        //    blocking closure below, where the borrowed C pointer struct is
        //    built pointing into it — so nothing non-`Send` (raw pointers) is
        //    ever held across the `.await`, keeping the future `Send` as the
        //    trait requires. ────────────────────────────────────────────────
        let projected_schema = match FFI_ArrowSchema::try_from(req.projected_schema.as_ref()) {
            Ok(s) => s,
            Err(e) => {
                log::warn!(
                    "[hudi-provider-abi] projected schema export failed; falling back to storage read: {e}"
                );
                return (None, BaseFileProviderStats::default());
            }
        };
        let data_schema: Option<FFI_ArrowSchema> = match req.data_schema {
            Some(s) => match FFI_ArrowSchema::try_from(s.as_ref()) {
                Ok(s) => Some(s),
                Err(e) => {
                    // Non-fatal: the provider can resolve partition types without it.
                    log::debug!("[hudi-provider-abi] data schema export failed; sending null: {e}");
                    None
                }
            },
            None => None,
        };
        let file_uri = req.file_uri.to_string();
        let partition_path = req.partition_path.to_string();
        let partition_fields: Vec<String> = req.partition_fields.to_vec();
        let can_push_predicate = req.can_push_predicate;
        let vtable = self.vtable;
        let ctx_addr = self.ctx as usize;

        // ── Cross the C boundary on a blocking-pool thread. The C call blocks
        //    on the provider's own runtime; doing that on an OBJECT_STORE_RUNTIME
        //    worker would panic, so we escape to `spawn_blocking`, whose thread
        //    carries no runtime context. All C-pointer data is built and lives
        //    inside the closure, valid for the whole synchronous call. ────────
        let call = tokio::task::spawn_blocking(move || {
            let field_slices: Vec<HudiStrSlice> = partition_fields
                .iter()
                .map(|f| HudiStrSlice::from_str(f))
                .collect();
            let c_req = HudiBaseFileDataRequest {
                file_uri: HudiStrSlice::from_str(&file_uri),
                projected_schema: &projected_schema,
                can_push_predicate,
                partition_path: HudiStrSlice::from_str(&partition_path),
                partition_fields: field_slices.as_ptr(),
                partition_fields_len: field_slices.len(),
                data_schema: data_schema
                    .as_ref()
                    .map_or(std::ptr::null(), |s| s as *const FFI_ArrowSchema),
            };
            let mut c_res = HudiBaseFileDataResult::empty();
            // SAFETY: `c_req` borrows only data owned by this closure, alive for
            // the whole synchronous call; `c_res` is a fresh out-parameter.
            let code = (vtable.try_base_file)(
                ctx_addr as *mut c_void,
                &c_req as *const HudiBaseFileDataRequest,
                &mut c_res as *mut HudiBaseFileDataResult,
            );
            (code, c_res)
        })
        .await;

        let (outcome_code, mut c_res) = match call {
            Ok(pair) => pair,
            Err(e) => {
                log::warn!(
                    "[hudi-provider-abi] provider call task failed; falling back to storage read: {e}"
                );
                return (None, BaseFileProviderStats::default());
            }
        };

        let mut stats: BaseFileProviderStats = c_res.stats.into();
        // The return value decides; `c_res.outcome` is advisory. They should agree,
        // and a provider that fills the stream, sets the field to SERVED and then
        // returns 0 (the "0 means success" C idiom) is the one way they will not —
        // so say so once per attempt rather than leaving an unexplained fallback
        // rate. See the doc on `HudiBaseFileDataResult`.
        if c_res.outcome != outcome_code {
            log::warn!(
                "[hudi-provider-abi] provider returned {outcome_code} but set \
                 out->outcome = {}; the RETURN VALUE is authoritative. If the \
                 provider believes it served this file, it must RETURN \
                 HUDI_PROVIDER_OUTCOME_SERVED ({HUDI_PROVIDER_OUTCOME_SERVED}), \
                 not 0",
                c_res.outcome
            );
        }
        if outcome_code != HUDI_PROVIDER_OUTCOME_SERVED {
            return (None, stats);
        }
        match Self::served_reader(&mut c_res) {
            Some(reader) => {
                // On the served path the drain counters are hudi-core's to fill:
                // the `served_batch_stream` (crates/core) tallies rows/bytes/batches as the stream
                // is consumed and merges them into the live stats slot. Zero
                // whatever the provider reported for them here, so a provider that
                // (against the contract) also populated these cannot cause a
                // double-count. Setup counters are kept.
                stats.rows_served = 0;
                stats.bytes_materialized = 0;
                stats.batches_received = 0;
                (Some(reader), stats)
            }
            None => {
                // The provider claimed SERVED but its stream could not be
                // imported, so the read below actually goes to object storage.
                // Reclassify to match what happened: leaving `files_served`
                // incremented would over-count served files and under-count
                // fallbacks in precisely the failure case an operator needs to
                // see. `saturating_sub` because a provider is not obliged to have
                // set `files_served` at all.
                stats.files_served = stats.files_served.saturating_sub(1);
                stats.storage_fallbacks += 1;
                stats.rows_served = 0;
                stats.bytes_materialized = 0;
                stats.batches_received = 0;
                (None, stats)
            }
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// C ABI entry points (defined by hudi-rs; called by the composition root)
// ════════════════════════════════════════════════════════════════════════════

/// Wrap a provider vtable + `ctx` into an owning handle for
/// `FfiReaderContext.base_file_provider_handle`.
///
/// Returns 0 if the vtable is null or its version is incompatible (the reader
/// then runs with no provider). On success, ownership of `ctx` transfers into the
/// handle; release it either by handing the handle to
/// `new_file_group_reader_with_context` (which consumes it) or, if the reader is
/// never built, by calling [`hudi_base_file_data_provider_free`].
///
/// # Safety
/// `vtable` must point to a valid [`HudiBaseFileDataProviderVTable`]; `ctx` must be the
/// context that vtable expects.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_base_file_data_provider_new(
    vtable: *const HudiBaseFileDataProviderVTable,
    ctx: *mut c_void,
) -> u64 {
    match unsafe { CApiBaseFileDataProvider::from_raw(vtable, ctx) } {
        Some(provider) => {
            let provider: BaseFileDataProviderRef = std::sync::Arc::new(provider);
            Box::into_raw(Box::new(provider)) as u64
        }
        None => 0,
    }
}

/// Release a handle from [`hudi_base_file_data_provider_new`] that was **not** consumed
/// by `new_file_group_reader_with_context` (e.g. the reader build was aborted).
/// Drops the provider, which runs its `destroy` exactly once. A 0 handle
/// is ignored.
///
/// # Safety
/// `handle` must have come from [`hudi_base_file_data_provider_new`] and not already been
/// consumed or freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_base_file_data_provider_free(handle: u64) {
    if handle != 0 {
        drop(unsafe { Box::from_raw(handle as *mut BaseFileDataProviderRef) });
    }
}

/// Consume a handle into the injectable provider reference. Returns `None` for a
/// 0 handle. Used by `new_file_group_reader_with_context`.
///
/// # Safety
/// `handle` must have come from [`hudi_base_file_data_provider_new`] and not already been
/// consumed or freed; ownership transfers to the returned value.
pub(crate) unsafe fn take_provider_from_handle(handle: u64) -> Option<BaseFileDataProviderRef> {
    if handle == 0 {
        return None;
    }
    Some(*unsafe { Box::from_raw(handle as *mut BaseFileDataProviderRef) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Shared across a single-threaded test via the provided ctx pointer.
    struct StubCtx {
        batches: Vec<RecordBatch>,
        outcome: c_int,
        /// When true, report SERVED but leave `out.stream` as hudi-rs
        /// pre-initialised it (empty). Models a provider that claims a serve it
        /// cannot back with a usable stream.
        serve_without_stream: bool,
        destroy_counter: *const AtomicUsize,
    }

    fn sample_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn sample_batch() -> RecordBatch {
        RecordBatch::try_new(
            sample_schema(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    extern "C" fn stub_try(
        ctx: *mut c_void,
        _req: *const HudiBaseFileDataRequest,
        out: *mut HudiBaseFileDataResult,
    ) -> c_int {
        let stub = unsafe { &*(ctx as *const StubCtx) };
        let out = unsafe { &mut *out };
        out.stats = HudiBaseFileProviderStats {
            files_served: (stub.outcome == HUDI_PROVIDER_OUTCOME_SERVED) as u64,
            storage_fallbacks: (stub.outcome != HUDI_PROVIDER_OUTCOME_SERVED) as u64,
            rows_served: 3,
            ..Default::default()
        };
        if stub.outcome == HUDI_PROVIDER_OUTCOME_SERVED && !stub.serve_without_stream {
            let schema = stub.batches[0].schema();
            let iter = RecordBatchIterator::new(stub.batches.clone().into_iter().map(Ok), schema);
            out.stream = FFI_ArrowArrayStream::new(Box::new(iter));
        }
        stub.outcome
    }

    extern "C" fn stub_destroy(ctx: *mut c_void) {
        let stub = unsafe { Box::from_raw(ctx as *mut StubCtx) };
        unsafe { &*stub.destroy_counter }.fetch_add(1, Ordering::SeqCst);
    }

    fn make_handle(outcome: c_int, counter: &AtomicUsize, version: u32) -> u64 {
        make_handle_inner(outcome, counter, version, false)
    }

    fn make_handle_inner(
        outcome: c_int,
        counter: &AtomicUsize,
        version: u32,
        serve_without_stream: bool,
    ) -> u64 {
        let stub = Box::new(StubCtx {
            batches: vec![sample_batch()],
            outcome,
            serve_without_stream,
            destroy_counter: counter as *const AtomicUsize,
        });
        let vtable = HudiBaseFileDataProviderVTable {
            abi_version: version,
            try_base_file: stub_try,
            destroy: stub_destroy,
        };
        unsafe { hudi_base_file_data_provider_new(&vtable, Box::into_raw(stub) as *mut c_void) }
    }

    fn sample_request<'a>(
        schema: &'a Arc<Schema>,
        fields: &'a [String],
    ) -> BaseFileDataRequest<'a> {
        BaseFileDataRequest {
            file_uri: "s3://bucket/table/part/base.parquet",
            projected_schema: schema,
            can_push_predicate: true,
            partition_path: "year=2024/month=01",
            partition_fields: fields,
            data_schema: None,
        }
    }

    /// EVERY FIELD of the request survives the C boundary, with the values the
    /// core seam put in it.
    ///
    /// Nothing else in this repository dereferences `HudiBaseFileDataRequest`.
    /// `stub_try` above takes it as `_req`; `cpp/src/lib.rs`'s end-to-end stub
    /// reads `projected_schema` and nothing more. So the seven tests that pin
    /// `can_push_predicate` in `reader_v2::engine` all assert against the CORE
    /// struct, and its transport to the only provider that exists outside tests
    /// — the C++ one, reached through this adapter and only this adapter — was
    /// asserted nowhere.
    ///
    /// That is not a theoretical gap. `let can_push_predicate = true;` one line
    /// above compiles, passes all eight rows of the mutation matrix and the whole
    /// suite, and tells a real provider it may push a predicate the core just
    /// withdrew — which is this branch's entire deliverable, defeated end to end.
    ///
    /// `can_push_predicate` is driven from **false** here on purpose:
    /// `sample_request`'s default is `true`, so a fixture that took the default
    /// could not tell a faithful copy from a hard-wired one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_request_field_survives_the_c_boundary() {
        use std::sync::Mutex as StdMutex;

        /// What the C side actually received, reconstructed from the raw request.
        #[derive(Debug, PartialEq)]
        struct SeenCRequest {
            file_uri: String,
            can_push_predicate: bool,
            partition_path: String,
            partition_fields: Vec<String>,
            partition_fields_len: usize,
            // The schemas' CONTENT, not just their null-ness. Round 5 showed that
            // `is_null()` alone leaves a mutation alive:
            // `FFI_ArrowSchema::try_from(req.data_schema.unwrap_or(req.projected_schema)…)`
            // compiles, keeps the pointer non-null, and hands the only real
            // provider the TABLE's schema where the per-file intersection belongs
            // — reinstating matrix row 6's corruption one layer below where row 6
            // pins it. The fixture's two schemas differ, so content tells them
            // apart and `is_null()` cannot.
            projected_schema: Option<Schema>,
            data_schema: Option<Schema>,
            /// Any schema that failed to import, as a message. Captured rather
            /// than unwrapped — see the import closure below.
            import_errors: Vec<String>,
        }
        static SEEN: StdMutex<Option<SeenCRequest>> = StdMutex::new(None);

        extern "C" fn capture_try(
            _ctx: *mut c_void,
            req: *const HudiBaseFileDataRequest,
            out: *mut HudiBaseFileDataResult,
        ) -> c_int {
            // SAFETY: the adapter guarantees `req` points at a live
            // `HudiBaseFileDataRequest` for the whole synchronous call, and its
            // slices borrow data owned by the calling closure.
            let req = unsafe { &*req };
            // `HudiStrSlice` is write-only in Rust (`from_str` and nothing else),
            // because the C side is its only reader. A test standing in for that
            // reader has to reconstruct the same way a C consumer does.
            //
            // SAFETY: the adapter builds each slice from a `&str` it owns for the
            // whole synchronous call.
            fn slice_to_string(s: &HudiStrSlice) -> String {
                if s.ptr.is_null() {
                    return String::new();
                }
                unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(s.ptr, s.len)) }
                    .to_string()
            }
            let fields = if req.partition_fields.is_null() {
                Vec::new()
            } else {
                // SAFETY: `partition_fields_len` is the adapter's own count for
                // the array it just built.
                unsafe {
                    std::slice::from_raw_parts(req.partition_fields, req.partition_fields_len)
                }
                .iter()
                .map(slice_to_string)
                .collect()
            };
            // SAFETY: both pointers, when non-null, are exports the adapter built
            // and keeps alive for the whole synchronous call.
            // The import's error is CAPTURED, never unwrapped: this is an
            // `extern "C" fn`, and since Rust 1.81 an unwind out of one ABORTS the
            // process. A `.expect()` here would take down the whole test binary —
            // every other result in the run — instead of failing one test.
            let import = |p: *const FFI_ArrowSchema| -> Result<Option<Schema>, String> {
                if p.is_null() {
                    Ok(None)
                } else {
                    Schema::try_from(unsafe { &*p })
                        .map(Some)
                        .map_err(|e| e.to_string())
                }
            };
            let (projected, data) = (import(req.projected_schema), import(req.data_schema));
            *SEEN.lock().unwrap() = Some(SeenCRequest {
                file_uri: slice_to_string(&req.file_uri),
                can_push_predicate: req.can_push_predicate,
                partition_path: slice_to_string(&req.partition_path),
                partition_fields: fields,
                partition_fields_len: req.partition_fields_len,
                projected_schema: projected.clone().unwrap_or(None),
                data_schema: data.clone().unwrap_or(None),
                import_errors: [projected.err(), data.err()]
                    .into_iter()
                    .flatten()
                    .collect(),
            });
            // SAFETY: a fresh out-parameter supplied by the adapter.
            unsafe { &mut *out }.stats = HudiBaseFileProviderStats {
                storage_fallbacks: 1,
                ..Default::default()
            };
            HUDI_PROVIDER_OUTCOME_NOT_SERVED
        }

        extern "C" fn capture_destroy(_ctx: *mut c_void) {}

        *SEEN.lock().unwrap() = None;
        let vtable = HudiBaseFileDataProviderVTable {
            abi_version: HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION,
            try_base_file: capture_try,
            destroy: capture_destroy,
        };
        let handle = unsafe { hudi_base_file_data_provider_new(&vtable, std::ptr::null_mut()) };
        let provider = unsafe { take_provider_from_handle(handle) }.expect("provider");

        // NOT all-nullable and NOT metadata-free. `Field`'s `PartialEq` compares
        // name, type, nullability AND metadata, so a fixture exercising only the
        // nullable/empty state would pass a marshaller that forced
        // `nullable = true` or dropped field metadata.
        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ts", DataType::Int64, true).with_metadata(
                [("unit".to_string(), "micros".to_string())]
                    .into_iter()
                    .collect(),
            ),
        ]));
        let data_schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("city", DataType::Utf8, true),
        ]));
        let fields = vec!["city".to_string(), "ts".to_string()];
        let (served, _stats) = provider
            .try_base_file(BaseFileDataRequest {
                file_uri: "s3://bucket/table/city=sf/base.parquet",
                projected_schema: &schema,
                // FALSE — the value the narrowing produces, and the one a
                // hard-wired marshaller would get wrong.
                can_push_predicate: false,
                partition_path: "city=sf/ts=2024",
                partition_fields: &fields,
                data_schema: Some(&data_schema),
            })
            .await;
        assert!(served.is_none(), "the capture stub declines");

        let seen = SEEN.lock().unwrap().take().expect("the C side was called");
        assert!(
            seen.import_errors.is_empty(),
            "both schemas must import cleanly on the C side: {:?}",
            seen.import_errors
        );
        assert_eq!(
            seen,
            SeenCRequest {
                file_uri: "s3://bucket/table/city=sf/base.parquet".to_string(),
                can_push_predicate: false,
                partition_path: "city=sf/ts=2024".to_string(),
                partition_fields: vec!["city".to_string(), "ts".to_string()],
                partition_fields_len: 2,
                // The PER-FILE intersection, not the table's data schema. These
                // two are deliberately different here, because that is the only
                // way the assertion can tell them apart.
                projected_schema: Some((*schema).clone()),
                data_schema: Some((*data_schema).clone()),
                import_errors: Vec::new(),
            },
            "every field must cross the boundary unaltered, CONTENT included — a \
             provider has nothing else to act on, and handing it the table's \
             schema where the file's intersection belongs is silent corruption \
             on a mislabelled file"
        );
    }

    /// EVERY STATS FIELD survives the C boundary INBOUND, in the right slot.
    ///
    /// The outbound half of this crossing — `to_ffi_base_file_provider_stats` in
    /// `cpp/src/lib.rs` — is pinned field-for-field with ten distinct values by
    /// `base_file_provider_stats_maps_all_fields`. The INBOUND half, the `From`
    /// impl above, had no such test: it was asserted only incidentally, by
    /// whatever the stubs happened to set. No stub in this repository sets
    /// `local_served` or `remote_served` at all, and three more fields are zeroed
    /// on the served path before anything can look at them.
    ///
    /// So swapping two adjacent `u64` lines —
    /// `local_served: s.remote_served, remote_served: s.local_served` — compiled,
    /// tripped no lint, and passed the entire suite. `files_served ==
    /// local_served + remote_served` still reconciles and the sum is unchanged,
    /// so nothing downstream notices; every dashboard fed by the Gluten metrics
    /// bridge just reports the inverse of the one signal the provider seam exists
    /// to produce — is this split being served locally or over the network.
    ///
    /// Ten DISTINCT non-zero values, on a NOT_SERVED outcome so the served path's
    /// deliberate zeroing of the three drain counters cannot mask them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_stats_field_survives_the_c_boundary_inbound() {
        extern "C" fn all_distinct(
            _ctx: *mut c_void,
            _req: *const HudiBaseFileDataRequest,
            out: *mut HudiBaseFileDataResult,
        ) -> c_int {
            // SAFETY: a fresh out-parameter supplied by the adapter.
            unsafe { &mut *out }.stats = HudiBaseFileProviderStats {
                files_served: 1,
                storage_fallbacks: 2,
                local_served: 3,
                remote_served: 4,
                rows_served: 5,
                bytes_served: 6,
                batches_received: 7,
                discover_wall_nanos: 8,
                connect_wall_nanos: 9,
                fetch_wall_nanos: 10,
            };
            HUDI_PROVIDER_OUTCOME_NOT_SERVED
        }
        extern "C" fn noop_destroy(_ctx: *mut c_void) {}

        let vtable = HudiBaseFileDataProviderVTable {
            abi_version: HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION,
            try_base_file: all_distinct,
            destroy: noop_destroy,
        };
        let handle = unsafe { hudi_base_file_data_provider_new(&vtable, std::ptr::null_mut()) };
        let provider = unsafe { take_provider_from_handle(handle) }.expect("provider");

        let schema: Arc<Schema> =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let fields: Vec<String> = Vec::new();
        let (served, stats) = provider
            .try_base_file(sample_request(&schema, &fields))
            .await;
        assert!(served.is_none(), "NOT_SERVED, so nothing is served");

        // Field for field, against the literals above — not against a re-read of
        // the source struct, which would pass under any permutation of it.
        assert_eq!(stats.files_served, 1, "files_served");
        assert_eq!(stats.storage_fallbacks, 2, "storage_fallbacks");
        assert_eq!(stats.local_served, 3, "local_served");
        assert_eq!(stats.remote_served, 4, "remote_served");
        assert_eq!(stats.rows_served, 5, "rows_served");
        assert_eq!(
            stats.bytes_materialized, 6,
            "the wire's `bytes_served` lands in hudi-core's `bytes_materialized` \
             — a deliberate rename, and the one field where a straight
             name-for-name copy would be wrong"
        );
        assert_eq!(stats.batches_received, 7, "batches_received");
        assert_eq!(stats.discover_wall_nanos, 8, "discover_wall_nanos");
        assert_eq!(stats.connect_wall_nanos, 9, "connect_wall_nanos");
        assert_eq!(stats.fetch_wall_nanos, 10, "fetch_wall_nanos");
    }

    /// `data_schema: None` must arrive as a NULL pointer, not as a dangling one.
    ///
    /// The header documents null as the legal spelling for "unavailable", and a
    /// provider is entitled to branch on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_absent_data_schema_arrives_as_null() {
        use std::sync::Mutex as StdMutex;
        static SAW_NULL: StdMutex<Option<bool>> = StdMutex::new(None);

        extern "C" fn null_probe(
            _ctx: *mut c_void,
            req: *const HudiBaseFileDataRequest,
            out: *mut HudiBaseFileDataResult,
        ) -> c_int {
            // SAFETY: as in `capture_try` above.
            *SAW_NULL.lock().unwrap() = Some(unsafe { &*req }.data_schema.is_null());
            // SAFETY: a fresh out-parameter supplied by the adapter.
            unsafe { &mut *out }.stats = HudiBaseFileProviderStats {
                storage_fallbacks: 1,
                ..Default::default()
            };
            HUDI_PROVIDER_OUTCOME_NOT_SERVED
        }
        extern "C" fn null_destroy(_ctx: *mut c_void) {}

        *SAW_NULL.lock().unwrap() = None;
        let vtable = HudiBaseFileDataProviderVTable {
            abi_version: HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION,
            try_base_file: null_probe,
            destroy: null_destroy,
        };
        let handle = unsafe { hudi_base_file_data_provider_new(&vtable, std::ptr::null_mut()) };
        let provider = unsafe { take_provider_from_handle(handle) }.expect("provider");

        let schema: Arc<Schema> =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let fields: Vec<String> = Vec::new();
        let _ = provider
            .try_base_file(sample_request(&schema, &fields))
            .await;

        assert_eq!(
            SAW_NULL.lock().unwrap().take(),
            Some(true),
            "an absent data schema must arrive as NULL, which is what the header \
             tells a provider to test for"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn served_outcome_roundtrips_batches() {
        let counter = AtomicUsize::new(0);
        let handle = make_handle(
            HUDI_PROVIDER_OUTCOME_SERVED,
            &counter,
            HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION,
        );
        let provider = unsafe { take_provider_from_handle(handle) }.expect("provider");

        let schema = sample_schema();
        let fields = vec!["year".to_string(), "month".to_string()];
        let (served, stats) = provider
            .try_base_file(sample_request(&schema, &fields))
            .await;

        // The served source is lazy: drain it here to assert the batches
        // survive the C round-trip.
        let reader = served.expect("expected served data");
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("drain served stream");
        assert_eq!(batches.len(), 1, "one batch expected");
        assert_eq!(
            batches[0],
            sample_batch(),
            "batch survives the C round-trip"
        );
        assert_eq!(stats.files_served, 1);
        // The stub deliberately mis-reports a drain counter (rows_served: 3). On
        // the served path hudi-core owns the drain counters — `served_batch_stream` (crates/core)
        // fills them as it consumes the stream — so provider-abi zeroes whatever
        // the provider claimed, preventing a double-count. Setup counters stay.
        assert_eq!(
            stats.rows_served, 0,
            "served-path drain counters are zeroed to avoid double-counting"
        );

        drop(provider);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "destroy runs exactly once"
        );
    }

    /// A provider that reports SERVED but hands back a stream that cannot be
    /// imported must degrade to "not served" so the read falls back to object
    /// storage — this is the one failure the streaming shape can still classify
    /// before any data has moved.
    ///
    /// Also pins the stats reclassification: the attempt must NOT be left counted
    /// as a served file, because the bytes actually came from storage. Getting
    /// that wrong inflates the served-file count and hides the fallback in exactly
    /// the case an operator is trying to diagnose.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn served_with_unimportable_stream_falls_back_and_reclassifies_stats() {
        let counter = AtomicUsize::new(0);
        let handle = make_handle_inner(
            HUDI_PROVIDER_OUTCOME_SERVED,
            &counter,
            HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION,
            /* serve_without_stream */ true,
        );
        let provider = unsafe { take_provider_from_handle(handle) }.expect("provider");

        let schema = sample_schema();
        let (served, stats) = provider.try_base_file(sample_request(&schema, &[])).await;

        assert!(
            served.is_none(),
            "an unimportable stream must degrade to a storage read, not a partial serve"
        );
        // The stub set files_served: 1 alongside its SERVED outcome; since the
        // stream was unusable, that has to be walked back.
        assert_eq!(
            stats.files_served, 0,
            "a failed import must not stay counted as a served file"
        );
        assert_eq!(
            stats.storage_fallbacks, 1,
            "a failed import must be counted as a storage fallback"
        );
        // Drain counters are hudi-core's on the served path and meaningless here.
        assert_eq!(stats.rows_served, 0, "no rows were served");
        assert_eq!(stats.bytes_materialized, 0, "no bytes were materialized");
        assert_eq!(stats.batches_received, 0, "no batches were received");

        drop(provider);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "destroy still runs exactly once on the failed-import path"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn not_served_falls_through_but_reports_stats() {
        let counter = AtomicUsize::new(0);
        let handle = make_handle(
            HUDI_PROVIDER_OUTCOME_NOT_SERVED,
            &counter,
            HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION,
        );
        let provider = unsafe { take_provider_from_handle(handle) }.expect("provider");

        let schema = sample_schema();
        let (served, stats) = provider.try_base_file(sample_request(&schema, &[])).await;

        assert!(served.is_none(), "expected no served data");
        assert_eq!(
            stats.storage_fallbacks, 1,
            "a storage fallback still reports its counters"
        );

        drop(provider);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn version_mismatch_yields_no_provider() {
        let counter = AtomicUsize::new(0);
        // Wrong ABI version → new returns 0 (no provider). ctx is intentionally
        // leaked (destroy not trusted for a mismatched vtable), so the counter
        // stays at 0 — asserting we never called the untrusted destroy.
        let handle = make_handle(
            HUDI_PROVIDER_OUTCOME_SERVED,
            &counter,
            HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION + 1,
        );
        assert_eq!(handle, 0, "incompatible version must not produce a handle");
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn free_unconsumed_handle_runs_destroy_once() {
        let counter = AtomicUsize::new(0);
        let handle = make_handle(
            HUDI_PROVIDER_OUTCOME_SERVED,
            &counter,
            HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION,
        );
        assert_ne!(handle, 0);
        unsafe { hudi_base_file_data_provider_free(handle) };
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "free drops the provider once"
        );
    }
}
