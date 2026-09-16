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

//! C ABI for the process-wide parquet schema cache.
//!
//! `hudi_core::storage::parquet_schema_cache_clear/_stats` are `pub`, and were
//! `pub` at the RUST boundary only — neither `cpp/` nor `crates/jvm-ffi` plumbed
//! them, so the escape hatch hudi-core documents did not exist for the embedders
//! this binding ships to (ISSUES OI-5).
//!
//! It matters because the cache is sound on "base parquet files are immutable",
//! which holds for Hudi's own writers and not for an operator running a
//! remediation tool — including one rewriting a footer to correct an
//! apache/hudi#18132 logical-type mislabel, which is the exact input the repair
//! gate keys on. With a warm cache and no TTL, a long-lived embedding process
//! keeps comparing the PRE-rewrite footer, and one direction of that error pushes
//! a `RowFilter` against reinterpreted values and drops rows that match.
//!
//! Whether the cache should have a TTL is #143's question. Whether an embedder
//! can clear it at all is not, and the answer was no.

/// Snapshot of the parquet schema cache counters. Mirrors
/// `hudi_core::storage::ParquetSchemaCacheStats`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct HudiParquetSchemaCacheStats {
    /// Warm reads served without a footer fetch. Monotonic since process start.
    pub hits: u64,
    /// Cold reads that fetched a footer. Monotonic since process start.
    pub misses: u64,
    /// Entries currently held.
    ///
    /// **Deliberately unpinned by the mutation ledger, unlike `hits` and
    /// `misses`.** moka's `entry_count` is documented as lagging recent inserts
    /// and invalidations, so any assertion on this field is an assertion about
    /// when the cache's internal bookkeeping catches up rather than about the
    /// marshalling below. `hits`/`misses` are monotonic counters and carry the
    /// same "is this the same cache, in the same slots" proof without that
    /// timing dependence, which is why they are the ones pinned. Swapping this
    /// field with either of them would therefore not be caught here — it is
    /// caught by `hits`/`misses` disagreeing, since all three are distinct
    /// numbers in the test's populated-cache state.
    pub entries: u64,
}
/// Read the parquet schema cache counters, for an embedder to export through its
/// own metrics registry (hudi-rs pulls in no metrics framework of its own).
///
/// Always safe to call, from any thread, with or without a runtime.
#[unsafe(no_mangle)]
pub extern "C" fn hudi_parquet_schema_cache_stats() -> HudiParquetSchemaCacheStats {
    let s = hudi_dep::storage::parquet_schema_cache_stats();
    HudiParquetSchemaCacheStats {
        hits: s.hits,
        misses: s.misses,
        entries: s.entries,
    }
}
/// Drop every cached parquet schema, so the next read of each file re-fetches its
/// footer.
///
/// A Hudi table never needs this: its base files are immutable. Call it after
/// rewriting a base file IN PLACE — e.g. a remediation tool correcting an
/// apache/hudi#18132 logical-type mislabel — or the reader keeps comparing the
/// footer as it was before the rewrite.
///
/// Process-wide, not per reader. Always safe to call, from any thread.
#[unsafe(no_mangle)]
pub extern "C" fn hudi_parquet_schema_cache_clear() {
    hudi_dep::storage::parquet_schema_cache_clear();
}

#[cfg(test)]
mod tests {
    // The behavioural test for these exports — that they read the REAL cache, in
    // the right slots, and that `clear()` actually clears — lives in
    // `cpp/tests/parquet_schema_cache_abi.rs`, not here.
    //
    // It cannot live in this binary. The counters are process-global, and six
    // other tests in the unit-test binary read the same fixture table at the same
    // URL with the same empty option map, i.e. the same cache key, concurrently.
    // Asserting an exact delta here is a race; asserting only `>` (which is what
    // this test used to do to cope) makes the assertion that matters unable to
    // fail for the right reason — a neighbour's cold read satisfies "clear()
    // really cleared", so a no-op `clear()` is not deterministically killed.
    //
    // Cargo gives each integration-test file its own process, so over there the
    // statics belong to that file alone and the deltas are exact.
    // `crates/core/tests/parquet_schema_cache_stats.rs` is isolated for the same
    // reason.
}
