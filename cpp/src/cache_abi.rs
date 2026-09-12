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
//
// `hudi_core::storage::parquet_schema_cache_clear/_stats` are `pub`, and were
// `pub` at the Rust boundary ONLY — neither `cpp/` nor `crates/jvm-ffi` plumbed
// them, so the documented escape hatch did not exist for the embedders this
// lineage actually ships to (ISSUES OI-5).
//
// It matters because the cache is sound on "base parquet files are immutable",
// which holds for Hudi's own writers and not for an operator running a
// remediation tool — including one rewriting a footer to correct the
// apache/hudi#18132 mislabel, which is the exact input the repair gate keys on.
// With a warm cache and no TTL, a long-lived embedding process keeps comparing
// the PRE-rewrite footer, and one direction of that error pushes a `RowFilter`
// against reinterpreted values and silently drops rows. The TTL question is
// #143's to answer; being able to clear the cache at all is not.
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
    use super::*;
    use hudi_test::QuickstartTripsTable;

    /// The exports reach the REAL cache, in the right slots, and `clear()`
    /// actually clears.
    ///
    /// Every assertion is against a POPULATED cache with `hits` and `misses` at
    /// DIFFERENT values. A version of this that only read a cold cache asserted
    /// `0 == 0` three times: swapping `hits`/`misses` in the marshaller passed,
    /// and so did making `clear()` a no-op. That is this workspace's recurring
    /// defect — an assertion that cannot fail — reappearing inside the test
    /// written to close it.
    ///
    /// `clear()` is proven BEHAVIOURALLY, by the next read missing again, not by
    /// `entries` dropping to zero: moka's `entry_count` is documented as lagging
    /// recent inserts and invalidations, so an assertion on it is about timing.
    /// hudi-core's own `clear()` test takes the same shape for the same reason.
    ///
    /// `serial`, because the cache is PROCESS-WIDE: `clear()` here is visible to
    /// every other test in this binary, and their reads move these counters.
    #[test]
    #[serial_test::serial]
    fn the_c_exports_read_and_clear_the_real_cache() {
        let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();

        // ── cold: a footer is fetched, so `misses` moves ────────────────────
        hudi_parquet_schema_cache_clear();
        let base = hudi_parquet_schema_cache_stats();
        crate::tests::provider_e2e::read_once(&table_path);
        let cold = hudi_parquet_schema_cache_stats();
        assert!(
            cold.misses > base.misses,
            "a read after clear() must MISS: {} -> {}",
            base.misses,
            cold.misses
        );

        // ── warm: the same file again hits, and fetches nothing ─────────────
        //
        // THREE warm reads, not one. One cold plus one warm leaves hits == misses
        // == 1, and the fixture check below — which exists precisely to catch
        // this — fired on the first run of this test.
        for _ in 0..3 {
            crate::tests::provider_e2e::read_once(&table_path);
        }
        let warm = hudi_parquet_schema_cache_stats();
        assert!(
            warm.hits > cold.hits,
            "a second read of the same file must HIT: {} -> {}",
            cold.hits,
            warm.hits
        );
        assert_eq!(
            warm.misses, cold.misses,
            "and must not fetch the footer again"
        );

        // ── the two views are the same cache, slot for slot ─────────────────
        //
        // `hits` and `misses` are now DIFFERENT numbers, which is what makes the
        // two slots distinguishable — without that, a marshaller that swapped
        // them would compare equal.
        let rust = hudi_dep::storage::parquet_schema_cache_stats();
        let c = hudi_parquet_schema_cache_stats();
        assert_ne!(
            rust.hits, rust.misses,
            "fixture check: the counters must differ, or the comparison below \
             cannot tell one slot from the other"
        );
        assert_eq!(
            (c.hits, c.misses),
            (rust.hits, rust.misses),
            "the C view must be the same cache, field for field — swapping two \
             u64s in the marshaller compiles and is invisible to any assertion \
             about shape"
        );
        // `entries` is compared too, but loosely: moka's count lags, so the two
        // calls above can legitimately straddle a pending task. Equality is the
        // common case and worth asserting; a difference of one is not a defect.
        assert!(
            c.entries.abs_diff(rust.entries) <= 1,
            "entries must track the same cache: C {} vs Rust {}",
            c.entries,
            rust.entries
        );

        // ── clear() really clears, proven by behaviour rather than by count ──
        hudi_parquet_schema_cache_clear();
        let before_third = hudi_parquet_schema_cache_stats();
        crate::tests::provider_e2e::read_once(&table_path);
        let third = hudi_parquet_schema_cache_stats();
        assert!(
            third.misses > before_third.misses,
            "after clear() the SAME file must miss again ({} -> {}) — a no-op \
             clear leaves a remediated file being read against its pre-rewrite \
             footer, which is the whole reason this export exists",
            before_third.misses,
            third.misses
        );
    }
}
