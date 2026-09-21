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

//! The C exports reach the real parquet schema cache, in the right slots, and
//! `clear()` really clears.
//!
//! **Why this is a separate test binary.** `hudi_parquet_schema_cache_stats()`
//! reports process-global counters. In the unit-test binary this test shared a
//! process with six others that read the same fixture table at the same URL with
//! the same (empty) option map — therefore the same cache key — so every counter
//! it read was being moved underneath it by neighbours. The previous version
//! coped by asserting only `>` and `>=`, which made two of its assertions unable
//! to fail for the right reason: a neighbour's cold read could satisfy the
//! "clear() really cleared" check, so the mutation that matters — making
//! `hudi_parquet_schema_cache_clear()` a no-op — was not deterministically
//! killed.
//!
//! Cargo gives each integration-test file its own process, so here the statics
//! belong to this file alone and the deltas are EXACT. `crates/core/tests/
//! parquet_schema_cache_stats.rs` is isolated for the same reason; this is the
//! C-ABI half of that.
//!
//! One test function, deliberately: a second one in this file would share the
//! process and reintroduce exactly the problem the file exists to remove. Add
//! `#[serial]` before adding a second.

use std::sync::Arc;

use hudi::cache_abi::{hudi_parquet_schema_cache_clear, hudi_parquet_schema_cache_stats};
use hudi::hudi_core::config::HudiConfigs;
use hudi::hudi_core::file_group::base_file::parquet::ParquetBaseFileReader;
use hudi::hudi_core::file_group::base_file::reader::{BaseFileReadOptions, BaseFileReader};
use hudi::hudi_core::storage::{Storage, parquet_schema_cache_stats};
use hudi::hudi_core::table::builder::OptionResolver;
use hudi_test::QuickstartTripsTable;

/// A base file that exists in the fixture table, relative to the table root.
const BASE_FILE: &str =
    "city=sf/fee86b18-67b1-4479-b517-075683aeb2d1-0_0-13-33_20260408053032350.parquet";

async fn table_storage(table_path: &str) -> Arc<Storage> {
    let empty_opts: Vec<(&str, &str)> = vec![];
    let mut resolver = OptionResolver::new_with_options(table_path, empty_opts);
    resolver.resolve_options().await.expect("resolve options");
    let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options.clone()));
    Storage::new(Arc::new(resolver.storage_options), hudi_configs).expect("create storage")
}

/// The C view is the same cache as the Rust view, slot for slot, and `clear()`
/// is proven by the next read MISSING again rather than by an entry count.
///
/// `entries` is deliberately not asserted: moka's `entry_count` lags inserts and
/// invalidations by design, so a value read from it says when the cache's
/// bookkeeping caught up, not whether the marshaller put the right number in the
/// right field. The behavioural proof below is stronger — a no-op `clear()`
/// cannot produce a miss on a file that was just read.
#[tokio::test]
async fn the_c_exports_read_and_clear_the_real_cache() {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.path_to_mor_avro();
    let reader = ParquetBaseFileReader::new(table_storage(&table_path).await);

    // ── cold: the footer is fetched, so exactly one MISS and no hit ──────────
    hudi_parquet_schema_cache_clear();
    let before = hudi_parquet_schema_cache_stats();
    reader
        .read_schema(BASE_FILE, BaseFileReadOptions::default())
        .await
        .expect("cold read");
    let cold = hudi_parquet_schema_cache_stats();
    assert_eq!(
        cold.misses - before.misses,
        1,
        "a read after clear() is exactly one miss"
    );
    assert_eq!(
        cold.hits - before.hits,
        0,
        "and not also a hit — swapping the two slots in the marshaller lands the \
         miss here"
    );

    // ── warm: each further read of the same file is exactly one HIT ─────────
    //
    // THREE, not one. One cold plus one warm leaves `hits == misses == 1`, and
    // the two slots are then indistinguishable — a marshaller that swapped them
    // would compare equal, and the fixture check below exists to catch exactly
    // that. It fires on a single warm read; this is not hypothetical.
    const WARM_READS: u64 = 3;
    for _ in 0..WARM_READS {
        reader
            .read_schema(BASE_FILE, BaseFileReadOptions::default())
            .await
            .expect("warm read");
    }
    let warm = hudi_parquet_schema_cache_stats();
    assert_eq!(
        warm.hits - cold.hits,
        WARM_READS,
        "every read after the cold one is a hit, and exactly one each"
    );
    assert_eq!(
        warm.misses, cold.misses,
        "and must not also count a miss — that would deflate the hit rate these \
         counters exist to report"
    );

    // ── the two views are the same cache, in the same slots ──────────────────
    //
    // EXACT equality is available here because nothing else in this process
    // touches the cache. `hits` and `misses` now hold different numbers, which is
    // what makes the two slots distinguishable at all: a marshaller that swapped
    // them would compare equal if they were the same.
    let c = hudi_parquet_schema_cache_stats();
    let rust = parquet_schema_cache_stats();
    assert_ne!(
        rust.hits, rust.misses,
        "fixture check: the two counters must differ, or the swap check below \
         cannot tell one slot from the other"
    );
    assert_eq!(
        (c.hits, c.misses),
        (rust.hits, rust.misses),
        "the C view must be the same cache in the same slots: C (hits {}, misses \
         {}) vs Rust (hits {}, misses {})",
        c.hits,
        c.misses,
        rust.hits,
        rust.misses
    );

    // ── clear() really clears, proven behaviourally ──────────────────────────
    hudi_parquet_schema_cache_clear();
    let after_clear = hudi_parquet_schema_cache_stats();
    reader
        .read_schema(BASE_FILE, BaseFileReadOptions::default())
        .await
        .expect("post-clear read");
    let third = hudi_parquet_schema_cache_stats();
    assert_eq!(
        third.misses - after_clear.misses,
        1,
        "after clear() the SAME file must miss again — a no-op clear leaves a \
         remediated file being read against its pre-rewrite footer, which is the \
         whole reason this export exists"
    );
    assert_eq!(
        third.hits - after_clear.hits,
        0,
        "and must not be served from the cache it was just told to drop"
    );
}
