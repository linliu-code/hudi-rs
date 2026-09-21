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

//! Exact hit/miss accounting for the parquet arrow-schema cache.
//!
//! **Why this is a separate test binary.** `parquet_schema_cache_stats()`
//! reports process-global counters, and inside the unit-test binary every test
//! that reads a base file moves them — `#[serial]` orders the tests that opt
//! into it, but not the dozens that read parquet without knowing this cache
//! exists. Asserting an exact delta there is a race that passes almost always,
//! which is worse than not asserting it. Cargo gives each integration test file
//! its own process, so here the statics belong to this file alone and the
//! deltas are exact.
//!
//! The unit tests in `storage::parquet_schema_cache` keep the assertions that
//! are local and therefore race-free: how many times the loader ran, which `Arc`
//! came back, and which error variant surfaced.
//!
//! Within this file the tests are `#[serial]` for the same reason they are
//! isolated from the unit-test binary: cargo runs test functions on threads of
//! one process, so without it they would move each other's counters.

use std::collections::HashMap;
use std::fs::canonicalize;
use std::path::Path;
use std::sync::Arc;

use hudi_core::config::HudiConfigs;
use hudi_core::config::table::HudiTableConfig;
use hudi_core::file_group::base_file::parquet::ParquetBaseFileReader;
use hudi_core::file_group::base_file::reader::{BaseFileReadOptions, BaseFileReader};
use hudi_core::storage::{Storage, parquet_schema_cache_clear, parquet_schema_cache_stats};
use serial_test::serial;
use url::Url;

const FIXTURE: &str = "a.parquet";

fn test_storage() -> Arc<Storage> {
    let base_url =
        Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
    let hudi_configs = Arc::new(HudiConfigs::new([(
        HudiTableConfig::BasePath.as_ref(),
        base_url.as_str().to_string(),
    )]));
    Storage::new(Arc::new(HashMap::new()), hudi_configs).unwrap()
}

/// A cold read is one miss and no hit; the warm read that follows is one hit and
/// no further miss. This is the accounting an embedder exports as a hit rate, so
/// an off-by-one in either direction misreports how well the cache is working.
#[tokio::test]
#[serial]
async fn a_cold_read_is_a_miss_and_the_warm_read_that_follows_is_a_hit() {
    parquet_schema_cache_clear();
    let reader = ParquetBaseFileReader::new(test_storage());

    let before = parquet_schema_cache_stats();
    reader
        .read_schema(FIXTURE, BaseFileReadOptions::default())
        .await
        .expect("cold read");
    let cold = parquet_schema_cache_stats();
    assert_eq!(cold.misses - before.misses, 1, "the cold read is one miss");
    assert_eq!(cold.hits - before.hits, 0, "and not also a hit");

    reader
        .read_schema(FIXTURE, BaseFileReadOptions::default())
        .await
        .expect("warm read");
    let warm = parquet_schema_cache_stats();
    assert_eq!(warm.hits - cold.hits, 1, "the warm read is one hit");
    assert_eq!(
        warm.misses, cold.misses,
        "and must not also count a miss — that would deflate the hit rate the \
         counters exist to report"
    );
}

/// A read of a file that does not exist counts as neither, so a run of missing
/// or corrupt files cannot make the hit rate look terrible. It is also not
/// cached: the next read tries again.
///
/// The POSITIVE CONTROL at the end is load-bearing, not padding. Every assertion
/// above it is of the form "this counter did not move", and a cache that is off
/// moves no counter either — `get_or_load` short-circuits before either one. So
/// without the control this test passes unchanged with
/// `HUDI_PARQUET_SCHEMA_CACHE_ENABLED=false`, i.e. it would report a healthy
/// cache in a process that has none.
#[tokio::test]
#[serial]
async fn a_failed_read_counts_as_neither_and_is_not_cached() {
    parquet_schema_cache_clear();
    let reader = ParquetBaseFileReader::new(test_storage());

    let before = parquet_schema_cache_stats();
    for attempt in 0..2 {
        reader
            .read_schema("does_not_exist.parquet", BaseFileReadOptions::default())
            .await
            .expect_err("a missing file must fail");
        let after = parquet_schema_cache_stats();
        assert_eq!(
            after.hits, before.hits,
            "attempt {attempt}: a failed load is not a hit"
        );
        assert_eq!(after.misses, before.misses, "attempt {attempt}: nor a miss");
    }

    // Positive control: a real read in this same process must still move the
    // counters. If it does not, the cache is not live here and the four "did not
    // move" assertions above proved nothing about failed reads.
    let after_failures = parquet_schema_cache_stats();
    reader
        .read_schema(FIXTURE, BaseFileReadOptions::default())
        .await
        .expect("the fixture must read");
    let control = parquet_schema_cache_stats();
    assert_eq!(
        control.misses - after_failures.misses,
        1,
        "the cache must be live in this process, or the assertions above are \
         vacuous: a successful read of an uncached file is one miss"
    );
}

/// `clear()` empties the cache, so the next read is a miss again. The escape
/// hatch for a caller outside the Hudi read path that overwrote a file in place;
/// if it did not actually evict, that caller would keep reading a stale schema.
#[tokio::test]
#[serial]
async fn clear_makes_the_next_read_a_miss_again() {
    parquet_schema_cache_clear();
    let reader = ParquetBaseFileReader::new(test_storage());

    reader
        .read_schema(FIXTURE, BaseFileReadOptions::default())
        .await
        .expect("cold read");
    let warm_before = parquet_schema_cache_stats();
    reader
        .read_schema(FIXTURE, BaseFileReadOptions::default())
        .await
        .expect("warm read");
    assert_eq!(
        parquet_schema_cache_stats().hits - warm_before.hits,
        1,
        "sanity: the entry really was cached before clear()"
    );

    parquet_schema_cache_clear();
    let after_clear = parquet_schema_cache_stats();
    reader
        .read_schema(FIXTURE, BaseFileReadOptions::default())
        .await
        .expect("post-clear read");
    let reloaded = parquet_schema_cache_stats();
    assert_eq!(
        reloaded.misses - after_clear.misses,
        1,
        "after clear() the footer is fetched again"
    );
    assert_eq!(
        reloaded.hits, after_clear.hits,
        "and it is not served from a stale entry"
    );
}
