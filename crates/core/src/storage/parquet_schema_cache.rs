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

//! Process-wide cache of per-file parquet arrow schemas, for the read hot path.
//!
//! [`ParquetBaseFileReader::read_schema`](crate::file_group::base_file::parquet::ParquetBaseFileReader)
//! runs once per base file on every read — the schema-evolution intersection in
//! the file group reader's `base_file_source` — but only needs the file's arrow
//! [`SchemaRef`]. Fetching it cold costs an S3 `HEAD` for the file size plus the
//! footer `GET` (~1 MiB), which measurably dominated point-query scan latency.
//!
//! A base parquet file is IMMUTABLE — Hudi never rewrites a file in place
//! (updates land in new files whose names carry commit-time + write-token +
//! UUID; cleaning only deletes). So a URI → schema mapping can never go stale on
//! the Hudi read path, and no invalidation is needed there. Non-Hudi callers
//! that overwrite a file in place can use [`clear`].

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow_schema::SchemaRef;
use moka::future::Cache;
use once_cell::sync::Lazy;
use url::Url;

use crate::storage::error::{Result, StorageError};

/// Entry capacity when `HUDI_PARQUET_SCHEMA_CACHE_MAX_ENTRIES` is unset.
///
/// Sized to hold the whole footer working set of a full-table scan (hundreds to
/// thousands of files) rather than to bound memory: identical schemas are
/// interned (see [`intern`]), so an entry costs its key string plus one `Arc`
/// pointer, and 200k of those is tens of MB.
const DEFAULT_MAX_ENTRIES: u64 = 200_000;

/// Safety valve on [`SCHEMA_INTERNER`]. Past it, schemas are cached
/// un-interned, which costs memory and never correctness.
const INTERNER_MAX: usize = 4096;

/// The cache itself, keyed by absolute storage URI plus the storage's option set
/// (see [`cache_key`]).
///
/// Cold misses on one key are single-flighted via `try_get_with`, so N
/// concurrent first reads of a file cost one footer fetch.
///
/// Configuration, read once per process on first use:
///   - `HUDI_PARQUET_SCHEMA_CACHE_MAX_ENTRIES`: entry capacity (default
///     [`DEFAULT_MAX_ENTRIES`]).
///   - `HUDI_PARQUET_SCHEMA_CACHE_ENABLED`: default ON; set
///     `false`/`0`/`off`/`no` to bypass.
static CACHE: Lazy<Cache<String, SchemaRef>> = Lazy::new(|| {
    let max_entries = parse_max_entries(
        std::env::var("HUDI_PARQUET_SCHEMA_CACHE_MAX_ENTRIES")
            .ok()
            .as_deref(),
    );
    Cache::builder()
        .name("parquet_arrow_schema")
        .max_capacity(max_entries)
        .build()
});

/// Interner for [`CACHE`]'s values: every file of a table version usually
/// carries an identical schema, so identical `Schema`s share one `Arc` instead
/// of one allocation per file. This is what bounds the cache's memory by
/// *distinct* schemas rather than entries × schema width.
///
/// Deliberately outlives the entries that reference it: an interned `Schema` is
/// never individually evicted, even after moka's LRU has evicted every entry
/// that pointed at it, so up to [`INTERNER_MAX`] distinct schemas stay resident
/// for the process's life. Intentional, not an oversight — tracking per-schema
/// reference counts (a `Weak`-based interner) to evict in step with the bounded
/// cache would add real complexity for a bound that is already small and
/// self-limiting in practice: a process reads from a bounded number of distinct
/// table versions, not an unbounded stream of distinct schemas. [`clear`] frees
/// it for callers that want the memory back.
static SCHEMA_INTERNER: Lazy<Mutex<HashSet<SchemaRef>>> = Lazy::new(|| Mutex::new(HashSet::new()));

static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

fn intern(schema: SchemaRef) -> SchemaRef {
    let mut set = SCHEMA_INTERNER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = set.get(&schema) {
        return existing.clone();
    }
    if set.len() < INTERNER_MAX {
        set.insert(schema.clone());
    }
    schema
}

/// Cumulative counters plus a size gauge for the schema cache, process-wide.
///
/// A read counts as a **miss** when it performed the footer load itself and that
/// load SUCCEEDED (failed loads count as neither, so the hit rate is not
/// deflated by missing or corrupt files), and as a **hit** when it did not load
/// — the schema came from a populated entry, or the read coalesced onto another
/// caller's in-flight load for the same key (single-flight; it too issues no
/// round trip of its own). Reads taken with the cache disabled count as neither.
/// Hit rate = `hits / (hits + misses)`. `entries` is moka's `entry_count`, which
/// may lag slightly behind recent inserts and evictions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetSchemaCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: u64,
}

/// Snapshot of the cache counters, for an embedding process to export through
/// its own metrics registry (hudi-core pulls in no metrics framework of its
/// own). `hits`/`misses` are monotonic since process start.
pub fn stats() -> ParquetSchemaCacheStats {
    ParquetSchemaCacheStats {
        hits: HITS.load(Ordering::Relaxed),
        misses: MISSES.load(Ordering::Relaxed),
        entries: CACHE.entry_count(),
    }
}

/// Drop every cached schema, and the interner with it.
///
/// For callers outside the Hudi read path that overwrite a parquet file in place
/// and need the next schema read to re-fetch the footer. A Hudi table never
/// needs this: its base files are immutable.
pub fn clear() {
    CACHE.invalidate_all();
    SCHEMA_INTERNER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

/// Pure parse of `HUDI_PARQUET_SCHEMA_CACHE_ENABLED`: only an explicit
/// `false`/`0`/`off`/`no` (any case, surrounding whitespace ignored) disables;
/// unset or anything else enables.
fn parse_enabled(raw: Option<&str>) -> bool {
    raw.map(|v| {
        !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "false" | "0" | "off" | "no"
        )
    })
    .unwrap_or(true)
}

/// Pure parse of `HUDI_PARQUET_SCHEMA_CACHE_MAX_ENTRIES`; an unparseable value
/// falls back to the default with a WARN rather than silently.
fn parse_max_entries(raw: Option<&str>) -> u64 {
    match raw {
        None => DEFAULT_MAX_ENTRIES,
        Some(v) => v.trim().parse().unwrap_or_else(|_| {
            log::warn!(
                "ignoring unparseable HUDI_PARQUET_SCHEMA_CACHE_MAX_ENTRIES={v:?}; \
                 using default {DEFAULT_MAX_ENTRIES}"
            );
            DEFAULT_MAX_ENTRIES
        }),
    }
}

/// Whether the cache is active (default **ON**; disable via
/// `HUDI_PARQUET_SCHEMA_CACHE_ENABLED=false`/`0`/`off`/`no`). Read once per
/// process — setting the env var after the first read has no effect.
fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = parse_enabled(
            std::env::var("HUDI_PARQUET_SCHEMA_CACHE_ENABLED")
                .ok()
                .as_deref(),
        );
        log::debug!(
            "parquet arrow-schema cache enabled={enabled} \
             (HUDI_PARQUET_SCHEMA_CACHE_ENABLED; read once per process)"
        );
        enabled
    })
}

/// Cache key: the file's absolute URI plus a **hash** of the storage's option
/// map.
///
/// The store identity matters because the same `s3://bucket/path` URI can
/// resolve to different physical stores under different options (custom
/// endpoints, per-session credentials) — a URI-only key would serve one
/// endpoint's schema for another's file.
///
/// It is hashed rather than embedded because the option map can hold credentials
/// (`aws_secret_access_key`, SAS tokens), and copying those into up to
/// `max_entries` (200k) keys — and into anything that ever logs or dumps a key —
/// is not acceptable. Only in-process stability is required of the hash, which
/// `DefaultHasher` provides; a collision needs two *distinct* option sets to
/// agree in 64 bits.
fn cache_key(file_url: &Url, options: &HashMap<String, String>) -> String {
    let mut hasher = DefaultHasher::new();
    // A `HashMap` has no stable iteration order, so hash a sorted view of it:
    // two `Storage`s built from equal option maps must produce the same key.
    let mut entries: Vec<(&String, &String)> = options.iter().collect();
    entries.sort_unstable();
    entries.hash(&mut hasher);
    format!("{file_url}|{:016x}", hasher.finish())
}

/// Serve `file_url`'s arrow schema from the cache, running `load` on a miss.
///
/// `load` is the uncached footer read. It runs at most once per key at a time
/// (single-flight), so N concurrent first reads of one file cost one fetch.
///
/// With the cache disabled this is exactly `load()`, counted as neither a hit
/// nor a miss.
pub(crate) async fn get_or_load<F, Fut>(
    file_url: &Url,
    options: &HashMap<String, String>,
    load: F,
) -> Result<SchemaRef>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<SchemaRef>>,
{
    if !enabled() {
        return load().await;
    }
    let key = cache_key(file_url, options);
    // Whether THIS caller ran the loader. A caller that coalesced onto another's
    // in-flight load issues no round trip either, so it counts as a hit.
    let loaded = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let loader_ran = loaded.clone();
    let result = CACHE
        .try_get_with(key, async move {
            loader_ran.store(true, Ordering::Relaxed);
            Ok::<SchemaRef, StorageError>(intern(load().await?))
        })
        .await;
    match result {
        Ok(schema) => {
            // Count the miss only on a successful load, so failed reads
            // (missing / corrupt file) do not deflate the hit rate.
            if loaded.load(Ordering::Relaxed) {
                MISSES.fetch_add(1, Ordering::Relaxed);
            } else {
                HITS.fetch_add(1, Ordering::Relaxed);
            }
            Ok(schema)
        }
        // `try_get_with` shares one load error across all coalesced waiters as
        // an `Arc`; unwrap it when we are the sole holder, so that caller keeps
        // the original error's variant and kind. A waiter that cannot reclaim
        // the `Arc` by value gets `CoalescedLoadFailed` instead of a remapped
        // variant — the original error's kind does not survive being shared, and
        // re-wrapping into e.g. `ParquetError` would assert a failure category
        // that may be wrong (a `NotFound` masquerading as a corrupt footer).
        //
        // Which arm a given caller takes is NOT deterministic, and callers
        // should not branch on it. Measured on moka 0.12: a lone sequential
        // caller always takes the degraded arm, because moka holds its own clone
        // of the error `Arc` while returning it; under concurrent waiters one of
        // them does reclaim it and sees the original variant. So the contract is
        // "the original variant OR `CoalescedLoadFailed`, always carrying the
        // original message, never a third variant" — which is what the two tests
        // below pin, one per shape.
        Err(shared) => Err(match Arc::try_unwrap(shared) {
            Ok(e) => e,
            Err(arc) => StorageError::CoalescedLoadFailed(arc.to_string()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use serial_test::serial;
    use std::sync::atomic::AtomicUsize;

    // The cache, the interner and the counters are process-wide, and one test
    // here calls `clear()`. Every test that touches that shared state is
    // `#[serial]`, so a counter delta or an interned `Arc` cannot be perturbed
    // by a neighbour. The pure-parsing and key tests need no such guard.

    fn schema_of(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]))
    }

    fn url(path: &str) -> Url {
        Url::parse(path).unwrap()
    }

    #[test]
    fn enabled_parses_only_the_documented_off_spellings() {
        for off in ["false", "FALSE", "0", "off", "No", "  false  "] {
            assert!(!parse_enabled(Some(off)), "{off:?} must disable the cache");
        }
        for on in ["true", "1", "yes", "", "anything"] {
            assert!(parse_enabled(Some(on)), "{on:?} must leave it enabled");
        }
        assert!(parse_enabled(None), "unset means enabled");
    }

    #[test]
    fn max_entries_falls_back_to_the_default_on_junk() {
        assert_eq!(parse_max_entries(None), DEFAULT_MAX_ENTRIES);
        assert_eq!(parse_max_entries(Some(" 42 ")), 42);
        assert_eq!(parse_max_entries(Some("banana")), DEFAULT_MAX_ENTRIES);
        assert_eq!(parse_max_entries(Some("-1")), DEFAULT_MAX_ENTRIES);
    }

    /// The key separates files, and separates the same file under different
    /// store options — the second is what stops one endpoint's schema being
    /// served for another's file.
    #[test]
    fn the_key_separates_files_and_store_identities() {
        let a = url("s3://bucket/t/a.parquet");
        let b = url("s3://bucket/t/b.parquet");
        let plain = HashMap::new();
        let custom: HashMap<String, String> =
            [("endpoint".to_string(), "http://minio:9000".to_string())]
                .into_iter()
                .collect();

        assert_ne!(
            cache_key(&a, &plain),
            cache_key(&b, &plain),
            "file identity"
        );
        assert_ne!(
            cache_key(&a, &plain),
            cache_key(&a, &custom),
            "store identity"
        );
        assert_eq!(
            cache_key(&a, &plain),
            cache_key(&a, &HashMap::new()),
            "equal option maps must key the same"
        );
    }

    /// Equal option maps built in different insertion orders must key the same.
    /// `HashMap` iteration order is not stable, so hashing it directly would
    /// make the key depend on how the map was built — every `Storage` would
    /// miss on files another had already cached.
    #[test]
    fn the_key_does_not_depend_on_option_insertion_order() {
        let file = url("s3://bucket/t/a.parquet");
        let forward: HashMap<String, String> = [
            ("aws_region".to_string(), "us-west-2".to_string()),
            ("endpoint".to_string(), "http://minio:9000".to_string()),
        ]
        .into_iter()
        .collect();
        let reverse: HashMap<String, String> = [
            ("endpoint".to_string(), "http://minio:9000".to_string()),
            ("aws_region".to_string(), "us-west-2".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(cache_key(&file, &forward), cache_key(&file, &reverse));
    }

    /// The credential values must not appear in the key: it is copied into up to
    /// `max_entries` cache entries and into anything that logs one.
    #[test]
    fn the_key_never_embeds_option_values() {
        let file = url("s3://bucket/t/a.parquet");
        let secret = "AKIAsupersecretvalue";
        let options: HashMap<String, String> = [
            ("aws_secret_access_key".to_string(), secret.to_string()),
            ("aws_region".to_string(), "us-west-2".to_string()),
        ]
        .into_iter()
        .collect();
        let key = cache_key(&file, &options);
        assert!(
            !key.contains(secret),
            "key must not carry the secret: {key}"
        );
        assert!(!key.contains("aws_region"), "nor the option names: {key}");
        assert!(key.starts_with(file.as_str()), "but does carry the URI");
    }

    /// A second read of one file is served without re-running the loader, and
    /// the counters say so.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn a_warm_read_does_not_reload_and_is_counted_as_a_hit() {
        let file = url("s3://bucket/t/warm-read-fixture.parquet");
        let options = HashMap::new();
        let loads = AtomicUsize::new(0);
        let load = || async {
            loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(schema_of("id"))
        };

        let cold = get_or_load(&file, &options, load).await.unwrap();
        let warm = get_or_load(&file, &options, load).await.unwrap();

        assert_eq!(
            loads.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the warm read must not run the loader again"
        );
        assert_eq!(cold, warm, "both reads return the same schema");
        assert!(
            Arc::ptr_eq(&cold, &warm),
            "the warm read returns the interned Arc, not a fresh allocation"
        );
        // The hit/miss counters are process-global and every test in this binary
        // that reads a base file moves them, so an exact delta here is a race
        // that would pass almost always. That accounting is asserted exactly in
        // `tests/parquet_schema_cache_stats.rs`, which gets its own process.
    }

    /// A failed load is not cached, is counted as neither a hit nor a miss (so a
    /// run of missing files cannot make the hit rate look terrible), and
    /// surfaces as [`StorageError::CoalescedLoadFailed`] carrying the original
    /// message.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn a_failed_load_surfaces_as_coalesced_and_is_never_cached() {
        let file = url("s3://bucket/t/failed-load-fixture.parquet");
        let options = HashMap::new();
        let attempts = AtomicUsize::new(0);
        let failing = || async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(StorageError::InvalidPath("no such file".to_string()))
        };

        let first = get_or_load(&file, &options, failing).await;
        let second = get_or_load(&file, &options, failing).await;

        // Pinned to the one arm rather than "either is fine", because with a
        // single caller it IS deterministic: moka holds its own clone of the
        // error `Arc` while returning it, so `Arc::try_unwrap` fails and the
        // degraded arm is taken every time. Accepting both here would make the
        // assertion unfailable, and it would stop noticing the degraded variant
        // being swapped for a re-attributed one — the entire reason
        // `CoalescedLoadFailed` exists. (Under concurrency one waiter does
        // reclaim it; that shape is
        // `concurrent_failed_loads_never_misattribute_the_error_kind`.)
        for (label, result) in [("first", first), ("second", second)] {
            match result.unwrap_err() {
                StorageError::CoalescedLoadFailed(msg) => assert!(
                    msg.contains("no such file"),
                    "{label}: the original error's message must survive: {msg}"
                ),
                other => panic!(
                    "{label}: a lone failed load must surface as \
                     CoalescedLoadFailed carrying the original message: {other:?}"
                ),
            }
        }
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a failure must not be cached — the second read retries"
        );
        // "a failure counts as neither a hit nor a miss" needs the global
        // counters; see `tests/parquet_schema_cache_stats.rs`.
    }

    /// Identical schemas from different files share one `Arc`. This is what
    /// bounds the cache by distinct schemas rather than by entries.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn identical_schemas_from_different_files_are_interned() {
        let options = HashMap::new();
        let a = get_or_load(&url("s3://bucket/t/intern-a.parquet"), &options, || async {
            Ok(schema_of("shared_col"))
        })
        .await
        .unwrap();
        let b = get_or_load(&url("s3://bucket/t/intern-b.parquet"), &options, || async {
            Ok(schema_of("shared_col"))
        })
        .await
        .unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "two files with the same schema must share one Arc"
        );
    }

    /// Concurrent cold reads of one key coalesce into a single load, and the
    /// coalesced waiters count as hits because they issued no round trip.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn concurrent_cold_reads_of_one_key_load_once() {
        let file = url("s3://bucket/t/single-flight-fixture.parquet");
        let options: HashMap<String, String> = HashMap::new();
        let loads = Arc::new(AtomicUsize::new(0));

        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let file = file.clone();
                let options = options.clone();
                let loads = loads.clone();
                tokio::spawn(async move {
                    get_or_load(&file, &options, || async move {
                        loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Hold the flight open so the others have to coalesce
                        // rather than each find a populated entry.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        Ok(schema_of("id"))
                    })
                    .await
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(
            loads.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "8 concurrent cold reads of one key must cost one footer fetch"
        );
    }

    /// `clear()` drops the entries, so the next read reloads. The escape hatch
    /// for a caller that overwrote a file in place.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clear_forces_the_next_read_to_reload() {
        let file = url("s3://bucket/t/clear-fixture.parquet");
        let options = HashMap::new();
        let loads = AtomicUsize::new(0);
        let load = || async {
            loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(schema_of("id"))
        };

        get_or_load(&file, &options, load).await.unwrap();
        clear();
        // moka applies invalidation asynchronously; run its pending tasks so the
        // assertion below is about the cache and not about the timing.
        CACHE.run_pending_tasks().await;
        get_or_load(&file, &options, load).await.unwrap();
        assert_eq!(
            loads.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "after clear() the footer is read again"
        );
    }
    /// Concurrent cold reads of a failing key all fail, coalesced onto one load,
    /// and every waiter's error carries the original message rather than a
    /// re-attributed variant.
    ///
    /// The sequential test above pins one caller, deterministically. This is the
    /// shape the degraded variant was introduced for — N waiters sharing one
    /// error `Arc`, where at most one can reclaim it — and here BOTH arms are
    /// legitimately reachable, so the assertion is the one that matters: never a
    /// third variant, and the original message always survives. A missing file
    /// reported as a corrupt footer would send its handler down the wrong path.
    /// Mirrors #127's
    /// `concurrent_cold_reads_of_missing_file_never_misattribute_the_error_kind`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn concurrent_failed_loads_never_misattribute_the_error_kind() {
        let file = url("s3://bucket/t/concurrent-failure-fixture.parquet");
        let options: HashMap<String, String> = HashMap::new();
        let attempts = Arc::new(AtomicUsize::new(0));

        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let file = file.clone();
                let options = options.clone();
                let attempts = attempts.clone();
                tokio::spawn(async move {
                    get_or_load(&file, &options, || async move {
                        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Hold the flight open so the others coalesce onto this
                        // failure rather than each starting their own.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        Err(StorageError::InvalidPath("no such file".to_string()))
                    })
                    .await
                })
            })
            .collect();

        for task in tasks {
            // Either arm is correct here — whichever waiter reclaims the sole
            // reference keeps the original variant, the rest are degraded — but
            // no other variant is, and the message must survive either way.
            match task.await.unwrap().unwrap_err() {
                StorageError::InvalidPath(msg) | StorageError::CoalescedLoadFailed(msg) => {
                    assert!(
                        msg.contains("no such file"),
                        "the original message must survive being shared: {msg}"
                    )
                }
                other => panic!(
                    "a coalesced failure must never be re-attributed to a \
                     different concrete variant — a missing file reported as, \
                     say, a corrupt footer sends its handler down the wrong \
                     path: {other:?}"
                ),
            }
        }
        assert!(
            attempts.load(std::sync::atomic::Ordering::SeqCst) < 8,
            "the waiters must have coalesced, or this asserts nothing about sharing"
        );
    }
}
