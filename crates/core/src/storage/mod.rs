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
//! This module is responsible for interacting with the storage layer.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use once_cell::sync::Lazy;

use async_recursion::async_recursion;
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme, parse_url_opts};
use url::Url;

use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;

use crate::storage::error::Result;
use crate::storage::error::StorageError::{self, Creation, InvalidPath};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::reader::StorageReader;
use crate::storage::util::join_url_segments;

#[cfg(test)]
pub(crate) mod counting;
pub mod error;
pub mod file_metadata;
pub mod reader;
pub mod util;

/// Builds a parquet `RowFilter` for a read, given the file's parquet schema and
/// the Arrow schema it maps to. Returning `None` means no filter is pushed.
///
/// `Arc` rather than `Box` so options holding it stay `Clone`. The captured
/// state must be `Send + Sync` because the parquet stream may evaluate the
/// filter on any worker thread.
pub type RowFilterBuilder = Arc<
    dyn Fn(
            &parquet::schema::types::SchemaDescriptor,
            &arrow_schema::Schema,
        ) -> Option<parquet::arrow::arrow_reader::RowFilter>
        + Send
        + Sync,
>;

/// Chooses which row groups a read fetches, from the file's parsed footer.
/// Returning `None` means "no opinion, read them all".
///
/// This is the only mechanism on the read path that can avoid reading bytes: a
/// [`RowFilterBuilder`] decides per row after the predicate columns are decoded,
/// so it saves decode, never IO, while a row group excluded here is never
/// fetched.
///
/// The selector must be CONSERVATIVE. Keeping a row group that cannot match
/// costs only time; dropping one that can match silently loses rows, and nothing
/// downstream can restore them.
///
/// `Arc` for the same reason as [`RowFilterBuilder`]: options holding it stay
/// `Clone`, and it may run on any worker thread.
pub type RowGroupSelector =
    Arc<dyn Fn(&parquet::file::metadata::ParquetMetaData) -> Option<Vec<usize>> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct Storage {
    pub(crate) base_url: Arc<Url>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) options: Arc<HashMap<String, String>>,
    pub(crate) hudi_configs: Arc<HudiConfigs>,
    /// Read-volume counters for the base-file reads made through this
    /// `Storage`.
    ///
    /// Here rather than on a read parameter so no read signature changes. The
    /// scope is this `Storage`'s lifetime, which is as narrow as the caller
    /// makes it: a per-read `Storage` yields per-read counters, while a
    /// `FileGroupReader` builds one `Storage` in its constructor and reuses
    /// it, so every slice read through that reader accumulates into the same
    /// counters. The `object_store` inside could not carry them: it is shared
    /// even wider, so its counts would be everyone's.
    pub(crate) read_volume: Arc<ReadVolume>,
}

/// Read-volume counters for one [`Storage`]'s lifetime.
///
/// Whether a predicate was pushed is not the same question as what a push
/// bought: a parquet `RowFilter` can be installed on every file and still read
/// every byte, because it decides per row after the predicate columns are
/// decoded. Only pruning avoids IO. These counters separate the two.
///
/// `bytes_read` and `io_calls` are counted at the `AsyncFileReader` boundary,
/// which makes them exact and independent of the OS page cache: a warm re-read
/// reports the same bytes as a cold one. Wall-clock does not have that property,
/// which is what makes these the transferable numbers when comparing read paths.
/// The boundary also bounds the scope: footer fetches happen before the
/// counting wrapper is installed and log-file IO never crosses it, so these
/// two count base-file column-chunk reads only.
///
/// All fields are `AtomicU64` under an `Arc` because the parquet stream is
/// polled on whichever worker thread drives it, while a consumer may read the
/// counters from another. `Relaxed` throughout: these are advisory counters, and
/// the happens-before that makes them visible is the consumer draining the
/// stream.
#[derive(Debug, Default)]
pub struct ReadVolume {
    /// Bytes actually fetched from the object store, summed over every range read.
    pub bytes_read: AtomicU64,
    /// Number of `get_bytes` / `get_byte_ranges` calls — round trips, not ranges.
    /// A two-pass read (predicate columns, then the selected rows) shows up here
    /// as roughly double the calls of a single-pass read over the same file.
    pub io_calls: AtomicU64,
    /// Row groups the reader was configured to scan. Equal to `file_row_groups`
    /// until something prunes; the gap between the two is what pruning bought.
    pub row_groups_read: AtomicU64,
    /// Row groups the file contains. Denominator for the line above.
    pub file_row_groups: AtomicU64,
    /// Times the row-group selector closure actually RAN.
    ///
    /// Separate from its outcome on purpose. A selector returns `None` when it
    /// cannot prune anything, so `row_groups_read == file_row_groups` reads the
    /// same whether the selector ran and found nothing or was never installed.
    /// Only this counter separates them.
    pub row_group_selector_calls: AtomicU64,
    /// Times a selector WAS installed by the caller but a gate refused to pass it
    /// down — either the merge-safety gate or a value-reinterpreting logical-type
    /// repair on the file.
    ///
    /// Without this a gate silently defeats the counter above: a suppressed
    /// selector is a third state that also reads zero calls.
    ///
    /// Counted regardless of cause, so it stays a faithful answer to "was one
    /// installed but not passed down"; [`Self::pushdown_suppressed_by_repair`]
    /// says which cause. Read them together:
    ///   calls > 0                   the selector ran
    ///   calls == 0, suppressed > 0  a gate refused it; by_repair == 0 means the
    ///                               merge-safety gate (the read merges, and the
    ///                               predicate is not primary-key-safe),
    ///                               by_repair > 0 means a repair conflict
    ///   calls == 0, suppressed == 0 no caller ever installed one
    pub row_group_selector_suppressed: AtomicU64,
    /// Base files where a pushed predicate read a column THIS file mislabels, so
    /// every pushdown mechanism was withdrawn for it.
    ///
    /// Counted once per such file whether or not a selector was installed, so
    /// unlike [`Self::row_group_selector_suppressed`] it also covers the
    /// row-filter side.
    ///
    /// Non-zero is expected on a table with legacy `parquet-mr` base files and a
    /// predicate over a tz-aware millis column: those files fell back to the
    /// post-scan filter, they did not lose rows. Rising on a table that should be
    /// all-micros is the signal worth chasing.
    pub pushdown_suppressed_by_repair: AtomicU64,
    /// Rows the file contains, from parquet metadata.
    pub file_rows: AtomicU64,
    /// Rows the stream actually yielded, after any row filter. `file_rows -
    /// rows_out` is what filtering removed; `bytes_read` says what it cost to
    /// remove it.
    pub rows_out: AtomicU64,
}

impl ReadVolume {
    /// One completed fetch: its bytes, and the round trip that carried them.
    pub(crate) fn add_bytes(&self, n: u64) {
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
        self.io_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// What the file holds, read off the footer the reader has already fetched.
    pub(crate) fn record_file_shape(&self, row_groups: u64, rows: u64) {
        self.file_row_groups
            .fetch_add(row_groups, Ordering::Relaxed);
        self.file_rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub(crate) fn add_row_groups_read(&self, n: u64) {
        self.row_groups_read.fetch_add(n, Ordering::Relaxed);
    }

    /// The selector ran. Counted whether or not it managed to prune.
    pub(crate) fn record_selector_call(&self) {
        self.row_group_selector_calls
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A caller installed a selector and the safety gate declined to pass it on.
    pub(crate) fn record_selector_suppressed(&self) {
        self.row_group_selector_suppressed
            .fetch_add(1, Ordering::Relaxed);
    }

    /// This base file mislabels a column the pushed predicate reads, so every
    /// pushdown mechanism was withdrawn for it. Counted separately from
    /// `record_selector_suppressed` because it fires with no selector installed
    /// too, and because the two causes must stay distinguishable.
    pub(crate) fn record_pushdown_suppressed_by_repair(&self) {
        self.pushdown_suppressed_by_repair
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_rows_out(&self, n: u64) {
        self.rows_out.fetch_add(n, Ordering::Relaxed);
    }
}

/// ENG-42276 — process-wide cache of built `ObjectStore`s.
///
/// `parse_url_opts` builds a fresh client per call, and for S3 that means a new
/// credential chain and a new TLS connection pool. Embedders construct a
/// `Storage` PER FILE GROUP (see `cpp/src/lib.rs`), so on a scan of N splits the
/// uncached path pays that N times and shares no connections between them.
///
/// Keyed by the store-identifying part of the URL plus the option set, so two
/// stores that differ in bucket, container, endpoint or credentials never share
/// an entry — see [`object_store_cache_key`].
///
/// Caveat, recorded deliberately: this map is unbounded and lives for the
/// process. That is bounded in practice by the number of DISTINCT
/// (host, options) pairs a process sees, which is small — but a caller that
/// mints per-request credentials would grow it without limit. Nothing here
/// evicts, matching internal main; if that ever becomes a problem the fix is an
/// entry-bounded cache, not a per-split rebuild.
static OBJECT_STORE_CACHE: Lazy<Mutex<HashMap<String, Arc<dyn ObjectStore>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Identity of a built store: the part of `base_url` the store is bound to,
/// plus the option set in a stable order.
///
/// The URL part is everything up to the path, plus whatever leading path
/// segments `parse_url_opts` consumes rather than hands back as the object
/// path. That is the derivation `object_store`'s own `DefaultObjectStoreRegistry`
/// uses, and it matters because the bucket or container is not always the host:
///
/// - `abfss://container@account.dfs.core.windows.net/tbl` carries the container
///   in the user-info, so scheme+host alone is the same for every container;
/// - `https://account.dfs.core.windows.net/container/tbl`, path-style
///   `https://s3.<region>.amazonaws.com/bucket/tbl` and R2 carry it in the first
///   path segment, which `parse_url_opts` strips.
///
/// A key that missed either would hand one container's client to a read of
/// another, which then resolves the same relative path inside the wrong
/// container and returns its data without error.
///
/// The option set is load-bearing too — the same `s3://bucket/path` resolves
/// to different physical stores under different endpoints or credentials, so a
/// URL-only key would hand one endpoint's client to another's read.
fn object_store_cache_key(base_url: &Url, options: &HashMap<String, String>) -> String {
    let url_part = match ObjectStoreScheme::parse(base_url) {
        Ok((_, path)) => {
            let segments = || base_url.path().split('/').filter(|s| !s.is_empty());
            let consumed = segments().count().saturating_sub(path.parts_count());
            let mut url_part = base_url[..url::Position::AfterPort].to_string();
            for segment in segments().take(consumed) {
                url_part.push('/');
                url_part.push_str(segment);
            }
            url_part
        }
        // Unrecognised: `parse_url_opts` refuses it too, so nothing is cached
        // under this key; the whole URL is the conservative identity.
        Err(_) => base_url.as_str().to_string(),
    };
    let mut opts: Vec<(&String, &String)> = options.iter().collect();
    opts.sort();
    format!("{url_part}|{opts:?}")
}

impl Storage {
    pub const CLOUD_STORAGE_PREFIXES: [&'static str; 3] = ["AWS_", "AZURE_", "GOOGLE_"];

    pub fn new(
        options: Arc<HashMap<String, String>>,
        hudi_configs: Arc<HudiConfigs>,
    ) -> Result<Arc<Storage>> {
        let base_url = match hudi_configs
            .try_get(HudiTableConfig::BasePath)
            .map_err(|e| Creation(format!("{e}")))?
        {
            Some(v) => v.to_url()?,
            None => {
                return Err(Creation(format!(
                    "{} is required.",
                    HudiTableConfig::BasePath.as_ref()
                )));
            }
        };

        let options = Self::with_region_fallback(&base_url, options);

        // ENG-42276 — consult the process-level store cache before building a
        // new client. See OBJECT_STORE_CACHE.
        let key = object_store_cache_key(&base_url, options.as_ref());
        let object_store: Arc<dyn ObjectStore> = {
            let mut cache = OBJECT_STORE_CACHE
                .lock()
                .expect("OBJECT_STORE_CACHE mutex poisoned");
            if let Some(existing) = cache.get(&key) {
                existing.clone()
            } else {
                // Bind hyper's dispatch task to the process-lifetime runtime
                // rather than the caller's per-task one: a cached store whose
                // dispatcher died with a transient runtime fails every later
                // read with `DispatchGone`.
                let _guard = crate::ffi_support::OBJECT_STORE_RUNTIME.enter();
                match parse_url_opts(&base_url, options.as_ref()) {
                    Ok((new_store, _)) => {
                        let arc: Arc<dyn ObjectStore> = Arc::new(new_store);
                        cache.insert(key, arc.clone());
                        arc
                    }
                    Err(e) => return Err(Creation(format!("Failed to create storage: {e}"))),
                }
            }
        };

        Ok(Arc::new(Storage {
            base_url: Arc::new(base_url),
            object_store,
            options,
            hudi_configs,
            read_volume: Arc::new(ReadVolume::default()),
        }))
    }

    /// Clone of this `Storage`'s read-volume counters, for a consumer that
    /// outlives the read and reports them once the stream has drained.
    pub fn read_volume(&self) -> Arc<ReadVolume> {
        self.read_volume.clone()
    }

    /// Fall back to `AWS_REGION` / `AWS_DEFAULT_REGION` for S3 URLs when the
    /// caller passed no region.
    ///
    /// Without this, `object_store::parse_url_opts` builds an `AmazonS3` client
    /// against the default us-east-1 endpoint, and a HEAD to a bucket in any
    /// other region fails with `BareRedirect`. Spark/EKS expose the region via
    /// `AWS_REGION` (set by IRSA, or by `spark.executorEnv.AWS_REGION`), so
    /// honouring it here means callers need not thread a region through the FFI
    /// props map.
    ///
    /// Load-bearing only for options that did not pass through
    /// `OptionResolver`, such as the FFI's, which it builds from its own props.
    /// `Table::new` and `FileGroupReader::new_with_options` resolve options
    /// first, and that copies every `AWS_*` environment variable into the
    /// storage options (lowercased), so on those paths the map already carries
    /// `aws_region` or `aws_default_region` whenever the environment does, and
    /// this returns early.
    ///
    /// Any spelling of a region key counts as the caller passing one:
    /// `object_store` lowercases every key before parsing it, so `AWS_REGION`
    /// in the map is as explicit as `region`, and `aws_default_region` /
    /// `default_region` also set the region. Injecting `region` beside any of
    /// them would leave `object_store` resolving two region settings in
    /// `HashMap` order, which is a different answer from one run to the next.
    ///
    /// Returns the SAME `Arc` when nothing applies, so the common path neither
    /// copies the map nor touches the environment.
    fn with_region_fallback(
        base_url: &Url,
        options: Arc<HashMap<String, String>>,
    ) -> Arc<HashMap<String, String>> {
        let scheme = base_url.scheme();
        if scheme != "s3" && scheme != "s3a" {
            return options;
        }
        let has_region = options.keys().any(|k| {
            matches!(
                k.to_ascii_lowercase().as_str(),
                "region" | "aws_region" | "default_region" | "aws_default_region"
            )
        });
        if has_region {
            return options;
        }
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .ok();
        let Some(region) = region else { return options };
        if region.is_empty() {
            return options;
        }
        // debug!, not info!: embedders construct a Storage per file group, so on
        // an s3 table whose region arrives only from the environment this fires
        // once per split rather than once per process.
        log::debug!("hudi-rs Storage: injecting region={region} from env for {scheme} url");
        let mut merged: HashMap<String, String> = (*options).clone();
        merged.insert("region".to_string(), region);
        Arc::new(merged)
    }

    /// Build storage over a caller-supplied object store.
    ///
    /// Test-only, so a test can wrap the real store and observe the requests a
    /// reader makes. Note that a wrapper takes the trait's default `get_ranges`,
    /// which coalesces, where `LocalFileSystem` overrides it and does not: the
    /// counts a test sees are therefore the ones an object store would serve, not
    /// the ones the local filesystem would.
    #[cfg(test)]
    pub(crate) fn new_with_object_store(
        base_url: Url,
        object_store: Arc<dyn ObjectStore>,
        hudi_configs: Arc<HudiConfigs>,
    ) -> Arc<Storage> {
        Arc::new(Storage {
            base_url: Arc::new(base_url),
            object_store,
            options: Arc::new(HashMap::new()),
            hudi_configs,
            read_volume: Arc::new(ReadVolume::default()),
        })
    }

    #[cfg(test)]
    pub fn new_with_base_url(base_url: Url) -> Result<Arc<Storage>> {
        let mut hudi_options = HashMap::new();
        hudi_options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_url.as_str().to_string(),
        );
        Self::new(
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::new(hudi_options)),
        )
    }

    #[cfg(feature = "datafusion")]
    pub fn register_object_store(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) {
        runtime_env.register_object_store(self.base_url.as_ref(), self.object_store.clone());
    }

    #[cfg(test)]
    /// Get basic file metadata (name, size) without loading the file content.
    async fn get_file_metadata_not_populated(&self, relative_path: &str) -> Result<FileMetadata> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let meta = self.object_store.head(&obj_path).await?;
        let name = meta.location.filename().ok_or_else(|| {
            InvalidPath(format!("Failed to get file name from: {:?}", meta.location))
        })?;
        Ok(FileMetadata::new(name.to_string(), meta.size))
    }

    pub async fn get_file_data(&self, relative_path: &str) -> Result<Bytes> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let result = self.object_store.get(&obj_path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    pub async fn get_file_data_from_absolute_path(&self, absolute_path: &str) -> Result<Bytes> {
        let obj_path = ObjPath::from_absolute_path(PathBuf::from(absolute_path))?;
        let result = self.object_store.get(&obj_path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    pub async fn get_storage_reader(&self, relative_path: &str) -> Result<StorageReader> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let obj_meta = obj_store.head(&obj_path).await?;
        StorageReader::new(obj_store, obj_meta)
            .await
            .map_err(StorageError::ReaderError)
    }

    /// A reader that fetches bounded windows instead of the whole file.
    ///
    /// Only the object metadata is fetched here; no file bytes are read until
    /// the caller reads.
    pub async fn get_streaming_storage_reader(&self, relative_path: &str) -> Result<StorageReader> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let obj_meta = obj_store.head(&obj_path).await?;
        let window_size = crate::storage::reader::stream_window_size(&self.hudi_configs)
            .map_err(StorageError::ReaderError)?;
        Ok(StorageReader::new_streaming(
            obj_store,
            obj_meta,
            window_size,
        ))
    }

    pub async fn list_dirs(&self, subdir: Option<&str>) -> Result<Vec<String>> {
        let dir_paths = self.list_dirs_as_obj_paths(subdir).await?;
        let mut dirs = Vec::new();
        for dir in dir_paths {
            dirs.push(
                dir.filename()
                    .ok_or_else(|| InvalidPath(format!("Failed to get file name from: {dir:?}")))?
                    .to_string(),
            )
        }
        Ok(dirs)
    }

    async fn list_dirs_as_obj_paths(&self, subdir: Option<&str>) -> Result<Vec<ObjPath>> {
        let prefix_url = join_url_segments(&self.base_url, &[subdir.unwrap_or_default()])?;
        let prefix_path = ObjPath::from_url_path(prefix_url.path())?;
        let list_res = self
            .object_store
            .list_with_delimiter(Some(&prefix_path))
            .await?;
        Ok(list_res.common_prefixes)
    }

    pub async fn list_files(&self, subdir: Option<&str>) -> Result<Vec<FileMetadata>> {
        let prefix_url = join_url_segments(&self.base_url, &[subdir.unwrap_or_default()])?;
        let prefix_path = ObjPath::from_url_path(prefix_url.path())?;
        let list_res = self
            .object_store
            .list_with_delimiter(Some(&prefix_path))
            .await?;
        let mut file_metadata = Vec::new();
        for obj_meta in list_res.objects {
            let location = obj_meta.location;
            let name = location
                .filename()
                .ok_or_else(|| InvalidPath(format!("Failed to get file name from {location:?}")))?;

            if name.ends_with(".crc") {
                continue;
            }

            file_metadata.push(FileMetadata::new(name.to_string(), obj_meta.size));
        }
        Ok(file_metadata)
    }
}

/// Get relative paths of leaf directories under a given directory.
///
/// **Example**
/// - /usr/hudi/table_name
/// - /usr/hudi/table_name/.hoodie
/// - /usr/hudi/table_name/dt=2024/month=01/day=01
/// - /usr/hudi/table_name/dt=2025/month=02
///
/// the result is \[".hoodie", "dt=2024/mont=01/day=01", "dt=2025/month=02"\]
#[async_recursion]
pub async fn get_leaf_dirs(storage: &Storage, subdir: Option<&str>) -> Result<Vec<String>> {
    let mut leaf_dirs = Vec::new();
    let child_dirs = storage.list_dirs(subdir).await?;
    if child_dirs.is_empty() {
        leaf_dirs.push(subdir.unwrap_or_default().to_owned());
    } else {
        for child_dir in child_dirs {
            let mut next_subdir = PathBuf::new();
            if let Some(curr) = subdir {
                next_subdir.push(curr);
            }
            next_subdir.push(child_dir);
            let next_subdir = next_subdir
                .to_str()
                .ok_or_else(|| InvalidPath(format!("Failed to convert path: {next_subdir:?}")))?;
            let curr_leaf_dir = get_leaf_dirs(storage, Some(next_subdir)).await?;
            leaf_dirs.extend(curr_leaf_dir);
        }
    }
    Ok(leaf_dirs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::collections::HashSet;
    use std::fs::canonicalize;
    use std::path::Path;

    // ── ENG-40156 — with_region_fallback ──────────────────────────────
    //
    // These tests cover the env-driven region injection. Tests that touch
    // process env are marked `#[serial(env_vars)]` so concurrent execution
    // doesn't clobber state. The env-var manipulations are inside `unsafe`
    // blocks per the 2024-edition std::env safety rules.
    //
    // The fallback semantics under test:
    //   - non-S3 schemes  → options returned unchanged.
    //   - already-set `region`/`aws_region` → never overridden.
    //   - S3 URL + AWS_REGION env set → `region` injected.
    //   - S3 URL + only AWS_DEFAULT_REGION set → `region` injected.
    //   - S3 URL + no env / empty env value → options returned unchanged.

    fn s3_url() -> Url {
        Url::parse("s3://example-bucket/path/").unwrap()
    }

    fn s3a_url() -> Url {
        Url::parse("s3a://example-bucket/path/").unwrap()
    }

    #[test]
    fn test_region_fallback_non_s3_scheme_is_passthrough() {
        let in_opts = Arc::new(HashMap::from([(
            "some_key".to_string(),
            "some_val".to_string(),
        )]));
        let url = Url::parse("file:///tmp/path/").unwrap();
        let out = Storage::with_region_fallback(&url, in_opts.clone());

        // Same Arc — no copy, no mutation.
        assert!(Arc::ptr_eq(&in_opts, &out));
        assert!(!out.contains_key("region"));
    }

    #[test]
    fn test_region_fallback_preserves_explicit_region() {
        let in_opts = Arc::new(HashMap::from([(
            "region".to_string(),
            "ap-south-1".to_string(),
        )]));
        let out = Storage::with_region_fallback(&s3_url(), in_opts.clone());

        // Caller-supplied region wins; we don't even peek at env.
        assert!(Arc::ptr_eq(&in_opts, &out));
        assert_eq!(out.get("region"), Some(&"ap-south-1".to_string()));
    }

    #[test]
    fn test_region_fallback_preserves_explicit_aws_region_alias() {
        let in_opts = Arc::new(HashMap::from([(
            "aws_region".to_string(),
            "eu-central-1".to_string(),
        )]));
        let out = Storage::with_region_fallback(&s3_url(), in_opts.clone());

        // `aws_region` is the alias object_store accepts — also respected.
        assert!(Arc::ptr_eq(&in_opts, &out));
        assert!(!out.contains_key("region"));
        assert_eq!(out.get("aws_region"), Some(&"eu-central-1".to_string()));
    }

    #[test]
    #[serial(env_vars)]
    fn test_region_fallback_injects_from_aws_region_env() {
        unsafe {
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
            std::env::set_var("AWS_REGION", "us-west-2");
        }
        let in_opts = Arc::new(HashMap::new());
        let out = Storage::with_region_fallback(&s3_url(), in_opts.clone());

        assert_eq!(out.get("region"), Some(&"us-west-2".to_string()));
        // A new Arc was returned — not the same pointer as input.
        assert!(!Arc::ptr_eq(&in_opts, &out));

        unsafe {
            std::env::remove_var("AWS_REGION");
        }
    }

    #[test]
    #[serial(env_vars)]
    fn test_region_fallback_injects_from_aws_default_region_env() {
        unsafe {
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
            std::env::set_var("AWS_DEFAULT_REGION", "us-west-2");
        }
        let out = Storage::with_region_fallback(&s3_url(), Arc::new(HashMap::new()));

        // AWS_REGION takes priority when both set; here only DEFAULT is set.
        assert_eq!(out.get("region"), Some(&"us-west-2".to_string()));

        unsafe {
            std::env::remove_var("AWS_DEFAULT_REGION");
        }
    }

    #[test]
    #[serial(env_vars)]
    fn test_region_fallback_aws_region_wins_over_default_region() {
        unsafe {
            std::env::set_var("AWS_REGION", "us-west-2");
            std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        }
        let out = Storage::with_region_fallback(&s3_url(), Arc::new(HashMap::new()));

        assert_eq!(out.get("region"), Some(&"us-west-2".to_string()));

        unsafe {
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
        }
    }

    #[test]
    #[serial(env_vars)]
    fn test_region_fallback_no_env_is_passthrough() {
        unsafe {
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
        }
        let in_opts = Arc::new(HashMap::new());
        let out = Storage::with_region_fallback(&s3_url(), in_opts.clone());

        // No env, no mutation, no region inserted.
        assert!(Arc::ptr_eq(&in_opts, &out));
        assert!(!out.contains_key("region"));
    }

    #[test]
    #[serial(env_vars)]
    fn test_region_fallback_empty_env_value_is_passthrough() {
        unsafe {
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
            std::env::set_var("AWS_REGION", "");
        }
        let in_opts = Arc::new(HashMap::new());
        let out = Storage::with_region_fallback(&s3_url(), in_opts.clone());

        // Empty string in env shouldn't be propagated as a "region" key —
        // object_store would build an invalid endpoint URL otherwise.
        assert!(Arc::ptr_eq(&in_opts, &out));
        assert!(!out.contains_key("region"));

        unsafe {
            std::env::remove_var("AWS_REGION");
        }
    }

    #[test]
    #[serial(env_vars)]
    fn test_region_fallback_s3a_scheme_also_works() {
        unsafe {
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
            std::env::set_var("AWS_REGION", "us-west-2");
        }
        let out = Storage::with_region_fallback(&s3a_url(), Arc::new(HashMap::new()));

        // Hadoop-style `s3a://` URLs hit the same injection path.
        assert_eq!(out.get("region"), Some(&"us-west-2".to_string()));

        unsafe {
            std::env::remove_var("AWS_REGION");
        }
    }

    #[test]
    #[serial(env_vars)]
    fn test_region_fallback_respects_an_explicit_region_in_any_key_spelling() {
        // object_store lowercases every key before parsing it, and reads both
        // default-region spellings as a region, so each of these is the caller
        // passing a region. None may get an env region injected beside it.
        unsafe {
            std::env::remove_var("AWS_DEFAULT_REGION");
            std::env::set_var("AWS_REGION", "us-east-1");
        }
        for key in [
            "AWS_REGION",
            "Region",
            "REGION",
            "aws_default_region",
            "AWS_DEFAULT_REGION",
            "default_region",
        ] {
            let in_opts = Arc::new(HashMap::from([(key.to_string(), "eu-west-1".to_string())]));
            let out = Storage::with_region_fallback(&s3_url(), in_opts.clone());
            assert!(
                Arc::ptr_eq(&in_opts, &out),
                "an explicit `{key}` must not get `region` injected beside it, got {out:?}"
            );
        }
        unsafe {
            std::env::remove_var("AWS_REGION");
        }
    }

    #[test]
    #[serial(env_vars)]
    fn test_region_fallback_keeps_an_uppercase_explicit_region_deterministic() {
        // Fold the result through the key parsing `parse_url_opts` applies.
        // Each iteration builds a fresh HashMap, so each has its own iteration
        // order: an injected `region` beside `AWS_REGION` would make the
        // resolved region follow that order instead of the caller.
        unsafe {
            std::env::remove_var("AWS_DEFAULT_REGION");
            std::env::set_var("AWS_REGION", "us-east-1");
        }
        for _ in 0..64 {
            let in_opts = Arc::new(HashMap::from([(
                "AWS_REGION".to_string(),
                "eu-west-1".to_string(),
            )]));
            let out = Storage::with_region_fallback(&s3_url(), in_opts);
            let builder = out.iter().fold(
                object_store::aws::AmazonS3Builder::new(),
                |builder, (k, v)| match k
                    .to_ascii_lowercase()
                    .parse::<object_store::aws::AmazonS3ConfigKey>()
                {
                    Ok(key) => builder.with_config(key, v),
                    Err(_) => builder,
                },
            );
            assert_eq!(
                builder
                    .get_config_value(&object_store::aws::AmazonS3ConfigKey::Region)
                    .as_deref(),
                Some("eu-west-1"),
                "the caller's region must win over the environment, every time"
            );
        }
        unsafe {
            std::env::remove_var("AWS_REGION");
        }
    }

    // ── ENG-42276 — OBJECT_STORE_CACHE ────────────────────────────────
    //
    // Internal main ships this cache with no test at all. These are written
    // here rather than ported, because "the same store is reused" is exactly
    // the property the change exists for and nothing else pins it.

    #[test]
    fn test_object_store_cache_key_separates_distinct_option_sets() {
        let url = Url::parse("s3://example-bucket/path/").unwrap();
        let a = HashMap::from([("region".to_string(), "us-west-2".to_string())]);
        let b = HashMap::from([("region".to_string(), "eu-west-1".to_string())]);
        assert_ne!(
            object_store_cache_key(&url, &a),
            object_store_cache_key(&url, &b),
            "the same bucket under a different region is a different store"
        );
    }

    #[test]
    fn test_object_store_cache_key_is_order_independent() {
        // HashMap iteration order is arbitrary, so a key built from it must be
        // sorted or two identical option sets would miss each other's entry.
        let url = Url::parse("s3://example-bucket/path/").unwrap();
        let a = HashMap::from([
            ("region".to_string(), "us-west-2".to_string()),
            ("endpoint".to_string(), "http://x".to_string()),
        ]);
        let b = HashMap::from([
            ("endpoint".to_string(), "http://x".to_string()),
            ("region".to_string(), "us-west-2".to_string()),
        ]);
        assert_eq!(
            object_store_cache_key(&url, &a),
            object_store_cache_key(&url, &b)
        );
    }

    #[test]
    fn test_storage_new_reuses_one_object_store_per_identity() {
        // The point of the cache: an embedder builds a Storage PER FILE GROUP,
        // and every one of those must share a client rather than mint a new
        // credential chain and TLS pool.
        let base = canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap();
        let url = Url::from_directory_path(&base).unwrap();
        let mut opts = HashMap::new();
        opts.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            url.as_str().to_string(),
        );
        let configs = Arc::new(HudiConfigs::new(opts));

        let first = Storage::new(Arc::new(HashMap::new()), configs.clone()).unwrap();
        let second = Storage::new(Arc::new(HashMap::new()), configs).unwrap();

        assert!(
            Arc::ptr_eq(&first.object_store, &second.object_store),
            "two Storages over the same (host, options) must share one ObjectStore"
        );
    }

    #[test]
    fn test_object_store_cache_key_separates_stores_that_differ_only_in_bucket_or_container() {
        // Every pair names two different stores whose scheme and host are the
        // same: the bucket or container lives in the user-info or the first
        // path segment. Each pair must key apart, or the second read is served
        // by the first store's client.
        let no_options = HashMap::new();
        let pairs = [
            (
                "abfss://container-a@acct.dfs.core.windows.net/tbl",
                "abfss://container-b@acct.dfs.core.windows.net/tbl",
            ),
            (
                "abfs://container-a@acct.blob.core.windows.net/tbl",
                "abfs://container-b@acct.blob.core.windows.net/tbl",
            ),
            (
                "https://acct.dfs.core.windows.net/container-a/tbl",
                "https://acct.dfs.core.windows.net/container-b/tbl",
            ),
            (
                "https://acct.blob.core.windows.net/container-a/tbl",
                "https://acct.blob.core.windows.net/container-b/tbl",
            ),
            (
                "https://s3.us-west-2.amazonaws.com/bucket-a/tbl",
                "https://s3.us-west-2.amazonaws.com/bucket-b/tbl",
            ),
            (
                "https://acct.r2.cloudflarestorage.com/bucket-a/tbl",
                "https://acct.r2.cloudflarestorage.com/bucket-b/tbl",
            ),
            ("s3://bucket-a/tbl", "s3://bucket-b/tbl"),
        ];
        for (a, b) in pairs {
            assert_ne!(
                object_store_cache_key(&Url::parse(a).unwrap(), &no_options),
                object_store_cache_key(&Url::parse(b).unwrap(), &no_options),
                "{a} and {b} are different stores"
            );
        }
    }

    #[test]
    fn test_object_store_cache_key_is_shared_by_tables_in_one_store() {
        // The other direction: the table path inside a store is not part of
        // its identity, so tables in one bucket or container share a client.
        let no_options = HashMap::new();
        let pairs = [
            (
                "abfss://container@acct.dfs.core.windows.net/tbl-a",
                "abfss://container@acct.dfs.core.windows.net/db/tbl-b",
            ),
            (
                "https://acct.dfs.core.windows.net/container/tbl-a",
                "https://acct.dfs.core.windows.net/container/db/tbl-b",
            ),
            (
                "https://s3.us-west-2.amazonaws.com/bucket/tbl-a",
                "https://s3.us-west-2.amazonaws.com/bucket/db/tbl-b",
            ),
            ("s3://bucket/tbl-a", "s3://bucket/db/tbl-b"),
            ("file:///tmp/tbl-a", "file:///var/db/tbl-b"),
        ];
        for (a, b) in pairs {
            assert_eq!(
                object_store_cache_key(&Url::parse(a).unwrap(), &no_options),
                object_store_cache_key(&Url::parse(b).unwrap(), &no_options),
                "{a} and {b} are the same store"
            );
        }
    }

    #[test]
    fn test_storage_new_does_not_share_an_object_store_across_containers() {
        // The negative half of the reuse test, through `Storage::new`: two
        // Azure containers under one account must get two clients.
        let storage_for = |base_path: &str| {
            let configs = Arc::new(HudiConfigs::new([(
                HudiTableConfig::BasePath.as_ref().to_string(),
                base_path.to_string(),
            )]));
            Storage::new(Arc::new(HashMap::new()), configs).unwrap()
        };
        let prod = storage_for("abfss://prod@acct.dfs.core.windows.net/sales");
        let staging = storage_for("abfss://staging@acct.dfs.core.windows.net/sales");
        let prod_again = storage_for("abfss://prod@acct.dfs.core.windows.net/orders");

        assert!(
            !Arc::ptr_eq(&prod.object_store, &staging.object_store),
            "two containers must not share one ObjectStore"
        );
        assert!(
            Arc::ptr_eq(&prod.object_store, &prod_again.object_store),
            "two tables in one container share one ObjectStore"
        );
    }

    #[test]
    fn test_storage_new_error_no_base_path() {
        let options = Arc::new(HashMap::new());
        let hudi_configs = Arc::new(HudiConfigs::empty());
        let result = Storage::new(options, hudi_configs);

        assert!(
            result.is_err(),
            "Should return error when no base path is provided."
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to create storage")
        );
    }

    #[test]
    fn test_storage_new_error_invalid_url() {
        let options = Arc::new(HashMap::new());
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath,
            "http://invalid_url",
        )]));
        let result = Storage::new(options, hudi_configs);

        assert!(
            result.is_err(),
            "Should return error when no base path is invalid."
        );
        assert!(matches!(result.unwrap_err(), Creation(_)));
    }

    #[tokio::test]
    async fn storage_list_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let first_level_dirs: HashSet<String> =
            storage.list_dirs(None).await.unwrap().into_iter().collect();
        assert_eq!(
            first_level_dirs,
            vec![".hoodie", "part1", "part2", "part3"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        let second_level_dirs: Vec<String> = storage.list_dirs(Some("part2")).await.unwrap();
        assert_eq!(second_level_dirs, vec!["part22"]);
        let no_dirs = storage.list_dirs(Some("part1")).await.unwrap();
        assert!(no_dirs.is_empty());
    }

    #[tokio::test]
    async fn storage_list_dirs_as_paths() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let first_level_dirs: HashSet<ObjPath> = storage
            .list_dirs_as_obj_paths(None)
            .await
            .unwrap()
            .into_iter()
            .collect();
        let expected_paths: HashSet<ObjPath> = vec![".hoodie", "part1", "part2", "part3"]
            .into_iter()
            .map(|dir| {
                ObjPath::from_url_path(join_url_segments(&storage.base_url, &[dir]).unwrap().path())
                    .unwrap()
            })
            .collect();
        assert_eq!(first_level_dirs, expected_paths);
    }

    #[tokio::test]
    async fn storage_list_files() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let file_info_1: Vec<FileMetadata> = storage
            .list_files(None)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_1, vec![FileMetadata::new("a.parquet", 0)]);
        let file_info_2: Vec<FileMetadata> = storage
            .list_files(Some("part1"))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_2, vec![FileMetadata::new("b.parquet", 0)],);
        let file_info_3: Vec<FileMetadata> = storage
            .list_files(Some("part2/part22"))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_3, vec![FileMetadata::new("c.parquet", 0)],);
    }

    #[tokio::test]
    async fn storage_list_files_excludes_crc_files() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();

        let files = storage.list_files(None).await.unwrap();

        assert!(!files.iter().any(|f| f.name.ends_with(".crc")));
        assert_eq!(files, vec![FileMetadata::new("a.parquet", 0)]);
    }

    #[tokio::test]
    async fn use_storage_to_get_leaf_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let leaf_dirs = get_leaf_dirs(&storage, None).await.unwrap();
        assert_eq!(
            leaf_dirs,
            vec![".hoodie", "part1", "part2/part22", "part3/part32/part33"]
        );
    }

    #[tokio::test]
    async fn use_storage_to_get_leaf_dirs_for_leaf_dir() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data/leaf_dir")).unwrap())
                .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let leaf_dirs = get_leaf_dirs(&storage, None).await.unwrap();
        assert_eq!(
            leaf_dirs,
            vec![""],
            "Listing a leaf dir should get the relative path to itself."
        );
    }

    #[tokio::test]
    async fn storage_get_file_info() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let file_metadata = storage
            .get_file_metadata_not_populated("a.parquet")
            .await
            .unwrap();
        assert_eq!(file_metadata.name, "a.parquet");
        assert_eq!(file_metadata.size, 866);
    }
}
