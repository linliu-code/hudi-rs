// ENG-42276 — local microbench to investigate v4 pushdown regression on q93/q94/q44/q32.
//
// Hypothesis (from problem map): when parquet column stats CANNOT prune row
// groups, RowFilter's two-pass behaviour costs more than the one-pass-no-filter
// baseline because:
//   pass 1: fetch + decode the predicate column(s)
//   pass 2: re-fetch + decode the remaining columns (only for surviving rows)
// If the predicate is unselective (~all rows survive), pass 2 ≈ a full read
// AND we paid pass 1 for nothing.
//
// This bench reads real TPC-DS 1GB store_sales parquet files through the EXACT
// same parquet layer Storage uses (ParquetRecordBatchStreamBuilder + with_row_filter),
// in three variants:
//
//   1. no_filter         — baseline; one pass over the file
//   2. unselective_filter — RowFilter that ALWAYS returns true (worst case)
//   3. selective_filter   — RowFilter that ALWAYS returns false (best case — pruning)
//
// All three read the same column projection (a subset, not full schema) to mirror
// how Spark TPC-DS queries project. Differences in wall-clock + bytes-fetched
// between variants confirm or refute the hypothesis.
//
// Run:   cargo test -p hudi-cpp --release --test pushdown_microbench -- --nocapture --test-threads=1

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow_array::{BooleanArray, RecordBatch};
use arrow::error::ArrowError;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

// Pick a handful of TPC-DS store_sales parquet files. Each is ~500KB, total ~3MB
// — small enough to keep the bench responsive but large enough that two-pass
// behaviour, if it exists, is measurable above timing noise.
const TPCDS_BASE: &str = "/work/tpcds-1gb/store_sales";

fn pick_files() -> Vec<PathBuf> {
    // Walk the partition dirs, take one parquet file from each of the first N partitions.
    let mut out = Vec::new();
    let entries: Vec<_> = match std::fs::read_dir(TPCDS_BASE) {
        Ok(rd) => rd.collect(),
        Err(e) => panic!("read_dir({TPCDS_BASE}) failed: {e}"),
    };
    let mut partition_paths: Vec<PathBuf> = entries
        .into_iter()
        .filter_map(|r| r.ok())
        .filter(|de| de.file_name().to_string_lossy().starts_with("ss_sold_date_sk="))
        .map(|de| de.path())
        .collect();
    partition_paths.sort();
    for p in partition_paths.into_iter().take(8) {
        if let Ok(rd) = std::fs::read_dir(&p) {
            for f in rd.flatten() {
                if f.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                    out.push(f.path());
                    break;
                }
            }
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// ObjectStore wrapper that counts bytes fetched + GET requests.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    bytes: Arc<AtomicU64>,
    gets: Arc<AtomicU64>,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        _l: &ObjPath,
        _p: PutPayload,
        _o: PutOptions,
    ) -> OsResult<PutResult> {
        unimplemented!("read-only bench")
    }

    async fn put_multipart_opts(
        &self,
        _l: &ObjPath,
        _o: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        unimplemented!("read-only bench")
    }

    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let r = self.inner.get_opts(location, options).await?;
        // GetResult.range describes what was actually returned; bytes are tracked
        // post-hoc via the payload, but parquet typically uses range reads, so
        // we capture the requested range length when present.
        let range_len: u64 = match &r.range {
            r => (r.end - r.start) as u64,
        };
        self.bytes.fetch_add(range_len, Ordering::Relaxed);
        Ok(r)
    }

    async fn head(&self, location: &ObjPath) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, _l: &ObjPath) -> OsResult<()> {
        unimplemented!("read-only bench")
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, _: Option<&ObjPath>) -> OsResult<ListResult> {
        unimplemented!()
    }

    async fn copy(&self, _: &ObjPath, _: &ObjPath) -> OsResult<()> {
        unimplemented!()
    }

    async fn copy_if_not_exists(&self, _: &ObjPath, _: &ObjPath) -> OsResult<()> {
        unimplemented!()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ArrowPredicate variants.
// ─────────────────────────────────────────────────────────────────────────────

/// Always-true predicate — worst case for two-pass RowFilter behaviour:
/// parquet still reads predicate columns first, evaluates, then re-reads
/// remaining columns for every surviving row (all of them).
struct AlwaysTrue {
    projection: ProjectionMask,
}
impl ArrowPredicate for AlwaysTrue {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        Ok(BooleanArray::from(vec![true; batch.num_rows()]))
    }
}

/// Always-false predicate — best case: parquet still has to evaluate per batch,
/// but no surviving rows means it can skip the rest of the read for any row group
/// where stats also said "no match." For us the stats won't prune (always-false
/// isn't expressible via stats), but the second pass over remaining columns
/// reads 0 rows.
struct AlwaysFalse {
    projection: ProjectionMask,
}
impl ArrowPredicate for AlwaysFalse {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        Ok(BooleanArray::from(vec![false; batch.num_rows()]))
    }
}

/// IS NOT NULL predicate — mimics what hudi-rs v4 actually installs for TPC-DS
/// regression queries q27/q29/q82/q84. The substrait predicate Velox sends to
/// hudi-rs is dominated by `isnotnull(...)` checks on join keys (because Spark
/// rewrites null-rejecting joins). RowFilter installs, evaluates this per row,
/// and produces all-true (since these columns are rarely null in fact tables) —
/// row group stats can never prune `IS NOT NULL`. Behaviour should match
/// AlwaysTrue: +1 GET per file vs no-filter baseline, no pruning benefit.
struct IsNotNullCol {
    projection: ProjectionMask,
}
impl ArrowPredicate for IsNotNullCol {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        // Predicate is on the first projected column.
        let col = batch.column(0);
        // is_null returns true for null rows; we want is_not_null.
        let nulls = col.logical_nulls();
        Ok(match nulls {
            None => BooleanArray::from(vec![true; batch.num_rows()]),
            Some(buf) => BooleanArray::from(buf.inner().clone()),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Read variants — three modes, same projection per call so the only delta is
// the row filter.
// ─────────────────────────────────────────────────────────────────────────────

/// Project a stable subset of columns to mirror Spark TPC-DS column-pruning.
/// Picks the first N top-level columns from the file; the "predicate column"
/// is one of the projected columns so RowFilter doesn't have to add extras.
fn build_projection(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    n_top_cols: usize,
) -> (ProjectionMask, usize /*predicate_col_idx*/) {
    let root_cols = parquet_schema.root_schema().get_fields().len();
    let n = std::cmp::min(n_top_cols, root_cols);
    let indices: Vec<usize> = (0..n).collect();
    let predicate_col_idx = 0; // first projected col is the predicate target
    let mask = ProjectionMask::roots(parquet_schema, indices);
    (mask, predicate_col_idx)
}

fn predicate_only_mask(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    col_idx: usize,
) -> ProjectionMask {
    ProjectionMask::roots(parquet_schema, [col_idx])
}

async fn read_file(
    path: &PathBuf,
    counter: Arc<CountingStore>,
    variant: ReadVariant,
    n_proj_cols: usize,
) -> Result<(u64, std::time::Duration), Box<dyn std::error::Error>> {
    let obj_path = ObjPath::from_filesystem_path(path)?;

    let t0 = Instant::now();
    let reader =
        ParquetObjectReader::new(counter.clone() as Arc<dyn ObjectStore>, obj_path);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

    let parquet_schema = builder.parquet_schema().clone();
    let (data_mask, predicate_col_idx) = build_projection(&parquet_schema, n_proj_cols);
    let pred_mask = predicate_only_mask(&parquet_schema, predicate_col_idx);

    let builder = builder.with_projection(data_mask);

    let builder = match variant {
        ReadVariant::NoFilter => builder,
        ReadVariant::UnselectiveFilter => {
            let pred = AlwaysTrue {
                projection: pred_mask,
            };
            let rf = RowFilter::new(vec![Box::new(pred)]);
            builder.with_row_filter(rf)
        }
        ReadVariant::SelectiveFilter => {
            let pred = AlwaysFalse {
                projection: pred_mask,
            };
            let rf = RowFilter::new(vec![Box::new(pred)]);
            builder.with_row_filter(rf)
        }
        ReadVariant::IsNotNullFilter => {
            let pred = IsNotNullCol {
                projection: pred_mask,
            };
            let rf = RowFilter::new(vec![Box::new(pred)]);
            builder.with_row_filter(rf)
        }
    };

    let mut stream = builder.build()?;
    let mut rows: u64 = 0;
    while let Some(b) = stream.next().await {
        rows += b?.num_rows() as u64;
    }
    Ok((rows, t0.elapsed()))
}

#[derive(Copy, Clone, Debug)]
enum ReadVariant {
    NoFilter,
    UnselectiveFilter,
    SelectiveFilter,
    IsNotNullFilter,
}

// ─────────────────────────────────────────────────────────────────────────────
// The bench.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pushdown_microbench() {
    if !std::path::Path::new(TPCDS_BASE).exists() {
        eprintln!("skipping: {TPCDS_BASE} not present");
        return;
    }

    let files = pick_files();
    if files.is_empty() {
        eprintln!("skipping: no parquet files found under {TPCDS_BASE}");
        return;
    }
    println!("benching against {} files from {TPCDS_BASE}", files.len());

    let n_proj_cols = 8; // mirrors typical TPC-DS column pruning

    // Warmup pass: read every file once so OS page cache is hot for ALL
    // variants. Without this, variant 1 pays the cold-cache cost and the
    // wall-clock comparison is invalid.
    {
        let bytes = Arc::new(AtomicU64::new(0));
        let gets = Arc::new(AtomicU64::new(0));
        let counter = Arc::new(CountingStore {
            inner: Arc::new(LocalFileSystem::new()),
            bytes,
            gets,
        });
        for f in &files {
            let _ = read_file(f, counter.clone(), ReadVariant::NoFilter, n_proj_cols)
                .await
                .unwrap();
        }
    }

    // Now run each variant 3 times back-to-back. Report the best wall-clock
    // so transient OS scheduling jitter doesn't dominate.
    let n_iters: usize = 3;

    for (label, variant) in [
        ("no_filter         ", ReadVariant::NoFilter),
        ("unselective_filter", ReadVariant::UnselectiveFilter),
        ("selective_filter  ", ReadVariant::SelectiveFilter),
        ("isnotnull_filter  ", ReadVariant::IsNotNullFilter),
    ] {
        // Warm up filesystem cache for the first variant; from variant 2 onward
        // the files are hot in the page cache. To get a fair comparison we run
        // all three back-to-back with the same hot cache.
        let bytes = Arc::new(AtomicU64::new(0));
        let gets = Arc::new(AtomicU64::new(0));
        let counter = Arc::new(CountingStore {
            inner: Arc::new(LocalFileSystem::new()),
            bytes: bytes.clone(),
            gets: gets.clone(),
        });

        let mut best = std::time::Duration::MAX;
        let mut total_rows: u64 = 0;
        let mut iter_bytes = 0u64;
        let mut iter_gets = 0u64;
        for iter in 0..n_iters {
            bytes.store(0, Ordering::Relaxed);
            gets.store(0, Ordering::Relaxed);
            let t0 = Instant::now();
            let mut rows = 0u64;
            for f in &files {
                let (r, _) = read_file(f, counter.clone(), variant, n_proj_cols)
                    .await
                    .unwrap();
                rows += r;
            }
            let elapsed = t0.elapsed();
            if elapsed < best {
                best = elapsed;
            }
            total_rows = rows;
            iter_bytes = bytes.load(Ordering::Relaxed);
            iter_gets = gets.load(Ordering::Relaxed);
            // Suppress detail per-iter for cleanliness, but keep the last one's counters.
            let _ = iter;
        }
        println!(
            "{label}  best_of_{n_iters}={best:>8.2?}  rows={total_rows:>6}  bytes={iter_bytes:>10}  gets={iter_gets:>5}"
        );
    }
}
