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

/*
 * hudi_base_file_data_provider.h - C ABI for injecting a base-file data provider
 * into hudi-rs.
 *
 * hudi-rs (libhudi.so) defines this interface and an adapter for it, but names
 * and links no provider implementation. A composition root outside hudi-rs
 * supplies a vtable + an opaque ctx, hands them to
 * hudi_base_file_data_provider_new(), and stashes the returned handle on
 * FfiReaderContext.base_file_provider_handle before building the reader.
 *
 * This header is the contract. It MUST stay layout-identical to the Rust
 * definitions in cpp/src/provider_abi.rs. HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION
 * guards against silent drift once the two sides are built and shipped
 * separately.
 *
 * Structs use the platform C ABI (matching Rust's #[repr(C)]). ArrowSchema and
 * ArrowArrayStream are the Arrow C Data Interface types from arrow/c/abi.h.
 */

#ifndef HUDI_BASE_FILE_DATA_PROVIDER_H
#define HUDI_BASE_FILE_DATA_PROVIDER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "arrow/c/abi.h" /* struct ArrowSchema, struct ArrowArrayStream */

#ifdef __cplusplus
extern "C" {
#endif

/* Bump on ANY change to the struct layouts below. hudi-rs rejects a vtable
 * whose abi_version does not match, degrading to "no provider". */
#define HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION 1u

/* try_base_file outcome codes. */
#define HUDI_PROVIDER_OUTCOME_NOT_SERVED 0
#define HUDI_PROVIDER_OUTCOME_SERVED 1

/* A borrowed UTF-8 slice: pointer + byte length, no NUL terminator. Valid only
 * for the duration of the try_base_file call it is passed to. */
typedef struct HudiStrSlice {
  const uint8_t *ptr;
  size_t len;
} HudiStrSlice;

/* Everything the provider needs to try one base file. All pointers are
 * borrowed for the duration of the call only. */
typedef struct HudiBaseFileDataRequest {
  /* Absolute storage URI of the base file (the identity a provider keys by). */
  HudiStrSlice file_uri;
  /* Projected ("intersection") schema the read wants back. Never NULL.
   *
   * Serve EXACTLY these fields, in this order, with these types. hudi-rs CHECKS
   * a served stream against this schema and DECLINES a stream that differs --
   * the file is then read from object storage instead, the attempt is counted as
   * a storage fallback rather than a serve, and one error line per file names
   * the first field that differed. A provider that gets the shape wrong is not
   * silently tolerated, but neither does it fail the read: it simply delivers no
   * benefit, at the cost of an error log per base file.
   *
   * Compared: the field COUNT, and each field's NAME and DATA TYPE, positionally.
   * A type is compared exactly, dictionary encoding included -- Dictionary(Int32,
   * Utf8) where Utf8 was asked for is a different physical layout.
   *
   * Deliberately NOT compared, so these are safe to differ: nullability, and
   * field- or schema-level key-value metadata. A provider that widens a non-null
   * column to nullable, or carries extra metadata through its own transport, is
   * still serving the right data.
   *
   * Serving FEWER fields is NOT the lenient case it looks like. Every field here
   * was read from THIS file's own footer, so none of them can be legitimately
   * absent from a serve of this file; hudi-rs null-fills a column it cannot find
   * by name, which is correct for a genuinely absent column and is exactly how a
   * dropped one would become a column of nulls in a successful read.
   *
   * VALUES CONTRACT: serve the base file's PHYSICAL values, unaltered. Where
   * apache/hudi#18132 applies, a file's footer may LABEL a timestamp column with
   * a unit its stored int64s are not in (declared micros, stored millis).
   * projected_schema carries the file's declared type, label included, because
   * hudi-rs applies the repair itself after the batches arrive. A provider that
   * "helpfully" rescales the values into the declared unit is applying the
   * repair a second time and lands values about 1000x too large. Do not convert,
   * do not normalize, do not round-trip through a typed representation that
   * would. Hand back what the file holds. */
  const struct ArrowSchema *projected_schema;
  /* Whether a pushed predicate may be applied to this file. When false, the
   * provider must serve unfiltered and let the post-merge filter apply it.
   *
   * PER FILE, not per split, and not a table property: it is exactly the
   * decision hudi-rs's own parquet RowFilter pushdown got for this same file.
   * Two independent gates must both pass -- the merge-safety gate (no log files
   * on the split, or a primary-key-safe predicate) and the repair gate (this
   * file's footer does not label a predicate column in a way the
   * apache/hudi#18132 logical-type repair reinterprets on read). The second is
   * decided from the file's own footer, so the SAME read can hand true for one
   * base file and false for the next. A provider that caches the answer across
   * files of a split is wrong, and drops rows that match. */
  bool can_push_predicate;
  /* Partition path of the split (e.g. "year=2024/month=01"). */
  HudiStrSlice partition_path;
  /* Partition field names, in order; partition_fields_len entries. */
  const HudiStrSlice *partition_fields;
  size_t partition_fields_len;
  /* Table data schema, or NULL if unavailable. */
  const struct ArrowSchema *data_schema;
} HudiBaseFileDataRequest;

/* Client-side counters for one attempt. Fill on both outcomes (a file that was
 * not served still reports its timings).
 *
 * Two kinds of counter, and only one of them is yours:
 *
 *  - SETUP counters -- files_served, storage_fallbacks, local_served,
 *    remote_served, and the three *_wall_nanos -- are knowable by the time
 *    try_base_file returns. Fill them.
 *
 *  - DRAIN counters -- rows_served, bytes_served, batches_received -- are only
 *    knowable once the served stream has been consumed, which happens AFTER
 *    try_base_file returns. LEAVE THEM ZERO. hudi-rs counts them as it pulls the
 *    stream and folds them into the same slot. On the served path hudi-rs zeroes
 *    whatever you put there before doing so, so filling them does not
 *    double-count -- it is simply discarded, and your numbers are not what a
 *    consumer reads back. */
typedef struct HudiBaseFileProviderStats {
  uint64_t files_served;
  uint64_t storage_fallbacks;
  uint64_t local_served;
  uint64_t remote_served;
  /* Drain counters: leave zero, hudi-rs fills these. See above. */
  uint64_t rows_served;
  uint64_t bytes_served;
  uint64_t batches_received;
  uint64_t discover_wall_nanos;
  uint64_t connect_wall_nanos;
  uint64_t fetch_wall_nanos;
} HudiBaseFileProviderStats;

/* Out-parameter the provider fills. On HUDI_PROVIDER_OUTCOME_SERVED, `stream`
 * holds a valid Arrow C stream of the projected batches; otherwise `stream` is
 * left untouched (hudi-rs pre-initializes it empty). */
typedef struct HudiBaseFileDataResult {
  /* Advisory. THE RETURN VALUE OF try_base_file IS AUTHORITATIVE; this field is
   * not read to decide the outcome, and hudi-rs pre-initializes it to
   * HUDI_PROVIDER_OUTCOME_NOT_SERVED.
   *
   * Which matters because of one habit: HUDI_PROVIDER_OUTCOME_SERVED is 1 and
   * HUDI_PROVIDER_OUTCOME_NOT_SERVED is 0, so a provider written to the "return
   * 0 on success" C idiom fills the stream, sets this field to SERVED, returns
   * 0, and has every file it serves silently DECLINED and re-read from object
   * storage. Return HUDI_PROVIDER_OUTCOME_SERVED. hudi-rs logs a warning on
   * exactly this pairing (field says SERVED, return value does not). */
  int outcome;
  struct ArrowArrayStream stream;
  HudiBaseFileProviderStats stats;
} HudiBaseFileDataResult;

/*
 * The provider's function-pointer table. hudi-rs copies this by value in
 * hudi_base_file_data_provider_new(), so the storage backing it need not outlive
 * that call.
 *
 * Concurrency contract: try_base_file MAY be called concurrently from several
 * threads on the same ctx; the implementation must be thread-safe.
 *
 * Lifetime contract: destroy is called exactly once, after the last
 * try_base_file has returned AND after every ArrowArrayStream this provider
 * served has been fully drained or released. A served stream's get_next/release
 * callbacks may therefore point into ctx. hudi-rs guarantees this structurally
 * (the task that owns a served stream holds a strong reference to the
 * provider), so it does not depend on the order in which the C++ caller frees
 * the reader handle and the stream.
 *
 * The other side of that guarantee: destroy is NOT ordered against the free of
 * the reader handle, and is NOT GUARANTEED to run on the thread that performs
 * it (it often will; it must not be relied on). When a
 * served stream outlives the handle, the last reference is held by a hudi-rs
 * background thread and destroy runs there. destroy must therefore be
 * thread-agnostic and self-sufficient: no reliance on a caller thread-local,
 * and a JNI-backed ctx must attach to the JVM itself rather than assume an
 * attached thread.
 *
 * Error contract: any internal failure must be reported by returning
 * HUDI_PROVIDER_OUTCOME_NOT_SERVED, never by a mechanism that could fail the
 * read.
 */
typedef struct HudiBaseFileDataProviderVTable {
  /* Must equal HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION. */
  uint32_t abi_version;
  int (*try_base_file)(void *ctx, const HudiBaseFileDataRequest *req,
                       HudiBaseFileDataResult *out);
  /* See the lifetime contract above for when this runs. */
  void (*destroy)(void *ctx);
} HudiBaseFileDataProviderVTable;

/*
 * Wrap a vtable + ctx into an owning handle for
 * FfiReaderContext.base_file_provider_handle.
 *
 * Returns 0, and the reader then runs with no provider, if `vtable` is NULL,
 * its abi_version is not exactly HUDI_BASE_FILE_DATA_PROVIDER_ABI_VERSION
 * (older is refused as well as newer: an unknown layout is unknown in both
 * directions), or either function-pointer member is NULL -- which is what
 * `HudiBaseFileDataProviderVTable vt = {0};`, a memset, a calloc, or a header
 * predating a field produces, and is refused rather than called at address 0.
 *
 * OWNERSHIP OF ctx ON FAILURE: a 0 return means ctx was NOT taken, and destroy
 * was NOT called -- a refused vtable's destroy is not trusted to run. The caller
 * still owns ctx and must release it by its own means. Do not call
 * hudi_base_file_data_provider_free(0) expecting it to clean up; a 0 handle is
 * ignored.
 *
 * On success, ownership of `ctx` transfers into the handle: release it either by
 * handing the handle to new_file_group_reader_with_context (which consumes it)
 * or, if the reader is never built, by calling hudi_base_file_data_provider_free.
 */
uint64_t hudi_base_file_data_provider_new(const HudiBaseFileDataProviderVTable *vtable,
                                     void *ctx);

/*
 * Release a handle from hudi_base_file_data_provider_new that was NOT consumed by
 * new_file_group_reader_with_context (e.g. the reader build was aborted). Runs
 * the provider's destroy exactly once. A 0 handle is ignored.
 */
void hudi_base_file_data_provider_free(uint64_t handle);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* HUDI_BASE_FILE_DATA_PROVIDER_H */
