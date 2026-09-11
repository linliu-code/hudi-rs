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
 * hudi_base_file_data_provider.h — C ABI for injecting a base-file data provider
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
  /* Projected ("intersection") schema the read wants back. Never NULL. */
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
 * not served still reports its timings). */
typedef struct HudiBaseFileProviderStats {
  uint64_t files_served;
  uint64_t storage_fallbacks;
  uint64_t local_served;
  uint64_t remote_served;
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
 * the reader handle, and may NOT run on the thread that performs it. When a
 * served stream outlives the handle, the last reference is held by a hudi-rs
 * background thread and destroy runs there. destroy must therefore be
 * thread-agnostic and self-sufficient — no reliance on a caller thread-local,
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
 * Returns 0 if `vtable` is NULL or its abi_version is incompatible (the reader
 * then runs with no provider). On success, ownership of `ctx` transfers into
 * the handle: release it either by handing the handle to
 * new_file_group_reader_with_context (which consumes it) or, if the reader is
 * never built, by calling hudi_base_file_data_provider_free.
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
