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
 * hudi_parquet_schema_cache.h - C ABI for the process-wide parquet schema cache.
 *
 * hudi-rs caches each base file's arrow schema so the schema-evolution check on
 * every base-file read skips a HEAD + footer GET on a warm read. The cache is
 * sound because Hudi base files are IMMUTABLE: a Hudi writer never rewrites one
 * in place.
 *
 * An operator's remediation tool can. Rewriting a footer to correct an
 * apache/hudi#18132 logical-type mislabel is exactly such a rewrite, and it is
 * the input the reader's repair gate keys on - so a long-lived process with a
 * warm cache keeps comparing the footer as it was BEFORE the rewrite, and one
 * direction of that error pushes a row filter against reinterpreted values and
 * drops rows that match.
 *
 * If you rewrite a base file in place, call hudi_parquet_schema_cache_clear()
 * afterwards. If you do not, you never need it.
 *
 * Both functions are safe to call from any thread, at any time, with or without
 * a runtime. They act on a PROCESS-WIDE cache, not on any one reader.
 *
 * Structs use the platform C ABI (matching Rust's #[repr(C)]).
 */

#ifndef HUDI_PARQUET_SCHEMA_CACHE_H
#define HUDI_PARQUET_SCHEMA_CACHE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Cache counters. hits/misses are monotonic since process start; entries is the
 * live count and moves in both directions. */
typedef struct HudiParquetSchemaCacheStats {
  uint64_t hits;
  uint64_t misses;
  uint64_t entries;
} HudiParquetSchemaCacheStats;

/* Read the counters, for export through your own metrics registry - hudi-rs
 * pulls in no metrics framework of its own. A read of a disabled cache reports
 * zeroes and is counted as neither a hit nor a miss. */
HudiParquetSchemaCacheStats hudi_parquet_schema_cache_stats(void);

/* Drop every cached schema, so the next read of each file re-fetches its footer.
 * Call after rewriting a base file IN PLACE; a Hudi table never needs it. */
void hudi_parquet_schema_cache_clear(void);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* HUDI_PARQUET_SCHEMA_CACHE_H */
