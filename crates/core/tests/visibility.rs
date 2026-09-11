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

//! Proves the surface the CXX bridge imports is reachable from outside
//! hudi-core, and that it is reachable ONLY through the `ffi_support` facade.
//! A compile failure here IS the test failure.
//!
//! The modules that actually define these items (`file_group::reader_v2`,
//! `timeline::selector`) are `pub(crate)`; the facade re-exports the individual
//! items, which is what makes them nameable here. The negative half of that
//! claim — that the direct paths are NOT public — cannot be written as an
//! integration test (a `compile_fail` check needs a doctest or `trybuild`), so
//! it is grep-pinned in the commit message and the fix-wave report instead:
//!   grep -nE '^pub mod (reader_v2|selector)' \
//!     crates/core/src/file_group/mod.rs crates/core/src/timeline/mod.rs
//! must print nothing.

#[test]
fn ffi_support_facade_is_publicly_reachable() {
    // Type-level only: naming these paths from an integration test (a separate
    // crate) fails to compile unless the facade re-exports them publicly.
    #[allow(unused_imports)]
    use hudi_core::ffi_support::{
        CompletionGateInputs, FileGroupMergeStream, FileGroupReaderSchemaHandler,
        HoodieFileGroupReader, InputSplit, InstantRange, ReaderContext, ReaderParameters,
        RecordContext, StreamReadStats, StreamStatsHandle,
    };

    // Value-level: the const, the static and the helper fn the bridge also uses.
    use hudi_core::ffi_support::{MAX_INSTANT_TIME, OBJECT_STORE_RUNTIME, stream_stats_handle};
    assert!(!MAX_INSTANT_TIME.is_empty());
    assert!(OBJECT_STORE_RUNTIME.handle().metrics().num_workers() > 0);
    let _: fn(&HoodieFileGroupReader) -> StreamStatsHandle = stream_stats_handle;
}
