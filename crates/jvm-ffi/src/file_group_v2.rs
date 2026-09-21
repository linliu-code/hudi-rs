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

//! Read ONE file group through `reader_v2` and hand it to a foreign caller as
//! an Arrow C stream.
//!
//! This is the read both the C ABI (`hudi_ffi_read_file_group_v2_into`) and the
//! JNI crate call. It holds no binding logic: strings in, a `RecordBatch` (or a
//! stream exported into caller-owned memory) out. Table options come from the
//! table's own `hoodie.properties` (so an MDT read picks up
//! `hoodie.table.base.file.format=HFILE` and its merge settings by itself).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use hudi::config::HudiConfigs;
use hudi::ffi_support::{
    FileGroupReaderSchemaHandler, HoodieFileGroupReader, InputSplit, InstantRange, KeyPredicate,
    OBJECT_STORE_RUNTIME, ReaderContext, ReaderParameters, RecordContext,
};
use hudi::storage::Storage;
use hudi::table::builder::OptionResolver;

const MERGE_MODE_KEY: &str = "hoodie.record.merge.mode";
const MERGE_STRATEGY_ID_KEY: &str = "hoodie.record.merge.strategy.id";
const BASE_FILE_FORMAT_KEY: &str = "hoodie.table.base.file.format";

/// Everything a caller must supply to read one file slice.
#[derive(Debug, Clone, Copy)]
pub struct FileGroupRequest<'a> {
    /// Table base URI (for an MDT read: `<table>/.hoodie/metadata`).
    pub table_path: &'a str,
    /// Partition path relative to the table (`record_index`), `""` for none.
    pub partition_path: &'a str,
    /// Base file NAME (not path); `""` for a log-only slice.
    pub base_file_name: &'a str,
    /// Log file NAMES in any order (the split sorts them).
    pub log_file_names: &'a [&'a str],
    /// Latest completed instant to read as of. REQUIRED: an empty string is refused
    /// (Java always passes a concrete instant — the last completed one or
    /// SOLO_COMMIT_TIMESTAMP); pass MAX_INSTANT_TIME to read everything.
    pub latest_instant: &'a str,
    /// Avro JSON of the table's data schema; `""` = none (the engine then
    /// infers as today).
    ///
    /// Java's `HoodieFileGroupReader` requires this; a log-only slice cannot be
    /// read without it because a freshly bootstrapped MDT file group is one
    /// empty delete block with no schema header.
    pub data_schema_json: &'a str,
    /// Keys (or key prefixes) to look up — Java's `Predicates.in(keys)` /
    /// `Predicates.startsWithAny(prefixes)` on the reader context, already
    /// encoded, sorted and deduplicated by the caller. Tri-state (D-12):
    /// `None` = no predicate (the whole slice, the base toy's read);
    /// `Some(&[])` = match NOTHING (zero rows — Java's `EmptyIterator` for an
    /// empty key set in `readSliceAndFilterByKeysIntoList`);
    /// `Some(keys)` = exactly these keys / prefixes — on a slice whose base file
    /// can seek by key (HFile — every metadata-table slice); a base file in a
    /// format that cannot (parquet) still returns every row, as Java does, so a
    /// non-MDT caller must filter (`crates/core/src/file_group/base_file/reader.rs:112-118`).
    pub lookup_keys: Option<&'a [&'a str]>,
    /// `true`: `lookup_keys` are prefixes; `false`: exact keys.
    pub lookup_keys_are_prefixes: bool,
    /// Explicit valid-instant set — Java's `InstantRange.EXACT_MATCH(validInstantTimestamps)`:
    /// a log block whose instant is not in the set is skipped. Empty means no range.
    /// Designed for a metadata-table read (a `table_path` ending in
    /// `.hoodie/metadata`), where reader_v2's `base_file_in_range`
    /// (`crates/core/src/file_group/reader_v2/engine.rs`) always keeps the base
    /// file. For any other path the base file is subject to the range too: it is
    /// kept only if its own commit instant is in the set (all-or-nothing per
    /// file, unlike the per-block log filtering; unresolvable instant = keep),
    /// so a data-table caller must include the base file's instant.
    pub valid_instants: &'a [&'a str],
}

impl Default for FileGroupRequest<'_> {
    /// All-empty: refused by `read_file_group_v2` on `table_path`. Exists so callers
    /// and tests can use struct-update syntax without naming every field.
    fn default() -> Self {
        Self {
            table_path: "",
            partition_path: "",
            base_file_name: "",
            log_file_names: &[],
            latest_instant: "",
            data_schema_json: "",
            lookup_keys: None,
            lookup_keys_are_prefixes: false,
            valid_instants: &[],
        }
    }
}

fn join_in_partition(partition: &str, name: &str) -> String {
    if partition.is_empty() {
        name.to_string()
    } else {
        format!("{partition}/{name}")
    }
}

fn build_reader_context(
    req: &FileGroupRequest<'_>,
    table_config: HashMap<String, String>,
    schema_handler: FileGroupReaderSchemaHandler,
) -> ReaderContext {
    let merge_mode = table_config
        .get(MERGE_MODE_KEY)
        .cloned()
        .unwrap_or_else(|| "COMMIT_TIME_ORDERING".to_string());
    let merge_strategy_id = table_config
        .get(MERGE_STRATEGY_ID_KEY)
        .cloned()
        .unwrap_or_default();
    // The resolver treats `hfile` as config-only (it can never be sniffed from a
    // path), so hand it the table's declared format, lower-cased.
    let base_file_format = table_config
        .get(BASE_FILE_FORMAT_KEY)
        .map(|v| v.to_ascii_lowercase())
        .unwrap_or_default();
    let latest_commit_time = req.latest_instant.to_string();
    let record_context = RecordContext::new(&table_config, req.partition_path.to_string());
    // `Some(&[])` deliberately builds an EMPTY matcher: it admits no key, so the
    // base-file read and every HFile log block yield zero rows — Java's EmptyIterator.
    let key_predicate = req.lookup_keys.map(|keys| {
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        if req.lookup_keys_are_prefixes {
            KeyPredicate::Prefixes(keys)
        } else {
            KeyPredicate::Keys(keys)
        }
    });
    let instant_range = if req.valid_instants.is_empty() {
        None
    } else {
        Some(InstantRange::exact_match(
            req.valid_instants.iter().copied(),
            "UTC",
        ))
    };
    ReaderContext {
        table_path: req.table_path.to_string(),
        latest_commit_time,
        base_file_format,
        has_log_files: !req.log_file_names.is_empty(),
        has_bootstrap_base_file: false,
        needs_bootstrap_merge: false,
        should_merge_use_record_position: false,
        enable_logical_timestamp_field_repair: false,
        iterator_mode: String::new(),
        merge_mode,
        merge_strategy_id,
        instant_range,
        record_context,
        schema_handler,
        table_config,
        hoodie_reader_config: HashMap::new(),
        row_filter_builder: None,
        row_group_selector: None,
        mor_pk_safe: false,
        key_predicate,
        // ENG-48206 / OSS #748 — empty is CORRECT here, not a disabled guard.
        // The gate exists to withdraw a pushed parquet predicate when a file's
        // logical-type repair would make it misread physical values. This path
        // installs no predicate at all (`row_filter_builder: None`,
        // `row_group_selector: None` directly above), so there is nothing a
        // repair could make wrong and nothing to withdraw. Should this
        // constructor ever start pushing a filter, it must populate this from
        // the predicate's referenced columns — see `repair_risk_columns_for`
        // in `cpp/src/lib.rs`, which is the analogous production path.
        repair_risk_columns: Vec::new(),
        completion_gate_inputs: None,
    }
}

/// Read the whole file group into one `RecordBatch`.
///
/// Blocking. Must be called from a plain native thread (a JNI or C caller),
/// never from inside a tokio runtime: it drives `OBJECT_STORE_RUNTIME` with
/// `block_on`, which panics on re-entry — so that case is refused up front.
pub fn read_file_group_v2(req: &FileGroupRequest<'_>) -> Result<RecordBatch, String> {
    if req.table_path.is_empty() {
        return Err("table_path is empty".to_string());
    }
    if req.latest_instant.is_empty() {
        return Err(
            "latest_instant is empty: pass the latest completed instant, or MAX_INSTANT_TIME \
             to read everything (Java passes SOLO_COMMIT_TIMESTAMP when the metadata table \
             has no completed instant)"
                .to_string(),
        );
    }
    if req.base_file_name.is_empty() && req.log_file_names.is_empty() {
        return Err("a file slice needs a base file or at least one log file".to_string());
    }
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(
            "read_file_group_v2 must not be called from within a tokio runtime \
             (it uses block_on on OBJECT_STORE_RUNTIME)"
                .to_string(),
        );
    }

    log::info!(
        "[hudi-rs-jni] read_file_group_v2 entered: table_path={} partition={} base_file={} \
         log_files={} latest_instant={} has_data_schema={} lookup_keys={} prefixes={} \
         valid_instants={}",
        req.table_path,
        req.partition_path,
        if req.base_file_name.is_empty() {
            "<none>"
        } else {
            req.base_file_name
        },
        req.log_file_names.len(),
        req.latest_instant,
        !req.data_schema_json.is_empty(),
        req.lookup_keys
            .map(|k| k.len().to_string())
            .unwrap_or_else(|| "none".to_string()),
        req.lookup_keys_are_prefixes,
        req.valid_instants.len(),
    );

    // Java's caller always supplies the data schema; when we get one, hand it to
    // the schema handler as both the data and the requested schema, exactly as
    // `HoodieBackedTableMetadata` does. Without it the engine has to infer the
    // output schema from the slice, which is impossible for a log-only slice
    // whose only block is an empty delete block.
    let mut schema_handler = FileGroupReaderSchemaHandler::new();
    if !req.data_schema_json.is_empty() {
        // Through the `ffi_support` facade, not `avro_to_arrow`: the latter has
        // no `AvroSchema::Ref` support and panics on the metadata table's own
        // record schema, whose `ColumnStatsMetadata.maxValue` union references
        // the wrapper records that `minValue` defines.
        let schema = hudi::ffi_support::arrow_schema_from_avro_json(req.data_schema_json)
            .map_err(|e| format!("invalid data schema: {e}"))?;
        log::debug!(
            "read_file_group_v2: data schema supplied, {} fields",
            schema.fields().len()
        );
        schema_handler = schema_handler
            .with_data_schema(schema.clone())
            .with_requested_schema(schema)
            // The same schema in Avro-land. That is what makes the read do what
            // Java's `GenericDatumReader(writerSchema, readerSchema)` does
            // (`HoodieNativeAvroHFileReader:453`): resolve the base HFile and
            // every log block against THIS schema at decode time, matching union
            // branches by name and filling reader-only fields from their Avro
            // defaults. Without it a file written by an older Hudi (a
            // table-version-6 metadata table, say) decodes in its own writer
            // schema and then has to be bent to this one in Arrow, which cannot
            // widen a union and has no notion of an Avro field default.
            .with_data_schema_json(req.data_schema_json.to_string())
            .with_requested_schema_json(req.data_schema_json.to_string());
    }

    let table_path = req.table_path.to_string();
    let (storage, table_config) = OBJECT_STORE_RUNTIME.block_on(async move {
        let no_options: Vec<(&str, &str)> = Vec::new();
        let mut resolver = OptionResolver::new_with_options(&table_path, no_options);
        resolver
            .resolve_options()
            .await
            .map_err(|e| format!("failed to resolve table options for {table_path}: {e}"))?;
        let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options.clone()));
        let storage = Storage::new(Arc::new(resolver.storage_options), hudi_configs)
            .map_err(|e| format!("failed to create storage for {table_path}: {e}"))?;
        Ok::<_, String>((storage, resolver.hudi_options))
    })?;
    log::debug!(
        "read_file_group_v2: resolved {} table options; base_file_format={:?} merge_mode={:?}",
        table_config.len(),
        table_config.get(BASE_FILE_FORMAT_KEY),
        table_config.get(MERGE_MODE_KEY),
    );

    let base_file_path = if req.base_file_name.is_empty() {
        None
    } else {
        Some(join_in_partition(req.partition_path, req.base_file_name))
    };
    let log_file_paths: Vec<String> = req
        .log_file_names
        .iter()
        .map(|name| join_in_partition(req.partition_path, name))
        .collect();
    let base_file_path_for_error = base_file_path.clone();
    let input_split = InputSplit::new(
        base_file_path,
        None,
        log_file_paths,
        req.partition_path.to_string(),
    );

    let reader_context = Arc::new(build_reader_context(req, table_config, schema_handler));
    let mut reader = HoodieFileGroupReader::builder()
        .with_reader_context(reader_context)
        .with_storage(storage)
        .with_input_split(input_split)
        .with_reader_parameters(ReaderParameters::default())
        .build()
        .map_err(|e| format!("failed to build the file group reader: {e}"))?;

    let batch = OBJECT_STORE_RUNTIME.block_on(reader.read()).map_err(|e| {
        // Names the slice the way the caller asked for it, so a log-only slice
        // does not render as a partition with an empty file name.
        let slice = match &base_file_path_for_error {
            Some(path) => path.clone(),
            None => format!(
                "{} (log-only, {} log files)",
                req.partition_path,
                req.log_file_names.len()
            ),
        };
        format!("failed to read file group {slice}: {e}")
    })?;
    log::debug!(
        "read_file_group_v2: done, {} rows x {} columns",
        batch.num_rows(),
        batch.num_columns()
    );
    Ok(batch)
}

/// Read the file group and export it as an Arrow C stream into `out_stream`.
///
/// # Safety
/// `out_stream` must be non-null and point to memory sized and aligned for an
/// `FFI_ArrowArrayStream` that the caller owns (Java: `ArrowArrayStream.allocateNew`).
/// Whatever was there is overwritten without being released; pass a fresh struct.
/// On `Err`, `out_stream` is left untouched.
pub unsafe fn export_file_group_stream_v2(
    req: &FileGroupRequest<'_>,
    out_stream: *mut FFI_ArrowArrayStream,
) -> Result<(), String> {
    if out_stream.is_null() {
        return Err("out_stream is null".to_string());
    }
    let batch = read_file_group_v2(req)?;
    let schema = batch.schema();
    let reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ));
    // SAFETY: the caller guarantees `out_stream` is valid, owned, writable memory.
    unsafe { std::ptr::write(out_stream, FFI_ArrowArrayStream::new(reader)) };
    Ok(())
}
