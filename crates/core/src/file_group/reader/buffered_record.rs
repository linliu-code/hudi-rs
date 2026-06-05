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

//! Mirrors `org.apache.hudi.common.table.read.BufferedRecord` and
//! `org.apache.hudi.common.model.DeleteRecord`.
//!
//! In Java Hudi, `BufferedRecord<T>` wraps a single engine-native record
//! with its key, ordering value, and operation type. In hudi-rs, the
//! "engine record" is an Arrow `RecordBatch`.
//!
//! During log scanning, records are accumulated into the record buffer.
//! At read time, base file records are merged with log records to produce
//! the final output.

use arrow_array::RecordBatch;

use super::record_context::RecordContext;

/// The universal record envelope flowing through the merge pipeline.
///
/// Mirrors Java's `BufferedRecord<T>`. In Rust/Arrow, a single
/// "record" is represented as a single-row `RecordBatch` when needed
/// for per-record operations, or as a multi-row batch for bulk operations.
///
/// During the log scanning phase, `BufferedRecord`s are stored in the
/// record buffer's map keyed by record key.
#[derive(Debug, Clone)]
pub struct BufferedRecord {
    /// The record key extracted from the record.
    pub record_key: String,

    /// The engine-native record data (Arrow RecordBatch).
    /// `None` for delete records, or after `to_binary()` has been called.
    pub data: Option<RecordBatch>,

    /// The binary form of the record data (Arrow IPC bytes).
    /// Populated by `to_binary()`, consumed by `get_record()`.
    /// Mirrors Java's binary representation after `toBinary(RecordContext)`.
    pub binary_data: Option<Vec<u8>>,

    /// The ordering value used for merge conflict resolution.
    /// Higher ordering value wins during delta merge.
    pub ordering_value: Option<OrderingValue>,

    /// Whether this record represents a deletion.
    pub is_delete: bool,

    /// Interned schema id, set when the record is encoded to compact bytes.
    ///
    /// Mirrors Java's `BufferedRecord.schemaId: Integer`. Different log
    /// blocks (and base files) may produce records with different writer
    /// schemas, so each record carries an opaque id into
    /// `RecordContext`'s schema cache. Used to look up the right
    /// `RowCodec` at decode time.
    ///
    /// `None` for records that have not been interned yet (e.g. legacy
    /// constructors that pre-date the schema cache).
    pub schema_id: Option<u32>,
}

impl BufferedRecord {
    /// Create a new data record.
    pub fn new_data(record_key: String, data: RecordBatch, ordering_value: Option<OrderingValue>) -> Self {
        Self {
            record_key,
            data: Some(data),
            binary_data: None,
            ordering_value,
            is_delete: false,
            schema_id: None,
        }
    }

    /// Create a new delete record.
    pub fn new_delete(record_key: String, ordering_value: Option<OrderingValue>) -> Self {
        Self {
            record_key,
            data: None,
            binary_data: None,
            ordering_value,
            is_delete: true,
            schema_id: None,
        }
    }

    /// Returns true if this record represents a deletion.
    pub fn is_delete(&self) -> bool {
        self.is_delete
    }

    /// Returns true if this record has no data payload (neither batch nor binary).
    pub fn is_empty(&self) -> bool {
        self.data.is_none() && self.binary_data.is_none()
    }

    /// Convert the record data to binary format for compact storage.
    ///
    /// Mirrors Java's `BufferedRecord.toBinary(RecordContext<T> recordContext)`:
    /// interns the record's writer schema in `record_context`'s schema cache,
    /// stores the resulting id on `self`, and encodes the row via the cached
    /// `RowCodec` for that schema. The codec format is ~10× more compact than
    /// the legacy per-record Arrow IPC stream (ENG-40160).
    ///
    /// Falls back to the legacy Arrow IPC stream when the schema contains
    /// data types `RowCodec` doesn't yet support (List / Struct / Map).
    pub fn to_binary(&mut self, record_context: &RecordContext) -> &mut Self {
        if let Some(batch) = self.data.take() {
            let schema = batch.schema();
            let id = record_context.intern_schema(schema.clone());
            self.schema_id = Some(id);

            // Try the compact codec first.
            if let Some(codec) = record_context.codec_for(id) {
                if let Ok(bytes) = codec.encode_row(&batch, 0) {
                    self.binary_data = Some(bytes);
                    return self;
                }
            }
            // Codec unavailable or encode failed → legacy IPC fallback.
            let bytes = RecordContext::to_binary_row(&schema, &batch);
            self.binary_data = Some(RecordContext::seal(bytes));
        }
        self
    }

    /// Return the record data, deserializing from binary if needed.
    ///
    /// Mirrors Java's `BufferedRecord.getRecord()` + the engine-specific
    /// unwrap. Uses `record_context`'s cached `RowCodec` (looked up by
    /// `self.schema_id`) for the compact codec path; falls back to
    /// Arrow IPC if `schema_id` is unset or codec decode failed.
    pub fn get_record(&self, record_context: &RecordContext) -> Option<RecordBatch> {
        if let Some(ref batch) = self.data {
            return Some(batch.clone());
        }
        let bytes = self.binary_data.as_ref()?;
        // Compact codec path: requires schema_id set by to_binary().
        if let Some(id) = self.schema_id {
            if let Some(codec) = record_context.codec_for(id) {
                if let Ok(batch) = codec.decode_row(bytes) {
                    return Some(batch);
                }
            }
        }
        // Legacy IPC fallback (records that pre-date schema_id, or codec
        // build failed for an unsupported schema).
        RecordContext::from_binary(bytes).ok()
    }
}

/// Comparable ordering value for merge conflict resolution.
///
/// In Java Hudi, this is `Comparable<?>`. In Rust, we support
/// the common types used as precombine fields.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderingValue {
    Long(i64),
    String(String),
}

impl PartialOrd for OrderingValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for OrderingValue {}

impl Ord for OrderingValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (OrderingValue::Long(a), OrderingValue::Long(b)) => a.cmp(b),
            (OrderingValue::String(a), OrderingValue::String(b)) => a.cmp(b),
            // Cross-type comparison: treat Long < String (arbitrary but consistent)
            (OrderingValue::Long(_), OrderingValue::String(_)) => std::cmp::Ordering::Less,
            (OrderingValue::String(_), OrderingValue::Long(_)) => std::cmp::Ordering::Greater,
        }
    }
}

/// Immutable value object representing a record to be deleted.
///
/// Mirrors Java's `org.apache.hudi.common.model.DeleteRecord`.
/// Created from `HoodieDeleteBlock` during log scanning.
#[derive(Debug, Clone)]
pub struct DeleteRecord {
    /// The record key to delete.
    pub record_key: String,

    /// The partition path.
    pub partition_path: String,

    /// Ordering value for merge conflict resolution with existing records.
    pub ordering_value: Option<OrderingValue>,
}

/// Factory methods for creating `BufferedRecord` instances.
///
/// Mirrors Java's `org.apache.hudi.common.table.read.BufferedRecords` (static factory).
pub struct BufferedRecords;

impl BufferedRecords {
    /// Create a `BufferedRecord` from an engine record (Arrow RecordBatch).
    ///
    /// Extracts the record key and ordering value from the batch using the
    /// provided schema and field names.
    pub fn from_engine_record(
        record_key: String,
        data: RecordBatch,
        ordering_value: Option<OrderingValue>,
        is_delete: bool,
    ) -> BufferedRecord {
        if is_delete {
            BufferedRecord::new_delete(record_key, ordering_value)
        } else {
            BufferedRecord::new_data(record_key, data, ordering_value)
        }
    }

    /// Create a `BufferedRecord` from a `DeleteRecord`.
    ///
    /// Produces a `BufferedRecord` with `data=None` and `is_delete=true`.
    pub fn from_delete_record(delete_record: &DeleteRecord) -> BufferedRecord {
        BufferedRecord::new_delete(
            delete_record.record_key.clone(),
            delete_record.ordering_value.clone(),
        )
    }
}

