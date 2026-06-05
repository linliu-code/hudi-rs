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

//! Compact per-row codec for `BufferedRecord` storage in MOR merge buffers.
//!
//! ## Motivation (ENG-40160)
//!
//! `KeyBasedFileGroupRecordBuffer` previously serialised each record as a
//! full Arrow IPC stream (`StreamWriter`) — a fresh stream per record, with
//! its own embedded schema message (~624 B) + flatbuffer framing (~880 B) on
//! top of ~200 B of actual row data. Measured at ~2,915 B resident per record
//! on TPC-DS 100GB MOR, causing OOMKilled executors at default 16-way
//! concurrency.
//!
//! This codec mirrors Java's `UnsafeRow` shape: schema known externally
//! (carried by the buffer's `reader_schema`), per-row bytes are just a null
//! bitmap + field bytes (fixed-width for primitives, length-prefixed for
//! variable-length). ~200-300 B per row for typical Hudi schemas.
//!
//! ## Invariants
//!
//! - All records entering the buffer share `reader_schema` — guaranteed by
//!   `row_extraction::reconcile_batch_to_schema` upstream of the buffer.
//! - The codec is constructed once per `RecordContext` / FG reader instance
//!   and reused for every record encode/decode.
//! - Encoded bytes are NOT cross-schema portable. Schema migration must
//!   happen before bytes are encoded.
//!
//! ## Wire format
//!
//! ```text
//! ┌─────────────────────────┬──────────────────────────────────────────┐
//! │ null bitmap (LSB-first) │ field bytes (concatenated, per schema)   │
//! │ ceil(N/8) bytes         │                                          │
//! └─────────────────────────┴──────────────────────────────────────────┘
//! ```
//!
//! When bit `i` in the null bitmap is set, field `i` is null and its bytes
//! are omitted. Per-field encoding:
//!
//! | DataType                              | Encoding                                    |
//! |---------------------------------------|---------------------------------------------|
//! | Int8 / Int16 / Int32 / Int64          | fixed-width LE, sign-extended in source     |
//! | UInt8 / UInt16 / UInt32 / UInt64      | fixed-width LE                              |
//! | Float32 / Float64                     | IEEE 754 LE bytes                           |
//! | Boolean                               | 1 byte (0 or 1)                             |
//! | Date32                                | 4 bytes LE                                  |
//! | Date64                                | 8 bytes LE                                  |
//! | Timestamp(* — any unit / tz)          | 8 bytes LE (i64 stored verbatim)            |
//! | Decimal128(p, s)                      | 16 bytes LE                                 |
//! | Utf8 / LargeUtf8 / Binary / LargeBinary | u32 LE length prefix + raw bytes        |
//! | Null                                  | (no bytes; always represented as null bit)  |
//!
//! Complex types (List / Struct / Map) currently return an error from
//! `RowCodec::new` — out of scope for the first pass. Callers can fall back
//! to the legacy IPC path if such types appear.

use crate::Result;
use crate::error::CoreError;

use arrow_array::Array;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder,
    LargeStringBuilder, NullBuilder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use std::sync::Arc;

/// Compact per-row codec for a fixed schema.
///
/// Construct once (`RowCodec::new(schema)`), reuse for every encode/decode.
/// Schema is shared via `SchemaRef`; the codec is small and cheap to clone
/// (~the field list + cached widths).
#[derive(Debug)]
pub struct RowCodec {
    schema: SchemaRef,
    fields: Vec<FieldKind>,
    null_bitmap_bytes: usize,
}

#[derive(Debug, Clone)]
enum FieldKind {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    Date32,
    Date64,
    /// `(unit, timezone)` — timezone is preserved as schema metadata; bytes are i64
    Timestamp(TimeUnit, Option<Arc<str>>),
    Decimal128(u8, i8),
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Null,
}

impl FieldKind {
    fn try_from_data_type(dt: &DataType) -> Result<Self> {
        match dt {
            DataType::Int8 => Ok(FieldKind::Int8),
            DataType::Int16 => Ok(FieldKind::Int16),
            DataType::Int32 => Ok(FieldKind::Int32),
            DataType::Int64 => Ok(FieldKind::Int64),
            DataType::UInt8 => Ok(FieldKind::UInt8),
            DataType::UInt16 => Ok(FieldKind::UInt16),
            DataType::UInt32 => Ok(FieldKind::UInt32),
            DataType::UInt64 => Ok(FieldKind::UInt64),
            DataType::Float32 => Ok(FieldKind::Float32),
            DataType::Float64 => Ok(FieldKind::Float64),
            DataType::Boolean => Ok(FieldKind::Boolean),
            DataType::Date32 => Ok(FieldKind::Date32),
            DataType::Date64 => Ok(FieldKind::Date64),
            DataType::Timestamp(unit, tz) => Ok(FieldKind::Timestamp(*unit, tz.clone())),
            DataType::Decimal128(p, s) => Ok(FieldKind::Decimal128(*p, *s)),
            DataType::Utf8 => Ok(FieldKind::Utf8),
            DataType::LargeUtf8 => Ok(FieldKind::LargeUtf8),
            DataType::Binary => Ok(FieldKind::Binary),
            DataType::LargeBinary => Ok(FieldKind::LargeBinary),
            DataType::Null => Ok(FieldKind::Null),
            other => Err(CoreError::Unsupported(format!(
                "RowCodec: unsupported data type {other:?} \
                 (List/Struct/Map and friends not yet implemented)"
            ))),
        }
    }
}

impl RowCodec {
    /// Build a codec for the given schema.
    ///
    /// Returns an error if any field has a data type the codec doesn't yet
    /// support (List/Struct/Map). Caller may fall back to the legacy IPC
    /// path in that case.
    pub fn new(schema: SchemaRef) -> Result<Self> {
        let fields: Result<Vec<_>> = schema
            .fields()
            .iter()
            .map(|f| FieldKind::try_from_data_type(f.data_type()))
            .collect();
        let fields = fields?;
        let null_bitmap_bytes = (schema.fields().len() + 7) / 8;
        Ok(Self {
            schema,
            fields,
            null_bitmap_bytes,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Encode row `row_idx` of `batch` as compact bytes.
    ///
    /// Panics if `row_idx >= batch.num_rows()`. Returns an error if a column's
    /// array type doesn't match the codec's recorded `FieldKind` (would indicate
    /// caller bypassed schema reconciliation).
    pub fn encode_row(&self, batch: &RecordBatch, row_idx: usize) -> Result<Vec<u8>> {
        debug_assert!(row_idx < batch.num_rows());
        // Pre-size: null bitmap + cheap upper bound for fixed-width fields.
        // Variable-length fields grow Vec dynamically.
        let mut out = Vec::with_capacity(self.null_bitmap_bytes + self.fields.len() * 8);
        out.resize(self.null_bitmap_bytes, 0u8);

        for (idx, kind) in self.fields.iter().enumerate() {
            let arr = batch.column(idx);
            if arr.is_null(row_idx) {
                out[idx / 8] |= 1u8 << (idx % 8);
                continue;
            }
            self.encode_field(idx, kind, arr.as_ref(), row_idx, &mut out)?;
        }

        Ok(out)
    }

    /// Decode bytes back into a single-row `RecordBatch` with the codec's schema.
    pub fn decode_row(&self, bytes: &[u8]) -> Result<RecordBatch> {
        if bytes.len() < self.null_bitmap_bytes {
            return Err(CoreError::ReadFileSliceError(format!(
                "RowCodec::decode_row: input too short for null bitmap \
                 (got {} bytes, need ≥ {})",
                bytes.len(),
                self.null_bitmap_bytes,
            )));
        }
        let null_bitmap = &bytes[..self.null_bitmap_bytes];
        let mut cursor = self.null_bitmap_bytes;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.fields.len());

        for (idx, kind) in self.fields.iter().enumerate() {
            let is_null = (null_bitmap[idx / 8] >> (idx % 8)) & 1 == 1;
            let array = self.decode_field(kind, is_null, bytes, &mut cursor)?;
            columns.push(array);
        }

        RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| {
            CoreError::ReadFileSliceError(format!("RowCodec::decode_row: build batch: {e}"))
        })
    }

    // ── encode helpers ────────────────────────────────────────────────────────

    fn encode_field(
        &self,
        idx: usize,
        kind: &FieldKind,
        arr: &dyn Array,
        row: usize,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        match kind {
            FieldKind::Int8 => out.extend_from_slice(&arr.as_primitive::<Int8Type>().value(row).to_le_bytes()),
            FieldKind::Int16 => out.extend_from_slice(&arr.as_primitive::<Int16Type>().value(row).to_le_bytes()),
            FieldKind::Int32 => out.extend_from_slice(&arr.as_primitive::<Int32Type>().value(row).to_le_bytes()),
            FieldKind::Int64 => out.extend_from_slice(&arr.as_primitive::<Int64Type>().value(row).to_le_bytes()),
            FieldKind::UInt8 => out.extend_from_slice(&arr.as_primitive::<UInt8Type>().value(row).to_le_bytes()),
            FieldKind::UInt16 => out.extend_from_slice(&arr.as_primitive::<UInt16Type>().value(row).to_le_bytes()),
            FieldKind::UInt32 => out.extend_from_slice(&arr.as_primitive::<UInt32Type>().value(row).to_le_bytes()),
            FieldKind::UInt64 => out.extend_from_slice(&arr.as_primitive::<UInt64Type>().value(row).to_le_bytes()),
            FieldKind::Float32 => out.extend_from_slice(&arr.as_primitive::<Float32Type>().value(row).to_le_bytes()),
            FieldKind::Float64 => out.extend_from_slice(&arr.as_primitive::<Float64Type>().value(row).to_le_bytes()),
            FieldKind::Boolean => {
                let v = arr.as_boolean().value(row);
                out.push(if v { 1 } else { 0 });
            }
            FieldKind::Date32 => out.extend_from_slice(&arr.as_primitive::<Date32Type>().value(row).to_le_bytes()),
            FieldKind::Date64 => out.extend_from_slice(&arr.as_primitive::<Date64Type>().value(row).to_le_bytes()),
            FieldKind::Timestamp(unit, _) => {
                let v: i64 = match unit {
                    TimeUnit::Second => arr.as_primitive::<TimestampSecondType>().value(row),
                    TimeUnit::Millisecond => arr.as_primitive::<TimestampMillisecondType>().value(row),
                    TimeUnit::Microsecond => arr.as_primitive::<TimestampMicrosecondType>().value(row),
                    TimeUnit::Nanosecond => arr.as_primitive::<TimestampNanosecondType>().value(row),
                };
                out.extend_from_slice(&v.to_le_bytes());
            }
            FieldKind::Decimal128(_, _) => out.extend_from_slice(&arr.as_primitive::<Decimal128Type>().value(row).to_le_bytes()),
            FieldKind::Utf8 => write_var_bytes(out, arr.as_string::<i32>().value(row).as_bytes()),
            FieldKind::LargeUtf8 => write_var_bytes(out, arr.as_string::<i64>().value(row).as_bytes()),
            FieldKind::Binary => write_var_bytes(out, arr.as_binary::<i32>().value(row)),
            FieldKind::LargeBinary => write_var_bytes(out, arr.as_binary::<i64>().value(row)),
            FieldKind::Null => {
                return Err(CoreError::ReadFileSliceError(format!(
                    "RowCodec: column[{idx}] is DataType::Null but row {row} not null"
                )));
            }
        }
        Ok(())
    }

    // ── decode helpers ────────────────────────────────────────────────────────

    fn decode_field(
        &self,
        kind: &FieldKind,
        is_null: bool,
        bytes: &[u8],
        cursor: &mut usize,
    ) -> Result<ArrayRef> {
        macro_rules! primitive {
            ($builder:ty, $ty:ty) => {{
                let mut b = <$builder>::with_capacity(1);
                if is_null {
                    b.append_null();
                } else {
                    let sz = std::mem::size_of::<$ty>();
                    let v = <$ty>::from_le_bytes(read_fixed::<{ std::mem::size_of::<$ty>() }>(bytes, cursor)?);
                    b.append_value(v);
                    let _ = sz;
                }
                Ok(Arc::new(b.finish()) as ArrayRef)
            }};
        }

        match kind {
            FieldKind::Int8 => primitive!(Int8Builder, i8),
            FieldKind::Int16 => primitive!(Int16Builder, i16),
            FieldKind::Int32 => primitive!(Int32Builder, i32),
            FieldKind::Int64 => primitive!(Int64Builder, i64),
            FieldKind::UInt8 => primitive!(UInt8Builder, u8),
            FieldKind::UInt16 => primitive!(UInt16Builder, u16),
            FieldKind::UInt32 => primitive!(UInt32Builder, u32),
            FieldKind::UInt64 => primitive!(UInt64Builder, u64),
            FieldKind::Float32 => primitive!(Float32Builder, f32),
            FieldKind::Float64 => primitive!(Float64Builder, f64),
            FieldKind::Boolean => {
                let mut b = BooleanBuilder::with_capacity(1);
                if is_null {
                    b.append_null();
                } else {
                    let v = read_fixed::<1>(bytes, cursor)?[0];
                    b.append_value(v != 0);
                }
                Ok(Arc::new(b.finish()) as ArrayRef)
            }
            FieldKind::Date32 => primitive!(Date32Builder, i32),
            FieldKind::Date64 => primitive!(Date64Builder, i64),
            FieldKind::Timestamp(unit, tz) => {
                let v_opt = if is_null {
                    None
                } else {
                    Some(i64::from_le_bytes(read_fixed::<8>(bytes, cursor)?))
                };
                let arr: ArrayRef = match unit {
                    TimeUnit::Second => {
                        let mut b = TimestampSecondBuilder::with_capacity(1);
                        match v_opt { Some(v) => b.append_value(v), None => b.append_null() }
                        let arr = b.finish();
                        match tz {
                            Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                            None => Arc::new(arr),
                        }
                    }
                    TimeUnit::Millisecond => {
                        let mut b = TimestampMillisecondBuilder::with_capacity(1);
                        match v_opt { Some(v) => b.append_value(v), None => b.append_null() }
                        let arr = b.finish();
                        match tz {
                            Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                            None => Arc::new(arr),
                        }
                    }
                    TimeUnit::Microsecond => {
                        let mut b = TimestampMicrosecondBuilder::with_capacity(1);
                        match v_opt { Some(v) => b.append_value(v), None => b.append_null() }
                        let arr = b.finish();
                        match tz {
                            Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                            None => Arc::new(arr),
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let mut b = TimestampNanosecondBuilder::with_capacity(1);
                        match v_opt { Some(v) => b.append_value(v), None => b.append_null() }
                        let arr = b.finish();
                        match tz {
                            Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                            None => Arc::new(arr),
                        }
                    }
                };
                Ok(arr)
            }
            FieldKind::Decimal128(p, s) => {
                let mut b = Decimal128Builder::with_capacity(1).with_precision_and_scale(*p, *s).map_err(|e| {
                    CoreError::ReadFileSliceError(format!("Decimal128 builder: {e}"))
                })?;
                if is_null {
                    b.append_null();
                } else {
                    b.append_value(i128::from_le_bytes(read_fixed::<16>(bytes, cursor)?));
                }
                Ok(Arc::new(b.finish()) as ArrayRef)
            }
            FieldKind::Utf8 => {
                let mut b = StringBuilder::new();
                if is_null { b.append_null(); }
                else {
                    let s = read_var_bytes(bytes, cursor)?;
                    let s = std::str::from_utf8(s).map_err(|e| CoreError::ReadFileSliceError(format!("Utf8 not valid: {e}")))?;
                    b.append_value(s);
                }
                Ok(Arc::new(b.finish()) as ArrayRef)
            }
            FieldKind::LargeUtf8 => {
                let mut b = LargeStringBuilder::new();
                if is_null { b.append_null(); }
                else {
                    let s = read_var_bytes(bytes, cursor)?;
                    let s = std::str::from_utf8(s).map_err(|e| CoreError::ReadFileSliceError(format!("LargeUtf8 not valid: {e}")))?;
                    b.append_value(s);
                }
                Ok(Arc::new(b.finish()) as ArrayRef)
            }
            FieldKind::Binary => {
                let mut b = BinaryBuilder::new();
                if is_null { b.append_null(); }
                else { b.append_value(read_var_bytes(bytes, cursor)?); }
                Ok(Arc::new(b.finish()) as ArrayRef)
            }
            FieldKind::LargeBinary => {
                let mut b = LargeBinaryBuilder::new();
                if is_null { b.append_null(); }
                else { b.append_value(read_var_bytes(bytes, cursor)?); }
                Ok(Arc::new(b.finish()) as ArrayRef)
            }
            FieldKind::Null => {
                let mut b = NullBuilder::new();
                b.append_null();
                Ok(Arc::new(b.finish()) as ArrayRef)
            }
        }
    }
}

// ── byte-level helpers ────────────────────────────────────────────────────────

fn write_var_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn read_fixed<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N]> {
    if *cursor + N > bytes.len() {
        return Err(CoreError::ReadFileSliceError(format!(
            "RowCodec::decode: short read of {N} bytes at offset {} (input {} bytes)",
            *cursor,
            bytes.len()
        )));
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&bytes[*cursor..*cursor + N]);
    *cursor += N;
    Ok(arr)
}

fn read_var_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8]> {
    let len = u32::from_le_bytes(read_fixed::<4>(bytes, cursor)?) as usize;
    if *cursor + len > bytes.len() {
        return Err(CoreError::ReadFileSliceError(format!(
            "RowCodec::decode: variable-length read of {len} bytes at offset {} (input {} bytes)",
            *cursor,
            bytes.len()
        )));
    }
    let slice = &bytes[*cursor..*cursor + len];
    *cursor += len;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{Field, Schema};

    fn schema_typical() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(18, 6), true),
            Field::new("active", DataType::Boolean, true),
            Field::new("event_date", DataType::Date32, true),
            Field::new("event_ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]))
    }

    fn batch_typical_one_row() -> RecordBatch {
        RecordBatch::try_new(
            schema_typical(),
            vec![
                Arc::new(StringArray::from(vec!["record-key-42"])),
                Arc::new(Int64Array::from(vec![1_700_000_000_000_i64])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12345_678901i128)])
                        .with_precision_and_scale(18, 6)
                        .unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(Date32Array::from(vec![Some(19_500)])),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1_700_000_123_456_789)])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn round_trip_typical_row() {
        let codec = RowCodec::new(schema_typical()).unwrap();
        let batch = batch_typical_one_row();
        let bytes = codec.encode_row(&batch, 0).unwrap();
        let decoded = codec.decode_row(&bytes).unwrap();
        assert_eq!(decoded.num_rows(), 1);
        assert_eq!(decoded.schema().fields(), batch.schema().fields());

        // Spot-check each column
        assert_eq!(decoded.column(0).as_string::<i32>().value(0), "record-key-42");
        assert_eq!(decoded.column(1).as_primitive::<Int64Type>().value(0), 1_700_000_000_000);
        assert_eq!(decoded.column(2).as_primitive::<Decimal128Type>().value(0), 12345_678901i128);
        assert!(decoded.column(3).as_boolean().value(0));
        assert_eq!(decoded.column(4).as_primitive::<Date32Type>().value(0), 19_500);
        assert_eq!(
            decoded.column(5).as_primitive::<TimestampMicrosecondType>().value(0),
            1_700_000_123_456_789
        );
    }

    #[test]
    fn round_trip_with_nulls() {
        let codec = RowCodec::new(schema_typical()).unwrap();
        let batch = RecordBatch::try_new(
            schema_typical(),
            vec![
                Arc::new(StringArray::from(vec!["nullable-row"])),
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(
                    Decimal128Array::from(vec![None::<i128>])
                        .with_precision_and_scale(18, 6)
                        .unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![None::<bool>])),
                Arc::new(Date32Array::from(vec![None::<i32>])),
                Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>])),
            ],
        )
        .unwrap();
        let bytes = codec.encode_row(&batch, 0).unwrap();
        let decoded = codec.decode_row(&bytes).unwrap();
        assert!(!decoded.column(0).is_null(0));
        assert!(!decoded.column(1).is_null(0));
        assert!(decoded.column(2).is_null(0));
        assert!(decoded.column(3).is_null(0));
        assert!(decoded.column(4).is_null(0));
        assert!(decoded.column(5).is_null(0));
    }

    #[test]
    fn compact_size_under_300_bytes_for_typical_row() {
        // Verify the bytes-per-row claim (target ~200-300 B for a typical Hudi
        // schema with 5 string meta cols + a handful of typed cols).
        let codec = RowCodec::new(schema_typical()).unwrap();
        let batch = batch_typical_one_row();
        let bytes = codec.encode_row(&batch, 0).unwrap();
        // 1 byte null bitmap (6 fields) + 13 bytes "record-key-42" + 4 byte len = 17
        // + 8 (i64) + 16 (Decimal128) + 1 (bool) + 4 (Date32) + 8 (Timestamp) = 54
        // Total: 1 + 17 + 8 + 16 + 1 + 4 + 8 = 55
        assert!(bytes.len() < 300, "row size {} exceeded budget", bytes.len());
        assert_eq!(bytes.len(), 55, "row layout shifted unexpectedly");
    }

    #[test]
    fn rejects_unsupported_data_types() {
        // Lists/structs/maps aren't supported in v1
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));
        let err = RowCodec::new(schema).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("unsupported data type"), "got: {msg}");
    }

    #[test]
    fn float_round_trip_preserves_bit_pattern() {
        // Use a value with a non-trivial mantissa to ensure no precision loss.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let codec = RowCodec::new(schema.clone()).unwrap();
        let original = std::f64::consts::PI;
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![original]))]).unwrap();
        let bytes = codec.encode_row(&batch, 0).unwrap();
        let decoded = codec.decode_row(&bytes).unwrap();
        assert_eq!(decoded.column(0).as_primitive::<Float64Type>().value(0).to_bits(), original.to_bits());
    }

    #[test]
    fn empty_string_distinguished_from_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let codec = RowCodec::new(schema.clone()).unwrap();
        // empty string
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec![Some("")]))]).unwrap();
        // null string
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![None::<&str>]))]).unwrap();
        let bytes1 = codec.encode_row(&batch1, 0).unwrap();
        let bytes2 = codec.encode_row(&batch2, 0).unwrap();
        let d1 = codec.decode_row(&bytes1).unwrap();
        let d2 = codec.decode_row(&bytes2).unwrap();
        assert!(!d1.column(0).is_null(0));
        assert_eq!(d1.column(0).as_string::<i32>().value(0), "");
        assert!(d2.column(0).is_null(0));
    }
}
