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

#![allow(dead_code)]

//! Mirrors Java's `org.apache.hudi.avro.AvroSchemaUtils`.
//!
//! Provides schema comparison utilities for checking projection equivalence
//! between Arrow schemas.

use arrow_schema::{DataType, Fields, SchemaRef};

/// Check if two schemas are projection-equivalent.
///
/// Mirrors Java's `AvroSchemaUtils.areSchemasProjectionEquivalent()` which
/// delegates to `AvroSchemaComparatorForRecordProjection`.
///
/// The comparison recurses through nested types (Struct, List, Map, etc.):
/// - **Record/Struct fields**: matched pairwise by case-insensitive name, then
///   types are compared recursively.
/// - **List / LargeList**: element types compared recursively.
/// - **Map**: entry struct (key + value) compared recursively.
/// - **All other types** (primitives, FixedSizeBinary, Decimal, Dictionary,
///   etc.): standard [`DataType`] equality which already checks parameters
///   such as precision, scale, and fixed-size width.
///
/// Nullable flag on fields is intentionally ignored — Java unwraps nullable
/// unions before comparing, which produces the same effect.
pub fn are_schemas_projection_equivalent(a: &SchemaRef, b: &SchemaRef) -> bool {
    record_fields_equivalent(a.fields(), b.fields())
}

/// Compare two sets of record fields for projection equivalence.
///
/// Fields are compared pairwise in order.  Two fields match when their names
/// are equal (case-insensitive, mirroring Java's
/// `AvroSchemaComparatorForRecordProjection.validateField`) and their data
/// types are recursively projection-equivalent.
fn record_fields_equivalent(a: &Fields, b: &Fields) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(fa, fb)| {
        fa.name().eq_ignore_ascii_case(fb.name())
            && types_equivalent(fa.data_type(), fb.data_type())
    })
}

/// Recursively compare two Arrow [`DataType`]s for projection equivalence.
/// See [`are_schemas_projection_equivalent`] for the per-type rules.
fn types_equivalent(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::Struct(fa), DataType::Struct(fb)) => record_fields_equivalent(fa, fb),
        (DataType::List(ea), DataType::List(eb))
        | (DataType::LargeList(ea), DataType::LargeList(eb)) => {
            types_equivalent(ea.data_type(), eb.data_type())
        }
        (DataType::Map(ea, sa), DataType::Map(eb, sb)) => {
            sa == sb && types_equivalent(ea.data_type(), eb.data_type())
        }
        _ => a == b,
    }
}

/// Whether two Avro schema JSONs describe the same schema, for the purpose of
/// deciding that resolving one against the other would be a no-op.
///
/// Needed because the two JSONs that meet at that decision are never the same
/// string even when they are the same schema. The reader side is
/// [`append_mandatory_fields_avro_json`]'s output, i.e. `serde_json::to_string`
/// of a parsed `Value` — and `serde_json` is built here without `preserve_order`,
/// so its objects are `BTreeMap`s and it re-emits keys **alphabetically**. The
/// writer side is whatever Java Avro's `Schema.toString()` wrote into the file,
/// which is not key-sorted. A `!=` on the two strings is therefore always true.
///
/// The comparison is structural (`serde_json::Value` equality, which is
/// key-order and whitespace insensitive) with `"doc"` stripped, and deliberately
/// nothing else:
///
/// * **Not** Avro's Parsing Canonical Form, the obvious candidate. It strips
///   `logicalType` along with `doc`/`default`/`aliases`, so a writer `"long"` and
///   a reader `{"type":"long","logicalType":"timestamp-micros"}` share a canonical
///   form — and skipping resolution there would decode `Int64` where the required
///   schema says `Timestamp`. Equal canonical forms do not license skipping.
/// * `doc` is dropped because it is prose: it cannot change a decoded value, a
///   type, or a branch. Only from schema nodes — a `default` value is data, and
///   a `doc` key inside one is left alone.
/// * Everything else counts as a difference and makes the read resolve, including
///   `default`, `aliases` and `avro.java.string`. Some of those would in fact be
///   harmless to ignore; being wrong in this direction costs one resolving
///   decoder, and being wrong in the other costs correctness.
pub(crate) fn avro_schema_json_equivalent(a_json: &str, b_json: &str) -> crate::Result<bool> {
    use serde_json::Value;

    fn strip_doc(value: &mut Value) {
        match value {
            Value::Object(map) => {
                map.remove("doc");
                // A `default` is a VALUE, not a schema node: a map or record
                // default can hold a key spelled `doc`, and that key is data.
                for (_, v) in map.iter_mut().filter(|(k, _)| k.as_str() != "default") {
                    strip_doc(v);
                }
            }
            Value::Array(items) => items.iter_mut().for_each(strip_doc),
            _ => {}
        }
    }

    let parse = |json: &str| -> crate::Result<Value> {
        serde_json::from_str(json)
            .map_err(|e| crate::error::CoreError::Schema(format!("bad avro json: {e}")))
    };
    let (mut a, mut b) = (parse(a_json)?, parse(b_json)?);
    strip_doc(&mut a);
    strip_doc(&mut b);
    Ok(a == b)
}

/// A writer schema that makes every field of `reader_json` that declares a
/// non-null `default` reader-only, at every depth, while leaving every other
/// field resolving to itself.
///
/// It is `reader_json` with each such field renamed to a name nothing in its
/// record answers to (and stripped of its aliases, which would otherwise match
/// it back). Resolving `reader_json` against it has no promotion to refuse —
/// every surviving field is its own type — so `arrow-avro` always builds it,
/// and stamps each renamed field's declared default on the result exactly as it
/// does for a field a real writer never wrote.
///
/// This is what the log rewrite takes its defaults from when the block's real
/// writer schema cannot be resolved against the reader: that refusal is the
/// very evolution the rewrite exists for, and a rewrite without defaults would
/// fill a reader-only field with NULL where the schema declares its value.
/// A default stamped on a field the real writer DID write is inert, because a
/// default is consulted only for a field absent from the batch.
///
/// A field whose default is `null` is left alone. Filling it with its default
/// and filling it with no default are the same null, and renaming it would hide
/// the defaults of any record nested inside it — the common shape, since a
/// nullable record column is declared `"default": null`. A field with a non-null
/// default on a record type does hide its nested defaults, but a default on a
/// nested type is refused when it is materialised anyway
/// (`batch_evolution::constant_array_from_avro_default`).
///
/// Only schema positions are walked (`fields`, a field's `type`, `items`,
/// `values`, union branches), never a `default` payload. A renamed field keeps
/// its type, so a named type it defines is still defined for any later
/// reference to it.
pub(crate) fn defaults_carrier_writer_json(reader_json: &str) -> crate::Result<String> {
    use serde_json::Value;

    fn walk(node: &mut Value) {
        match node {
            Value::Array(branches) => branches.iter_mut().for_each(walk),
            Value::Object(map) => {
                if let Some(Value::Array(fields)) = map.get_mut("fields") {
                    let taken: std::collections::HashSet<String> = fields
                        .iter()
                        .flat_map(|f| {
                            let aliases = f["aliases"].as_array().cloned().unwrap_or_default();
                            std::iter::once(f["name"].clone()).chain(aliases)
                        })
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect();
                    let mut next = 0usize;
                    for field in fields.iter_mut() {
                        let Value::Object(field) = field else {
                            continue;
                        };
                        if let Some(ty) = field.get_mut("type") {
                            walk(ty);
                        }
                        if field.get("default").is_some_and(|d| !d.is_null()) {
                            let fresh = loop {
                                let candidate = format!("_default_carrier_{next}");
                                next += 1;
                                if !taken.contains(&candidate) {
                                    break candidate;
                                }
                            };
                            field.insert("name".to_string(), Value::String(fresh));
                            field.remove("aliases");
                        }
                    }
                }
                for key in ["items", "values", "type"] {
                    if let Some(child @ (Value::Object(_) | Value::Array(_))) = map.get_mut(key) {
                        walk(child);
                    }
                }
            }
            _ => {}
        }
    }

    let mut schema: Value = serde_json::from_str(reader_json)
        .map_err(|e| crate::error::CoreError::Schema(format!("bad reader avro json: {e}")))?;
    walk(&mut schema);
    serde_json::to_string(&schema)
        .map_err(|e| crate::error::CoreError::Schema(format!("serialize avro json: {e}")))
}

/// Append top-level field definitions (copied verbatim from `source_json`) to
/// `base_json` for every name in `field_names` not already present in base.
///
/// Mirrors Java's `AvroSchemaUtils.appendFieldsToSchemaDedupNested` for the
/// top-level mandatory-merge-field case used by `generateRequiredSchema`.
pub fn append_mandatory_fields_avro_json(
    base_json: &str,
    source_json: &str,
    field_names: &[&str],
) -> crate::Result<String> {
    use serde_json::Value;
    let mut base: Value = serde_json::from_str(base_json)
        .map_err(|e| crate::error::CoreError::Schema(format!("bad base avro json: {e}")))?;
    let source: Value = serde_json::from_str(source_json)
        .map_err(|e| crate::error::CoreError::Schema(format!("bad source avro json: {e}")))?;

    let source_fields = source["fields"].as_array().cloned().unwrap_or_default();

    let base_fields = base["fields"]
        .as_array_mut()
        .ok_or_else(|| crate::error::CoreError::Schema("base avro json has no fields".into()))?;
    for name in field_names {
        // Covers both pre-existing base fields and ones pushed by earlier iterations.
        if base_fields.iter().any(|f| f["name"] == *name) {
            continue;
        }
        if let Some(def) = source_fields.iter().find(|f| f["name"] == *name) {
            base_fields.push(def.clone());
        }
        // Absent from source: skip — Java throws only for fields it already
        // resolved from tableSchema; our callers pass table-derived names.
    }
    serde_json::to_string(&base)
        .map_err(|e| crate::error::CoreError::Schema(format!("serialize avro json: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Helper: build an Arrow schema from `(name, DataType)` pairs.
    fn make_schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    /// Helper: build a simple schema where every field is Utf8.
    fn make_simple_schema(names: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))
    }

    // =========================================================================
    // Ported from Java TestAvroSchemaUtils.java — record-level tests
    // =========================================================================

    /// Java: testAreSchemasProjectionEquivalentRecordSchemas
    /// Two record schemas with the same field name are projection-equivalent
    /// regardless of schema/record name (Arrow schemas have no "record name").
    #[test]
    fn test_are_schemas_projection_equivalent_record_schemas() {
        let s1 = make_schema(&[("f1", DataType::Int32)]);
        let s2 = make_schema(&[("f1", DataType::Int32)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentFieldCountInRecords
    #[test]
    fn test_are_schemas_projection_equivalent_different_field_count_in_records() {
        let s1 = make_schema(&[("a", DataType::Int32)]);
        let s2: SchemaRef = Arc::new(Schema::empty());
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentNestedRecordSchemas
    /// Nested struct fields — top-level field names match so these are equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_nested_record_schemas() {
        let inner1 = DataType::Struct(vec![Field::new("x", DataType::Utf8, true)].into());
        let inner2 = DataType::Struct(vec![Field::new("x", DataType::Utf8, true)].into());
        let s1 = make_schema(&[("inner", inner1)]);
        let s2 = make_schema(&[("inner", inner2)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentArraySchemas
    /// Schemas with identically-named array fields.
    #[test]
    fn test_are_schemas_projection_equivalent_array_schemas() {
        let s1 = make_schema(&[(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);
        let s2 = make_schema(&[(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentElementTypeInArray
    /// Same field name but different list element types → not equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_different_element_type_in_array() {
        let s1 = make_schema(&[(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);
        let s2 = make_schema(&[(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        )]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentMapSchemas
    #[test]
    fn test_are_schemas_projection_equivalent_map_schemas() {
        let s1 = make_schema(&[(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        let s2 = make_schema(&[(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentMapValueTypes
    /// Same field name but different map value types → not equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_different_map_value_types() {
        let s1 = make_schema(&[(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        let s2 = make_schema(&[(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentNullableSchemaComparison
    /// One field nullable, the other not — same field name and type → equivalent.
    /// Java unwraps nullable unions before comparing; Arrow nullable is a field
    /// property that we intentionally ignore, producing the same result.
    #[test]
    fn test_are_schemas_projection_equivalent_nullable_schema_comparison() {
        let s1 = make_schema(&[("f", DataType::Int32)]);
        // In Arrow, nullable is a field property, not a union wrapper.
        let s2 = Arc::new(Schema::new(vec![Field::new("f", DataType::Int32, false)]));
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentListVsString
    /// Same field name but List type vs String type → not equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_list_vs_string() {
        let s1 = make_schema(&[(
            "f",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);
        let s2 = make_schema(&[("f", DataType::Utf8)]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
        assert!(!are_schemas_projection_equivalent(&s2, &s1));
    }

    /// Java: testAreSchemasProjectionEquivalentMapVsString
    /// Same field name but Map type vs String type → not equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_map_vs_string() {
        let s1 = make_schema(&[(
            "f",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        )]);
        let s2 = make_schema(&[("f", DataType::Utf8)]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
        assert!(!are_schemas_projection_equivalent(&s2, &s1));
    }

    /// Java: testAreSchemasProjectionEquivalentEqualFixedSchemas
    #[test]
    fn test_are_schemas_projection_equivalent_equal_fixed_schemas() {
        let s1 = make_schema(&[("f", DataType::FixedSizeBinary(16))]);
        let s2 = make_schema(&[("f", DataType::FixedSizeBinary(16))]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentFixedSize
    /// Same field name but different FixedSizeBinary sizes → not equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_different_fixed_size() {
        let s1 = make_schema(&[("f", DataType::FixedSizeBinary(8))]);
        let s2 = make_schema(&[("f", DataType::FixedSizeBinary(4))]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentEnums
    /// Arrow uses Dictionary encoding as the closest analog to Avro enums.
    #[test]
    fn test_are_schemas_projection_equivalent_enums() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let s1 = make_schema(&[("e", dict_type.clone())]);
        let s2 = make_schema(&[("e", dict_type)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentEnumSymbols
    /// Different Dictionary index types model incompatible enum encodings → not
    /// equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_different_enum_symbols() {
        let s1 = make_schema(&[(
            "e",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        )]);
        let s2 = make_schema(&[(
            "e",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        )]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentEnumSymbolSubset
    /// Avro allows the first enum to be a prefix-subset of the second's symbols.
    /// Arrow Dictionary types do not carry symbol lists, so two identical
    /// Dictionary types are always equivalent in both directions.
    #[test]
    fn test_are_schemas_projection_equivalent_enum_symbol_subset() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let s1 = make_schema(&[("e", dict_type.clone())]);
        let s2 = make_schema(&[("e", dict_type)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
        assert!(are_schemas_projection_equivalent(&s2, &s1));
    }

    /// Java: testAreSchemasProjectionEquivalentEqualDecimalLogicalTypes
    #[test]
    fn test_are_schemas_projection_equivalent_equal_decimal_logical_types() {
        let s1 = make_schema(&[("d", DataType::Decimal128(12, 2))]);
        let s2 = make_schema(&[("d", DataType::Decimal128(12, 2))]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentDifferentPrecision
    /// Same field name but different Decimal precision → not equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_different_precision() {
        let s1 = make_schema(&[("d", DataType::Decimal128(12, 2))]);
        let s2 = make_schema(&[("d", DataType::Decimal128(13, 2))]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentLogicalVsNoLogicalType
    /// Decimal field vs plain Binary field — same field name but different types
    /// → not equivalent.
    #[test]
    fn test_are_schemas_projection_equivalent_logical_vs_no_logical_type() {
        let s1 = make_schema(&[("d", DataType::Decimal128(10, 2))]);
        let s2 = make_schema(&[("d", DataType::Binary)]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    /// Java: testAreSchemasProjectionEquivalentSameReferenceSchema
    #[test]
    fn test_are_schemas_projection_equivalent_same_reference_schema() {
        let s = make_simple_schema(&["f"]);
        assert!(are_schemas_projection_equivalent(&s, &s));
    }

    /// Java: testAreSchemasProjectionEquivalentNullSchemaComparison
    /// Rust uses references so null is not applicable.  We test empty schemas
    /// and schemas with different names instead to cover the boundary case.
    #[test]
    fn test_are_schemas_projection_equivalent_empty_schemas() {
        let s1: SchemaRef = Arc::new(Schema::empty());
        let s2: SchemaRef = Arc::new(Schema::empty());
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    // =========================================================================
    // Additional edge-case tests (no direct Java equivalent)
    // =========================================================================

    #[test]
    fn test_are_schemas_projection_equivalent_different_field_names() {
        let s1 = make_simple_schema(&["a"]);
        let s2 = make_simple_schema(&["b"]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    #[test]
    fn test_are_schemas_projection_equivalent_case_insensitive_field_names() {
        let s1 = make_schema(&[("Field_A", DataType::Int32)]);
        let s2 = make_schema(&[("field_a", DataType::Int32)]);
        assert!(are_schemas_projection_equivalent(&s1, &s2));
    }

    #[test]
    fn test_are_schemas_projection_equivalent_field_order_matters() {
        let s1 = make_simple_schema(&["a", "b"]);
        let s2 = make_simple_schema(&["b", "a"]);
        assert!(!are_schemas_projection_equivalent(&s1, &s2));
    }

    #[test]
    fn test_append_mandatory_fields_avro_json() {
        let data = r#"{"type":"record","name":"rec","fields":[
            {"name":"_hoodie_record_key","type":["null","string"],"default":null},
            {"name":"id","type":"int"},
            {"name":"price","type":["null","double"],"default":null}]}"#;
        let requested = r#"{"type":"record","name":"rec","fields":[
            {"name":"price","type":["null","double"],"default":null}]}"#;

        let out =
            append_mandatory_fields_avro_json(requested, data, &["_hoodie_record_key", "price"])
                .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let fields = v["fields"].as_array().unwrap();
        // price kept first (requested order), _hoodie_record_key appended, no duplicate price
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "price");
        assert_eq!(fields[1]["name"], "_hoodie_record_key");
        // appended field definition copied verbatim from data schema (type + default)
        assert_eq!(fields[1]["type"], serde_json::json!(["null", "string"]));
    }

    #[test]
    fn test_append_mandatory_fields_avro_json_missing_in_source_is_skipped() {
        let data = r#"{"type":"record","name":"rec","fields":[{"name":"id","type":"int"}]}"#;
        let requested = r#"{"type":"record","name":"rec","fields":[{"name":"id","type":"int"}]}"#;
        let out = append_mandatory_fields_avro_json(requested, data, &["not_a_field"]).unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["fields"].as_array().unwrap().len(), 1);
    }

    /// The comparison the HFile cost gate runs. What must be equal is what the
    /// producers actually differ by (key order, whitespace, prose); what must NOT
    /// be equal is anything that changes a decoded type or value.
    #[test]
    fn avro_schema_json_equivalence_ignores_key_order_and_doc_but_nothing_else() {
        let base = r#"{"type":"record","name":"R","namespace":"ns","fields":[
            {"name":"a","type":"long"},{"name":"b","type":["null","string"],"default":null}]}"#;

        // Key order and whitespace: the exact difference between Java Avro's
        // `Schema.toString()` and `serde_json`'s key-sorted re-serialization.
        let reordered = r#"{"fields":[{"type":"long","name":"a"},
            {"default":null,"name":"b","type":["null","string"]}],
            "namespace":"ns","name":"R","type":"record"}"#;
        assert!(avro_schema_json_equivalent(base, reordered).unwrap());

        // `doc` is prose.
        let documented = r#"{"type":"record","name":"R","namespace":"ns","doc":"hi","fields":[
            {"name":"a","type":"long","doc":"an a"},
            {"name":"b","type":["null","string"],"default":null}]}"#;
        assert!(avro_schema_json_equivalent(base, documented).unwrap());

        // A logical type changes the decoded Arrow type — Avro's Parsing
        // Canonical Form strips it, which is why this is not canonical form.
        let logical = r#"{"type":"record","name":"R","namespace":"ns","fields":[
            {"name":"a","type":{"type":"long","logicalType":"timestamp-micros"}},
            {"name":"b","type":["null","string"],"default":null}]}"#;
        assert!(!avro_schema_json_equivalent(base, logical).unwrap());

        // An added field is the whole point of resolving.
        let evolved = r#"{"type":"record","name":"R","namespace":"ns","fields":[
            {"name":"a","type":"long"},{"name":"b","type":["null","string"],"default":null},
            {"name":"c","type":"boolean","default":false}]}"#;
        assert!(!avro_schema_json_equivalent(base, evolved).unwrap());

        // A changed default changes what a reader-only field is filled with.
        let redefaulted = r#"{"type":"record","name":"R","namespace":"ns","fields":[
            {"name":"a","type":"long"},{"name":"b","type":["null","string"],"default":null},
            {"name":"c","type":"boolean","default":true}]}"#;
        assert!(!avro_schema_json_equivalent(evolved, redefaulted).unwrap());

        // Malformed input is an error, not a false "equivalent".
        assert!(avro_schema_json_equivalent(base, "{not json").is_err());
    }
    /// A `doc` key inside a DEFAULT value is data, not prose, and must count.
    ///
    /// A map- or record-typed field's default is a JSON object, and a key of that
    /// object can be spelled `doc`; stripping it there would judge two schemas
    /// with different defaults equivalent. The control: `doc` on the schema node
    /// that holds the default is still stripped.
    #[test]
    fn avro_schema_json_equivalence_keeps_doc_inside_a_default_value() {
        let with_default = |default: &str, field_doc: &str| {
            format!(
                r#"{{"type":"record","name":"R","fields":[{{"name":"m","doc":"{field_doc}","type":{{"type":"map","values":"string"}},"default":{default}}}]}}"#
            )
        };
        assert!(
            !avro_schema_json_equivalent(
                &with_default(r#"{"doc":"alpha","k":"v"}"#, "same"),
                &with_default(r#"{"doc":"BETA","k":"v"}"#, "same"),
            )
            .unwrap(),
            "map defaults that differ under a `doc` key are different defaults"
        );
        assert!(
            avro_schema_json_equivalent(
                &with_default(r#"{"doc":"alpha","k":"v"}"#, "one"),
                &with_default(r#"{"doc":"alpha","k":"v"}"#, "two"),
            )
            .unwrap(),
            "the field's own `doc` is still prose"
        );
        let record_default = |doc_value: &str| {
            format!(
                r#"{{"type":"record","name":"R","fields":[{{"name":"s","type":{{"type":"record","name":"S","fields":[{{"name":"doc","type":"string"}}]}},"default":{{"doc":"{doc_value}"}}}}]}}"#
            )
        };
        assert!(
            !avro_schema_json_equivalent(&record_default("x"), &record_default("y")).unwrap(),
            "a record default whose field is named `doc` is data"
        );
    }

    /// `defaults_carrier_writer_json` renames away exactly the fields that declare
    /// a default, at every depth, and resolving the reader against the result
    /// stamps each of those defaults — including nested ones — while the fields
    /// without one resolve to themselves.
    #[test]
    fn the_defaults_carrier_makes_every_defaulted_field_reader_only() {
        use arrow_schema::DataType;
        let reader = r#"{"type":"record","name":"R","fields":[
            {"name":"id","type":"long"},
            {"name":"_default_carrier_0","type":"int"},
            {"name":"flag","type":"boolean","default":true,"aliases":["id"]},
            {"name":"s","type":["null",{"type":"record","name":"S","fields":[
                {"name":"x","type":"int","default":5},
                {"name":"y","type":"string"}
            ]}],"default":null},
            {"name":"again","type":["null","S"]},
            {"name":"arr","type":{"type":"array","items":{"type":"record","name":"A","fields":[
                {"name":"a","type":"long","default":9}
            ]}}},
            {"name":"m","type":{"type":"map","values":{"type":"record","name":"V","fields":[
                {"name":"v","type":"string","default":"z"}
            ]}}},
            {"name":"rec_default","type":{"type":"record","name":"D","fields":[
                {"name":"d","type":"int"}
            ]},"default":{"d":1,"fields":[{"name":"not_a_schema","default":0}]}}
        ]}"#;
        let carrier = defaults_carrier_writer_json(reader).unwrap();
        let carrier_value: serde_json::Value = serde_json::from_str(&carrier).unwrap();
        let top: Vec<&str> = carrier_value["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        // `flag` and `rec_default` declare non-null defaults; `s` declares a null
        // one and is kept, so the record inside it is still walked. The name
        // already taken by a real field is skipped, and the alias that would
        // match `id` is gone.
        assert_eq!(
            top,
            [
                "id",
                "_default_carrier_0",
                "_default_carrier_1",
                "s",
                "again",
                "arr",
                "m",
                "_default_carrier_2"
            ]
        );
        assert!(carrier_value["fields"][2].get("aliases").is_none());
        assert_eq!(
            carrier_value["fields"][3]["type"][1]["fields"][0]["name"],
            "_default_carrier_0"
        );
        assert_eq!(
            carrier_value["fields"][7]["default"]["fields"][0]["name"], "not_a_schema",
            "a default payload is data, not a schema, and is never rewritten"
        );

        let resolved = crate::file_group::log_file::avro::AvroBlockDecoder::try_new_with_reader(
            &carrier,
            Some(reader),
            1,
        )
        .expect("a reader always resolves against its own defaults carrier")
        .schema();
        let default_of = |field: &arrow_schema::Field| {
            field
                .metadata()
                .get("avro.field.default")
                .cloned()
                .unwrap_or_default()
        };
        assert_eq!(
            default_of(resolved.field_with_name("flag").unwrap()),
            "true"
        );
        assert_eq!(default_of(resolved.field_with_name("id").unwrap()), "");
        // `again` references `S` by name; the carrier's `S` still defines it.
        for name in ["s", "again"] {
            let DataType::Struct(children) = resolved.field_with_name(name).unwrap().data_type()
            else {
                panic!("{name} is a struct");
            };
            assert_eq!(children.len(), 2);
            assert_eq!(default_of(&children[0]), "5", "{name}.x");
            assert_eq!(default_of(&children[1]), "", "{name}.y");
        }
        let DataType::List(item) = resolved.field_with_name("arr").unwrap().data_type() else {
            panic!("arr is a list");
        };
        let DataType::Struct(a) = item.data_type() else {
            panic!("arr items are records");
        };
        assert_eq!(default_of(&a[0]), "9");
        let DataType::Map(entries, _) = resolved.field_with_name("m").unwrap().data_type() else {
            panic!("m is a map");
        };
        let DataType::Struct(kv) = entries.data_type() else {
            panic!("map entries are a struct");
        };
        let DataType::Struct(v) = kv[1].data_type() else {
            panic!("map values are records");
        };
        assert_eq!(default_of(&v[0]), "\"z\"");
    }
}
