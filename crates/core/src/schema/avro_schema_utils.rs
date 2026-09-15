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

/// `schema` with every field that `reader_json` declares a `default` for
/// carrying that default as `avro.field.default` metadata, at every depth.
///
/// `schema` must be the Arrow conversion of `reader_json` itself; fields are
/// matched by name and nested types by position in the Avro tree (a record's
/// fields, an array's items, a map's values, a union's branches). The metadata
/// value is the default's JSON text, which is how `arrow-avro` stamps it on a
/// schema it resolved and what [`project_batch_to_schema`] reads to fill a field
/// the source never wrote. Nothing else about `schema` changes.
///
/// This is what the log rewrite takes its defaults from. It used to take them
/// from a schema `arrow-avro` resolved against the block's writer, which fails
/// twice over: `arrow-avro` refuses to resolve exactly the evolutions the
/// rewrite exists for (`int -> string`), and when it does resolve, a named type
/// referenced after the record that defines it closes comes back in the
/// WRITER's shape, without the fields — and so without the defaults — the
/// evolution added. The defaults are the reader's, so they are read from the
/// reader schema.
///
/// A name that does not resolve, or a union whose branch count disagrees with
/// the Arrow union, is left unstamped rather than guessed at: the fill then
/// behaves as it would with no default declared. A recursive reference stops at
/// the first repeat.
///
/// [`project_batch_to_schema`]: crate::schema::batch_evolution::project_batch_to_schema
pub(crate) fn with_avro_defaults(
    schema: &arrow_schema::Schema,
    reader_json: &str,
) -> crate::Result<arrow_schema::Schema> {
    use arrow_schema::Field;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::Arc;

    const PRIMITIVES: [&str; 8] = [
        "null", "boolean", "int", "long", "float", "double", "bytes", "string",
    ];

    fn full_name(name: &str, namespace: Option<&str>) -> String {
        match namespace {
            Some(ns) if !name.contains('.') && !ns.is_empty() => format!("{ns}.{name}"),
            _ => name.to_string(),
        }
    }

    /// The namespace a named type's own children resolve names against.
    fn own_namespace<'a>(
        node: &'a serde_json::Map<String, Value>,
        enclosing: Option<&'a str>,
    ) -> Option<&'a str> {
        let name = node.get("name").and_then(Value::as_str);
        match (
            node.get("namespace").and_then(Value::as_str),
            name.and_then(|n| n.rsplit_once('.')),
        ) {
            (_, Some((ns, _))) => Some(ns),
            (Some(ns), None) => Some(ns),
            (None, None) => enclosing,
        }
    }

    /// Every named type (record, enum, fixed) by full name.
    fn register<'a>(
        node: &'a Value,
        namespace: Option<&'a str>,
        named: &mut HashMap<String, &'a Value>,
    ) {
        match node {
            Value::Array(branches) => branches.iter().for_each(|b| register(b, namespace, named)),
            Value::Object(map) => {
                let inner = own_namespace(map, namespace);
                if let (Some(name), Some("record" | "error" | "enum" | "fixed")) = (
                    map.get("name").and_then(Value::as_str),
                    map.get("type").and_then(Value::as_str),
                ) {
                    named.entry(full_name(name, inner)).or_insert(node);
                }
                if let Some(Value::Array(fields)) = map.get("fields") {
                    for field in fields {
                        if let Some(ty) = field.get("type") {
                            register(ty, inner, named);
                        }
                    }
                }
                for key in ["items", "values", "type"] {
                    if let Some(child @ (Value::Object(_) | Value::Array(_))) = map.get(key) {
                        register(child, inner, named);
                    }
                }
            }
            _ => {}
        }
    }

    struct Stamper<'a> {
        named: HashMap<String, &'a Value>,
        in_progress: Vec<String>,
    }

    impl<'a> Stamper<'a> {
        fn stamp_fields(
            &mut self,
            fields_json: &'a [Value],
            fields: &arrow_schema::Fields,
            namespace: Option<&'a str>,
        ) -> Vec<arrow_schema::FieldRef> {
            fields
                .iter()
                .map(|field| {
                    let Some(json) = fields_json
                        .iter()
                        .find(|f| f.get("name").and_then(Value::as_str) == Some(field.name()))
                    else {
                        return field.clone();
                    };
                    let data_type = match json.get("type") {
                        Some(ty) => self.stamp_type(ty, field.data_type(), namespace),
                        None => field.data_type().clone(),
                    };
                    let mut metadata = field.metadata().clone();
                    if let Some(default) = json.get("default") {
                        metadata.insert(
                            crate::schema::batch_evolution::AVRO_FIELD_DEFAULT_KEY.to_string(),
                            default.to_string(),
                        );
                    }
                    Arc::new(
                        Field::new(field.name(), data_type, field.is_nullable())
                            .with_metadata(metadata),
                    )
                })
                .collect()
        }

        fn stamp_child(
            &mut self,
            ty: &'a Value,
            field: &arrow_schema::FieldRef,
            namespace: Option<&'a str>,
        ) -> arrow_schema::FieldRef {
            let data_type = self.stamp_type(ty, field.data_type(), namespace);
            Arc::new(field.as_ref().clone().with_data_type(data_type))
        }

        fn stamp_type(
            &mut self,
            ty: &'a Value,
            data_type: &DataType,
            namespace: Option<&'a str>,
        ) -> DataType {
            match ty {
                Value::String(name) if PRIMITIVES.contains(&name.as_str()) => data_type.clone(),
                Value::String(name) => {
                    let key = [full_name(name, namespace), name.clone()]
                        .into_iter()
                        .find(|k| self.named.contains_key(k));
                    match key {
                        Some(key) if !self.in_progress.contains(&key) => {
                            let definition = self.named[&key];
                            self.in_progress.push(key);
                            let stamped = self.stamp_type(definition, data_type, namespace);
                            self.in_progress.pop();
                            stamped
                        }
                        _ => data_type.clone(),
                    }
                }
                Value::Array(branches) => {
                    let non_null: Vec<&'a Value> = branches
                        .iter()
                        .filter(|b| b.as_str() != Some("null"))
                        .collect();
                    match data_type {
                        DataType::Union(union_fields, mode)
                            if union_fields.len() == branches.len() =>
                        {
                            let (ids, fields): (Vec<i8>, Vec<arrow_schema::FieldRef>) =
                                union_fields
                                    .iter()
                                    .zip(branches)
                                    .map(|((id, f), branch)| {
                                        (id, self.stamp_child(branch, f, namespace))
                                    })
                                    .unzip();
                            match arrow_schema::UnionFields::try_new(ids, fields) {
                                Ok(stamped) => DataType::Union(stamped, *mode),
                                Err(_) => data_type.clone(),
                            }
                        }
                        DataType::Union(..) => data_type.clone(),
                        _ if branches.len() == 2 && non_null.len() == 1 => {
                            self.stamp_type(non_null[0], data_type, namespace)
                        }
                        _ => data_type.clone(),
                    }
                }
                Value::Object(map) => {
                    let inner = own_namespace(map, namespace);
                    match (map.get("type"), data_type) {
                        (Some(Value::String(kind)), DataType::Struct(fields))
                            if kind == "record" || kind == "error" =>
                        {
                            match map.get("fields") {
                                Some(Value::Array(fields_json)) => DataType::Struct(
                                    self.stamp_fields(fields_json, fields, inner).into(),
                                ),
                                _ => data_type.clone(),
                            }
                        }
                        (Some(Value::String(kind)), DataType::List(item)) if kind == "array" => {
                            match map.get("items") {
                                Some(items) => DataType::List(self.stamp_child(items, item, inner)),
                                None => data_type.clone(),
                            }
                        }
                        (Some(Value::String(kind)), DataType::LargeList(item))
                            if kind == "array" =>
                        {
                            match map.get("items") {
                                Some(items) => {
                                    DataType::LargeList(self.stamp_child(items, item, inner))
                                }
                                None => data_type.clone(),
                            }
                        }
                        (Some(Value::String(kind)), DataType::Map(entries, sorted))
                            if kind == "map" =>
                        {
                            match (map.get("values"), entries.data_type()) {
                                (Some(values), DataType::Struct(kv)) if kv.len() == 2 => {
                                    let kv: Vec<arrow_schema::FieldRef> = vec![
                                        kv[0].clone(),
                                        self.stamp_child(values, &kv[1], inner),
                                    ];
                                    DataType::Map(
                                        Arc::new(
                                            entries
                                                .as_ref()
                                                .clone()
                                                .with_data_type(DataType::Struct(kv.into())),
                                        ),
                                        *sorted,
                                    )
                                }
                                _ => data_type.clone(),
                            }
                        }
                        // `{"type": <schema>}` wrapping another schema, or a
                        // primitive carrying a logical type.
                        (Some(inner_ty @ (Value::Object(_) | Value::Array(_))), _) => {
                            self.stamp_type(inner_ty, data_type, namespace)
                        }
                        (Some(inner_ty @ Value::String(name)), _)
                            if !matches!(
                                name.as_str(),
                                "record" | "error" | "array" | "map" | "enum" | "fixed"
                            ) =>
                        {
                            self.stamp_type(inner_ty, data_type, namespace)
                        }
                        _ => data_type.clone(),
                    }
                }
                _ => data_type.clone(),
            }
        }
    }

    let reader: Value = serde_json::from_str(reader_json)
        .map_err(|e| crate::error::CoreError::Schema(format!("bad reader avro json: {e}")))?;
    let Some(Value::Array(fields_json)) = reader.get("fields") else {
        return Ok(schema.clone());
    };
    let mut named = HashMap::new();
    register(&reader, None, &mut named);
    let namespace = reader.as_object().and_then(|m| own_namespace(m, None));
    let mut stamper = Stamper {
        named,
        in_progress: Vec::new(),
    };
    let fields = stamper.stamp_fields(fields_json, schema.fields(), namespace);
    Ok(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
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
    /// `with_avro_defaults` stamps each declared default as its JSON text at every
    /// depth — record fields, array items, map values, nullable and general union
    /// branches, named types referenced by simple or full name anywhere after their
    /// definition — changes nothing else, and terminates on a recursive reference.
    #[test]
    fn with_avro_defaults_stamps_every_declared_default_at_every_depth() {
        use arrow_schema::{DataType, Field};
        let reader = r#"{"type":"record","name":"R","namespace":"top","fields":[
            {"name":"id","type":"long"},
            {"name":"flag","type":"boolean","default":true},
            {"name":"note","type":["null","string"],"default":null},
            {"name":"a","type":{"type":"record","name":"A","namespace":"other","fields":[
                {"name":"s","type":{"type":"record","name":"S","fields":[
                    {"name":"x","type":"int","default":5},
                    {"name":"y","type":"string"}]}}]}},
            {"name":"by_full_name","type":["null","other.S"],"default":null},
            {"name":"arr","type":{"type":"array","items":{"type":"record","name":"I","fields":[
                {"name":"i","type":"long","default":9}]}}},
            {"name":"m","type":{"type":"map","values":{"type":"record","name":"V","fields":[
                {"name":"v","type":{"type":"string","logicalType":"uuid"},"default":"z"}]}}},
            {"name":"u","type":["int",{"type":"record","name":"U","fields":[
                {"name":"w","type":"double","default":1.5}]}]},
            {"name":"node","type":{"type":"record","name":"Node","fields":[
                {"name":"next","type":["null","Node"],"default":null},
                {"name":"d","type":"int","default":0}]}},
            {"name":"after","type":"I"}
        ]}"#;
        // A stand-in for the Arrow conversion: the recursive `node.next` is cut
        // at one level, which is all an Arrow schema can hold.
        let s_struct = || {
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Int32, false),
                    Field::new("y", DataType::Utf8, false),
                ]
                .into(),
            )
        };
        let i_struct = || DataType::Struct(vec![Field::new("i", DataType::Int64, false)].into());
        let node_leaf = DataType::Struct(vec![Field::new("d", DataType::Int32, false)].into());
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("note", DataType::Utf8, true),
            Field::new(
                "a",
                DataType::Struct(vec![Field::new("s", s_struct(), false)].into()),
                false,
            ),
            Field::new("by_full_name", s_struct(), true),
            Field::new(
                "arr",
                DataType::List(Arc::new(Field::new("item", i_struct(), false))),
                false,
            ),
            Field::new(
                "m",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new(
                                    "value",
                                    DataType::Struct(
                                        vec![Field::new("v", DataType::FixedSizeBinary(16), false)]
                                            .into(),
                                    ),
                                    false,
                                ),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
            Field::new(
                "u",
                DataType::Union(
                    arrow_schema::UnionFields::try_new(
                        vec![0, 1],
                        vec![
                            Field::new("int", DataType::Int32, false),
                            Field::new(
                                "U",
                                DataType::Struct(
                                    vec![Field::new("w", DataType::Float64, false)].into(),
                                ),
                                false,
                            ),
                        ],
                    )
                    .unwrap(),
                    arrow_schema::UnionMode::Dense,
                ),
                false,
            ),
            Field::new(
                "node",
                DataType::Struct(
                    vec![
                        Field::new("next", node_leaf, true),
                        Field::new("d", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new("after", i_struct(), false),
        ]);

        let stamped = with_avro_defaults(&schema, reader).unwrap();
        let default_of = |f: &Field| f.metadata().get("avro.field.default").cloned();
        let child = |dt: &DataType, name: &str| -> Field {
            match dt {
                DataType::Struct(fields) => fields
                    .iter()
                    .find(|f| f.name() == name)
                    .unwrap_or_else(|| panic!("no child {name}"))
                    .as_ref()
                    .clone(),
                other => panic!("{other} is not a struct"),
            }
        };
        let top = |name: &str| stamped.field_with_name(name).unwrap().clone();

        assert_eq!(default_of(&top("id")), None);
        assert_eq!(default_of(&top("flag")).as_deref(), Some("true"));
        assert_eq!(default_of(&top("note")).as_deref(), Some("null"));
        let s_in_a = child(top("a").data_type(), "s");
        assert_eq!(
            default_of(&child(s_in_a.data_type(), "x")).as_deref(),
            Some("5")
        );
        assert_eq!(default_of(&child(s_in_a.data_type(), "y")), None);
        assert_eq!(
            default_of(&child(top("by_full_name").data_type(), "x")).as_deref(),
            Some("5"),
            "a reference by full name, after its definition closed"
        );
        let DataType::List(item) = top("arr").data_type().clone() else {
            panic!("arr")
        };
        assert_eq!(
            default_of(&child(item.data_type(), "i")).as_deref(),
            Some("9")
        );
        let DataType::Map(entries, _) = top("m").data_type().clone() else {
            panic!("m")
        };
        let value = child(entries.data_type(), "value");
        assert_eq!(
            default_of(&child(value.data_type(), "v")).as_deref(),
            Some("\"z\"")
        );
        let DataType::Union(branches, _) = top("u").data_type().clone() else {
            panic!("u")
        };
        assert_eq!(
            default_of(&child(branches.iter().nth(1).unwrap().1.data_type(), "w")).as_deref(),
            Some("1.5")
        );
        let node = top("node");
        assert_eq!(
            default_of(&child(node.data_type(), "next")).as_deref(),
            Some("null")
        );
        assert_eq!(
            default_of(&child(node.data_type(), "d")).as_deref(),
            Some("0")
        );
        assert_eq!(
            default_of(&child(child(node.data_type(), "next").data_type(), "d")).as_deref(),
            Some("0"),
            "a recursive reference stamps the level the Arrow schema holds, and terminates"
        );
        assert_eq!(
            default_of(&child(top("after").data_type(), "i")).as_deref(),
            Some("9")
        );

        // Nothing but the metadata changed.
        let strip = |schema: &Schema| {
            fn strip_dt(dt: &DataType) -> DataType {
                match dt {
                    DataType::Struct(fs) => DataType::Struct(
                        fs.iter().map(|f| strip_field(f)).collect::<Vec<_>>().into(),
                    ),
                    DataType::List(f) => DataType::List(Arc::new(strip_field(f))),
                    DataType::Map(f, s) => DataType::Map(Arc::new(strip_field(f)), *s),
                    DataType::Union(fs, m) => DataType::Union(
                        arrow_schema::UnionFields::try_new(
                            fs.iter().map(|(id, _)| id).collect::<Vec<_>>(),
                            fs.iter().map(|(_, f)| strip_field(f)).collect::<Vec<_>>(),
                        )
                        .unwrap(),
                        *m,
                    ),
                    other => other.clone(),
                }
            }
            fn strip_field(f: &Field) -> Field {
                let mut md = f.metadata().clone();
                md.remove("avro.field.default");
                Field::new(f.name(), strip_dt(f.data_type()), f.is_nullable()).with_metadata(md)
            }
            Schema::new(
                schema
                    .fields()
                    .iter()
                    .map(|f| strip_field(f))
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(strip(&stamped), schema);
    }
}
