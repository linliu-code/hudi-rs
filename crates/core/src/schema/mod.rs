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
use crate::error::{CoreError, Result};
use crate::metadata::meta_field::MetaField;
use crate::schema::resolver::sanitize_avro_schema_str;
use arrow_schema::{Schema, SchemaRef};
use serde_json::Value;
use std::sync::Arc;

pub mod avro_schema_utils;
pub mod batch_evolution;
pub mod delete;
pub mod extended_promotion;
pub mod parquet_list_norm;
pub mod resolver;

/// Rewrite every `+00:00`-spelled timestamp zone as `UTC`, recursively.
///
/// The two converters disagree on the spelling of the same zone: `avro_to_arrow`
/// (and the parquet reader) write `UTC`, `arrow-avro` writes the offset `+00:00`.
/// Arrow compares timezones as strings, so a required schema spelled one way and
/// a base batch spelled the other are judged to have different types. The log
/// decoder already normalises its batches the same way
/// (`log_file::avro::normalize_utc_timestamps`); this keeps every schema derived
/// from the decoder route on that side of the disagreement, so switching those
/// conversions to it changed which schemas can be converted and nothing else.
///
/// Every caller that converts an Avro JSON through
/// `ffi_support::arrow_schema_from_avro_json` and then compares or concatenates
/// the result against a parquet-derived or `avro_to_arrow`-derived schema must
/// run it through here. Today that is
/// `FileGroupReaderSchemaHandler::prepare_required_schema` (the required schema)
/// and `log_file::content`'s rewrite targets.
///
/// The recursion covers `Struct`, `List`, `LargeList`, `Map` and `Union`, which
/// is every nested type `arrow-avro` emits that can contain a timestamp: an Avro
/// `array` becomes `List`, a `map` becomes `Map`, a `fixed` becomes
/// `FixedSizeBinary` and an `enum` becomes `Dictionary(Int32, Utf8)` — neither of
/// the last two can hold one. `FixedSizeList`, `Dictionary`, `RunEndEncoded` and
/// the list-view types are therefore not walked.
pub(crate) fn normalize_utc_timezone_spelling(
    schema: &arrow_schema::Schema,
) -> arrow_schema::Schema {
    use arrow_schema::{DataType, Field};

    fn is_utc_alias(tz: &str) -> bool {
        matches!(tz, "+00:00" | "+0000" | "00:00" | "Z" | "z")
    }

    fn fix(dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Timestamp(unit, Some(tz)) if is_utc_alias(tz) => {
                Some(DataType::Timestamp(*unit, Some("UTC".into())))
            }
            DataType::Struct(fields) => {
                let fixed: Vec<_> = fields.iter().map(fix_field).collect();
                fields
                    .iter()
                    .zip(&fixed)
                    .any(|(a, b)| a != b)
                    .then(|| DataType::Struct(fixed.into()))
            }
            DataType::List(f) => (fix_field(f) != *f).then(|| DataType::List(fix_field(f))),
            DataType::LargeList(f) => {
                (fix_field(f) != *f).then(|| DataType::LargeList(fix_field(f)))
            }
            DataType::Map(f, sorted) => {
                (fix_field(f) != *f).then(|| DataType::Map(fix_field(f), *sorted))
            }
            DataType::Union(fields, mode) => {
                let fixed: Vec<_> = fields.iter().map(|(id, f)| (id, fix_field(f))).collect();
                fields
                    .iter()
                    .zip(&fixed)
                    .any(|((_, a), (_, b))| a != b)
                    .then(|| {
                        DataType::Union(
                            fixed.iter().map(|(id, f)| (*id, f.clone())).collect(),
                            *mode,
                        )
                    })
            }
            _ => None,
        }
    }

    fn fix_field(field: &arrow_schema::FieldRef) -> arrow_schema::FieldRef {
        match fix(field.data_type()) {
            Some(dt) => Arc::new(
                Field::new(field.name(), dt, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            ),
            None => field.clone(),
        }
    }

    let fields: Vec<_> = schema.fields().iter().map(fix_field).collect();
    arrow_schema::Schema::new_with_metadata(fields, schema.metadata().clone())
}

pub fn prepend_meta_fields(schema: SchemaRef) -> Result<Schema> {
    let meta_field_schema = MetaField::schema();
    Schema::try_merge([meta_field_schema.as_ref().clone(), schema.as_ref().clone()])
        .map_err(CoreError::ArrowError)
}

// TODO use this when applicable, like some table config says there is an operation field
pub fn prepend_meta_fields_with_operation(schema: SchemaRef) -> Result<Schema> {
    let meta_field_schema = MetaField::schema_with_operation();
    Schema::try_merge([meta_field_schema.as_ref().clone(), schema.as_ref().clone()])
        .map_err(CoreError::ArrowError)
}

pub fn prepend_meta_fields_to_avro_schema_str(avro_schema_str: &str) -> Result<String> {
    let mut schema: Value = serde_json::from_str(&sanitize_avro_schema_str(avro_schema_str))
        .map_err(|e| CoreError::Schema(format!("Failed to parse Avro schema JSON: {e}")))?;

    let fields = schema
        .get_mut("fields")
        .and_then(|f| f.as_array_mut())
        .ok_or_else(|| CoreError::Schema("Avro schema has no 'fields' array".to_string()))?;

    let meta_field_defs: Vec<Value> = MetaField::field_names()
        .iter()
        .map(|name| {
            serde_json::json!({
                "name": name,
                "type": ["null", "string"],
                "default": null
            })
        })
        .collect();

    let existing_names: std::collections::HashSet<&str> = fields
        .iter()
        .filter_map(|f| f.get("name").and_then(|n| n.as_str()))
        .collect();

    let new_meta_fields: Vec<Value> = meta_field_defs
        .into_iter()
        .filter(|f| {
            f.get("name")
                .and_then(|n| n.as_str())
                .is_none_or(|name| !existing_names.contains(name))
        })
        .collect();

    let mut all_fields = new_meta_fields;
    all_fields.append(fields);
    *fields = all_fields;

    serde_json::to_string(&schema)
        .map_err(|e| CoreError::Schema(format!("Failed to serialize Avro schema: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use hudi_test::assert_arrow_field_names_eq;
    use std::sync::Arc;

    /// Every UTC spelling is rewritten to `UTC` at every depth the recursion
    /// claims — `Struct`, `List`, `LargeList`, `Map`, `Union` — while names,
    /// nullability, field and schema metadata, a non-UTC zone and a zone-less
    /// timestamp are left exactly as they were.
    #[test]
    fn normalize_utc_timezone_spelling_rewrites_every_alias_at_every_depth() {
        use arrow_schema::{TimeUnit, UnionFields, UnionMode};
        use std::collections::HashMap;

        let md = |k: &str| HashMap::from([(k.to_string(), "v".to_string())]);
        // `tz(alias)` is the zone a field is built with; the expected schema is
        // the same builder with every alias mapped to "UTC".
        let build = |tz: &dyn Fn(&str) -> String| {
            let ts =
                |alias: &str| DataType::Timestamp(TimeUnit::Microsecond, Some(tz(alias).into()));
            Schema::new_with_metadata(
                vec![
                    Field::new("offset", ts("+00:00"), true),
                    Field::new("compact", ts("+0000"), false),
                    Field::new("unsigned", ts("00:00"), true),
                    Field::new("zulu", ts("Z"), true).with_metadata(md("f")),
                    Field::new("lower_zulu", ts("z"), true),
                    Field::new("utc", ts("UTC"), true),
                    Field::new(
                        "other_zone",
                        DataType::Timestamp(TimeUnit::Millisecond, Some("+01:00".into())),
                        true,
                    ),
                    Field::new(
                        "no_zone",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    Field::new(
                        "s",
                        DataType::Struct(
                            vec![
                                Field::new("inner", ts("+00:00"), false).with_metadata(md("s")),
                                Field::new("n", DataType::Int32, true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                    Field::new(
                        "l",
                        DataType::List(Arc::new(Field::new("item", ts("Z"), false))),
                        true,
                    ),
                    Field::new(
                        "ll",
                        DataType::LargeList(Arc::new(Field::new("element", ts("+0000"), true))),
                        true,
                    ),
                    Field::new(
                        "m",
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", DataType::Utf8, false),
                                        Field::new("value", ts("z"), true),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        true,
                    ),
                    Field::new(
                        "u",
                        DataType::Union(
                            UnionFields::try_new(
                                vec![0, 1],
                                vec![
                                    Field::new("ts", ts("00:00"), true),
                                    Field::new("i", DataType::Int32, true),
                                ],
                            )
                            .unwrap(),
                            UnionMode::Dense,
                        ),
                        true,
                    ),
                    Field::new(
                        "nested",
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::Struct(
                                vec![Field::new(
                                    "deep",
                                    DataType::Map(
                                        Arc::new(Field::new(
                                            "entries",
                                            DataType::Struct(
                                                vec![
                                                    Field::new("key", DataType::Utf8, false),
                                                    Field::new("value", ts("+00:00"), true),
                                                ]
                                                .into(),
                                            ),
                                            false,
                                        )),
                                        false,
                                    ),
                                    true,
                                )]
                                .into(),
                            ),
                            true,
                        ))),
                        true,
                    ),
                ],
                md("schema"),
            )
        };
        let as_written = build(&|alias| alias.to_string());
        let expected = build(&|alias| match alias {
            "+00:00" | "+0000" | "00:00" | "Z" | "z" => "UTC".to_string(),
            other => other.to_string(),
        });
        assert_ne!(as_written, expected, "the fixture must contain aliases");
        assert_eq!(normalize_utc_timezone_spelling(&as_written), expected);
        // Idempotent: a normalised schema comes back unchanged.
        assert_eq!(normalize_utc_timezone_spelling(&expected), expected);
    }

    #[test]
    fn test_prepend_meta_fields() {
        let schema = Schema::new(vec![Field::new("field1", DataType::Int32, false)]);
        let new_schema = prepend_meta_fields(Arc::new(schema)).unwrap();
        assert_arrow_field_names_eq!(
            new_schema,
            [MetaField::field_names(), vec!["field1"]].concat()
        )
    }

    #[test]
    fn test_prepend_meta_fields_with_operation() {
        let schema = Schema::new(vec![Field::new("field1", DataType::Int32, false)]);
        let new_schema = prepend_meta_fields_with_operation(Arc::new(schema)).unwrap();
        assert_arrow_field_names_eq!(
            new_schema,
            [MetaField::field_names_with_operation(), vec!["field1"]].concat()
        )
    }

    #[test]
    fn test_prepend_meta_fields_to_avro_schema_str() {
        let avro_schema =
            r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"int"}]}"#;
        let result = prepend_meta_fields_to_avro_schema_str(avro_schema).unwrap();
        let parsed: Value = serde_json::from_str(&result).unwrap();
        let fields = parsed["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 6, "Expected 5 meta fields + 1 data field");
        assert_eq!(fields[0]["name"], "_hoodie_commit_time");
        assert_eq!(fields[5]["name"], "id");
    }

    #[test]
    fn test_prepend_meta_fields_to_avro_schema_str_dedup() {
        let avro_schema = r#"{"type":"record","name":"TestRecord","fields":[{"name":"_hoodie_commit_time","type":["null","string"],"default":null},{"name":"id","type":"int"}]}"#;
        let result = prepend_meta_fields_to_avro_schema_str(avro_schema).unwrap();
        let parsed: Value = serde_json::from_str(&result).unwrap();
        let fields = parsed["fields"].as_array().unwrap();
        assert_eq!(
            fields.len(),
            6,
            "Expected 5 meta fields + 1 data field (deduped existing meta field)"
        );
        // The existing _hoodie_commit_time should not be duplicated
        let commit_time_count = fields
            .iter()
            .filter(|f| f["name"] == "_hoodie_commit_time")
            .count();
        assert_eq!(commit_time_count, 1);
    }
}
