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
//! Spell Avro named types the one way `arrow-avro` can match them.
//!
//! The Avro spec gives a named type two equivalent spellings: `name` short with
//! the namespace inherited from the enclosing type, or `name` short with an
//! explicit `namespace` (or `name` dotted, which is a fullname and overrides the
//! attribute). Java writes the first — `Schema.toString()` emits `namespace`
//! only when it differs from the enclosing one — and Java writes every schema
//! Hudi stores. Avro's own object model materialises the inherited namespace, so
//! a caller that built its reader schema through `Schema.Parser` and handed it
//! over, or anything that went through avro-tools, is in the second.
//!
//! `arrow-avro` 58 matches a writer named type against a reader named type on
//! the fullname it computes from that type's LITERAL `name` and `namespace`
//! attributes, dropping the enclosing namespace at exactly that comparison
//! (`codec.rs` `full_name_set` -> `make_full_name(name, ns, None)`), although it
//! applies inheritance correctly when parsing, when fingerprinting and when
//! resolving references. So the two spellings do not match each other, and the
//! error prints the bare names: `Record name mismatch writer=X, reader=X`.
//!
//! Rather than pick a spelling of our own, this module re-emits both sides in
//! Java's — the form every writer schema in the wild already has, which makes
//! the pass the identity on them, so a schema that somehow reached `arrow-avro`
//! without it behaves as it does today rather than newly failing.

use crate::Result;
use crate::error::CoreError;
use serde_json::{Map, Value};

/// The names `org.apache.avro.Schema.Type` occupies — exactly
/// `Schema.Type.values()`, `error` excluded because Java spells that type
/// `record` on the way out. Used for two different claims:
///
/// * In [`qualify_reference`], a named type whose short name collides with one
///   of these is written as a fullname. That is Java's `shouldWriteFull` and it
///   holds for all fourteen.
/// * At a schema position, a bare string that is one of these is taken to BE
///   that type. Java is narrower: its parser resolves only the eight primitive
///   names that way and would read a bare `"record"` as a reference to a named
///   type called `record`. Reachable only from a schema that both defines such a
///   type and refers to it from inside its own namespace, which no Java writer
///   emits — `Name.getQualified` writes that reference as a fullname precisely
///   to avoid the ambiguity.
const TYPE_NAMES: [&str; 14] = [
    "null", "boolean", "int", "long", "float", "double", "bytes", "string", "record", "enum",
    "array", "map", "union", "fixed",
];

fn is_type_name(name: &str) -> bool {
    TYPE_NAMES.contains(&name)
}

/// Java's `Schema.Name(name, space)`: a dotted `name` carries its own namespace
/// and overrides the attribute, and an empty namespace is a null namespace.
///
/// `ns_attr` is `Some` only when the JSON actually carried a `namespace` key —
/// an absent key inherits `enclosing`, while an explicit `""` does not
/// (`Schema.java:1709-1713` inherits only when `getOptionalText` returned null).
fn split_name<'a>(
    name: &'a str,
    ns_attr: Option<&'a str>,
    enclosing: Option<&'a str>,
) -> (&'a str, Option<&'a str>) {
    if let Some((space, short)) = name.rsplit_once('.') {
        return (short, non_empty(space));
    }
    match ns_attr {
        Some(attr) => (name, non_empty(attr)),
        None => (name, enclosing),
    }
}

/// A JSON value for an error message, truncated. Every other value this module
/// names in an error is a scalar, but a `namespace` that is not a string can be
/// an arbitrarily large subtree, and an error is not the place to print one.
fn brief(value: &Value) -> String {
    const MAX_CHARS: usize = 60;
    let rendered = value.to_string();
    match rendered.char_indices().nth(MAX_CHARS) {
        Some((cut, _)) => format!("{}...", &rendered[..cut]),
        None => rendered,
    }
}

fn non_empty(space: &str) -> Option<&str> {
    if space.is_empty() { None } else { Some(space) }
}

/// What Java's `Name.writeName` puts in the `namespace` field, if anything
/// (`Schema.java:744-753`): the namespace when it differs from the enclosing
/// one, `""` for a null namespace inside a non-null one, nothing otherwise.
fn namespace_attribute(space: Option<&str>, enclosing: Option<&str>) -> Option<String> {
    match space {
        Some(space) if Some(space) != enclosing => Some(space.to_string()),
        Some(_) => None,
        None if enclosing.is_some() => Some(String::new()),
        None => None,
    }
}

/// What Java's `Name.getQualified` writes for a reference to an already-defined
/// type (`Schema.java:755-780`): the short name when the type's namespace is
/// non-null and equal to the enclosing one, the fullname otherwise.
fn qualify_reference(reference: &str, enclosing: Option<&str>) -> String {
    let (short, space) = split_name(reference, None, enclosing);
    let write_full = match space {
        Some(space) => Some(space) != enclosing || is_type_name(short),
        None => true,
    };
    match (write_full, space) {
        (true, Some(space)) => format!("{space}.{short}"),
        _ => short.to_string(),
    }
}

/// Re-emit `json` with every Avro named type spelled as Java's
/// `Schema.toString()` spells it.
///
/// Public, and supported as such: a foreign caller that hands its own schema
/// over the FFI has to spell it the way this crate spells the schemas it reads,
/// and the alternative — hiding it and re-exporting a shim for the cross-crate
/// test that pins the Java-form identity — would hide it from those callers too.
///
/// Everything that is not a named type's `name`/`namespace` or a reference to
/// one is preserved exactly: field order, key order, `doc`, `default`,
/// `aliases`, `logicalType` and any other attribute, which is why this is a JSON
/// rewrite and not `apache_avro`'s canonical form (that drops all of them).
/// The pass is idempotent and is the identity on Java-form input.
///
/// The result is memoised per distinct input string, because the callers run
/// this on the schema-sized path they otherwise optimise to the microsecond: the
/// reader schema is canonicalised once per HFile WINDOW and once per log BLOCK,
/// and re-parsing plus re-emitting the metadata table's 8 KB record schema each
/// time costs more than everything else those loops do. The writer side has had
/// the equivalent cache since it was written (`log_file::content`'s
/// `registered_for`); this gives the reader side one too, without moving the
/// canonicalisation out of the one place that guarantees both sides get it.
///
/// # Errors
///
/// The input is not JSON, is not an Avro schema (a schema position holding a
/// number, say), a named type has no string `name` or a non-string `namespace`,
/// or a record's `fields` is not an array of objects. That is the whole list:
/// this is a renamer, not a validator, and anything else it does not understand
/// — an object at a schema position with no `type`, a name that is not a legal
/// Avro identifier, a type defined twice — is copied through for `arrow-avro` to
/// reject a few microseconds later.
pub fn canonicalize_avro_schema_json(json: &str) -> Result<String> {
    if let Some(hit) = MEMO.with_borrow_mut(|memo| {
        let found = memo.iter().position(|(input, _)| input == json)?;
        // Most-recently-used first, so the two live spellings of one read stay
        // resident whichever order the callers ask for them in.
        memo[..=found].rotate_right(1);
        Some(memo[0].1.clone())
    }) {
        return Ok(hit);
    }
    let canonical = canonicalize_uncached(json)?;
    MEMO.with_borrow_mut(|memo| {
        memo.truncate(MEMO_ENTRIES - 1);
        memo.insert(0, (json.to_string(), canonical.clone()));
    });
    #[cfg(test)]
    MEMO_MISSES.with(|misses| misses.set(misses.get() + 1));
    Ok(canonical)
}

/// How many distinct schema strings the memo holds. Two, because one read has
/// two live spellings — the file's writer schema and the caller's reader schema
/// — and they alternate; a third would only buy something for a caller that
/// interleaves reads of differently-shaped files on one thread.
const MEMO_ENTRIES: usize = 2;

thread_local! {
    /// `(input, canonical)`, most-recently-used first.
    ///
    /// Thread-local rather than shared: the pass is a pure function of its
    /// input, so a per-thread copy needs no lock on a path that is otherwise
    /// lock-free, and a thread that never canonicalises pays nothing. The cost
    /// is that a thread retains at most `MEMO_ENTRIES` schema strings and their
    /// canonical forms for its lifetime — tens of kilobytes for the metadata
    /// table's schema.
    static MEMO: std::cell::RefCell<Vec<(String, String)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
thread_local! {
    static MEMO_MISSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How many canonicalisations have actually run on the current thread, i.e. memo
/// misses. Per-thread like the memo itself, so a test reads only its own work
/// even though the suite runs in parallel; callers compare a delta, never an
/// absolute.
#[cfg(test)]
pub(crate) fn canonicalizations_run() -> u64 {
    MEMO_MISSES.with(std::cell::Cell::get)
}

/// [`canonicalize_avro_schema_json`] without the memo. Also what the tests that
/// assert the pass itself call, so a cache hit can never stand in for the
/// algorithm — an idempotence check whose second input is the first's output
/// would otherwise be answered from the memo.
fn canonicalize_uncached(json: &str) -> Result<String> {
    let value: Value = serde_json::from_str(json)
        .map_err(|e| CoreError::Schema(format!("Avro schema JSON is not parseable: {e}")))?;
    let canonical = rewrite_schema(&value, None)?;
    serde_json::to_string(&canonical)
        .map_err(|e| CoreError::Schema(format!("Failed to re-emit the Avro schema: {e}")))
}

/// `value` sits where the Avro grammar expects a schema: a type name or
/// reference, a union, or an object.
fn rewrite_schema(value: &Value, enclosing: Option<&str>) -> Result<Value> {
    match value {
        Value::String(name) if is_type_name(name) => Ok(value.clone()),
        Value::String(reference) => Ok(Value::String(qualify_reference(reference, enclosing))),
        Value::Array(branches) => branches
            .iter()
            .map(|branch| rewrite_schema(branch, enclosing))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        Value::Object(object) => rewrite_object(object, enclosing),
        other => Err(CoreError::Schema(format!(
            "Avro schema expected, found {other}"
        ))),
    }
}

fn rewrite_object(object: &Map<String, Value>, enclosing: Option<&str>) -> Result<Value> {
    match object.get("type").and_then(Value::as_str) {
        Some("record" | "error" | "enum" | "fixed") => rewrite_named(object, enclosing),
        _ => rewrite_unnamed(object, enclosing),
    }
}

/// A record / error / enum / fixed definition: rename it, and carry its own
/// namespace into its body the way `RecordSchema.toJson` sets `names.space`
/// before writing the fields (`Schema.java:1013-1019`).
fn rewrite_named(object: &Map<String, Value>, enclosing: Option<&str>) -> Result<Value> {
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| CoreError::Schema("Avro named type has no string `name`".to_string()))?;
    let ns_attr = match object.get("namespace") {
        None => None,
        Some(Value::String(space)) => Some(space.as_str()),
        Some(other) => {
            return Err(CoreError::Schema(format!(
                "Avro `namespace` of `{name}` must be a string, found {}",
                brief(other)
            )));
        }
    };
    let (short, space) = split_name(name, ns_attr, enclosing);
    let namespace = namespace_attribute(space, enclosing);
    let declares_namespace = object.contains_key("namespace");

    let mut out = Map::new();
    for (key, value) in object {
        match key.as_str() {
            "name" => {
                out.insert("name".to_string(), Value::String(short.to_string()));
                // Java writes `namespace` straight after `name`; the input only
                // lacks the key when the namespace came from a dotted name or
                // from inheritance.
                if !declares_namespace && let Some(namespace) = &namespace {
                    out.insert("namespace".to_string(), Value::String(namespace.clone()));
                }
            }
            "namespace" => {
                if let Some(namespace) = &namespace {
                    out.insert("namespace".to_string(), Value::String(namespace.clone()));
                }
            }
            "fields" => {
                out.insert("fields".to_string(), rewrite_fields(value, space, short)?);
            }
            _ => {
                out.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(Value::Object(out))
}

fn rewrite_fields(fields: &Value, enclosing: Option<&str>, record: &str) -> Result<Value> {
    let fields = fields.as_array().ok_or_else(|| {
        CoreError::Schema(format!("Avro record `{record}` has non-array `fields`"))
    })?;
    let mut out = Vec::with_capacity(fields.len());
    for field in fields {
        let field = field.as_object().ok_or_else(|| {
            CoreError::Schema(format!("Avro record `{record}` has a non-object field"))
        })?;
        let mut rewritten = Map::new();
        for (key, value) in field {
            let value = if key == "type" {
                rewrite_schema(value, enclosing)?
            } else {
                value.clone()
            };
            rewritten.insert(key.clone(), value);
        }
        out.push(Value::Object(rewritten));
    }
    Ok(Value::Array(out))
}

/// An array, a map, a primitive carrying a `logicalType`, or an object whose
/// `type` is itself a schema or a reference.
fn rewrite_unnamed(object: &Map<String, Value>, enclosing: Option<&str>) -> Result<Value> {
    let mut out = Map::new();
    for (key, value) in object {
        let value = match key.as_str() {
            "type" | "items" | "values" => rewrite_schema(value, enclosing)?,
            _ => value.clone(),
        };
        out.insert(key.clone(), value);
    }
    Ok(Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every Avro schema checked into `crates/test/data`, through the pass and
    /// back, is the same schema: `apache_avro`'s canonical form (which resolves
    /// namespaces and drops the attributes this pass preserves) is unchanged,
    /// and a second pass changes nothing.
    ///
    /// Byte-identity is asserted separately, on a schema that really is Java's
    /// output — `crates/jvm-ffi/tests/file_group_v2_tests.rs`'s
    /// `a_java_written_writer_schema_is_already_canonical`, which takes the
    /// writer schema out of a v6 MDT HFile. The `.avsc` files here are not that:
    /// `metadata_v6_record_index/HoodieMetadataRecord-with-meta-fields.avsc` was
    /// hand-derived from hudi-internal's `HoodieMetadata.avsc` by deleting the
    /// redundant nested `namespace`s, which leaves its type REFERENCES spelled
    /// as fullnames where Java writes them short (measured on that HFile:
    /// one `"namespace"`, zero dotted references).
    #[test]
    fn every_checked_in_avro_schema_survives_the_pass_with_its_meaning_intact() {
        let data = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates/")
            .join("test/data");
        let mut seen = 0usize;
        let mut checked_semantically = 0usize;
        let mut stack = vec![data.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read crates/test/data") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().is_none_or(|e| e != "avsc") {
                    continue;
                }
                let json = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
                    .trim()
                    .to_string();
                let canonical = canonicalize_avro_schema_json(&json)
                    .unwrap_or_else(|e| panic!("canonicalise {}: {e}", path.display()));
                // `apache_avro` is the semantic oracle: its canonical form
                // resolves namespaces and drops every attribute this pass
                // preserves, so an unchanged canonical form means the pass
                // changed only spelling. Two of the fixtures here are partial
                // schemas that reference a type defined in a sibling file
                // (`HoodieRollbackPlan.avsc` -> `HoodieInstantInfo`), which no
                // Avro parser can resolve on its own; those get the idempotence
                // check only.
                if let Ok(before) = apache_avro::Schema::parse_str(&json) {
                    let after = apache_avro::Schema::parse_str(&canonical)
                        .unwrap_or_else(|e| panic!("re-parse {}: {e}", path.display()));
                    assert_eq!(
                        before.canonical_form(),
                        after.canonical_form(),
                        "{} changed meaning",
                        path.display()
                    );
                    checked_semantically += 1;
                }
                assert_eq!(
                    canonicalize_uncached(&canonical).unwrap(),
                    canonical,
                    "{} is not idempotent",
                    path.display()
                );
                println!(
                    "avro_names {} bytes_identical={}",
                    path.file_name().expect("file name").to_string_lossy(),
                    canonical == json
                );
                seen += 1;
            }
        }
        assert!(seen > 0, "no .avsc fixtures found under {}", data.display());
        assert!(
            checked_semantically > 0,
            "at least one fixture must be a complete schema the oracle can parse"
        );
    }

    #[test]
    fn an_explicit_namespace_equal_to_the_enclosing_one_is_dropped() {
        let explicit = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","namespace":"org.example","fields":[{"name":"v","type":"int"}]}}]}"#;
        let java_form = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}]}"#;
        assert_eq!(canonicalize_avro_schema_json(explicit).unwrap(), java_form);
    }

    #[test]
    fn a_dotted_name_becomes_a_short_name_plus_a_namespace() {
        let dotted =
            r#"{"type":"record","name":"org.example.Outer","fields":[{"name":"v","type":"int"}]}"#;
        let java_form = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"v","type":"int"}]}"#;
        assert_eq!(canonicalize_avro_schema_json(dotted).unwrap(), java_form);
    }

    /// A dotted `name` overrides the `namespace` attribute, as `Schema.Name`
    /// does, and the resulting namespace is what the body inherits.
    #[test]
    fn a_dotted_name_overrides_the_namespace_attribute() {
        let mixed = r#"{"type":"record","name":"org.example.Outer","namespace":"ignored.me","fields":[{"name":"inner","type":{"type":"record","name":"Inner","namespace":"org.example","fields":[]}}]}"#;
        let java_form = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[]}}]}"#;
        assert_eq!(canonicalize_avro_schema_json(mixed).unwrap(), java_form);
    }

    /// A nested namespace that really differs is kept — the pass drops redundant
    /// namespaces, it does not flatten the schema into one.
    #[test]
    fn a_nested_namespace_that_differs_is_preserved() {
        let schema = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","namespace":"org.other","fields":[{"name":"deeper","type":{"type":"record","name":"Deeper","namespace":"org.other","fields":[]}}]}}]}"#;
        let java_form = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","namespace":"org.other","fields":[{"name":"deeper","type":{"type":"record","name":"Deeper","fields":[]}}]}}]}"#;
        assert_eq!(canonicalize_avro_schema_json(schema).unwrap(), java_form);
    }

    /// A null namespace inside a non-null one is Java's `"namespace":""`
    /// (`Schema.java:750-751`), not an omission — omitting it would make the
    /// type inherit on the next parse.
    #[test]
    fn a_null_namespace_inside_a_non_null_one_is_written_as_empty() {
        let schema = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","namespace":"","fields":[]}}]}"#;
        assert_eq!(canonicalize_avro_schema_json(schema).unwrap(), schema);
    }

    #[test]
    fn enum_and_fixed_are_renamed_like_records() {
        let explicit = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"e","type":{"type":"enum","name":"E","namespace":"org.example","symbols":["A","B"],"default":"A","doc":"an enum"}},{"name":"f","type":{"type":"fixed","name":"org.example.F","size":16}}]}"#;
        let java_form = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"],"default":"A","doc":"an enum"}},{"name":"f","type":{"type":"fixed","name":"F","size":16}}]}"#;
        assert_eq!(canonicalize_avro_schema_json(explicit).unwrap(), java_form);
    }

    /// A reference is short inside the namespace that defines it and full
    /// outside it — `Name.getQualified`. References reach the walker through
    /// unions, arrays, maps and a field's bare `type` alike.
    #[test]
    fn string_references_are_qualified_the_way_java_qualifies_them() {
        let explicit = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"a","type":{"type":"record","name":"Inner","namespace":"org.example","fields":[]}},{"name":"same_ns","type":"org.example.Inner"},{"name":"in_union","type":["null","org.example.Inner"],"default":null},{"name":"in_array","type":{"type":"array","items":"org.example.Inner"}},{"name":"in_map","type":{"type":"map","values":"org.example.Inner"}},{"name":"other_ns","type":{"type":"record","name":"Far","namespace":"org.other","fields":[{"name":"back","type":"org.example.Inner"}]}}]}"#;
        let java_form = r#"{"type":"record","name":"Outer","namespace":"org.example","fields":[{"name":"a","type":{"type":"record","name":"Inner","fields":[]}},{"name":"same_ns","type":"Inner"},{"name":"in_union","type":["null","Inner"],"default":null},{"name":"in_array","type":{"type":"array","items":"Inner"}},{"name":"in_map","type":{"type":"map","values":"Inner"}},{"name":"other_ns","type":{"type":"record","name":"Far","namespace":"org.other","fields":[{"name":"back","type":"org.example.Inner"}]}}]}"#;
        assert_eq!(canonicalize_avro_schema_json(explicit).unwrap(), java_form);
    }

    /// Everything that is not a name keeps its value and its position.
    #[test]
    fn defaults_docs_aliases_and_logical_types_are_preserved() {
        let schema = r#"{"type":"record","name":"Outer","namespace":"org.example","doc":"outer doc","aliases":["OldOuter"],"customAttribute":{"anything":[1,2,3]},"fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"},"doc":"a doc","default":0,"order":"ignore","aliases":["oldTs"]},{"name":"nested","type":{"type":"record","name":"Inner","namespace":"org.example","aliases":["OldInner"],"fields":[]},"default":{}}]}"#;
        let canonical = canonicalize_avro_schema_json(schema).unwrap();
        assert!(
            canonical.contains(r#""aliases":["OldOuter"]"#)
                && canonical.contains(r#""aliases":["oldTs"]"#)
                && canonical.contains(r#""aliases":["OldInner"]"#),
            "{canonical}"
        );
        assert!(
            canonical.contains(r#""doc":"outer doc""#)
                && canonical.contains(r#""doc":"a doc""#)
                && canonical.contains(r#""default":0"#)
                && canonical.contains(r#""order":"ignore""#)
                && canonical.contains(r#""logicalType":"timestamp-micros""#)
                && canonical.contains(r#""customAttribute":{"anything":[1,2,3]}"#),
            "{canonical}"
        );
        // Only `Inner`'s redundant namespace is gone.
        assert_eq!(
            canonical,
            schema.replace(
                r#""name":"Inner","namespace":"org.example","#,
                r#""name":"Inner","#
            )
        );
    }

    #[test]
    fn the_pass_is_idempotent() {
        for schema in [
            r#"{"type":"record","name":"org.example.Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","namespace":"org.example","fields":[{"name":"v","type":["null","org.example.Inner"],"default":null}]}}]}"#,
            r#"{"type":"record","name":"Outer","fields":[{"name":"v","type":"int"}]}"#,
        ] {
            // Uncached on purpose: `once` is often `schema` itself, and a memo
            // hit would answer the second call without running the pass.
            let once = canonicalize_uncached(schema).unwrap();
            let twice = canonicalize_uncached(&once).unwrap();
            assert_eq!(once, twice, "not idempotent for {schema}");
        }
    }

    /// A top-level union, which is a legal schema, walks like any other.
    #[test]
    fn a_top_level_union_is_walked() {
        let explicit =
            r#"["null",{"type":"record","name":"R","namespace":"org.example","fields":[]}]"#;
        assert_eq!(
            canonicalize_avro_schema_json(explicit).unwrap(),
            r#"["null",{"type":"record","name":"R","namespace":"org.example","fields":[]}]"#
        );
    }

    /// The pass runs once per distinct input, however many times it is asked.
    ///
    /// This is what keeps the reader side off the schema-sized path: the reader
    /// schema is canonicalised once per HFile window and once per log block, and
    /// both loops hand over the same string every time.
    #[test]
    fn the_memo_runs_the_pass_once_per_distinct_schema() {
        let one = r#"{"type":"record","name":"A","namespace":"org.example","fields":[]}"#;
        let two = r#"{"type":"record","name":"B","namespace":"org.example","fields":[]}"#;

        let before = canonicalizations_run();
        let expected = canonicalize_avro_schema_json(one).unwrap();
        for _ in 0..32 {
            assert_eq!(canonicalize_avro_schema_json(one).unwrap(), expected);
        }
        assert_eq!(
            canonicalizations_run() - before,
            1,
            "32 calls with one schema must run the pass once"
        );

        // Both spellings of one read stay resident: alternating between them
        // must not evict either.
        let before = canonicalizations_run();
        for _ in 0..32 {
            canonicalize_avro_schema_json(one).unwrap();
            canonicalize_avro_schema_json(two).unwrap();
        }
        assert_eq!(
            canonicalizations_run() - before,
            1,
            "only the second schema is new; alternating must not thrash the memo"
        );
    }

    /// Shapes the rest of the suite does not reach: an `error` record (Java's
    /// exception type, named and referenced like a record), a top-level `array`
    /// and `map` (legal schemas whose root carries no name), and a `fixed` with
    /// a logical type, whose extra attributes have to survive the rename.
    #[test]
    fn error_records_top_level_containers_and_a_logical_fixed_are_handled() {
        assert_eq!(
            canonicalize_uncached(
                r#"{"type":"error","name":"E","namespace":"org.example","fields":[{"name":"cause","type":["null","org.example.E"],"default":null}]}"#
            )
            .unwrap(),
            r#"{"type":"error","name":"E","namespace":"org.example","fields":[{"name":"cause","type":["null","E"],"default":null}]}"#
        );
        assert_eq!(
            canonicalize_uncached(
                r#"{"type":"array","items":{"type":"record","name":"org.example.R","fields":[]}}"#
            )
            .unwrap(),
            r#"{"type":"array","items":{"type":"record","name":"R","namespace":"org.example","fields":[]}}"#
        );
        assert_eq!(
            canonicalize_uncached(
                r#"{"type":"map","values":{"type":"enum","name":"org.example.Color","symbols":["RED"]}}"#
            )
            .unwrap(),
            r#"{"type":"map","values":{"type":"enum","name":"Color","namespace":"org.example","symbols":["RED"]}}"#
        );
        assert_eq!(
            canonicalize_uncached(
                r#"{"type":"record","name":"R","namespace":"org.example","fields":[{"name":"amount","type":{"type":"fixed","name":"Decimal","namespace":"org.example","size":16,"logicalType":"decimal","precision":38,"scale":10}}]}"#
            )
            .unwrap(),
            r#"{"type":"record","name":"R","namespace":"org.example","fields":[{"name":"amount","type":{"type":"fixed","name":"Decimal","size":16,"logicalType":"decimal","precision":38,"scale":10}}]}"#
        );
    }

    /// A `namespace` that is a whole subtree is named in the error, not printed
    /// into it.
    #[test]
    fn an_oversized_namespace_value_is_truncated_in_the_error() {
        let namespace = format!(r#"{{"a":"{}"}}"#, "x".repeat(4096));
        let err = canonicalize_uncached(&format!(
            r#"{{"type":"record","name":"R","namespace":{namespace},"fields":[]}}"#
        ))
        .expect_err("a non-string namespace is an error");
        let message = err.to_string();
        assert!(message.contains("must be a string"), "{message}");
        assert!(
            message.len() < 200,
            "the error must not carry the whole subtree: {} chars",
            message.len()
        );
    }

    #[test]
    fn malformed_input_is_an_error() {
        for (input, expected) in [
            ("{not avro}", "not parseable"),
            ("42", "Avro schema expected"),
            (r#"{"type":"record","fields":[]}"#, "no string `name`"),
            (
                r#"{"type":"record","name":"R","namespace":7,"fields":[]}"#,
                "must be a string",
            ),
            (r#"{"type":"record","name":"R","fields":{}}"#, "non-array"),
            (
                r#"{"type":"record","name":"R","fields":["v"]}"#,
                "non-object field",
            ),
        ] {
            let err = canonicalize_avro_schema_json(input)
                .expect_err("malformed schema must be an error");
            assert!(
                err.to_string().contains(expected),
                "{input}: expected `{expected}`, got `{err}`"
            );
        }
    }
}
