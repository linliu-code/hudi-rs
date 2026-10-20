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
//! Historical native properties and Java byte-stream compatibility.

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use hudi_core::config::table::{HudiTableConfig, TableTypeValue};
use hudi_core::table::{ReadOptions, Table};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const CODEC: &str = "hoodie.read.native.properties.codec";
const MARKER: &str = "# hudi-rs.properties.codec=java-v1";
const PROPERTY: &str = "hoodie.compat.example";
const SCHEMA: &str = r#"{"type":"record","name":"row","fields":[{"name":"id","type":"string","default":"line\nnext"}]}"#;

fn inventory(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}

fn properties(root: &Path) -> PathBuf {
    root.join(".hoodie/hoodie.properties")
}

fn unmarked(root: &Path) -> String {
    std::fs::read_to_string(properties(root))
        .unwrap()
        .lines()
        .filter(|line| !line.starts_with("# hudi-rs.properties.codec"))
        .map(|line| format!("{line}\n"))
        .collect()
}

fn raw_property(body: &str, key: &str, value: &str) -> String {
    let prefix = format!("{key}=");
    let mut lines: String = body
        .lines()
        .filter(|line| !line.starts_with(&prefix))
        .map(|line| format!("{line}\n"))
        .collect();
    lines.push_str(&format!("{key}={value}\n"));
    lines
}

async fn fixture(name: &str, version: isize, metadata: bool) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    Table::create(dir.path().to_str().unwrap())
        .with_table_name(name)
        .with_table_version(version)
        .with_record_key_fields(["id"])
        .with_metadata(metadata)
        .create()
        .await
        .unwrap();
    dir
}

#[tokio::test]
async fn test_old_native_unicode_identity_reopens_without_rewriting() {
    let mut names = Vec::new();
    for version in [8, 9] {
        let dir = fixture("café", version, false).await;
        // Exact old serialization of this value; all other values are plain ASCII.
        let body = raw_property(
            &unmarked(dir.path()),
            HudiTableConfig::TableName.as_ref(),
            "café",
        );
        std::fs::write(properties(dir.path()), body).unwrap();
        let before = inventory(dir.path());
        let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        names.push(table.table_name());
        assert_eq!(inventory(dir.path()), before);
    }
    assert_eq!(names, ["café", "café"]);
}

#[tokio::test]
async fn test_old_native_compact_schema_preserves_json_escapes() {
    let mut observations = Vec::new();
    for version in [8, 9] {
        let dir = fixture("schema", version, false).await;
        let body = raw_property(
            &unmarked(dir.path()),
            HudiTableConfig::CreateSchema.as_ref(),
            SCHEMA,
        );
        std::fs::write(properties(dir.path()), body).unwrap();
        let before = inventory(dir.path());
        let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        let raw = table
            .hudi_configs
            .get_raw(HudiTableConfig::CreateSchema)
            .unwrap();
        let schema = table.get_schema().await;
        observations.push((
            raw == SCHEMA,
            schema.as_ref().is_ok_and(|s| s.field(0).name() == "id"),
        ));
        assert_eq!(inventory(dir.path()), before);
    }
    assert_eq!(observations, [(true, true), (true, true)]);
}

#[tokio::test]
async fn test_unmarked_native_ambiguity_requires_explicit_codec() {
    let mut cases = Vec::new();
    for value in [r"line\nnext", "line\nnext"] {
        let dir = tempfile::tempdir().unwrap();
        Table::create(dir.path().to_str().unwrap())
            .with_table_name("collision")
            .with_table_version(9)
            .with_record_key_fields(["id"])
            .with_metadata(false)
            .with_option(PROPERTY, value)
            .create()
            .await
            .unwrap();
        cases.push(dir);
    }
    let dir = &cases[0];
    // Replace only the old raw writer's property value, preserving the full layout/checksum.
    let old_body = unmarked(dir.path())
        .lines()
        .map(|line| {
            if line.starts_with(&format!("{PROPERTY}=")) {
                format!("{PROPERTY}=line\\nnext\n")
            } else {
                format!("{line}\n")
            }
        })
        .collect::<String>();
    let new_body = unmarked(cases[1].path());
    assert_eq!(
        old_body, new_body,
        "different logical values produce byte-identical complete files"
    );
    std::fs::write(properties(dir.path()), old_body).unwrap();
    let before = inventory(dir.path());
    let path = dir.path().to_str().unwrap();
    let error = Table::new(path)
        .await
        .expect_err("ambiguous properties must fail");
    assert!(error.to_string().contains(CODEC), "{error}");
    for (codec, expected) in [("legacy-utf8", r"line\nnext"), ("java", "line\nnext")] {
        let table = Table::new_with_options(path, [(CODEC, codec)])
            .await
            .unwrap();
        assert_eq!(table.hudi_configs.get_raw(PROPERTY), Some(expected));
    }
    assert_eq!(inventory(dir.path()), before);
}

#[tokio::test]
async fn test_native_literal_invalid_java_escape_remains_legacy() {
    let dir = fixture("café", 9, false).await;
    let body = raw_property(
        &unmarked(dir.path()),
        HudiTableConfig::TableName.as_ref(),
        "café",
    );
    let body = raw_property(&body, PROPERTY, r"C:\user\uNOT0");
    std::fs::write(properties(dir.path()), body).unwrap();
    let before = inventory(dir.path());
    let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(table.table_name(), "café");
    assert_eq!(table.hudi_configs.get_raw(PROPERTY), Some(r"C:\user\uNOT0"));
    assert_eq!(inventory(dir.path()), before);
}

#[tokio::test]
async fn test_actual_java_files_keep_byte_stream_semantics() {
    for (version, bytes) in [
        (
            8,
            include_bytes!("fixtures/properties/java-v8.properties").as_slice(),
        ),
        (
            9,
            include_bytes!("fixtures/properties/java-v9.properties").as_slice(),
        ),
    ] {
        let dir = fixture("replaced", version, false).await;
        let mut bytes = bytes.to_vec();
        bytes.extend_from_slice(include_bytes!(
            "fixtures/properties/java-byte-stream.properties"
        ));
        std::fs::write(properties(dir.path()), bytes).unwrap();
        let before = inventory(dir.path());
        // A native-only hint cannot reinterpret an actual Java file as UTF-8.
        for codec in ["auto", "legacy-utf8", "java"] {
            let table = Table::new_with_options(dir.path().to_str().unwrap(), [(CODEC, codec)])
                .await
                .unwrap();
            assert_eq!(table.table_name(), "café");
            assert_eq!(
                table.hudi_configs.get_raw(HudiTableConfig::CreateSchema),
                Some(SCHEMA)
            );
            assert_eq!(table.get_schema().await.unwrap().field(0).name(), "id");
            for (key, expected) in [
                (
                    "hoodie.test.special",
                    " leading café =:#!\\literal\nactual-newline",
                ),
                ("hoodie.test.iso", "é"),
                ("hoodie.test.utf8", "Ã©"),
                ("hoodie.test.continued", "onetwo"),
                ("hoodie.test.supplementary", "😀"),
                ("hoodie.test.key with spaces", "value=colon:"),
            ] {
                assert_eq!(table.hudi_configs.get_raw(key), Some(expected), "{key}");
            }
        }
        assert_eq!(inventory(dir.path()), before);
    }
}

#[tokio::test]
async fn test_marker_authority_and_invalid_options_do_not_mutate() {
    let dir = fixture("marked", 9, false).await;
    let original = raw_property(&unmarked(dir.path()), PROPERTY, r"line\nnext");
    let path = dir.path().to_str().unwrap();
    for (marker, success) in [
        (MARKER.to_string(), true),
        ("# hudi-rs.properties.codec=java-v2".to_string(), false),
        ("# hudi-rs.properties.codec".to_string(), false),
        (format!("{MARKER}\n{MARKER}"), false),
        (
            format!("{MARKER}\n# hudi-rs.properties.codec=legacy-utf8"),
            false,
        ),
    ] {
        std::fs::write(properties(dir.path()), format!("{marker}\n{original}")).unwrap();
        let before = inventory(dir.path());
        let result = Table::new_with_options(path, [(CODEC, "legacy-utf8")]).await;
        if success {
            assert_eq!(
                result.unwrap().hudi_configs.get_raw(PROPERTY),
                Some("line\nnext")
            );
        } else {
            let error = result.expect_err("invalid marker must fail");
            assert!(error.to_string().contains("codec marker"), "{error}");
        }
        assert_eq!(inventory(dir.path()), before);
    }
    std::fs::write(properties(dir.path()), format!("{MARKER}\n{original}")).unwrap();
    let before = inventory(dir.path());
    let error = Table::new_with_options(path, [(CODEC, "typo")])
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains(CODEC), "{error}");
    assert_eq!(inventory(dir.path()), before);
    let empty = tempfile::tempdir().unwrap();
    let error = Table::create(empty.path().to_str().unwrap())
        .with_table_name("invalid")
        .with_option(CODEC, "java")
        .create()
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("caller-only"), "{error}");
    assert!(inventory(empty.path()).is_empty());
}

#[tokio::test]
async fn test_native_marker_and_fallback_propagate_per_metadata_file() {
    let dir = fixture("parent", 9, true).await;
    let mdt = dir.path().join(".hoodie/metadata");
    for root in [dir.path(), mdt.as_path()] {
        let body = std::fs::read_to_string(properties(root)).unwrap();
        assert!(body.is_ascii());
        assert!(body.lines().any(|line| line == MARKER));
    }
    let path = dir.path().to_str().unwrap();
    let mut healthy = Table::new(path).await.unwrap();
    healthy
        .append([RecordBatch::try_from_iter([(
            "id",
            Arc::new(StringArray::from(vec!["retained"])) as ArrayRef,
        )])
        .unwrap()])
        .await
        .unwrap();
    let reopened = Table::new(path).await.unwrap();
    let before = inventory(dir.path());
    let batches = reopened.read(&ReadOptions::new()).await.unwrap();
    let ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().to_string())
        })
        .collect();
    assert_eq!(ids, ["retained"]);
    assert_eq!(inventory(dir.path()), before);

    let marked_child = raw_property(
        &std::fs::read_to_string(properties(&mdt)).unwrap(),
        PROPERTY,
        r"caf\u00E9",
    );
    std::fs::write(properties(&mdt), marked_child).unwrap();
    let mut body = unmarked(dir.path());
    body = raw_property(&body, HudiTableConfig::CreateSchema.as_ref(), SCHEMA);
    body = raw_property(&body, PROPERTY, r"line\nnext");
    std::fs::write(properties(dir.path()), &body).unwrap();
    let before = inventory(dir.path());
    let table = Table::new_with_options(path, [(CODEC, "legacy-utf8")])
        .await
        .unwrap();
    assert_eq!(table.table_name(), "parent");
    // The marked child must use Java despite the parent's legacy fallback.
    let child = table.new_metadata_table().await.unwrap();
    assert_eq!(child.table_name(), "parent_metadata");
    assert_eq!(child.hudi_configs.get_raw(PROPERTY), Some("café"));
    assert_eq!(inventory(dir.path()), before);

    let child_body = raw_property(&unmarked(&mdt), PROPERTY, r"line\nnext");
    std::fs::write(properties(&mdt), child_body).unwrap();
    let before = inventory(dir.path());
    let child = table.new_metadata_table().await.unwrap();
    assert_eq!(child.hudi_configs.get_raw(PROPERTY), Some(r"line\nnext"));
    // Inferred legacy for the parent must not become an implicit choice for its child.
    let inferred = Table::new(path).await.unwrap();
    assert!(
        inferred
            .new_metadata_table()
            .await
            .err()
            .unwrap()
            .to_string()
            .contains(CODEC)
    );
    assert_eq!(inventory(dir.path()), before);

    // A stored fallback never chooses the decoder or propagates to an unmarked child.
    body = raw_property(&body, CODEC, "java");
    std::fs::write(properties(dir.path()), body).unwrap();
    let before = inventory(dir.path());
    let inferred = Table::new(path).await.unwrap();
    assert!(inferred.hudi_configs.get_raw(CODEC).is_none());
    assert!(inferred.new_metadata_table().await.is_err());
    let explicit = Table::new_with_options(path, [(CODEC, "legacy-utf8")])
        .await
        .unwrap();
    assert_eq!(
        explicit
            .new_metadata_table()
            .await
            .unwrap()
            .hudi_configs
            .get_raw(PROPERTY),
        Some(r"line\nnext")
    );
    assert_eq!(inventory(dir.path()), before);
}

#[tokio::test]
async fn test_empty_continuation_comment_preserves_configured_upsert_keys() {
    let uuids = [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
    ];
    let expected_rows: Vec<_> = uuids
        .iter()
        .map(|uuid| ("same".to_string(), uuid.to_string(), uuid.to_string()))
        .collect();
    let mut failures = Vec::new();
    for table_type in [TableTypeValue::CopyOnWrite, TableTypeValue::MergeOnRead] {
        for version in [8, 9] {
            for newline in ["\n", "\r\n", "\r"] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().to_str().unwrap();
                Table::create(path)
                    .with_table_name("continued")
                    .with_table_type(table_type.clone())
                    .with_table_version(version)
                    .with_record_key_fields(["id"])
                    .with_metadata(false)
                    .create()
                    .await
                    .unwrap();
                let mut body = std::fs::read_to_string(properties(dir.path())).unwrap();
                // Java ignores the comment, including its final slash, after an empty
                // continued line. The following assignment must still override `id`.
                body.push_str(
                    "\\\n# comment ends in a slash\\\nhoodie.table.recordkey.fields=uuid\n",
                );
                std::fs::write(properties(dir.path()), body.replace('\n', newline)).unwrap();
                let before = inventory(dir.path());
                let mut table = Table::new(path).await.unwrap();
                let selected = table
                    .hudi_configs
                    .get_raw(HudiTableConfig::RecordKeyFields)
                    .unwrap()
                    .to_string();
                assert_eq!(inventory(dir.path()), before);
                let batch = RecordBatch::try_from_iter([
                    (
                        "id",
                        Arc::new(StringArray::from(vec!["same", "same"])) as ArrayRef,
                    ),
                    (
                        "uuid",
                        Arc::new(StringArray::from(uuids.to_vec())) as ArrayRef,
                    ),
                ])
                .unwrap();
                table.upsert([batch]).await.unwrap();
                let reopened = Table::new(path).await.unwrap();
                let before = inventory(dir.path());
                let batches = reopened.read(&ReadOptions::new()).await.unwrap();
                let mut rows = Vec::new();
                for batch in batches {
                    let columns = ["id", "uuid", "_hoodie_record_key"].map(|name| {
                        batch
                            .column_by_name(name)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                    });
                    for row in 0..batch.num_rows() {
                        rows.push((
                            columns[0].value(row).to_string(),
                            columns[1].value(row).to_string(),
                            columns[2].value(row).to_string(),
                        ));
                    }
                }
                rows.sort();
                assert_eq!(inventory(dir.path()), before);
                if selected != "uuid" || rows != expected_rows {
                    failures.push(format!(
                        "{table_type:?} v{version} newline={newline:?}: selected={selected}, rows={rows:?}"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
