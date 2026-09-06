# `metadata_v6_record_index`

The `record_index` partition of a **table-version-6** (Hudi 0.14-era) metadata
table, plus the **current** `HoodieMetadataRecord` reader schema — the pair that
makes Avro schema resolution observable on the native index-reader path.

## Why it exists

`TestUpgradeFromV6IndexTypes.testUpgradePreservesIndexFunctionality[6]`
(hudi-internal, `hudi-spark-datasource/hudi-spark`) upserts into a v6 table with
the record index on. Java reads the MDT with
`HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema())` as the
Avro **reader** schema against each HFile's own **writer** schema, so Avro's
resolver matches union branches by name and fills reader-only fields from their
declared defaults. The native reader decoded with the writer schema only and then
bent the batch to the requested schema in Arrow, which cannot cast a 12-branch
union to a 14-branch one — the read failed loudly.

The v6 writer schema differs from the current one by pure additions:

| | v6 writer (in the HFile) | current `HoodieMetadata.avsc` |
|---|---|---|
| top-level | 11 fields | + `SecondaryIndexMetadata` |
| `ColumnStatsMetadata` | 9 fields | + `isTightBound` (non-nullable, `default: false`) + `valueType` |
| `ColumnStatsMetadata.minValue`/`.maxValue` | 12 union branches | + `LocalDateWrapper` + `ArrayWrapper` = 14 |
| `recordIndexMetadata` | 7 fields | + `position` |

## `v6_record_index_014.zip`

Extracted from hudi-internal's checked-in upgrade fixture
`hudi-spark-datasource/hudi-spark/src/test/resources/upgrade-downgrade-fixtures/index-tables/hudi-v6-table-index-record-index.zip`
(entries dated 2026-05-05, hence the `20260505…` instants). Only the metadata
table's `record_index` partition is carried, and only the files two slices need:

* `.hoodie/hoodie.properties` — the metadata table's own properties
  (`hoodie.table.version=6`, `hoodie.table.base.file.format=HFILE`,
  `HoodieMetadataPayload`). Nothing else of the timeline is needed: the FFI read
  names its base and log files explicitly.
* `record_index/record-index-0005-0_4-1636-3849_20260505162917195001.hfile` —
  **the file the Java test fails on**, verbatim.
* `record_index/record-index-0004-0_5-1636-3850_20260505162917195001.hfile` plus
  `record_index/.record-index-0004-0_20260505162917195001.log.1_0-1713-3978` — a
  base+log slice of the same partition, so the log-block half of the resolution
  is covered too.
* `record_index/.hoodie_partition_metadata`.

## `HoodieMetadataRecord-with-meta-fields.avsc`

The reader schema, i.e. what `HoodieBackedTableMetadata.SCHEMA` is:
`hudi-common/src/main/avro/HoodieMetadata.avsc` from hudi-internal
(`davis/rli-native-hfile-read-toy`, tip `40cac3f5c`, file last touched by
`7f6f5c9e4`), with

* the ASF `/* … */` header and the `//` comments stripped (neither is JSON; Avro's
  Jackson parser tolerates them, `serde_json`/`arrow-avro` do not),
* the five `_hoodie_*` meta fields prepended exactly as
  `HoodieAvroUtils.addMetadataFields` prepends them
  (`["null","string"]`, `doc: ""`, `default: null`),
* minified.

Refresh it the same way if `HoodieMetadata.avsc` gains a field; the test asserts
against what is in this file, not against a hard-coded field list.
