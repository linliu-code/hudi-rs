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
table's `record_index` partition is carried, and of that only the newest file
slice of each shard — 21 KB of the source zip's 406 KB. Contents:

* `.hoodie/hoodie.properties` — the metadata table's own properties
  (`hoodie.table.version=6`, `hoodie.table.base.file.format=HFILE`,
  `HoodieMetadataPayload`). Nothing else of the timeline is needed: the FFI read
  names its base and log files explicitly.
* `record_index/` — the **nine** HFiles written by the `20260505162917195001`
  compaction (shards `0000, 0001, 0002, 0004, 0005, 0006, 0007, 0008, 0009`;
  6.8 KB each, one record apiece — `id1` … `id9`). Among them:
  * `record-index-0005-0_4-1636-3849_20260505162917195001.hfile` — **the file the
    Java test fails on**, verbatim.
  * `record-index-0002-0_3-1636-3848_20260505162917195001.hfile`, whose log file
    below carries an Avro DATA block, so the log-block half of the resolution is
    covered.
  * `record-index-0004-0_5-1636-3850_20260505162917195001.hfile`, whose log file
    below is a DELETE block against its base row.
* `record_index/` — the **two** log files at that same instant:
  `.record-index-0002-0_20260505162917195001.log.1_0-1654-3883` (data block,
  adds `id10`) and `.record-index-0004-0_20260505162917195001.log.1_0-1713-3978`
  (delete block).
* `record_index/.hoodie_partition_metadata`.

Every shard is read by
`v6_record_index_hfiles_read_under_the_current_metadata_schema` — one shard alone
would also pass for a reader that always returned one row.

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
* every nested `namespace` that equals its enclosing one **removed**, because
  that is what Java Avro's `Schema.toString()` emits — and `SCHEMA.toString()` is
  the string `NativeIndexSliceReader` actually hands the native reader. All three
  real MDT writer schemas in this repo (v6, v8, and the v9
  `metadata_multi_block_hfile`) are in that form, since they were written by
  Java. It is not cosmetic: `arrow-avro` 58.4.0 matches named types on their
  DECLARED name (`codec.rs` `names_match` → `full_name_set`) without applying
  Avro's enclosing-namespace inheritance, so a reader schema that spells
  `org.apache.hudi.avro.model.HoodieValueTypeInfo` explicitly cannot resolve
  against a writer that inherits it — it fails loudly with
  `Record name mismatch writer=HoodieValueTypeInfo, reader=HoodieValueTypeInfo`.
* minified.

Refresh it the same way if `HoodieMetadata.avsc` gains a field — including the
namespace step; the test asserts against what is in this file, not against a
hard-coded field list.
