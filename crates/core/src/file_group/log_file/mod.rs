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
use crate::Result;
use crate::error::CoreError;
use crate::storage::file_metadata::FileMetadata;
use crate::timeline::completion_time::CompletionTimeView;
use std::cmp::Ordering;
use std::fmt::Display;
use std::str::FromStr;

pub(crate) mod avro;
pub(crate) mod content;
pub mod log_block;
pub mod log_format;
pub mod reader;
pub mod scanner;

/// Represents a Hudi log file (delta log).
///
/// The `timestamp` field is the timestamp embedded in the log file name:
/// - For v6 tables: base commit timestamp (matches the base file's commit timestamp).
/// - For v8+ tables: request instant timestamp of the deltacommit.
///
/// The `completion_timestamp` field is used to determine file slice association
/// (which base instant a log file belongs to):
/// - For v6 tables: This is always `None` (v6 does not track completion timestamps).
/// - For v8+ tables: Set from the timeline when the commit is completed.
///   If `None`, the commit is still pending and the file should not be included in queries.
///
/// Ordering ([Ord]) is by `timestamp` (the deltaCommitTime / request instant) → `version`
/// → `write_token` → `suffix`, mirroring all four keys of Java's
/// `HoodieLogFile.getLogFileComparator`. Completion time is NOT used for ordering — only
/// for slice association and committed-file filtering.
///
/// That 4th key, `suffix`, is Java's `getSuffix()` — the optional `.cdc` marker — so the ordering
/// matches `getLogFileComparator` on all four of its keys.
///
/// Two further keys, `extension` then `file_id`, follow those. They are **not** gold keys
/// (Java compares neither). They exist so that **no two files with distinct `file_name()`
/// fields compare `Equal`** — the key set covers every field `file_name()` interpolates, so a
/// `BTreeSet<LogFile>` cannot silently drop one of them. Neither can reorder anything the gold
/// orders: they only separate files all four gold keys tie.
///
/// The converse of that guarantee does NOT hold, and is not this type's to fix: [PartialEq]
/// compares the *formatted* `file_name()`, which is lossy across the `_` separators, so two
/// different field-sets can render one name and compare `Equal` while `cmp` says otherwise
/// (`{file_id: "a", timestamp: "b_c"}` and `{file_id: "a_b", timestamp: "c"}`). Unreachable
/// through `parse_file_name`, which splits on the first `_`, but the fields are `pub`.
/// ⚠️ **Adding a field here is a breaking change for downstream struct-literal
/// construction.** Every field is `pub` and the type is deliberately NOT
/// `#[non_exhaustive]` — making it so now would forbid literal construction
/// outright, which is a larger break than the one it prevents. `suffix` was added
/// this way (m22); if you add another, say so in the commit message, because
/// nothing in the type system will.
#[derive(Clone, Debug)]
pub struct LogFile {
    pub file_id: String,
    /// The timestamp embedded in the log file name.
    pub timestamp: String,
    /// The completion timestamp of the commit that wrote this log file.
    ///
    /// For v6 tables: This is always `None` (v6 does not track completion timestamps).
    /// For v8+ tables: Set from the timeline; `None` if commit is pending.
    pub completion_timestamp: Option<String>,
    pub extension: String,
    /// Log file version number. Starts at 1 and increments when the log file rolls over
    /// (e.g., reaches size limit) within the same delta commit by the same writer.
    pub version: u32,
    pub write_token: String,
    /// Java's `getSuffix()` — `FSUtils.LOG_FILE_PATTERN` group 10, the optional
    /// `.cdc` marker on a change-data-capture log file.
    ///
    /// `""` when the file name carries no marker, mirroring Java's
    /// `matcher.group(10) == null ? "" : matcher.group(10)`. Stored WITH its
    /// leading dot, as Java stores it, so it appends to the write token without a
    /// separator in [`Self::file_name`].
    pub suffix: String,
    pub file_metadata: Option<FileMetadata>,
}

const LOG_FILE_PREFIX: char = '.';

/// Java's `HoodieCDCUtils.CDC_LOGFILE_SUFFIX`. The only value `suffix` can take
/// other than `""`: `LOG_FILE_PATTERN`'s group 10 is the literal `(\.cdc)?`.
const CDC_LOGFILE_SUFFIX: &str = ".cdc";

impl LogFile {
    /// Whether a file name is a log file's rather than a base file's.
    ///
    /// A log-only write stat names its log file in `path`, where a base file
    /// name would otherwise be expected, so the two have to be told apart
    /// before either is parsed.
    pub fn is_log_file_name(file_name: &str) -> bool {
        file_name.starts_with(LOG_FILE_PREFIX)
    }
}

impl LogFile {
    /// Parse a log file's name into parts.
    ///
    /// File name format:
    ///
    /// ```text
    /// .[File Id]_[Base commit or deltacommit's timestamp].[Log File Extension].[Log File Version]_[File Write Token][.cdc]
    /// ```
    ///
    /// The trailing `.cdc` is optional and is Java's `LOG_FILE_PATTERN` group 10,
    /// returned here as the last tuple element. It is stripped off the write
    /// token rather than left glued to it: Java's group 6 is the write token
    /// ALONE, and its comparator orders on the two separately — write token
    /// third, suffix fourth.
    fn parse_file_name(file_name: &str) -> Result<(String, String, String, u32, String, String)> {
        let err_msg = format!("Failed to parse file name '{file_name}' for log file.");

        if !file_name.starts_with(LOG_FILE_PREFIX) {
            return Err(CoreError::FileGroup(err_msg));
        }

        let file_name = &file_name[LOG_FILE_PREFIX.len_utf8()..];

        let (file_id, rest) = file_name
            .split_once('_')
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?;

        let (middle, file_write_token) = rest
            .rsplit_once('_')
            .ok_or_else(|| CoreError::FileGroup(err_msg.clone()))?;

        // Group 10: the `.cdc` marker rides on the END of the name, after the
        // write token, so it has to come off before the token is validated.
        let (file_write_token, suffix) = match file_write_token.strip_suffix(CDC_LOGFILE_SUFFIX) {
            Some(token) => (token, CDC_LOGFILE_SUFFIX),
            None => (file_write_token, ""),
        };

        let parts: Vec<&str> = middle.split('.').collect();
        if parts.len() != 3 {
            return Err(CoreError::FileGroup(err_msg.clone()));
        }

        let timestamp = parts[0];
        let log_file_extension = parts[1];
        let log_file_version_str = parts[2];

        if file_id.is_empty()
            || timestamp.is_empty()
            || log_file_extension.is_empty()
            || log_file_version_str.is_empty()
            || file_write_token.is_empty()
        {
            return Err(CoreError::FileGroup(err_msg.clone()));
        }

        let log_file_version = log_file_version_str
            .parse::<u32>()
            .map_err(|_| CoreError::FileGroup(err_msg.clone()))?;

        Ok((
            file_id.to_string(),
            timestamp.to_string(),
            log_file_extension.to_string(),
            log_file_version,
            file_write_token.to_string(),
            suffix.to_string(),
        ))
    }

    #[inline]
    pub fn file_name(&self) -> String {
        format!(
            "{prefix}{file_id}_{timestamp}.{extension}.{version}_{write_token}{suffix}",
            prefix = LOG_FILE_PREFIX,
            file_id = self.file_id,
            timestamp = self.timestamp,
            extension = self.extension,
            version = self.version,
            write_token = self.write_token,
            suffix = self.suffix
        )
    }

    /// Returns true if this log file has a completion timestamp (i.e., the commit is completed).
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.completion_timestamp.is_some()
    }

    /// Set the completion timestamp from a completion time view.
    ///
    /// Looks up the completion timestamp using this file's `timestamp`
    /// (request time) and sets `completion_timestamp` if found.
    ///
    /// For v6 tables, the view returns `None` and this is a no-op.
    /// For v8+ tables, this sets the completion timestamp for completed commits.
    pub fn set_completion_time<V: CompletionTimeView>(&mut self, view: &V) {
        self.completion_timestamp = view
            .get_completion_time(&self.timestamp)
            .map(|s| s.to_string());
    }
}

impl FromStr for LogFile {
    type Err = CoreError;

    /// Parse a log file name into a [LogFile].
    ///
    /// Note: `completion_timestamp` is set to `None` by default. For v6 tables,
    /// it should remain `None` (v6 does not track completion times). For v8+ tables,
    /// the caller should set it from the timeline.
    fn from_str(file_name: &str) -> Result<Self, Self::Err> {
        let (file_id, timestamp, extension, version, write_token, suffix) =
            Self::parse_file_name(file_name)?;
        Ok(LogFile {
            file_id,
            timestamp,
            completion_timestamp: None,
            extension,
            version,
            write_token,
            suffix,
            file_metadata: None,
        })
    }
}

impl TryFrom<FileMetadata> for LogFile {
    type Error = CoreError;

    fn try_from(metadata: FileMetadata) -> Result<Self> {
        let file_name = metadata.name.as_str();
        let (file_id, timestamp, extension, version, write_token, suffix) =
            Self::parse_file_name(file_name)?;
        Ok(LogFile {
            file_id,
            timestamp,
            completion_timestamp: None,
            extension,
            version,
            write_token,
            suffix,
            file_metadata: Some(metadata),
        })
    }
}

impl Display for LogFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogFile: {}", self.file_name())
    }
}

impl PartialEq for LogFile {
    fn eq(&self, other: &Self) -> bool {
        self.file_name() == other.file_name()
    }
}

impl Eq for LogFile {}

impl PartialOrd for LogFile {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogFile {
    fn cmp(&self, other: &Self) -> Ordering {
        // GOLD KEYS — Java's `HoodieLogFile.LogFileComparator` (`getLogFileComparator`),
        // keys 1-3: deltaCommitTime (the request instant embedded in the file name, stored
        // here as `timestamp`) → logVersion → logWriteToken.
        //
        // Completion time is deliberately absent. Java uses it to bucket a log file into
        // the right file slice (`HoodieFileGroup.getBaseInstantTime`) and to filter
        // uncommitted files, never to order the merge — see
        // `hudi-common/.../table/read/InputSplit.java`, which sorts the MOR read's own log
        // list with this comparator. Ordering by completion time applies log blocks in the
        // wrong sequence whenever writers complete out of request order, which silently
        // picks a different merge winner than the Java reader for the same table.
        //
        // GOLD KEY 4 — `getSuffix()`, `FSUtils.LOG_FILE_PATTERN` group 10, i.e. the `.cdc`
        // marker, and NOT the file extension. `parse_file_name` now pulls it into its own
        // field, so this is the gold's key and sits where the gold puts it: after the write
        // token, before anything of hudi-rs's own.
        //
        // ⚠️ It has to land in the SAME change as the parsing, and that is not a style
        // preference. Before the parse, `.cdc` was glued onto `write_token`, so a CDC file
        // and its non-CDC sibling differed in key 3 and a `BTreeSet<LogFile>` kept both.
        // Extracting the suffix makes them tie on keys 1-3, on `extension` and on
        // `file_id` — so without this key they compare `Equal`, are `!=` under `Eq`, and
        // the set silently drops one. No fixture could express that case before the parse
        // existed, which is why it would have landed green.
        // `cdc_and_non_cdc_siblings_do_not_collapse_in_a_btreeset` is that fixture.
        //
        // EQ-CONSISTENCY KEYS, which are hudi-rs's and not the gold's. `Ord` must agree
        // with `Eq`, and `Eq` compares the whole `file_name()` — file_id, timestamp,
        // extension, version, write_token, suffix. A `BTreeSet<LogFile>` dedups on `Ord`
        // returning `Equal`, so any `file_name()` field missing from the key set lets two
        // distinct files collapse and one be silently dropped. `extension` and `file_id`
        // are the two the gold's four keys do not cover — Java compares neither. Neither
        // can reorder anything the gold orders: they are only consulted when all FOUR gold
        // keys tie, and a `FileSlice` holds one file group, so `file_id` is constant
        // wherever the gold's ordering applies.
        self.timestamp
            .cmp(&other.timestamp)
            .then(self.version.cmp(&other.version))
            .then(self.write_token.cmp(&other.write_token))
            .then(self.suffix.cmp(&other.suffix))
            .then(self.extension.cmp(&other.extension))
            .then(self.file_id.cmp(&other.file_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_file_name_in_formatted_str() {
        let filename = ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115";
        let log_file = LogFile::from_str(filename).unwrap();
        assert!(format!("{log_file}").contains(filename));
    }

    #[test]
    fn test_valid_filename_parsing() {
        let filename = ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115";
        let log_file = LogFile::from_str(filename).unwrap();

        assert_eq!(log_file.file_id, "54e9a5e9-ee5d-4ed2-acee-720b5810d380-0");
        assert_eq!(log_file.timestamp, "20250109233025121");
        assert_eq!(log_file.extension, "log");
        assert_eq!(log_file.version, 1);
        assert_eq!(log_file.write_token, "0-51-115");
        // Completion timestamp is None after parsing (set separately from timeline)
        assert_eq!(log_file.completion_timestamp, None);
        assert!(!log_file.is_completed());
    }

    #[test]
    fn test_filename_reconstruction() {
        let original = ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115";
        let log_file = LogFile::from_str(original).unwrap();
        assert_eq!(log_file.file_name(), original);
    }

    #[test]
    fn test_missing_dot_prefix() {
        let filename = "myfile_20250109233025121.log.v1_abc123";
        assert!(matches!(
            LogFile::from_str(filename),
            Err(CoreError::FileGroup(_))
        ));
    }

    #[test]
    fn test_missing_first_underscore() {
        let filename = ".myfile20250109233025121.log.v1_abc123";
        assert!(matches!(
            LogFile::from_str(filename),
            Err(CoreError::FileGroup(_))
        ));
    }

    #[test]
    fn test_missing_last_underscore() {
        let filename = ".myfile_20250109233025121.log.v1abc123";
        assert!(matches!(
            LogFile::from_str(filename),
            Err(CoreError::FileGroup(_))
        ));
    }

    #[test]
    fn test_incorrect_dot_parts() {
        let filename = ".myfile_20250109233025121.log.v1.extra_abc123";
        assert!(matches!(
            LogFile::from_str(filename),
            Err(CoreError::FileGroup(_))
        ));
    }

    #[test]
    fn test_empty_components() {
        let filenames = vec![
            "._20250109233025121.log.v1_abc123",     // empty file_id
            ".myfile_.log.v1_abc123",                // empty timestamp
            ".myfile_20250109233025121..v1_abc123",  // empty extension
            ".myfile_20250109233025121.log._abc123", // empty version
            ".myfile_20250109233025121.log.v1_",     // empty token
        ];

        for filename in filenames {
            assert!(matches!(
                LogFile::from_str(filename),
                Err(CoreError::FileGroup(_))
            ));
        }
    }

    #[test]
    fn test_log_file_ordering_no_completion_timestamp() {
        // When no completion_timestamp is set, ordering falls back to request timestamp
        // This simulates v6 table behavior or uncommitted files in v8+ tables
        let log1 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        let log2 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 2,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        // Different timestamp
        let log3 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            timestamp: "20250113230424191".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        // Same timestamp and version, different write token
        let log4 = LogFile {
            file_id: "ee2ace10-7667-40f5-9848-0a144b5ea064-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "1-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        // Test ordering by timestamp, then version, then write_token
        assert!(log1 < log2, "version ordering failed");
        assert!(log1 < log3, "timestamp ordering failed");
        assert!(log2 < log3, "timestamp ordering failed");
        assert!(log1 < log4, "write token ordering failed");

        // Test sorting
        let mut logs = vec![log3.clone(), log4.clone(), log1.clone(), log2.clone()];
        logs.sort();
        assert_eq!(logs, vec![log1, log4, log2, log3]);

        // Test version 10 > version 2 (integer ordering, not string ordering)
        let log_v2 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 2,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        let log_v10 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 10,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        assert!(
            log_v2 < log_v10,
            "version 2 should be less than version 10 (integer ordering)"
        );
    }

    #[test]
    fn test_log_file_ordering_ignores_completion_timestamp() {
        // Gold parity: completion timestamp must NOT influence ordering. Ordering is
        // by deltaCommitTime (`timestamp`) alone (then version, then write_token),
        // regardless of whether/what completion timestamps are set. This mirrors
        // Java's `HoodieLogFile.getLogFileComparator`, which keys only off the
        // file-name fields.

        // Later request instant, but earliest completion.
        let log1 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: Some("20250113230310000".to_string()),
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        // Earliest request instant, but latest completion.
        let log2 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230300000".to_string(), // earliest request time
            completion_timestamp: Some("20250113230320000".to_string()), // latest completion time
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        // Latest request instant, and no completion timestamp. On a layout-v2 table
        // this is not only a pending file: an ARCHIVED deltacommit also has no entry
        // in the completion map yet passes `TimelineView::is_committed`, so a mixed
        // Some/None pair is reachable in production. Ordering by request instant is
        // what makes that case come out right.
        let log3 = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230305000".to_string(), // latest request time
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        // Ordering follows request/deltaCommit time (log2 < log1 < log3), NOT completion.
        assert!(log2 < log1, "order by request instant, not completion");
        assert!(log1 < log3, "order by request instant, not completion");
        assert!(log2 < log3, "order by request instant, not completion");

        // Presence/absence of a completion timestamp does not reorder relative to
        // request instant: log3 (no completion) still sorts last by its request time.
        let mut logs = vec![log3.clone(), log1.clone(), log2.clone()];
        logs.sort();
        assert_eq!(logs, vec![log2, log1, log3]);
    }

    #[test]
    fn test_log_file_ordering_matches_java_delta_commit_time() {
        // Gold parity: `HoodieLogFile.LogFileComparator` orders log files by
        // deltaCommitTime (the request instant embedded in the file name) →
        // logVersion → writeToken. It NEVER uses completion time for ordering.
        //
        // This exercises the divergence case: two committed v8+ log files whose
        // completion order is the INVERSE of their delta-commit (request) order,
        // as happens with concurrent writers that complete out of request order.
        // The merge sequence must follow delta-commit time, matching the Java
        // reader — otherwise the last-applied (winning) record differs.

        // Earlier request instant, but COMPLETES later.
        let earlier_request = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230300000".to_string(), // earlier deltaCommitTime
            completion_timestamp: Some("20250113230320000".to_string()), // later completion
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        // Later request instant, but COMPLETES earlier.
        let later_request = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230310000".to_string(), // later deltaCommitTime
            completion_timestamp: Some("20250113230315000".to_string()), // earlier completion
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        assert!(
            earlier_request < later_request,
            "log files must order by deltaCommitTime (request instant), like Java's \
             getLogFileComparator, not by completion timestamp"
        );

        let mut logs = vec![later_request.clone(), earlier_request.clone()];
        logs.sort();
        assert_eq!(
            logs,
            vec![earlier_request.clone(), later_request],
            "sorted order must follow deltaCommitTime ascending"
        );

        // KEY ORDER, gold keys 2 and 3: logVersion outranks logWriteToken. Every other
        // precedence pair in `impl Ord` is pinned somewhere -- deltaCommitTime over version
        // by `test_log_file_ordering_no_completion_timestamp`, version over extension and
        // extension over file_id by `..._tiebreaks_are_extension_then_file_id` -- but this
        // adjacent pair of the GOLD's own keys had no fixture where the two disagree, so
        // swapping them in `impl Ord` changed nothing any test could see (review round 12).
        //
        // The two keys must CONFLICT or the assertion is vacuous: `earlier_request` is
        // {version 1, write_token "0-188-387"}; this is {version 2, write_token "0-188-000"}.
        // Version-first says Less (1 < 2); write-token-first says Greater ("387" > "000").
        let later_version_earlier_token = LogFile {
            version: 2,
            write_token: "0-188-000".to_string(),
            suffix: String::new(),
            ..earlier_request.clone()
        };
        assert_eq!(
            earlier_request.cmp(&later_version_earlier_token),
            Ordering::Less,
            "logVersion must take precedence over logWriteToken, as in Java's \
             LogFileComparator (Integer.compare on version, then the write token)"
        );
    }

    #[test]
    fn test_log_file_ordering_tiebreaks_are_extension_then_file_id() {
        // `extension` and `file_id` are hudi-rs's OWN keys, not the gold's — Java's 4th
        // key is `getSuffix()`, the `.cdc` marker, which hudi-rs does not parse, and Java
        // never compares `fileId` at all (see `impl Ord`). They exist so that `Ord` agrees
        // with `Eq`, which compares the whole `file_name()`: without them two distinct
        // files can compare `Equal` and a `BTreeSet<LogFile>` silently drops one.
        //
        // This test pins their presence AND their relative order, which is the part a
        // key-drop mutation cannot see.
        let base = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "cdc".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };
        // Identical to `base` in every ordering key except extension ("cdc" < "log").
        let log = LogFile {
            extension: "log".to_string(),
            ..base.clone()
        };

        // Extension breaks the tie: "cdc" < "log".
        assert_eq!(base.cmp(&log), Ordering::Less);
        // ...and they are NOT equal, so a BTreeSet must keep both.
        assert_ne!(base, log);

        let mut set = std::collections::BTreeSet::new();
        set.insert(log.clone());
        set.insert(base.clone());
        assert_eq!(set.len(), 2, "distinct extensions must not collapse");
        // Ordered ascending: the "cdc" extension sorts before "log".
        assert_eq!(
            set.into_iter().collect::<Vec<_>>(),
            vec![base.clone(), log.clone()]
        );

        // KEY ORDER: version outranks extension. The two keys must DISAGREE here or
        // the assertion cannot fail -- `base` is {version 1, extension "cdc"}, so
        // comparing it against {version 2, extension "log"} has both keys pointing the
        // same way and passes under either order. Flip the extension so they conflict:
        // version-first says Less (1 < 2), extension-first says Greater ("log" > "cdc").
        let later_version_earlier_extension = LogFile {
            extension: "cdc".to_string(),
            version: 2,
            ..log.clone() // {extension "log", version 1}
        };
        assert_eq!(
            log.cmp(&later_version_earlier_extension),
            Ordering::Less,
            "version must take precedence over extension"
        );

        // And `file_id` is last of all, after extension -- the same conflict shape.
        let other_group_earlier_extension = LogFile {
            file_id: "file-1".to_string(),
            extension: "cdc".to_string(),
            ..log.clone() // {file_id "file-0", extension "log"}
        };
        assert_eq!(
            log.cmp(&other_group_earlier_extension),
            Ordering::Greater,
            "extension must take precedence over file_id"
        );

        // `file_id` closes the last `file_name()` field the gold keys do not cover:
        // without it two files from DIFFERENT groups that tie on every other key
        // compare Equal while being `!=`, and a BTreeSet silently drops one.
        let other_group = LogFile {
            file_id: "file-1".to_string(),
            ..log.clone()
        };
        assert_ne!(log, other_group);
        assert_eq!(log.cmp(&other_group), Ordering::Less);
        let mut cross_group = std::collections::BTreeSet::new();
        cross_group.insert(log.clone());
        cross_group.insert(other_group);
        assert_eq!(
            cross_group.len(),
            2,
            "distinct file_ids must not collapse either"
        );
    }

    #[test]
    fn test_log_file_ordering_puts_an_archived_log_file_first() {
        // The mixed Some/None case, which is REACHABLE in production and which the
        // pre-m7.3 `Ord` got backwards.
        //
        // `completion_timestamp: None` does not mean "pending". On a layout-v2 table
        // `TimelineView::is_committed` (timeline/view.rs) also admits a request below
        // `earliest_active_instant` -- an ARCHIVED deltacommit, which is completed by
        // definition -- while `get_completion_time` reads a map built from the ACTIVE
        // timeline only. So an archived log file passes the committed filter
        // (builder.rs / table/listing.rs) and reaches a slice carrying `None`.
        //
        // Archived means older than every active instant, so it must merge FIRST. The
        // old `(None, Some(_)) => Ordering::Greater` arm sorted it after every completed
        // file, making the oldest log in the slice the merge winner. Ordering by request
        // instant is what makes the case come out right, and this is the pair that says
        // so: the `None` file has the EARLIER request instant, so under the old arms it
        // sorted last and under the gold's ordering it sorts first.
        let archived = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230300000".to_string(), // earliest request
            completion_timestamp: None,                 // archived: not in the active map
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };
        let active = LogFile {
            timestamp: "20250113230310000".to_string(),
            completion_timestamp: Some("20250113230315000".to_string()),
            ..archived.clone()
        };

        assert!(
            archived < active,
            "an archived log file is older than every active one and must merge first"
        );
        let mut logs = vec![active.clone(), archived.clone()];
        logs.sort();
        assert_eq!(logs, vec![archived, active]);
    }

    /// RED-1 (m22, the `.cdc` item): the `.cdc` marker must be its own field, not
    /// glued onto the write token. Java parses it as `LOG_FILE_PATTERN` group 10
    /// and `getWriteToken()` as group 6; today hudi-rs's `rsplit_once('_')` hands
    /// the whole `1-1-1.cdc` back as the write token.
    #[test]
    fn cdc_suffix_is_parsed_into_its_own_field() {
        let cdc = LogFile::from_str(".file1_2.log.1_1-1-1.cdc").unwrap();
        assert_eq!(cdc.write_token, "1-1-1", "group 6 is the write token alone");
        assert_eq!(cdc.suffix, ".cdc", "group 10 is the suffix");

        let plain = LogFile::from_str(".file1_2.log.1_1-1-1").unwrap();
        assert_eq!(plain.write_token, "1-1-1");
        assert_eq!(
            plain.suffix, "",
            "Java's getSuffix() is \"\" when group 10 is null"
        );
    }

    /// Round-trip: `file_name()` must reproduce the name it was parsed from, with
    /// and without the marker. `PartialEq` compares the formatted name, so a
    /// suffix that parsed but did not render would make a CDC file `==` its
    /// non-CDC sibling while `cmp` said otherwise.
    #[test]
    fn cdc_suffix_round_trips_through_file_name() {
        for name in [
            ".file1_2.log.1_1-1-1.cdc",
            ".file1_2.log.1_1-1-1",
            ".54e9a5e9-ee5d-4ed2-acee-720b5810d380-0_20250109233025121.log.1_0-51-115.cdc",
        ] {
            assert_eq!(LogFile::from_str(name).unwrap().file_name(), name);
        }
    }

    /// A write token that merely CONTAINS "cdc" is not a suffix. Only a trailing
    /// `.cdc` is group 10; `strip_suffix` must not fire on anything else.
    #[test]
    fn cdc_suffix_is_only_stripped_when_it_is_the_whole_trailing_marker() {
        let not_a_suffix = LogFile::from_str(".file1_2.log.1_1-cdc-1").unwrap();
        assert_eq!(not_a_suffix.write_token, "1-cdc-1");
        assert_eq!(not_a_suffix.suffix, "");

        // A name that is ONLY the marker where the write token should be leaves an
        // empty token, which the existing emptiness check must still reject.
        assert!(LogFile::from_str(".file1_2.log.1_.cdc").is_err());

        // ⚠️ A DOUBLE marker is accepted here and rejected by Java, whose group 10 is a
        // single `(\.cdc)?`. Only one `.cdc` comes off, so the rest stays on the token.
        // This is a consequence of hudi-rs never validating the write token against
        // `\d+-\d+-\d+` — which predates the suffix and is unchanged by it — rather
        // than of the stripping. Pinned so the gap is a recorded decision and not a
        // surprise: tightening it means validating the token's shape, which would also
        // reject names this reader accepts today.
        let double = LogFile::from_str(".file1_2.log.1_1-1-1.cdc.cdc").unwrap();
        assert_eq!(double.write_token, "1-1-1.cdc");
        assert_eq!(double.suffix, ".cdc");
        assert_eq!(
            double.file_name(),
            ".file1_2.log.1_1-1-1.cdc.cdc",
            "still round-trips"
        );
    }

    /// **Gold parity.** Java's own `TestFSUtils.testLogFilesComparisonWithCDCFile`,
    /// re-expressed: the same five names, the same `TreeSet`/`BTreeSet` insertion,
    /// the same expected order.
    ///
    /// Gold captured at `hudi-internal origin/master-1x`
    /// `4fbf94d3cfd4c08b82cd48a901594fec764fe413`; the comparator and
    /// `FSUtils.LOG_FILE_PATTERN` are quoted in this effort's archived artifact
    /// `.../09110013-09110300-.../evidence/raw/ac4-gold-master1x.txt`.
    #[test]
    fn log_file_ordering_matches_java_testlogfilescomparisonwithcdcfile() {
        // makeLogFileName(fileId, ".log", deltaCommitTime, version, writeToken)
        let log1 = LogFile::from_str(".file1_1.log.0_0-0-1").unwrap();
        let log2 = LogFile::from_str(".file1_2.log.0_0-0-1").unwrap();
        let log3 = LogFile::from_str(".file1_2.log.1_0-0-1").unwrap();
        let log4 = LogFile::from_str(".file1_2.log.1_1-1-1").unwrap();
        let log5 = LogFile::from_str(".file1_2.log.1_1-1-1.cdc").unwrap();

        let mut set = std::collections::BTreeSet::new();
        for lf in [&log4, &log2, &log5, &log1, &log3] {
            set.insert(lf.clone());
        }
        assert_eq!(set.len(), 5, "no two of the gold's five names may collapse");
        assert_eq!(
            set.into_iter().map(|lf| lf.file_name()).collect::<Vec<_>>(),
            vec![
                log1.file_name(),
                log2.file_name(),
                log3.file_name(),
                log4.file_name(),
                log5.file_name(),
            ],
            "must match Java's TestFSUtils.testLogFilesComparisonWithCDCFile"
        );
    }

    /// **The fixture `I-23` said could not exist before `.cdc` parsing landed, and
    /// the reason the parse and the comparator key had to land together.**
    ///
    /// A CDC log file and its non-CDC sibling differ in exactly one thing: the
    /// marker. Once the marker is its own field they tie on `timestamp`,
    /// `version`, `write_token`, `extension` AND `file_id` — so with the gold's
    /// 4th key absent they compare `Equal`, while `Eq` (which compares
    /// `file_name()`) says they are different files. A `BTreeSet<LogFile>` — which
    /// is what `FileSlice::log_files` is — then silently drops one, and the read
    /// loses a whole log file with nothing failing.
    ///
    /// Before the parse this case was unreachable: `.cdc` rode on `write_token`,
    /// so the two differed in key 3. That is why no existing fixture covers it and
    /// why this one is the point of the change rather than an extra.
    #[test]
    fn cdc_and_non_cdc_siblings_do_not_collapse_in_a_btreeset() {
        let plain = LogFile::from_str(".file-0_20250113230302428.log.1_0-188-387").unwrap();
        let cdc = LogFile::from_str(".file-0_20250113230302428.log.1_0-188-387.cdc").unwrap();

        // They tie on all three of the gold's first keys, and on both of hudi-rs's own.
        assert_eq!(plain.timestamp, cdc.timestamp);
        assert_eq!(plain.version, cdc.version);
        assert_eq!(plain.write_token, cdc.write_token);
        assert_eq!(plain.extension, cdc.extension);
        assert_eq!(plain.file_id, cdc.file_id);
        // ...and are different files.
        assert_ne!(plain, cdc);

        assert_eq!(
            plain.cmp(&cdc),
            Ordering::Less,
            "the gold's 4th key must separate them: \"\" < \".cdc\""
        );

        let mut set = std::collections::BTreeSet::new();
        set.insert(cdc.clone());
        set.insert(plain.clone());
        assert_eq!(
            set.len(),
            2,
            "a CDC log file and its non-CDC sibling must both survive a BTreeSet"
        );
        assert_eq!(
            set.into_iter().collect::<Vec<_>>(),
            vec![plain, cdc],
            "the non-CDC file sorts first, as in Java"
        );
    }

    /// KEY ORDER: the gold's `suffix` outranks both of hudi-rs's own keys. The two
    /// must DISAGREE in each pair or the assertion is vacuous.
    #[test]
    fn log_file_ordering_suffix_outranks_extension_and_file_id() {
        let base = LogFile {
            file_id: "file-0".to_string(),
            timestamp: "20250113230302428".to_string(),
            completion_timestamp: None,
            extension: "log".to_string(),
            version: 1,
            write_token: "0-188-387".to_string(),
            suffix: String::new(),
            file_metadata: None,
        };

        // suffix vs extension: suffix-first says Less ("" < ".cdc"); extension-first
        // says Greater ("log" > "cdc").
        let cdc_suffix_earlier_extension = LogFile {
            suffix: ".cdc".to_string(),
            extension: "cdc".to_string(),
            ..base.clone()
        };
        assert_eq!(
            base.cmp(&cdc_suffix_earlier_extension),
            Ordering::Less,
            "suffix must take precedence over extension"
        );

        // suffix vs file_id: suffix-first says Less; file_id-first says Greater.
        let cdc_suffix_earlier_file_id = LogFile {
            suffix: ".cdc".to_string(),
            file_id: "file-".to_string(),
            ..base.clone()
        };
        assert_eq!(
            base.cmp(&cdc_suffix_earlier_file_id),
            Ordering::Less,
            "suffix must take precedence over file_id"
        );

        // ...and the write token still outranks the suffix, as in Java, which
        // compares the token before it ever looks at the suffix. The two keys must
        // CONFLICT or the assertion is vacuous, so the pair below gives the EARLIER
        // token the LATER suffix: token-first says Less ("0-188-387" < "0-188-999"),
        // suffix-first says Greater (".cdc" > "").
        let later_token_earlier_suffix = LogFile {
            write_token: "0-188-999".to_string(),
            suffix: String::new(),
            ..base.clone()
        };
        let earlier_token_later_suffix = LogFile {
            suffix: ".cdc".to_string(),
            ..base.clone()
        };
        assert_eq!(
            earlier_token_later_suffix.cmp(&later_token_earlier_suffix),
            Ordering::Less,
            "logWriteToken must take precedence over suffix, as in Java's LogFileComparator"
        );
    }
}

#[cfg(test)]
mod memory_bench;
