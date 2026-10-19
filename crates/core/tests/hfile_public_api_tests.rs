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
use hudi_core::file_group::log_file::log_block::{BlockType, LogBlock, LogBlockContent};
use hudi_core::file_group::log_file::log_format::LogFormatVersion;
use hudi_core::file_group::log_file::scanner::ScanResult;
use hudi_core::hfile::HFileRecord;
use std::collections::HashMap;

#[test]
fn test_original_hfile_record_and_log_public_shapes() {
    let record = HFileRecord {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
    };
    assert_eq!(record.key(), b"key");
    assert_eq!(record.value(), b"value");
    assert_eq!(
        record.clone(),
        HFileRecord::from_str_key("key", b"value".to_vec())
    );
    let HFileRecord { key, value } = record;
    assert_eq!(key, b"key");
    assert_eq!(value, b"value");
    let block = LogBlock {
        format_version: LogFormatVersion::V1,
        block_type: BlockType::HfileData,
        header: HashMap::new(),
        content: LogBlockContent::HFileRecords(vec![HFileRecord { key, value }]),
        footer: HashMap::new(),
        deferred_content: None,
        resident_content: None,
        skipped: false,
    };
    let records: Vec<HFileRecord> = match block.content {
        LogBlockContent::HFileRecords(records) => records,
        LogBlockContent::Records(_) | LogBlockContent::Empty => Vec::new(),
    };
    let records: Vec<HFileRecord> = match ScanResult::HFileRecords(records) {
        ScanResult::HFileRecords(records) => records,
        ScanResult::RecordBatches(_) | ScanResult::Empty => Vec::new(),
    };
    assert_eq!(
        records,
        vec![HFileRecord::new(b"key".to_vec(), b"value".to_vec())]
    );
}
