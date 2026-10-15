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
//! Java `Properties.load(InputStream)` / `store(OutputStream)` compatible codec.

use crate::config::{Result, error::ConfigError::ParseLine};
use std::collections::HashMap;

pub(crate) fn parse_properties(bytes: &[u8]) -> Result<HashMap<String, String>> {
    // Java's byte-stream format is ISO-8859-1; Unicode is represented by UTF-16 escapes.
    let text: String = bytes.iter().map(|b| char::from(*b)).collect();
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut properties = HashMap::new();
    let mut logical = String::new();
    let mut continued = false;
    for line in text.split('\n') {
        let line = line.trim_start_matches([' ', '\t', '\u{c}']);
        if !continued && (line.is_empty() || line.starts_with(['#', '!'])) {
            continue;
        }
        logical.push_str(line);
        continued = logical.bytes().rev().take_while(|b| *b == b'\\').count() % 2 == 1;
        if continued {
            logical.pop();
            continue;
        }
        parse_logical_line(&logical, &mut properties)?;
        logical.clear();
    }
    if !logical.is_empty() {
        parse_logical_line(&logical, &mut properties)?;
    }
    Ok(properties)
}

fn parse_logical_line(line: &str, properties: &mut HashMap<String, String>) -> Result<()> {
    let mut escaped = false;
    let mut end = line.len();
    for (i, ch) in line.char_indices() {
        if !escaped && matches!(ch, '=' | ':' | ' ' | '\t' | '\u{c}') {
            end = i;
            break;
        }
        escaped = ch == '\\' && !escaped;
    }
    let mut value = line[end..].trim_start_matches([' ', '\t', '\u{c}']);
    if value.starts_with(['=', ':']) {
        value = &value[1..];
    }
    value = value.trim_start_matches([' ', '\t', '\u{c}']);
    properties.insert(unescape(&line[..end])?, unescape(value)?);
    Ok(())
}

fn unescape(input: &str) -> Result<String> {
    let mut units = Vec::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        let ch = if ch == '\\' {
            match chars.next() {
                Some('u') => {
                    let mut code = 0u16;
                    for _ in 0..4 {
                        let digit = chars.next().and_then(|c| c.to_digit(16)).ok_or_else(|| {
                            ParseLine("malformed Unicode escape in properties".to_string())
                        })?;
                        code = (code << 4) | digit as u16;
                    }
                    units.push(code);
                    continue;
                }
                Some('t') => '\t',
                Some('n') => '\n',
                Some('r') => '\r',
                Some('f') => '\u{c}',
                Some(ch) => ch,
                None => break,
            }
        } else {
            ch
        };
        let mut encoded = [0; 2];
        units.extend_from_slice(ch.encode_utf16(&mut encoded));
    }
    String::from_utf16(&units).map_err(|e| ParseLine(format!("invalid UTF-16 in properties: {e}")))
}

pub(crate) fn escape_property(input: &str, key: bool) -> String {
    let mut out = String::new();
    for (i, ch) in input.chars().enumerate() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\u{c}' => out.push_str("\\f"),
            ' ' if key || i == 0 => out.push_str("\\ "),
            '=' | ':' | '#' | '!' => {
                out.push('\\');
                out.push(ch);
            }
            ch if !(' '..='~').contains(&ch) => {
                let mut units = [0; 2];
                for unit in ch.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04X}"));
                }
            }
            ch => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_java_properties_grammar_and_unicode() {
        // Equivalent to java.util.Properties.load on this byte stream.
        let bytes = b"!comment\r\n  key\\ with\\ spaces : \\ leading\\tvalue  \r\ncontinued=one\\\r\n  two\nlatin=\xe9\nunicode=\\uD83D\\uDE00\nempty\n";
        let props = parse_properties(bytes).unwrap();
        assert_eq!(props["key with spaces"], " leading\tvalue  ");
        assert_eq!(props["continued"], "onetwo");
        assert_eq!(props["latin"], "é");
        assert_eq!(props["unicode"], "😀");
        assert_eq!(props["empty"], "");
        assert!(parse_properties(br"bad=\uNOT0").is_err());
        assert!(parse_properties(br"bad=\uD800").is_err());
    }

    #[test]
    fn test_properties_roundtrip_special_values() {
        for value in [
            "café",
            "😀",
            " leading and trailing ",
            "line\nnext\t",
            r"path\next",
            "=:#!",
        ] {
            let key = " key:=\\";
            let encoded = format!(
                "{}={}\n",
                escape_property(key, true),
                escape_property(value, false)
            );
            assert!(encoded.is_ascii());
            assert_eq!(parse_properties(encoded.as_bytes()).unwrap()[key], value);
        }
    }
}
