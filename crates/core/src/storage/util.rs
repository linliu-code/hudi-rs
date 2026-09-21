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

//! Utility functions for storage.
use url::Url;

use crate::storage::Result;
use crate::storage::error::StorageError::{InvalidPath, UrlParseError};

/// Parses a URI string into a URL.
pub fn parse_uri(uri: &str) -> Result<Url> {
    let mut url = match Url::parse(uri) {
        Ok(url) => url,
        Err(e) => Url::from_directory_path(uri).map_err(|_| UrlParseError(e))?,
    };

    // Collapse redundant slashes in the path (e.g. "a//b" -> "a/b"). Hadoop and
    // the JVM Hudi reader tolerate empty path segments, but the object_store
    // `Path` parser rejects them ("contained empty path segment"), so normalize
    // them away at ingestion rather than letting a "//" base path fail later.
    //
    // Object stores do not share Hadoop's tolerance: in S3 an empty segment is
    // part of the key, so `s3://bucket/a//b` and `s3://bucket/a/b` name
    // different objects. Collapsing therefore trades the loud `EmptySegment`
    // failure such a path used to hit for a read of the single-slash path. That
    // is accepted because this crate parses table base paths here (through
    // `HudiConfigValue::to_url`), where a doubled slash is almost always a
    // string-concatenation accident rather than a deliberate key.
    //
    // Collapse the runs of `/` directly in the ALREADY-percent-encoded path
    // string and write it back with `set_path`. The previous approach round-
    // tripped through `path_segments()` (which yields decoded-view segments) and
    // `path_segments_mut().extend()` (which percent-encodes its input), so an
    // already-encoded segment got encoded a second time — e.g. a space `%20`
    // became `%2520`, pointing the reader at a nonexistent path. `set_path`
    // leaves existing `%xx` escapes intact, and `/` never appears inside an
    // escape, so the pure-string collapse is encoding-safe.
    if url.path().contains("//") {
        let mut collapsed = String::with_capacity(url.path().len());
        let mut prev_slash = false;
        for ch in url.path().chars() {
            let is_slash = ch == '/';
            if !(is_slash && prev_slash) {
                collapsed.push(ch);
            }
            prev_slash = is_slash;
        }
        url.set_path(&collapsed);
    }

    if url.path().ends_with('/') {
        let err = InvalidPath(format!("Url {url:?} cannot be a base"));
        url.path_segments_mut().map_err(|_| err)?.pop();
    }

    Ok(url)
}

/// Returns the scheme and authority of a URL in the form of `scheme://authority`.
pub fn get_scheme_authority(url: &Url) -> String {
    format!("{}://{}", url.scheme(), url.authority())
}

/// Joins a base URL with a list of segments.
pub fn join_url_segments(base_url: &Url, segments: &[&str]) -> Result<Url> {
    let mut url = base_url.clone();

    if url.path().ends_with('/') {
        url.path_segments_mut().unwrap().pop();
    }

    for &seg in segments {
        let segs: Vec<_> = seg.split('/').filter(|&s| !s.is_empty()).collect();
        let err = InvalidPath(format!("Url {url:?} cannot be a base"));
        url.path_segments_mut().map_err(|_| err)?.extend(segs);
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parse_valid_uri_in_various_forms() {
        let urls = vec![
            parse_uri("/foo/").unwrap(),
            parse_uri("file:/foo/").unwrap(),
            parse_uri("file:///foo/").unwrap(),
            parse_uri("hdfs://foo/").unwrap(),
            parse_uri("s3://foo").unwrap(),
            parse_uri("s3://foo/").unwrap(),
            parse_uri("s3a://foo/bar/").unwrap(),
            parse_uri("gs://foo/").unwrap(),
            parse_uri("wasb://foo/bar").unwrap(),
            parse_uri("wasbs://foo/").unwrap(),
        ];
        let schemes = vec![
            "file", "file", "file", "hdfs", "s3", "s3", "s3a", "gs", "wasb", "wasbs",
        ];
        let paths = vec![
            "/foo", "/foo", "/foo", "/", "", "/", "/bar", "/", "/bar", "/",
        ];
        assert_eq!(urls.iter().map(|u| u.scheme()).collect::<Vec<_>>(), schemes);
        assert_eq!(urls.iter().map(|u| u.path()).collect::<Vec<_>>(), paths);
    }

    #[test]
    fn parse_uri_collapses_redundant_slashes() {
        // A base path with a doubled slash (common from naive string concat, and
        // tolerated by Hadoop/JVM Hudi) must normalize to a single slash so the
        // object_store Path parser doesn't reject the empty segment later.
        assert_eq!(
            parse_uri("/tmp/junit-123//mor-with-logs").unwrap().path(),
            "/tmp/junit-123/mor-with-logs"
        );
        assert_eq!(parse_uri("file:///a//b///c/").unwrap().path(), "/a/b/c");
        assert_eq!(parse_uri("s3://bucket/a//b").unwrap().path(), "/a/b");
        // Three-or-more consecutive slashes collapse to one.
        assert_eq!(parse_uri("s3://bucket/a////b").unwrap().path(), "/a/b");
        // The resulting path must be usable as an object_store Path.
        let url = parse_uri("/tmp/x//y").unwrap();
        object_store::path::Path::from_url_path(url.path())
            .expect("normalized path is a valid object_store Path");

        // A percent-encoded segment (e.g. a space) combined with a doubled slash
        // must NOT be double-encoded by the collapse: `%20` stays `%20`, not
        // `%2520`. (Regression: the old path_segments()/extend() round-trip
        // re-encoded the already-encoded segment.)
        let url = parse_uri("s3://bucket/my dir//tbl").unwrap();
        assert_eq!(url.path(), "/my%20dir/tbl");
        assert!(
            !url.path().contains("%2520"),
            "segment must not be double-encoded"
        );
    }

    #[test]
    fn join_base_url_with_segments() {
        let base_url = Url::from_str("file:///base").unwrap();

        assert_eq!(
            join_url_segments(&base_url, &["foo"]).unwrap(),
            Url::from_str("file:///base/foo").unwrap()
        );

        assert_eq!(
            join_url_segments(&base_url, &["/foo"]).unwrap(),
            Url::from_str("file:///base/foo").unwrap()
        );

        assert_eq!(
            join_url_segments(&base_url, &["/foo", "bar/", "/baz/"]).unwrap(),
            Url::from_str("file:///base/foo/bar/baz").unwrap()
        );

        assert_eq!(
            join_url_segments(&base_url, &["foo/", "", "bar/baz"]).unwrap(),
            Url::from_str("file:///base/foo/bar/baz").unwrap()
        );

        assert_eq!(
            join_url_segments(&base_url, &["foo1/bar1", "foo2/bar2"]).unwrap(),
            Url::from_str("file:///base/foo1/bar1/foo2/bar2").unwrap()
        );
    }

    #[test]
    fn join_failed_due_to_invalid_base() {
        let base_url = Url::from_str("foo:text/plain,bar").unwrap();
        let result = join_url_segments(&base_url, &["foo"]);
        assert!(result.is_err());
    }
}
