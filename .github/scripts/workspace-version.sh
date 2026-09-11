#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Prints THE authoritative project version: `[workspace.package] version` in the root
# Cargo.toml. Every other place that needs a version -- the JNI carrier workflow, the
# Makefile, cpp/CMakeLists.txt, the release workflow's tag check -- reads it from here
# instead of carrying its own copy, so a version bump is one edit in one file.
#
# Deliberately parses the file rather than shelling out to `cargo metadata`: this runs in
# places that have no cargo (a manylinux container before rustup, a CMake configure step)
# and it must never be the reason one of them fails.
#
# It is section-anchored, unlike the `grep version Cargo.toml | head -n 1` idiom it replaces:
# that one returns whichever line happens to mention "version" first, which is a different
# value the moment anything is inserted above [workspace.package].
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
manifest="${1:-$repo_root/Cargo.toml}"

version="$(
  awk '
    /^\[workspace\.package\]/ { inblk = 1; next }
    /^\[/                     { inblk = 0 }
    inblk && /^[[:space:]]*version[[:space:]]*=/ {
      if (match($0, /"[^"]+"/)) {
        print substr($0, RSTART + 1, RLENGTH - 2)
        exit
      }
    }
  ' "$manifest"
)"

if [ -z "$version" ]; then
  echo "workspace-version.sh: no [workspace.package] version in $manifest" >&2
  exit 1
fi

printf '%s\n' "$version"
