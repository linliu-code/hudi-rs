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

# Asserts that the project version has ONE authority -- `[workspace.package] version` in the
# root Cargo.toml -- and that nothing carries a private copy of it.
#
# Why a checker and not pure derivation: three of the four kinds of site CAN derive and now do
# (jni-native.yml, the Makefile, cpp/CMakeLists.txt). The fourth cannot: Cargo's manifest format
# has no interpolation, so an intra-workspace dependency's `version = "..."` requirement has to
# be a literal, and `cargo publish` needs it to be there. Rather than leave those literals to be
# remembered at the next bump, this asserts them against the authority and CI runs it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
exec python3 "$(pwd)/.github/scripts/check_version_single_source.py" "$@"
