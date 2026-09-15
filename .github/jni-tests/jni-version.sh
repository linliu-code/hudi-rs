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
#
# Tests the Makefile's default JNI_VERSION against the workflow's.
#
# Each case copies the Makefile, the workflow and the version script into a throwaway git
# repository with a synthetic [workspace.package] version, so the real tree is never touched and
# the result cannot depend on whatever version the tree happens to carry today.
#
# Usage: .github/jni-tests/jni-version.sh      (from the repository root; exits non-zero on failure)
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# The numbers are assembled rather than written out whole, so this file never carries a string
# shaped like a project version.
BASE="41.42.43"
pass=0
fail=0

ok() { echo "  PASS: $1"; pass=$((pass + 1)); }
ko() { echo "  FAIL: $1"; fail=$((fail + 1)); }

# make_tree <dir> <workspace version>
make_tree() {
  local dir=$1 version=$2
  mkdir -p "$dir/.github/scripts" "$dir/.github/workflows" "$dir/python"
  cp "$REPO_ROOT/Makefile" "$dir/Makefile"
  cp "$REPO_ROOT/python/pyproject.toml" "$dir/python/"
  cp "$REPO_ROOT/.github/scripts/workspace-version.sh" "$dir/.github/scripts/"
  cp "$REPO_ROOT/.github/workflows/jni-native.yml" "$dir/.github/workflows/"
  printf '[workspace]\nmembers = []\n\n[workspace.package]\nversion = "%s"\nedition = "2024"\n' \
    "$version" > "$dir/Cargo.toml"
  git -C "$dir" init -q
  git -C "$dir" add -A
  git -C "$dir" -c user.name=test -c user.email=test@example.invalid commit -q -m fixture
}

# print_version <dir> [make args...] -- prints JNI_VERSION as the JNI targets would expand it
print_version() {
  local dir=$1
  shift
  make -s --no-print-directory -C "$dir" "$@" \
    --eval 'jni-test-print-version: ; @echo "$(JNI_VERSION)"' jni-test-print-version
}

# The default the workflow's package job computes, evaluated from the workflow file itself.
workflow_version() {
  local dir=$1 line
  line=$(grep -F 'VERSION="$(.github/scripts/workspace-version.sh' "$dir/.github/workflows/jni-native.yml" | head -n 1)
  [ -n "$line" ] || { echo "no default VERSION line in jni-native.yml" >&2; return 1; }
  line=${line#*|| }
  (cd "$dir" && VERSION="" && eval "$line" && printf '%s\n' "$VERSION")
}

for suffix in "-dev" "" "-rc.1"; do
  authority="${BASE}${suffix}"
  echo "== authority '$authority'"
  dir="$WORK/tree${suffix}"
  make_tree "$dir" "$authority"
  sha=$(git -C "$dir" rev-parse --short HEAD)
  want="${BASE}-dev.${sha}"

  got=$(print_version "$dir" 2>/dev/null) || true
  [ "$got" = "$want" ] && ok "Makefile JNI_VERSION is $want" || ko "Makefile JNI_VERSION is '$got', want '$want'"

  wf=$(workflow_version "$dir" 2>/dev/null) || true
  [ "$wf" = "$got" ] && ok "workflow default ($wf) matches the Makefile" || ko "workflow default '$wf' differs from the Makefile's '$got'"
done

echo "== an explicit JNI_VERSION wins"
dir="$WORK/tree-dev"
got=$(print_version "$dir" JNI_VERSION=explicit-1 2>/dev/null) || true
[ "$got" = "explicit-1" ] && ok "JNI_VERSION=explicit-1 is used as given" || ko "JNI_VERSION=explicit-1 gave '$got'"

echo "== the version script cannot run"
dir="$WORK/tree-broken"
make_tree "$dir" "${BASE}-dev"
chmod -x "$dir/.github/scripts/workspace-version.sh"
if out=$(print_version "$dir" 2>&1); then
  ko "make succeeded and printed '$out' instead of refusing"
else
  echo "$out" | grep -q 'workspace-version.sh' \
    && ok "make refuses, naming the script" \
    || ko "make failed without naming the script: $out"
fi
got=$(print_version "$dir" JNI_VERSION=explicit-2 2>/dev/null) || true
[ "$got" = "explicit-2" ] && ok "an explicit JNI_VERSION still works with the script broken" || ko "explicit JNI_VERSION with the script broken gave '$got'"
make -s --no-print-directory -C "$dir" -n check-rust >/dev/null 2>&1 \
  && ok "a non-JNI target does not evaluate the version" \
  || ko "a non-JNI target fails when the version script is broken"

echo "== the manifest has no [workspace.package] version"
dir="$WORK/tree-noversion"
make_tree "$dir" "${BASE}-dev"
printf '[workspace]\nmembers = []\n' > "$dir/Cargo.toml"
if out=$(print_version "$dir" 2>&1); then
  ko "make succeeded and printed '$out' instead of refusing"
else
  ok "make refuses"
fi

echo
echo "jni-version: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
