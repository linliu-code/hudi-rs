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
# Tests `make jni-jar-multi`'s packaging body against stand-in libraries.
#
# The libraries are tiny shared objects compiled here with two `Java_` exports, so the packaging
# rules can be exercised in seconds without building hudi-jni. Nothing is loaded or run; only the
# checks the packaging makes on the files are under test. Everything is written under a temporary
# directory: the stage, the extra native dir and the jar.
#
# Needs: make, gcc, strip, objdump, readelf, nm, md5sum, cargo (for stage-legal.sh) and a JDK's
# `jar` on PATH.
#
# Usage: .github/jni-tests/jni-package-multi.sh      (from the repository root; exits non-zero on failure)
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass=0
fail=0
ok() { echo "  PASS: $1"; pass=$((pass + 1)); }
ko() { echo "  FAIL: $1"; fail=$((fail + 1)); }

cat > "$WORK/two.c" <<'C'
#include <string.h>
size_t Java_org_example_Stub_one(const char *s) { return strlen(s); }
size_t Java_org_example_Stub_two(const char *s) { return strlen(s) + 1; }
C
cat > "$WORK/one.c" <<'C'
#include <string.h>
size_t Java_org_example_Stub_one(const char *s) { return strlen(s); }
C
gcc -shared -fPIC -O1 -o "$WORK/two-unstripped.so" "$WORK/two.c"
gcc -shared -fPIC -O1 -o "$WORK/one-unstripped.so" "$WORK/one.c"
strip --strip-unneeded -o "$WORK/two.so" "$WORK/two-unstripped.so"
strip --strip-unneeded -o "$WORK/one.so" "$WORK/one-unstripped.so"

# place <root> <arch> <library>
place() {
  mkdir -p "$1/native/linux-$2"
  cp "$3" "$1/native/linux-$2/libhudi_jni.so"
}

# package <case dir> -- runs the packaging against <case>/out/stage and <case>/extra; output in <case>/make.log
package() {
  local c=$1
  mkdir -p "$c/out/stage/native" "$c/out/stage/META-INF" "$c/extra/native"
  make -s --no-print-directory -C "$REPO_ROOT" jni-jar-multi JNI_JAR_MULTI_PREREQ= \
    JNI_VERSION=test JNI_OUT="$c/out" JNI_STAGE="$c/out/stage" JNI_EXTRA_NATIVE_DIR="$c/extra" \
    > "$c/make.log" 2>&1
}

props() { cat "$1/out/stage/META-INF/hudi-jni-native.properties" 2>/dev/null || true; }

# refused <case dir> <what the refusal must name> <description>
refused() {
  local c=$1 needle=$2 what=$3
  mkdir -p "$c/out/stage/native"
  (cd "$c" && find out/stage/native -type f -exec md5sum {} + | sort > stage-before.md5)
  if package "$c"; then
    ko "$what: packaged anyway"
  elif ! grep -q -- "$needle" "$c/make.log"; then
    ko "$what: failed without naming '$needle': $(tail -n 3 "$c/make.log" | tr '\n' ' ')"
  elif [ -e "$c/out/stage/META-INF/hudi-jni-native.properties" ]; then
    ko "$what: refused, but only after writing a partial properties file"
  elif [ -e "$c/out/hudi-jni-native-test.jar" ]; then
    ko "$what: refused, but a jar was written"
  elif ! (cd "$c" && find out/stage/native -type f -exec md5sum {} + | sort | cmp -s - stage-before.md5); then
    ko "$what: refused, but the staged libraries were changed first"
  else
    ok "$what: refused, naming '$needle', before writing anything"
  fi
}

echo "== both arches"
c="$WORK/both"
place "$c/out/stage" x86_64 "$WORK/two.so"
place "$c/extra" aarch64 "$WORK/two.so"
if package "$c"; then
  ok "packaged"
  for key in 'arch=linux-x86_64,linux-aarch64' 'md5.linux-x86_64=' 'md5.linux-aarch64=' \
             'glibc.floor.linux-x86_64=2\.' 'glibc.floor.linux-aarch64=2\.' 'stripped=true'; do
    props "$c" | grep -q "^$key" && ok "properties carry $key" || ko "properties lack $key"
  done
  listing=$(unzip -l "$c/out/hudi-jni-native-test.jar")
  for entry in native/linux-x86_64/libhudi_jni.so native/linux-aarch64/libhudi_jni.so \
               META-INF/hudi-jni-native.properties META-INF/LICENSE META-INF/NOTICE META-INF/THIRD-PARTY.txt; do
    echo "$listing" | grep -q " $entry\$" && ok "jar carries $entry" || ko "jar lacks $entry"
  done
else
  ko "both arches did not package: $(tail -n 5 "$c/make.log" | tr '\n' ' ')"
fi

echo "== only x86_64 staged"
c="$WORK/x86-only"
place "$c/out/stage" x86_64 "$WORK/two.so"
refused "$c" "linux-aarch64" "x86_64 without aarch64"

echo "== only aarch64 staged"
c="$WORK/aarch64-only"
place "$c/extra" aarch64 "$WORK/two.so"
refused "$c" "linux-x86_64" "aarch64 without x86_64"

echo "== an unstripped library"
c="$WORK/unstripped"
place "$c/out/stage" x86_64 "$WORK/two.so"
place "$c/extra" aarch64 "$WORK/two-unstripped.so"
refused "$c" "not stripped" "an unstripped aarch64 library"

echo "== an unstripped extra library must not overwrite the staged one"
c="$WORK/overwrite"
place "$c/out/stage" x86_64 "$WORK/two.so"
place "$c/extra" x86_64 "$WORK/two-unstripped.so"
place "$c/extra" aarch64 "$WORK/two.so"
refused "$c" "not stripped" "an unstripped x86_64 library in JNI_EXTRA_NATIVE_DIR"

echo "== a library missing a Java_ export"
c="$WORK/one-export"
place "$c/out/stage" x86_64 "$WORK/one.so"
place "$c/extra" aarch64 "$WORK/two.so"
refused "$c" "Java_" "an x86_64 library with one Java_ export"

echo
echo "jni-package-multi: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
