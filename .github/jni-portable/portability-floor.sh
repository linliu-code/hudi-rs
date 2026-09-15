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
# D-27 portability floor for the carrier's libhudi_jni.so.
#
# ONE implementation, called from BOTH `.github/workflows/jni-native.yml` and
# `make jni-lib-portable`, so a local portable build and a CI leg assert exactly the same
# thing.  Run it by hand against any candidate library:
#
#     .github/jni-portable/portability-floor.sh target/jni-portable/stage/native/linux-aarch64/libhudi_jni.so
#
# Every check is fail-closed: an empty/failed measurement aborts rather than passing.
#
# Env overrides (for RED-testing this script itself, not for production use):
#   JNI_GLIBC_CEILING   default 2.28
#   JNI_ALLOWED_NEEDED  default: the six sonames a self-contained library may keep
#   JNI_EXPECT_JAVA_SYMS default 2
set -euo pipefail

SO=${1:?usage: portability-floor.sh <libhudi_jni.so>}
[ -f "$SO" ] || { echo "FAIL: no such library: $SO"; exit 1; }

GLIBC_CEILING=${JNI_GLIBC_CEILING:-2.28}
# F-5: an ALLOW-list, not a deny-list.  A deny-list of {libstdc++, libgcc_s} accepted any NEW
# dynamic dependency a future Rust crate might drag in (libssl.so.3, libzstd.so.1, ...): those
# carry no GLIBC_-versioned symbols, so the glibc-floor check cannot see them, and both smokes
# run on hosts that may happen to provide them.  The shipped artifact's NEEDED set is minimal
# today (measured on a published carrier's two libraries), so freeze it.
JNI_ALLOWED_NEEDED=${JNI_ALLOWED_NEEDED:-"libc.so.6 libm.so.6 libdl.so.2 libpthread.so.0 librt.so.1 ld-linux-*.so.*"}
EXPECT_JAVA_SYMS=${JNI_EXPECT_JAVA_SYMS:-2}

echo "--- portability floor: $SO"
ls -l "$SO"

# 1. glibc symbol-version floor -------------------------------------------------------------
MAXGLIBC=$(objdump -T "$SO" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -V | tail -1)
[ -n "$MAXGLIBC" ] || { echo "FAIL: could not read any GLIBC_ symbol version from $SO"; exit 1; }
echo "max GLIBC_$MAXGLIBC (ceiling $GLIBC_CEILING)"
TOP=$(printf '%s\n%s\n' "$MAXGLIBC" "$GLIBC_CEILING" | sort -V | tail -1)
[ "$TOP" = "$GLIBC_CEILING" ] || { echo "FAIL: glibc floor $MAXGLIBC exceeds the $GLIBC_CEILING ceiling"; exit 1; }

# 2. no versioned C++ runtime imports -------------------------------------------------------
CXXCOUNT=$(objdump -T "$SO" | grep -cE 'GLIBCXX_|CXXABI_' || true)
echo "GLIBCXX_/CXXABI_ versioned imports: $CXXCOUNT"
[ "$CXXCOUNT" -eq 0 ] || { echo "FAIL: versioned GLIBCXX_/CXXABI_ imports present"; exit 1; }

# 3. NEEDED allow-list ----------------------------------------------------------------------
# F-13: no `|| true` here.  A `.so` always has NEEDED entries, so an empty result means readelf
# failed or the file is not what we think it is — that must fail, not silently satisfy the
# assertions below.
NEEDED=$(readelf -d "$SO" | sed -n 's/.*(NEEDED).*\[\(.*\)\]/\1/p')
[ -n "$NEEDED" ] || { echo "FAIL: no NEEDED entries read from $SO (readelf failed, or not an ELF shared object)"; exit 1; }
echo "NEEDED: $(echo "$NEEDED" | tr '\n' ' ')"
echo "allowed: $JNI_ALLOWED_NEEDED"
OFFENDERS=""
while IFS= read -r soname; do
  [ -n "$soname" ] || continue
  ok=0
  for pattern in $JNI_ALLOWED_NEEDED; do
    # shellcheck disable=SC2254  # $pattern is a glob on purpose (ld-linux-*.so.*)
    case "$soname" in $pattern) ok=1; break;; esac
  done
  [ "$ok" -eq 1 ] || OFFENDERS="${OFFENDERS:+$OFFENDERS }$soname"
done <<EOF
$NEEDED
EOF
[ -z "$OFFENDERS" ] || {
  echo "FAIL: NEEDED soname(s) outside the allow-list: $OFFENDERS"
  echo "      allow-list: $JNI_ALLOWED_NEEDED"
  echo "      A new dynamic dependency must either be linked statically or be added to the"
  echo "      allow-list with a decision recording why every deployment host will have it."
  exit 1
}

# 4. no undefined unwinder / C++ ABI symbols ------------------------------------------------
# A NEEDED list and a glibc-symbol-version check alone missed 19 undefined _Unwind_*/__cxa_* symbols (libgcc.a
# alone lacks the unwinder, which lives in libgcc_eh.a); the library loaded in this box's own
# smoke only because the host JVM already had libgcc_s in its global scope — not proof of a
# clean link.  Assert directly that no such symbol is left undefined.
# Excludes: symbols nm marks weak ("w", e.g. __cxa_pure_virtual — libstdc++'s own convention is
# to leave this optional) and symbols with an "@GLIBC_x.y" version tag (e.g.
# __cxa_atexit@GLIBC_2.17 — legitimately provided by libc.so.6 itself, and present in ANY C++
# binary including a known-good -static-libstdc++ -static-libgcc reference build measured here;
# an unfiltered grep would never reach 0 and would permanently fail this gate).
echo "all _Unwind_/__cxa_/__gxx_ hits (for the record):"
nm -D --undefined-only "$SO" | grep -E '_Unwind_|__cxa_|__gxx_' || true
UNDEF=$(nm -D --undefined-only "$SO" | awk '$1=="U" && $2 ~ /_Unwind_|__cxa_|__gxx_/ && $2 !~ /@GLIB/' | grep -c . || true)
echo "undefined unwinder/C++ symbols (excluding weak and glibc-versioned): $UNDEF"
[ "$UNDEF" -eq 0 ] || { echo "FAIL: $UNDEF unresolved unwinder/C++ symbols"; exit 1; }

# 5. the JNI entry points survived the strip -------------------------------------------------
# D-29 (F-7): the staged library is `strip --strip-unneeded`ed.  `.dynsym` is what JNI binds
# against and a strip must never touch it — assert that here, on the stripped artifact, so a
# future strip-flag change cannot quietly produce an unloadable library.
JAVASYMS=$(nm -D --defined-only "$SO" | grep -c ' T Java_' || true)
nm -D --defined-only "$SO" | grep ' T Java_' || true
echo "exported Java_ symbols: $JAVASYMS (expected $EXPECT_JAVA_SYMS)"
[ "$JAVASYMS" -eq "$EXPECT_JAVA_SYMS" ] || { echo "FAIL: expected $EXPECT_JAVA_SYMS exported Java_ symbols, found $JAVASYMS"; exit 1; }

# 6. the library is stripped -----------------------------------------------------------------
# The carrier's properties record `stripped=true`. A library that still has a .symtab was not
# stripped, whichever job staged it, so the claim is checked on the bytes rather than assumed.
if readelf -S "$SO" | grep -q ' \.symtab'; then
  echo "FAIL: $SO is not stripped (it still has a .symtab section)"
  exit 1
fi
echo "stripped: no .symtab section"

echo "PASS: glibc floor $MAXGLIBC <= $GLIBC_CEILING; no versioned C++ runtime imports; NEEDED within the allow-list; no undefined unwinder/C++ symbols; $JAVASYMS Java_ symbols exported; stripped"
