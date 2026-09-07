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

# D-27 portable-build linker wrapper (manylinux_2_28 container builds only).
#
# librocksdb-sys (a C++ dependency of hudi-core) emits an EXPLICIT dynamic `-lstdc++`/
# `-lgcc_s` on the final libhudi_jni.so link line. RUSTFLAGS' `-C link-arg=-static-libstdc++
# -C link-arg=-static-libgcc` do NOT remove these: those two flags only rewrite gcc's OWN
# AUTOMATIC C++-runtime linking (what g++ adds implicitly when it decides a link is C++), and
# have no effect on an explicit `-l` reference — measured empirically in this environment:
# `cc`/`gcc` (not `g++`) is the linker driver rustc invokes, and even a `g++`-driven link with
# an explicit -lstdc++ still produces a dynamic libstdc++.so.6 NEEDED entry alongside
# -static-libstdc++. See investigations/m3-carrier-glibc-floor/ (D-27 fix round) for the
# measurement.
#
# This wrapper rewrites `-lstdc++`/`-lgcc_s` IN PLACE (same position in the argument list, so
# ordering relative to the .rlib archives that reference them — which `--as-needed` linking
# depends on — is preserved) to force the static archive via `-Wl,-Bstatic ... -Wl,-Bdynamic`
# bracketing. It only touches the libhudi_jni cdylib's own final link (matched by
# "libhudi_jni" appearing among the arguments); every other rustc-invoked link (build scripts,
# proc-macros, host helper binaries) passes through untouched, since those may rely on gcc's
# normal implicit libgcc_s linking for exception unwinding and typically carry no explicit
# -lstdc++/-lgcc_s token for us to rewrite.
#
# Usage: RUSTFLAGS="-C linker=/path/to/static-cxx-linker.sh"; REAL_CC=$(command -v gcc) (or cc)
set -euo pipefail
REAL="${REAL_CC:-cc}"
case " $* " in
  *" "*"libhudi_jni"*" "*)
    args=()
    for a in "$@"; do
      case "$a" in
        -lstdc++) args+=(-Wl,-Bstatic -lstdc++ -Wl,-Bdynamic) ;;
        -lgcc_s)  args+=(-Wl,-Bstatic -lgcc -Wl,-Bdynamic) ;;
        *) args+=("$a") ;;
      esac
    done
    exec "$REAL" "${args[@]}"
    ;;
  *)
    exec "$REAL" "$@"
    ;;
esac
