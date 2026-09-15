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
# have no effect on an explicit `-l` reference — measured empirically: `cc`/`gcc` (not `g++`)
# is the linker driver rustc invokes, and even a `g++`-driven link with an explicit -lstdc++
# still produces a dynamic libstdc++.so.6 NEEDED entry alongside -static-libstdc++.
#
# This wrapper rewrites `-lstdc++`/`-lgcc_s` IN PLACE (same position in the argument list, so
# ordering relative to the .rlib archives that reference them — which `--as-needed` linking
# depends on — is preserved) to force the static archives.
#
# The FIRST version of this wrapper mapped `-lgcc_s` -> `libgcc.a` alone. `libgcc.a` does NOT carry the unwinder
# (`_Unwind_*`) — that lives in `libgcc_eh.a` — so the resulting .so had 19 undefined
# `_Unwind_*`/`__cxa_*` symbols and no `libgcc_s.so.1` NEEDED to resolve them dynamically
# either. Nothing failed the LINK itself (a shared object links fine with undefined symbols by
# default), so this passed the "Portability floor" step (which only checked glibc-symbol
# versions and NEEDED, not unresolved symbols) and even passed a local Java smoke test — but
# only because this box's own JVM happens to have `libgcc_s.so.1` already loaded in the
# process's GLOBAL symbol scope, silently satisfying the dlopen at runtime. The runner's
# Temurin JVM does not, and the same load throws `UnsatisfiedLinkError: undefined symbol:
# _Unwind_GetTextRelBase`. THIS IS A TRAP: a native library loading successfully in one process
# proves only that whatever happened to already be loaded in THAT process's global scope
# covered its gaps — never trust it as proof of a clean, self-contained link.
#
# `-Wl,-z,defs` (below) is added as defense in depth and DOES catch a plain, unversioned
# undefined symbol at build time (measured: an ordinary undefined C function reference fails
# the link immediately with `-z,defs`). It does NOT, however, catch THIS bug's actual symbol
# class: `_Unwind_*`/`__cxa_*` references from libstdc++.a/libgcc_eh.a carry an explicit ELF
# symbol-version requirement (e.g. `_Unwind_Resume@GCC_3.0`), and GNU ld's `-z,defs` does not
# treat an unresolved VERSIONED reference as a hard "undefined symbol" error the way it does an
# unversioned one — measured directly: relinking a throwing C++ object against libstdc++.a
# without libgcc_eh.a, with `-Wl,-z,defs` present, still LINKS (exit 0) despite leaving
# `_Unwind_*@GCC_3.0`-style symbols undefined. So the load-bearing check for this specific
# class is NOT `-z,defs` — it is the `nm -D --undefined-only` assertion, checked by both the
# workflow's Portability floor step and `make jni-lib-portable`, independently confirmed in a
# process with NO pre-loaded libgcc_s (a bare `env -i` `java` load, as crates/jni/README.md describes).
#
# Fix: `-lgcc_s` now maps to the ABSOLUTE paths of BOTH `libgcc_eh.a` and `libgcc.a` (in that
# order — `libgcc_eh.a` first, since it provides the unwinder that libstdc++'s personality
# routines call into, and it must appear AFTER libstdc++.a's own substitution on the command
# line, which is naturally true here since -lgcc_s already sits after -lstdc++ in rustc's
# default link line and this wrapper substitutes every token in place without reordering).
# Resolved via `$REAL -print-file-name=<archive>`, which returns the bare filename (not an
# absolute path) when the archive can't be found — checked explicitly; this wrapper refuses to
# silently continue with a linker error, since a missing archive would otherwise just as
# silently omit the unwinder again as the original bug did.
#
# It only touches the libhudi_jni cdylib's own final link (matched by "libhudi_jni" appearing
# among the arguments); every other rustc-invoked link (build scripts, proc-macros, host helper
# binaries) passes through untouched, since an earlier unconditional version of this wrapper
# broke a `zerocopy` build-script link (undefined reference to _Unwind_Resume) by interfering
# with gcc's implicit unwind-library linking for binaries that carry no explicit -lgcc_s token.
# That scoped link ALSO gets `-Wl,-z,defs` (catches a plain unversioned undefined symbol
# introduced by a future change here; it will NOT by itself catch a regression of THIS bug's
# versioned-symbol class — see above — which is why the nm-based check stays load-bearing).
#
# Usage: RUSTFLAGS="-C linker=/path/to/static-cxx-linker.sh"; REAL_CC=$(command -v gcc) (or cc)
set -euo pipefail
REAL="${REAL_CC:-cc}"

static_archive() {
  # $1: archive name (e.g. libgcc_eh.a). Prints its absolute path or fails loudly.
  local path
  path=$("$REAL" -print-file-name="$1")
  case "$path" in
    /*) [ -f "$path" ] || { echo "static-cxx-linker: $1 resolved to '$path' but it does not exist" >&2; exit 1; } ;;
    *) echo "static-cxx-linker: $REAL cannot locate $1 (got '$path'); install the matching gcc-toolset libstdc++/libgcc static archives" >&2; exit 1 ;;
  esac
  printf '%s\n' "$path"
}

case " $* " in
  *" "*"libhudi_jni"*" "*)
    args=()
    for a in "$@"; do
      case "$a" in
        -lstdc++) args+=(-Wl,-Bstatic -lstdc++ -Wl,-Bdynamic) ;;
        -lgcc_s)
          eh=$(static_archive libgcc_eh.a)
          ga=$(static_archive libgcc.a)
          args+=("$eh" "$ga")
          ;;
        *) args+=("$a") ;;
      esac
    done
    args+=(-Wl,-z,defs)
    exec "$REAL" "${args[@]}"
    ;;
  *)
    exec "$REAL" "$@"
    ;;
esac
