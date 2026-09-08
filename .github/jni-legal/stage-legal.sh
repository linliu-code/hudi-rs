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
# F-8: write the carrier jar's legal files into <stage>/META-INF.
#
#   META-INF/LICENSE         this repository's Apache-2.0 licence text
#   META-INF/NOTICE          the project notice + the statically linked GCC runtime paragraph
#   META-INF/THIRD-PARTY.txt every crate in `hudi-jni`'s normal-dependency closure, with licence
#
# ONE implementation, called from BOTH `make jni-jar-multi` and the `package` job in
# `.github/workflows/jni-native.yml`, so a locally assembled carrier and a CI-built one carry
# byte-identical legal files.
#
# Usage: stage-legal.sh <stage-dir>          (run from the repository root)
set -euo pipefail

STAGE=${1:?usage: stage-legal.sh <stage-dir>}
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mkdir -p "$STAGE/META-INF"

cp "$REPO_ROOT/LICENSE" "$STAGE/META-INF/LICENSE"

# --- THIRD-PARTY.txt ------------------------------------------------------------------------
# `cargo license` is NOT used here: it has no per-package selector, so in this workspace it
# reports the union of every member's dependencies (pyo3, datafusion, the Python bindings) —
# crates that are not in libhudi_jni.so.  `cargo tree -p hudi-jni -e normal` is the accurate
# closure; `cargo metadata` supplies each crate's declared SPDX licence.  Both ship with cargo,
# so no extra tool has to be installed locally or in CI.
command -v cargo >/dev/null || { echo "FAIL: cargo is required to generate META-INF/THIRD-PARTY.txt"; exit 1; }

CLOSURE=$(cd "$REPO_ROOT" && cargo tree -p hudi-jni -e normal --no-dedupe --prefix none --format '{p}' \
  --target x86_64-unknown-linux-gnu --target aarch64-unknown-linux-gnu \
  | sed 's/ (\*)$//; s/ (proc-macro)$//; s| (/.*)$||' | sort -u)
[ -n "$CLOSURE" ] || { echo "FAIL: cargo tree returned an empty dependency closure for hudi-jni"; exit 1; }

METADATA=$(cd "$REPO_ROOT" && cargo metadata --format-version 1)

printf '%s' "$METADATA" | CLOSURE="$CLOSURE" python3 -c '
import json, os, sys
meta = json.load(sys.stdin)
lic = {}
for p in meta["packages"]:
    lic[(p["name"], p["version"])] = (p.get("license") or "").strip() or None
rows, missing = [], []
for line in os.environ["CLOSURE"].splitlines():
    line = line.strip()
    if not line:
        continue
    name, _, ver = line.rpartition(" v")
    key = (name, ver)
    if key not in lic:
        sys.exit("FAIL: %s %s is in the tree but not in cargo metadata" % (name, ver))
    l = lic[key]
    if l is None:
        missing.append("%s %s" % (name, ver))
        l = "see the crates .io page / repository (no SPDX license field declared)"
    rows.append((name, ver, l))
rows.sort()
out = []
out.append("hudi-jni-native carrier — third-party notices")
out.append("")
out.append("libhudi_jni.so is a statically linked Rust cdylib.  Every crate below is part of")
out.append("`hudi-jni`s normal-dependency closure (cargo tree -p hudi-jni -e normal, resolved for")
out.append("both x86_64-unknown-linux-gnu and aarch64-unknown-linux-gnu) and may therefore have code")
out.append("linked into the shipped library.  Licence strings are the SPDX expressions the crates")
out.append("declare in their own Cargo.toml (cargo metadata); an \"OR\" expression is the crate")
out.append("authors offer of a choice, not a determination made here.  The full licence text of each")
out.append("crate is in its published source package.")
out.append("")
out.append("Build- and dev-dependencies are excluded (cargo tree -e normal).  A few crates listed")
out.append("here are proc-macros that execute only at compile time and contribute no machine code to")
out.append("the library; they are kept in the list rather than filtered, so this file is a superset")
out.append("of what is linked and never a subset.")
out.append("")
out.append("%d crates:" % len(rows))
out.append("")
width = max(len("%s %s" % (n, v)) for n, v in [(r[0], r[1]) for r in rows])
for n, v, l in rows:
    out.append("%-*s  %s" % (width, "%s %s" % (n, v), l))
if missing:
    out.append("")
    out.append("Crates with no SPDX license field declared in Cargo.toml (%d): %s" % (len(missing), ", ".join(missing)))
sys.stdout.write("\n".join(out) + "\n")
' > "$STAGE/META-INF/THIRD-PARTY.txt"

CRATE_COUNT=$(grep -c ' crates:$' "$STAGE/META-INF/THIRD-PARTY.txt" >/dev/null && sed -n 's/^\([0-9]*\) crates:$/\1/p' "$STAGE/META-INF/THIRD-PARTY.txt")

# --- NOTICE ---------------------------------------------------------------------------------
{
  cat "$REPO_ROOT/NOTICE"
  cat <<'NOTICE_TAIL'

--------------------------------------------------------------------------------
Statically linked runtime libraries in native/linux-<arch>/libhudi_jni.so
--------------------------------------------------------------------------------

This artifact ships prebuilt shared libraries (native/linux-x86_64/libhudi_jni.so and
native/linux-aarch64/libhudi_jni.so).  They are linked with -static-libstdc++/-static-libgcc
and an explicit libgcc_eh.a, so each library contains machine code from the GCC runtime
libraries:

  * libstdc++  (the GNU Standard C++ Library)
  * libgcc     (the GCC low-level runtime library)
  * libgcc_eh  (the GCC exception-unwinding runtime)

Those libraries are part of GCC and are distributed under the GNU General Public License
version 3 WITH the GCC Runtime Library Exception, version 3.1, which permits distributing a
program that links them without the program itself becoming subject to the GPL.  See
<https://www.gnu.org/licenses/gcc-exception-3.1.html> for the exception text and
<https://gcc.gnu.org/onlinedocs/libstdc++/manual/license.html> for libstdc++'s licensing.
The libraries are built in the manylinux_2_28 images (quay.io/pypa/manylinux_2_28_<arch>),
whose GCC sources are available from the AlmaLinux 8 / Red Hat Developer Toolset upstreams.

The libraries additionally contain compiled Rust crates.  Every crate in the linked closure,
with the SPDX licence expression its authors declare, is listed in META-INF/THIRD-PARTY.txt in
this artifact.  Notable statically linked C/C++ sources reaching the binary through those
crates include RocksDB (librocksdb-sys), zstd, lz4, bzip2, zlib and AWS-LC; their licences are
the ones recorded for the corresponding crates in that file.
NOTICE_TAIL
} > "$STAGE/META-INF/NOTICE"

echo "staged legal files into $STAGE/META-INF:"
ls -l "$STAGE/META-INF/LICENSE" "$STAGE/META-INF/NOTICE" "$STAGE/META-INF/THIRD-PARTY.txt"
echo "THIRD-PARTY.txt covers ${CRATE_COUNT:-?} crates"
