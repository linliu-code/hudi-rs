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
"""Enforces one authority for the project version. Invoked by check-version-single-source.sh.

With --fix, rewrites the intra-workspace dependency requirements to the authority instead of
reporting them, which is what `make version-sync` runs after a bump. The other two rules are
never auto-fixed: a stray hardcode and a lost derivation both need a human to decide what the
site should read instead.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Files that are allowed to contain a literal of the project's own version. Everything else
# must derive it. Cargo.lock is generated and cargo keeps it in step by itself.
LITERAL_ALLOWED_SUFFIXES = ("Cargo.toml", "Cargo.lock")

# Prose is exempt: crates/jni/README.md records the versions of carriers that were actually
# published, which are history and must NOT be rewritten by a bump.
SWEEP_SKIP_SUFFIXES = (".md",)

# Sites that used to carry a hardcoded copy and must now visibly derive one. Rule 2 catches a
# re-hardcode; this catches the other way a derivation can be lost -- being deleted outright.
MUST_DERIVE = {
    ".github/workflows/jni-native.yml": "workspace-version.sh",
    ".github/workflows/release.yml": "workspace-version.sh",
    "Makefile": "workspace-version.sh",
    "cpp/CMakeLists.txt": "[workspace.package]",
}

DEV_SHAPED = re.compile(r"\b\d+\.\d+\.\d+-dev\b")
SECTION = re.compile(r"^\[([^\]]+)\]\s*$")
ENTRY = re.compile(r"^([A-Za-z0-9_.-]+)\s*=\s*(.*)$")
KEY_PATH = re.compile(r'(?<![A-Za-z0-9_-])path\s*=\s*"([^"]*)"')
KEY_VERSION = re.compile(r'(?<![A-Za-z0-9_-])version\s*=\s*"([^"]*)"')


def dep_entries(text: str):
    """Yields (section, name, inline_table_text) for every dependency written as a table.

    Hand-rolled rather than via `tomllib`, which only exists on Python 3.11+ -- this has to run
    on whatever python3 a contributor's machine and the CI runner happen to have. It only needs
    to recognise `name = { ... }` inside a `*dependencies` section, and it accumulates until the
    braces balance so an entry split across lines is checked like any other.
    """
    section = ""
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        m = SECTION.match(line.strip())
        if m:
            section = m.group(1)
            i += 1
            continue
        if not section.split(".")[-1].endswith("dependencies"):
            i += 1
            continue
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            i += 1
            continue
        m = ENTRY.match(stripped)
        if not m or not m.group(2).lstrip().startswith("{"):
            i += 1
            continue
        name, buf = m.group(1), m.group(2)
        depth = buf.count("{") - buf.count("}")
        while depth > 0 and i + 1 < len(lines):
            i += 1
            buf += "\n" + lines[i]
            depth += lines[i].count("{") - lines[i].count("}")
        yield section, name, buf
        i += 1


def authority() -> str:
    out = subprocess.run(
        [str(ROOT / ".github/scripts/workspace-version.sh")],
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip()


def tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True,
    )
    return [line for line in out.stdout.splitlines() if line]


def main() -> int:
    fix = "--fix" in sys.argv[1:]
    want = authority()
    failures: list[str] = []
    fixed: list[str] = []
    checked_deps = 0

    print(f"authority: [workspace.package] version = {want}  (Cargo.toml)")

    # ---- Rule 1: every intra-workspace path dependency requests exactly the authority. ----
    # Read structurally with a TOML parser rather than by line, so a dependency written across
    # several lines is checked like any other instead of being silently skipped.
    for rel in tracked_files():
        if not rel.endswith("Cargo.toml"):
            continue
        for section, name, spec in dep_entries((ROOT / rel).read_text()):
            if not KEY_PATH.search(spec):
                continue  # not an intra-workspace dependency
            got = KEY_VERSION.search(spec)
            if got is None:
                continue  # path-only: cargo resolves it by path, there is no literal to drift
            checked_deps += 1
            if got.group(1) == want:
                continue
            if fix:
                path = ROOT / rel
                before = path.read_text()
                after = before.replace(spec, spec[:got.start(1)] + want + spec[got.end(1):], 1)
                if after == before:
                    failures.append(f"{rel}: [{section}] {name} could not be rewritten")
                else:
                    path.write_text(after)
                    fixed.append(f"{rel}: [{section}] {name} {got.group(1)} -> {want}")
            else:
                failures.append(
                    f"{rel}: [{section}] {name} requests version \"{got.group(1)}\" "
                    f"but the authority is \"{want}\""
                )

    # ---- Rule 2: nothing else carries a copy of a project version. ----
    for rel in tracked_files():
        if rel.endswith(LITERAL_ALLOWED_SUFFIXES) or rel.endswith(SWEEP_SKIP_SUFFIXES):
            continue
        path = ROOT / rel
        if not path.is_file():
            continue
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue  # binary or unreadable: cannot contain a version literal we care about
        for lineno, line in enumerate(text.splitlines(), 1):
            for hit in DEV_SHAPED.findall(line):
                failures.append(
                    f"{rel} line {lineno}: hardcoded project version \"{hit}\" -- derive it from "
                    f".github/scripts/workspace-version.sh instead"
                )

    # ---- Rule 3: the sites that must derive still visibly do. ----
    for rel, marker in MUST_DERIVE.items():
        path = ROOT / rel
        if not path.is_file():
            failures.append(f"{rel}: expected to derive the version, but the file is missing")
            continue
        if marker not in path.read_text():
            failures.append(
                f"{rel}: no longer references {marker!r}, so it may have stopped deriving the version"
            )

    # Absence is not success: say what was examined, so an empty result set cannot be mistaken
    # for a clean one.
    print(f"checked: {checked_deps} intra-workspace dependency requirement(s), "
          f"{len(tracked_files())} tracked file(s) swept, {len(MUST_DERIVE)} derivation site(s)")

    if checked_deps == 0:
        failures.append(
            "found NO intra-workspace dependency requirements at all -- rule 1 asserted nothing, "
            "which means this checker is broken rather than the tree being clean"
        )

    for line in fixed:
        print(f"rewrote {line}")

    if failures:
        print("\nFAIL: the project version is not single-sourced:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        print("\nTo bump the version: edit [workspace.package] version in Cargo.toml, then run "
              "`make version-sync`.", file=sys.stderr)
        return 1

    print("OK: one authority, and every other site derives from it or matches it."
          + (f" ({len(fixed)} rewritten)" if fixed else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
