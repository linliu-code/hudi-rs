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
# The marker is the INVOCATION, not the bare script name: `release.yml` and `cpp/CMakeLists.txt`
# both MENTION their source in prose, so a rule anchored on the name would still pass if the
# derivation itself were deleted and only the comment left behind.
MUST_DERIVE = {
    ".github/workflows/jni-native.yml": "$(.github/scripts/workspace-version.sh",
    ".github/workflows/release.yml": "$(.github/scripts/workspace-version.sh",
    "Makefile": "$(shell .github/scripts/workspace-version.sh",
    "cpp/CMakeLists.txt": "file(READ",
}

# Rule 2 sweeps for TWO patterns, because either alone has a hole:
#   * the `-dev` shape catches any development version, including a STALE one left behind
#     by a bump -- but it finds nothing at all once the project ships a non-dev version;
#   * the authority string itself catches a fresh copy of the current version, which is
#     the case the first pattern misses at a real release.
# Left lookbehind rejects a preceding DIGIT or DOT, so a longer number is not read as a version
# starting mid-way through it, but deliberately ALLOWS a letter so a `v`-prefixed copy is a hit --
# `\b` would not be, because between `v` and the digit both sides are word characters.
# Right lookahead rejects a following word character (so `-development` is not a version) but
# ALLOWS a following dot, because the carrier form this check exists to catch is exactly
# `<version>.<short sha>`.
DEV_SHAPED = re.compile(r"(?<![\d.])\d+\.\d+\.\d+-dev(?![\w-])")
SECTION = re.compile(r"^\[([^\[\]]+)\]\s*$")
# `[[bench]]` / `[[bin]]` are array-of-table headers. Without matching them the previous
# section stayed in force, so an inline table inside a `[[bench]]` that followed a
# `[dependencies]` block was read AS a dependency -- a false failure, and under --fix a
# rewrite of a line that is not a dependency requirement.
ARRAY_SECTION = re.compile(r"^\[\[([^\[\]]+)\]\]\s*$")
# A dependency can also be written as its own sub-table -- `[dependencies.hudi-core]` with its
# keys on the lines that follow. That is legal Cargo and the repo already uses it
# (`python/Cargo.toml` has `[dependencies.pyo3]`). It is PARSED here rather than refused: a shape
# this checker skipped would let a drifted version be reported as clean, and a shape it refused
# would demand the repo rewrite a perfectly good manifest to suit the checker.
DEP_SUBTABLE = re.compile(r"^((?:target\.[^.]+\.)?(?:dev-|build-)?dependencies)\.(.+)$")
ENTRY = re.compile(r"^\"?([A-Za-z0-9_.-]+)\"?\s*=\s*(.*)$")
KEY_PATH = re.compile(r'(?<![A-Za-z0-9_-])path\s*=\s*"([^"]*)"')
KEY_VERSION = re.compile(r'(?<![A-Za-z0-9_-])version\s*=\s*"([^"]*)"')


def dep_entries(text: str):
    """Yields (section, name, inline_table_text) for every dependency written as an inline table.

    Handles three shapes: `name = { ... }` inline tables, quoted keys, and `[dependencies.name]`
    sub-tables. Yields ("__UNPARSED__", <what>, "") for anything else, so the caller fails loudly
    instead of reporting a skipped dependency as a clean one.

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
        stripped_line = line.strip()
        m = ARRAY_SECTION.match(stripped_line)
        if m:
            section = m.group(1)
            i += 1
            continue
        m = SECTION.match(stripped_line)
        if m:
            section = m.group(1)
            sub = DEP_SUBTABLE.match(section)
            if sub:
                # Collect the sub-table's own keys, up to the next section header of any kind.
                body, j = [], i + 1
                while j < len(lines) and not (
                    SECTION.match(lines[j].strip()) or ARRAY_SECTION.match(lines[j].strip())
                ):
                    body.append(lines[j])
                    j += 1
                yield sub.group(1), sub.group(2), "\n".join(body)
                i = j
                continue
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
    script = ROOT / ".github/scripts/workspace-version.sh"
    try:
        out = subprocess.run([str(script)], capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        stderr = getattr(exc, "stderr", "") or ""
        raise SystemExit(
            f"cannot read the authoritative version: {script} failed ({exc}). {stderr.strip()}\n"
            f"Check it exists and is executable (chmod +x)."
        )
    return out.stdout.strip()


def tracked_files() -> list[str]:
    # Rule 2 sweeps the tracked set rather than the directory tree, so build outputs and vendored
    # sources cannot manufacture a finding. The cost is a dependency on git, which an extracted
    # source tarball does not have -- say that plainly rather than failing as a git error.
    try:
        out = subprocess.run(
            ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SystemExit(
            f"this check needs a git work tree to enumerate tracked files, and `git ls-files` "
            f"failed in {ROOT} ({exc}). Run it from a clone rather than an extracted archive."
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
            if section == "__UNPARSED__":
                failures.append(
                    f"{rel}: {name} is a Cargo shape this checker does not parse -- teach it to "
                    f"read this shape rather than skipping it, since a skipped dependency would "
                    f"let a drifted version be reported as clean"
                )
                continue
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
        authority_shaped = re.compile(r"(?<![\d.])" + re.escape(want) + r"(?![\w-])")
        for lineno, line in enumerate(text.splitlines(), 1):
            # Both patterns match the same text when the authority is itself dev-shaped, so the
            # hits are de-duplicated -- one line reported twice reads as two defects.
            hits = {h for pattern in (DEV_SHAPED, authority_shaped) for h in pattern.findall(line)}
            for hit in sorted(hits):
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
