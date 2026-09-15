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
reporting them, which is what `make version-sync` runs after a bump. The other rules are
never auto-fixed: a second authority, a stray hardcode and a lost derivation all need a human
to decide what the site should read instead.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Files that are allowed to contain a literal of the project's own version. Everything else
# must derive it. Cargo.lock is generated and cargo keeps it in step by itself.
#
# Cargo.toml is exempt from the SWEEP because a manifest is full of legitimate version literals
# -- every third-party requirement is one -- so sweeping it would be all false positives. The two
# fields in a manifest that CAN carry a second copy of the project's own version are therefore
# checked structurally instead, by rule 0 (the member's own `[package] version`) and rule 1 (an
# intra-workspace dependency requirement). A literal parked anywhere else in a manifest, such as
# under `[package.metadata]`, is deliberately NOT a failure: it is inert to cargo and to every
# artifact this project publishes, and a rule broad enough to catch it would have to guess which
# of a manifest's many versions is the project's own.
# Compared with the file's NAME, so `fooCargo.toml` is swept like any other file.
LITERAL_ALLOWED_NAMES = ("Cargo.toml", "Cargo.lock")

# One prose file is exempt: crates/jni/README.md records the versions of carriers that were
# actually published, which are history and must NOT be rewritten by a bump. The exemption is
# named rather than extended to `*.md`, because a blanket suffix rule would let a pinned version
# in ANY document rot silently -- documentation that tells a reader to install the wrong version
# is the same defect as a stale literal in a script, just slower to notice.
SWEEP_SKIP_FILES = (
    "crates/jni/README.md",
    # Release-process prose: its bump-rule sentence quotes three example versions (the
    # current dev version and the minor/major bumps of it) that are meant to stay as written
    # after every real bump. Upstream #749/#759 added the sentence; it is not a version site.
    "release/README.md",
)

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

# Rule 2 sweeps for more than one pattern, because each alone has a hole:
#   * the `-dev` shape catches any development version, including a STALE one left behind
#     by a bump -- but it finds nothing at all once the project ships a non-dev version;
#   * the authority string itself catches a fresh copy of the current version, which is
#     the case the first pattern misses at a real release.
# The authority is only swept for everywhere when the string is distinctive enough to be ours: a
# pre-release version (`x.y.z-rc.1`), or any version followed by `.<short sha>`, the carrier form.
# A bare `x.y.z` is not: at a release it collides with unrelated third-party versions -- a
# changelog line bumping a dependency "from 0.6.0", an action pinned at `v0.9.0`, a tool config
# at `1.0.0` -- and a check that fires on those turns red on every release branch over text that
# is not a copy of anything. A bare `x.y.z` is therefore swept only in the MUST_DERIVE files below,
# the sites that used to carry a copy and are the ones a re-hardcode would land in.
# Left lookbehind rejects a preceding DIGIT or DOT, so a longer number is not read as a version
# starting mid-way through it, but deliberately ALLOWS a letter so a `v`-prefixed copy is a hit --
# `\b` would not be, because between `v` and the digit both sides are word characters.
# Right lookahead rejects a following word character (so `-development` is not a version) but
# ALLOWS a following dot, because the carrier form this check exists to catch is exactly
# `<version>.<short sha>`.
DEV_SHAPED = re.compile(r"(?<![\d.])\d+\.\d+\.\d+-dev(?![\w-])")


def authority_patterns(want: str) -> tuple[re.Pattern, re.Pattern]:
    """Returns (swept in every file, swept only in MUST_DERIVE files) for the authority `want`."""
    exact = r"(?<![\d.])" + re.escape(want) + r"(?![\w-])"
    carrier = r"(?<![\d.])" + re.escape(want) + r"\.[0-9a-f]{7,40}(?![\w-])"
    everywhere = re.compile(exact if "-" in want else carrier)
    return everywhere, re.compile(exact)


SECTION = re.compile(r"^\[([^\[\]]+)\]\s*$")
# `[[bench]]` / `[[bin]]` are array-of-table headers. Without matching them the previous
# section stayed in force, so an inline table inside a `[[bench]]` that followed a
# `[dependencies]` block was read AS a dependency -- a false failure, and under --fix a
# rewrite of a line that is not a dependency requirement.
ARRAY_SECTION = re.compile(r"^\[\[([^\[\]]+)\]\]\s*$")
DEP_KINDS = ("dependencies", "dev-dependencies", "build-dependencies")
ENTRY = re.compile(r"""^("[^"]*"|'[^']*'|[A-Za-z0-9_.-]+)\s*=\s*(.*)$""")
KEY_PATH = re.compile(r'(?<![A-Za-z0-9_-])path\s*=\s*"([^"]*)"')
KEY_VERSION = re.compile(r'(?<![A-Za-z0-9_-])version\s*=\s*"([^"]*)"')
KEY_PACKAGE = re.compile(r'(?<![A-Za-z0-9_-])package\s*=\s*"([^"]*)"')
# `name = "1.2.3"` and `name = '1.2.3'`: a dependency given as a bare version requirement.
STRING_VALUE = re.compile(r"""^("([^"]*)"|'([^']*)')\s*(#.*)?$""")
UNPARSED = "__UNPARSED__"


def split_key(key: str) -> list[str]:
    """Splits a dotted TOML key into its parts, honouring quotes.

    `target.'cfg(target_env = "gnu.x")'.dependencies` is three parts, not four: a plain
    `str.split(".")` would cut the quoted cfg expression at its dot.
    """
    parts, buf, quote = [], "", None
    for ch in key.strip():
        if quote:
            buf += ch
            if ch == quote:
                quote = None
        elif ch in "\"'":
            quote = ch
            buf += ch
        elif ch == ".":
            parts.append(buf.strip())
            buf = ""
        else:
            buf += ch
    parts.append(buf.strip())
    return parts


def unquote(part: str) -> str:
    return part[1:-1] if len(part) >= 2 and part[0] == part[-1] and part[0] in "\"'" else part


def dep_table(section: str) -> tuple[str, str | None] | None:
    """Classifies a section header as a dependency table.

    Returns (table, None) for `[dependencies]`, `[target.<cfg>.dependencies]` and
    `[workspace.dependencies]` (and the dev-/build- kinds), (table, name) for a dependency written
    as its own sub-table such as `[dependencies.hudi-core]`, and None for any other section.
    """
    parts = split_key(section)
    if parts[-1] in DEP_KINDS and (
        len(parts) == 1 or (len(parts) == 3 and parts[0] == "target")
        or (len(parts) == 2 and parts[0] == "workspace")
    ):
        return section, None
    if len(parts) >= 2 and parts[-2] in DEP_KINDS and (
        len(parts) == 2 or (len(parts) == 4 and parts[0] == "target")
        or (len(parts) == 3 and parts[0] == "workspace")
    ):
        return ".".join(parts[:-1]), unquote(parts[-1])
    return None


def dep_entries(text: str):
    """Yields (section, name, spec, raw) for every dependency in a manifest.

    `spec` is text KEY_PATH / KEY_VERSION / KEY_PACKAGE can be searched in, and `raw` is the exact
    text the entry occupies in the manifest, so --fix can rewrite it in place. Handles four shapes:
    `name = { ... }` inline tables (also split across lines), `name = "<requirement>"` bare
    version strings, quoted keys, and `[dependencies.name]` sub-tables. Yields
    (UNPARSED, <what>, "", "") for any other shape -- a dotted key such as
    `hudi-core.version = "..."`, or a value that is neither a table nor a string -- so the caller
    fails loudly instead of reporting a skipped dependency as a clean one.

    Hand-rolled rather than via `tomllib`, which only exists on Python 3.11+ -- this has to run
    on whatever python3 a contributor's machine and the CI runner happen to have.
    """
    table = None
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        m = ARRAY_SECTION.match(stripped)
        if m:
            table = None
            i += 1
            continue
        m = SECTION.match(stripped)
        if m:
            kind = dep_table(m.group(1))
            table = None
            if kind and kind[1] is not None:
                # Collect the sub-table's own keys, up to the next section header of any kind.
                body, j = [], i + 1
                while j < len(lines) and not (
                    SECTION.match(lines[j].strip()) or ARRAY_SECTION.match(lines[j].strip())
                ):
                    body.append(lines[j])
                    j += 1
                raw = "\n".join(body)
                yield kind[0], kind[1], raw, raw
                i = j
                continue
            if kind:
                table = kind[0]
            i += 1
            continue
        if table is None or not stripped or stripped.startswith("#"):
            i += 1
            continue
        m = ENTRY.match(stripped)
        if not m:
            yield UNPARSED, f"[{table}] line `{stripped}`", "", ""
            i += 1
            continue
        key, value = split_key(m.group(1)), m.group(2).strip()
        name = unquote(key[0])
        if len(key) > 1:
            yield UNPARSED, f"[{table}] {name} written as the dotted key `{m.group(1)}`", "", ""
            i += 1
            continue
        if value.startswith("{"):
            raw = lines[i]
            depth = raw.count("{") - raw.count("}")
            while depth > 0 and i + 1 < len(lines):
                i += 1
                raw += "\n" + lines[i]
                depth += lines[i].count("{") - lines[i].count("}")
            yield table, name, raw[raw.index("=") + 1:], raw
        elif STRING_VALUE.match(value):
            sm = STRING_VALUE.match(value)
            requirement = sm.group(2) if sm.group(2) is not None else sm.group(3)
            yield table, name, f'version = "{requirement}"', lines[i]
        else:
            yield UNPARSED, f"[{table}] {name} = {value}", "", ""
        i += 1


def workspace_members() -> list[str]:
    """Returns the manifest path of every member of the root workspace.

    Reads and expands `[workspace] members` from the root manifest rather than shelling out to
    `cargo metadata`, for the same reason the dependency parsing is hand-rolled: this has to run
    wherever a contributor or a CI runner has a python3, without requiring a cargo on PATH or a
    resolvable dependency graph.

    Members are what rule 0 governs. A manifest OUTSIDE the member list -- `demo/apps/*` -- carries
    its own unrelated version on purpose and is none of this checker's business: it is not part of
    this workspace and nothing published from here derives from it.
    """
    text = (ROOT / "Cargo.toml").read_text()
    block = re.search(r"^\[workspace\]\s*$(.*?)(?=^\[)", text, re.M | re.S)
    if not block:
        raise SystemExit("Cargo.toml has no [workspace] section, so rule 0 cannot know what the "
                         "members are. This checker assumes a workspace root.")
    listing = re.search(r"members\s*=\s*\[(.*?)\]", block.group(1), re.S)
    if not listing:
        raise SystemExit("[workspace] has no `members` list, so rule 0 cannot know what to check.")
    manifests: list[str] = []
    for pattern in re.findall(r'"([^"]+)"', listing.group(1)):
        for d in sorted(ROOT.glob(pattern)):
            manifest = d / "Cargo.toml"
            if manifest.is_file():
                manifests.append(str(manifest.relative_to(ROOT)))
    return manifests


PACKAGE_VERSION_KEY = re.compile(r"^version\s*[.=]")
# Both spellings Cargo accepts for inheriting the workspace version.
INHERITS_VERSION = (
    re.compile(r"^version\s*\.\s*workspace\s*=\s*true\s*(#.*)?$"),
    re.compile(r"^version\s*=\s*\{\s*workspace\s*=\s*true\s*\}\s*(#.*)?$"),
)
PACKAGE_NAME = re.compile(r'^name\s*=\s*"([^"]+)"')


def package_section(text: str):
    """Yields the stripped lines of a manifest's own `[package]` section."""
    section = ""
    for line in text.splitlines():
        stripped = line.strip()
        m = ARRAY_SECTION.match(stripped) or SECTION.match(stripped)
        if m:
            section = m.group(1)
            continue
        if section == "package":
            yield stripped


def package_version_key(text: str) -> str | None:
    """Returns the raw `version` line from a manifest's own `[package]` section, or None.

    Matches the `version` key itself, not any key that merely starts with those letters.
    """
    for stripped in package_section(text):
        if PACKAGE_VERSION_KEY.match(stripped):
            return stripped
    return None


def inherits_version(key: str) -> bool:
    return any(p.match(key) for p in INHERITS_VERSION)


def package_name(text: str) -> str | None:
    for stripped in package_section(text):
        m = PACKAGE_NAME.match(stripped)
        if m:
            return m.group(1)
    return None


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
    tracked = tracked_files()
    swept_everywhere, swept_in_derivation_sites = authority_patterns(want)

    # ---- Rule 0: every workspace member INHERITS the authority; none declares its own. ----
    # Without this, the authority is not single: a member can set a literal `version` in its
    # own `[package]`, publish under that number, and every other rule still passes -- rule 1 reads
    # dependency requirements, and the rule 2 sweep exempts Cargo.toml entirely. That is the exact
    # shape this milestone exists to make impossible, so it is checked rather than assumed.
    members = workspace_members()
    for rel in members:
        key = package_version_key((ROOT / rel).read_text())
        if key is None:
            failures.append(
                f"{rel}: workspace member has no `version` key in [package] -- it must read "
                f"`version.workspace = true` so the authority stays single"
            )
        elif not inherits_version(key):
            failures.append(
                f"{rel}: workspace member declares its own version (`{key}`) instead of "
                f"`version.workspace = true` -- that is a SECOND authority; the version belongs "
                f"in [workspace.package] in the root Cargo.toml and nowhere else"
            )

    if not members:
        failures.append(
            "found NO workspace members at all -- rule 0 asserted nothing, which means this "
            "checker is broken rather than the tree being clean"
        )

    # ---- Rule 1: every intra-workspace dependency requests exactly the authority. ----
    # Read structurally rather than by line, so a dependency written across several lines is
    # checked like any other instead of being silently skipped. A dependency is intra-workspace if
    # it has a `path`, or if it sits in a workspace manifest and names a workspace member:
    # `hudi-core = "0.5.0"` has no path, but it is still a second copy of the project's version in
    # a published manifest. (A manifest outside the workspace, such as a demo app, may depend on a
    # released version from the registry on purpose, so there only a `path` makes it ours.)
    member_names = {package_name((ROOT / rel).read_text()) for rel in members} - {None}
    for rel in tracked:
        if Path(rel).name != "Cargo.toml":
            continue
        in_workspace = rel == "Cargo.toml" or rel in members
        for section, name, spec, raw in dep_entries((ROOT / rel).read_text()):
            if section == UNPARSED:
                failures.append(
                    f"{rel}: {name} is a Cargo shape this checker does not parse -- teach it to "
                    f"read this shape rather than skipping it, since a skipped dependency would "
                    f"let a drifted version be reported as clean"
                )
                continue
            renamed = KEY_PACKAGE.search(spec)
            names_member = in_workspace and (renamed.group(1) if renamed else name) in member_names
            if not KEY_PATH.search(spec) and not names_member:
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
                key_text, _, value = raw.partition("=")
                if STRING_VALUE.match(value.strip()):
                    new_raw = key_text + "=" + value.replace(got.group(1), want, 1)
                else:
                    new_raw = KEY_VERSION.sub(
                        lambda v: v.group(0)[: v.start(1) - v.start(0)] + want + v.group(0)[v.end(1) - v.start(0):],
                        raw, count=1)
                after = before.replace(raw, new_raw, 1)
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
    for rel in tracked:
        if Path(rel).name in LITERAL_ALLOWED_NAMES or rel in SWEEP_SKIP_FILES:
            continue
        path = ROOT / rel
        if not path.is_file():
            continue
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue  # binary or unreadable: cannot contain a version literal we care about
        patterns = (DEV_SHAPED, swept_everywhere)
        if rel in MUST_DERIVE:
            patterns += (swept_in_derivation_sites,)
        for lineno, line in enumerate(text.splitlines(), 1):
            # The patterns overlap (a dev-shaped authority matches more than one), so the hits are
            # de-duplicated -- one line reported twice reads as two defects.
            hits = {h for pattern in patterns for h in pattern.findall(line)}
            hits = {h for h in hits if not any(o.startswith(h + ".") for o in hits)}
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
    print(f"checked: {len(members)} workspace member(s), {checked_deps} intra-workspace "
          f"dependency requirement(s), {len(tracked)} tracked file(s) swept, "
          f"{len(MUST_DERIVE)} derivation site(s)")

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
