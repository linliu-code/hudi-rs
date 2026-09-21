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
from typing import NamedTuple

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

# Exemptions are named files rather than a suffix rule such as `*.md`, because a blanket rule
# would let a pinned version in ANY document rot silently -- documentation that tells a reader to
# install the wrong version is the same defect as a stale literal in a script, just slower to
# notice.
SWEEP_SKIP_FILES = (
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


SECTION = re.compile(r"^\[([^\[\]]+)\]$")
# `[[bench]]` / `[[bin]]` are array-of-table headers. Without matching them the previous
# section stayed in force, so an inline table inside a `[[bench]]` that followed a
# `[dependencies]` block was read AS a dependency -- a false failure, and under --fix a
# rewrite of a line that is not a dependency requirement.
ARRAY_SECTION = re.compile(r"^\[\[([^\[\]]+)\]\]$")
DEP_KINDS = ("dependencies", "dev-dependencies", "build-dependencies")
ENTRY = re.compile(r"""^("[^"]*"|'[^']*'|[A-Za-z0-9_.-]+)\s*=\s*(.*)$""")
# Values may be basic ("...") or literal ('...') strings; exactly one of the two groups matches.
_STRING = r"""(?:"([^"]*)"|'([^']*)')"""
# The key itself may be bare or quoted (`"version" = ...` is the same key). Search with find_key(),
# which ignores a match that sits inside a string value.
KEY_PATH = re.compile(r"""(?<![A-Za-z0-9_-])(?:path|"path"|'path')\s*=\s*""" + _STRING)
KEY_VERSION = re.compile(r"""(?<![A-Za-z0-9_-])(?:version|"version"|'version')\s*=\s*""" + _STRING)
KEY_PACKAGE = re.compile(r"""(?<![A-Za-z0-9_-])(?:package|"package"|'package')\s*=\s*""" + _STRING)
# A dependency that names a registry or a git source is not the workspace's own crate, whatever
# its name.
KEY_ELSEWHERE = re.compile(r"""(?<![A-Za-z0-9_-])(?:git|"git"|'git'|registry|"registry"|'registry')\s*=""")
# `name = "1.2.3"` and `name = '1.2.3'`: a dependency given as a bare version requirement.
STRING_VALUE = re.compile(r"""^("([^"]*)"|'([^']*)')$""")
UNPARSED = "__UNPARSED__"
TRIPLE_QUOTE = re.compile(r"\"\"\"|\'\'\'")


class Dep(NamedTuple):
    """One dependency as a manifest writes it.

    `spec` is its keys as `key = value` text with comments removed, for KEY_* searches.
    `version_line` is the index of the line holding its `version` literal (None if it has none),
    so --fix rewrites that one line and never some other occurrence of the same text.
    """

    section: str
    name: str
    spec: str
    version_line: int | None


def string_value(m: re.Match) -> tuple[str, int, int]:
    """(value, start, end) of whichever string group of a KEY_* match matched."""
    g = 1 if m.group(1) is not None else 2
    return m.group(g), m.start(g), m.end(g)


def blank_strings(code: str) -> str:
    """Returns `code` with the contents of quoted strings replaced by spaces (same length).

    Used to count brackets and braces, which must not be counted inside a string such as
    `package = "a{b"`.
    """
    out, quote, i = [], None, 0
    while i < len(code):
        ch = code[i]
        if quote:
            if ch == "\\" and quote == '"' and i + 1 < len(code):
                out.append("  ")
                i += 2
                continue
            if ch == quote:
                quote = None
                out.append(ch)
            else:
                out.append(" ")
        else:
            if ch in "\"'":
                quote = ch
            out.append(ch)
        i += 1
    return "".join(out)


def find_key(pattern: re.Pattern, text: str) -> re.Match | None:
    """The first match of a KEY_* pattern that is not inside a string value.

    `features = ["version = '1'"]` contains the text of a version key, but not the key.
    """
    bare = blank_strings(text)
    for m in pattern.finditer(text):
        if bare[m.start()] == text[m.start()]:
            return m
    return None


def manifest_lines(text: str, keepends: bool = False) -> list[str]:
    """Splits a manifest on LF only.

    `str.splitlines()` also splits on U+2028, U+0085 and friends, which TOML allows inside comments
    and strings. With keepends=False the line ending (LF or CRLF) is removed.
    """
    kept = [part for part in re.split(r"(?<=\n)", text) if part]
    return kept if keepends else [part.rstrip("\n").rstrip("\r") for part in kept]


def depth(code: str, opener: str, closer: str) -> int:
    bare = blank_strings(code)
    return bare.count(opener) - bare.count(closer)


def code_part(line: str) -> str:
    """Returns `line` without an unquoted trailing `# comment`, right-stripped.

    A `#` inside a quoted string is kept. The result is always a prefix of `line`.
    """
    quote = None
    i = 0
    while i < len(line):
        ch = line[i]
        if quote:
            if ch == "\\" and quote == '"':
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in "\"'":
            quote = ch
        elif ch == "#":
            return line[:i].rstrip()
        i += 1
    return line.rstrip()


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


def header(line: str) -> tuple[str, re.Match] | None:
    code = code_part(line).strip()
    m = ARRAY_SECTION.match(code)
    if m:
        return "array", m
    m = SECTION.match(code)
    return ("table", m) if m else None


def version_line_in(lines: list[str], indices) -> int | None:
    for j in indices:
        if find_key(KEY_VERSION, code_part(lines[j])):
            return j
    return None


def dep_entries(text: str):
    """Yields a Dep for every dependency in a manifest.

    Handles five shapes: `name = { ... }` inline tables (also split across lines), bare version
    strings `name = "<requirement>"`, quoted keys, dotted keys (`name.workspace = true`,
    `name.version = "..."`, gathered per dependency), and `[dependencies.name]` sub-tables.
    Comments are ignored, including a `#` after a section header or a value. Yields
    Dep(UNPARSED, <what>, "", None) for any other shape -- including any multi-line (triple-quoted)
    string, whose quotes this line-based reader cannot track -- so the caller fails loudly instead
    of reporting a skipped dependency as a clean one.

    Known limit: a triple-quoted string OUTSIDE a dependency table (say a `[package]` `readme`
    whose text contains `[dependencies]` lines) is not recognised as a string, so lines inside it
    are read as TOML. No manifest in this repository uses triple-quoted strings.

    Hand-rolled rather than via `tomllib`, which only exists on Python 3.11+ -- this has to run
    on whatever python3 a contributor's machine and the CI runner happen to have.
    """
    lines = manifest_lines(text)
    table = None
    dotted: dict[str, list[int]] = {}

    def flush():
        # `hudi-core.version = "x"` and `hudi-core.path = "..."` read as `version = "x"` and
        # `path = "..."` of one dependency.
        for name, idxs in dotted.items():
            spec = "\n".join(code_part(lines[j]).strip().split(".", 1)[1] for j in idxs)
            yield Dep(table, name, spec, version_line_in(lines, idxs))
        dotted.clear()

    i = 0
    while i < len(lines):
        h = header(lines[i])
        if h:
            yield from flush()
            kind = dep_table(h[1].group(1)) if h[0] == "table" else None
            table = None
            if kind and kind[1] is not None:
                # The sub-table's own keys, up to the next section header of any kind.
                j = i + 1
                while j < len(lines) and not header(lines[j]):
                    j += 1
                body = range(i + 1, j)
                spec = "\n".join(code_part(lines[k]) for k in body)
                if TRIPLE_QUOTE.search(spec):
                    yield Dep(UNPARSED, f"[{kind[0]}.{kind[1]}] uses a multi-line string", "", None)
                else:
                    yield Dep(kind[0], kind[1], spec, version_line_in(lines, body))
                i = j
                continue
            if kind:
                table = kind[0]
            i += 1
            continue
        code = code_part(lines[i]).strip()
        if table is None or not code:
            i += 1
            continue
        m = ENTRY.match(code)
        if not m:
            yield Dep(UNPARSED, f"[{table}] line `{code}`", "", None)
            i += 1
            continue
        key, value = split_key(m.group(1)), m.group(2).strip()
        name = unquote(key[0])
        start = i
        if value.startswith(("[", "{")) and not TRIPLE_QUOTE.search(value):
            # A value that may continue on later lines: consume them, up to the next header.
            opener, closer = ("[", "]") if value.startswith("[") else ("{", "}")
            level = depth(value, opener, closer)
            while level > 0 and i + 1 < len(lines) and not header(lines[i + 1]):
                i += 1
                level += depth(code_part(lines[i]), opener, closer)
            shape = "array" if opener == "[" else "inline table"
            if level > 0:
                yield Dep(UNPARSED, f"[{table}] {name} has an unterminated {shape}", "", None)
                i += 1
                continue
        span = range(start, i + 1)
        if any(TRIPLE_QUOTE.search(code_part(lines[k])) for k in span):
            yield Dep(UNPARSED, f"[{table}] {name} uses a multi-line string", "", None)
        elif len(key) == 2:
            dotted.setdefault(name, []).append(start)
        elif len(key) > 2:
            yield Dep(UNPARSED, f"[{table}] {name} written as the dotted key `{m.group(1)}`", "", None)
        elif value.startswith("{"):
            spec = "\n".join([value] + [code_part(lines[k]) for k in range(start + 1, i + 1)])
            yield Dep(table, name, spec, version_line_in(lines, span))
        elif STRING_VALUE.match(value):
            sm = STRING_VALUE.match(value)
            requirement = sm.group(2) if sm.group(2) is not None else sm.group(3)
            yield Dep(table, name, f'version = "{requirement}"', i)
        else:
            yield Dep(UNPARSED, f"[{table}] {name} = {value}", "", None)
        i += 1
    yield from flush()


def rewrite_version(line: str, want: str) -> str:
    """Rewrites the version literal in one manifest line, leaving any trailing comment alone."""
    code = code_part(line)
    rest = line[len(code):]
    m = find_key(KEY_VERSION, code)
    if m:
        _, start, end = string_value(m)
        return code[:start] + want + code[end:] + rest
    key, eq, value = code.partition("=")
    sm = STRING_VALUE.match(value.strip())
    if not eq or not sm:
        return line
    lead = value[: len(value) - len(value.lstrip())]
    return key + eq + lead + value.strip()[0] + want + value.strip()[-1] + rest


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
    try:
        raw = (ROOT / "Cargo.toml").read_bytes().decode("utf-8")
    except UnicodeDecodeError:
        raise SystemExit("Cargo.toml is not valid UTF-8, which Cargo requires, so rule 0 cannot read "
                         "its workspace members.")
    text = "\n".join(code_part(line) for line in manifest_lines(raw))
    block = re.search(r"^\[workspace\]\s*$(.*?)(?=^\[)", text, re.M | re.S)
    if not block:
        raise SystemExit("Cargo.toml has no [workspace] section, so rule 0 cannot know what the "
                         "members are. This checker assumes a workspace root.")
    # Anchored so `default-members = [...]` (a subset, often listed first) is not read as `members`.
    listing = re.search(r"(?<![A-Za-z0-9_-])members\s*=\s*\[(.*?)\]", block.group(1), re.S)
    if not listing:
        raise SystemExit("[workspace] has no `members` list, so rule 0 cannot know what to check.")
    manifests: list[str] = []
    for m in re.finditer(_STRING, listing.group(1)):
        pattern = string_value(m)[0]
        for d in sorted(ROOT.glob(pattern)):
            manifest = d / "Cargo.toml"
            if manifest.is_file():
                manifests.append(str(manifest.relative_to(ROOT)))
    return manifests


PACKAGE_VERSION_KEY = re.compile(r"""^(version|"version"|'version')\s*[.=]""")
# Both spellings Cargo accepts for inheriting the workspace version.
INHERITS_VERSION = (
    re.compile(r"""^(version|"version"|'version')\s*\.\s*workspace\s*=\s*true$"""),
    re.compile(r"""^(version|"version"|'version')\s*=\s*\{\s*workspace\s*=\s*true\s*\}$"""),
)
PACKAGE_NAME = re.compile(r"""^(?:name|"name"|'name')\s*=\s*""" + _STRING)


def package_section(text: str):
    """Yields the stripped lines of a manifest's own `[package]` section."""
    section = ""
    for line in manifest_lines(text):
        stripped = code_part(line).strip()
        m = ARRAY_SECTION.match(stripped) or SECTION.match(stripped)
        if m:
            section = m.group(1)
            continue
        if section == "package" and stripped:
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
            return string_value(m)[0]
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


def read_manifest(rel: str, failures: list[str], cache: dict[str, str | None]) -> str | None:
    """The manifest's text, or None (with one failure recorded) if it is not valid UTF-8.

    Read as bytes, not with read_text(): universal newlines would turn a CRLF manifest into LF
    when --fix writes it back.
    """
    if rel not in cache:
        try:
            cache[rel] = (ROOT / rel).read_bytes().decode("utf-8")
        except UnicodeDecodeError:
            failures.append(f"{rel}: not valid UTF-8, which Cargo requires; it cannot be checked")
            cache[rel] = None
    return cache[rel]


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
    manifests: dict[str, str | None] = {}
    for rel in members:
        text = read_manifest(rel, failures, manifests)
        if text is None:
            continue
        key = package_version_key(text)
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
    member_names = {package_name(manifests[rel]) for rel in members if manifests[rel] is not None} - {None}
    for rel in tracked:
        if Path(rel).name != "Cargo.toml":
            continue
        in_workspace = rel == "Cargo.toml" or rel in members
        text = read_manifest(rel, failures, manifests)
        if text is None:
            continue
        lines_kept = manifest_lines(text, keepends=True)
        rewrites: dict[int, str] = {}
        for section, name, spec, version_line in dep_entries(text):
            if section == UNPARSED:
                failures.append(
                    f"{rel}: {name} is a Cargo shape this checker does not parse -- teach it to "
                    f"read this shape rather than skipping it, since a skipped dependency would "
                    f"let a drifted version be reported as clean"
                )
                continue
            renamed = find_key(KEY_PACKAGE, spec)
            renamed = string_value(renamed)[0] if renamed else None
            # A bare `tpch = "0.3"` from the registry would still read as the member `tpch` here; no
            # such dependency exists, and one would be reported loudly rather than skipped.
            names_member = (in_workspace and not find_key(KEY_ELSEWHERE, spec)
                            and (renamed or name) in member_names)
            if not find_key(KEY_PATH, spec) and not names_member:
                continue  # not an intra-workspace dependency
            got = find_key(KEY_VERSION, spec)
            got = string_value(got)[0] if got else None
            if got is None:
                continue  # path-only: cargo resolves it by path, there is no literal to drift
            checked_deps += 1
            if got == want:
                continue
            if fix:
                line = lines_kept[version_line] if version_line is not None else ""
                body = line.rstrip("\r\n")
                new = rewrite_version(body, want)
                if version_line is None or new == body:
                    failures.append(f"{rel}: [{section}] {name} could not be rewritten")
                else:
                    rewrites[version_line] = new + line[len(body):]
                    fixed.append(f"{rel}: [{section}] {name} {got} -> {want}")
            else:
                failures.append(
                    f"{rel}: [{section}] {name} requests version \"{got}\" "
                    f"but the authority is \"{want}\""
                )
        if rewrites:
            for index, new_line in rewrites.items():
                lines_kept[index] = new_line
            (ROOT / rel).write_bytes("".join(lines_kept).encode("utf-8"))

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
