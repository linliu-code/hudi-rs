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
"""Tests for check_version_single_source.py.

Each test copies this repository's tracked files into a throwaway git repository, changes one
thing, and runs the real checker there, so the verdicts are the ones CI would reach on a tree
with that change -- and the release-bump tests exercise the tree as it actually is.

Versions in this file are assembled at run time (see `dev`) rather than written out, so the file
does not itself carry a string the checker's sweep would report.

Run with:  python3 -m unittest discover -s .github/scripts -p 'test_*.py'   (or `make test-version-check`)
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CHECKER = ".github/scripts/check-version-single-source.sh"


def dev(release: str) -> str:
    return release + "-" + "dev"


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


class Tree:
    """A throwaway git repository holding a copy of this repository's tracked files."""

    _template: Path | None = None

    @classmethod
    def template(cls) -> Path:
        if cls._template is None:
            base = Path(tempfile.mkdtemp(prefix="version-check-template-"))
            listing = run(["git", "ls-files", "-z"], ROOT)
            assert listing.returncode == 0, listing.stderr
            for rel in filter(None, listing.stdout.split("\0")):
                src = ROOT / rel
                if src.is_file() and not src.is_symlink():
                    dst = base / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                elif src.is_symlink():
                    dst = base / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    dst.symlink_to(src.readlink())
            cls._template = base
        return cls._template

    def __init__(self) -> None:
        self.dir = Path(tempfile.mkdtemp(prefix="version-check-"))
        shutil.rmtree(self.dir)
        shutil.copytree(self.template(), self.dir, symlinks=True)
        for cmd in (["git", "init", "-q"], ["git", "add", "-A"],
                    ["git", "-c", "user.name=test", "-c", "user.email=test@example.invalid",
                     "commit", "-q", "-m", "fixture"]):
            result = run(cmd, self.dir)
            assert result.returncode == 0, result.stderr

    def close(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def read(self, rel: str) -> str:
        return (self.dir / rel).read_text()

    def write(self, rel: str, text: str, track: bool = False) -> None:
        path = self.dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        if track:
            assert run(["git", "add", rel], self.dir).returncode == 0

    def sub(self, rel: str, pattern: str, replacement: str) -> None:
        before = self.read(rel)
        after, n = re.subn(pattern, lambda _: replacement, before, count=1, flags=re.M)
        assert n == 1 and after != before, f"fixture edit matched nothing in {rel}: {pattern}"
        self.write(rel, after)

    def append(self, rel: str, text: str) -> None:
        self.write(rel, self.read(rel).rstrip("\n") + "\n" + text)

    def authority(self) -> str:
        result = run([".github/scripts/workspace-version.sh"], self.dir)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    def set_authority(self, version: str) -> None:
        self.sub("Cargo.toml", r'(?<=^\[workspace\.package\]\nversion = ")[^"]+', version)

    def check(self, *args: str) -> subprocess.CompletedProcess:
        return run([CHECKER, *args], self.dir)


class CheckerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tree = Tree()
        self.addCleanup(self.tree.close)
        self.want = self.tree.authority()
        self.release = self.want.split("-")[0]

    def assertPasses(self, result: subprocess.CompletedProcess) -> None:
        self.assertEqual(result.returncode, 0, f"checker failed:\n{result.stdout}{result.stderr}")

    def assertFails(self, result: subprocess.CompletedProcess, *needles: str) -> None:
        self.assertEqual(result.returncode, 1, f"checker did not fail:\n{result.stdout}{result.stderr}")
        for needle in needles:
            self.assertIn(needle, result.stderr)

    def hudi_core_dep(self) -> str:
        return r"^hudi-core = \{ version = \"[^\"]+\", path = \"\.\./core\", default-features = false \}$"

    def release_tree(self) -> None:
        """Moves the copy to the non-dev release of its version, the way a releaser would."""
        self.tree.set_authority(self.release)
        self.assertPasses(self.tree.check("--fix"))


class TreeAsIs(CheckerTestCase):
    def test_passes(self):
        self.assertPasses(self.tree.check())


class ReleaseBumps(CheckerTestCase):
    """Rule 2 must not turn red at a release over prose and history that is not a copy."""

    def test_the_versions_a_release_moves_to_pass(self):
        x, y, z = (int(p) for p in self.release.split("."))
        for version in (self.release, f"{self.release}-rc.1", f"{x}.{y + 1}.0", f"{x + 1}.0.0",
                        f"{x}.{y}.{z + 1}", dev(f"{x}.{y + 1}.0")):
            with self.subTest(version=version):
                tree = Tree()
                try:
                    tree.set_authority(version)
                    self.assertPasses(tree.check("--fix"))
                    self.assertPasses(tree.check())
                finally:
                    tree.close()

    def test_a_bare_release_version_in_prose_is_not_a_copy(self):
        self.release_tree()
        self.tree.append("README.md", f"Bumps an unrelated tool from {self.release} to a newer one.\n")
        self.assertPasses(self.tree.check())

    def test_the_carrier_form_of_a_release_version_is_caught_anywhere(self):
        self.release_tree()
        self.tree.append("README.md", f"Pin the carrier {self.release}.abc1234.\n")
        self.assertFails(self.tree.check(), "README.md line")

    def test_a_bare_release_version_in_a_derivation_site_is_caught(self):
        self.release_tree()
        self.tree.append("Makefile", f"HUDI_RELEASE ?= {self.release}\n")
        self.assertFails(self.tree.check(), "Makefile line")

    def test_a_prerelease_version_is_caught_anywhere(self):
        self.tree.set_authority(f"{self.release}-rc.1")
        self.assertPasses(self.tree.check("--fix"))
        self.tree.append("README.md", f"Install {self.release}-rc.1.\n")
        self.assertFails(self.tree.check(), "README.md line")


class Rule0Members(CheckerTestCase):
    def test_a_member_declaring_its_own_version_fails(self):
        self.tree.sub("crates/core/Cargo.toml", r"^version\.workspace = true$", f'version = "{dev("0.5.0")}"')
        self.assertFails(self.tree.check(), "crates/core/Cargo.toml: workspace member declares its own version")

    def test_a_member_with_no_version_key_fails(self):
        self.tree.sub("crates/core/Cargo.toml", r"^version\.workspace = true\n", "")
        self.assertFails(self.tree.check(), "crates/core/Cargo.toml: workspace member has no `version` key")

    def test_the_inline_table_spelling_of_inheritance_passes(self):
        self.tree.sub("crates/core/Cargo.toml", r"^version\.workspace = true$", "version = { workspace = true }")
        self.assertPasses(self.tree.check())

    def test_a_key_that_only_starts_with_version_is_not_the_version(self):
        self.tree.sub("crates/core/Cargo.toml", r"^version\.workspace = true$",
                      'versioning-note = "see the workspace"\nversion.workspace = true')
        self.assertPasses(self.tree.check())


class Rule1Dependencies(CheckerTestCase):
    def test_a_drifted_inline_table_fails(self):
        self.tree.sub("crates/hudi/Cargo.toml", self.hudi_core_dep(),
                      f'hudi-core = {{ version = "{dev("0.5.0")}", path = "../core", default-features = false }}')
        self.assertFails(self.tree.check(), "crates/hudi/Cargo.toml: [dependencies] hudi-core requests version")

    def test_a_drift_split_across_lines_fails(self):
        self.tree.sub("crates/hudi/Cargo.toml", self.hudi_core_dep(),
                      f'hudi-core = {{\n    version = "{dev("0.4.0")}",\n    path = "../core",\n'
                      f'    default-features = false,\n}}')
        self.assertFails(self.tree.check(), "hudi-core requests version")

    def test_a_drifted_sub_table_fails(self):
        self.tree.sub("crates/hudi/Cargo.toml", self.hudi_core_dep(),
                      f'[dependencies.hudi-core]\nversion = "{dev("0.3.0")}"\npath = "../core"\n'
                      f'default-features = false\n\n[dependencies]')
        self.assertFails(self.tree.check(), "hudi-core requests version")

    def test_a_drifted_quoted_key_fails(self):
        self.tree.sub("crates/hudi/Cargo.toml", self.hudi_core_dep(),
                      f'"hudi-core" = {{ version = "{dev("0.2.0")}", path = "../core", default-features = false }}')
        self.assertFails(self.tree.check(), "hudi-core requests version")

    def test_a_bare_version_string_naming_a_member_fails(self):
        self.tree.sub("crates/hudi/Cargo.toml", self.hudi_core_dep(), 'hudi-core = "0.5.0"')
        self.assertFails(self.tree.check(), 'hudi-core requests version "0.5.0"')

    def test_fix_rewrites_a_bare_version_string(self):
        self.tree.sub("crates/hudi/Cargo.toml", self.hudi_core_dep(), 'hudi-core = "0.5.0"')
        self.assertPasses(self.tree.check("--fix"))
        self.assertIn(f'hudi-core = "{self.want}"', self.tree.read("crates/hudi/Cargo.toml"))
        self.assertPasses(self.tree.check())

    def test_fix_rewrites_a_drifted_inline_table(self):
        self.tree.sub("crates/hudi/Cargo.toml", self.hudi_core_dep(),
                      f'hudi-core = {{ version = "{dev("0.5.0")}", path = "../core", default-features = false }}')
        self.assertPasses(self.tree.check("--fix"))
        self.assertPasses(self.tree.check())

    def test_a_dotted_key_dependency_fails_loudly(self):
        self.tree.sub("crates/hudi/Cargo.toml", self.hudi_core_dep(),
                      'hudi-core.version = "0.5.0"\nhudi-core.path = "../core"')
        self.assertFails(self.tree.check(), "is a Cargo shape this checker does not parse")

    def test_a_target_sub_table_with_a_dotted_cfg_is_checked(self):
        self.tree.append("crates/hudi/Cargo.toml",
                         f"\n[target.'cfg(target_env = \"gnu.x\")'.dependencies.hudi-core]\n"
                         f'version = "{dev("0.1.0")}"\npath = "../core"\n')
        self.assertFails(self.tree.check(), "hudi-core requests version")

    def test_an_inline_table_in_an_array_of_tables_is_not_a_dependency(self):
        self.tree.append("crates/hudi/Cargo.toml",
                         '\n[[bench]]\nname = "probe"\nharness = false\n'
                         'something = { path = "../core", version = "0.1.0" }\n')
        self.assertPasses(self.tree.check())

    def test_a_manifest_outside_the_workspace_keeps_its_own_versions(self):
        self.tree.sub("demo/apps/datafusion/Cargo.toml", r'^version = "0\.1\.0"$', 'version = "0.1.1"')
        self.assertPasses(self.tree.check())


class Rule2Sweep(CheckerTestCase):
    def test_a_rehardcoded_carrier_version_in_a_workflow_fails(self):
        self.tree.sub(".github/workflows/jni-native.yml", r'VERSION="\$\(\.github/scripts/workspace-version\.sh.*$',
                      f'VERSION="{self.want}.$(git rev-parse --short HEAD)"')
        self.assertFails(self.tree.check(), ".github/workflows/jni-native.yml line")

    def test_a_v_prefixed_copy_fails(self):
        self.tree.sub(".github/workflows/jni-native.yml", r"^  cancel-in-progress: false$",
                      f"  cancel-in-progress: false\n# carrier pinned at v{self.want}")
        self.assertFails(self.tree.check(), ".github/workflows/jni-native.yml line")

    def test_the_carrier_form_fails(self):
        self.tree.sub(".github/workflows/jni-native.yml", r"^  cancel-in-progress: false$",
                      f"  cancel-in-progress: false\n# was {self.want}.abc1234")
        self.assertFails(self.tree.check(), ".github/workflows/jni-native.yml line")

    def test_a_pinned_dev_version_in_any_other_document_fails(self):
        self.tree.append("README.md", f"Install the {dev('0.4.2')} build.\n")
        self.assertFails(self.tree.check(), "README.md line")

    def test_a_file_whose_name_only_ends_in_cargo_toml_is_swept(self):
        self.tree.write("docs/fooCargo.toml", f'version = "{dev("0.4.2")}"\n', track=True)
        self.assertFails(self.tree.check(), "docs/fooCargo.toml line 1")


class Rule3Derivations(CheckerTestCase):
    def test_a_derivation_replaced_by_a_literal_fails(self):
        self.tree.sub("Makefile", r"\$\(shell \.github/scripts/workspace-version\.sh[^)]*\)", dev("1.1.1"))
        self.assertFails(self.tree.check(), "Makefile: no longer references")

    def test_prose_that_only_mentions_the_script_does_not_count(self):
        self.tree.sub("Makefile", r"\$\(shell \.github/scripts/workspace-version\.sh[^)]*\)", "9.9.9")
        self.tree.append("Makefile", "# derived from .github/scripts/workspace-version.sh\n")
        self.assertFails(self.tree.check(), "Makefile: no longer references")


@unittest.skipUnless(shutil.which("cmake"), "cmake is not on PATH")
class CMakeDerivation(unittest.TestCase):
    """cpp/CMakeLists.txt's version regex, run by cmake itself on LF and CRLF manifests."""

    def derive(self, newline: str) -> subprocess.CompletedProcess:
        text = (ROOT / "cpp/CMakeLists.txt").read_text()
        start = text.index("\nfile(READ") + 1
        end = text.index("\nproject(hudi-cpp") + 1
        work = Path(tempfile.mkdtemp(prefix="version-check-cmake-"))
        self.addCleanup(shutil.rmtree, work, True)
        (work / "cpp").mkdir()
        manifest = (ROOT / "Cargo.toml").read_text().replace("\n", newline)
        (work / "Cargo.toml").write_bytes(manifest.encode())
        script = work / "cpp" / "derive.cmake"
        script.write_text(
            f'set(CMAKE_CURRENT_SOURCE_DIR "{work / "cpp"}")\n'
            + text[start:end]
            + 'message(STATUS "DERIVED=${HUDI_CPP_VERSION}")\n'
        )
        return run(["cmake", "-P", str(script)], work)

    def test_lf_and_crlf_manifests_derive_the_same_version(self):
        want = run([".github/scripts/workspace-version.sh"], ROOT).stdout.strip().split("-")[0]
        for name, newline in (("LF", "\n"), ("CRLF", "\r\n")):
            with self.subTest(line_endings=name):
                result = self.derive(newline)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn(f"DERIVED={want}", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
