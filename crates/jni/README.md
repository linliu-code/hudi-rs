<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# hudi-jni

JNI exports over hudi-rs for `org.apache.hudi.io.nativereader.NativeFileGroupReader`. This
crate is marshalling only: Java strings in, an Arrow C stream written into a Java-allocated
`ArrowArrayStream` out. The read itself lives in `hudi_jvm_ffi::file_group_v2`. Every export
catches panics — a panic that unwinds into the JVM aborts the process, so failures are turned
into a thrown `NativeReaderException` instead.

## What it exports

Built as a `cdylib` (`libhudi_jni.so` on Linux). Two `Java_…` symbols:

- `Java_org_apache_hudi_io_nativereader_NativeFileGroupReader_readFileGroupInto` — the read.
- `Java_org_apache_hudi_io_nativereader_NativeFileGroupReader_version` — a liveness probe for
  the loader; returns `"hudi-jni <crate version> abi=<JNI_ABI_VERSION>"`.

`JNI_ABI_VERSION` (`crates/jni/src/lib.rs`) is the revision of the `Java_…` export signatures.
It is bumped whenever the parameter list of any exported function changes. The Java side
(`NativeFileGroupReader.REQUIRED_JNI_ABI`) refuses to load a library that reports a lower
revision, turning a stale `libhudi_jni.so` under an up-to-date jar into a load-time error
instead of a mis-read argument slot. The check is one-directional (`abi < REQUIRED_JNI_ABI`):
it protects a jar that is upgraded first, never one that lags behind — **never deploy the
library ahead of the jar; ship jar and library together.**

## How hudi-internal loads it

`hudi-native-reader` resolves `libhudi_jni.so` in one of two ways:

- **Development**: `-Dhudi.native.lib.path=/path/to/libhudi_jni.so` — point at a freshly
  built library (e.g. straight out of `target/release/`).
- **CI / deployments**: the classpath resource `/native/<os>-<arch>/libhudi_jni.so`, packaged
  into the `io.onehouse.hudi-rs:hudi-jni-native` Maven artifact that `hudi-native-reader`
  depends on (runtime scope, version pinned by the `hudi.jni.native.version` property). This
  is what lets aarch64 CI runners with no access to this repo run the native reader.

## Where the carrier is served from

The Maven coordinates are the same regardless of where the jar comes from — only the repository
it resolves from (and whether a classifier is present) changes:

- **CodeArtifact**, via CI (`jni-native.yml`, see "Building the carrier" below — a single
  classifier-less `io.onehouse.hudi-rs:hudi-jni-native:<version>` jar carrying BOTH Linux
  arches, D-26) or, for a single-arch jar, `make jni-deploy`
  (`io.onehouse.hudi-rs:hudi-jni-native:<version>:<os>-<arch>`, needs a fresh `codeartifact`
  token in `~/.m2/settings.xml`).
- **This machine's local Maven repository**, via `make jni-install` (single-arch,
  classifier) or `make jni-install JNI_MULTI=1` (multi-arch, no classifier).
- **A pre-release asset on `onehouseinc/hudi-internal`**. hudi-internal's
  `.github/actions/install-jni-carrier` installs this asset into the runner's local repository
  when the coordinate does not resolve from CodeArtifact (D-18 in the effort workspace) — the
  fallback route if CI's OIDC assume, or CodeArtifact itself, is down.

  **The asset MUST be the multi-arch, classifier-less jar — the same bytes CodeArtifact holds.**
  The consuming action runs `install:install-file` with **no** `-Dclassifier`, so whatever bytes
  it downloads become `io.onehouse.hudi-rs:hudi-jni-native:<version>`, the coordinate every
  module and all 14 bundles resolve. Publishing a single-arch `-linux-aarch64` jar there would
  seed every runner with an aarch64-only library under the multi-arch coordinate: aarch64 passes,
  x86_64 builds succeed and then fail the bundle smoke (no `native/linux-x86_64/…`), and any
  x86_64 deployment built from it fails loud at lookup time, far from the cause. The action pins
  the asset's MD5, which is what enforces "same bytes".

  This is what was actually done for the `0.5.0-dev.5fa3c21` carrier (asset `549507607`,
  md5 `eb80d7ba1aeeaf866cf02318cfe44141`): the jar was taken from the workflow run's own
  `hudi-jni-native-carrier` artifact — never rebuilt, never re-jarred — and uploaded as-is.
  ```
  gh run download <run-id> -R onehouseinc/hudi-rs-internal -n hudi-jni-native-carrier -D /tmp/carrier
  md5sum /tmp/carrier/hudi-jni-native-<version>.jar        # must equal the CodeArtifact artifact's md5
  gh release create hudi-jni-native/<version> \
    /tmp/carrier/hudi-jni-native-<version>.jar \
    --repo onehouseinc/hudi-internal --prerelease --target <a pushed hudi-internal sha>
  ```
  Then set that md5 in `install-jni-carrier/action.yml`. If CI is unavailable altogether, build
  the jar locally with `make jni-jar-multi-portable` (below) — the portable build, never
  `jni-lib`'s host build — and merge in the other arch's `.so`; a jar carrying only this
  machine's arch must not be uploaded.

## Building the carrier

### CI (`.github/workflows/jni-native.yml`)

Triggers:
- Push a tag `jni-native/<version>` (e.g. `jni-native/0.5.0-dev.abc1234`) at the commit to
  build.
- `workflow_dispatch` (kept for when this file reaches the default branch, AS-16) with an
  optional `version` input; defaults to the tag's suffix, else `0.5.0-dev.<short sha>`.

> **Pushing a `jni-native/*` tag IS a publish.** It builds a library from the tagged commit and
> deploys it to the org's shared internal CodeArtifact Maven repository under the OIDC publisher
> role — a shared repository, not just this branch. Tag pushes are *not* covered by branch
> protection, so anyone with push access to this repository can start one. A `guard` job runs
> before any build and refuses the two cheapest abuses: a version outside
> `^[0-9]+\.[0-9]+\.[0-9]+(-dev\.[0-9a-f]{7,})?$` (so a stray `jni-native/1.0.0` cannot mint a
> release-looking coordinate) and a tagged commit that is not an ancestor of `origin/main` or of
> `origin/davis/rli-native-hfile-read` (so a tag on arbitrary code cannot be published). The
> allowed-branch list lives in `ALLOWED_BRANCHES` in `.github/workflows/jni-native.yml` — keep it
> in sync with this paragraph. The remaining control is a repository setting an agent cannot
> make and the repository owner must: put the publish step behind a GitHub `environment` with
> deployment/tag protection so use of the publisher role is auditable and approvable.

A `build` job matrix runs BOTH Linux arches in parallel — `x86_64` on `ubuntu-24.04`,
`aarch64` on `ubuntu-24.04-arm` — each leg builds INSIDE a `manylinux_2_28_<arch>` container
(D-27, below), asserts the `.so` exports exactly 2 `Java_...` symbols, asserts the portability
floor, and runs the **JNI load smoke** twice: once on the runner itself and once inside an
`eclipse-temurin:17-jdk-jammy` container (glibc 2.35, older than the runner's own glibc 2.39) —
both via `.github/jni-smoke/org/apache/hudi/io/nativereader/NativeFileGroupReader.java` (same
package/class as the real reader, only the `version()` liveness probe), which `System.load`s the
just-built library and checks its output matches `SMOKE hudi-jni .* abi=3`. Each leg uploads its
`.so` plus properties as a workflow artifact (`libhudi_jni-linux-<arch>`).

A `package` job (needs both legs) downloads both artifacts, assembles ONE classifier-less jar
(layout below), uploads it as a workflow artifact (`hudi-jni-native-carrier`), then publishes it
to the org's CodeArtifact Maven repo (`onehouse-internal`, D-26) via `mvn deploy:deploy-file`
under the OIDC role `GithubActionsPublishHudi-RepositoryPublisherRole-10H7ABJVFNSQ9` — the same
role hudi-rs-internal's former `publish-native-lib.yml` assumed — and verifies the publish
resolves with `dependency:get` against a clean local `~/.m2/repository`. A new coordinate per
version; never an overwrite. If the OIDC assume fails, the jar still exists as a workflow
artifact and the orchestrator falls back to the release-asset route above.

### The glibc floor (D-27)

A Rust cdylib links the versioned glibc symbols of the BUILD host's libc, so a library built
directly on the `ubuntu-24.04`/`ubuntu-24.04-arm` runners (glibc 2.39) cannot load on any older
host — Amazon Linux 2023 (2.34), RHEL/Alma 8 (2.28), or even this box (2.35). The first cut of
this workflow did exactly that and failed to load anywhere but the runners themselves
(`investigations/m3-carrier-glibc-floor/`). Fix: each leg's `make jni-lib` now runs inside
`quay.io/pypa/manylinux_2_28_<arch>` (AlmaLinux 8, glibc 2.28), which pins the *glibc* floor to
2.28 for free. The *libstdc++*/*libgcc* floor (from `librocksdb-sys`, a C++ dependency) is a
second, independent problem: `RUSTFLAGS`'s `-C link-arg=-static-libstdc++
-C link-arg=-static-libgcc` do **not** remove it — those flags only rewrite gcc's own
*automatic* C++-runtime linking, and have no effect on the *explicit* `-lstdc++`/`-lgcc_s` that
`librocksdb-sys` emits (measured empirically: even a `g++`-driven link with an explicit
`-lstdc++` keeps a dynamic `libstdc++.so.6` NEEDED entry alongside `-static-libstdc++`). The
actual fix is `.github/jni-portable/static-cxx-linker.sh`, a linker wrapper installed via
`RUSTFLAGS="-C linker=..."` that rewrites `-lstdc++`/`-lgcc_s` **in place** (same position in
the link line, so ordering relative to the archives that need them — which `--as-needed`
depends on — is preserved): `-lstdc++` becomes `-Wl,-Bstatic -lstdc++ -Wl,-Bdynamic`; `-lgcc_s`
becomes the ABSOLUTE paths of **both** `libgcc_eh.a` and `libgcc.a` (resolved via
`gcc -print-file-name=...`, which the wrapper verifies actually exists — see the trap below for
why `libgcc.a` alone is not enough). It only touches the `libhudi_jni` cdylib's own final link
(matched by `"libhudi_jni"` appearing in the arguments); every other link (build scripts,
proc-macros, host helper binaries) passes through untouched — an earlier unconditional version
of this wrapper broke a `zerocopy` build-script link this way.

The workflow's **Portability floor** step (after the container build, on the runner) fails the
job unless, on the just-built `.so`:
- max `GLIBC_` symbol version (`objdump -T ... | grep -o 'GLIBC_[0-9.]*' | sort -V | tail -1`)
  is `<= 2.28`;
- there are zero versioned `GLIBCXX_`/`CXXABI_` imports;
- `readelf -d`'s NEEDED list contains neither `libstdc++.so.6` nor `libgcc_s.so.1`;
- `nm -D --undefined-only`'s `_Unwind_`/`__cxa_`/`__gxx_` hits, EXCLUDING weak symbols (`w`,
  e.g. `__cxa_pure_virtual` — libstdc++'s own convention leaves this one optional) and symbols
  with an `@GLIBC_x.y` version tag (e.g. `__cxa_atexit@GLIBC_2.17` — legitimately provided by
  `libc.so.6` itself, present in ANY C++ binary, static or dynamic), come to zero.

The measured floor is written into the properties as `glibc.floor=<version>` (single-arch,
`jni-lib`) or `glibc.floor.linux-<arch>=<version>` per arch (multi-arch, the workflow's
`package` job and `jni-jar-multi` — both re-measure with the same `objdump` one-liner rather
than trusting a leg's own properties file).

#### The trap: "it loads for me" is not proof (fix round 3)

The first version of this wrapper mapped `-lgcc_s` to `libgcc.a` alone. `libgcc.a` does **not**
carry the unwinder (`_Unwind_*`) — that lives in **`libgcc_eh.a`** — so the resulting `.so` had
19 undefined `_Unwind_*`/`__cxa_*` symbols and no `libgcc_s.so.1` NEEDED to resolve them
dynamically either. Nothing failed the *link* (a shared object links fine with undefined
symbols by default — `-Wl,-z,defs`, added as defense in depth, catches a plain *unversioned*
undefined symbol but was measured to NOT catch this specific class: `_Unwind_*`/`__cxa_*`
references carry an explicit ELF symbol-version requirement, e.g. `_Unwind_Resume@GCC_3.0`,
and GNU ld's `-z,defs` does not treat an unresolved *versioned* reference as a hard error the
way it does a plain one — confirmed directly by relinking a throwing C++ object without
`libgcc_eh.a` and watching the link succeed anyway). The "Portability floor" step at the time
only checked glibc-symbol versions and NEEDED, so it passed too. It even passed a full Java
module-test run locally — but only because this box's own JVM already had `libgcc_s.so.1`
loaded in the *process's global symbol scope*, silently satisfying the `dlopen` at runtime. The
CI runner's Temurin JVM does not, and the identical load there threw `UnsatisfiedLinkError:
undefined symbol: _Unwind_GetTextRelBase` (hudi-rs run 34164782183,
`investigations/m3-carrier-unwinder-symbols/`).

**A native library loading successfully in one process proves only that whatever happened to
already be loaded in that process's global scope covered its gaps — never trust it as proof of
a clean, self-contained link.** The only trustworthy checks are static: `nm -D
--undefined-only <so> | grep -E '_Unwind_|__cxa_|__gxx_'` (then apply the weak/`@GLIBC_`
exclusions above) and, independently, loading the library in a process guaranteed to have
nothing preloaded (`env -i PATH=/usr/bin:/bin <jdk>/bin/java -cp ... NativeFileGroupReader
<so>` — no ambient JVM, no inherited `LD_PRELOAD`) plus `ldd <so>` showing no "not found".
Both are run as part of the local proof (`evidence/m3-t1-fix2-portable-floor.txt`, "fix round
3" section) and as workflow steps.

### Local fallback (`make jni-jar-multi-portable`, `make jni-jar-multi`, `make jni-lib-portable`)

`make jni-lib` always builds on THIS host, whichever glibc that happens to be (documented via
`glibc.floor=`, not asserted) — a normal local build is not floor-2.28. `make jni-lib-portable`
instead runs the SAME container build the workflow does (`docker run` against
`quay.io/pypa/manylinux_2_28_<arch>`, the `static-cxx-linker.sh` wrapper, `JNI_ARCH` picking
the image), staging under `target/jni-portable/` — it never touches `target/release/` or the
plain `jni-lib`'s `target/jni-native/stage/`, so it is safe to run alongside other gates on a
shared box. Needs `docker`; only builds THIS machine's arch (no cross-arch emulation).

To assemble a full multi-arch jar locally (CI-built or floor-2.28), merge in another arch's
library built elsewhere:

**Portable (floor-2.28) carrier — use this one for a release asset:**

```
make jni-jar-multi-portable JNI_EXTRA_NATIVE_DIR=/path/to/dir
```

builds this machine's arch in the `manylinux_2_28` container, asserts the portability floor, and
packages `target/jni-portable/stage` plus `JNI_EXTRA_NATIVE_DIR` into
`target/jni-portable/hudi-jni-native-<version>.jar`.

**Host build (development only — NOT floor-2.28):**

```
make jni-lib                                          # this machine's arch, into target/jni-native/stage
make jni-jar-multi JNI_EXTRA_NATIVE_DIR=/path/to/dir  # dir holds native/linux-<other-arch>/libhudi_jni.so
```

produces `target/jni-native/hudi-jni-native-<version>.jar`.

**Packaging a stage that already exists, without rebuilding anything:**

```
make jni-jar-multi JNI_JAR_MULTI_PREREQ= JNI_OUT=target/jni-portable JNI_EXTRA_NATIVE_DIR=/path/to/dir
```

`jni-jar-multi`'s build prerequisite is the variable `JNI_JAR_MULTI_PREREQ` (default `jni-lib`)
and the directory it packages is `JNI_STAGE` (default `$(JNI_OUT)/stage`). Emptying the first and
pointing `JNI_OUT` (or `JNI_STAGE`) at an existing stage packages those exact bytes. **Do not**
run plain `make jni-jar-multi` after `jni-lib-portable` expecting to get the portable library:
the `jni-lib` prerequisite would rebuild on this host *and* `rm -rf` its own stage, silently
producing a host-glibc carrier.

In all three shapes `JNI_EXTRA_NATIVE_DIR` is required — the target refuses to run without it —
and both arches end up in one classifier-less jar. `make jni-install JNI_MULTI=1` / `make
jni-deploy JNI_MULTI=1` (both depend on `jni-jar-multi`, so they still need
`JNI_EXTRA_NATIVE_DIR`) install/deploy that jar under
`io.onehouse.hudi-rs:hudi-jni-native:<version>` with no classifier.

### Multi-arch jar layout and properties keys

```
native/linux-x86_64/libhudi_jni.so
native/linux-aarch64/libhudi_jni.so
META-INF/hudi-jni-native.properties
META-INF/LICENSE             this repository's Apache-2.0 licence text
META-INF/NOTICE              project notice + the statically linked GCC runtime (libstdc++,
                             libgcc_eh, libgcc, under the GCC Runtime Library Exception)
META-INF/THIRD-PARTY.txt     every crate in hudi-jni's normal-dependency closure, with licence
```

The three `META-INF` legal files are written by `.github/jni-legal/stage-legal.sh`, called from
both `make jni-jar-multi` and the workflow's `package` job, so a locally assembled carrier and a
CI-built one carry identical attribution (F-8). `THIRD-PARTY.txt` is generated from `cargo tree
-p hudi-jni -e normal` (the accurate closure — `cargo license` has no per-package selector and
in this workspace reports every member's dependencies) joined with `cargo metadata`'s SPDX
licence fields; both ship with cargo, so nothing extra has to be installed.

```
hudi-rs.sha=<git rev-parse HEAD>
abi=<JNI_ABI_VERSION>
built=<UTC timestamp>
arch=linux-x86_64,linux-aarch64
md5.linux-x86_64=<md5 of native/linux-x86_64/libhudi_jni.so>
md5.linux-aarch64=<md5 of native/linux-aarch64/libhudi_jni.so>
glibc.floor.linux-x86_64=<max GLIBC_ symbol version in native/linux-x86_64/libhudi_jni.so>
glibc.floor.linux-aarch64=<max GLIBC_ symbol version in native/linux-aarch64/libhudi_jni.so>
stripped=true
```

`stripped=true` records D-29: the staged libraries are `strip --strip-unneeded`ed (measured
aarch64: 74,330,712 B → 55,604,776 B) and the floor script asserts both `Java_` entry points
survived in `.dynsym`.

(the single-arch jar's properties file keeps its original shape plus `glibc.floor=<version>` —
see "Make targets" below.) The `x86_64` then `aarch64` order is fixed — both the workflow's
`package` job and `jni-jar-multi` iterate the two arches in that same order (only the ones
actually present) for BOTH the `md5.linux-<arch>=` and `glibc.floor.linux-<arch>=` lines, so
they always come out identical between CI and a local build, whichever arch this machine's
`jni-lib`/`jni-lib-portable` staged first.

### Consumer side (hudi-internal)

`hudi-native-reader` resolves the classpath resource `/native/<os>-<arch>/libhudi_jni.so` out of
whichever jar is on its runtime classpath. The multi-arch jar satisfies that resolution on
either Linux arch from the SAME coordinate (no classifier), so hudi-internal's dependency
declaration and its `bot.yml`/publish-consuming workflow no longer need to pick a classifier per
runner — only the `hudi.jni.native.version` property needs to move in lockstep with
`JNI_ABI_VERSION` (see "Lockstep rule" below).

## Make targets

```
make jni-lib        # cargo build -p hudi-jni --release; strip the .so; stage it plus
                     # META-INF/hudi-jni-native.properties (incl. glibc.floor=) under
                     # target/jni-native/stage (rm -rf's the stage dir first; refuses a
                     # dirty tree unless JNI_ALLOW_DIRTY=1); builds on THIS host, whatever
                     # its glibc happens to be
make jni-lib-portable  # D-27: same build, inside quay.io/pypa/manylinux_2_28_<JNI_ARCH>
                     # (docker) with the static-cxx-linker wrapper; glibc floor <=2.28;
                     # stages under target/jni-portable, never target/release or
                     # target/jni-native/stage
make jni-jar        # package the staged tree as
                     # target/jni-native/hudi-jni-native-<version>-<os>-<arch>.jar;
                     # JNI_MULTI=1 drops the classifier (same name jni-jar-multi
                     # produces, but packaging only THIS arch's library — a naming
                     # sanity check, not a substitute for jni-jar-multi)
make jni-jar-multi  # merge in JNI_EXTRA_NATIVE_DIR (another arch's native/<os>-<arch>/
                     # libhudi_jni.so) and package $(JNI_STAGE) as ONE classifier-less
                     # $(JNI_OUT)/hudi-jni-native-<version>.jar, with the
                     # META-INF LICENSE/NOTICE/THIRD-PARTY.txt (F-8);
                     # JNI_JAR_MULTI_PREREQ= packages an EXISTING stage and
                     # builds nothing
make jni-jar-multi-portable
                     # jni-lib-portable + the same packaging against
                     # target/jni-portable/stage — the shape to use for a
                     # release asset or any jar that leaves this machine
make jni-deploy      # mvn deploy:deploy-file the jar to CodeArtifact
                     # (server id `codeartifact` in ~/.m2/settings.xml);
                     # JNI_MULTI=1 deploys the jni-jar-multi jar (no classifier)
make jni-install     # mvn install:install-file the jar into the local Maven
                     # repository (~/.m2) for builds on this machine;
                     # JNI_MULTI=1 installs the jni-jar-multi jar (no classifier)
```

`jni-jar` depends on `jni-lib`; `jni-jar-multi` depends on `$(JNI_JAR_MULTI_PREREQ)` (default
`jni-lib`, empty to package an existing stage); `jni-jar-multi-portable` depends on
`jni-lib-portable`; `jni-deploy` and `jni-install` depend on `jni-jar` (or, with `JNI_MULTI=1`,
on `jni-jar-multi` — which then also needs `JNI_EXTRA_NATIVE_DIR`). `jar` must come from a JDK
on `PATH` (e.g. `export JAVA_HOME=~/.jenv/versions/17; export PATH="$JAVA_HOME/bin:$PATH"`).

The staged properties file (`META-INF/hudi-jni-native.properties`) records the hudi-rs commit,
the JNI ABI, the build timestamp, the measured glibc floor (D-27), `stripped=true` (D-29), and
the `.so`'s md5:

```
hudi-rs.sha=<git rev-parse HEAD>
abi=<JNI_ABI_VERSION>
built=<UTC timestamp>
glibc.floor=<max GLIBC_ symbol version in the staged .so, via objdump -T>
md5=<md5 of the stripped .so>
arch=<os>-<arch>
stripped=true
```

## Version scheme

`JNI_VERSION` defaults to `0.5.0-dev.<hudi-rs short sha>` — one Maven coordinate per hudi-rs
commit that touches the library. A rebuild after any hudi-rs change is a **new** coordinate,
never an overwrite of an existing one, so a CI run always resolves the exact library its
commit was built against and nothing already published is ever silently replaced.

**Never re-tag an existing version — cut a new `-dev.<sha>` instead.** A re-run produces
different bytes even from the identical commit (`built=` is a timestamp, and the Rust build is
not bit-reproducible), so re-pushing `jni-native/<an existing version>` either fails on
CodeArtifact's immutability or, if the release asset is re-cut from it, silently invalidates the
MD5 that hudi-internal's `install-jni-carrier` action pins for that version.

## Lockstep rule

A `JNI_ABI_VERSION` bump in this crate is not a change you can land alone: it means a new
carrier version must be published, and hudi-internal's `hudi.jni.native.version` property must
be bumped to that version **in the same change** as its `REQUIRED_JNI_ABI` bump. Both sides
move together, or the ABI guard above stops the mismatched pair at load time instead of
silently misreading arguments.

The constants that must move in that same change:

| where | what |
|---|---|
| `crates/jni/src/lib.rs` | `JNI_ABI_VERSION` |
| hudi-internal `NativeFileGroupReader` | `REQUIRED_JNI_ABI` |
| hudi-internal root `pom.xml` | `hudi.jni.native.version` (and the carrier md5 in `.github/actions/install-jni-carrier/action.yml`) |
| `.github/jni-smoke/org/apache/hudi/io/nativereader/NativeFileGroupReader.java` | the hardcoded `" abi=<n>"` the stub prints |
| `.github/workflows/jni-native.yml` | the `grep -q '^SMOKE hudi-jni .* abi=<n>'` assertions in both smoke steps |

The last two are easy to miss: the workflow's smoke asserts an ABI string, so an
`JNI_ABI_VERSION` bump that forgets them fails the carrier build on the tag push, after both
legs have already compiled.

**Deploy jar and library together, never library-first.** Publishing the `.so` (`jni-deploy`)
ahead of a hudi-internal change that raises `REQUIRED_JNI_ABI` — or ahead of the corresponding
jar-side change in general — can leave CI resolving a library that doesn't match what the Java
code expects. Always land the hudi-internal property bump and the carrier deploy as one
coordinated step.
