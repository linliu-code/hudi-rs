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

`JNI_ABI_VERSION` (`crates/jni/src/lib.rs`) is the revision of the `Java_…` export contract.
The Java side (`NativeFileGroupReader.REQUIRED_JNI_ABI`, `NativeFileGroupReader.java:56,91`)
refuses to load a library that reports a lower revision, turning a stale `libhudi_jni.so`
under an up-to-date jar into a load-time error instead of a mis-read argument slot. The check
is one-directional (`abi < REQUIRED_JNI_ABI`): it protects a jar that is upgraded first, never
one that lags behind — **never deploy the library ahead of the jar; ship jar and library
together.**

### When to bump it

This is the rule, stated once; the history entries on the constant in `crates/jni/src/lib.rs`
record which change triggered which revision.

**Bump when either holds:**

1. **The parameter list of any exported `Java_…` function changes** — arity, order, or types.
   A jar built against the old list would otherwise read the wrong argument slots.
2. **The change makes some old-jar/new-library or new-jar/old-library pairing unsafe** — i.e.
   that pairing would produce *wrong results*, or an *unrecoverable failure where the same
   input previously worked*. The parameter list can be untouched and this can still hold:
   revision 3 was bumped for exactly that shape (an empty `lookupKeys` array changed from
   "whole slice" to "match nothing", D-12; an empty `latestInstant` went from "read
   everything" to refused, D-13), because a jar still holding the old meanings would have
   mis-read a lookup against the new library.

**Do not bump when** a change only turns a *failure* into a *correct result*, and every input
that previously succeeded produces *byte-identical* output. Nothing a jar can be holding is
made unsafe by that: the old jar gets strictly more working reads and identical bytes on the
rest, and the old library gets exactly the behaviour it always had. Bumping there would break
a deployment that works, in exchange for no protection.

#### Worked example — `638ce3b`, which did **not** bump (OI-58 / RV-4)

`638ce3b` ("resolve an older HFile's Avro schema up to the reader schema, as Java does")
changed what the native side does with the *same* `dataSchemaJson` argument. Before it, the
string only produced an Arrow projection target and was then discarded (`reader_schema_json`
stayed `None`); after it, the same string is also retained in Avro form and becomes the reader
schema at decode time for the base HFile and every log block, with `required_schema` replaced
by a UTC-normalised derivation (`crates/jvm-ffi/src/file_group_v2.rs:249-263`). That is a
changed *meaning* of an input, so rule 2 is the one to check — and it does not fire, in either
direction:

- **Old jar (pre-`638ce3b`) + new library.** For a table whose writer schema differs from the
  reader schema (a v6 `record_index` HFile under the current `HoodieMetadataRecord`), the read
  previously threw and now returns the correct rows — an error becoming a correct result. For
  a table whose writer schema *equals* the reader schema, the output is byte-identical, values
  and schema alike; that is not an argument but a test:
  `v8_record_index_read_is_unchanged_when_the_writer_schema_is_the_reader_schema`
  (`crates/jvm-ffi/tests/file_group_v2_tests.rs:2088`) reads every v8 `record_index` shard both
  with the file's own schema as the requested schema and with none at all, and asserts the two
  batches are equal. No previously-succeeding input changes.
- **New jar + old library.** The v6 case keeps the pre-fix loud failure — unchanged, not newly
  broken — and the v8 case is unchanged. Nothing regresses relative to what that pairing
  already did.
- **No caller relied on the old meaning.** `HoodieBackedTableMetadata` is the only caller of
  this export and always passes the same constant `SCHEMA`, so nothing depended on
  `dataSchemaJson` being discarded after projection.

So `638ce3b` sits squarely in the "do not bump" case, and `JNI_ABI_VERSION` stays at 3.

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

- **CodeArtifact**, via `make jni-deploy` — a single-arch
  `io.onehouse.hudi-rs:hudi-jni-native:<version>:<os>-<arch>` jar, or with `JNI_MULTI=1` the
  classifier-less `io.onehouse.hudi-rs:hudi-jni-native:<version>` jar carrying BOTH Linux arches
  (needs a fresh `codeartifact` token in `~/.m2/settings.xml`). CI (`jni-native.yml`, see
  "Building the carrier" below) builds that multi-arch jar but does not publish it: it uploads
  it as the `hudi-jni-native-carrier` workflow artifact.
- **This machine's local Maven repository**, via `make jni-install` (single-arch,
  classifier) or `make jni-install JNI_MULTI=1` (multi-arch, no classifier).
- **A pre-release asset on `onehouseinc/hudi-internal`**. hudi-internal's
  `.github/actions/install-jni-carrier` installs this asset into the runner's local repository
  when the coordinate does not resolve from CodeArtifact.

  **The asset MUST be the multi-arch, classifier-less jar, uploaded byte-for-byte as built; the
  action pins that jar's own md5.**
  The consuming action runs `install:install-file` with **no** `-Dclassifier`, so whatever bytes
  it downloads become `io.onehouse.hudi-rs:hudi-jni-native:<version>`, the coordinate every
  module and all 14 bundles resolve. Publishing a single-arch `-linux-aarch64` jar there would
  seed every runner with an aarch64-only library under the multi-arch coordinate: aarch64 passes,
  x86_64 builds succeed and then fail the bundle smoke (no `native/linux-x86_64/…`), and any
  x86_64 deployment built from it fails loud at lookup time, far from the cause. The action pins
  the asset's MD5, which is what ties a version to exactly one jar.

  Take the jar from the workflow run's own `hudi-jni-native-carrier` artifact — never rebuilt,
  never re-jarred — and upload it as-is:
  ```
  gh run download <run-id> -R onehouseinc/hudi-rs-internal -n hudi-jni-native-carrier -D /tmp/carrier
  (cd /tmp/carrier && md5sum -c hudi-jni-native-<version>.jar.md5)   # checks the md5 the workflow recorded
  cut -d' ' -f1 /tmp/carrier/hudi-jni-native-<version>.jar.md5        # the value to pin
  gh release create hudi-jni-native/<version> \
    /tmp/carrier/hudi-jni-native-<version>.jar \
    --repo onehouseinc/hudi-internal --prerelease --target <a pushed hudi-internal sha>
  ```
  Then set that md5 in `install-jni-carrier/action.yml`. If CI is unavailable altogether, build
  the jar locally with `make jni-jar-multi-portable` (below) — the portable build, never
  `jni-lib`'s host build — and merge in the other arch's `.so`; a jar carrying only this
  machine's arch must not be uploaded. That jar is uploaded as-is too, and the md5 to pin is
  `md5sum` of that jar.

### Published carriers

Recorded here rather than derived, because a published coordinate is **history**: it names bytes
that already exist in CodeArtifact and must not be rewritten by a version bump. This file is the
one place the single-source check exempts for exactly that reason.

| coordinate | cut at | notes |
|---|---|---|
| `io.onehouse.hudi-rs:hudi-jni-native:0.6.0-dev.b3adac9` | `b3adac9`, tag `jni-native/0.6.0-dev.b3adac9` | **the `0.6.0-dev` carrier to pin** — and read the scope note below before assuming what it contains. |
| `io.onehouse.hudi-rs:hudi-jni-native:0.6.0-dev.856dca2` | `856dca2`, tag `jni-native/0.6.0-dev.856dca2` | superseded — an interior commit of the same branch. Do not pin. |

Two `0.6.0-dev` carriers exist and the tag list alone does not say which is current, so the row
above is the answer rather than an invitation to guess. `b3adac9` is no longer the tip of any
branch (the `m16` restack moved that branch); the commit is preserved by its tag and the published
jar is immutable, so the coordinate remains correct.

> **Scope — what `…b3adac9` does NOT contain.** It is the fork point plus this branch's CI-only
> work. It is **not** a carrier for the delivery stack's native code:
>
> ```
> git merge-base --is-ancestor 919e730 b3adac9   # -> 1 (does NOT contain the m7.3 port landing)
> git merge-base --is-ancestor f67db83 b3adac9   # -> 1 (does NOT contain the reader_v2 / provider work)
> ```
>
> Concretely, `cpp/src/cache_abi.rs` and `served_batch_stream` do not exist at `b3adac9`, so none of
> the base-file-provider ABI or its correctness fixes are in those bytes. Pin this coordinate when
> you need *a* `0.6.0-dev` carrier; do **not** pin it expecting the composed stack's native code.
>
> **There is currently no carrier for the composed tip, by design.** `ALLOWED_BRANCHES` lists only
> `main` and `davis/rli-native-hfile-read`, and the stack's tip is an ancestor of neither, so a
> `jni-native/*` tag there is refused by the `guard` job. That is the control working as intended —
> cutting one means landing the chain on `main` first, or a deliberate, temporary, per-branch
> widening of the kind this file's `ALLOWED_BRANCHES` comment warns about.

## Building the carrier

### CI (`.github/workflows/jni-native.yml`)

Triggers:
- Push a tag `jni-native/<version>` (e.g. `jni-native/<x.y.z>-dev.abc1234`) at the commit to
  build.
- `workflow_dispatch` (kept for when this file reaches the default branch) with an optional
  `version` input; defaults to the tag's suffix, else `<x.y.z>-dev.<short sha>` — see "Version
  scheme" below.

The workflow builds, checks and packages the carrier. It publishes nothing: its output is the
`hudi-jni-native-carrier` workflow artifact.

A `build` job matrix runs BOTH Linux arches in parallel — `x86_64` on `ubuntu-24.04`,
`aarch64` on `ubuntu-24.04-arm` — each leg builds INSIDE a `manylinux_2_28_<arch>` container
(D-27, below) with `.github/jni-portable/container-build.sh`, asserts the `.so` exports exactly
2 `Java_...` symbols, asserts the portability floor, and runs the **JNI load smoke** twice: once
on the runner itself and once inside an
`eclipse-temurin:17-jdk-jammy` container (glibc 2.35, older than the runner's own glibc 2.39) —
both via `.github/jni-smoke/org/apache/hudi/io/nativereader/NativeFileGroupReader.java` (same
package/class as the real reader, only the `version()` liveness probe), which `System.load`s the
just-built library and checks its output matches `SMOKE hudi-jni .* abi=3`. Each leg uploads its
`.so` plus properties as a workflow artifact (`libhudi_jni-linux-<arch>`).

A `package` job (needs both legs) downloads both artifacts, re-asserts the portability floor on
each library it is about to package, assembles ONE classifier-less jar (layout below) and
uploads it as a workflow artifact (`hudi-jni-native-carrier`).

### The glibc floor (D-27)

A Rust cdylib links the versioned glibc symbols of the BUILD host's libc, so a library built
directly on the `ubuntu-24.04`/`ubuntu-24.04-arm` runners (glibc 2.39) cannot load on any older
host — Amazon Linux 2023 (2.34), RHEL/Alma 8 (2.28), or even this box (2.35). The first cut of
this workflow did exactly that and failed to load anywhere but the runners themselves. Fix:
each leg's `make jni-lib` now runs inside `quay.io/pypa/manylinux_2_28_<arch>` (AlmaLinux 8,
glibc 2.28), which pins the *glibc* floor to 2.28 for free. The *libstdc++*/*libgcc* floor
(from `librocksdb-sys`, a C++ dependency) is a
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
- every soname in `readelf -d`'s NEEDED list is on an ALLOW-list — `libc.so.6`, `libm.so.6`,
  `libdl.so.2`, `libpthread.so.0`, `librt.so.1` and `ld-linux-*.so.*` (`JNI_ALLOWED_NEEDED` in
  `.github/jni-portable/portability-floor.sh`). Any other dynamic dependency, `libstdc++.so.6`
  and `libgcc_s.so.1` included, fails the step: link it statically, or add it to that list
  together with the reason every deployment host will have it;
- `nm -D --undefined-only`'s `_Unwind_`/`__cxa_`/`__gxx_` hits, EXCLUDING weak symbols (`w`,
  e.g. `__cxa_pure_virtual` — libstdc++'s own convention leaves this one optional) and symbols
  with an `@GLIBC_x.y` version tag (e.g. `__cxa_atexit@GLIBC_2.17` — legitimately provided by
  `libc.so.6` itself, present in ANY C++ binary, static or dynamic), come to zero;
- exactly two `Java_` symbols are exported from `.dynsym`;
- the library is stripped (no `.symtab` section).

The checks are `.github/jni-portable/portability-floor.sh`, the one script the workflow's build
and package jobs and `make jni-lib-portable` all run.

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
undefined symbol: _Unwind_GetTextRelBase`.

**A native library loading successfully in one process proves only that whatever happened to
already be loaded in that process's global scope covered its gaps — never trust it as proof of
a clean, self-contained link.** The only trustworthy checks are static: `nm -D
--undefined-only <so> | grep -E '_Unwind_|__cxa_|__gxx_'` (then apply the weak/`@GLIBC_`
exclusions above) and, independently, loading the library in a process guaranteed to have
nothing preloaded (`env -i PATH=/usr/bin:/bin <jdk>/bin/java -cp ... NativeFileGroupReader
<so>` — no ambient JVM, no inherited `LD_PRELOAD`) plus `ldd <so>` showing no "not found".
The static check is part of the portability floor every build asserts; the clean-process load is
the way to confirm a library by hand.

### Local fallback (`make jni-jar-multi-portable`, `make jni-jar-multi`, `make jni-lib-portable`)

`make jni-lib` always builds on THIS host, whichever glibc that happens to be (documented via
`glibc.floor=`, not asserted) — a normal local build is not floor-2.28. `make jni-lib-portable`
instead runs the SAME container build the workflow does (`docker run` of
`.github/jni-portable/container-build.sh` in `quay.io/pypa/manylinux_2_28_<arch>`, with the
`static-cxx-linker.sh` wrapper, `JNI_ARCH` picking the image; `CARGO_BUILD_JOBS` is passed into
the container when set), staging under `target/jni-portable/` — it never touches
`target/release/` or the plain `jni-lib`'s `target/jni-native/stage/`, so it is safe to run
alongside other gates on a shared box. Needs `docker`; only builds THIS machine's arch (no
cross-arch emulation).

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

In all three shapes `JNI_EXTRA_NATIVE_DIR` is required and must contain a `native/` directory — the
target refuses to run otherwise —
and both arches end up in one classifier-less jar. For each arch the library comes from
`JNI_EXTRA_NATIVE_DIR` if it has one (replacing the staged one), else from the stage. Before copying
or writing anything the target refuses unless both `native/linux-x86_64/libhudi_jni.so` and
`native/linux-aarch64/libhudi_jni.so` are present, each stripped and each exporting the two `Java_`
symbols; a jar missing an arch would otherwise publish a one-arch library under the multi-arch
coordinate. `make jni-install JNI_MULTI=1` / `make jni-deploy
JNI_MULTI=1` (both depend on `jni-jar-multi`, so they still need `JNI_EXTRA_NATIVE_DIR`)
install/deploy that jar under `io.onehouse.hudi-rs:hudi-jni-native:<version>` with no
classifier.

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
aarch64: 74,330,712 B → 55,604,776 B). It is a claim about bytes the packaging step may only
have copied, so it is checked where it is written: the workflow's `package` job runs the floor
script (stripped, both `Java_` entry points in `.dynsym`) on each library, and `jni-jar-multi`
makes the same two checks.

(the single-arch jar's properties file keeps its original shape plus `glibc.floor=<version>` —
see "Make targets" below.) The `x86_64` then `aarch64` order is fixed — both the workflow's
`package` job and `jni-jar-multi` iterate the two arches in that same order for BOTH the
`md5.linux-<arch>=` and `glibc.floor.linux-<arch>=` lines, so they always come out identical
between CI and a local build, whichever arch this machine's `jni-lib`/`jni-lib-portable` staged
first.

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
                     # target/jni-native/stage (replaces the stage dir once the build
                     # succeeds; refuses a dirty tree unless JNI_ALLOW_DIRTY=1); builds
                     # on THIS host, whatever its glibc happens to be
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

`JNI_VERSION` defaults to `<x.y.z>-dev.<hudi-rs short sha>`, where `<x.y.z>` is
`[workspace.package] version` in the root `Cargo.toml` (read by
`.github/scripts/workspace-version.sh`) without any pre-release suffix — the same default the
workflow computes, and never a release-looking `<x.y.z>.<sha>`. If the version cannot be read,
the JNI targets stop rather than guess; pass `JNI_VERSION=...` to override. That is one Maven
coordinate per hudi-rs commit that touches the library. A rebuild after any hudi-rs change is a
**new** coordinate, never an overwrite of an existing one, so a CI run always resolves the exact
library its commit was built against and nothing already published is ever silently replaced.

**Never re-tag an existing version — cut a new `-dev.<sha>` instead.** A re-run produces
different bytes even from the identical commit (`built=` is a timestamp, and the Rust build is
not bit-reproducible), so rebuilding `jni-native/<an existing version>` yields a second jar under
the same version: deploying it fails on CodeArtifact's immutability and, if the release asset is
re-cut from it, it silently invalidates the MD5 that hudi-internal's `install-jni-carrier` action
pins for that version.

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
