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
- **A pre-release asset on `onehouseinc/hudi-internal`**:
  ```
  gh release create hudi-jni-native/<version> \
    target/jni-native/hudi-jni-native-<version>-linux-aarch64.jar \
    --repo onehouseinc/hudi-internal --prerelease --target <a pushed hudi-internal sha>
  ```
  hudi-internal's `bot.yml` installs this asset into the runner's local repository before its
  Maven build when the coordinate does not resolve from CodeArtifact (D-18 in the effort
  workspace). This is the fallback route if CI's OIDC assume fails (see below).

## Building the carrier

### CI (`.github/workflows/jni-native.yml`)

Triggers:
- Push a tag `jni-native/<version>` (e.g. `jni-native/0.5.0-dev.abc1234`) at the commit to
  build.
- `workflow_dispatch` (kept for when this file reaches the default branch, AS-16) with an
  optional `version` input; defaults to the tag's suffix, else `0.5.0-dev.<short sha>`.

A `build` job matrix runs BOTH Linux arches in parallel — `x86_64` on `ubuntu-24.04`,
`aarch64` on `ubuntu-24.04-arm` — each leg: `make jni-lib JNI_ARCH=<arch>`, an assertion that
the `.so` exports exactly 2 `Java_...` symbols, and a **JNI load smoke**
(`.github/jni-smoke/org/apache/hudi/io/nativereader/NativeFileGroupReader.java` — same
package/class as the real reader, only the `version()` liveness probe) that `System.load`s the
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

### Local fallback (`make jni-jar-multi`)

When CI isn't available (or to rehearse the assembly locally), build this machine's arch and
merge in another arch's library built elsewhere:

```
make jni-lib                                          # this machine's arch, into target/jni-native/stage
make jni-jar-multi JNI_EXTRA_NATIVE_DIR=/path/to/dir  # dir holds native/linux-<other-arch>/libhudi_jni.so
```

produces `target/jni-native/hudi-jni-native-<version>.jar` with both arches staged, no
classifier. `JNI_EXTRA_NATIVE_DIR` is required — the target refuses to run without it. `make
jni-install JNI_MULTI=1` / `make jni-deploy JNI_MULTI=1` (both depend on `jni-jar-multi`, so
still need `JNI_EXTRA_NATIVE_DIR`) install/deploy that jar under
`io.onehouse.hudi-rs:hudi-jni-native:<version>` with no classifier.

### Multi-arch jar layout and properties keys

```
native/linux-x86_64/libhudi_jni.so
native/linux-aarch64/libhudi_jni.so
META-INF/hudi-jni-native.properties
```

```
hudi-rs.sha=<git rev-parse HEAD>
abi=<JNI_ABI_VERSION>
built=<UTC timestamp>
arch=linux-x86_64,linux-aarch64
md5.linux-x86_64=<md5 of native/linux-x86_64/libhudi_jni.so>
md5.linux-aarch64=<md5 of native/linux-aarch64/libhudi_jni.so>
```

(the single-arch jar's properties file keeps its original shape — see "Make targets" below.)

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
                     # META-INF/hudi-jni-native.properties under target/jni-native/stage
                     # (rm -rf's the stage dir first; refuses a dirty tree unless
                     # JNI_ALLOW_DIRTY=1)
make jni-jar        # package the staged tree as
                     # target/jni-native/hudi-jni-native-<version>-<os>-<arch>.jar;
                     # JNI_MULTI=1 drops the classifier (same name jni-jar-multi
                     # produces, but packaging only THIS arch's library — a naming
                     # sanity check, not a substitute for jni-jar-multi)
make jni-jar-multi  # merge in JNI_EXTRA_NATIVE_DIR (another arch's native/<os>-<arch>/
                     # libhudi_jni.so) and package ONE classifier-less
                     # target/jni-native/hudi-jni-native-<version>.jar
make jni-deploy      # mvn deploy:deploy-file the jar to CodeArtifact
                     # (server id `codeartifact` in ~/.m2/settings.xml);
                     # JNI_MULTI=1 deploys the jni-jar-multi jar (no classifier)
make jni-install     # mvn install:install-file the jar into the local Maven
                     # repository (~/.m2) for builds on this machine;
                     # JNI_MULTI=1 installs the jni-jar-multi jar (no classifier)
```

`jni-jar` and `jni-jar-multi` depend on `jni-lib`; `jni-deploy` and `jni-install` depend on
`jni-jar` (or, with `JNI_MULTI=1`, on `jni-jar-multi` — which then also needs
`JNI_EXTRA_NATIVE_DIR`). `jar` must come from a JDK on `PATH` (e.g. `export
JAVA_HOME=~/.jenv/versions/17; export PATH="$JAVA_HOME/bin:$PATH"`).

The staged properties file (`META-INF/hudi-jni-native.properties`) records the hudi-rs commit,
the JNI ABI, the build timestamp, and the `.so`'s md5:

```
hudi-rs.sha=<git rev-parse HEAD>
abi=<JNI_ABI_VERSION>
built=<UTC timestamp>
md5=<md5 of the stripped .so>
arch=<os>-<arch>
```

## Version scheme

`JNI_VERSION` defaults to `0.5.0-dev.<hudi-rs short sha>` — one Maven coordinate per hudi-rs
commit that touches the library. A rebuild after any hudi-rs change is a **new** coordinate,
never an overwrite of an existing one, so a CI run always resolves the exact library its
commit was built against and nothing already published is ever silently replaced.

## Lockstep rule

A `JNI_ABI_VERSION` bump in this crate is not a change you can land alone: it means a new
carrier version must be published, and hudi-internal's `hudi.jni.native.version` property must
be bumped to that version **in the same change** as its `REQUIRED_JNI_ABI` bump. Both sides
move together, or the ABI guard above stops the mismatched pair at load time instead of
silently misreading arguments.

**Deploy jar and library together, never library-first.** Publishing the `.so` (`jni-deploy`)
ahead of a hudi-internal change that raises `REQUIRED_JNI_ABI` — or ahead of the corresponding
jar-side change in general — can leave CI resolving a library that doesn't match what the Java
code expects. Always land the hudi-internal property bump and the carrier deploy as one
coordinated step.
