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

The Maven coordinates (`io.onehouse.hudi-rs:hudi-jni-native:<version>:<os>-<arch>`) are the same
regardless of where the jar comes from — only the repository it resolves from changes:

- **CodeArtifact**, via `make jni-deploy` (needs a fresh `codeartifact` token in
  `~/.m2/settings.xml`).
- **This machine's local Maven repository**, via `make jni-install`.
- **A pre-release asset on `onehouseinc/hudi-internal`**:
  ```
  gh release create hudi-jni-native/<version> \
    target/jni-native/hudi-jni-native-<version>-linux-aarch64.jar \
    --repo onehouseinc/hudi-internal --prerelease --target <a pushed hudi-internal sha>
  ```
  hudi-internal's `bot.yml` installs this asset into the runner's local repository before its
  Maven build when the coordinate does not resolve from CodeArtifact (D-18 in the effort
  workspace).

## Make targets

```
make jni-lib     # cargo build -p hudi-jni --release; strip the .so; stage it plus
                  # META-INF/hudi-jni-native.properties under target/jni-native/stage
make jni-jar     # package the staged tree as
                  # target/jni-native/hudi-jni-native-<version>-<os>-<arch>.jar
make jni-deploy  # mvn deploy:deploy-file the jar to CodeArtifact
                  # (server id `codeartifact` in ~/.m2/settings.xml)
make jni-install # mvn install:install-file the jar into the local Maven
                  # repository (~/.m2) for builds on this machine
```

`jni-jar` depends on `jni-lib`; `jni-deploy` and `jni-install` depend on `jni-jar`. `jar` must
come from a JDK on `PATH` (e.g. `export JAVA_HOME=~/.jenv/versions/17;
export PATH="$JAVA_HOME/bin:$PATH"`).

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
