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
# Builds and stages libhudi_jni.so INSIDE a quay.io/pypa/manylinux_2_28_<arch> container
# (AlmaLinux 8, glibc 2.28), so the library's glibc floor is 2.28 whatever the host runs.
#
# ONE implementation, called from BOTH `make jni-lib-portable` and the `build` job in
# `.github/workflows/jni-native.yml`, so a local portable build and a CI leg install the same
# toolchain and run the same build.
#
# Usage, from the repository root mounted as the container's working directory:
#
#     container-build.sh <arch> <JNI_OUT> <JNI_CARGO_TARGET_DIR>
#
# CARGO_BUILD_JOBS, when set in the container's environment, bounds the build's parallelism.
set -euo pipefail

ARCH=${1:?usage: container-build.sh <arch> <JNI_OUT> <JNI_CARGO_TARGET_DIR>}
OUT=${2:?usage: container-build.sh <arch> <JNI_OUT> <JNI_CARGO_TARGET_DIR>}
CARGO_TARGET=${3:?usage: container-build.sh <arch> <JNI_OUT> <JNI_CARGO_TARGET_DIR>}

[ "$(uname -m)" = "$ARCH" ] || { echo "FAIL: this container is $(uname -m), asked to build $ARCH"; exit 1; }

git config --global --add safe.directory "$PWD"
dnf install -y -q clang clang-devel

# AlmaLinux 8's protobuf-compiler is 3.5.0, which predates --experimental_allow_proto3_optional
# (needed by lance-encoding/lance-file), so a protoc release is installed instead. The version
# and checksums are the ones .github/scripts/manylinux-build-deps.sh pins for the wheels; keep
# the two in step. The library this builds is published, so the archive is verified rather than
# trusted, and `curl -f` turns an HTTP error into a failure here instead of a bad zip later.
PROTOC_VERSION=36.1
case "$ARCH" in
  x86_64)
    protoc_arch=x86_64
    protoc_sha256=c4bc672d9d49214dc8cafdceadf4df92182d6ca8e3ec65a56b2d7de5602669b4
    ;;
  aarch64)
    protoc_arch=aarch_64
    protoc_sha256=237a68856edf1bd28b6204bddd0596c1cf46d298bc29c620012540b2e44c73e7
    ;;
  *)
    echo "FAIL: unsupported architecture: $ARCH"
    exit 1
    ;;
esac
curl -fsSL -o /tmp/protoc.zip \
  "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip"
echo "${protoc_sha256}  /tmp/protoc.zip" | sha256sum -c -
unzip -qo /tmp/protoc.zip -d /usr/local bin/protoc 'include/*'
chmod +x /usr/local/bin/protoc
protoc --version

curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --default-toolchain none
export PATH="$HOME/.cargo/bin:$PATH"

# librocksdb-sys emits an explicit dynamic -lstdc++/-lgcc_s that -C link-arg=-static-libstdc++ and
# -static-libgcc do NOT remove (measured; see static-cxx-linker.sh for why), so they are forced
# static in place by a linker wrapper.
cp .github/jni-portable/static-cxx-linker.sh /usr/local/bin/static-cxx-linker.sh
chmod +x /usr/local/bin/static-cxx-linker.sh
REAL_CC=$(command -v gcc)
export REAL_CC
export RUSTFLAGS="-C linker=/usr/local/bin/static-cxx-linker.sh"

# JNI_ALLOW_DIRTY=1: jni-lib refuses a dirty tree so a local build cannot silently be made from
# uncommitted code, but this container writes under the mounted tree before jni-lib runs its own
# check, which `git status --porcelain` would see.
make jni-lib JNI_ARCH="$ARCH" JNI_ALLOW_DIRTY=1 JNI_OUT="$OUT" JNI_CARGO_TARGET_DIR="$CARGO_TARGET"

# The container runs as root; hand what it wrote back to the owner of the checkout so later steps
# (and the host user, locally) can read and remove it.
chown -R --reference=Makefile "$OUT" "$CARGO_TARGET"
