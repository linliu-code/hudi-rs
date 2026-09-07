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

SHELL := /bin/bash

.DEFAULT_GOAL := help

VENV := .venv
PYTHON_DIR = python
MATURIN_VERSION := $(shell grep 'requires =' $(PYTHON_DIR)/pyproject.toml | cut -d= -f2- | tr -d '[ "]')
PACKAGE_VERSION := $(shell grep version Cargo.toml | head -n 1 | awk '{print $$3}' | tr -d '"' )

# Check if uv is installed (only enforced for Python-related targets)
UV_CHECK := $(shell command -v uv 2> /dev/null)
define check_uv
	@if [ -z "$(UV_CHECK)" ]; then \
		echo "Error: uv is not installed. Please install it first: curl -LsSf https://astral.sh/uv/install.sh | sh"; \
		exit 1; \
	fi
endef

# Check if cargo-tarpaulin is installed (only enforced for coverage targets)
TARPAULIN_CHECK := $(shell command -v cargo-tarpaulin 2> /dev/null)
define check_tarpaulin
	@if [ -z "$(TARPAULIN_CHECK)" ]; then \
		echo "Error: cargo-tarpaulin is not installed. Run: cargo install cargo-tarpaulin"; \
		exit 1; \
	fi
endef

# =============================================================================
# Coverage Configuration
# =============================================================================
COV_OUTPUT_DIR := ./cov-reports
COV_THRESHOLD ?= 60
# `--exclude hudi-cpp` (the PACKAGE, not just its files): cpp/src was already excluded from the
# REPORT below, but --workspace still COMPILED the crate, and on this fork that drags in
# `substrait` -> `protobuf-src`, which builds protobuf from source and exhausted the runner's disk
# (`No space left on device` installing libprotoc.a). Excluding the package changes no coverage
# number -- its sources were already out of the report -- and the crate keeps its own dedicated
# gate, the `cpp-ffi` job, which builds and tests it.
COV_EXCLUDE := \
	--exclude hudi-cpp \
	--exclude-files 'cpp/src/*' \
	--exclude-files 'crates/core/src/avro_to_arrow/*' \
	--exclude-files 'benchmark/*'
TARPAULIN_COMMON := --engine llvm --no-dead-code --no-fail-fast \
	--all-features --workspace $(COV_EXCLUDE) --skip-clean

.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: setup-venv
setup-venv: ## Setup the virtualenv
	$(call check_uv)
	$(info --- Setup virtualenv ---)
	uv venv $(VENV)

.PHONY: setup
setup: ## Setup the requirements
	$(call check_uv)
	$(info --- Setup dependencies ---)
	uv pip install "$(MATURIN_VERSION)"

.PHONY: setup-pre-commit
setup-pre-commit: ## Install pre-commit hooks for local development
	$(call check_uv)
	$(info --- Setup pre-commit hooks ---)
	uv pip install pre-commit
	pre-commit install
	pre-commit install --hook-type pre-push

.PHONY: build
build: setup ## Build Python binding of hudi-rs
	$(info --- Build Python binding ---)
	./build-wrapper.sh maturin build --features datafusion,testing $(MATURIN_EXTRA_ARGS) -m $(PYTHON_DIR)/Cargo.toml

.PHONY: develop
develop: setup ## Install Python binding of hudi-rs
	$(info --- Develop with Python binding ---)
	./build-wrapper.sh maturin develop --extras=devel,datafusion --features datafusion,testing $(MATURIN_EXTRA_ARGS) -m $(PYTHON_DIR)/Cargo.toml

.PHONY: format
format: format-rust format-python ## Format Rust and Python code

.PHONY: format-rust
format-rust: ## Format Rust code
	$(info --- Format Rust code ---)
	./build-wrapper.sh cargo fmt --all

.PHONY: format-python
format-python: ## Format Python code
	$(info --- Format Python code ---)
	ruff format $(PYTHON_DIR)

.PHONY: check
check: check-rust check-python ## Run check on Rust and Python

.PHONY: check-rust
check-rust: ## Run check on Rust
	$(info --- Check Rust clippy ---)
	./build-wrapper.sh cargo clippy --all-targets --all-features --workspace --no-deps -- -D warnings
	$(info --- Check Rust format ---)
	./build-wrapper.sh cargo fmt --all -- --check

.PHONY: check-python
check-python: ## Run check on Python
	$(info --- Check Python format ---)
	ruff format --check --diff $(PYTHON_DIR)
	$(info --- Check Python linting ---)
	ruff check $(PYTHON_DIR)
	$(info --- Check Python typing ---)
	pushd $(PYTHON_DIR); mypy .; popd

.PHONY: test
test: test-rust test-python ## Run tests on Rust and Python

.PHONY: test-rust
test-rust: ## Run tests on Rust
	$(info --- Run Rust tests ---)
	./build-wrapper.sh cargo test --no-fail-fast --all-targets --all-features --workspace

.PHONY: test-python
test-python: ## Run tests on Python
	$(call check_uv)
	$(info --- Run Python tests ---)
	uv run pytest -s $(PYTHON_DIR)

# ---- JNI library carrier (hudi-internal's hudi-native-reader resolves it from Maven) ----
JNI_VERSION ?= 0.5.0-dev.$(shell git rev-parse --short HEAD)
JNI_ARCH ?= $(shell uname -m | sed 's/arm64/aarch64/;s/amd64/x86_64/')
JNI_OS ?= linux
JNI_OUT ?= target/jni-native
JNI_STAGE := $(JNI_OUT)/stage
# internal-only default; override for other hosts
CODEARTIFACT_URL ?= https://onehouse-194159489498.d.codeartifact.us-west-2.amazonaws.com/maven/onehouse-internal/

# JNI_MULTI=1 switches jni-jar/jni-install/jni-deploy onto the classifier-less multi-arch jar
# name (the same one jni-jar-multi produces) instead of the single-arch classifier jar; it does
# NOT by itself merge in another arch's library — that's jni-jar-multi's JNI_EXTRA_NATIVE_DIR job,
# which jni-install/jni-deploy pull in as their prerequisite under JNI_MULTI=1.
ifeq ($(JNI_MULTI),1)
JNI_JAR := $(JNI_OUT)/hudi-jni-native-$(JNI_VERSION).jar
JNI_DEPLOY_PREREQ := jni-jar-multi
JNI_CLASSIFIER_ARG :=
JNI_DEPLOY_COORD := io.onehouse.hudi-rs:hudi-jni-native:$(JNI_VERSION)
else
JNI_JAR := $(JNI_OUT)/hudi-jni-native-$(JNI_VERSION)-$(JNI_OS)-$(JNI_ARCH).jar
JNI_DEPLOY_PREREQ := jni-jar
JNI_CLASSIFIER_ARG := -Dclassifier=$(JNI_OS)-$(JNI_ARCH)
JNI_DEPLOY_COORD := io.onehouse.hudi-rs:hudi-jni-native:$(JNI_VERSION):$(JNI_OS)-$(JNI_ARCH)
endif

.PHONY: jni-lib
jni-lib: ## Build libhudi_jni.so (release) and stage a stripped copy under target/jni-native (refuses a dirty tree; JNI_ALLOW_DIRTY=1 overrides)
	$(info --- Build hudi-jni (release) ---)
	test -z "$$(git status --porcelain)" || { echo "dirty tree; set JNI_ALLOW_DIRTY=1 to override"; test -n "$(JNI_ALLOW_DIRTY)"; }
	rm -rf $(JNI_STAGE)
	./build-wrapper.sh cargo build -p hudi-jni --release
	mkdir -p $(JNI_STAGE)/native/$(JNI_OS)-$(JNI_ARCH) $(JNI_STAGE)/META-INF
	strip -o $(JNI_STAGE)/native/$(JNI_OS)-$(JNI_ARCH)/libhudi_jni.so target/release/libhudi_jni.so
	printf 'hudi-rs.sha=%s\nabi=%s\nbuilt=%s\nmd5=%s\narch=%s-%s\n' \
	  "$$(git rev-parse HEAD)" \
	  "$$(grep -o 'JNI_ABI_VERSION: u32 = [0-9]*' crates/jni/src/lib.rs | grep -o '[0-9]*$$')" \
	  "$$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	  "$$(md5sum $(JNI_STAGE)/native/$(JNI_OS)-$(JNI_ARCH)/libhudi_jni.so | cut -d' ' -f1)" \
	  "$(JNI_OS)" "$(JNI_ARCH)" > $(JNI_STAGE)/META-INF/hudi-jni-native.properties
	cat $(JNI_STAGE)/META-INF/hudi-jni-native.properties

.PHONY: jni-jar
jni-jar: jni-lib ## Package the staged library as hudi-jni-native-<version>-<os>-<arch>.jar (JNI_MULTI=1 drops the classifier, matching jni-jar-multi's name)
	$(info --- Package $(JNI_JAR) ---)
	rm -f $(JNI_JAR) && jar cf $(JNI_JAR) -C $(JNI_STAGE) .
	unzip -l $(JNI_JAR)

.PHONY: jni-jar-multi
jni-jar-multi: jni-lib ## Package this arch's library plus JNI_EXTRA_NATIVE_DIR (native/<os>-<arch>/libhudi_jni.so from other legs) as ONE jar
	test -n "$(JNI_EXTRA_NATIVE_DIR)" || { echo "JNI_EXTRA_NATIVE_DIR is required"; exit 2; }
	cp -r $(JNI_EXTRA_NATIVE_DIR)/native/. $(JNI_STAGE)/native/
	printf 'hudi-rs.sha=%s\nabi=%s\nbuilt=%s\narch=%s\n' "$$(git rev-parse HEAD)" \
	  "$$(grep -o 'JNI_ABI_VERSION: u32 = [0-9]*' crates/jni/src/lib.rs | grep -o '[0-9]*$$')" \
	  "$$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$$(ls $(JNI_STAGE)/native | paste -sd,)" > $(JNI_STAGE)/META-INF/hudi-jni-native.properties
	for d in $(JNI_STAGE)/native/*; do printf 'md5.%s=%s\n' "$$(basename $$d)" "$$(md5sum $$d/libhudi_jni.so | cut -d' ' -f1)" >> $(JNI_STAGE)/META-INF/hudi-jni-native.properties; done
	rm -f $(JNI_OUT)/hudi-jni-native-$(JNI_VERSION).jar && jar cf $(JNI_OUT)/hudi-jni-native-$(JNI_VERSION).jar -C $(JNI_STAGE) . && unzip -l $(JNI_OUT)/hudi-jni-native-$(JNI_VERSION).jar

.PHONY: jni-deploy
jni-deploy: $(JNI_DEPLOY_PREREQ) ## Deploy the carrier jar to CodeArtifact (server id `codeartifact` in ~/.m2/settings.xml); JNI_MULTI=1 deploys the classifier-less multi-arch jar (needs JNI_EXTRA_NATIVE_DIR)
	$(info --- Deploy $(JNI_JAR) as $(JNI_DEPLOY_COORD) ---)
	mvn -B -ntp deploy:deploy-file -Dfile=$(JNI_JAR) -DgroupId=io.onehouse.hudi-rs -DartifactId=hudi-jni-native \
	  -Dversion=$(JNI_VERSION) $(JNI_CLASSIFIER_ARG) -Dpackaging=jar -DgeneratePom=true \
	  -DrepositoryId=codeartifact -Durl=$(CODEARTIFACT_URL)

.PHONY: jni-install
jni-install: $(JNI_DEPLOY_PREREQ) ## Install the carrier jar into the local Maven repository (~/.m2) for builds on this machine; JNI_MULTI=1 installs the classifier-less multi-arch jar (needs JNI_EXTRA_NATIVE_DIR)
	$(info --- Install $(JNI_JAR) into the local Maven repository as $(JNI_DEPLOY_COORD) ---)
	mvn -B -ntp install:install-file -Dfile=$(JNI_JAR) -DgroupId=io.onehouse.hudi-rs -DartifactId=hudi-jni-native \
	  -Dversion=$(JNI_VERSION) $(JNI_CLASSIFIER_ARG) -Dpackaging=jar -DgeneratePom=true

.PHONY: coverage
coverage: coverage-rust ## Generate coverage report (alias for coverage-rust)

.PHONY: coverage-rust
coverage-rust: ## Generate HTML coverage report for Rust
	$(call check_tarpaulin)
	@mkdir -p $(COV_OUTPUT_DIR)
	./build-wrapper.sh cargo tarpaulin $(TARPAULIN_COMMON) \
		-o Html --output-dir $(COV_OUTPUT_DIR)
	@echo "Coverage report generated at $(COV_OUTPUT_DIR)/tarpaulin-report.html"

.PHONY: coverage-xml
coverage-xml: ## Generate XML coverage report for Rust (CI format)
	$(call check_tarpaulin)
	@mkdir -p $(COV_OUTPUT_DIR)
	./build-wrapper.sh cargo tarpaulin $(TARPAULIN_COMMON) \
		-o xml --output-dir $(COV_OUTPUT_DIR)

.PHONY: coverage-open
coverage-open: coverage-rust ## Generate and open HTML coverage report in browser
	@command -v open >/dev/null 2>&1 && open $(COV_OUTPUT_DIR)/tarpaulin-report.html || \
	 command -v xdg-open >/dev/null 2>&1 && xdg-open $(COV_OUTPUT_DIR)/tarpaulin-report.html || \
	 echo "Open $(COV_OUTPUT_DIR)/tarpaulin-report.html manually"

.PHONY: coverage-check
coverage-check: ## Fail if coverage is below threshold (COV_THRESHOLD=60)
	$(call check_tarpaulin)
	./build-wrapper.sh cargo tarpaulin $(TARPAULIN_COMMON) \
		--fail-under $(COV_THRESHOLD)

.PHONY: clean-coverage
clean-coverage: ## Remove coverage reports
	rm -rf $(COV_OUTPUT_DIR)

# =============================================================================
# TPC-H Benchmark
# =============================================================================
SF ?= 0.001
ENGINE ?= datafusion
FORMAT ?= hudi
QUERIES ?=
HUDI_DIR ?=
PARQUET_DIR ?=
TPCH_DIR := benchmark/tpch
TPCH_DATA_DIR := $(TPCH_DIR)/data
TPCH_RESULTS_DIR := $(TPCH_DIR)/results

.PHONY: tpch-generate
tpch-generate: ## Generate TPC-H parquet tables (SF=0.001)
	$(info --- Generate TPC-H parquet tables at SF=$(SF) ---)
	$(TPCH_DIR)/run.sh generate --scale-factor $(SF)

.PHONY: tpch-create-tables
tpch-create-tables: ## Create Hudi COW tables from parquet (SF=0.001, requires Spark)
	$(info --- Create Hudi tables at SF=$(SF) ---)
	$(TPCH_DIR)/run.sh create-tables --scale-factor $(SF)

.PHONY: bench-tpch
bench-tpch: ## Run TPC-H benchmark (ENGINE=datafusion|spark SF=0.001 QUERIES=1,3,6 HUDI_DIR=gs://...)
	$(info --- Benchmark at SF=$(SF) ---)
ifeq ($(ENGINE),spark)
	$(TPCH_DIR)/run.sh bench-spark --scale-factor $(SF) --format $(FORMAT) $(if $(QUERIES),--queries $(QUERIES)) $(if $(HUDI_DIR),--hudi-dir $(HUDI_DIR)) $(if $(PARQUET_DIR),--parquet-dir $(PARQUET_DIR)) --output-dir $(TPCH_RESULTS_DIR)
else ifeq ($(ENGINE),datafusion)
	$(TPCH_DIR)/run.sh bench-datafusion --scale-factor $(SF) --format $(FORMAT) $(if $(QUERIES),--queries $(QUERIES)) $(if $(HUDI_DIR),--hudi-dir $(HUDI_DIR)) $(if $(PARQUET_DIR),--parquet-dir $(PARQUET_DIR)) --output-dir $(TPCH_RESULTS_DIR)
else
	$(error Unknown ENGINE=$(ENGINE). Use datafusion or spark)
endif

.PHONY: tpch-compare
tpch-compare: ## Compare persisted TPC-H benchmark results (ENGINES=datafusion,spark SF=0.001)
	$(TPCH_DIR)/run.sh compare --scale-factor $(SF) --engines $(ENGINES) --format $(FORMAT)
