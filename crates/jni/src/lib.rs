/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! JNI exports for `org.apache.hudi.io.nativereader.NativeFileGroupReader`.
//!
//! Marshalling only: Java strings in, an Arrow C stream written into a
//! Java-allocated `ArrowArrayStream` out. The read itself is
//! `hudi_jvm_ffi::file_group_v2`. Every export catches panics: a panic that
//! unwinds into the JVM aborts the process, so failures are turned into a
//! thrown `NativeReaderException` instead.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use hudi_jvm_ffi::file_group_v2::{FileGroupRequest, export_file_group_stream_v2};
use jni::JNIEnv;
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::{jboolean, jlong, jstring};

const EXCEPTION_CLASS: &str = "org/apache/hudi/io/nativereader/NativeReaderException";

static LOGGER: OnceLock<()> = OnceLock::new();

fn init_logger() {
    LOGGER.get_or_init(|| {
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .try_init();
    });
}

fn jstring_to_string(env: &mut JNIEnv, value: &JString, what: &str) -> Result<String, String> {
    if value.as_raw().is_null() {
        return Err(format!("{what} is null"));
    }
    env.get_string(value)
        .map(|s| s.into())
        .map_err(|e| format!("{what}: not a valid Java string: {e}"))
}

fn jstring_array_to_vec(
    env: &mut JNIEnv,
    array: &JObjectArray,
    what: &str,
) -> Result<Vec<String>, String> {
    if array.as_raw().is_null() {
        // Null means absent (e.g. no log files for a base-file-only slice, or no lookup-keys /
        // valid-instants filter), not an error.
        return Ok(Vec::new());
    }
    let len = env
        .get_array_length(array)
        .map_err(|e| format!("{what}: cannot read array length: {e}"))?;
    let mut out = Vec::with_capacity(len as usize);
    for i in 0..len {
        // `get_object_array_element` allocates a local ref for the element,
        // and `get_string` allocates two more of its own internally
        // (`find_class("java/lang/String")` and `get_object_class`) — none of
        // `JObject`/`JString`/`JClass` implement `Drop` in jni-0.21.1, so
        // without a frame every one of those refs would stay live for the
        // rest of the call. Running the whole per-element read inside
        // `with_local_frame` pops all of them, on both the success and the
        // error path, keeping live refs bounded regardless of array length.
        let value = env
            .with_local_frame(4, |env| -> Result<String, jni::errors::Error> {
                let element = env.get_object_array_element(array, i)?;
                let element = JString::from(element);
                env.get_string(&element).map(String::from)
            })
            .map_err(|e| format!("{what}[{i}]: not a valid Java string: {e}"))?;
        out.push(value);
    }
    Ok(out)
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "hudi-jni panicked".to_string())
}

fn throw(env: &mut JNIEnv, message: String) {
    log::error!("[hudi-rs-jni] {message}");
    // If an exception is already pending (e.g. from a failed JNI call), leave it.
    if matches!(env.exception_check(), Ok(true)) {
        return;
    }
    if let Err(e) = env.throw_new(EXCEPTION_CLASS, message) {
        log::error!("[hudi-rs-jni] failed to throw NativeReaderException: {e}");
    }
}

/// Reads one file group through reader_v2 into the Java-allocated stream at
/// `stream_address`. Throws `NativeReaderException` on any failure.
///
/// `dataSchemaJson` is the Avro JSON of the table's data schema, the way Java's
/// own `HoodieFileGroupReader` is always given one. It is optional: a null Java
/// string is "no schema" rather than an error, and the engine then infers the
/// output schema from the slice — which cannot work for a log-only slice.
///
/// `lookupKeys` is Java's `Predicates.in` keys, or key prefixes when
/// `lookupKeysArePrefixes` is set; a null array means the whole slice (no key
/// filter). `validInstants` is the set of valid instant timestamps; a null
/// array means no instant filter.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_hudi_io_nativereader_NativeFileGroupReader_readFileGroupInto<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    table_path: JString<'local>,
    partition_path: JString<'local>,
    base_file_name: JString<'local>,
    log_file_names: JObjectArray<'local>,
    latest_instant: JString<'local>,
    data_schema_json: JString<'local>,
    lookup_keys: JObjectArray<'local>,
    lookup_keys_are_prefixes: jboolean,
    valid_instants: JObjectArray<'local>,
    stream_address: jlong,
) {
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        init_logger();
        let table_path = jstring_to_string(&mut env, &table_path, "tablePath")?;
        let partition_path = jstring_to_string(&mut env, &partition_path, "partition")?;
        let base_file_name = jstring_to_string(&mut env, &base_file_name, "baseFile")?;
        let logs = jstring_array_to_vec(&mut env, &log_file_names, "logFiles")?;
        let latest_instant = jstring_to_string(&mut env, &latest_instant, "latestInstant")?;
        // Optional: null means "no schema", so do not throw for it.
        let data_schema_json = if data_schema_json.as_raw().is_null() {
            String::new()
        } else {
            jstring_to_string(&mut env, &data_schema_json, "dataSchemaJson")?
        };
        let lookup_keys = jstring_array_to_vec(&mut env, &lookup_keys, "lookupKeys")?;
        let valid_instants = jstring_array_to_vec(&mut env, &valid_instants, "validInstants")?;
        if stream_address == 0 {
            return Err("streamAddress is 0".to_string());
        }
        let log_refs: Vec<&str> = logs.iter().map(String::as_str).collect();
        let key_refs: Vec<&str> = lookup_keys.iter().map(String::as_str).collect();
        let instant_refs: Vec<&str> = valid_instants.iter().map(String::as_str).collect();
        let req = FileGroupRequest {
            table_path: &table_path,
            partition_path: &partition_path,
            base_file_name: &base_file_name,
            log_file_names: &log_refs,
            latest_instant: &latest_instant,
            data_schema_json: &data_schema_json,
            lookup_keys: &key_refs,
            lookup_keys_are_prefixes: lookup_keys_are_prefixes != 0,
            valid_instants: &instant_refs,
        };
        // SAFETY: the address comes from ArrowArrayStream.allocateNew on the Java side.
        unsafe { export_file_group_stream_v2(&req, stream_address as *mut FFI_ArrowArrayStream) }
    }));
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(message)) => throw(&mut env, message),
        Err(panic) => throw(&mut env, panic_message(panic)),
    }
}

/// Liveness probe for the loader: returns `"hudi-jni <version>"`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_hudi_io_nativereader_NativeFileGroupReader_version<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        init_logger();
        env.new_string(format!("hudi-jni {}", env!("CARGO_PKG_VERSION")))
            .map(|s| s.into_raw())
            .map_err(|e| format!("cannot allocate the version string: {e}"))
    }));
    match outcome {
        Ok(Ok(raw)) => raw,
        Ok(Err(message)) => {
            throw(&mut env, message);
            std::ptr::null_mut()
        }
        Err(panic) => {
            throw(&mut env, panic_message(panic));
            std::ptr::null_mut()
        }
    }
}
