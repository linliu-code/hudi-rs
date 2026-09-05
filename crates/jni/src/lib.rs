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
use jni::sys::{jlong, jstring};

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
        return Ok(Vec::new());
    }
    let len = env
        .get_array_length(array)
        .map_err(|e| format!("{what}: cannot read array length: {e}"))?;
    let mut out = Vec::with_capacity(len as usize);
    for i in 0..len {
        let element = env
            .get_object_array_element(array, i)
            .map_err(|e| format!("{what}[{i}]: {e}"))?;
        let element = JString::from(element);
        out.push(jstring_to_string(env, &element, &format!("{what}[{i}]"))?);
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
    let _ = env.throw_new(EXCEPTION_CLASS, message);
}

/// Reads one file group through reader_v2 into the Java-allocated stream at
/// `stream_address`. Throws `NativeReaderException` on any failure.
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
    stream_address: jlong,
) {
    init_logger();
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        let table_path = jstring_to_string(&mut env, &table_path, "tablePath")?;
        let partition_path = jstring_to_string(&mut env, &partition_path, "partition")?;
        let base_file_name = jstring_to_string(&mut env, &base_file_name, "baseFile")?;
        let logs = jstring_array_to_vec(&mut env, &log_file_names, "logFiles")?;
        let latest_instant = jstring_to_string(&mut env, &latest_instant, "latestInstant")?;
        if stream_address == 0 {
            return Err("streamAddress is 0".to_string());
        }
        let log_refs: Vec<&str> = logs.iter().map(String::as_str).collect();
        let req = FileGroupRequest {
            table_path: &table_path,
            partition_path: &partition_path,
            base_file_name: &base_file_name,
            log_file_names: &log_refs,
            latest_instant: &latest_instant,
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
    init_logger();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
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
