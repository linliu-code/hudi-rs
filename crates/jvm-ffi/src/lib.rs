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
//! Crate `hudi-core`.
//!
//! # The [config] module is responsible for managing configurations.
//!

//! A plain C ABI over hudi-rs, so a JVM can read a file slice.
//!
//! The `cpp` crate already exposes the same reads, but `cxx` generates
//! C++-mangled symbols and `CxxString`/`CxxVector` arguments, neither of which a
//! JVM can bind to. This crate restates those calls in C: null-terminated
//! strings in, opaque handles and an `ArrowArrayStream` out. It holds no reading
//! logic of its own.
//!
//! # Ownership
//!
//! Every non-null pointer returned here is owned by the caller and must be
//! released exactly once, with the matching `hudi_ffi_free_*`. Releasing twice
//! is a double free; not releasing leaks the reader or the Arrow buffers behind
//! the stream.
//!
//! # Errors and panics
//!
//! A failing call returns null and leaves a message retrievable with
//! [`hudi_ffi_last_error`] until the next call on the same thread. The one
//! exception is [`hudi_ffi_read_file_group_v2_into`], which writes into a
//! caller-owned stream and so signals failure by returning `-1`
//! (`0` on success) rather than null; its message is retrieved the same way.
//! Panics are caught at every boundary and turned into that same
//! failure-plus-message, because a panic unwinding into the JVM aborts the
//! process.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};

use arrow::array::RecordBatchIterator;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use hudi::file_group::reader::FileGroupReader;
use hudi::table::{ReadOptions, Table};

pub mod file_group_v2;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_error(message: impl Into<String>) {
    let text = message.into();
    let encoded = CString::new(text).unwrap_or_else(|_| {
        CString::new("hudi-rs error message contained an interior nul byte")
            .expect("this literal has no nul")
    });
    LAST_ERROR.with(|slot| *slot.borrow_mut() = Some(encoded));
}

/// Run `body`, turning any error or panic into a null return plus a message.
///
/// Every exported function goes through this. A panic that unwinds across the C
/// ABI aborts the process, so catching it here is what keeps a Rust bug from
/// killing the JVM that called us.
fn guard<T>(what: &str, body: impl FnOnce() -> Result<*mut T, String>) -> *mut T {
    // The previous call's message dies here, so `hudi_ffi_last_error` means
    // "the last call failed", not "some call once failed" — a binding that
    // checks the message rather than the return value must not see a stale one.
    LAST_ERROR.with(|slot| *slot.borrow_mut() = None);
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(ptr)) => ptr,
        Ok(Err(message)) => {
            set_error(format!("{what}: {message}"));
            std::ptr::null_mut()
        }
        Err(payload) => {
            let detail = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            set_error(format!("{what} panicked: {detail}"));
            std::ptr::null_mut()
        }
    }
}

/// Borrow a C string, naming the argument in any error.
unsafe fn as_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{name} is null"));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|e| format!("{name} is not valid UTF-8: {e}"))
}

/// Borrow `len` C strings from an array.
unsafe fn as_strs<'a>(
    ptr: *const *const c_char,
    len: usize,
    name: &str,
) -> Result<Vec<&'a str>, String> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(format!("{name} is null but len is {len}"));
    }
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    slice
        .iter()
        .enumerate()
        .map(|(i, p)| unsafe { as_str(*p, &format!("{name}[{i}]")) })
        .collect()
}

/// A reader plus the runtime its async calls are driven on.
///
/// The runtime is owned here rather than created per read: a current-thread
/// runtime is cheap to build but the reader holds object-store clients whose
/// connection pools should outlive one call.
pub struct HudiFfiReader {
    inner: FileGroupReader,
    runtime: tokio::runtime::Runtime,
}

/// A table handle, for the metadata read.
///
/// Separate from [`HudiFfiReader`] because the two wrap different things: a file
/// group reader is handed a slice, while a metadata read resolves its own.
pub struct HudiFfiTable {
    inner: Table,
    runtime: tokio::runtime::Runtime,
}

/// Open the table at `base_uri` for a metadata read.
///
/// # Safety
/// Pointers must be null-terminated C strings valid for the call, and the two
/// option arrays must each hold `option_count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_table_open(
    base_uri: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_count: usize,
) -> *mut HudiFfiTable {
    guard("hudi_ffi_table_open", || {
        let base_uri = unsafe { as_str(base_uri, "base_uri") }?;
        let keys = unsafe { as_strs(option_keys, option_count, "option_keys") }?;
        let values = unsafe { as_strs(option_values, option_count, "option_values") }?;
        let options: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build a tokio runtime: {e}"))?;
        let inner = runtime
            .block_on(Table::new_with_options(base_uri, options))
            .map_err(|e| format!("failed to open {base_uri}: {e}"))?;
        Ok(Box::into_raw(Box::new(HudiFfiTable { inner, runtime })))
    })
}

/// Read the metadata table's `files` partition as an Arrow stream.
///
/// `keys` may be null when `key_count` is zero, which reads every record.
/// Keys are matched as stored, so a non-partitioned table's record is asked for
/// as `"."`.
///
/// # Safety
/// `table` must come from [`hudi_ffi_table_open`] and not yet be freed. The
/// returned stream must be released with [`hudi_ffi_free_stream`] exactly once,
/// including on paths where the caller stops reading partway.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_read_metadata_files_partition(
    table: *mut HudiFfiTable,
    keys: *const *const c_char,
    key_count: usize,
) -> *mut FFI_ArrowArrayStream {
    guard("hudi_ffi_read_metadata_files_partition", || {
        if table.is_null() {
            return Err("table is null".to_string());
        }
        let table = unsafe { &*table };
        let keys = unsafe { as_strs(keys, key_count, "keys") }?;

        let batch = table
            .runtime
            .block_on(table.inner.read_metadata_table_files_partition_arrow(&keys))
            .map_err(|e| format!("failed to read the metadata table: {e}"))?;

        let schema = batch.schema();
        let iterator = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Ok(Box::into_raw(Box::new(FFI_ArrowArrayStream::new(
            Box::new(iterator),
        ))))
    })
}

/// Release a table handle. Null is accepted and ignored.
///
/// # Safety
/// `table` must come from [`hudi_ffi_table_open`] and be released once only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_free_table(table: *mut HudiFfiTable) {
    if !table.is_null() {
        drop(unsafe { Box::from_raw(table) });
    }
}

/// Open a reader for the table at `base_uri`.
///
/// `option_keys` and `option_values` are parallel arrays of `option_count`
/// entries. Returns null on failure; see [`hudi_ffi_last_error`].
///
/// # Safety
/// All pointers must be null-terminated C strings valid for the call, and the
/// two option arrays must each hold `option_count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_reader_open(
    base_uri: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_count: usize,
) -> *mut HudiFfiReader {
    guard("hudi_ffi_reader_open", || {
        let base_uri = unsafe { as_str(base_uri, "base_uri") }?;
        let keys = unsafe { as_strs(option_keys, option_count, "option_keys") }?;
        let values = unsafe { as_strs(option_values, option_count, "option_values") }?;
        let options: Vec<(&str, &str)> = keys.into_iter().zip(values).collect();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build a tokio runtime: {e}"))?;
        let inner = runtime
            .block_on(FileGroupReader::new_with_options(base_uri, options))
            .map_err(|e| format!("failed to open {base_uri}: {e}"))?;
        Ok(Box::into_raw(Box::new(HudiFfiReader { inner, runtime })))
    })
}

/// Read one file slice, named by its base file and log files, into a stream.
///
/// `log_file_paths` may be null when `log_file_count` is zero, which reads a
/// base file on its own. Returns null on failure.
///
/// # Safety
/// `reader` must come from [`hudi_ffi_reader_open`] and not yet be freed. The
/// returned stream must be released with [`hudi_ffi_free_stream`] exactly once,
/// including on paths where the caller aborts partway through reading it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_read_slice(
    reader: *mut HudiFfiReader,
    base_file_path: *const c_char,
    log_file_paths: *const *const c_char,
    log_file_count: usize,
) -> *mut FFI_ArrowArrayStream {
    guard("hudi_ffi_read_slice", || {
        if reader.is_null() {
            return Err("reader is null".to_string());
        }
        let reader = unsafe { &*reader };
        let base_file_path = unsafe { as_str(base_file_path, "base_file_path") }?;
        let logs = unsafe { as_strs(log_file_paths, log_file_count, "log_file_paths") }?;

        let batch = reader
            .runtime
            .block_on(reader.inner.read_file_slice_from_paths(
                base_file_path,
                logs,
                &ReadOptions::new(),
            ))
            .map_err(|e| format!("failed to read {base_file_path}: {e}"))?;

        // Eager, matching what the cxx bridge does today: one batch wrapped as a
        // stream. `read_file_slice_from_paths_stream` exists and would avoid
        // materialising the slice, but driving an async stream from inside the
        // stream's synchronous `get_next` callback needs care that belongs in its
        // own change.
        let schema = batch.schema();
        let iterator = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Ok(Box::into_raw(Box::new(FFI_ArrowArrayStream::new(
            Box::new(iterator),
        ))))
    })
}

/// Release a reader. Null is accepted and ignored.
///
/// # Safety
/// `reader` must come from [`hudi_ffi_reader_open`] and be released once only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_free_reader(reader: *mut HudiFfiReader) {
    if !reader.is_null() {
        drop(unsafe { Box::from_raw(reader) });
    }
}

/// Release a stream. Null is accepted and ignored.
///
/// # Safety
/// `stream` must come from [`hudi_ffi_read_slice`] and be released once only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_free_stream(stream: *mut FFI_ArrowArrayStream) {
    if !stream.is_null() {
        drop(unsafe { Box::from_raw(stream) });
    }
}

/// The last error on this thread, or null if the last call succeeded.
///
/// The returned string is owned by this library and stays valid until the next
/// call on the same thread.
#[unsafe(no_mangle)]
pub extern "C" fn hudi_ffi_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(message) => message.as_ptr(),
        None => std::ptr::null(),
    })
}

/// Read one file group through reader_v2 and export it into a caller-allocated
/// `ArrowArrayStream`. Returns 0 on success, -1 on failure (message via
/// [`hudi_ffi_last_error`]). `out_stream` is owned by the caller and must be a
/// fresh, zeroed struct (`ArrowArrayStream.allocateNew` on the JVM side).
///
/// Not routed through [`guard`], which returns a pointer: this one reports
/// through a status code because the stream travels out through `out_stream`.
/// The error and panic handling is the same, down to clearing the previous
/// call's message first.
///
/// `latest_instant` is required and must be non-empty; the read is refused otherwise.
///
/// `data_schema_json` is the Avro JSON of the table's data schema, the way
/// Java's `HoodieFileGroupReader` is always given one. It is optional: null or
/// an empty string means "no schema", and the engine then infers the output
/// schema from the slice — which cannot work for a log-only slice.
///
/// `lookup_keys` is tri-state: a NULL pointer (count must be 0) means no
/// predicate — the whole slice; a non-null pointer with `lookup_key_count == 0`
/// means match nothing (zero rows, Java's EmptyIterator); otherwise the listed
/// keys, as prefixes when `lookup_keys_are_prefixes` is set. `valid_instants`
/// (with `valid_instant_count`) is optional: null or a count of 0 means no
/// instant filter.
///
/// # Safety
/// All string pointers must be valid NUL-terminated UTF-8; `log_file_names`
/// must point to `log_file_count` such strings; `lookup_keys` must point to
/// `lookup_key_count` such strings; `valid_instants` must point to
/// `valid_instant_count` such strings; `out_stream` must be a valid pointer to
/// writable memory sized for an `ArrowArrayStream`. `lookup_keys_are_prefixes`
/// is a C `bool` (`_Bool`, one byte, 0 or 1); any other value is undefined
/// behaviour.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hudi_ffi_read_file_group_v2_into(
    table_path: *const c_char,
    partition_path: *const c_char,
    base_file_name: *const c_char,
    log_file_names: *const *const c_char,
    log_file_count: usize,
    latest_instant: *const c_char,
    data_schema_json: *const c_char,
    lookup_keys: *const *const c_char,
    lookup_key_count: usize,
    lookup_keys_are_prefixes: bool,
    valid_instants: *const *const c_char,
    valid_instant_count: usize,
    out_stream: *mut FFI_ArrowArrayStream,
) -> i32 {
    const WHAT: &str = "hudi_ffi_read_file_group_v2_into";
    LAST_ERROR.with(|slot| *slot.borrow_mut() = None);
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        let table_path = unsafe { as_str(table_path, "table_path") }?;
        let partition_path = unsafe { as_str(partition_path, "partition_path") }?;
        let base_file_name = unsafe { as_str(base_file_name, "base_file_name") }?;
        let logs = unsafe { as_strs(log_file_names, log_file_count, "log_file_names") }?;
        let latest_instant = unsafe { as_str(latest_instant, "latest_instant") }?;
        // Optional: null is "no schema", not an error.
        let data_schema_json = if data_schema_json.is_null() {
            ""
        } else {
            unsafe { as_str(data_schema_json, "data_schema_json") }?
        };
        // Tri-state (D-12): a null pointer is "no predicate"; a non-null pointer with
        // a count of 0 is "match nothing"; otherwise the listed keys.
        let lookup_keys_vec = if lookup_keys.is_null() {
            if lookup_key_count != 0 {
                return Err(format!(
                    "lookup_keys is null but lookup_key_count is {lookup_key_count}"
                ));
            }
            None
        } else {
            Some(unsafe { as_strs(lookup_keys, lookup_key_count, "lookup_keys") }?)
        };
        let valid_instants =
            unsafe { as_strs(valid_instants, valid_instant_count, "valid_instants") }?;
        let req = file_group_v2::FileGroupRequest {
            table_path,
            partition_path,
            base_file_name,
            log_file_names: &logs,
            latest_instant,
            data_schema_json,
            lookup_keys: lookup_keys_vec.as_deref(),
            lookup_keys_are_prefixes,
            valid_instants: &valid_instants,
        };
        unsafe { file_group_v2::export_file_group_stream_v2(&req, out_stream) }
    }));
    match outcome {
        Ok(Ok(())) => 0,
        Ok(Err(message)) => {
            set_error(format!("{WHAT}: {message}"));
            -1
        }
        Err(payload) => {
            let detail = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            set_error(format!("{WHAT} panicked: {detail}"));
            -1
        }
    }
}

/// Panic on purpose, so a caller can prove a panic does not cross the boundary.
///
/// Behind a feature that is off by default, so it is absent from a shipped
/// library. A test that needs it builds with `--features ffi-test-hooks`; the
/// point is to exercise [`guard`]'s `catch_unwind` from the far side of the C
/// ABI, which a Rust unit test cannot do because it never crosses it.
#[cfg(feature = "ffi-test-hooks")]
#[unsafe(no_mangle)]
pub extern "C" fn hudi_ffi_panic_for_test() -> *mut FFI_ArrowArrayStream {
    guard("hudi_ffi_panic_for_test", || {
        panic!("deliberate panic, to prove it is caught at the boundary")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use std::ffi::CString;

    /// A panic inside a guarded body becomes null plus a message, not an abort.
    ///
    /// This is the whole of the panic-safety contract on the Rust side: a panic
    /// unwinding across `extern "C"` aborts the process, so every export routes
    /// through `guard`. Removing the `catch_unwind` fails this test by aborting
    /// the test binary, which is the loudest possible failure.
    #[test]
    fn a_panic_becomes_a_null_and_a_message() {
        let ptr = guard::<u8>("boom", || panic!("deliberate"));
        assert!(ptr.is_null(), "a panicking body must return null");
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert!(
            message.contains("boom panicked") && message.contains("deliberate"),
            "the message must name the call and the payload, got {message:?}"
        );
    }

    /// An error becomes null plus a message that names the call.
    #[test]
    fn an_error_becomes_a_null_and_a_message() {
        let ptr = guard::<u8>("open", || Err("no such table".to_string()));
        assert!(ptr.is_null());
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(message, "open: no such table");
    }

    /// A successful call clears the previous failure's message.
    ///
    /// `hudi_ffi_last_error` documents "null if the last call succeeded", and a
    /// JVM binding that checks the message rather than the return value relies
    /// on it: without the clear, one failure makes every later success look
    /// like that same failure.
    #[test]
    fn a_success_clears_the_previous_failure() {
        let _ = guard::<u8>("first", || Err("first failed".to_string()));
        assert!(
            !hudi_ffi_last_error().is_null(),
            "the failure must set a message, or the clearing below is unproven"
        );

        let ptr = guard::<u8>("second", || Ok(Box::into_raw(Box::new(7u8))));
        assert!(!ptr.is_null());
        drop(unsafe { Box::from_raw(ptr) });
        assert!(
            hudi_ffi_last_error().is_null(),
            "a successful call must leave no error to read"
        );
    }

    /// Freeing null is a no-op, on every free.
    ///
    /// A JVM caller's `finally` runs whether or not the call succeeded, so it
    /// will free a null handle on the failure path. That has to be safe or the
    /// error path becomes a crash.
    #[test]
    fn freeing_null_is_a_no_op() {
        unsafe {
            hudi_ffi_free_reader(std::ptr::null_mut());
            hudi_ffi_free_stream(std::ptr::null_mut());
            hudi_ffi_free_table(std::ptr::null_mut());
        }
    }

    /// A null argument is refused rather than dereferenced.
    #[test]
    fn null_arguments_are_refused() {
        let reader = unsafe {
            hudi_ffi_reader_open(std::ptr::null(), std::ptr::null(), std::ptr::null(), 0)
        };
        assert!(reader.is_null(), "a null base_uri must not open a reader");

        let stream = unsafe {
            hudi_ffi_read_metadata_files_partition(std::ptr::null_mut(), std::ptr::null(), 0)
        };
        assert!(stream.is_null(), "a null table must not produce a stream");
    }

    /// The v2 file-group export reports through its status code, so its refusal
    /// path is proven separately from the pointer-returning exports above.
    #[test]
    fn the_v2_file_group_export_refuses_null_arguments() {
        let mut stream = FFI_ArrowArrayStream::empty();
        let rc = unsafe {
            hudi_ffi_read_file_group_v2_into(
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                0,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                0,
                false,
                std::ptr::null(),
                0,
                &mut stream as *mut _,
            )
        };
        assert_eq!(rc, -1, "a null table_path must not be dereferenced");
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert!(
            message.contains("hudi_ffi_read_file_group_v2_into") && message.contains("table_path"),
            "the message must name the call and the argument, got {message:?}"
        );
    }

    /// A non-UTF-8 argument is refused with a message rather than panicking.
    #[test]
    fn invalid_utf8_is_refused() {
        let bad = [0xffu8, 0xfe, 0x00];
        let ptr = unsafe {
            hudi_ffi_reader_open(
                bad.as_ptr() as *const c_char,
                std::ptr::null(),
                std::ptr::null(),
                0,
            )
        };
        assert!(ptr.is_null());
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert!(message.contains("not valid UTF-8"), "got {message:?}");
    }

    /// The error slot is per thread, so one thread's failure is not another's.
    #[test]
    fn the_error_slot_is_per_thread() {
        let _ = guard::<u8>("outer", || Err("outer failed".to_string()));
        std::thread::spawn(|| {
            assert!(
                hudi_ffi_last_error().is_null(),
                "a fresh thread must start with no error"
            );
        })
        .join()
        .unwrap();
        let message = unsafe { CStr::from_ptr(hudi_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(
            message, "outer: outer failed",
            "the outer thread keeps its own"
        );
    }

    fn mdt_and_richest_hfile() -> (String, String) {
        let mdt = format!(
            "{}/.hoodie/metadata",
            hudi_test::QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro()
        );
        let dir = format!("{mdt}/record_index");
        let mut best: Option<(String, u64)> = None;
        for entry in std::fs::read_dir(&dir).expect("record_index dir") {
            let name = entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .to_string();
            if name.starts_with("._") || !name.ends_with(".hfile") {
                continue;
            }
            let bytes = std::fs::read(format!("{dir}/{name}")).expect("read hfile");
            let n = hudi::hfile::HFileReader::new(bytes)
                .expect("hfile")
                .num_entries();
            if best.as_ref().is_none_or(|(_, m)| n > *m) {
                best = Some((name, n));
            }
        }
        let (base, n) = best.expect("an hfile");
        assert!(n > 0, "the control hfile must hold rows");
        (mdt, base)
    }

    fn rows_via_c_abi(lookup_keys: *const *const c_char, lookup_key_count: usize) -> usize {
        let (mdt, base) = mdt_and_richest_hfile();
        let mdt_c = CString::new(mdt).unwrap();
        let part_c = CString::new("record_index").unwrap();
        let base_c = CString::new(base).unwrap();
        let instant_c = CString::new(hudi::ffi_support::MAX_INSTANT_TIME).unwrap();
        let mut stream = FFI_ArrowArrayStream::empty();
        let rc = unsafe {
            hudi_ffi_read_file_group_v2_into(
                mdt_c.as_ptr(),
                part_c.as_ptr(),
                base_c.as_ptr(),
                std::ptr::null(),
                0,
                instant_c.as_ptr(),
                std::ptr::null(),
                lookup_keys,
                lookup_key_count,
                false,
                std::ptr::null(),
                0,
                &mut stream,
            )
        };
        assert_eq!(rc, 0, "read failed: {:?}", unsafe {
            let p = hudi_ffi_last_error();
            if p.is_null() {
                None
            } else {
                Some(CStr::from_ptr(p).to_string_lossy().to_string())
            }
        });
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut stream) }.expect("stream");
        reader.map(|b| b.expect("batch").num_rows()).sum()
    }

    /// D-12 at the C boundary: a null `lookup_keys` reads the whole slice, a non-null
    /// pointer with a count of 0 reads nothing.
    #[test]
    fn c_abi_lookup_keys_null_is_absent_and_empty_is_match_nothing() {
        let whole = rows_via_c_abi(std::ptr::null(), 0);
        assert!(whole > 0, "the control read must return rows");
        let none: [*const c_char; 0] = [];
        let empty = rows_via_c_abi(none.as_ptr(), 0);
        assert_eq!(
            empty, 0,
            "a non-null pointer with count 0 must match nothing"
        );
    }
}
