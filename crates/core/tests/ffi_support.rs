//! Guards the surface cpp/ imports from hudi-core.

#[test]
fn object_store_runtime_is_multi_thread_and_reusable() {
    use hudi_core::ffi_support::OBJECT_STORE_RUNTIME;

    // The DispatchGone bug: a per-call current_thread runtime lets hyper's
    // connection dispatcher die when the runtime drops, so the SECOND
    // file-group read fails. Two sequential block_on calls on the shared
    // runtime must both succeed.
    let a = OBJECT_STORE_RUNTIME.block_on(async { 1 + 1 });
    let b = OBJECT_STORE_RUNTIME.block_on(async { 2 + 2 });
    assert_eq!((a, b), (2, 4));

    // Must be multi_thread: block_on from inside Iterator::next while the
    // runtime also drives IO requires more than one worker.
    let n = OBJECT_STORE_RUNTIME.block_on(async { tokio::spawn(async { 7 }).await.unwrap() });
    assert_eq!(
        n, 7,
        "spawn inside block_on requires a multi-thread runtime"
    );
}
