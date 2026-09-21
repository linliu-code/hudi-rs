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

    // Must be multi_thread: the FFI adapter calls block_on from inside
    // Iterator::next while the same runtime drives IO. Prove it with a
    // SYNCHRONOUS block inside the block_on future: on a multi-thread runtime
    // the spawned task runs on a worker thread and sends; on a current-thread
    // runtime the only thread is parked here in recv_timeout, the spawned
    // task can never be scheduled, and the recv times out. (A plain
    // `tokio::spawn(..).await` does NOT distinguish the two -- current_thread
    // cooperatively runs spawned tasks whenever the outer future yields.)
    let (tx, rx) = std::sync::mpsc::channel::<u8>();
    OBJECT_STORE_RUNTIME.block_on(async move {
        let worker = tokio::spawn(async move {
            tx.send(7).expect("receiver dropped");
        });
        let got = rx.recv_timeout(std::time::Duration::from_secs(5)).expect(
            "spawned task never ran while this thread blocked: runtime is not multi_thread",
        );
        assert_eq!(got, 7);
        worker.await.expect("spawned task panicked");
    });
}
