//! The result-store contract scenarios, one async function each. They are the
//! executable form of the durable-result-store spec; the `result_store_contract`
//! macro wraps each as a `#[tokio::test]` over a caller-provided harness.
//!
//! These cover the backend-agnostic contract: round-trip, the `unknown key ->
//! None` boundary, last-writer-wins overwrite, and key isolation. TTL expiry is
//! intentionally excluded here — it is a Redis-only capability and is covered by
//! a Redis-specific test rather than this shared suite.

use worklane_core::{JobId, ResultStore};

use crate::harness::ResultStoreContractHarness;

/// A stored value round-trips: `get` returns exactly the bytes `store` wrote.
pub async fn round_trip<H: ResultStoreContractHarness>(h: &H) {
    let store = h.store();
    let id = JobId::new();
    let data = b"result-bytes".to_vec();
    store.store(&id, &data).await.expect("store must succeed");
    let got = store.get(&id).await.expect("get must succeed");
    assert_eq!(
        got.as_deref(),
        Some(data.as_slice()),
        "stored bytes must round-trip unchanged"
    );
}

/// `get` on a never-stored key returns `None` — not an error and not empty bytes.
pub async fn unknown_key_returns_none<H: ResultStoreContractHarness>(h: &H) {
    let store = h.store();
    let got = store.get(&JobId::new()).await.expect("get must succeed");
    assert!(
        got.is_none(),
        "an unstored key must resolve to None, not an error or empty value"
    );
}

/// Storing twice under the same key is last-writer-wins: the second value
/// replaces the first.
pub async fn overwrite_replaces_value<H: ResultStoreContractHarness>(h: &H) {
    let store = h.store();
    let id = JobId::new();
    store.store(&id, b"first").await.expect("store first");
    store.store(&id, b"second").await.expect("store second");
    let got = store.get(&id).await.expect("get must succeed");
    assert_eq!(
        got.as_deref(),
        Some(b"second".as_slice()),
        "a second store under the same key must overwrite the first"
    );
}

/// Values are isolated by key: a value stored under one id is never visible
/// under another.
pub async fn distinct_keys_isolated<H: ResultStoreContractHarness>(h: &H) {
    let store = h.store();
    let a = JobId::new();
    let b = JobId::new();
    store.store(&a, b"a-bytes").await.expect("store a");
    assert_eq!(
        store.get(&a).await.expect("get a").as_deref(),
        Some(b"a-bytes".as_slice()),
        "the stored key must return its own value"
    );
    assert!(
        store.get(&b).await.expect("get b").is_none(),
        "a value stored under one key must not be visible under another"
    );
}
