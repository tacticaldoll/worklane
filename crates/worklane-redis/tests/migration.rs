//! Schema-version policy for the Redis backend.
//!
//! Redis is *drain-don't-migrate*: it does not migrate storage in place across a
//! schema-version boundary. worklane is pre-1.0 with a single baseline layout, so
//! a fresh namespace opens at the baseline and a namespace stamped with any other
//! version is rejected (flush and re-enqueue) rather than read under the current
//! assumptions.
//!
//! Requires a reachable Redis: set `WORKLANE_REDIS_TEST_URL`. When unset each test
//! visibly skips so `cargo test` stays green without a database.

use std::sync::atomic::{AtomicU64, Ordering};

use redis::AsyncCommands;
use worklane_redis::RedisBroker;

/// The baseline schema version this build supports (kept in sync with the
/// internal `SCHEMA_VERSION`).
const BASELINE_VERSION: i64 = 1;

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_REDIS_TEST_URL").ok()
}

static NS_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_namespace() -> String {
    format!(
        "wlmig:{}:{}",
        std::process::id(),
        NS_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Stamp `{namespace}:schema_version` to `version` on a fresh namespace.
async fn stamp_version(url: &str, namespace: &str, version: i64) {
    let client = redis::Client::open(url).expect("open redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect redis");
    let _: () = conn
        .set(format!("{namespace}:schema_version"), version)
        .await
        .expect("stamp schema_version");
}

/// A fresh namespace (no version stamp) opens cleanly and is stamped the baseline.
#[tokio::test]
async fn fresh_namespace_opens() {
    let Some(url) = test_url() else {
        eprintln!("SKIP fresh_namespace_opens: set WORKLANE_REDIS_TEST_URL");
        return;
    };
    let ns = unique_namespace();
    RedisBroker::connect_with_namespace(&url, &ns)
        .await
        .expect("a fresh namespace must open");
}

/// A namespace stamped with any non-baseline version (an old ladder version or a
/// newer one) is rejected: pre-1.0 there is no in-place migration.
#[tokio::test]
async fn a_different_schema_generation_is_rejected() {
    let Some(url) = test_url() else {
        eprintln!("SKIP a_different_schema_generation_is_rejected: set WORKLANE_REDIS_TEST_URL");
        return;
    };
    let ns = unique_namespace();
    stamp_version(&url, &ns, BASELINE_VERSION + 5).await;
    let err = RedisBroker::connect_with_namespace(&url, &ns)
        .await
        .err()
        .expect("opening a non-baseline namespace must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("baseline") && (msg.contains("flush") || msg.contains("re-enqueue")),
        "the error must explain the drain-don't-migrate policy, got: {msg}"
    );
}
