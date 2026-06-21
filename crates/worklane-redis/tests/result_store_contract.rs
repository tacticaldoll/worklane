//! `RedisResultStore` runs the shared result-store conformance suite from
//! `worklane-test` against a live Redis, proving the non-SQL backend satisfies
//! the durable-result-store contract.
//!
//! These tests require a reachable Redis: set `WORKLANE_REDIS_TEST_URL`
//! (e.g. `redis://localhost:6379`). When it is unset each test visibly skips, so
//! `cargo test` stays green in environments without a database. Each test runs in
//! its own key namespace for isolation. TTL expiry is covered separately by a
//! Redis-specific unit test, as it is not part of the backend-agnostic contract.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use worklane_redis::{RedisBroker, RedisResultStore};
use worklane_test::ResultStoreContractHarness;

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_REDIS_TEST_URL").ok()
}

static NS_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique key namespace per harness so concurrent tests are isolated.
fn unique_namespace() -> String {
    format!(
        "wl_rs_{}_{}",
        std::process::id(),
        NS_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

struct RedisResultStoreHarness {
    store: Arc<RedisResultStore>,
}

impl RedisResultStoreHarness {
    async fn new(url: &str) -> Self {
        let broker = RedisBroker::connect_with_namespace(url, &unique_namespace())
            .await
            .expect("connect to test redis");
        RedisResultStoreHarness {
            store: Arc::new(broker.result_store()),
        }
    }
}

impl ResultStoreContractHarness for RedisResultStoreHarness {
    type Store = RedisResultStore;

    fn store(&self) -> Arc<RedisResultStore> {
        self.store.clone()
    }
}

/// Generate a `#[tokio::test]` per scenario that builds a fresh harness and runs
/// it, or visibly skips when no test database is configured.
macro_rules! redis_result_store {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(url) = test_url() else {
                eprintln!(concat!(
                    "SKIP ", stringify!($name),
                    ": set WORKLANE_REDIS_TEST_URL to run the redis result-store conformance suite"
                ));
                return;
            };
            let h = RedisResultStoreHarness::new(&url).await;
            worklane_test::result_store_scenarios::$name(&h).await;
        }
    )*};
}

redis_result_store!(
    round_trip,
    unknown_key_returns_none,
    overwrite_replaces_value,
    distinct_keys_isolated,
);
