//! `PostgresResultStore` runs the shared result-store conformance suite from
//! `worklane-test` against a live Postgres, proving the networked backend
//! satisfies the durable-result-store contract.
//!
//! These tests require a reachable Postgres: set `WORKLANE_POSTGRES_TEST_URL`
//! (e.g. `postgres://user:pass@localhost:5432/db`). When it is unset each test
//! visibly skips, so `cargo test` stays green in environments without a database.
//! Each test runs in its own freshly-created schema for isolation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use worklane_postgres::{PostgresBroker, PostgresResultStore};
use worklane_test::ResultStoreContractHarness;

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_POSTGRES_TEST_URL").ok()
}

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, safe schema name per harness so concurrent tests are isolated.
fn unique_schema() -> String {
    format!(
        "wl_rs_{}_{}",
        std::process::id(),
        SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

struct PgResultStoreHarness {
    store: Arc<PostgresResultStore>,
}

impl PgResultStoreHarness {
    async fn new(url: &str) -> Self {
        // Connecting runs the migrations that create the `results` table.
        let broker = PostgresBroker::connect_with_pool(url, &unique_schema(), 3)
            .await
            .expect("connect to test postgres");
        PgResultStoreHarness {
            store: Arc::new(broker.result_store()),
        }
    }
}

impl ResultStoreContractHarness for PgResultStoreHarness {
    type Store = PostgresResultStore;

    fn store(&self) -> Arc<PostgresResultStore> {
        self.store.clone()
    }
}

/// Generate a `#[tokio::test]` per scenario that builds a fresh harness and runs
/// it, or visibly skips when no test database is configured.
macro_rules! pg_result_store {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(url) = test_url() else {
                eprintln!(concat!(
                    "SKIP ", stringify!($name),
                    ": set WORKLANE_POSTGRES_TEST_URL to run the postgres result-store conformance suite"
                ));
                return;
            };
            let h = PgResultStoreHarness::new(&url).await;
            worklane_test::result_store_scenarios::$name(&h).await;
        }
    )*};
}

pg_result_store!(
    round_trip,
    unknown_key_returns_none,
    overwrite_replaces_value,
    distinct_keys_isolated,
);
