//! `PostgresBroker` runs the shared configured-broker conformance tier from
//! `worklane-test`: the bounded-redelivery (poison-pill) and dead-letter
//! retention scenarios, which need a broker built to a specific `BrokerConfig`
//! (a `max_deliveries` bound or a `RetentionPolicy`) on a manual clock.
//!
//! Requires a reachable Postgres: set `WORKLANE_POSTGRES_TEST_URL`. When unset
//! each test visibly skips so `cargo test` stays green without a database. Each
//! built broker pins a unique schema for isolation. The scenario set is
//! enumerated from the single-source `for_each_configured_scenario!` driver, so
//! this backend can never silently drop a poison or retention scenario.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::{Semaphore, SemaphorePermit};
use worklane_postgres::PostgresBroker;
use worklane_test::{BrokerConfig, ConfigurableBrokerHarness, ManualClock};

/// Per-broker pool size — small so many brokers can share one server.
const TEST_POOL_SIZE: usize = 2;

/// Cap concurrently-live broker connections across this test binary, mirroring
/// the conformance harness: each built broker opens its own small pool, and the
/// runner builds them in parallel, so without a budget a shared server's
/// `max_connections` can be exceeded.
static CONN_BUDGET: Semaphore = Semaphore::const_new(48);

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_POSTGRES_TEST_URL").ok()
}

/// A unique, safe schema name per built broker so scenarios are isolated.
fn unique_schema() -> String {
    format!(
        "wlcfg_{}_{}",
        std::process::id(),
        SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Connects per `build` on a fresh schema; holds each broker's connection
/// permit for the harness's (and so the broker's) lifetime.
struct PgConfigurableHarness {
    url: String,
    permits: Mutex<Vec<SemaphorePermit<'static>>>,
}

impl PgConfigurableHarness {
    fn new(url: String) -> Self {
        Self {
            url,
            permits: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ConfigurableBrokerHarness for PgConfigurableHarness {
    type Broker = PostgresBroker;

    async fn build(&self, config: BrokerConfig) -> (Arc<PostgresBroker>, Arc<ManualClock>) {
        let permit = CONN_BUDGET
            .acquire_many(TEST_POOL_SIZE as u32)
            .await
            .expect("connection budget semaphore is never closed");
        self.permits.lock().unwrap().push(permit);
        let clock = Arc::new(ManualClock::new());
        let mut broker =
            PostgresBroker::connect_with_pool(&self.url, &unique_schema(), TEST_POOL_SIZE)
                .await
                .expect("connect to test postgres")
                .with_clock(clock.clone())
                .with_lease(config.lease)
                .with_dead_letter_retention(config.retention);
        if let Some(max) = config.max_deliveries {
            broker = broker.with_max_deliveries(max);
        }
        (Arc::new(broker), clock)
    }
}

macro_rules! pg_configured {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(url) = test_url() else {
                eprintln!(concat!(
                    "SKIP ", stringify!($name),
                    ": set WORKLANE_POSTGRES_TEST_URL to run the postgres configured-contract tests"
                ));
                return;
            };
            let h = PgConfigurableHarness::new(url);
            worklane_test::scenarios::$name(&h).await;
        }
    )*};
}

worklane_test::for_each_configured_scenario!(pg_configured);
