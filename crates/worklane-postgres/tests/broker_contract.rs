//! `PostgresBroker` runs the shared broker conformance suite from `worklane-test`
//! against a live Postgres, proving the networked durable backend satisfies the
//! broker contract without any change to the `Broker` trait.
//!
//! These tests require a reachable Postgres: set `WORKLANE_POSTGRES_TEST_URL`
//! (e.g. `postgres://user:pass@localhost:5432/db`). When it is unset each test
//! visibly skips, so `cargo test` stays green in environments without a database.
//! Each test runs in its own freshly-created schema for isolation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Semaphore, SemaphorePermit};
use worklane_core::DeadLetter;
use worklane_postgres::PostgresBroker;
use worklane_test::{BrokerContractHarness, ManualClock, TimedBrokerContractHarness};

/// Per-broker pool size for the conformance harness — small so many brokers can
/// share one server.
const TEST_POOL_SIZE: usize = 3;

/// Cap concurrently-live broker connections across this test binary. Each test
/// builds its own broker (its own schema + pool), and the harness runs them in
/// parallel; on a high-core machine the default thread count times `TEST_POOL_SIZE`
/// can exceed a shared Postgres's `max_connections` (default ~100). A `pool.get()`
/// that then opens a connection the server refuses surfaces as a non-stale error
/// in the timing-sensitive `concurrent_*` cases — the prior intermittent failure.
/// Bounding the binary to `CONN_BUDGET` connections (≈16 brokers × 3) keeps a safe
/// margin; excess test threads wait here instead of opening a doomed connection.
static CONN_BUDGET: Semaphore = Semaphore::const_new(48);

/// Acquire the connection permits for one broker (held for its lifetime), then
/// connect with the small test pool on a fresh schema.
async fn connect_budgeted(url: &str) -> (PostgresBroker, SemaphorePermit<'static>) {
    let permit = CONN_BUDGET
        .acquire_many(TEST_POOL_SIZE as u32)
        .await
        .expect("connection budget semaphore is never closed");
    let broker = PostgresBroker::connect_with_pool(url, &unique_schema(), TEST_POOL_SIZE)
        .await
        .expect("connect to test postgres");
    (broker, permit)
}

/// The connection URL for the test database, if configured.
fn test_url() -> Option<String> {
    std::env::var("WORKLANE_POSTGRES_TEST_URL").ok()
}

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, safe schema name per harness so concurrent tests are isolated.
fn unique_schema() -> String {
    format!(
        "wl_{}_{}",
        std::process::id(),
        SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

const TEST_LEASE: Duration = Duration::from_secs(30);

/// Required tier: a broker on its own schema with the default (wall) clock and a
/// small pool.
struct PgHarness {
    broker: Arc<PostgresBroker>,
    _permit: SemaphorePermit<'static>,
}

impl PgHarness {
    async fn new(url: &str) -> Self {
        let (broker, permit) = connect_budgeted(url).await;
        PgHarness {
            broker: Arc::new(broker),
            _permit: permit,
        }
    }
}

#[async_trait]
impl BrokerContractHarness for PgHarness {
    type Broker = PostgresBroker;

    fn broker(&self) -> Arc<PostgresBroker> {
        self.broker.clone()
    }

    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self.broker.clone())
    }

    async fn dead_letters(&self, broker: &PostgresBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters().await.expect("dead-letter query"))
    }
}

/// Timed tier: a broker on a manual clock with a known lease.
struct TimedPgHarness {
    broker: Arc<PostgresBroker>,
    clock: Arc<ManualClock>,
    _permit: SemaphorePermit<'static>,
}

impl TimedPgHarness {
    async fn new(url: &str) -> Self {
        let clock = Arc::new(ManualClock::new());
        let (broker, permit) = connect_budgeted(url).await;
        let broker = broker.with_clock(clock.clone()).with_lease(TEST_LEASE);
        TimedPgHarness {
            broker: Arc::new(broker),
            clock,
            _permit: permit,
        }
    }
}

#[async_trait]
impl BrokerContractHarness for TimedPgHarness {
    type Broker = PostgresBroker;

    fn broker(&self) -> Arc<PostgresBroker> {
        self.broker.clone()
    }

    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self.broker.clone())
    }

    async fn dead_letters(&self, broker: &PostgresBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters().await.expect("dead-letter query"))
    }
}

impl TimedBrokerContractHarness for TimedPgHarness {
    fn advance(&self, delta: Duration) {
        self.clock.advance(delta);
    }

    fn lease(&self) -> Duration {
        TEST_LEASE
    }
}

/// Generate a `#[tokio::test]` per scenario that builds a fresh `PgHarness` and
/// runs it, or visibly skips when no test database is configured.
macro_rules! pg_capability {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(url) = test_url() else {
                eprintln!(concat!(
                    "SKIP ", stringify!($name),
                    ": set WORKLANE_POSTGRES_TEST_URL to run the postgres conformance suite"
                ));
                return;
            };
            let h = PgHarness::new(&url).await;
            worklane_test::scenarios::$name(&h).await;
        }
    )*};
}

/// As `pg_required!`, but builds a `TimedPgHarness` for the deterministic-time
/// tier.
macro_rules! pg_timed {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(url) = test_url() else {
                eprintln!(concat!(
                    "SKIP ", stringify!($name),
                    ": set WORKLANE_POSTGRES_TEST_URL to run the postgres conformance suite"
                ));
                return;
            };
            let h = TimedPgHarness::new(&url).await;
            worklane_test::scenarios::$name(&h).await;
        }
    )*};
}

// Enumerate lifecycle, optional capability, and timed batteries from the
// single-source drivers in `worklane-test`, so Postgres runs an identical
// supported scenario set to every other first-party backend and a scenario can
// never be silently dropped from this list.
worklane_test::for_each_lifecycle_scenario!(pg_capability);
worklane_test::for_each_dead_letter_scenario!(pg_capability);
worklane_test::for_each_queue_stats_scenario!(pg_capability);
worklane_test::for_each_batch_enqueue_scenario!(pg_capability);
worklane_test::for_each_scheduled_scenario!(pg_capability);
worklane_test::for_each_timed_scenario!(pg_timed);
