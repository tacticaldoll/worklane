//! `RedisBroker` runs the shared broker conformance suite from `worklane-test`
//! against a live Redis, proving the non-SQL durable backend satisfies the broker
//! contract without any change to the `Broker` trait.
//!
//! These tests require a reachable Redis: set `WORKLANE_REDIS_TEST_URL`
//! (e.g. `redis://localhost:6379`). When it is unset each test visibly skips, so
//! `cargo test` stays green in environments without a database. Each test runs in
//! its own key namespace for isolation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::DeadLetter;
use worklane_redis::RedisBroker;
use worklane_test::{BrokerContractHarness, ManualClock, TimedBrokerContractHarness};

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_REDIS_TEST_URL").ok()
}

static NS_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique key namespace per harness so concurrent tests are isolated.
fn unique_namespace() -> String {
    format!(
        "wltest:{}:{}",
        std::process::id(),
        NS_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

const TEST_LEASE: Duration = Duration::from_secs(30);

/// Required tier: a broker on its own namespace with the default (wall) clock.
struct RedisHarness {
    broker: Arc<RedisBroker>,
}

impl RedisHarness {
    async fn new(url: &str) -> Self {
        let broker = RedisBroker::connect_with_namespace(url, &unique_namespace())
            .await
            .expect("connect to test redis");
        RedisHarness {
            broker: Arc::new(broker),
        }
    }
}

#[async_trait]
impl BrokerContractHarness for RedisHarness {
    type Broker = RedisBroker;

    fn broker(&self) -> Arc<RedisBroker> {
        self.broker.clone()
    }

    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self.broker.clone())
    }

    async fn dead_letters(&self, broker: &RedisBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters().await.expect("dead-letter query"))
    }
}

/// Timed tier: a broker on a manual clock with a known lease.
struct TimedRedisHarness {
    broker: Arc<RedisBroker>,
    clock: Arc<ManualClock>,
}

impl TimedRedisHarness {
    async fn new(url: &str) -> Self {
        let clock = Arc::new(ManualClock::new());
        let broker = RedisBroker::connect_with_namespace(url, &unique_namespace())
            .await
            .expect("connect to test redis")
            .with_clock(clock.clone())
            .with_lease(TEST_LEASE);
        TimedRedisHarness {
            broker: Arc::new(broker),
            clock,
        }
    }
}

#[async_trait]
impl BrokerContractHarness for TimedRedisHarness {
    type Broker = RedisBroker;

    fn broker(&self) -> Arc<RedisBroker> {
        self.broker.clone()
    }

    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self.broker.clone())
    }

    async fn dead_letters(&self, broker: &RedisBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters().await.expect("dead-letter query"))
    }
}

impl TimedBrokerContractHarness for TimedRedisHarness {
    fn advance(&self, delta: Duration) {
        self.clock.advance(delta);
    }

    fn lease(&self) -> Duration {
        TEST_LEASE
    }
}

macro_rules! redis_required {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(url) = test_url() else {
                eprintln!(concat!(
                    "SKIP ", stringify!($name),
                    ": set WORKLANE_REDIS_TEST_URL to run the redis conformance suite"
                ));
                return;
            };
            let h = RedisHarness::new(&url).await;
            worklane_test::scenarios::$name(&h).await;
        }
    )*};
}

macro_rules! redis_timed {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(url) = test_url() else {
                eprintln!(concat!(
                    "SKIP ", stringify!($name),
                    ": set WORKLANE_REDIS_TEST_URL to run the redis conformance suite"
                ));
                return;
            };
            let h = TimedRedisHarness::new(&url).await;
            worklane_test::scenarios::$name(&h).await;
        }
    )*};
}

// Enumerate both tiers from the single-source drivers in `worklane-test`, so
// Redis runs an identical scenario set to every other backend and a scenario can
// never be silently dropped from this list. `redis_required!` / `redis_timed!`
// supply the env-gated harness wiring per name.
worklane_test::for_each_required_scenario!(redis_required);
worklane_test::for_each_timed_scenario!(redis_timed);
