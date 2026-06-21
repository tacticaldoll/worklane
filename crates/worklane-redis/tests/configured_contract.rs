//! `RedisBroker` runs the shared configured-broker conformance tier from
//! `worklane-test`: the bounded-redelivery (poison-pill) and dead-letter
//! retention scenarios, which need a broker built to a specific `BrokerConfig`
//! (a `max_deliveries` bound or a `RetentionPolicy`) on a manual clock.
//!
//! Requires a reachable Redis: set `WORKLANE_REDIS_TEST_URL`. When unset each
//! test visibly skips so `cargo test` stays green without a database. Each built
//! broker pins a unique namespace for isolation. The scenario set is enumerated
//! from the single-source `for_each_configured_scenario!` driver, so this backend
//! can never silently drop a poison or retention scenario.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use worklane_redis::RedisBroker;
use worklane_test::{BrokerConfig, ConfigurableBrokerHarness, ManualClock};

static NS_COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_REDIS_TEST_URL").ok()
}

/// A unique key namespace per built broker so scenarios are isolated.
fn unique_namespace() -> String {
    format!(
        "wlcfg:{}:{}",
        std::process::id(),
        NS_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Connects per `build` on a fresh namespace.
struct RedisConfigurableHarness {
    url: String,
}

impl RedisConfigurableHarness {
    fn new(url: String) -> Self {
        Self { url }
    }
}

#[async_trait]
impl ConfigurableBrokerHarness for RedisConfigurableHarness {
    type Broker = RedisBroker;

    async fn build(&self, config: BrokerConfig) -> (Arc<RedisBroker>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new());
        let mut broker = RedisBroker::connect_with_namespace(&self.url, &unique_namespace())
            .await
            .expect("connect to test redis")
            .with_clock(clock.clone())
            .with_lease(config.lease)
            .with_dead_letter_retention(config.retention);
        if let Some(max) = config.max_deliveries {
            broker = broker.with_max_deliveries(max);
        }
        (Arc::new(broker), clock)
    }
}

macro_rules! redis_configured {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(url) = test_url() else {
                eprintln!(concat!(
                    "SKIP ", stringify!($name),
                    ": set WORKLANE_REDIS_TEST_URL to run the redis configured-contract tests"
                ));
                return;
            };
            let h = RedisConfigurableHarness::new(url);
            worklane_test::scenarios::$name(&h).await;
        }
    )*};
}

worklane_test::for_each_configured_scenario!(redis_configured);
