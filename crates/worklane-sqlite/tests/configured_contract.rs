//! `SqliteBroker` runs the shared configured-broker conformance tier from
//! `worklane-test`: the bounded-redelivery (poison-pill) and dead-letter
//! retention scenarios, which need a broker built to a specific `BrokerConfig`
//! (a `max_deliveries` bound or a `RetentionPolicy`) on a manual clock.
//!
//! The scenario set is enumerated from the single-source
//! `for_each_configured_scenario!` driver, so this backend can never silently
//! drop a poison or retention scenario from a hand-maintained list.

use std::sync::Arc;

use async_trait::async_trait;
use worklane_sqlite::SqliteBroker;
use worklane_test::{BrokerConfig, ConfigurableBrokerHarness, ManualClock};

/// Builds a fresh in-memory SQLite broker to each scenario's config on a manual
/// clock.
struct SqliteConfigurableHarness;

#[async_trait]
impl ConfigurableBrokerHarness for SqliteConfigurableHarness {
    type Broker = SqliteBroker;

    async fn build(&self, config: BrokerConfig) -> (Arc<SqliteBroker>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new());
        let mut broker = SqliteBroker::open_in_memory()
            .expect("open in-memory sqlite")
            .with_clock(clock.clone())
            .with_lease(config.lease)
            .with_dead_letter_retention(config.retention);
        if let Some(max) = config.max_deliveries {
            broker = broker.with_max_deliveries(max);
        }
        (Arc::new(broker), clock)
    }
}

macro_rules! emit_configured {
    ($($name:ident),* $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            worklane_test::scenarios::$name(&SqliteConfigurableHarness).await;
        }
    )*};
}

worklane_test::for_each_configured_scenario!(emit_configured);
