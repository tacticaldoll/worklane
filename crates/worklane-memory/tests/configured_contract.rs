//! `InMemoryBroker` runs the shared configured-broker conformance tier from
//! `worklane-test`: the bounded-redelivery (poison-pill) and dead-letter
//! retention scenarios, which need a broker built to a specific `BrokerConfig`
//! (a `max_deliveries` bound or a `RetentionPolicy`) on a manual clock.
//!
//! The scenario set is enumerated from the single-source
//! `for_each_configured_scenario!` driver, so this backend can never silently
//! drop a poison or retention scenario from a hand-maintained list.

use std::sync::Arc;

use async_trait::async_trait;
use worklane_memory::InMemoryBroker;
use worklane_test::{BrokerConfig, ConfigurableBrokerHarness, ManualClock};

/// Builds a fresh in-memory broker to each scenario's config on a manual clock.
struct MemoryConfigurableHarness;

#[async_trait]
impl ConfigurableBrokerHarness for MemoryConfigurableHarness {
    type Broker = InMemoryBroker;

    async fn build(&self, config: BrokerConfig) -> (Arc<InMemoryBroker>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new());
        let mut broker = InMemoryBroker::with_clock(clock.clone())
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
            worklane_test::scenarios::$name(&MemoryConfigurableHarness).await;
        }
    )*};
}

worklane_test::for_each_configured_scenario!(emit_configured);
