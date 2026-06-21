//! `InMemoryBroker` runs the shared broker conformance suite from
//! `worklane-test`, proving it satisfies the broker contract.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::DeadLetter;
use worklane_memory::InMemoryBroker;
use worklane_test::{BrokerContractHarness, ManualClock, TimedBrokerContractHarness};

/// Required tier: a plain broker on the default clock and lease.
struct MemHarness {
    broker: Arc<InMemoryBroker>,
}

impl MemHarness {
    fn new() -> Self {
        MemHarness {
            broker: Arc::new(InMemoryBroker::new()),
        }
    }
}

#[async_trait]
impl BrokerContractHarness for MemHarness {
    type Broker = InMemoryBroker;

    fn broker(&self) -> Arc<InMemoryBroker> {
        self.broker.clone()
    }

    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self.broker.clone())
    }

    async fn dead_letters(&self, broker: &InMemoryBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters())
    }
}

/// Timed tier: a broker on a manual clock with a known lease.
const TEST_LEASE: Duration = Duration::from_secs(30);

struct TimedMemHarness {
    broker: Arc<InMemoryBroker>,
    clock: Arc<ManualClock>,
}

impl TimedMemHarness {
    fn new() -> Self {
        let clock = Arc::new(ManualClock::new());
        let broker = Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(TEST_LEASE));
        TimedMemHarness { broker, clock }
    }
}

#[async_trait]
impl BrokerContractHarness for TimedMemHarness {
    type Broker = InMemoryBroker;

    fn broker(&self) -> Arc<InMemoryBroker> {
        self.broker.clone()
    }

    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self.broker.clone())
    }

    async fn dead_letters(&self, broker: &InMemoryBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters())
    }
}

impl TimedBrokerContractHarness for TimedMemHarness {
    fn advance(&self, delta: Duration) {
        self.clock.advance(delta);
    }

    fn lease(&self) -> Duration {
        TEST_LEASE
    }
}

// Draw both tiers from the single-source drivers in `worklane-test`; the emitter
// turns each name into a `#[tokio::test]` against a fresh in-process harness.
macro_rules! emit_required {
    ($($name:ident),* $(,)?) => {
        $(worklane_test::contract_tests!(MemHarness::new(); $name);)*
    };
}
macro_rules! emit_timed {
    ($($name:ident),* $(,)?) => {
        $(worklane_test::contract_tests!(TimedMemHarness::new(); $name);)*
    };
}
worklane_test::for_each_required_scenario!(emit_required);
worklane_test::for_each_timed_scenario!(emit_timed);
