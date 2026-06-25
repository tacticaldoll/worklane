//! `SqliteBroker` runs the shared broker conformance suite from `worklane-test`,
//! proving the durable backend satisfies the broker contract without any change
//! to the `Broker` trait.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::DeadLetter;
use worklane_sqlite::SqliteBroker;
use worklane_test::{BrokerContractHarness, ManualClock, TimedBrokerContractHarness};

/// Required tier: a broker on a fresh in-memory database and the default clock
/// and lease. A private `:memory:` database per harness gives scenario isolation.
struct SqliteHarness {
    broker: Arc<SqliteBroker>,
}

impl SqliteHarness {
    fn new() -> Self {
        SqliteHarness {
            broker: Arc::new(SqliteBroker::open_in_memory().expect("open in-memory sqlite")),
        }
    }
}

#[async_trait]
impl BrokerContractHarness for SqliteHarness {
    type Broker = SqliteBroker;

    fn broker(&self) -> Arc<SqliteBroker> {
        self.broker.clone()
    }

    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self.broker.clone())
    }

    async fn dead_letters(&self, broker: &SqliteBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters().expect("dead-letter query"))
    }
}

/// Timed tier: a broker on a manual clock with a known lease.
const TEST_LEASE: Duration = worklane_core::spi::DEFAULT_LEASE;

struct TimedSqliteHarness {
    broker: Arc<SqliteBroker>,
    clock: Arc<ManualClock>,
}

impl TimedSqliteHarness {
    fn new() -> Self {
        let clock = Arc::new(ManualClock::new());
        let broker = Arc::new(
            SqliteBroker::open_in_memory()
                .expect("open in-memory sqlite")
                .with_clock(clock.clone())
                .with_lease(TEST_LEASE),
        );
        TimedSqliteHarness { broker, clock }
    }
}

#[async_trait]
impl BrokerContractHarness for TimedSqliteHarness {
    type Broker = SqliteBroker;

    fn broker(&self) -> Arc<SqliteBroker> {
        self.broker.clone()
    }

    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self.broker.clone())
    }

    async fn dead_letters(&self, broker: &SqliteBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters().expect("dead-letter query"))
    }
}

impl TimedBrokerContractHarness for TimedSqliteHarness {
    fn advance(&self, delta: Duration) {
        self.clock.advance(delta);
    }

    fn lease(&self) -> Duration {
        TEST_LEASE
    }
}

// Draw lifecycle and optional capability batteries from the single-source
// drivers in `worklane-test`; the emitter turns each name into a `#[tokio::test]`
// against a fresh durable harness.
macro_rules! emit_capability {
    ($($name:ident),* $(,)?) => {
        $(worklane_test::contract_tests!(SqliteHarness::new(); $name);)*
    };
}
macro_rules! emit_timed {
    ($($name:ident),* $(,)?) => {
        $(worklane_test::contract_tests!(TimedSqliteHarness::new(); $name);)*
    };
}
worklane_test::for_each_lifecycle_scenario!(emit_capability);
worklane_test::for_each_dead_letter_scenario!(emit_capability);
worklane_test::for_each_queue_stats_scenario!(emit_capability);
worklane_test::for_each_batch_enqueue_scenario!(emit_capability);
worklane_test::for_each_scheduled_scenario!(emit_capability);
worklane_test::for_each_timed_scenario!(emit_timed);
