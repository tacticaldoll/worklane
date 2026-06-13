//! `SqliteBroker` runs the shared broker conformance suite from `worklane-test`,
//! proving the durable backend satisfies the broker contract without any change
//! to the `Broker` trait.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::DeadLetter;
use worklane_sqlite::SqliteBroker;
use worklane_test::{
    BrokerContractHarness, ManualClock, TimedBrokerContractHarness, broker_contract_required,
    broker_contract_timed,
};

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

    async fn dead_letters(&self, broker: &SqliteBroker) -> Option<Vec<DeadLetter>> {
        Some(broker.dead_letters().expect("dead-letter query"))
    }
}

/// Timed tier: a broker on a manual clock with a known lease.
const TEST_LEASE: Duration = Duration::from_secs(30);

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

broker_contract_required!(SqliteHarness::new());
broker_contract_timed!(TimedSqliteHarness::new());
