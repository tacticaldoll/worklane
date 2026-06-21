//! The shared broker conformance suite against a *file-backed* `SqliteBroker`,
//! which uses the r2d2 connection pool (WAL). The in-memory suite in
//! `broker_contract.rs` covers the single-connection path; this one exercises the
//! pool — concurrent reservers and resolvers genuinely run on separate
//! connections, so the `concurrent_*` cases prove the no-double-hand-out
//! guarantee holds under real connection concurrency, not just under a mutex.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::DeadLetter;
use worklane_sqlite::SqliteBroker;
use worklane_test::{BrokerContractHarness, ManualClock, TimedBrokerContractHarness};

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temp database path that deletes itself (and its WAL sidecars) on drop.
struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "wl-sqlite-{}-{}.db",
            std::process::id(),
            DB_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        TempDb { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        // WAL mode leaves `-wal` and `-shm` sidecars alongside the database.
        for suffix in ["", "-wal", "-shm"] {
            let mut p = self.path.clone();
            if !suffix.is_empty() {
                p.set_file_name(format!(
                    "{}{suffix}",
                    self.path.file_name().unwrap().to_string_lossy()
                ));
            }
            let _ = std::fs::remove_file(&p);
        }
    }
}

const TEST_LEASE: Duration = Duration::from_secs(30);

/// Required tier: a file-backed broker (pooled) with the default clock and lease.
/// `_db` is declared after `broker` so the broker (and its pool) drops first,
/// closing connections before the files are removed.
struct FileSqliteHarness {
    broker: Arc<SqliteBroker>,
    _db: TempDb,
}

impl FileSqliteHarness {
    fn new() -> Self {
        let db = TempDb::new();
        let broker = Arc::new(SqliteBroker::open(&db.path).expect("open file sqlite"));
        FileSqliteHarness { broker, _db: db }
    }
}

#[async_trait]
impl BrokerContractHarness for FileSqliteHarness {
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

/// Timed tier: a file-backed broker on a manual clock with a known lease.
struct TimedFileSqliteHarness {
    broker: Arc<SqliteBroker>,
    clock: Arc<ManualClock>,
    _db: TempDb,
}

impl TimedFileSqliteHarness {
    fn new() -> Self {
        let db = TempDb::new();
        let clock = Arc::new(ManualClock::new());
        let broker = Arc::new(
            SqliteBroker::open(&db.path)
                .expect("open file sqlite")
                .with_clock(clock.clone())
                .with_lease(TEST_LEASE),
        );
        TimedFileSqliteHarness {
            broker,
            clock,
            _db: db,
        }
    }
}

#[async_trait]
impl BrokerContractHarness for TimedFileSqliteHarness {
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

impl TimedBrokerContractHarness for TimedFileSqliteHarness {
    fn advance(&self, delta: Duration) {
        self.clock.advance(delta);
    }

    fn lease(&self) -> Duration {
        TEST_LEASE
    }
}

// Draw both tiers from the single-source drivers in `worklane-test`; the emitter
// turns each name into a `#[tokio::test]` against a fresh file-backed harness.
macro_rules! emit_required {
    ($($name:ident),* $(,)?) => {
        $(worklane_test::contract_tests!(FileSqliteHarness::new(); $name);)*
    };
}
macro_rules! emit_timed {
    ($($name:ident),* $(,)?) => {
        $(worklane_test::contract_tests!(TimedFileSqliteHarness::new(); $name);)*
    };
}
worklane_test::for_each_required_scenario!(emit_required);
worklane_test::for_each_timed_scenario!(emit_timed);
