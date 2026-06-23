use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::{
    Broker, DeadLetter, DeadLetterStore, QueueStats, ResultStore, RetentionPolicy, ScheduledStore,
};

use crate::ManualClock;

/// Glue a broker implementation provides so the shared contract suite can drive
/// and observe it.
///
/// One harness instance represents one scenario's context — the suite's macros
/// build a fresh harness per test, so each scenario gets an isolated broker.
/// The suite asserts only through the [`Broker`] trait plus this adapter; broker
/// conveniences (live counts, dead-letter listing) are never required on the
/// trait itself.
#[async_trait]
pub trait BrokerContractHarness: Send + Sync {
    /// The broker implementation under test. The mandatory lifecycle suite only
    /// requires the core [`Broker`] trait; optional capability suites add their
    /// own bounds when a broker opts into them.
    type Broker: Broker;

    /// The broker for this scenario.
    fn broker(&self) -> Arc<Self::Broker>;

    /// Expose this broker's [`ScheduledStore`], if it implements one. `None`
    /// means the capability is absent; the suite then visibly skips schedule
    /// assertions rather than reporting a false pass.
    fn scheduled_store(&self) -> Option<std::sync::Arc<dyn ScheduledStore>> {
        None
    }

    /// Return the broker's dead-letter records when the implementation exposes a
    /// readable dead-letter store for contract assertions. `None` (the default)
    /// means the capability is absent; the suite then visibly skips dead-letter
    /// assertions rather than reporting a false pass. Override it when your
    /// broker can enumerate its dead letters.
    async fn dead_letters(&self, _broker: &Self::Broker) -> Option<Vec<DeadLetter>> {
        None
    }
}

/// Glue a [`ResultStore`] implementation provides so the shared result-store
/// contract suite can drive it.
///
/// As with [`BrokerContractHarness`], the suite builds a fresh harness per test
/// so each scenario gets an isolated store, and observes the store only through
/// the [`ResultStore`] trait — implementation conveniences never leak onto it.
pub trait ResultStoreContractHarness: Send + Sync {
    /// The result-store implementation under test.
    type Store: ResultStore;

    /// The store for this scenario.
    fn store(&self) -> Arc<Self::Store>;
}

/// A harness whose broker derives time from a clock the test can advance,
/// enabling the deterministic-time tier of the contract suite.
pub trait TimedBrokerContractHarness: BrokerContractHarness {
    /// Advance the broker's injected clock by `delta`.
    fn advance(&self, delta: Duration);

    /// The visibility lease the broker was constructed with, so scenarios can
    /// advance past it deterministically.
    fn lease(&self) -> Duration;
}

/// The broker knobs a configured-broker scenario asks its harness to build with.
///
/// The bounded-redelivery (poison) and dead-letter-retention scenarios cannot
/// run against a default broker — they need a `max_deliveries` bound or a
/// [`RetentionPolicy`], plus a clock the test can advance to expire leases and
/// age records. Rather than each backend hand-wiring those constructors (where a
/// scenario could be silently dropped), the scenario states the config it needs
/// and the harness builds it. This keeps the configured tier on the same
/// single-source-driver footing as the required and timed tiers.
#[derive(Clone)]
pub struct BrokerConfig {
    /// The visibility lease the broker should use.
    pub lease: Duration,
    /// The optional bounded-redelivery cap; `None` leaves redelivery unbounded.
    pub max_deliveries: Option<u32>,
    /// The dead-letter retention policy; the default retains without bound.
    pub retention: RetentionPolicy,
}

impl BrokerConfig {
    /// The default visibility lease: the single source of the value scenarios
    /// build with and advance the clock past, so it cannot drift between the
    /// config default and a scenario's own copy.
    pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

    /// A default config: the [`DEFAULT_LEASE`](Self::DEFAULT_LEASE), unbounded
    /// redelivery, unbounded retention.
    pub fn new() -> Self {
        Self {
            lease: Self::DEFAULT_LEASE,
            max_deliveries: None,
            retention: RetentionPolicy::new(),
        }
    }

    /// Set the visibility lease (builder style).
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Bound redelivery at `max` deliveries (builder style).
    pub fn with_max_deliveries(mut self, max: u32) -> Self {
        self.max_deliveries = Some(max);
        self
    }

    /// Apply a dead-letter retention policy (builder style).
    pub fn with_retention(mut self, retention: RetentionPolicy) -> Self {
        self.retention = retention;
        self
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Glue for the configured-broker contract tier: a factory that builds a fresh,
/// isolated broker to the scenario's [`BrokerConfig`] on a [`ManualClock`] the
/// scenario controls.
///
/// Unlike [`BrokerContractHarness`] (which hands back one pre-built broker), a
/// configured scenario needs the broker built to *its* spec — a `max_deliveries`
/// bound for poison scenarios, a `RetentionPolicy` for retention scenarios — so
/// the trait is a factory. `build` is async so database-backed brokers (which
/// connect per build) fit; in-process brokers simply build synchronously inside
/// it. Each `build` MUST yield an isolated store (a fresh namespace/schema or a
/// fresh in-memory instance) so scenarios never see each other's records.
#[async_trait]
pub trait ConfigurableBrokerHarness: Send + Sync {
    /// The broker implementation under test. Like [`BrokerContractHarness`], the
    /// configured-tier scenarios exercise the dead-letter and queue-stats
    /// capabilities, so a tested broker must provide them.
    type Broker: Broker + DeadLetterStore + QueueStats;

    /// Build a fresh, isolated broker to `config` on a manual clock, returning
    /// both so the scenario can drive the broker and advance its time.
    async fn build(&self, config: BrokerConfig) -> (Arc<Self::Broker>, Arc<ManualClock>);
}
