use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::{Broker, DeadLetter};

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
    /// The broker implementation under test.
    type Broker: Broker;

    /// The broker for this scenario.
    fn broker(&self) -> Arc<Self::Broker>;

    /// Inspect the dead-letter store, if this broker exposes one. `None` means
    /// the capability is absent; the suite then visibly skips dead-letter
    /// assertions rather than reporting a false pass.
    async fn dead_letters(&self, broker: &Self::Broker) -> Option<Vec<DeadLetter>>;
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
