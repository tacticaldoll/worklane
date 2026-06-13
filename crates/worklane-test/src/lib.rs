//! Reusable broker conformance suite for `worklane`.
//!
//! Any [`Broker`](worklane_core::Broker) implementation can prove it satisfies
//! the broker contract (enqueue, reserve, visibility lease, receipt validation,
//! retry, fail, dead-letter, lane isolation) by providing a small
//! [`BrokerContractHarness`] and invoking the suite macros. The suite observes a
//! broker only through the `Broker` trait plus the harness adapter, so
//! implementation conveniences never leak onto the trait.
//!
//! ```ignore
//! use worklane_test::{broker_contract_required, broker_contract_timed};
//!
//! broker_contract_required!(MyHarness::new());
//! broker_contract_timed!(MyTimedHarness::new());
//! ```
//!
//! The suite is split into two tiers, declared at the call site:
//! - [`broker_contract_required`] — every broker (time-free).
//! - [`broker_contract_timed`] — only brokers that can advance their injected
//!   clock (deterministic-time scenarios).

mod clock;
mod harness;
pub mod scenarios;

pub use clock::ManualClock;
pub use harness::{BrokerContractHarness, TimedBrokerContractHarness};

/// Generate the time-free broker contract tests over a
/// [`BrokerContractHarness`] expression. Each invocation builds a fresh harness
/// per test for scenario isolation.
#[macro_export]
macro_rules! broker_contract_required {
    ($harness:expr) => {
        #[::tokio::test]
        async fn contract_enqueue_then_reserve_same_lane() {
            $crate::scenarios::enqueue_then_reserve_same_lane(&$harness).await;
        }
        #[::tokio::test]
        async fn contract_reserve_isolates_lanes() {
            $crate::scenarios::reserve_isolates_lanes(&$harness).await;
        }
        #[::tokio::test]
        async fn contract_reserve_does_not_double_hand_out() {
            $crate::scenarios::reserve_does_not_double_hand_out(&$harness).await;
        }
        #[::tokio::test]
        async fn contract_ack_removes_job() {
            $crate::scenarios::ack_removes_job(&$harness).await;
        }
        #[::tokio::test]
        async fn contract_retry_zero_delay_increments_and_revisible() {
            $crate::scenarios::retry_zero_delay_increments_and_revisible(&$harness).await;
        }
        #[::tokio::test]
        async fn contract_fail_removes_live_job_and_dead_letters() {
            $crate::scenarios::fail_removes_live_job_and_dead_letters(&$harness).await;
        }
        #[::tokio::test]
        async fn contract_unknown_receipt_rejected() {
            $crate::scenarios::unknown_receipt_rejected(&$harness).await;
        }
    };
}

/// Generate the deterministic-time broker contract tests over a
/// [`TimedBrokerContractHarness`] expression. Only brokers that can advance
/// their injected clock should invoke this.
#[macro_export]
macro_rules! broker_contract_timed {
    ($harness:expr) => {
        #[::tokio::test]
        async fn contract_retry_delay_hides_then_exposes() {
            $crate::scenarios::retry_delay_hides_then_exposes(&$harness).await;
        }
        #[::tokio::test]
        async fn contract_expired_receipt_rejected_without_mutation() {
            $crate::scenarios::expired_receipt_rejected_without_mutation(&$harness).await;
        }
        #[::tokio::test]
        async fn contract_superseded_receipt_rejected_current_resolves() {
            $crate::scenarios::superseded_receipt_rejected_current_resolves(&$harness).await;
        }
    };
}
