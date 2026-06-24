//! Reusable broker conformance suite for `worklane`.
//!
//! This crate is for broker implementors. Add it as a dev-dependency to prove a
//! custom [`Broker`](worklane_core::Broker) implementation satisfies the same
//! contract that first-party brokers must pass. Application runtime code should
//! not depend on it.
//!
//! Any [`Broker`](worklane_core::Broker) implementation can prove it satisfies
//! the core lifecycle contract (enqueue, reserve, visibility lease, receipt
//! validation, retry, fail, lane isolation, classification, and uniqueness) by
//! providing a small
//! [`BrokerContractHarness`] and enumerating the suite via the single-source
//! scenario drivers. The suite observes a broker only through the `Broker` trait
//! plus the harness adapter, so implementation conveniences never leak onto the
//! trait.
//!
//! Every backend draws its scenario set from one place — the
//! [`for_each_lifecycle_scenario`] / optional capability drivers /
//! [`for_each_timed_scenario`] / [`for_each_configured_scenario`] drivers — so a
//! scenario can never be silently dropped from one backend's hand-maintained
//! list. A backend supplies an *emitter* macro that turns each name into a
//! `#[tokio::test]` (synchronous for in-process brokers, async + env-gated for
//! brokers needing a live database) and feeds it to the driver:
//!
//! ```ignore
//! // in-process broker
//! macro_rules! emit_lifecycle {
//!     ($($name:ident),* $(,)?) => {
//!         $(worklane_test::contract_tests!(MyHarness::new(); $name);)*
//!     };
//! }
//! worklane_test::for_each_lifecycle_scenario!(emit_lifecycle);
//! ```
//!
//! The suite is split into lifecycle and optional capability batteries:
//! - [`for_each_lifecycle_scenario`] — every broker (time-free core
//!   lifecycle).
//! - [`for_each_dead_letter_scenario`] — brokers with dead-letter inspection and
//!   requeue.
//! - [`for_each_queue_stats_scenario`] — brokers with queue-depth statistics.
//! - [`for_each_batch_enqueue_scenario`] — brokers with atomic batch enqueue.
//! - [`for_each_scheduled_scenario`] — brokers with scheduled enqueue.
//! - [`for_each_timed_scenario`] — only brokers that can advance their injected
//!   clock (deterministic-time scenarios).
//! - [`for_each_configured_scenario`] — brokers built to a specific
//!   [`BrokerConfig`] (a `max_deliveries` bound or a `RetentionPolicy`) on a
//!   manual clock: the bounded-redelivery (poison) and dead-letter retention
//!   scenarios.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod clock;
mod harness;
pub mod result_store_scenarios;
/// Shared broker-contract scenario functions used by the generated test macros.
pub mod scenarios;

pub use clock::ManualClock;
pub use harness::{
    BrokerConfig, BrokerContractHarness, ConfigurableBrokerHarness, ResultStoreContractHarness,
    TimedBrokerContractHarness,
};

/// Expand each scenario name into a `#[tokio::test]` that runs `scenarios::<name>`
/// against a fresh harness built from `$harness`. The harness expression is
/// re-evaluated per test, so each scenario runs in isolation. Used by the
/// `broker_contract_*` macros; not invoked directly.
#[macro_export]
macro_rules! contract_tests {
    ($harness:expr; $($name:ident),+ $(,)?) => {
        $(
            #[::tokio::test]
            async fn $name() {
                $crate::scenarios::$name(&$harness).await;
            }
        )+
    };
}

/// Emit a visible passing test for an optional capability this broker does not
/// claim. This is useful in third-party conformance wiring because omitted
/// optional suites should be visible rather than silently absent.
#[macro_export]
macro_rules! omitted_capability_test {
    ($name:ident, $capability:literal) => {
        #[test]
        fn $name() {
            eprintln!(concat!(
                "SKIP ",
                stringify!($name),
                ": optional capability omitted: ",
                $capability
            ));
        }
    };
}

/// The single source of truth for the mandatory lifecycle scenario set.
///
/// Invokes the callback macro `$cb` with the comma-separated scenario names so
/// every backend enumerates an *identical* set. A backend can no longer drop a
/// scenario by hand-maintaining its own list — adding a name here forces all
/// backends to generate it (or fail to compile). `$cb` receives
/// `name1, name2, ...` and turns each into a `#[tokio::test]`: synchronously via
/// [`contract_tests`] for in-process brokers, or with an async, env-gated
/// harness for brokers that need a live database (Postgres, Redis).
#[macro_export]
macro_rules! for_each_lifecycle_scenario {
    ($cb:ident) => {
        $cb! {
            enqueue_then_reserve_same_lane,
            reserve_isolates_lanes,
            reserve_does_not_double_hand_out,
            ack_removes_job,
            retry_zero_delay_increments_and_revisible,
            fail_removes_live_job_and_dead_letters,
            unknown_receipt_rejected,
            enqueue_preserves_envelope_fields,
            classify_dead_lettered_after_fail,
            classify_completed_or_unknown_for_acked,
            classify_completed_or_unknown_for_unknown,
            classify_live_for_pending_and_leased,
            concurrent_reserve_no_double_handout,
            concurrent_ack_resolves_once,
            concurrent_retry_resolves_once,
            concurrent_fail_resolves_once,
            unique_enqueue_dedups_held_key,
            concurrent_unique_enqueue_dedups,
            unique_key_released_after_ack,
            unique_key_released_after_fail,
            distinct_unique_keys_not_deduped,
            unique_key_accepts_arbitrary_characters,
            no_unique_key_no_dedup,
            reserve_highest_priority_first,
            defer_reschedules_without_incrementing_attempts,
            defer_rejects_a_stale_receipt,
            enqueue_is_idempotent_on_job_id,
            distinct_job_ids_both_enqueue,
        }
    };
}

/// The single source of truth for the dead-letter inspection and maintenance
/// capability scenario set.
#[macro_export]
macro_rules! for_each_dead_letter_scenario {
    ($cb:ident) => {
        $cb! {
            read_returns_failed_job,
            read_bounded_by_limit,
            read_is_lane_scoped,
            read_preserves_opaque_envelope,
            read_empty_store,
            count_reflects_dead_lettered_jobs,
            count_is_lane_scoped,
            count_empty_store_is_zero,
            count_is_non_destructive,
            count_consistent_after_requeue,
            purge_removes_lane_dead_letters,
            purge_is_lane_scoped,
            purge_empty_lane_is_zero,
            read_succeeds_concurrent_with_requeue,
            requeue_makes_reservable_again,
            requeue_preserves_opaque_envelope,
            requeue_unknown_rejected,
            requeue_reacquires_free_unique_key,
            requeue_conflicts_when_unique_key_held,
            requeue_conflicts_when_job_id_is_live,
            classify_live_after_requeue,
            classify_is_non_destructive,
            dead_letter_accessors_present,
        }
    };
}

/// The single source of truth for the queue-depth statistics capability
/// scenario set.
#[macro_export]
macro_rules! for_each_queue_stats_scenario {
    ($cb:ident) => {
        $cb! {
            pending_count_reflects_live_jobs,
            pending_count_is_lane_scoped,
            pending_count_includes_in_flight,
            queue_stats_accessor_present,
        }
    };
}

/// The single source of truth for the atomic batch-enqueue capability scenario
/// set.
#[macro_export]
macro_rules! for_each_batch_enqueue_scenario {
    ($cb:ident) => {
        $cb! {
            batch_all_visible,
            batch_preserves_order,
            batch_intra_unique_dedup,
            batch_mixed_unique_and_plain,
            batch_concurrent_overlapping_unique_no_deadlock,
            batch_empty,
        }
    };
}

/// The single source of truth for the scheduled enqueue capability scenario set.
#[macro_export]
macro_rules! for_each_scheduled_scenario {
    ($cb:ident) => {
        $cb! {
            enqueue_scheduled_semantics,
            enqueue_scheduled_initial_state,
            enqueue_scheduled_unique_key_semantics,
            enqueue_scheduled_dedups_live_job_id,
            enqueue_scheduled_unix_second_watermark,
            remove_schedule_resets_watermark,
            concurrent_enqueue_scheduled_claims_once,
        }
    };
}

/// Expand each result-store scenario name into a `#[tokio::test]` that runs
/// `result_store_scenarios::<name>` against a fresh harness built from
/// `$harness`. Used by [`result_store_contract`]; not invoked directly.
#[macro_export]
macro_rules! result_store_tests {
    ($harness:expr; $($name:ident),+ $(,)?) => {
        $(
            #[::tokio::test]
            async fn $name() {
                $crate::result_store_scenarios::$name(&$harness).await;
            }
        )+
    };
}

/// Generate the backend-agnostic result-store contract tests over a
/// [`ResultStoreContractHarness`] expression. Each invocation builds a fresh
/// harness per test for scenario isolation. Backends whose harness construction
/// is async or environment-gated (Postgres, Redis) should hand-roll the
/// equivalent wiring with a skip guard instead of using this macro.
#[macro_export]
macro_rules! result_store_contract {
    ($harness:expr) => {
        $crate::result_store_tests!($harness;
            round_trip,
            unknown_key_returns_none,
            overwrite_replaces_value,
            distinct_keys_isolated,
        );
    };
}

/// The single source of truth for the configured-broker contract scenario set:
/// scenarios that need a broker built to a specific [`BrokerConfig`] (a
/// `max_deliveries` bound or a [`RetentionPolicy`](worklane_core::RetentionPolicy))
/// on a clock the test advances. Every broker that supports those configs
/// enumerates this set so a poison or retention scenario can never be silently
/// dropped from one backend's hand-maintained list.
///
/// `$cb` receives the scenario names and turns each into a `#[tokio::test]` that
/// builds a [`ConfigurableBrokerHarness`] (synchronous for in-process brokers, or
/// async + env-gated for brokers needing a live database) and runs
/// `scenarios::<name>` against it. See [`for_each_lifecycle_scenario`] for the
/// callback contract.
#[macro_export]
macro_rules! for_each_configured_scenario {
    ($cb:ident) => {
        $cb! {
            poison_delivery_bound_dead_letters,
            poison_skips_to_next_eligible_job,
            poison_delivery_bound_releases_unique_key,
            redelivery_unbounded_by_default,
            retention_no_policy_retains_everything,
            retention_max_count_bounds_dead_letters,
            retention_max_age_drops_on_fail,
            retention_max_age_idle_lane_lingers,
            retention_max_age_and_count_combined,
        }
    };
}

/// The single source of truth for the deterministic-time broker-contract
/// scenario set (those that advance an injected clock). Only brokers that can
/// advance their injected clock invoke this. See [`for_each_lifecycle_scenario`]
/// for the callback contract.
#[macro_export]
macro_rules! for_each_timed_scenario {
    ($cb:ident) => {
        $cb! {
            retry_delay_hides_then_exposes,
            expired_receipt_rejected_without_mutation,
            superseded_receipt_rejected_current_resolves,
            reservation_conveys_lease,
            extend_holds_past_original_lease,
            extend_after_expiry_rejected,
            superseded_receipt_cannot_extend,
            delayed_enqueue_hidden_until_due,
            reserve_oldest_within_same_priority,
            reserve_fifo_within_identical_visibility,
            retry_extreme_delay_saturates,
        }
    };
}
