//! Typed background jobs for Rust services.
//!
//! `worklane` is the public-facing facade. Enqueue typed jobs with a [`Client`]
//! and run handlers with a [`Worker`] over any [`Broker`] (for example the
//! in-memory broker in the `worklane-memory` crate).
//! Application code should usually depend on this crate plus one broker crate.
//! Lower-level crates such as `worklane-core` are for broker implementations and
//! optional integrations.
//!
//! Core loop: typed payload -> envelope -> broker reserve -> dispatch by kind
//! -> run handler -> ack / retry / fail / dead-letter.
//!
//! Delivery is at-least-once: a job may run more than once (e.g. after a lease
//! expiry or crash), so **handlers must be idempotent**.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod claim_check;
mod client;
mod client_builder;
mod worker;
mod workflow;

pub use claim_check::{ClaimCheck, DEFAULT_OFFLOAD_THRESHOLD, FilePayloadStore};
pub use client::{Client, DEFAULT_MAX_ATTEMPTS, JobBuilder};
pub use worker::{
    Building, CircuitBreaker, CircuitBreakerPolicy, DEFAULT_POLL_INTERVAL, JobAttemptEvent,
    JobEvent, JobObserver, JobOutcome, Middleware, Next, Ready, Worker, WorkerState,
};
pub use workflow::{FanInPolicy, FanInResults, Workflow};
/// The fan-in watcher job/payload are an implementation detail of [`Client::fan_in`]
/// — the runtime constructs and reschedules them. They are reachable for
/// conformance testing but are not part of the supported public API.
#[doc(hidden)]
pub use workflow::{FanInWatcherJob, FanInWatcherPayload};

/// Re-exported so handlers can annotate their [`Job`] impl with
/// `#[worklane::async_trait]`.
pub use async_trait::async_trait;

pub use worklane_core::{
    Broker, Cancellation, Clock, DEFAULT_LANE, DeadLetter, DeadLetterStore, Error, HandlerError,
    HandlerResult, Job, JobContext, JobEnvelope, JobId, JobIdParseError, JobState, Lane, LaneError,
    NewJob, PayloadStore, QueueStats, Reservation, ReservationReceipt, Result, ResultStore,
    RetryPolicy, ScheduledStore, SystemClock, WallClock, from_payload, to_payload,
};
