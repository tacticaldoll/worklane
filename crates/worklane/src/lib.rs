//! Typed background jobs for Rust services.
//!
//! `worklane` is the public-facing facade. Enqueue typed jobs with a [`Client`]
//! and run handlers with a [`Worker`] over any [`Broker`] (for example the
//! in-memory broker in the `worklane-memory` crate).
//!
//! Core loop: typed payload -> envelope -> broker reserve -> dispatch by kind
//! -> run handler -> ack / retry / fail / dead-letter.
//!
//! Delivery is at-least-once: a job may run more than once (e.g. after a lease
//! expiry or crash), so **handlers must be idempotent**.

mod client;
mod worker;

pub use client::{Client, DEFAULT_MAX_ATTEMPTS};
pub use worker::{DEFAULT_LANE, Worker};

/// Re-exported so handlers can annotate their [`Job`] impl with
/// `#[worklane::async_trait]`.
pub use async_trait::async_trait;

pub use worklane_core::{
    Broker, DeadLetter, Error, HandlerError, HandlerResult, Job, JobContext, JobEnvelope, JobId,
    NewJob, Reservation, ReservationReceipt, Result, RetryPolicy, from_payload, to_payload,
};
