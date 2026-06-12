//! Core traits, job model, envelope, and errors for `worklane`.
//!
//! This crate defines the backend-agnostic contract for typed background jobs:
//! the [`Job`] trait, the opaque [`JobEnvelope`], the [`Broker`] trait that
//! stores and hands out envelopes, the [`RetryPolicy`], and the crate-wide
//! [`Error`] type. Broker implementations (e.g. an in-memory broker) live in
//! separate crates and depend only on this one.

mod broker;
mod envelope;
mod error;
mod id;
mod job;
mod payload;
mod retry;

pub use broker::Broker;
pub use envelope::{DeadLetter, JobEnvelope, NewJob, Reservation, ReservationReceipt};
pub use error::{Error, Result};
pub use id::JobId;
pub use job::{HandlerError, HandlerResult, Job, JobContext};
pub use payload::{from_payload, to_payload};
pub use retry::RetryPolicy;
