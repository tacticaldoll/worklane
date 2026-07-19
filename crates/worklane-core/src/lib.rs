//! Core traits, job model, envelope, and errors for `worklane`.
//!
//! This crate defines the backend-agnostic contract for typed background jobs:
//! the [`Job`] trait, the opaque [`JobEnvelope`], the [`Broker`] trait that
//! stores and hands out envelopes, the [`Clock`] time source brokers derive
//! visibility and lease decisions from, the [`RetryPolicy`], and the crate-wide
//! [`Error`] type. Broker implementations (e.g. an in-memory broker) live in
//! separate crates and depend only on this one.
//!
//! Application code usually depends on the `worklane` facade instead. Depend on
//! `worklane-core` directly when implementing a broker, writing an integration
//! crate, or testing against the shared broker contract.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod broker;
mod clock;
mod envelope;
mod error;
mod id;
mod job;
mod lane;
mod observer;
mod payload;
mod payload_store;
mod redact;
mod result_store;
mod retention;
mod retry;
pub mod spi;

pub use broker::{BatchEnqueue, Broker, DeadLetterStore, JobState, QueueStats, ScheduledStore};
pub use clock::{Clock, SystemClock, WallClock};
pub use envelope::{
    DEFAULT_MAX_ATTEMPTS, DeadLetter, JobEnvelope, NewJob, Reservation, ReservationReceipt,
};
pub use error::{Error, Result};
pub use id::{JobId, JobIdParseError};
pub use job::{Cancellation, HandlerError, HandlerResult, Job, JobContext};
pub use lane::{DEFAULT_LANE, Lane, LaneError, LaneRegistry};
pub use observer::{JobAttemptEvent, JobEvent, JobObserver, JobOutcome};
pub use payload::{from_payload, to_payload};
pub use payload_store::{PayloadStore, claim_check};
pub use redact::redact_credentials;
pub use result_store::ResultStore;
pub use retention::{RetentionPolicy, UnboundedDlqWarning};
pub use retry::RetryPolicy;
