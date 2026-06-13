use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::id::JobId;

/// A job to be enqueued: the lane it targets, its kind, an already-serialized
/// payload, and how many attempts it may take before being dead-lettered.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NewJob {
    /// The lane this job is enqueued to.
    pub lane: String,
    /// The job kind, matching a [`Job::KIND`](crate::Job::KIND).
    pub kind: String,
    /// The serialized payload bytes.
    pub payload: Vec<u8>,
    /// The maximum number of attempts before the job is dead-lettered.
    pub max_attempts: u32,
}

impl NewJob {
    /// Create a job to be enqueued to `lane`.
    pub fn new(
        lane: impl Into<String>,
        kind: impl Into<String>,
        payload: Vec<u8>,
        max_attempts: u32,
    ) -> Self {
        NewJob {
            lane: lane.into(),
            kind: kind.into(),
            payload,
            max_attempts,
        }
    }
}

/// The broker's view of an enqueued job. The payload is opaque: the broker
/// never inspects or deserializes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobEnvelope {
    /// The unique job id.
    pub id: JobId,
    /// The lane this job was enqueued to.
    pub lane: String,
    /// The job kind, used to dispatch to a handler.
    pub kind: String,
    /// The opaque serialized payload bytes.
    pub payload: Vec<u8>,
    /// The number of attempts made so far.
    pub attempts: u32,
    /// The maximum number of attempts before dead-lettering.
    pub max_attempts: u32,
}

impl JobEnvelope {
    /// Create a freshly enqueued envelope on `lane` with `attempts = 0`.
    pub fn new(
        id: JobId,
        lane: impl Into<String>,
        kind: impl Into<String>,
        payload: Vec<u8>,
        max_attempts: u32,
    ) -> Self {
        JobEnvelope {
            id,
            lane: lane.into(),
            kind: kind.into(),
            payload,
            attempts: 0,
            max_attempts,
        }
    }
}

/// An opaque token proving authority to resolve a specific reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReservationReceipt(Uuid);

impl ReservationReceipt {
    /// Generate a new opaque reservation receipt.
    pub fn new() -> Self {
        ReservationReceipt(Uuid::new_v4())
    }
}

impl Default for ReservationReceipt {
    fn default() -> Self {
        Self::new()
    }
}

/// A reserved job and the receipt required to resolve it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Reservation {
    /// The reserved job envelope.
    pub envelope: JobEnvelope,
    /// The opaque receipt for this reservation instance.
    pub receipt: ReservationReceipt,
}

impl Reservation {
    /// Pair a reserved envelope with the receipt that resolves it.
    pub fn new(envelope: JobEnvelope, receipt: ReservationReceipt) -> Self {
        Reservation { envelope, receipt }
    }
}

/// A job that exhausted its attempts (or failed unrecoverably), retained for
/// inspection along with the last error message.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeadLetter {
    /// The envelope as it was when it failed.
    pub envelope: JobEnvelope,
    /// The last error message.
    pub error: String,
}

impl DeadLetter {
    /// Build a dead-letter record retaining the failing envelope and `error`.
    pub fn new(envelope: JobEnvelope, error: impl Into<String>) -> Self {
        DeadLetter {
            envelope,
            error: error.into(),
        }
    }
}
