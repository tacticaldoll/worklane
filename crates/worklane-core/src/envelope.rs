use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::id::JobId;

/// A job to be enqueued: its kind, an already-serialized payload, and how many
/// attempts it may take before being dead-lettered.
#[derive(Debug, Clone)]
pub struct NewJob {
    /// The job kind, matching a [`Job::KIND`](crate::Job::KIND).
    pub kind: String,
    /// The serialized payload bytes.
    pub payload: Vec<u8>,
    /// The maximum number of attempts before the job is dead-lettered.
    pub max_attempts: u32,
}

/// The broker's view of an enqueued job. The payload is opaque: the broker
/// never inspects or deserializes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEnvelope {
    /// The unique job id.
    pub id: JobId,
    /// The job kind, used to dispatch to a handler.
    pub kind: String,
    /// The opaque serialized payload bytes.
    pub payload: Vec<u8>,
    /// The number of attempts made so far.
    pub attempts: u32,
    /// The maximum number of attempts before dead-lettering.
    pub max_attempts: u32,
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
pub struct Reservation {
    /// The reserved job envelope.
    pub envelope: JobEnvelope,
    /// The opaque receipt for this reservation instance.
    pub receipt: ReservationReceipt,
}

/// A job that exhausted its attempts (or failed unrecoverably), retained for
/// inspection along with the last error message.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// The envelope as it was when it failed.
    pub envelope: JobEnvelope,
    /// The last error message.
    pub error: String,
}
