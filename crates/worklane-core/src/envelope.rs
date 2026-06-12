use serde::{Deserialize, Serialize};

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

/// A job that exhausted its attempts (or failed unrecoverably), retained for
/// inspection along with the last error message.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// The envelope as it was when it failed.
    pub envelope: JobEnvelope,
    /// The last error message.
    pub error: String,
}
