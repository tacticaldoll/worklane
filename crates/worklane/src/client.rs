use std::sync::Arc;

use worklane_core::{Broker, Job, JobId, NewJob, Result, to_payload};

/// The default `max_attempts` applied to enqueued jobs.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// Enqueues typed jobs onto a broker.
pub struct Client {
    broker: Arc<dyn Broker>,
    default_max_attempts: u32,
}

impl Client {
    /// Create a client over the given broker.
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Client {
            broker,
            default_max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Set the default `max_attempts` for enqueued jobs (builder style).
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.default_max_attempts = max_attempts;
        self
    }

    /// Enqueue a typed job. The payload is serialized before submission; a
    /// serialization failure returns an error and submits nothing.
    pub async fn enqueue<J: Job>(&self, payload: J::Payload) -> Result<JobId> {
        let bytes = to_payload(&payload)?;
        let job = NewJob {
            kind: J::KIND.to_string(),
            payload: bytes,
            max_attempts: self.default_max_attempts,
        };
        self.broker.enqueue(job).await
    }
}
