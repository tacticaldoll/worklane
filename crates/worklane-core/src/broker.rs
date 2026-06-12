use std::time::Duration;

use async_trait::async_trait;

use crate::envelope::{JobEnvelope, NewJob};
use crate::error::Result;
use crate::id::JobId;

/// A backend-agnostic job store and lifecycle primitive.
///
/// A broker operates purely on opaque [`JobEnvelope`]s; it never inspects or
/// deserializes payloads and knows nothing about Rust handler types.
#[async_trait]
pub trait Broker: Send + Sync {
    /// Store a new job and return its assigned id.
    async fn enqueue(&self, job: NewJob) -> Result<JobId>;

    /// Reserve at most one currently-visible job on `lane`, hiding it for a
    /// visibility lease. Returns `None` when no job is available.
    async fn reserve(&self, lane: &str) -> Result<Option<JobEnvelope>>;

    /// Acknowledge a job as done, removing it permanently.
    async fn ack(&self, id: JobId) -> Result<()>;

    /// Increment the job's attempts and schedule it to become visible again
    /// after `delay`.
    async fn retry(&self, id: JobId, delay: Duration) -> Result<()>;

    /// Move the job to the dead-letter store, retaining `error`.
    async fn fail(&self, id: JobId, error: String) -> Result<()>;
}
