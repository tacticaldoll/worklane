use std::time::Duration;

use async_trait::async_trait;

use crate::envelope::{NewJob, Reservation, ReservationReceipt};
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
    async fn reserve(&self, lane: &str) -> Result<Option<Reservation>>;

    /// Acknowledge a reservation as done, removing its job permanently.
    async fn ack(&self, receipt: ReservationReceipt) -> Result<()>;

    /// Increment the reserved job's attempts and schedule it to become visible
    /// again after `delay`.
    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()>;

    /// Move the reserved job to the dead-letter store, retaining `error`.
    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()>;
}
