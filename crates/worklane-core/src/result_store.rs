use crate::error::Result;
use crate::id::JobId;
use async_trait::async_trait;

/// A pluggable backend for storing opaque job results.
///
/// This trait is strictly separated from `Broker`, preserving the "opaque
/// payload, minimal contract" core loop. A `ResultStore` acts purely as
/// an egress point for successful jobs before they are acknowledged.
#[async_trait]
pub trait ResultStore: Send + Sync + 'static {
    /// Store an opaque byte payload representing the successful return
    /// value of a job.
    ///
    /// Any TTL (Time-To-Live) constraints should be enforced internally by
    /// the backend configuration rather than via this API.
    async fn store(&self, job_id: &JobId, result: &[u8]) -> Result<()>;

    /// Retrieve the stored opaque byte payload for a job, if it exists.
    async fn get(&self, job_id: &JobId) -> Result<Option<Vec<u8>>>;
}
