use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::id::JobId;

/// A boxed error returned by a job handler.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// The result of running a job handler.
pub type HandlerResult = std::result::Result<(), HandlerError>;

/// Per-run context handed to a job handler.
#[derive(Debug, Clone)]
pub struct JobContext {
    /// The job id.
    pub id: JobId,
    /// The number of attempts made before this one.
    pub attempts: u32,
    /// The maximum number of attempts allowed.
    pub max_attempts: u32,
}

/// A typed background job.
///
/// Implementors declare a serde-serializable [`Payload`](Job::Payload), a unique
/// [`KIND`](Job::KIND) string used for dispatch, and an async
/// [`run`](Job::run) method.
#[async_trait]
pub trait Job: Send + Sync + 'static {
    /// The payload type carried by this job.
    type Payload: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// The unique kind identifier for this job.
    const KIND: &'static str;

    /// Execute the job. Returning `Err` causes a retry (until attempts are
    /// exhausted) or dead-lettering.
    async fn run(&self, ctx: JobContext, payload: Self::Payload) -> HandlerResult;
}
