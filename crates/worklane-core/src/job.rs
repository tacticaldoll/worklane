use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::envelope::JobEnvelope;
use crate::id::JobId;
use crate::lane::Lane;

/// A cooperative cancellation flag for a running job.
///
/// The worker flips it when it stops maintaining the job's reservation lease —
/// because the lease was lost (a heartbeat came back stale, so the job will be
/// redelivered) or the handler timeout elapsed. A long-running, cooperative
/// handler can poll [`JobContext::is_cancelled`] at safe points and return early
/// to stop doing work that will be thrown away. It is *advisory*: ignoring it is
/// safe (delivery is at-least-once regardless), and a handler with no cancellation
/// checks behaves exactly as before. Cloning shares the underlying flag.
#[derive(Debug, Clone, Default)]
pub struct Cancellation(Arc<AtomicBool>);

impl Cancellation {
    /// A fresh, un-cancelled flag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal cancellation. Idempotent.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether cancellation has been signalled.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// A boxed error returned by a job handler.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// The result of running a job handler.
pub type HandlerResult<T> = std::result::Result<T, HandlerError>;

/// Per-run context handed to a job handler.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobContext {
    /// The job id.
    pub id: JobId,
    /// The lane the job was reserved from.
    pub lane: Lane,
    /// The number of attempts made before this one.
    pub attempts: u32,
    /// The maximum number of attempts allowed.
    pub max_attempts: u32,
    /// The priority of the job.
    pub priority: u8,
    /// The job kind.
    pub kind: String,
    /// Optional W3C TraceContext propagation headers carried on the envelope,
    /// exposed so a handler can read or forward them without re-parsing. `None`
    /// when the caller injected no trace context.
    pub trace_context: Option<HashMap<String, String>>,
    /// Cooperative cancellation for this run (see [`Cancellation`]). Defaults to a
    /// never-cancelled flag; the worker injects a shared one via
    /// [`with_cancellation`](JobContext::with_cancellation) and flips it when it
    /// abandons the lease.
    cancellation: Cancellation,
}

impl JobContext {
    /// Build the per-run context for a dispatched job.
    pub fn new(
        id: JobId,
        lane: Lane,
        attempts: u32,
        max_attempts: u32,
        priority: u8,
        kind: String,
        trace_context: Option<HashMap<String, String>>,
    ) -> Self {
        JobContext {
            id,
            lane,
            attempts,
            max_attempts,
            priority,
            kind,
            trace_context,
            cancellation: Cancellation::new(),
        }
    }

    /// Attach a shared [`Cancellation`] (builder style). The worker uses this to
    /// hand the handler the same flag it flips on lease loss or timeout.
    #[must_use = "this value must be used"]
    pub fn with_cancellation(mut self, cancellation: Cancellation) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Whether the worker has signalled cooperative cancellation for this run —
    /// the lease was lost (the job will be redelivered) or the handler timed out.
    /// A cooperative handler can check this at safe points and return early to
    /// avoid wasting work; ignoring it is safe.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

impl From<&JobEnvelope> for JobContext {
    /// Project the per-run context from a reserved envelope, keeping the
    /// envelope-to-context field mapping in one place.
    fn from(envelope: &JobEnvelope) -> Self {
        JobContext::new(
            envelope.id,
            envelope.lane.clone(),
            envelope.attempts,
            envelope.max_attempts,
            envelope.priority,
            envelope.kind.clone(),
            envelope.trace_context.clone(),
        )
    }
}

/// A typed background job.
///
/// Implementors declare a serde-serializable [`Payload`](Job::Payload), a unique
/// [`KIND`](Job::KIND) string used for dispatch, and an async
/// [`run`](Job::run) method.
///
/// **Handlers must be idempotent.** Delivery is at-least-once: a lease that
/// expires before the job is resolved makes the job visible again, so a handler
/// can run more than once for the same job. This happens on a worker crash, but
/// also when the broker's wall clock steps forward (e.g. an NTP jump) past a
/// reserved job's remaining lease — expiring it while the original handler is
/// still running. `run` must therefore tolerate re-execution (e.g. guard side
/// effects with the [`JobContext::id`] or an external dedup key); the broker does
/// not prevent duplicate execution.
#[async_trait]
pub trait Job: Send + Sync + 'static {
    /// The payload type carried by this job.
    type Payload: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// The output type returned by this job upon success.
    type Output: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// The unique kind identifier for this job.
    const KIND: &'static str;

    /// Execute the job. Returning `Err` causes a retry (until attempts are
    /// exhausted) or dead-lettering.
    async fn run(&self, ctx: JobContext, payload: Self::Payload) -> HandlerResult<Self::Output>;
}
