//! The observer SPI: a telemetry extension point for job resolution.
//!
//! A [`JobObserver`] is the symmetric counterpart to a [`Broker`](crate::Broker):
//! where a broker is the storage extension point, an observer is the telemetry
//! extension point. It lives in `worklane-core` so a telemetry integration (for
//! example `worklane-metrics`) can depend on the contract alone, without pulling
//! in the `worklane` facade and its runtime.
//!
//! The `worklane` facade re-exports these types, and its `Worker` calls the
//! observer inline as it resolves jobs.

use std::time::Duration;

/// What ultimately happened to a job, reported to a [`JobObserver`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobOutcome {
    /// The handler succeeded and the job was acked.
    Acked,
    /// The handler failed and the job was scheduled for a retry.
    Retried,
    /// The job was moved to the dead-letter store (attempts exhausted, an
    /// unrecoverable payload, an unknown kind, or a timeout with no retries left).
    DeadLettered,
}

/// A finished-job event handed to a [`JobObserver`].
///
/// Reported only when the resolution actually took effect — a resolution rejected
/// as stale (the lease was lost and the job will be redelivered) is *not* reported,
/// so a redelivered job is counted once, when it finally resolves.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct JobEvent<'a> {
    /// The lane the job ran on.
    pub lane: &'a str,
    /// The job kind.
    pub kind: &'a str,
    /// The terminal outcome.
    pub outcome: JobOutcome,
    /// Time from dispatch start to resolution (handler plus result-store and
    /// broker calls). Near zero for an unknown-kind job (no handler runs).
    pub duration: Duration,
}

impl<'a> JobEvent<'a> {
    /// Construct a job event. The worker builds these; this constructor lets
    /// observer implementors build one in their own tests despite the type being
    /// `#[non_exhaustive]`.
    pub fn new(lane: &'a str, kind: &'a str, outcome: JobOutcome, duration: Duration) -> Self {
        JobEvent {
            lane,
            kind,
            outcome,
            duration,
        }
    }
}

/// A per-attempt in-flight event handed to a [`JobObserver`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct JobAttemptEvent<'a> {
    /// The lane the job attempt is running on.
    pub lane: &'a str,
    /// The job kind.
    pub kind: &'a str,
}

impl<'a> JobAttemptEvent<'a> {
    /// Construct an attempt event. The worker builds these; this constructor lets
    /// observer implementors build one in their own tests despite the type being
    /// `#[non_exhaustive]`.
    pub fn new(lane: &'a str, kind: &'a str) -> Self {
        JobAttemptEvent { lane, kind }
    }
}

/// Observes the outcome of every job a `Worker` resolves.
///
/// A hook for telemetry — most usefully metrics (job counts by outcome, a
/// processing-duration histogram). The `worklane-metrics` crate provides an
/// implementation over the `metrics` facade. The callback runs inline on the
/// worker, so it must be cheap and non-blocking (record and return).
pub trait JobObserver: Send + Sync {
    /// Called when a reserved job attempt enters the worker's in-flight set.
    fn on_job_started(&self, _event: JobAttemptEvent<'_>) {}

    /// Called when an in-flight job attempt leaves the worker, including stale
    /// resolution, defer, timeout, or future cancellation. This pairs with
    /// [`on_job_started`](JobObserver::on_job_started) for in-flight gauges.
    fn on_job_stopped(&self, _event: JobAttemptEvent<'_>) {}

    /// Called once per job, after its resolution takes effect.
    fn on_job_finished(&self, event: JobEvent<'_>);
}
