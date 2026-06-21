//! The per-job lifecycle: dispatch one reserved job to its handler and resolve
//! the outcome (ack / retry / fail), honouring at-least-once semantics. The
//! orchestration that decides *when* to run a job (reserve, concurrency,
//! shutdown, poll) lives in the parent module; this is the unit it drives.

use std::any::Any;
use std::borrow::Cow;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use worklane_core::{
    Broker, Cancellation, Error, JobContext, JobEnvelope, JobId, Reservation, ReservationReceipt,
    Result,
};

/// Render a caught panic payload as a handler-error message, best effort.
fn panic_message(payload: Box<dyn Any + Send>) -> String {
    let detail = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "handler panicked".to_string());
    format!("handler panicked: {detail}")
}

/// The outcome of running a handler future, possibly under a timeout.
enum HandlerOutcome {
    /// The handler completed (successfully or with an error).
    Completed(Result<Vec<u8>>),
    /// The handler did not finish within its configured timeout.
    TimedOut(Duration),
}

use std::sync::Arc;
use worklane_core::RetryPolicy;

use super::{Dispatch, JobAttemptEvent, JobEvent, JobOutcome};

/// Aborts the wrapped task when dropped. The heartbeat in `run_maintained` runs
/// on a detached `tokio::spawn`ed task, so it is *not* a child of the `process`
/// future: if that future is hard-cancelled (dropped mid-job, e.g. the operator
/// drops `run` instead of resolving its shutdown signal), the normal
/// fall-through `abort()` never executes and the orphaned heartbeat would keep
/// extending the lease — suppressing the at-least-once redelivery the worker
/// promises for an abandoned job. Tying the handle's lifetime to this guard
/// restores the inline-`select!` semantics: a dropped `run_maintained` tears the
/// heartbeat down too.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct InFlightGuard {
    observer: Option<Arc<dyn super::JobObserver>>,
    lane: String,
    kind: String,
}

impl InFlightGuard {
    fn new(
        observer: Option<Arc<dyn super::JobObserver>>,
        lane: &worklane_core::Lane,
        kind: &str,
    ) -> Self {
        let guard = InFlightGuard {
            observer,
            lane: lane.to_string(),
            kind: kind.to_string(),
        };
        if let Some(observer) = &guard.observer {
            observer.on_job_started(JobAttemptEvent::new(&guard.lane, &guard.kind));
        }
        guard
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Some(observer) = &self.observer {
            observer.on_job_stopped(JobAttemptEvent::new(&self.lane, &self.kind));
        }
    }
}

/// The context required to process a single job, cloneable for spawning tasks.
#[derive(Clone)]
pub(super) struct ProcessCtx {
    pub broker: Arc<dyn Broker>,
    pub result_store: Option<Arc<dyn worklane_core::ResultStore>>,
    pub payload_store: Option<Arc<dyn worklane_core::PayloadStore>>,
    pub observer: Option<Arc<dyn super::JobObserver>>,
    pub circuit_breaker: Option<Arc<super::CircuitBreaker>>,
    pub middleware: Vec<Arc<dyn super::Middleware>>,
    pub retry: RetryPolicy,
    pub handler_timeout: Option<Duration>,
    pub lease_keepalive: bool,
}

impl ProcessCtx {
    /// Dispatch a reserved job to its handler and resolve the outcome with the
    /// reservation's receipt.
    pub(super) async fn process(
        &self,
        dispatch: Option<Arc<dyn Dispatch>>,
        reservation: Reservation,
    ) -> Result<()> {
        let receipt = reservation.receipt;
        let lease = reservation.lease;
        let envelope = reservation.envelope;
        let id = envelope.id;
        // Processing-duration clock for the observer: from dispatch start to
        // resolution.
        let started = Instant::now();
        // Hand the handler a cancellation flag the worker flips if it abandons the
        // lease (timeout or lease loss), so a cooperative handler can bail early.
        let cancellation = Cancellation::new();
        let ctx = JobContext::from(&envelope).with_cancellation(cancellation.clone());
        let _in_flight = InFlightGuard::new(self.observer.clone(), &envelope.lane, &envelope.kind);

        let Some(dispatch) = dispatch else {
            tracing::warn!(job_id = %id, kind = %envelope.kind, "no handler; dead-lettering");
            return self
                .resolve(
                    id,
                    &envelope,
                    JobOutcome::DeadLettered,
                    started,
                    self.broker
                        .fail(receipt, format!("unknown job kind: {}", envelope.kind))
                        .await,
                )
                .await;
        };

        // Circuit breaker: if this kind's circuit is open, short-circuit dispatch
        // — defer the job (no attempt spent) for the remaining cooldown instead of
        // running a handler that is likely to fail again. Done before resolving the
        // payload so a deferred job does no work at all.
        if let Some(breaker) = &self.circuit_breaker {
            if let Some(delay) = breaker.admit(&envelope.kind) {
                tracing::debug!(job_id = %id, kind = %envelope.kind, ?delay, "circuit open; deferring job without spending an attempt");
                return match self.broker.defer(receipt, delay).await {
                    // A stale defer just means the lease was already lost (the job
                    // will be redelivered) — not an error for the worker.
                    Ok(()) | Err(Error::StaleReservation(_)) => Ok(()),
                    Err(err) => Err(err),
                };
            }
        }

        // Resolve a Claim Check reference back to the real payload before
        // dispatch. A non-reference payload passes through borrowed (no copy). A
        // fetch failure (store down, or a dangling reference) is a job failure, not
        // a worker crash: route it through the normal retry/dead-letter path. Note
        // the fetch happens before the heartbeat starts, so it counts against the
        // initial lease window — size the lease (or enable keepalive) for large
        // offloaded payloads.
        let payload = match self.resolve_payload(&envelope.payload).await {
            Ok(payload) => payload,
            Err(err) => {
                tracing::warn!(job_id = %id, error = %err, "failed to resolve claim-check payload");
                return self
                    .handle_failure(id, receipt, &envelope, started, err)
                    .await;
            }
        };

        tracing::info!(job_id = %id, kind = %envelope.kind, attempt = envelope.attempts.saturating_add(1), "dispatching job");
        // Run the middleware chain (outermost first), terminating at the handler.
        // With no middleware configured this is just the handler call.
        let chain = super::Next::new(&self.middleware, dispatch.as_ref());
        // Contain a panic that unwinds out of the handler (or a middleware): catch
        // it and surface it as a handler error, so it flows through the normal
        // failure path (retry / dead-letter) instead of crashing the worker and
        // abandoning sibling in-flight jobs. `AssertUnwindSafe` is sound here
        // because a panicking job is discarded, never resumed, so no state leaks.
        let handler = AssertUnwindSafe(chain.run(ctx, payload.as_ref()))
            .catch_unwind()
            .map(|caught| match caught {
                Ok(result) => result,
                Err(payload) => Err(Error::Handler(panic_message(payload))),
            });
        // Maintain the lease (heartbeat) while the handler runs when either a
        // handler timeout or lease keepalive is configured; the timeout, if set,
        // also bounds a stuck handler. With neither, behave exactly as before:
        // just run the handler with no heartbeat, relying on lease expiry and
        // at-least-once redelivery.
        let outcome = if self.handler_timeout.is_some() || self.lease_keepalive {
            self.run_maintained(
                handler,
                receipt,
                lease,
                self.handler_timeout,
                id,
                &cancellation,
            )
            .await
        } else {
            HandlerOutcome::Completed(handler.await)
        };

        match outcome {
            HandlerOutcome::Completed(Ok(output_bytes)) => {
                // The handler returned Ok, but the job has not *completed* until its
                // result is stored and the ack lands. Recording breaker success here
                // (before those) would reset the breaker even when the job then
                // fails to complete — so a kind whose result-store write always fails
                // would loop forever without the breaker ever tripping, defeating
                // its purpose. Credit success only after a real ack (below).
                if let Some(store) = &self.result_store {
                    tracing::debug!(job_id = %id, "storing result");
                    if let Err(err) = store.store(&id, &output_bytes).await {
                        tracing::warn!(job_id = %id, error = %err, "failed to store result; failing job");
                        // The attempt did not complete: count it against the breaker,
                        // like any other failed attempt, then route to the failure path.
                        self.record_breaker(&envelope.kind, false);
                        return self
                            .handle_failure(id, receipt, &envelope, started, err)
                            .await;
                    }
                }

                tracing::info!(job_id = %id, "job succeeded; ack");
                let acked = self.broker.ack(receipt).await;
                if acked.is_ok() {
                    // The job is fully committed: now the kind's dependency is proven
                    // healthy.
                    self.record_breaker(&envelope.kind, true);
                    // Drop any Claim Check blob it carried. Best effort — a delete
                    // failure only leaves an orphan blob (the store tolerates a later
                    // sweep), so it must not fail an already-acked job. A dead-lettered
                    // job keeps its blob (see `handle_failure`).
                    //
                    // Deleting the blob here is race-free against a redelivery, even
                    // though `resolve_payload` treats a missing blob as a failure.
                    // The delete is gated on a *successful* ack, and ack is a
                    // receipt+lease CAS on a single-valued `receipt` field (uniform
                    // across all backends): it succeeds only if this worker's receipt
                    // is still current and the lease unexpired, and it removes the row
                    // atomically. A second worker can only be resolving this job if it
                    // re-reserved it, which overwrites `receipt` — making this ack fail
                    // and skip the delete. So a successful ack proves sole ownership,
                    // and the blob (distinct per job) is never deleted out from under a
                    // concurrent resolver.
                    self.delete_payload(id, &envelope.payload).await;
                }
                // A failed ack is a lost lease (redelivery), not a dependency-health
                // signal, so the breaker is left untouched rather than recorded as a
                // failure — penalizing lease churn would trip it for the wrong reason.
                self.resolve(id, &envelope, JobOutcome::Acked, started, acked)
                    .await
            }
            HandlerOutcome::Completed(Err(Error::Serialization(msg))) => {
                // The payload will never deserialize; dead-letter immediately.
                tracing::warn!(job_id = %id, error = %msg, "payload error; dead-lettering");
                self.resolve(
                    id,
                    &envelope,
                    JobOutcome::DeadLettered,
                    started,
                    self.broker
                        .fail(
                            receipt,
                            worklane_core::redact_credentials(&format!(
                                "serialization error: {msg}"
                            )),
                        )
                        .await,
                )
                .await
            }
            HandlerOutcome::Completed(Err(err)) => {
                // A handler failure counts against the kind's circuit breaker.
                self.record_breaker(&envelope.kind, false);
                self.handle_failure(id, receipt, &envelope, started, err)
                    .await
            }
            HandlerOutcome::TimedOut(timeout) => {
                tracing::warn!(job_id = %id, ?timeout, "handler exceeded its timeout; failing");
                // A timeout is a failure for the breaker (a stuck dependency).
                self.record_breaker(&envelope.kind, false);
                let err = Error::Handler(format!("handler timed out after {timeout:?}"));
                self.handle_failure(id, receipt, &envelope, started, err)
                    .await
            }
        }
    }

    /// Resolve a job's stored payload to the bytes the handler sees. A Claim Check
    /// reference is fetched from the payload store; any other payload is returned
    /// borrowed (no copy). Errors if a reference is present but no store is
    /// configured, or the blob is missing — both are job failures, not panics.
    async fn resolve_payload<'a>(&self, payload: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        let Some(key) = worklane_core::claim_check::reference_key(payload) else {
            return Ok(Cow::Borrowed(payload));
        };
        let Some(store) = &self.payload_store else {
            return Err(Error::Handler(
                "job payload is a claim-check reference but the worker has no payload store \
                 configured (call Worker::with_payload_store)"
                    .to_string(),
            ));
        };
        match store.get(key).await? {
            Some(bytes) => Ok(Cow::Owned(bytes)),
            None => Err(Error::Handler(format!(
                "claim-check payload {key} is missing from the store"
            ))),
        }
    }

    /// Record a handler outcome for `kind` against the circuit breaker, if one is
    /// configured. A payload (serialization) error is the job's own fault, not a
    /// dependency's, so callers do not record it.
    fn record_breaker(&self, kind: &str, success: bool) {
        if let Some(breaker) = &self.circuit_breaker {
            breaker.record(kind, success);
        }
    }

    /// Best-effort delete of a job's Claim Check blob after a successful ack. A
    /// non-reference payload or an absent store is a no-op; a delete error is
    /// logged and swallowed (it only orphans a blob, never fails an acked job).
    async fn delete_payload(&self, id: JobId, payload: &[u8]) {
        let Some(key) = worklane_core::claim_check::reference_key(payload) else {
            return;
        };
        let Some(store) = &self.payload_store else {
            return;
        };
        if let Err(err) = store.delete(key).await {
            tracing::warn!(job_id = %id, error = %err, "failed to delete claim-check payload after ack; blob orphaned");
        }
    }

    /// Run `handler` while maintaining its reservation lease, heartbeating every
    /// `lease / 3` to extend it so a slow handler is not redelivered. The third
    /// fraction (rather than a half) leaves margin for the `extend` round-trip to
    /// a remote broker to complete before the current lease expires. Returns once
    /// the handler completes, or — when `timeout` is `Some` — once the timeout
    /// elapses; when `timeout` is `None` (keepalive without a deadline) the lease
    /// is held for as long as the handler runs. If a heartbeat is rejected (the
    /// lease was already lost, e.g. redelivery), it stops extending and lets the
    /// handler finish — its later resolution is then stale-rejected and logged,
    /// exactly as for any lost lease.
    async fn run_maintained<F>(
        &self,
        handler: F,
        receipt: ReservationReceipt,
        lease: Duration,
        timeout: Option<Duration>,
        id: JobId,
        cancellation: &Cancellation,
    ) -> HandlerOutcome
    where
        F: Future<Output = Result<Vec<u8>>>,
    {
        tokio::pin!(handler);
        let heartbeat_every = (lease / 3).max(Duration::from_millis(50));

        // Heartbeat on its OWN task so lease extension ticks independently of how
        // often the handler yields back to the runtime: a slow (or briefly
        // CPU-bound) handler can no longer starve its own heartbeat on a
        // multi-thread runtime, since the extend runs on a different worker thread
        // rather than waiting for this task's `select!` to be re-polled. (On a
        // current-thread runtime a non-yielding handler still blocks the single
        // executor thread — run such work via `spawn_blocking`.) On a rejected
        // extend it signals cancellation and stops, exactly as the inline version.
        let heartbeat = AbortOnDrop(tokio::spawn({
            let broker = self.broker.clone();
            let cancellation = cancellation.clone();
            async move {
                loop {
                    tokio::time::sleep(heartbeat_every).await;
                    match broker.extend(receipt).await {
                        Ok(()) => tracing::trace!(job_id = %id, "lease extended by heartbeat"),
                        Err(Error::StaleReservation(msg)) => {
                            tracing::warn!(job_id = %id, error = %msg, "heartbeat rejected as stale; lease lost, stop extending");
                            // The lease is gone; signal cancellation so a
                            // cooperative handler bails rather than doing work that
                            // will be redelivered.
                            cancellation.cancel();
                            break;
                        }
                        Err(err) => {
                            tracing::error!(job_id = %id, error = %err, "heartbeat extend failed; stop extending");
                            cancellation.cancel();
                            break;
                        }
                    }
                }
            }
        }));

        // The handler runs on THIS task (so it needn't be `Send`). A `Some`
        // timeout races it via the deadline arm; a `None` keepalive leaves the
        // deadline permanently pending.
        let deadline = async {
            match timeout {
                Some(t) => tokio::time::sleep(t).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(deadline);
        let outcome = tokio::select! {
            biased;
            res = &mut handler => HandlerOutcome::Completed(res),
            _ = &mut deadline => {
                // Timed out: the handler future is dropped on return (cancelled at
                // its next await); signal the flag for cooperative handlers too.
                cancellation.cancel();
                HandlerOutcome::TimedOut(timeout.unwrap_or_default())
            }
        };
        // Handler resolved (or timed out): stop heartbeating. Dropping the guard
        // aborts the task here on the normal path; the `AbortOnDrop` wrapper also
        // covers the cancellation path where this future is dropped before we get
        // here. An extend already in flight is harmless — it is stale-rejected
        // once the job is ack'd/failed.
        drop(heartbeat);
        outcome
    }

    async fn handle_failure(
        &self,
        id: JobId,
        receipt: ReservationReceipt,
        envelope: &JobEnvelope,
        started: Instant,
        err: Error,
    ) -> Result<()> {
        let completed = envelope.attempts.saturating_add(1);
        if completed < envelope.max_attempts {
            // Seed retry jitter from the job id so jobs that fail in lockstep
            // spread their retries (a no-op unless `RetryPolicy::jitter` is set).
            let seed = {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                id.hash(&mut h);
                h.finish()
            };
            let delay = self.retry.delay_for_seeded(envelope.attempts, seed);
            // Scrub the handler error before it reaches the log, the same way the
            // dead-letter reason is scrubbed below: a handler that embeds a
            // connection string must not leak credentials to either surface.
            let reason = worklane_core::redact_credentials(&err.to_string());
            tracing::warn!(job_id = %id, attempt = completed, ?delay, error = %reason, "job failed; retrying");
            self.resolve(
                id,
                envelope,
                JobOutcome::Retried,
                started,
                self.broker.retry(receipt, delay).await,
            )
            .await
        } else {
            // The handler error becomes the persisted dead-letter reason (and is
            // shown by the CLI); a handler that embeds a connection string would
            // otherwise leak credentials to the log and the store, so scrub it once
            // at this boundary — the same redaction the backend error mappers apply
            // — and use the scrubbed text for both.
            let reason = worklane_core::redact_credentials(&err.to_string());
            tracing::warn!(job_id = %id, attempt = completed, error = %reason, "job failed; dead-lettering");
            self.resolve(
                id,
                envelope,
                JobOutcome::DeadLettered,
                started,
                self.broker.fail(receipt, reason).await,
            )
            .await
        }
    }

    /// Resolve a terminal broker call and, on real success, report the outcome to
    /// the observer. A stale rejection (lost lease → redelivery) is swallowed and
    /// **not** reported, so a redelivered job is counted once, when it resolves.
    async fn resolve(
        &self,
        id: JobId,
        envelope: &JobEnvelope,
        outcome: JobOutcome,
        started: Instant,
        result: Result<()>,
    ) -> Result<()> {
        match result {
            Ok(()) => {
                if let Some(observer) = &self.observer {
                    observer.on_job_finished(JobEvent::new(
                        envelope.lane.as_str(),
                        &envelope.kind,
                        outcome,
                        started.elapsed(),
                    ));
                }
                Ok(())
            }
            Err(Error::StaleReservation(msg)) => {
                tracing::warn!(job_id = %id, kind = %envelope.kind, error = %msg, "resolution rejected as stale; continuing");
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}
