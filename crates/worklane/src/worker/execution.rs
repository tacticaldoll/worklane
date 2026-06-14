//! The per-job lifecycle: dispatch one reserved job to its handler and resolve
//! the outcome (ack / retry / fail), honouring at-least-once semantics. The
//! orchestration that decides *when* to run a job (reserve, concurrency,
//! shutdown, poll) lives in the parent module; this is the unit it drives.

use std::future::Future;
use std::time::Duration;

use worklane_core::{
    Error, JobContext, JobEnvelope, JobId, Reservation, ReservationReceipt, Result,
};

use super::Worker;

/// The outcome of running a handler future, possibly under a timeout.
enum HandlerOutcome {
    /// The handler completed (successfully or with an error).
    Completed(Result<()>),
    /// The handler did not finish within its configured timeout.
    TimedOut(Duration),
}

impl Worker {
    /// Dispatch a reserved job to its handler and resolve the outcome with the
    /// reservation's receipt.
    pub(super) async fn process(&self, reservation: Reservation) -> Result<()> {
        let receipt = reservation.receipt;
        let lease = reservation.lease;
        let envelope = reservation.envelope;
        let id = envelope.id;
        let ctx = JobContext::new(id, envelope.attempts, envelope.max_attempts);

        let Some(dispatch) = self.handlers.get(envelope.kind.as_str()) else {
            tracing::warn!(job_id = %id, kind = %envelope.kind, "no handler; dead-lettering");
            return self
                .resolve(
                    id,
                    &envelope.kind,
                    self.broker
                        .fail(receipt, format!("unknown job kind: {}", envelope.kind))
                        .await,
                )
                .await;
        };

        tracing::info!(job_id = %id, kind = %envelope.kind, attempt = envelope.attempts + 1, "dispatching job");
        let handler = dispatch.dispatch(ctx, &envelope.payload);
        // With no handler timeout configured, behave exactly as before: just run
        // the handler. With one configured, heartbeat to hold the lease while it
        // runs, and abandon it at the timeout.
        let outcome = match self.handler_timeout {
            None => HandlerOutcome::Completed(handler.await),
            Some(timeout) => self.run_bounded(handler, receipt, lease, timeout, id).await,
        };

        match outcome {
            HandlerOutcome::Completed(Ok(())) => {
                tracing::info!(job_id = %id, "job succeeded; ack");
                self.resolve(id, &envelope.kind, self.broker.ack(receipt).await)
                    .await
            }
            HandlerOutcome::Completed(Err(Error::Serialization(msg))) => {
                // The payload will never deserialize; dead-letter immediately.
                tracing::warn!(job_id = %id, error = %msg, "payload error; dead-lettering");
                self.resolve(
                    id,
                    &envelope.kind,
                    self.broker
                        .fail(receipt, format!("serialization error: {msg}"))
                        .await,
                )
                .await
            }
            HandlerOutcome::Completed(Err(err)) => {
                self.handle_failure(id, receipt, &envelope, err).await
            }
            HandlerOutcome::TimedOut(timeout) => {
                tracing::warn!(job_id = %id, ?timeout, "handler exceeded its timeout; failing");
                let err = Error::Handler(format!("handler timed out after {timeout:?}"));
                self.handle_failure(id, receipt, &envelope, err).await
            }
        }
    }

    /// Run `handler` under `timeout`, heartbeating every `lease / 2` to extend
    /// the reservation so a slow handler is not redelivered. Returns once the
    /// handler completes or the timeout elapses. If a heartbeat is rejected
    /// (the lease was already lost, e.g. redelivery), it stops extending and
    /// lets the handler finish — its later resolution is then stale-rejected and
    /// logged, exactly as for any lost lease.
    async fn run_bounded<F>(
        &self,
        handler: F,
        receipt: ReservationReceipt,
        lease: Duration,
        timeout: Duration,
        id: JobId,
    ) -> HandlerOutcome
    where
        F: Future<Output = Result<()>>,
    {
        tokio::pin!(handler);
        let heartbeat_every = (lease / 2).max(Duration::from_millis(1));
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        let mut heartbeating = true;

        loop {
            let tick = tokio::time::sleep(heartbeat_every);
            tokio::select! {
                biased;
                res = &mut handler => return HandlerOutcome::Completed(res),
                _ = &mut deadline => return HandlerOutcome::TimedOut(timeout),
                _ = tick, if heartbeating => match self.broker.extend(receipt).await {
                    Ok(()) => tracing::trace!(job_id = %id, "lease extended by heartbeat"),
                    Err(Error::StaleReservation(msg)) => {
                        tracing::warn!(job_id = %id, error = %msg, "heartbeat rejected as stale; lease lost, stop extending");
                        heartbeating = false;
                    }
                    Err(err) => {
                        tracing::error!(job_id = %id, error = %err, "heartbeat extend failed; stop extending");
                        heartbeating = false;
                    }
                },
            }
        }
    }

    async fn handle_failure(
        &self,
        id: JobId,
        receipt: ReservationReceipt,
        envelope: &JobEnvelope,
        err: Error,
    ) -> Result<()> {
        let completed = envelope.attempts + 1;
        if completed < envelope.max_attempts {
            let delay = self.retry.delay_for(envelope.attempts);
            tracing::warn!(job_id = %id, attempt = completed, ?delay, error = %err, "job failed; retrying");
            self.resolve(id, &envelope.kind, self.broker.retry(receipt, delay).await)
                .await
        } else {
            tracing::warn!(job_id = %id, attempt = completed, error = %err, "job failed; dead-lettering");
            self.resolve(
                id,
                &envelope.kind,
                self.broker.fail(receipt, err.to_string()).await,
            )
            .await
        }
    }

    async fn resolve(&self, id: JobId, kind: &str, result: Result<()>) -> Result<()> {
        match result {
            Ok(()) => Ok(()),
            Err(Error::StaleReservation(msg)) => {
                tracing::warn!(job_id = %id, kind = %kind, error = %msg, "resolution rejected as stale; continuing");
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}
