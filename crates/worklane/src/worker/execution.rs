//! The per-job lifecycle: dispatch one reserved job to its handler and resolve
//! the outcome (ack / retry / fail), honouring at-least-once semantics. The
//! orchestration that decides *when* to run a job (reserve, concurrency,
//! shutdown, poll) lives in the parent module; this is the unit it drives.

use worklane_core::{
    Error, JobContext, JobEnvelope, JobId, Reservation, ReservationReceipt, Result,
};

use super::Worker;

impl Worker {
    /// Dispatch a reserved job to its handler and resolve the outcome with the
    /// reservation's receipt.
    pub(super) async fn process(&self, reservation: Reservation) -> Result<()> {
        let receipt = reservation.receipt;
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
        match dispatch.dispatch(ctx, &envelope.payload).await {
            Ok(()) => {
                tracing::info!(job_id = %id, "job succeeded; ack");
                self.resolve(id, &envelope.kind, self.broker.ack(receipt).await)
                    .await
            }
            Err(Error::Serialization(msg)) => {
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
            Err(err) => self.handle_failure(id, receipt, &envelope, err).await,
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
