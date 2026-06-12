use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use worklane_core::{
    Broker, Error, Job, JobContext, JobEnvelope, JobId, Reservation, ReservationReceipt, Result,
    RetryPolicy, from_payload,
};

/// The default lane a worker reserves from.
pub const DEFAULT_LANE: &str = "default";

/// Type-erased handler dispatch: deserialize the payload and run the handler.
#[async_trait]
trait Dispatch: Send + Sync {
    async fn dispatch(&self, ctx: JobContext, payload: &[u8]) -> Result<()>;
}

struct JobDispatcher<J: Job> {
    handler: Arc<J>,
}

#[async_trait]
impl<J: Job> Dispatch for JobDispatcher<J> {
    async fn dispatch(&self, ctx: JobContext, payload: &[u8]) -> Result<()> {
        let payload: J::Payload = from_payload(payload)?;
        self.handler
            .run(ctx, payload)
            .await
            .map_err(|e| Error::Handler(e.to_string()))
    }
}

/// Runs registered job handlers, processing one job at a time.
pub struct Worker {
    broker: Arc<dyn Broker>,
    handlers: HashMap<&'static str, Box<dyn Dispatch>>,
    retry: RetryPolicy,
    lane: String,
}

impl Worker {
    /// Create a worker over the given broker.
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Worker {
            broker,
            handlers: HashMap::new(),
            retry: RetryPolicy::default(),
            lane: DEFAULT_LANE.to_string(),
        }
    }

    /// Set the retry policy (builder style).
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Set the lane to reserve from (builder style).
    pub fn with_lane(mut self, lane: impl Into<String>) -> Self {
        self.lane = lane.into();
        self
    }

    /// Register a handler for a job kind. Rejects a duplicate kind.
    pub fn register<J: Job>(&mut self, handler: J) -> Result<&mut Self> {
        if self.handlers.contains_key(J::KIND) {
            return Err(Error::Registration(format!(
                "duplicate handler for kind {}",
                J::KIND
            )));
        }
        self.handlers.insert(
            J::KIND,
            Box::new(JobDispatcher {
                handler: Arc::new(handler),
            }),
        );
        Ok(self)
    }

    /// Reserve and process a single job. Returns `true` if a job was processed,
    /// `false` if the lane had no available job.
    pub async fn process_next(&self) -> Result<bool> {
        let reserved = match self.broker.reserve(&self.lane).await {
            Ok(reserved) => reserved,
            Err(err) => {
                tracing::error!(lane = %self.lane, error = %err, "reserve failed");
                return Err(err);
            }
        };
        match reserved {
            Some(envelope) => {
                tracing::debug!(lane = %self.lane, job_id = %envelope.envelope.id, kind = %envelope.envelope.kind, "reserved job");
                self.process(envelope).await?;
                Ok(true)
            }
            None => {
                tracing::trace!(lane = %self.lane, "no job available; idle");
                Ok(false)
            }
        }
    }

    /// Process jobs until no job is currently available.
    ///
    /// Note: jobs scheduled for the future (e.g. pending retries) are not waited
    /// for. A long-running, polling worker loop is a planned follow-up.
    pub async fn run_until_idle(&self) -> Result<()> {
        while self.process_next().await? {}
        Ok(())
    }

    async fn process(&self, reservation: Reservation) -> Result<()> {
        let receipt = reservation.receipt;
        let envelope = reservation.envelope;
        let id = envelope.id;
        let ctx = JobContext {
            id,
            attempts: envelope.attempts,
            max_attempts: envelope.max_attempts,
        };

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
