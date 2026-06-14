use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use worklane_core::{Broker, Error, Job, JobContext, Result, RetryPolicy, from_payload};

mod execution;

/// The default lane a worker reserves from.
pub const DEFAULT_LANE: &str = "default";

/// The default idle poll interval for [`Worker::run`].
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

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

/// Runs registered job handlers, processing up to a configured concurrency of
/// jobs at a time (default 1, i.e. strictly sequential).
pub struct Worker {
    broker: Arc<dyn Broker>,
    handlers: HashMap<&'static str, Box<dyn Dispatch>>,
    retry: RetryPolicy,
    lane: String,
    poll_interval: Duration,
    concurrency: usize,
    handler_timeout: Option<Duration>,
}

impl Worker {
    /// Create a worker over the given broker.
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Worker {
            broker,
            handlers: HashMap::new(),
            retry: RetryPolicy::default(),
            lane: DEFAULT_LANE.to_string(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            concurrency: 1,
            handler_timeout: None,
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

    /// Set the idle poll interval used by [`run`](Self::run) (builder style).
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Set the maximum number of jobs processed concurrently by
    /// [`run`](Self::run) (builder style). Defaults to 1 (strictly sequential).
    /// A value of 0 is treated as 1 so the worker always makes progress.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Set the maximum wall-clock time a single handler may run (builder style).
    ///
    /// When set, [`run`](Self::run) heartbeats to extend each job's reservation
    /// lease while its handler runs within this bound, so a slow handler keeps
    /// its reservation instead of being redelivered. A handler that exceeds the
    /// timeout is abandoned and routed through the normal failure path (retry
    /// while attempts remain, else dead-letter), so a stuck handler stays
    /// bounded. Unset (the default) means no heartbeat and no timeout: lease
    /// expiry may redeliver a long handler, as before.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        self.handler_timeout = Some(timeout);
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
    /// for. For a long-running daemon that does wait, use [`run`](Self::run).
    pub async fn run_until_idle(&self) -> Result<()> {
        while self.process_next().await? {}
        Ok(())
    }

    /// Run as a long-lived daemon: process available jobs, then wait the
    /// configured poll interval when idle and check again, until `shutdown`
    /// resolves. Up to [`with_concurrency`](Self::with_concurrency) jobs are
    /// processed at once (default 1, strictly sequential).
    ///
    /// Concurrency is in-task: up to N `reserve -> dispatch -> resolve` futures
    /// run interleaved on this task (handlers overlap at their await points).
    ///
    /// Shutdown is cooperative: it is only honoured between reservations, so
    /// every in-flight handler always completes and is resolved (ack / retry /
    /// fail) before `run` returns — all in-flight jobs are drained. Dropping the
    /// returned future instead (a hard cancel) may leave in-flight jobs
    /// unresolved; they are redelivered later under at-least-once delivery.
    ///
    /// If a job's resolution hits a non-stale broker error, the worker stops
    /// reserving, drains the in-flight jobs, and returns the first such error.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) -> Result<()> {
        tokio::pin!(shutdown);
        let concurrency = self.concurrency.max(1);
        let mut in_flight = futures_util::stream::FuturesUnordered::new();
        let mut shutting_down = false;
        let mut first_err: Option<Error> = None;

        loop {
            // Observe a shutdown (non-blocking) before reserving more work, so a
            // signal that fired during a job — including from within a handler —
            // stops us between jobs.
            if !shutting_down {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => shutting_down = true,
                    _ = std::future::ready(()) => {}
                }
            }

            // Fill spare capacity by reserving more jobs, unless shutting down.
            while !shutting_down && in_flight.len() < concurrency {
                match self.broker.reserve(&self.lane).await {
                    Ok(Some(reservation)) => {
                        tracing::debug!(lane = %self.lane, job_id = %reservation.envelope.id, kind = %reservation.envelope.kind, "reserved job");
                        in_flight.push(self.process(reservation));
                    }
                    Ok(None) => break, // no currently-available job on this lane
                    Err(err) => {
                        tracing::error!(lane = %self.lane, error = %err, "reserve failed");
                        first_err.get_or_insert(err);
                        shutting_down = true;
                    }
                }
            }

            // Nothing in flight: stop if shutting down, else idle-wait for work.
            if in_flight.is_empty() {
                if shutting_down {
                    break;
                }
                tokio::select! {
                    biased;
                    _ = &mut shutdown => shutting_down = true,
                    _ = tokio::time::sleep(self.poll_interval) => {}
                }
                continue;
            }

            // Draining: await in-flight completions only, reserve no more.
            if shutting_down {
                if let Some(result) = in_flight.next().await
                    && let Err(err) = result
                {
                    first_err.get_or_insert(err);
                }
                continue;
            }

            // Running: wait for a job to finish, a shutdown, or — if we have
            // spare capacity — a poll tick to re-check the lane for new work.
            let have_capacity = in_flight.len() < concurrency;
            tokio::select! {
                biased;
                _ = &mut shutdown => shutting_down = true,
                result = in_flight.next() => {
                    if let Some(Err(err)) = result {
                        first_err.get_or_insert(err);
                        shutting_down = true;
                    }
                }
                _ = tokio::time::sleep(self.poll_interval), if have_capacity => {}
            }
        }

        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}
