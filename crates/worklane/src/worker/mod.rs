use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use worklane_core::{Broker, Error, Job, JobContext, Lane, Result, RetryPolicy, from_payload};

mod circuit_breaker;
mod config;
mod execution;
mod middleware;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerPolicy};
pub use middleware::{Middleware, Next};

/// The default idle poll interval for [`Worker::run`].
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The observer SPI, defined in `worklane-core` so telemetry integrations can
/// depend on the contract without the facade. Re-exported here as the worker is
/// what invokes it.
pub use worklane_core::{JobAttemptEvent, JobEvent, JobObserver, JobOutcome};

/// Type-erased handler dispatch: deserialize the payload and run the handler.
#[async_trait]
trait Dispatch: Send + Sync {
    async fn dispatch(&self, ctx: JobContext, payload: &[u8]) -> Result<Vec<u8>>;
}

struct JobDispatcher<J: Job> {
    handler: Arc<J>,
}

#[async_trait]
impl<J: Job> Dispatch for JobDispatcher<J> {
    async fn dispatch(&self, ctx: JobContext, payload: &[u8]) -> Result<Vec<u8>> {
        let payload: J::Payload = from_payload(payload)?;
        let output = self
            .handler
            .run(ctx, payload)
            .await
            .map_err(|e| Error::Handler(e.to_string()))?;
        // Re-tag an output-encode failure as `OutputEncode` (not the
        // `Serialization` that `to_payload` returns): an undecodable *input*
        // payload is unrecoverable and dead-lettered immediately, but the
        // handler already succeeded here, so a (possibly transient) encode
        // failure must take the normal retry/dead-letter path instead.
        worklane_core::to_payload(&output).map_err(|e| Error::OutputEncode(e.to_string()))
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Marks a [`Worker`]'s lifecycle phase (typestate). Sealed: the only states are
/// [`Building`] and [`Ready`].
pub trait WorkerState: sealed::Sealed {}

/// Configuration phase: handlers and options can be set. Reached by
/// [`Worker::new`]; left by [`Worker::build`].
pub struct Building;
/// Execution phase: configuration is frozen and the worker can [`run`](Worker::run).
/// Reached only via [`Worker::build`].
pub struct Ready;

impl sealed::Sealed for Building {}
impl sealed::Sealed for Ready {}
impl WorkerState for Building {}
impl WorkerState for Ready {}

/// Runs registered job handlers, processing up to a configured concurrency of
/// jobs at a time (default 1, i.e. strictly sequential).
///
/// The worker is a **typestate** with two phases, so misuse is a compile error:
/// - `Worker<Building>` (from [`new`](Worker::new)) accepts `with_*` options and
///   [`register`](Worker::register), but cannot run.
/// - [`build`](Worker::build) freezes configuration, rejects a worker with no
///   handlers, and returns `Worker<Ready>`.
/// - `Worker<Ready>` can [`run`](Worker::run) / [`run_until_idle`](Worker::run_until_idle)
///   / [`process_next`](Worker::process_next), but no longer accepts configuration —
///   so handlers cannot be added after the worker has started.
pub struct Worker<S: WorkerState = Building> {
    broker: Arc<dyn Broker>,
    result_store: Option<Arc<dyn worklane_core::ResultStore>>,
    payload_store: Option<Arc<dyn worklane_core::PayloadStore>>,
    observer: Option<Arc<dyn JobObserver>>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    middleware: Vec<Arc<dyn Middleware>>,
    handlers: HashMap<&'static str, Arc<dyn Dispatch>>,
    retry: RetryPolicy,
    lane: Lane,
    poll_interval: Duration,
    idle_backoff_cap: Duration,
    resilient: bool,
    concurrency: usize,
    handler_timeout: Option<Duration>,
    lease_keepalive: bool,
    shutdown_timeout: Option<Duration>,
    _state: PhantomData<S>,
}

impl Worker<Building> {
    /// Create a worker over the given broker. Returns a `Worker<Building>`:
    /// configure it, [`register`](Worker::register) handlers, then
    /// [`build`](Worker::build) it into a runnable `Worker<Ready>`.
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Worker {
            broker,
            result_store: None,
            payload_store: None,
            observer: None,
            circuit_breaker: None,
            middleware: Vec::new(),
            handlers: HashMap::new(),
            retry: RetryPolicy::default(),
            lane: Lane::default(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            idle_backoff_cap: DEFAULT_POLL_INTERVAL,
            resilient: false,
            concurrency: 1,
            handler_timeout: None,
            lease_keepalive: false,
            shutdown_timeout: None,
            _state: PhantomData,
        }
    }

    /// Freeze configuration and transition to a runnable `Worker<Ready>`. Rejects
    /// a worker with no registered handlers (which would otherwise dead-letter
    /// every reserved job as an unknown kind).
    pub fn build(self) -> Result<Worker<Ready>> {
        if self.handlers.is_empty() {
            return Err(Error::Registration(
                "worker has no registered handlers; register at least one before build()"
                    .to_string(),
            ));
        }
        Ok(Worker {
            broker: self.broker,
            result_store: self.result_store,
            payload_store: self.payload_store,
            observer: self.observer,
            circuit_breaker: self.circuit_breaker,
            middleware: self.middleware,
            handlers: self.handlers,
            retry: self.retry,
            lane: self.lane,
            poll_interval: self.poll_interval,
            idle_backoff_cap: self.idle_backoff_cap,
            resilient: self.resilient,
            concurrency: self.concurrency,
            handler_timeout: self.handler_timeout,
            lease_keepalive: self.lease_keepalive,
            shutdown_timeout: self.shutdown_timeout,
            _state: PhantomData,
        })
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
            Arc::new(JobDispatcher {
                handler: Arc::new(handler),
            }),
        );
        Ok(self)
    }
}

impl Worker<Ready> {
    /// Assemble the per-job processing context from the worker's configuration.
    /// Single-sourced so every field is wired identically at both dispatch sites.
    fn process_ctx(&self) -> execution::ProcessCtx {
        execution::ProcessCtx {
            broker: self.broker.clone(),
            result_store: self.result_store.clone(),
            payload_store: self.payload_store.clone(),
            observer: self.observer.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            middleware: self.middleware.clone(),
            retry: self.retry.clone(),
            handler_timeout: self.handler_timeout,
            lease_keepalive: self.lease_keepalive,
        }
    }

    /// The idle wait after `empties` consecutive empty polls:
    /// `min(base * 2^empties, cap)`, with `cap` never below `base`. Doubling
    /// saturates and stops early at the cap, so a large `empties` cannot
    /// overflow or loop unboundedly.
    fn idle_wait(&self, empties: u32) -> Duration {
        let base = self.poll_interval;
        let cap = self.idle_backoff_cap.max(base);
        let mut wait = base;
        for _ in 0..empties {
            if wait >= cap {
                return cap;
            }
            wait = wait.saturating_mul(2);
        }
        wait.min(cap)
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
                let ctx = self.process_ctx();
                let dispatch = self.handlers.get(envelope.envelope.kind.as_str()).cloned();
                ctx.process(dispatch, envelope).await?;
                Ok(true)
            }
            None => {
                tracing::trace!(lane = %self.lane, "no job available; idle");
                Ok(false)
            }
        }
    }

    /// Process jobs until no job is currently available, then return. Up to
    /// [`with_concurrency`](Self::with_concurrency) jobs run at once (default 1,
    /// strictly sequential), the same in-task concurrency model as [`run`](Self::run).
    ///
    /// "Idle" means a `reserve` found no currently-visible job *and* nothing is
    /// in flight; jobs scheduled for the future (e.g. pending retries) are not
    /// waited for. For a long-running daemon that idles and waits instead of
    /// returning, use [`run`](Self::run). Fails fast: on the first reserve or
    /// resolve error it stops reserving, drains the in-flight jobs, and returns
    /// that error.
    pub async fn run_until_idle(&self) -> Result<()> {
        let concurrency = self.concurrency.max(1);
        let mut in_flight = tokio::task::JoinSet::new();
        let mut first_err: Option<Error> = None;
        let mut stop_reserving = false;

        loop {
            // Fill spare capacity with currently-available jobs (unless an error
            // has put us into drain-only mode).
            while !stop_reserving && in_flight.len() < concurrency {
                match self.broker.reserve(&self.lane).await {
                    Ok(Some(reservation)) => {
                        tracing::debug!(lane = %self.lane, job_id = %reservation.envelope.id, kind = %reservation.envelope.kind, "reserved job");
                        let ctx = self.process_ctx();
                        let dispatch = self
                            .handlers
                            .get(reservation.envelope.kind.as_str())
                            .cloned();
                        in_flight.spawn(async move { ctx.process(dispatch, reservation).await });
                    }
                    Ok(None) => break, // no currently-available job on this lane
                    Err(err) => {
                        tracing::error!(lane = %self.lane, error = %err, "reserve failed");
                        first_err.get_or_insert(err);
                        stop_reserving = true;
                        break;
                    }
                }
            }

            // Idle: nothing available to reserve and nothing in flight.
            if in_flight.is_empty() {
                break;
            }

            // Wait for one in-flight job to finish, then loop to refill (a
            // zero-delay retry may have made another job available).
            if let Some(join_result) = in_flight.join_next().await {
                if let Some(err) = self.classify_join(join_result) {
                    first_err.get_or_insert(err);
                    stop_reserving = true; // fail fast: drain, then return
                }
            }
        }

        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Run as a long-lived daemon: process available jobs, then wait an adaptive
    /// idle backoff (see [`with_idle_backoff`](Self::with_idle_backoff)) when the
    /// lane is empty and check again, until `shutdown` resolves. Up to
    /// [`with_concurrency`](Self::with_concurrency) jobs are processed at once
    /// (default 1, strictly sequential).
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
    /// On a non-stale broker error the worker fails fast by default: it stops
    /// reserving, drains the in-flight jobs, and returns the first such error.
    /// With [`with_resilient(true)`](Self::with_resilient) it instead logs the
    /// error and keeps running, retrying after an idle backoff.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) -> Result<()> {
        tokio::pin!(shutdown);
        let concurrency = self.concurrency.max(1);
        let mut in_flight = tokio::task::JoinSet::new();
        let mut shutting_down = false;
        let mut first_err: Option<Error> = None;
        // Consecutive empty/failed polls, driving the idle backoff. Reset to 0
        // the moment a job is reserved.
        let mut empty_polls: u32 = 0;
        // Set the first time the drain begins, to bound it by `shutdown_timeout`.
        let mut drain_deadline: Option<tokio::time::Instant> = None;

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
                        empty_polls = 0; // work found: reset the idle backoff

                        let ctx = self.process_ctx();
                        let dispatch = self
                            .handlers
                            .get(reservation.envelope.kind.as_str())
                            .cloned();

                        in_flight.spawn(async move { ctx.process(dispatch, reservation).await });
                    }
                    Ok(None) => break, // no currently-available job on this lane
                    Err(err) => {
                        tracing::error!(lane = %self.lane, error = %err, "reserve failed");
                        // Fail-fast records the error and drains; resilient mode
                        // keeps running. Either way, stop filling this cycle and
                        // fall through to the idle backoff before retrying.
                        self.record_error(err, &mut first_err, &mut shutting_down);
                        break;
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
                    _ = tokio::time::sleep(self.idle_wait(empty_polls)) => {
                        empty_polls = empty_polls.saturating_add(1);
                    }
                }
                continue;
            }

            // Draining: await in-flight completions only, reserve no more. Bounded
            // by `shutdown_timeout` if set, so one stuck handler cannot block
            // shutdown forever.
            if shutting_down {
                let drained = match self.shutdown_timeout {
                    Some(timeout) => {
                        let deadline = *drain_deadline
                            .get_or_insert_with(|| tokio::time::Instant::now() + timeout);
                        tokio::select! {
                            biased;
                            result = in_flight.join_next() => result,
                            _ = tokio::time::sleep_until(deadline) => {
                                tracing::warn!(
                                    remaining = in_flight.len(),
                                    "shutdown timeout elapsed; abandoning in-flight job(s) — \
                                     they become visible again after lease expiry"
                                );
                                break;
                            }
                        }
                    }
                    None => in_flight.join_next().await,
                };
                if let Some(join_result) = drained {
                    if let Some(err) = self.classify_join(join_result) {
                        first_err.get_or_insert(err);
                    }
                }
                continue;
            }

            // Running: wait for a job to finish, a shutdown, or — if we have
            // spare capacity — a poll tick to re-check the lane for new work.
            let have_capacity = in_flight.len() < concurrency;
            tokio::select! {
                biased;
                _ = &mut shutdown => shutting_down = true,
                join_result = in_flight.join_next() => {
                    if let Some(result) = join_result {
                        if let Some(err) = self.classify_join(result) {
                            self.record_error(err, &mut first_err, &mut shutting_down);
                        }
                    }
                }
                _ = tokio::time::sleep(self.idle_wait(empty_polls)), if have_capacity => {
                    empty_polls = empty_polls.saturating_add(1);
                }
            }
        }

        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Apply the broker-error policy after a reserve or job-resolution failure.
    /// Fail-fast (the default) records the first error and begins draining;
    /// resilient mode keeps running, so the error is neither recorded nor does it
    /// trigger shutdown (the caller logs it and retries after an idle backoff).
    fn record_error(&self, err: Error, first_err: &mut Option<Error>, shutting_down: &mut bool) {
        if !self.resilient {
            first_err.get_or_insert(err);
            *shutting_down = true;
        }
    }

    /// Interpret a finished in-flight task's join result, logging it consistently,
    /// and return the error the caller should record (if any). Single-sourced so
    /// every join site logs the same way; the caller decides what recording an
    /// error means (fail-fast, drain, or resilient continue).
    ///
    /// A handler panic is caught by `catch_unwind` inside the task, so a panic
    /// surfacing here bypassed it (e.g. `panic = "abort"`, or a panic outside the
    /// handler) and is mapped to a handler error. A non-panic `JoinError` is a
    /// cancellation (e.g. the `JoinSet` aborted during a hard shutdown) and is not
    /// an error.
    fn classify_join(
        &self,
        join_result: std::result::Result<Result<()>, tokio::task::JoinError>,
    ) -> Option<Error> {
        match join_result {
            Ok(Ok(())) => None,
            Ok(Err(err)) => {
                tracing::error!(lane = %self.lane, error = %err, "job resolution failed");
                Some(err)
            }
            Err(join_err) if join_err.is_panic() => {
                tracing::error!(lane = %self.lane, error = %join_err, "worker task panicked");
                Some(Error::Handler("worker task panicked".to_string()))
            }
            Err(join_err) => {
                tracing::debug!(lane = %self.lane, error = %join_err, "worker task cancelled");
                None
            }
        }
    }
}
