use std::sync::Arc;
use std::time::Duration;

use worklane_core::{Lane, RetryPolicy};

use super::{Building, CircuitBreaker, CircuitBreakerPolicy, JobObserver, Middleware, Worker};

impl Worker<Building> {
    /// Bound how long graceful shutdown waits for in-flight jobs to finish.
    #[must_use = "this value must be used"]
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = Some(timeout);
        self
    }

    /// Set an optional result store to save successful job outputs.
    #[must_use = "this value must be used"]
    pub fn with_result_store(mut self, result_store: Arc<dyn worklane_core::ResultStore>) -> Self {
        self.result_store = Some(result_store);
        self
    }

    /// Set an optional payload store so offloaded payloads can be resolved.
    #[must_use = "this value must be used"]
    pub fn with_payload_store(
        mut self,
        payload_store: Arc<dyn worklane_core::PayloadStore>,
    ) -> Self {
        self.payload_store = Some(payload_store);
        self
    }

    /// Set a [`JobObserver`] notified of every job's outcome.
    #[must_use = "this value must be used"]
    pub fn with_observer(mut self, observer: Arc<dyn JobObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Enable a per-kind circuit breaker.
    #[must_use = "this value must be used"]
    pub fn with_circuit_breaker(mut self, policy: CircuitBreakerPolicy) -> Self {
        self.circuit_breaker = Some(Arc::new(CircuitBreaker::new(policy)));
        self
    }

    /// Add a [`Middleware`] wrapping handler dispatch.
    #[must_use = "this value must be used"]
    pub fn with_middleware(mut self, middleware: Arc<dyn Middleware>) -> Self {
        self.middleware.push(middleware);
        self
    }

    /// Set the retry policy.
    #[must_use = "this value must be used"]
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Set the lane to reserve from.
    #[must_use = "this value must be used"]
    pub fn with_lane(mut self, lane: Lane) -> Self {
        self.lane = lane;
        self
    }

    /// Set the idle poll interval used by [`run`](Self::run).
    #[must_use = "this value must be used"]
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Configure the idle backoff used by [`run`](Self::run).
    #[must_use = "this value must be used"]
    pub fn with_idle_backoff(mut self, base: Duration, cap: Duration) -> Self {
        self.poll_interval = base;
        self.idle_backoff_cap = cap.max(base);
        self
    }

    /// Enable resilient mode for [`run`](Self::run).
    #[must_use = "this value must be used"]
    pub fn with_resilient(mut self, resilient: bool) -> Self {
        self.resilient = resilient;
        self
    }

    /// Set the maximum number of jobs processed concurrently by [`run`](Self::run).
    #[must_use = "this value must be used"]
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Set the maximum wall-clock time a single handler may run.
    #[must_use = "this value must be used"]
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        self.handler_timeout = Some(timeout);
        self
    }

    /// Enable lease keepalive while a handler runs.
    #[must_use = "this value must be used"]
    pub fn with_lease_keepalive(mut self, keepalive: bool) -> Self {
        self.lease_keepalive = keepalive;
        self
    }
}
