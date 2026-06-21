//! The per-kind circuit breaker: after a threshold of consecutive handler
//! failures, the worker stops dispatching that kind and defers its jobs (without
//! spending their retry budget) instead of running and dead-lettering them.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use worklane::{
    CircuitBreakerPolicy, Client, HandlerError, HandlerResult, Job, JobContext, Lane, QueueStats,
    Worker, async_trait,
};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct Unit;

/// Always fails, counting how many times its handler actually ran.
struct CountingFailJob {
    runs: Arc<AtomicUsize>,
}

#[async_trait]
impl Job for CountingFailJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "always_fail";
    async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(HandlerError::from("dependency down"))
    }
}

#[tokio::test]
async fn open_circuit_defers_jobs_without_running_or_dead_lettering_them() {
    let broker = Arc::new(InMemoryBroker::new());
    // Generous retry budget: without the breaker every job would run and retry.
    let client = Client::new(broker.clone()).with_max_attempts(5);
    for _ in 0..4 {
        client.enqueue::<CountingFailJob>(Unit).await.unwrap();
    }

    let runs = Arc::new(AtomicUsize::new(0));
    let mut worker = Worker::new(broker.clone()).with_circuit_breaker(CircuitBreakerPolicy {
        failure_threshold: 2,
        open_duration: Duration::from_secs(60),
    });
    worker
        .register(CountingFailJob { runs: runs.clone() })
        .unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    // The first two failures trip the breaker; the remaining two jobs are deferred
    // without ever running their handler.
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "the handler runs only until the breaker opens, then jobs are deferred"
    );
    // Deferred (and retrying) jobs are still live — none was dead-lettered.
    assert!(
        broker.dead_letters().is_empty(),
        "an open circuit must defer, not dead-letter"
    );
    assert_eq!(
        broker.pending_count(&Lane::default()).await.unwrap(),
        4,
        "all four jobs are still pending (two retrying, two deferred)"
    );
}

#[tokio::test]
async fn without_a_breaker_every_job_runs() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone()).with_max_attempts(5);
    for _ in 0..4 {
        client.enqueue::<CountingFailJob>(Unit).await.unwrap();
    }

    let runs = Arc::new(AtomicUsize::new(0));
    let mut worker = Worker::new(broker.clone()); // no breaker
    worker
        .register(CountingFailJob { runs: runs.clone() })
        .unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(
        runs.load(Ordering::SeqCst),
        4,
        "with no breaker, all four jobs run (and then retry)"
    );
}
