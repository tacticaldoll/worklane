//! Tests for handler panic isolation: a handler that panics is contained and
//! routed through the failure path (retry / dead-letter) instead of crashing
//! the worker, and a panic in one job does not abandon concurrent siblings.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::time::timeout;
use worklane::{Client, HandlerResult, Job, JobContext, RetryPolicy, Worker, async_trait};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct Unit;

/// Always panics.
struct PanicJob;

#[async_trait]
impl Job for PanicJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "panic";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
        panic!("boom");
    }
}

/// Panics on its first attempt, then succeeds.
struct FlakyPanicJob {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Job for FlakyPanicJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "flaky_panic";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
        let n = self.calls.fetch_add(1, SeqCst);
        assert!(n > 0, "panicking on the first attempt");
        Ok(())
    }
}

/// Blocks on a gate, then succeeds — a sibling held in flight during a panic.
struct GateJob {
    ran: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

#[async_trait]
impl Job for GateJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "gate";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
        self.ran.fetch_add(1, SeqCst);
        let permit = self.gate.acquire().await.unwrap();
        drop(permit);
        Ok(())
    }
}

async fn spin_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        yield_now().await;
    }
    panic!("condition not reached within bound");
}

/// A handler that panics on its final attempt is dead-lettered with a panic
/// error, and the worker does not crash (run_until_idle returns normally).
#[tokio::test]
async fn panicking_handler_is_dead_lettered() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone()).with_max_attempts(1);
    let mut worker = Worker::new(broker.clone());
    worker.register(PanicJob).unwrap();

    client.enqueue::<PanicJob>(Unit).await.unwrap();
    let worker = worker.build().unwrap();
    worker
        .run_until_idle()
        .await
        .expect("a handler panic must not propagate out of the worker");

    let dead = broker.dead_letters();
    assert_eq!(dead.len(), 1, "the panicking job is dead-lettered");
    assert!(
        dead[0].error.contains("panicked"),
        "dead-letter records a panic error, got: {}",
        dead[0].error
    );
    assert_eq!(broker.len(), 0, "no live job remains");
}

/// A handler that panics below max attempts is retried, and a later
/// non-panicking attempt acks the job.
#[tokio::test]
async fn panicking_handler_is_retried_then_acked() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone()).with_max_attempts(3);
    let calls = Arc::new(AtomicUsize::new(0));
    // Zero-delay retries so run_until_idle picks the retry up immediately.
    let mut worker = Worker::new(broker.clone()).with_retry_policy(RetryPolicy {
        base: Duration::ZERO,
        factor: 1,
        cap: Duration::ZERO,
        jitter: 0.0,
    });
    worker
        .register(FlakyPanicJob {
            calls: calls.clone(),
        })
        .unwrap();

    client.enqueue::<FlakyPanicJob>(Unit).await.unwrap();
    let worker = worker.build().unwrap();
    worker
        .run_until_idle()
        .await
        .expect("worker survives the panic");

    assert_eq!(calls.load(SeqCst), 2, "panicked once, then succeeded");
    assert_eq!(broker.len(), 0, "the retried job was acked");
    assert!(broker.dead_letters().is_empty(), "not dead-lettered");
}

/// Under concurrency, a panic in one in-flight handler does not crash the worker
/// or abandon a sibling job that is still running.
#[tokio::test]
async fn panic_does_not_abandon_siblings() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone()).with_max_attempts(1);
    let ran = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let worker = {
        let mut w = Worker::new(broker.clone()).with_concurrency(2);
        w.register(PanicJob).unwrap();
        w.register(GateJob {
            ran: ran.clone(),
            gate: gate.clone(),
        })
        .unwrap();
        Arc::new(w.build().unwrap())
    };

    // The gate job will be held in flight while the panic job panics.
    client.enqueue::<GateJob>(Unit).await.unwrap();
    client.enqueue::<PanicJob>(Unit).await.unwrap();

    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // The panic job is dead-lettered while the gate job is still running: the
    // worker survived the panic and did not abandon its sibling.
    spin_until(|| !broker.dead_letters().is_empty()).await;
    assert_eq!(
        ran.load(SeqCst),
        1,
        "the sibling is still in flight, not abandoned"
    );

    // Release the sibling: it completes and is acked.
    gate.add_permits(1);
    spin_until(|| broker.is_empty()).await;
    assert_eq!(broker.len(), 0, "sibling acked; nothing left live");
    assert_eq!(
        broker.dead_letters().len(),
        1,
        "only the panic job dead-lettered"
    );

    let _ = tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("run returns; the worker did not crash on the panic")
        .expect("task join")
        .expect("run result");
}
