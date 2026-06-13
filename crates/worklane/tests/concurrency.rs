//! Tests for bounded concurrent processing in `Worker::run`: the concurrency
//! limit is honoured, shutdown drains all in-flight jobs, and a handler that
//! outlives its lease is redelivered (at-least-once) without crashing the
//! worker. Handlers block on a semaphore "gate" so overlap is observable;
//! counters use `SeqCst` atomics.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, oneshot};
use tokio::task::yield_now;
use tokio::time::timeout;
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;
use worklane_test::ManualClock;

#[derive(Serialize, Deserialize)]
struct Unit;

/// A handler that records concurrency and blocks on a shared gate until the test
/// releases it, so the test can observe how many run at once.
struct GateJob {
    current: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    runs: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

#[async_trait]
impl Job for GateJob {
    type Payload = Unit;
    const KIND: &'static str = "gate";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult {
        self.runs.fetch_add(1, SeqCst);
        let now = self.current.fetch_add(1, SeqCst) + 1;
        self.max.fetch_max(now, SeqCst);
        // Block until the test grants a permit.
        let permit = self.gate.acquire().await.unwrap();
        drop(permit);
        self.current.fetch_sub(1, SeqCst);
        Ok(())
    }
}

struct Counters {
    current: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    runs: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

fn counters() -> Counters {
    Counters {
        current: Arc::new(AtomicUsize::new(0)),
        max: Arc::new(AtomicUsize::new(0)),
        runs: Arc::new(AtomicUsize::new(0)),
        gate: Arc::new(Semaphore::new(0)),
    }
}

fn worker_with(broker: Arc<InMemoryBroker>, c: &Counters, concurrency: usize) -> Arc<Worker> {
    let mut w = Worker::new(broker).with_concurrency(concurrency);
    w.register(GateJob {
        current: c.current.clone(),
        max: c.max.clone(),
        runs: c.runs.clone(),
        gate: c.gate.clone(),
    })
    .unwrap();
    Arc::new(w)
}

/// Spin the runtime (bounded) until `cond` holds, yielding between checks.
async fn spin_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        yield_now().await;
    }
    panic!("condition not reached within bound");
}

/// With concurrency N and more than N jobs available, at most N handlers run at
/// once; once released, every job is processed.
#[tokio::test]
async fn concurrency_bounds_jobs_in_flight() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let c = counters();
    let worker = worker_with(broker.clone(), &c, 3);

    for _ in 0..5 {
        client.enqueue::<GateJob>(Unit).await.unwrap();
    }

    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // Exactly the concurrency limit should be in flight, never more.
    spin_until(|| c.current.load(SeqCst) >= 3).await;
    assert_eq!(
        c.current.load(SeqCst),
        3,
        "the concurrency limit runs at once"
    );
    assert_eq!(c.max.load(SeqCst), 3, "never more than the limit in flight");

    // Release everything; all five jobs should be processed and acked.
    c.gate.add_permits(5);
    spin_until(|| broker.is_empty()).await;
    assert_eq!(broker.len(), 0, "all jobs processed");
    assert_eq!(c.runs.load(SeqCst), 5);

    let _ = tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("run returns after shutdown")
        .expect("task join")
        .expect("run result");
}

/// Shutdown fired while N handlers are in flight drains them all to resolution
/// before `run` returns.
#[tokio::test]
async fn shutdown_drains_all_in_flight() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let c = counters();
    let worker = worker_with(broker.clone(), &c, 3);

    for _ in 0..3 {
        client.enqueue::<GateJob>(Unit).await.unwrap();
    }

    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // All three in flight (and blocked).
    spin_until(|| c.current.load(SeqCst) >= 3).await;

    // Shut down while they are still running, then release them.
    let _ = tx.send(());
    yield_now().await;
    c.gate.add_permits(3);

    // run must drain all three before returning.
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("run returns after draining")
        .expect("task join")
        .expect("run result");
    assert_eq!(broker.len(), 0, "all in-flight jobs drained and acked");
    assert_eq!(c.runs.load(SeqCst), 3);
}

/// A handler that outlives its lease is redelivered and run a second time
/// (at-least-once); the stale resolution of the first run is rejected and
/// logged, and the worker keeps running.
#[tokio::test(start_paused = true)]
async fn handler_exceeding_lease_is_redelivered() {
    let clock = Arc::new(ManualClock::new());
    let lease = Duration::from_secs(10);
    let poll = Duration::from_secs(1);
    let broker = Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(lease));
    let client = Client::new(broker.clone());
    let c = counters();
    let worker = {
        let mut w = Worker::new(broker.clone())
            .with_concurrency(2)
            .with_poll_interval(poll);
        w.register(GateJob {
            current: c.current.clone(),
            max: c.max.clone(),
            runs: c.runs.clone(),
            gate: c.gate.clone(),
        })
        .unwrap();
        Arc::new(w)
    };

    client.enqueue::<GateJob>(Unit).await.unwrap();

    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // First reservation: the handler starts and blocks (holding the lease).
    spin_until(|| c.runs.load(SeqCst) >= 1).await;
    yield_now().await; // let `run` park on its poll-interval wait

    // The lease expires while the handler is still running, then the worker
    // polls and re-reserves the now-visible job.
    clock.advance(Duration::from_secs(11));
    tokio::time::advance(poll).await;
    spin_until(|| c.runs.load(SeqCst) >= 2).await;
    assert_eq!(c.runs.load(SeqCst), 2, "job redelivered after lease expiry");

    // Release both runs: the first resolves stale (rejected, logged), the second
    // resolves validly, so the job is removed exactly once.
    c.gate.add_permits(2);
    spin_until(|| broker.is_empty()).await;
    assert_eq!(
        broker.len(),
        0,
        "job resolved exactly once despite running twice"
    );

    let _ = tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("run returns; worker did not crash on the stale resolution")
        .expect("task join")
        .expect("run result");
}
