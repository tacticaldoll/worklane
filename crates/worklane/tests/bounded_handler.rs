//! Tests for bounded long-handler support in `Worker::run`: with a handler
//! timeout configured, the worker heartbeats to hold a slow handler's lease so
//! it is not redelivered; a handler that exceeds its timeout is failed; the
//! default (no timeout) still redelivers a long handler; and a heartbeat
//! rejected as stale is tolerated without crashing the worker.
//!
//! Both clocks are advanced in tandem: the injected `ManualClock` drives the
//! broker's lease math, and tokio's (paused) virtual time drives the worker's
//! heartbeat ticks, timeout deadline, and poll interval.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::time::timeout;
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;
use worklane_test::ManualClock;

#[derive(Serialize, Deserialize)]
struct Unit;

/// A handler that counts its runs and blocks on a shared gate until the test
/// releases it (or the handler is abandoned by a timeout).
struct GateJob {
    runs: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

#[async_trait]
impl Job for GateJob {
    type Payload = Unit;
    const KIND: &'static str = "gate";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult {
        self.runs.fetch_add(1, SeqCst);
        let permit = self.gate.acquire().await.unwrap();
        drop(permit);
        Ok(())
    }
}

fn worker_with(
    broker: Arc<InMemoryBroker>,
    runs: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
    concurrency: usize,
    poll: Duration,
    handler_timeout: Option<Duration>,
) -> Arc<Worker> {
    let mut w = Worker::new(broker)
        .with_concurrency(concurrency)
        .with_poll_interval(poll);
    if let Some(t) = handler_timeout {
        w = w.with_handler_timeout(t);
    }
    w.register(GateJob { runs, gate }).unwrap();
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

/// Advance the broker clock and tokio's virtual time together, in steps, so
/// heartbeat ticks and lease expiry stay consistent.
async fn step(clock: &ManualClock, by: Duration, times: u32) {
    for _ in 0..times {
        clock.advance(by);
        tokio::time::advance(by).await;
        yield_now().await;
    }
}

/// With a handler timeout configured, a handler that runs well past its lease is
/// kept alive by the heartbeat: it is never redelivered and runs exactly once,
/// even though the worker has spare capacity to re-reserve it.
#[tokio::test(start_paused = true)]
async fn heartbeat_holds_slow_handler() {
    let clock = Arc::new(ManualClock::new());
    let lease = Duration::from_secs(10); // heartbeat every 5s
    let poll = Duration::from_secs(1);
    let broker = Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(lease));
    let client = Client::new(broker.clone());
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let worker = worker_with(
        broker.clone(),
        runs.clone(),
        gate.clone(),
        2,
        poll,
        Some(Duration::from_secs(600)),
    );

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

    spin_until(|| runs.load(SeqCst) >= 1).await;
    yield_now().await;

    // Advance 15s (well past the 10s lease) in 1s steps. Heartbeats at 5s and
    // 10s keep extending the lease, so the spare-capacity worker never re-reserves.
    step(&clock, Duration::from_secs(1), 15).await;
    assert_eq!(
        runs.load(SeqCst),
        1,
        "the heartbeat must hold the lease so the job is not redelivered"
    );

    // Release the handler: it completes and is acked exactly once.
    gate.add_permits(1);
    spin_until(|| broker.is_empty()).await;
    assert_eq!(runs.load(SeqCst), 1, "the job ran exactly once");
    assert_eq!(broker.len(), 0, "the job was acked");

    let _ = tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("run returns after shutdown")
        .expect("task join")
        .expect("run result");
}

/// The default (no handler timeout) does not heartbeat: a handler that outlives
/// its lease while the worker has spare capacity is redelivered (runs twice).
/// This is the contrast to `heartbeat_holds_slow_handler`.
#[tokio::test(start_paused = true)]
async fn default_no_timeout_redelivers() {
    let clock = Arc::new(ManualClock::new());
    let lease = Duration::from_secs(10);
    let poll = Duration::from_secs(1);
    let broker = Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(lease));
    let client = Client::new(broker.clone());
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let worker = worker_with(broker.clone(), runs.clone(), gate.clone(), 2, poll, None);

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

    spin_until(|| runs.load(SeqCst) >= 1).await;
    yield_now().await;

    // Past the lease, the spare-capacity worker re-reserves and runs it again.
    step(&clock, Duration::from_secs(1), 12).await;
    spin_until(|| runs.load(SeqCst) >= 2).await;
    assert_eq!(
        runs.load(SeqCst),
        2,
        "without a heartbeat the long handler is redelivered"
    );

    gate.add_permits(2);
    spin_until(|| broker.is_empty()).await;
    let _ = tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("run returns after shutdown")
        .expect("task join")
        .expect("run result");
}

/// A handler that never completes is abandoned at its timeout and routed through
/// the failure path; with one attempt it is dead-lettered with a timeout error,
/// and the worker keeps running.
#[tokio::test(start_paused = true)]
async fn timed_out_handler_is_dead_lettered() {
    let clock = Arc::new(ManualClock::new());
    let lease = Duration::from_secs(30);
    let poll = Duration::from_secs(1);
    let broker = Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(lease));
    let client = Client::new(broker.clone()).with_max_attempts(1);
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0)); // never released: the handler hangs
    let worker = worker_with(
        broker.clone(),
        runs.clone(),
        gate.clone(),
        1,
        poll,
        Some(Duration::from_secs(10)),
    );

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

    spin_until(|| runs.load(SeqCst) >= 1).await;
    yield_now().await;

    // Cross the 10s timeout: the handler is abandoned and dead-lettered.
    step(&clock, Duration::from_secs(1), 12).await;
    spin_until(|| !broker.dead_letters().is_empty()).await;

    let dead = broker.dead_letters();
    assert_eq!(dead.len(), 1, "the timed-out job is dead-lettered");
    assert!(
        dead[0].error.contains("timed out"),
        "dead-letter records a timeout error, got: {}",
        dead[0].error
    );
    assert_eq!(broker.len(), 0, "no live job remains");

    let _ = tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("worker did not stall on the timeout")
        .expect("task join")
        .expect("run result");
}

/// If the lease is lost mid-handler (an abrupt clock jump lets the spare-capacity
/// worker re-reserve and supersede the receipt), the first handler's heartbeat is
/// rejected as stale. The worker tolerates it: no crash, the stale resolution is
/// dropped, and the job resolves exactly once via the current reservation.
#[tokio::test(start_paused = true)]
async fn stale_heartbeat_is_tolerated() {
    let clock = Arc::new(ManualClock::new());
    let lease = Duration::from_secs(10);
    let poll = Duration::from_secs(1);
    let broker = Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(lease));
    let client = Client::new(broker.clone());
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let worker = worker_with(
        broker.clone(),
        runs.clone(),
        gate.clone(),
        2,
        poll,
        Some(Duration::from_secs(600)),
    );

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

    spin_until(|| runs.load(SeqCst) >= 1).await;
    yield_now().await;

    // Abrupt jump past the lease before the next heartbeat: the broker sees the
    // lease expired, the spare-capacity worker re-reserves (superseding the first
    // receipt), and the first handler's next heartbeat is then rejected as stale.
    clock.advance(Duration::from_secs(25));
    tokio::time::advance(poll).await;
    spin_until(|| runs.load(SeqCst) >= 2).await;
    step(&clock, Duration::from_secs(1), 6).await; // let the stale heartbeat fire

    // Release both runs: the first resolves stale (rejected, logged), the second
    // resolves validly, so the job is removed exactly once and nothing crashes.
    gate.add_permits(2);
    spin_until(|| broker.is_empty()).await;
    assert_eq!(
        broker.len(),
        0,
        "job resolved exactly once despite a stale heartbeat"
    );

    let _ = tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("worker did not crash on the stale heartbeat")
        .expect("task join")
        .expect("run result");
}
