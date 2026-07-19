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
use worklane::{Client, HandlerResult, Job, JobContext, Ready, Worker, async_trait};
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
    type Output = ();
    const KIND: &'static str = "gate";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
        self.runs.fetch_add(1, SeqCst);
        let permit = self.gate.acquire().await.unwrap();
        drop(permit);
        Ok(())
    }
}

/// Build a ready worker over `broker` with a `GateJob` wired to `runs`/`gate`.
fn worker_with(
    broker: Arc<InMemoryBroker>,
    runs: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
    concurrency: usize,
    poll: Duration,
    handler_timeout: Option<Duration>,
) -> Arc<Worker<Ready>> {
    let mut w = Worker::new(broker)
        .with_concurrency(concurrency)
        .with_poll_interval(poll);
    if let Some(t) = handler_timeout {
        w = w.with_handler_timeout(t);
    }
    w.register(GateJob { runs, gate }).unwrap();
    Arc::new(w.build().unwrap())
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

/// Lease keepalive without a handler timeout holds a slow handler's lease: the
/// heartbeat extends the reservation for as long as the handler runs, so even a
/// spare-capacity worker never redelivers it, and it runs exactly once. This is
/// the opt-in fix for the default's redelivery (see `default_no_timeout_redelivers`).
#[tokio::test(start_paused = true)]
async fn keepalive_holds_slow_handler_without_timeout() {
    let clock = Arc::new(ManualClock::new());
    let lease = Duration::from_secs(10); // heartbeat every ~3.3s (lease/3)
    let poll = Duration::from_secs(1);
    let broker = Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(lease));
    let client = Client::new(broker.clone());
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    // No handler timeout, but keepalive is enabled.
    let worker = {
        let mut w = Worker::new(broker.clone())
            .with_concurrency(2)
            .with_poll_interval(poll)
            .with_lease_keepalive(true);
        w.register(GateJob {
            runs: runs.clone(),
            gate: gate.clone(),
        })
        .unwrap();
        Arc::new(w.build().unwrap())
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

    spin_until(|| runs.load(SeqCst) >= 1).await;
    yield_now().await;

    // Advance 15s (past the 10s lease) in 1s steps. Keepalive heartbeats keep
    // extending the lease, so the spare-capacity worker never re-reserves.
    step(&clock, Duration::from_secs(1), 15).await;
    assert_eq!(
        runs.load(SeqCst),
        1,
        "keepalive must hold the lease so the job is not redelivered without a timeout"
    );

    // Release the handler: it completes and is acked exactly once. With no
    // timeout, it would have run as long as needed.
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

/// A non-cooperative handler that blocks its thread without yielding is still
/// bounded by the timeout: with the handler on its own task, the timeout fires on
/// a free worker thread, the job is dead-lettered, and the worker keeps processing
/// other jobs rather than wedging its slot. This needs real time and a
/// multi-thread runtime, because the behaviour under test is a blocked thread,
/// which paused virtual time cannot model. (Before the handler was decoupled the
/// timeout shared the handler's task and could not fire here at all.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonyielding_handler_is_timed_out_without_wedging() {
    struct BlockJob;
    #[async_trait]
    impl Job for BlockJob {
        type Payload = Unit;
        type Output = ();
        const KIND: &'static str = "block";
        async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
            // Block the worker thread for far longer than the timeout without ever
            // yielding at an `.await` — stands in for a tight CPU loop or a
            // blocking syscall.
            std::thread::sleep(Duration::from_secs(1));
            Ok(())
        }
    }
    struct QuickJob {
        ran: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Job for QuickJob {
        type Payload = Unit;
        type Output = ();
        const KIND: &'static str = "quick";
        async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
            self.ran.fetch_add(1, SeqCst);
            Ok(())
        }
    }

    let broker = Arc::new(InMemoryBroker::new().with_lease(Duration::from_secs(30)));
    let client = Client::new(broker.clone()).with_max_attempts(1);
    let quick_ran = Arc::new(AtomicUsize::new(0));

    let mut w = Worker::new(broker.clone())
        .with_concurrency(2)
        .with_poll_interval(Duration::from_millis(10))
        .with_handler_timeout(Duration::from_millis(200));
    w.register(BlockJob).unwrap();
    w.register(QuickJob {
        ran: quick_ran.clone(),
    })
    .unwrap();
    let worker = Arc::new(w.build().unwrap());

    client.enqueue::<BlockJob>(Unit).await.unwrap();
    client.enqueue::<QuickJob>(Unit).await.unwrap();

    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // Well before the 1s block would finish: the quick job completed (the worker
    // is not wedged) and the blocking job was dead-lettered by the timeout.
    let observed = timeout(Duration::from_millis(800), async {
        loop {
            if quick_ran.load(SeqCst) >= 1 && !broker.dead_letters().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        observed.is_ok(),
        "the timeout must fire and the quick job must run before the blocking handler finishes"
    );

    let dead = broker.dead_letters();
    assert_eq!(dead.len(), 1, "the blocking job is dead-lettered");
    assert_eq!(dead[0].envelope.kind, "block");
    assert!(
        dead[0].error.contains("timed out"),
        "dead-letter records a timeout error, got: {}",
        dead[0].error
    );
    assert_eq!(
        quick_ran.load(SeqCst),
        1,
        "the quick job ran while the other handler was blocked"
    );

    let _ = tx.send(());
    // Generous: the orphaned blocking handler holds its thread until its sleep
    // ends; the worker itself has already drained.
    let _ = timeout(Duration::from_secs(5), handle).await;
}

/// A handler that panics is contained on the timeout path too. With a timeout
/// configured the handler runs on its own task, so the panic surfaces as a
/// `JoinError` rather than via the inline `catch_unwind`; it is still routed to
/// the failure path and dead-lettered, and the worker survives. (The no-timeout
/// inline path is covered in `panic_isolation.rs`.)
#[tokio::test(start_paused = true)]
async fn panicking_handler_with_timeout_is_dead_lettered() {
    struct PanicJob;
    #[async_trait]
    impl Job for PanicJob {
        type Payload = Unit;
        type Output = ();
        const KIND: &'static str = "panic";
        async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
            panic!("boom in handler");
        }
    }

    let clock = Arc::new(ManualClock::new());
    let broker =
        Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(Duration::from_secs(30)));
    let client = Client::new(broker.clone()).with_max_attempts(1);
    let mut w = Worker::new(broker.clone())
        .with_concurrency(1)
        .with_poll_interval(Duration::from_secs(1))
        // A timeout is configured (so the handler runs on its own task), but the
        // panic fires long before it.
        .with_handler_timeout(Duration::from_secs(600));
    w.register(PanicJob).unwrap();
    let worker = Arc::new(w.build().unwrap());

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

    spin_until(|| !broker.dead_letters().is_empty()).await;
    let dead = broker.dead_letters();
    assert_eq!(dead.len(), 1, "the panicking job is dead-lettered");
    assert!(
        dead[0].error.contains("panicked"),
        "dead-letter records the panic, got: {}",
        dead[0].error
    );
    assert_eq!(
        broker.len(),
        0,
        "no live job remains; the worker survived the panic"
    );

    let _ = tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .expect("worker survived the handler panic")
        .expect("task join")
        .expect("run result");
}
