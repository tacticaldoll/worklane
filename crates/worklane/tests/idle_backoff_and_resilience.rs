// Tests for the worker's adaptive idle backoff and resilient daemon mode.
// Time is driven deterministically with paused tokio time; no real sleeps.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use worklane::{
    Broker, Client, Error, HandlerResult, Job, JobContext, JobId, Lane, NewJob, Reservation,
    ReservationReceipt, Result, Worker, async_trait,
};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct Unit;

struct OkJob;

#[async_trait]
impl Job for OkJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "ok";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
        Ok(())
    }
}

/// Wraps an in-memory broker to count `reserve` calls and, on demand, inject a
/// non-stale broker error from `reserve` — enough to drive both the backoff
/// cadence and the resilient-mode tests.
struct FlakyBroker {
    inner: InMemoryBroker,
    reserves: AtomicU64,
    fail_reserve: AtomicBool,
}

impl FlakyBroker {
    fn new() -> Self {
        FlakyBroker {
            inner: InMemoryBroker::new(),
            reserves: AtomicU64::new(0),
            fail_reserve: AtomicBool::new(false),
        }
    }
    fn reserve_count(&self) -> u64 {
        self.reserves.load(Ordering::SeqCst)
    }
    fn set_failing(&self, failing: bool) {
        self.fail_reserve.store(failing, Ordering::SeqCst);
    }
}

#[async_trait]
impl Broker for FlakyBroker {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        self.inner.enqueue(job).await
    }
    async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        self.inner.enqueue_batch(jobs).await
    }
    async fn reserve(&self, lane: &Lane) -> Result<Option<Reservation>> {
        self.reserves.fetch_add(1, Ordering::SeqCst);
        if self.fail_reserve.load(Ordering::SeqCst) {
            return Err(Error::Broker("injected transient error".into()));
        }
        self.inner.reserve(lane).await
    }
    async fn ack(&self, receipt: ReservationReceipt) -> Result<()> {
        self.inner.ack(receipt).await
    }
    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        self.inner.retry(receipt, delay).await
    }
    async fn defer(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        self.inner.defer(receipt, delay).await
    }
    async fn extend(&self, receipt: ReservationReceipt) -> Result<()> {
        self.inner.extend(receipt).await
    }
    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()> {
        self.inner.fail(receipt, error).await
    }
    async fn classify(&self, id: JobId) -> Result<worklane_core::JobState> {
        self.inner.classify(id).await
    }
    fn dead_letter_store(&self) -> Option<&dyn worklane_core::DeadLetterStore> {
        self.inner.dead_letter_store()
    }
    fn queue_stats(&self) -> Option<&dyn worklane_core::QueueStats> {
        self.inner.queue_stats()
    }
    fn scheduled_store(self: Arc<Self>) -> Option<Arc<dyn worklane_core::ScheduledStore>> {
        // FlakyBroker implements ScheduledStore; expose it through the accessor
        // so the concrete and `dyn Broker` views agree.
        Some(self)
    }
}

/// Yield enough times for a woken worker task to run one full loop cycle and
/// re-park. Time is paused, so this spins the scheduler, not the clock.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

const SEC: Duration = Duration::from_secs(1);

/// Idle backoff doubles from base toward the cap across consecutive empty polls
/// and never exceeds the cap.
#[tokio::test(start_paused = true)]
async fn idle_backoff_grows_and_caps() {
    let broker = Arc::new(FlakyBroker::new());
    let worker = {
        let mut w = Worker::new(broker.clone()).with_idle_backoff(SEC, 4 * SEC);
        w.register(OkJob).unwrap();
        Arc::new(w.build().unwrap())
    };
    let (_tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    settle().await;
    assert_eq!(broker.reserve_count(), 1, "first poll, now waiting base=1s");

    tokio::time::advance(SEC).await;
    settle().await;
    assert_eq!(
        broker.reserve_count(),
        2,
        "base elapsed → poll; now waiting 2s"
    );

    tokio::time::advance(SEC).await;
    settle().await;
    assert_eq!(
        broker.reserve_count(),
        2,
        "only 1s of the 2s wait → no poll (grew)"
    );

    tokio::time::advance(SEC).await;
    settle().await;
    assert_eq!(
        broker.reserve_count(),
        3,
        "2s elapsed → poll; now waiting 4s"
    );

    tokio::time::advance(2 * SEC).await;
    settle().await;
    assert_eq!(
        broker.reserve_count(),
        3,
        "only 2s of the 4s wait → no poll (grew)"
    );

    tokio::time::advance(2 * SEC).await;
    settle().await;
    assert_eq!(
        broker.reserve_count(),
        4,
        "4s elapsed → poll; now waiting 4s (cap)"
    );

    tokio::time::advance(4 * SEC).await;
    settle().await;
    assert_eq!(
        broker.reserve_count(),
        5,
        "capped at 4s — would still be waiting if uncapped (8s)"
    );

    handle.abort();
}

/// Finding work resets the backoff to base.
#[tokio::test(start_paused = true)]
async fn idle_backoff_resets_on_work() {
    let broker = Arc::new(FlakyBroker::new());
    let client = Client::new(broker.clone());
    let worker = {
        let mut w = Worker::new(broker.clone()).with_idle_backoff(SEC, 8 * SEC);
        w.register(OkJob).unwrap();
        Arc::new(w.build().unwrap())
    };
    let (_tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // Grow the wait to 4s (empty polls at 1s, then 2s).
    settle().await; // poll, wait 1s
    tokio::time::advance(SEC).await;
    settle().await; // poll, wait 2s
    tokio::time::advance(2 * SEC).await;
    settle().await; // poll, wait 4s

    // Work appears; wake after the current 4s wait and process it.
    client.enqueue::<OkJob>(Unit).await.unwrap();
    tokio::time::advance(4 * SEC).await;
    settle().await;
    assert_eq!(broker.inner.len(), 0, "job picked up after the grown wait");

    // Backoff must be reset: a second job is picked up after just the base (1s),
    // not the grown/cap wait it would otherwise be.
    client.enqueue::<OkJob>(Unit).await.unwrap();
    tokio::time::advance(SEC).await;
    settle().await;
    assert_eq!(
        broker.inner.len(),
        0,
        "wait reset to base; job picked up after 1s"
    );

    handle.abort();
}

/// Shutdown interrupts an in-progress idle backoff wait, even a long one.
#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_idle_wait() {
    let broker = Arc::new(InMemoryBroker::new());
    let worker = {
        let mut w = Worker::new(broker.clone()).with_idle_backoff(100 * SEC, 100 * SEC);
        w.register(OkJob).unwrap();
        Arc::new(w.build().unwrap())
    };
    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    settle().await; // worker parks on a 100s idle wait
    let _ = tx.send(()); // shutdown while waiting
    tokio::time::timeout(SEC, handle)
        .await
        .expect("run returns promptly without waiting out the 100s backoff")
        .expect("task join")
        .expect("run result is Ok");
}

/// Default (non-resilient) mode fails fast on a non-stale broker error.
#[tokio::test(start_paused = true)]
async fn default_mode_fails_fast_on_broker_error() {
    let broker = Arc::new(FlakyBroker::new());
    broker.set_failing(true);
    let worker = {
        let mut w = Worker::new(broker.clone());
        w.register(OkJob).unwrap();
        Arc::new(w.build().unwrap())
    };
    let (_tx, rx) = oneshot::channel::<()>();
    let result = tokio::time::timeout(
        SEC,
        worker.run(async {
            let _ = rx.await;
        }),
    )
    .await
    .expect("fail-fast returns without waiting");
    assert!(
        matches!(result, Err(Error::Broker(_))),
        "non-stale broker error is surfaced"
    );
}

/// Resilient mode logs and keeps running through broker errors, then resumes
/// once the broker recovers.
#[tokio::test(start_paused = true)]
async fn resilient_mode_continues_then_recovers() {
    let broker = Arc::new(FlakyBroker::new());
    broker.set_failing(true);
    let client = Client::new(broker.clone());
    let worker = {
        let mut w = Worker::new(broker.clone()).with_resilient(true);
        w.register(OkJob).unwrap();
        Arc::new(w.build().unwrap())
    };
    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // Erroring: the worker keeps polling and backing off, run does not return.
    settle().await;
    tokio::time::advance(SEC).await;
    settle().await;
    assert!(
        !handle.is_finished(),
        "resilient run keeps going through errors"
    );
    assert!(
        broker.reserve_count() >= 2,
        "it retried reserve while failing"
    );

    // Broker recovers and a job appears.
    broker.set_failing(false);
    client.enqueue::<OkJob>(Unit).await.unwrap();
    tokio::time::advance(SEC).await;
    settle().await;
    assert_eq!(
        broker.inner.len(),
        0,
        "worker resumes and processes the job"
    );

    let _ = tx.send(());
    tokio::time::timeout(SEC, handle)
        .await
        .expect("run returns after shutdown")
        .expect("task join")
        .expect("run result is Ok");
}

/// Shutdown is honoured in resilient mode (drains and returns Ok) even while the
/// broker is erroring.
#[tokio::test(start_paused = true)]
async fn resilient_mode_honours_shutdown() {
    let broker = Arc::new(FlakyBroker::new());
    broker.set_failing(true);
    let worker = {
        let mut w = Worker::new(broker.clone()).with_resilient(true);
        w.register(OkJob).unwrap();
        Arc::new(w.build().unwrap())
    };
    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    settle().await; // erroring + backing off
    let _ = tx.send(());
    tokio::time::timeout(SEC, handle)
        .await
        .expect("run returns promptly on shutdown")
        .expect("task join")
        .expect("run result is Ok even though the broker was failing");
}

/// Idle backoff triggers and grows even if there are jobs currently in-flight
/// (concurrency > 1 and spare capacity).
#[tokio::test(start_paused = true)]
async fn idle_backoff_grows_when_jobs_in_flight() {
    let broker = Arc::new(FlakyBroker::new());
    let client = Client::new(broker.clone());

    // We need a job that takes a long time so it stays in-flight.
    struct LongJob;
    #[async_trait]
    impl Job for LongJob {
        type Payload = Unit;
        type Output = ();
        const KIND: &'static str = "long";
        async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
            // Sleep for 100 seconds to simulate a long running job
            tokio::time::sleep(100 * SEC).await;
            Ok(())
        }
    }

    let worker = {
        // Concurrency is 2, so after picking up 1 job, have_capacity is true.
        let mut w = Worker::new(broker.clone())
            .with_concurrency(2)
            .with_idle_backoff(SEC, 4 * SEC);
        w.register(LongJob).unwrap();
        Arc::new(w.build().unwrap())
    };

    let (_tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // Enqueue one long job.
    client.enqueue::<LongJob>(Unit).await.unwrap();
    settle().await;

    // The worker picks up the job. The reserve_count is now 1 (the one that got the job).
    // Because have_capacity is true (1 < 2), it loops again, polls the empty queue (count 2),
    // and should wait base (1s).
    assert_eq!(
        broker.reserve_count(),
        2,
        "picked up job, then polled empty queue"
    );

    tokio::time::advance(SEC).await;
    settle().await;
    assert_eq!(
        broker.reserve_count(),
        3,
        "1s elapsed -> polled empty queue, now waiting 2s"
    );

    tokio::time::advance(SEC).await;
    settle().await;
    assert_eq!(broker.reserve_count(), 3, "only 1s of 2s wait -> no poll");

    tokio::time::advance(SEC).await;
    settle().await;
    assert_eq!(
        broker.reserve_count(),
        4,
        "2s elapsed -> polled empty queue, now waiting 4s"
    );

    handle.abort();
}

#[async_trait::async_trait]
impl worklane_core::ScheduledStore for FlakyBroker {
    async fn enqueue_scheduled(
        &self,
        schedule_id: &str,
        occurrence: i64,
        job: NewJob,
    ) -> Result<bool> {
        self.inner
            .enqueue_scheduled(schedule_id, occurrence, job)
            .await
    }
    async fn remove_schedule(&self, schedule_id: &str) -> Result<()> {
        self.inner.remove_schedule(schedule_id).await
    }
}
