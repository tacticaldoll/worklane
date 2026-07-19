// Tests for the recurring (cron) `Scheduler`. Time is driven deterministically:
// a `ManualClock` supplies civil time to cron, and paused tokio time fires the
// daemon's sleeps. To advance one tick we move the manual clock to the target
// instant and advance tokio time enough to wake the parked sleep.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use worklane_core::{
    Broker, Clock, Error, HandlerResult, Job, JobContext, JobId, Lane, NewJob, Reservation,
    ReservationReceipt, Result as WlResult, ScheduledStore,
};
use worklane_memory::InMemoryBroker;
use worklane_scheduler::Scheduler;
use worklane_test::ManualClock;

/// A broker that fails its first `fail_first` `enqueue_scheduled` calls with a
/// transient broker error, then delegates to an inner in-memory broker. Used to
/// exercise the scheduler's fail-fast vs resilient handling of fire errors.
struct FlakyScheduledBroker {
    inner: Arc<InMemoryBroker>,
    fail_first: AtomicUsize,
}

impl FlakyScheduledBroker {
    fn new(inner: Arc<InMemoryBroker>, fail_first: usize) -> Self {
        FlakyScheduledBroker {
            inner,
            fail_first: AtomicUsize::new(fail_first),
        }
    }
}

#[async_trait]
impl Broker for FlakyScheduledBroker {
    async fn enqueue(&self, job: NewJob) -> WlResult<JobId> {
        self.inner.enqueue(job).await
    }
    async fn reserve(&self, lane: &Lane) -> WlResult<Option<Reservation>> {
        self.inner.reserve(lane).await
    }
    async fn ack(&self, receipt: ReservationReceipt) -> WlResult<()> {
        self.inner.ack(receipt).await
    }
    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> WlResult<()> {
        self.inner.retry(receipt, delay).await
    }
    async fn defer(&self, receipt: ReservationReceipt, delay: Duration) -> WlResult<()> {
        self.inner.defer(receipt, delay).await
    }
    async fn extend(&self, receipt: ReservationReceipt) -> WlResult<()> {
        self.inner.extend(receipt).await
    }
    async fn fail(&self, receipt: ReservationReceipt, error: String) -> WlResult<()> {
        self.inner.fail(receipt, error).await
    }
    async fn classify(&self, id: JobId) -> WlResult<worklane_core::JobState> {
        self.inner.classify(id).await
    }
    fn dead_letter_store(&self) -> Option<&dyn worklane_core::DeadLetterStore> {
        self.inner.dead_letter_store()
    }
    fn queue_stats(&self) -> Option<&dyn worklane_core::QueueStats> {
        self.inner.queue_stats()
    }
    fn scheduled_store(self: Arc<Self>) -> Option<Arc<dyn worklane_core::ScheduledStore>> {
        Some(self)
    }
}

#[derive(Serialize, Deserialize)]
struct Unit;

struct Tick;

#[async_trait]
impl Job for Tick {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "tick";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
        Ok(())
    }
}

/// Cron firing at second 0 of every minute (`sec min hour dom mon dow`).
const EVERY_MINUTE: &str = "0 * * * * *";
const MIN: Duration = Duration::from_secs(60);

async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

/// Move both clocks forward so the daemon, parked on a sleep of `wake_after`,
/// wakes and re-reads the manual clock now sitting at `manual_to`.
async fn tick_to(clock: &ManualClock, manual_to: Duration, wake_after: Duration) {
    // Manual clock first, so the woken loop reads the advanced civil time.
    let current = clock.now();
    if manual_to > current {
        clock.advance(manual_to - current);
    }
    tokio::time::advance(wake_after).await;
    settle().await;
}

fn spawn_run(scheduler: Arc<Scheduler>, rx: oneshot::Receiver<()>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        scheduler
            .run(async {
                let _ = rx.await;
            })
            .await
            .expect("scheduler run ok");
    })
}

/// A due schedule enqueues its templated job to the target lane.
#[tokio::test(start_paused = true)]
async fn fires_due_schedule_to_lane() {
    let broker = Arc::new(InMemoryBroker::new());
    let clock = Arc::new(ManualClock::new());
    let scheduler = {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone());
        s.schedule_to::<Tick>("t", EVERY_MINUTE, "critical".parse().unwrap(), Unit)
            .unwrap();
        Arc::new(s)
    };
    let (_tx, rx) = oneshot::channel();
    let handle = spawn_run(scheduler, rx);

    settle().await; // seed cursor (next fire at 00:01:00) and park
    tick_to(&clock, MIN, MIN).await;

    assert_eq!(broker.len(), 1, "one job enqueued at the first occurrence");
    assert!(
        broker
            .reserve(&"critical".parse().unwrap())
            .await
            .unwrap()
            .is_some(),
        "the job is on the targeted lane",
    );
    handle.abort();
}

/// Successive occurrences enqueue once each as the clock advances.
#[tokio::test(start_paused = true)]
async fn fires_once_per_occurrence() {
    let broker = Arc::new(InMemoryBroker::new());
    let clock = Arc::new(ManualClock::new());
    let scheduler = {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone());
        s.schedule::<Tick>("t", EVERY_MINUTE, Unit).unwrap();
        Arc::new(s)
    };
    let (_tx, rx) = oneshot::channel();
    let handle = spawn_run(scheduler, rx);

    settle().await;
    tick_to(&clock, MIN, MIN).await;
    assert_eq!(broker.len(), 1, "first minute");
    tick_to(&clock, 2 * MIN, MIN).await;
    assert_eq!(broker.len(), 2, "second minute");
    handle.abort();
}

/// Two schedules due at the same instant each enqueue.
#[tokio::test(start_paused = true)]
async fn multiple_schedules_due_together() {
    let broker = Arc::new(InMemoryBroker::new());
    let clock = Arc::new(ManualClock::new());
    let scheduler = {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone());
        s.schedule::<Tick>("a", EVERY_MINUTE, Unit).unwrap();
        s.schedule::<Tick>("b", EVERY_MINUTE, Unit).unwrap();
        Arc::new(s)
    };
    let (_tx, rx) = oneshot::channel();
    let handle = spawn_run(scheduler, rx);

    settle().await;
    tick_to(&clock, MIN, MIN).await;
    assert_eq!(broker.len(), 2, "both schedules fired at the same minute");
    handle.abort();
}

/// Occurrences missed while not running are not backfilled: a single fire.
#[tokio::test(start_paused = true)]
async fn missed_occurrences_not_backfilled() {
    let broker = Arc::new(InMemoryBroker::new());
    let clock = Arc::new(ManualClock::new());
    let scheduler = {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone());
        s.schedule::<Tick>("t", EVERY_MINUTE, Unit).unwrap();
        Arc::new(s)
    };
    let (_tx, rx) = oneshot::channel();
    let handle = spawn_run(scheduler, rx);

    settle().await; // parked on a 60s sleep (next fire 00:01:00)
    // Jump civil time five minutes ahead, then wake the parked sleep.
    tick_to(&clock, 5 * MIN, MIN).await;
    assert_eq!(
        broker.len(),
        1,
        "five missed minutes collapse to a single fire, not five",
    );
    handle.abort();
}

/// A `ScheduledStore` whose first `enqueue_scheduled` advances the clock past
/// the next occurrence before returning, simulating a slow fire (broker I/O).
/// Subsequent calls delegate without advancing, so any backfilled fire is
/// directly observable as an extra enqueued job.
struct SlowFire {
    inner: Arc<InMemoryBroker>,
    clock: Arc<ManualClock>,
    bump: Duration,
    advanced: AtomicBool,
}

#[async_trait]
impl ScheduledStore for SlowFire {
    async fn enqueue_scheduled(
        &self,
        schedule_id: &str,
        occurrence: i64,
        job: NewJob,
    ) -> WlResult<bool> {
        if !self.advanced.swap(true, Ordering::SeqCst) {
            self.clock.advance(self.bump);
        }
        self.inner
            .enqueue_scheduled(schedule_id, occurrence, job)
            .await
    }
    async fn remove_schedule(&self, schedule_id: &str) -> WlResult<()> {
        self.inner.remove_schedule(schedule_id).await
    }
}

/// A slow fire must not backfill an occurrence that elapses while it runs: the
/// cursor advances past the clock as observed *after* the fire, not the stale
/// pre-fire time. Regression test for the scheduler stale-`now` over-fire.
#[tokio::test(start_paused = true)]
async fn slow_fire_does_not_backfill_elapsed_occurrence() {
    let inner = Arc::new(InMemoryBroker::new());
    let clock = Arc::new(ManualClock::new());
    // The first fire takes ~90s of civil time — crossing the next minute
    // occurrence (00:02:00) — before enqueue_scheduled returns.
    let store = Arc::new(SlowFire {
        inner: inner.clone(),
        clock: clock.clone(),
        bump: Duration::from_secs(90),
        advanced: AtomicBool::new(false),
    });
    let scheduler = {
        let mut s = Scheduler::with_scheduled_store(store).with_clock(clock.clone());
        s.schedule::<Tick>("t", EVERY_MINUTE, Unit).unwrap();
        Arc::new(s)
    };
    let (_tx, rx) = oneshot::channel();
    let handle = spawn_run(scheduler, rx);

    settle().await; // seed cursor at 00:01:00, park
    // Wake the daemon at 00:01:00; firing advances the clock to ~00:02:30.
    tick_to(&clock, MIN, MIN).await;
    // Give any erroneously-scheduled short follow-up sleep room to wake. With the
    // stale-`now` bug the cursor would have advanced only to 00:02:00, already
    // past the post-fire clock, so the daemon would fire 00:02:00 a second time.
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;

    assert_eq!(
        inner.len(),
        1,
        "an occurrence elapsing during a slow fire must be skipped, not backfilled",
    );
    handle.abort();
}

/// Shutdown interrupts the wait for the next due time.
#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_wait() {
    let broker = Arc::new(InMemoryBroker::new());
    let clock = Arc::new(ManualClock::new());
    let scheduler = {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone());
        // Yearly: the daemon parks on a ~year-long sleep.
        s.schedule::<Tick>("t", "0 0 0 1 1 *", Unit).unwrap();
        Arc::new(s)
    };
    let (tx, rx) = oneshot::channel();
    let handle = spawn_run(scheduler, rx);

    settle().await;
    let _ = tx.send(());
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("run returns promptly on shutdown, not after a year")
        .expect("task join");
}

/// The HA coordination (via `enqueue_scheduled`) makes overlapping schedulers
/// idempotent for the same instant, even if per-fire dedup is not explicitly enabled.
#[tokio::test(start_paused = true)]
async fn ha_coordination_prevents_double_firing() {
    // Two schedulers over one broker, same clock, same schedule+instant.
    let broker = Arc::new(InMemoryBroker::new());
    let clock = Arc::new(ManualClock::new());
    let make = || {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone());
        s.schedule::<Tick>("t", EVERY_MINUTE, Unit).unwrap();
        Arc::new(s)
    };

    // Both schedulers fire the same instant → one live job due to HA coordination.
    let (_t1, r1) = oneshot::channel();
    let (_t2, r2) = oneshot::channel();
    let h1 = spawn_run(make(), r1);
    let h2 = spawn_run(make(), r2);
    settle().await;
    tick_to(&clock, MIN, MIN).await;
    assert_eq!(
        broker.len(),
        1,
        "HA coordination keeps one live job for the same fire"
    );
    h1.abort();
    h2.abort();
}

/// An invalid cron expression is rejected when the schedule is added.
#[tokio::test]
async fn invalid_cron_rejected() {
    let broker = Arc::new(InMemoryBroker::new());
    let mut s = Scheduler::new(broker.clone()).unwrap();
    let err = s.schedule::<Tick>("bad", "not a cron", Unit);
    assert!(err.is_err(), "unparseable cron expression must be rejected");
}

/// Default (fail-fast) mode: a broker error while firing ends `run` with that
/// error.
#[tokio::test(start_paused = true)]
async fn fail_fast_surfaces_fire_error() {
    let inner = Arc::new(InMemoryBroker::new());
    let broker = Arc::new(FlakyScheduledBroker::new(inner.clone(), usize::MAX));
    let clock = Arc::new(ManualClock::new());
    let scheduler = {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone());
        s.schedule::<Tick>("t", EVERY_MINUTE, Unit).unwrap();
        Arc::new(s)
    };
    let (_tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        scheduler
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    settle().await;
    tick_to(&clock, MIN, MIN).await;

    let result = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("run returns promptly on a fire error")
        .expect("task join");
    assert!(
        result.is_err(),
        "fail-fast mode surfaces the broker error from run"
    );
    assert_eq!(inner.len(), 0, "no job was enqueued");
}

/// Resilient mode: a transient fire error is logged and the loop continues, so a
/// later occurrence still fires once the broker recovers.
#[tokio::test(start_paused = true)]
async fn resilient_mode_logs_and_continues() {
    let inner = Arc::new(InMemoryBroker::new());
    // Fail only the first fire; the next occurrence succeeds.
    let broker = Arc::new(FlakyScheduledBroker::new(inner.clone(), 1));
    let clock = Arc::new(ManualClock::new());
    let scheduler = {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone())
            .with_resilient(true);
        s.schedule::<Tick>("t", EVERY_MINUTE, Unit).unwrap();
        Arc::new(s)
    };
    let (_tx, rx) = oneshot::channel();
    let handle = spawn_run(scheduler, rx);

    settle().await;
    // First occurrence: fire fails, is logged, loop continues; nothing enqueued.
    tick_to(&clock, MIN, MIN).await;
    assert_eq!(inner.len(), 0, "the failed first fire enqueued nothing");
    // Second occurrence: broker has recovered, the fire succeeds.
    tick_to(&clock, 2 * MIN, MIN).await;
    assert_eq!(
        inner.len(),
        1,
        "resilient mode kept running and fired the next occurrence"
    );
    handle.abort();
}

/// Resilient mode still honours cooperative shutdown.
#[tokio::test(start_paused = true)]
async fn resilient_mode_honours_shutdown() {
    let inner = Arc::new(InMemoryBroker::new());
    let broker = Arc::new(FlakyScheduledBroker::new(inner, usize::MAX));
    let clock = Arc::new(ManualClock::new());
    let scheduler = {
        let mut s = Scheduler::new(broker.clone())
            .unwrap()
            .with_clock(clock.clone())
            .with_resilient(true);
        s.schedule::<Tick>("t", "0 0 0 1 1 *", Unit).unwrap();
        Arc::new(s)
    };
    let (tx, rx) = oneshot::channel();
    let handle = spawn_run(scheduler, rx);

    settle().await;
    let _ = tx.send(());
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("resilient run returns promptly on shutdown")
        .expect("task join");
}

/// A duplicate schedule id is rejected at registration. The id is the
/// cluster-wide occurrence key (it keys both the `enqueue_scheduled` watermark
/// and the dedup `unique_key`), so two entries sharing it would have one
/// silently swallow the other's fires — fail loudly at registration instead.
#[tokio::test]
async fn duplicate_schedule_id_rejected() {
    let broker = Arc::new(InMemoryBroker::new());
    let mut s = Scheduler::new(broker.clone()).unwrap();
    s.schedule::<Tick>("dup", EVERY_MINUTE, Unit)
        .expect("first registration with a fresh id succeeds");
    // `expect_err` is unavailable here: the `Ok` value is `&mut Scheduler`, which
    // is not `Debug`. Match instead.
    let err = match s.schedule::<Tick>("dup", EVERY_MINUTE, Unit) {
        Ok(_) => panic!("a second registration with the same id must be rejected"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("already registered"),
        "the error must explain the id is a duplicate, got: {err}"
    );
}

#[async_trait::async_trait]
impl worklane_core::ScheduledStore for FlakyScheduledBroker {
    async fn enqueue_scheduled(
        &self,
        schedule_id: &str,
        occurrence: i64,
        job: NewJob,
    ) -> WlResult<bool> {
        // Fail transiently while the counter is positive, then delegate.
        if self
            .fail_first
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n > 0 { Some(n - 1) } else { None }
            })
            .is_ok()
        {
            return Err(Error::Broker("transient enqueue_scheduled failure".into()));
        }
        self.inner
            .enqueue_scheduled(schedule_id, occurrence, job)
            .await
    }
    async fn remove_schedule(&self, schedule_id: &str) -> WlResult<()> {
        self.inner.remove_schedule(schedule_id).await
    }
}
