//! Cooperative cancellation: when the worker abandons a job's reservation lease
//! (here, the lease is lost mid-flight under keepalive), it signals the handler's
//! `JobContext` so a cooperative handler can observe `is_cancelled()` and bail
//! out instead of doing work that will be redelivered.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::time::timeout;
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;
use worklane_test::ManualClock;

#[derive(Serialize, Deserialize)]
struct Unit;

/// A handler that does one long "work chunk" (a real timer await, so the task
/// parks rather than busy-loops), then checks cancellation when the chunk ends.
struct CancelWatchJob {
    started: Arc<AtomicBool>,
    observed: Arc<AtomicBool>,
}

#[async_trait]
impl Job for CancelWatchJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "cancel_watch";
    async fn run(&self, ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
        self.started.store(true, SeqCst);
        tokio::time::sleep(Duration::from_secs(3600)).await;
        if ctx.is_cancelled() {
            self.observed.store(true, SeqCst);
        }
        Ok(())
    }
}

async fn spin_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if cond() {
            return;
        }
        yield_now().await;
    }
    panic!("condition not reached within bound");
}

/// Losing the lease mid-handler (a heartbeat comes back stale) flips the job's
/// cancellation flag, which a cooperative handler observes via `is_cancelled()`.
#[tokio::test(start_paused = true)]
async fn lease_loss_signals_cancellation() {
    let clock = Arc::new(ManualClock::new());
    let lease = Duration::from_secs(10);
    let broker = Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(lease));
    let client = Client::new(broker.clone());

    let started = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicBool::new(false));
    let worker = {
        // Keepalive makes the worker heartbeat (so run_maintained drives the
        // lease), with no hard deadline.
        let mut w = Worker::new(broker.clone()).with_lease_keepalive(true);
        w.register(CancelWatchJob {
            started: started.clone(),
            observed: observed.clone(),
        })
        .unwrap();
        Arc::new(w.build().unwrap())
    };

    client.enqueue::<CancelWatchJob>(Unit).await.unwrap();

    let (tx, rx) = oneshot::channel::<()>();
    let run_worker = worker.clone();
    let handle = tokio::spawn(async move {
        run_worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });

    // The handler is running and watching for cancellation.
    spin_until(|| started.load(SeqCst)).await;

    // Expire the lease on the broker's clock *before* any heartbeat fires, then
    // let the heartbeat tick elapse: the extend finds the lease already gone and
    // signals cancellation while the handler is mid-chunk.
    clock.advance(Duration::from_secs(11));
    tokio::time::advance(lease / 3 + Duration::from_millis(1)).await;
    // End the handler's work chunk; it then observes the cancellation.
    tokio::time::advance(Duration::from_secs(3600)).await;

    spin_until(|| observed.load(SeqCst)).await;
    assert!(
        observed.load(SeqCst),
        "the handler observed cancellation after its lease was lost"
    );

    let _ = tx.send(());
    let _ = timeout(Duration::from_secs(1), handle).await;
}
