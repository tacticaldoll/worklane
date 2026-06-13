//! Tests for the long-running `Worker::run` daemon loop and cooperative
//! shutdown. Time is driven deterministically with paused tokio time; no test
//! sleeps in real time.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct Unit;

/// Always succeeds.
struct OkJob;

#[async_trait]
impl Job for OkJob {
    type Payload = Unit;
    const KIND: &'static str = "ok";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult {
        Ok(())
    }
}

/// Fires a shutdown signal from inside its handler, to exercise shutdown that
/// arrives while a job is in flight.
struct ShutdownJob {
    tx: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl Job for ShutdownJob {
    type Payload = Unit;
    const KIND: &'static str = "shutdown";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult {
        if let Some(tx) = self.tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

/// `run` drains every currently available job, then returns once shutdown is
/// signalled while the lane is idle.
#[tokio::test(start_paused = true)]
async fn run_drains_then_stops_when_idle() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let worker = {
        let mut w = Worker::new(broker.clone());
        w.register(OkJob).unwrap();
        Arc::new(w)
    };

    for _ in 0..3 {
        client.enqueue::<OkJob>(Unit).await.unwrap();
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

    // Let the worker drain all three and reach its idle wait.
    tokio::task::yield_now().await;
    assert_eq!(broker.len(), 0, "all available jobs drained");

    // Shutdown while idle: run returns.
    let _ = tx.send(());
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("run should return after shutdown")
        .expect("task join")
        .expect("run result");
}

/// An idle worker picks up a job that becomes available later, on the next poll.
#[tokio::test(start_paused = true)]
async fn run_picks_up_work_after_idle() {
    let poll_interval = Duration::from_secs(2);
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let worker = {
        let mut w = Worker::new(broker.clone()).with_poll_interval(poll_interval);
        w.register(OkJob).unwrap();
        Arc::new(w)
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

    // Let the worker poll once (finds nothing) and park on its idle wait.
    tokio::task::yield_now().await;

    // A job appears while the worker is idle.
    client.enqueue::<OkJob>(Unit).await.unwrap();
    assert_eq!(broker.len(), 1, "job enqueued while worker idles");

    // Fire the poll tick so the worker wakes and processes it.
    tokio::time::advance(poll_interval).await;
    tokio::task::yield_now().await;

    assert_eq!(broker.len(), 0, "idle worker picks up the new job");

    let _ = tx.send(());
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("run should return after shutdown")
        .expect("task join")
        .expect("run result");
}

/// Shutdown that arrives during a job's handler lets that job finish and be
/// acked, and stops the worker from reserving the next job.
#[tokio::test]
async fn cooperative_shutdown_finishes_in_flight_job() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let (tx, rx) = oneshot::channel::<()>();
    let mut worker = Worker::new(broker.clone());
    worker
        .register(ShutdownJob {
            tx: Mutex::new(Some(tx)),
        })
        .unwrap();
    worker.register(OkJob).unwrap();

    // First job fires shutdown mid-run; second job must not be reserved.
    client.enqueue::<ShutdownJob>(Unit).await.unwrap();
    client.enqueue::<OkJob>(Unit).await.unwrap();

    worker
        .run(async {
            let _ = rx.await;
        })
        .await
        .unwrap();

    assert_eq!(
        broker.len(),
        1,
        "in-flight job acked; the next job is left unreserved after shutdown"
    );
}
