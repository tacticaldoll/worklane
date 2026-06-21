//! `Worker::with_shutdown_timeout` bounds graceful shutdown: a stuck in-flight
//! handler cannot block `run` from returning forever.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct Unit;

/// Never returns — simulates a deadlocked / non-cooperative handler.
struct StuckJob;

#[async_trait]
impl Job for StuckJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "stuck";
    async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
        std::future::pending::<()>().await; // hang forever (cooperatively)
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_timeout_abandons_a_stuck_in_flight_job() {
    let broker = Arc::new(InMemoryBroker::new());
    Client::new(broker.clone())
        .enqueue::<StuckJob>(Unit)
        .await
        .unwrap();

    let mut worker = Worker::new(broker.clone())
        // Long lease so the stuck job stays in-flight; short shutdown timeout.
        .with_shutdown_timeout(Duration::from_millis(150));
    worker.register(StuckJob).unwrap();
    let worker = worker.build().unwrap();

    // Trigger shutdown almost immediately; the handler will be in-flight and hung.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        worker
            .run(async {
                let _ = rx.await;
            })
            .await
    });
    // Let the worker reserve+dispatch the stuck job, then signal shutdown.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = tx.send(());

    // `run` must return within the shutdown timeout (+ slack), not hang forever.
    let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
    assert!(
        result.is_ok(),
        "run() must return after the shutdown timeout despite the stuck handler"
    );
    result
        .unwrap()
        .unwrap()
        .expect("run returns Ok after abandoning the drain");
}
