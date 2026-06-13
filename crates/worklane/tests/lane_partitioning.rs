//! Client/Worker lane-routing integration (the broker-level lane isolation
//! invariants live in the shared contract suite in `worklane-test`).

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct Unit;

struct OkJob;

#[async_trait]
impl Job for OkJob {
    type Payload = Unit;
    const KIND: &'static str = "ok";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult {
        Ok(())
    }
}

/// Regression: client and worker both default to `DEFAULT_LANE`, so a job
/// enqueued without a lane is still reserved and run.
#[tokio::test]
async fn default_lane_round_trips() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone());
    worker.register(OkJob).unwrap();

    client.enqueue::<OkJob>(Unit).await.unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(broker.len(), 0, "default-lane job should be acked");
}

/// A worker configured for a custom lane receives a job enqueued to that lane.
#[tokio::test]
async fn worker_receives_its_lane_job() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone()).with_lane("critical");
    let mut worker = Worker::new(broker.clone()).with_lane("critical");
    worker.register(OkJob).unwrap();

    client.enqueue::<OkJob>(Unit).await.unwrap();

    assert!(
        worker.process_next().await.unwrap(),
        "critical worker should receive the critical job"
    );
    assert_eq!(broker.len(), 0);
}

/// A worker on one lane does not reserve another lane's job; that job stays
/// reservable on its own lane.
#[tokio::test]
async fn other_lane_cannot_steal() {
    let broker = Arc::new(InMemoryBroker::new());
    Client::new(broker.clone())
        .with_lane("critical")
        .enqueue::<OkJob>(Unit)
        .await
        .unwrap();

    let mut default_worker = Worker::new(broker.clone()); // DEFAULT_LANE
    default_worker.register(OkJob).unwrap();
    assert!(
        !default_worker.process_next().await.unwrap(),
        "default lane must not steal the critical job"
    );
    assert_eq!(broker.len(), 1, "critical job is still live");

    let mut critical_worker = Worker::new(broker.clone()).with_lane("critical");
    critical_worker.register(OkJob).unwrap();
    assert!(
        critical_worker.process_next().await.unwrap(),
        "critical worker can still take it"
    );
    assert_eq!(broker.len(), 0);
}
