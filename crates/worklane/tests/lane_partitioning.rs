//! Client/Worker lane-routing integration (the broker-level lane isolation
//! invariants live in the shared contract suite in `worklane-test`).

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
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

/// Records the lane carried by each handler invocation's context.
struct RecordLaneJob {
    seen: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Job for RecordLaneJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "record_lane";
    async fn run(&self, ctx: JobContext, _payload: Unit) -> HandlerResult<()> {
        self.seen.lock().unwrap().push(ctx.lane.to_string());
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
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(broker.len(), 0, "default-lane job should be acked");
}

/// A worker configured for a custom lane receives a job enqueued to that lane.
#[tokio::test]
async fn worker_receives_its_lane_job() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone()).with_lane("critical".parse().unwrap());
    let mut worker = Worker::new(broker.clone()).with_lane("critical".parse().unwrap());
    worker.register(OkJob).unwrap();

    client.enqueue::<OkJob>(Unit).await.unwrap();

    let worker = worker.build().unwrap();
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
        .with_lane("critical".parse().unwrap())
        .enqueue::<OkJob>(Unit)
        .await
        .unwrap();

    let mut default_worker = Worker::new(broker.clone()); // DEFAULT_LANE
    default_worker.register(OkJob).unwrap();
    let default_worker = default_worker.build().unwrap();
    assert!(
        !default_worker.process_next().await.unwrap(),
        "default lane must not steal the critical job"
    );
    assert_eq!(broker.len(), 1, "critical job is still live");

    let mut critical_worker = Worker::new(broker.clone()).with_lane("critical".parse().unwrap());
    critical_worker.register(OkJob).unwrap();
    let critical_worker = critical_worker.build().unwrap();
    assert!(
        critical_worker.process_next().await.unwrap(),
        "critical worker can still take it"
    );
    assert_eq!(broker.len(), 0);
}

/// `enqueue_to` targets a lane for one call without changing the client's
/// configured lane: a later `enqueue` still uses the configured default.
#[tokio::test]
async fn enqueue_to_overrides_lane_for_one_call() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone()); // configured for DEFAULT_LANE

    // Override to "critical" for this one call.
    client
        .enqueue_to::<OkJob>("critical".parse().unwrap(), Unit)
        .await
        .unwrap();
    let mut critical_worker = Worker::new(broker.clone()).with_lane("critical".parse().unwrap());
    critical_worker.register(OkJob).unwrap();
    let critical_worker = critical_worker.build().unwrap();
    assert!(
        critical_worker.process_next().await.unwrap(),
        "enqueue_to should place the job on the overridden lane"
    );

    // The configured lane is unchanged: a plain enqueue goes to the default lane.
    client.enqueue::<OkJob>(Unit).await.unwrap();
    let mut default_worker = Worker::new(broker.clone()); // DEFAULT_LANE
    default_worker.register(OkJob).unwrap();
    let default_worker = default_worker.build().unwrap();
    assert!(
        default_worker.process_next().await.unwrap(),
        "a later enqueue still uses the client's configured default lane"
    );
    assert_eq!(broker.len(), 0);
}

/// A dispatched handler observes, through its `JobContext`, the lane the job was
/// reserved from.
#[tokio::test]
async fn handler_observes_its_lane() {
    let broker = Arc::new(InMemoryBroker::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone()).with_lane("critical".parse().unwrap());
    worker
        .register(RecordLaneJob { seen: seen.clone() })
        .unwrap();

    client
        .enqueue_to::<RecordLaneJob>("critical".parse().unwrap(), Unit)
        .await
        .unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        ["critical"],
        "the handler context should carry the lane the job ran on"
    );
}
