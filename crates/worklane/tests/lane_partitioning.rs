//! Lane partitioning scenarios (see `specs/broker` and `specs/client` of the
//! add-lane-partitioning change).

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use worklane::{Broker, Client, HandlerResult, Job, JobContext, NewJob, Worker, async_trait};
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

/// A worker on one lane must not reserve another lane's job; that job stays
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

/// Two lanes, interleaved: each `reserve` returns only its own lane's job.
#[tokio::test]
async fn lanes_are_isolated() {
    let broker = Arc::new(InMemoryBroker::new());
    Client::new(broker.clone())
        .with_lane("a")
        .enqueue::<OkJob>(Unit)
        .await
        .unwrap();
    Client::new(broker.clone())
        .with_lane("b")
        .enqueue::<OkJob>(Unit)
        .await
        .unwrap();

    let a = broker.reserve("a").await.unwrap().expect("lane a job");
    assert_eq!(a.envelope.lane, "a");
    let b = broker.reserve("b").await.unwrap().expect("lane b job");
    assert_eq!(b.envelope.lane, "b");

    assert!(
        broker.reserve("a").await.unwrap().is_none(),
        "lane a had only one job"
    );
    assert!(
        broker.reserve("b").await.unwrap().is_none(),
        "lane b had only one job"
    );
}

/// `reserve` on a lane with no jobs returns `None` even while another lane has
/// jobs waiting.
#[tokio::test]
async fn reserve_empty_lane_returns_none() {
    let broker = Arc::new(InMemoryBroker::new());
    Client::new(broker.clone())
        .with_lane("critical")
        .enqueue::<OkJob>(Unit)
        .await
        .unwrap();

    assert!(
        broker.reserve("default").await.unwrap().is_none(),
        "default lane is empty"
    );
    assert_eq!(broker.len(), 1, "critical job untouched");
}

/// A dead-lettered job retains the lane it was enqueued to.
#[tokio::test]
async fn dead_letter_retains_lane() {
    let broker = InMemoryBroker::new();
    broker
        .enqueue(NewJob::new("critical", "ok", b"null".to_vec(), 3))
        .await
        .unwrap();

    let reserved = broker
        .reserve("critical")
        .await
        .unwrap()
        .expect("critical job");
    broker
        .fail(reserved.receipt, "boom".to_string())
        .await
        .unwrap();

    let dead = broker.dead_letters();
    assert_eq!(dead.len(), 1);
    assert_eq!(
        dead[0].envelope.lane, "critical",
        "dead-letter must retain the lane"
    );
}
