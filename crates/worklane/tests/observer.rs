//! The worker reports each job's outcome to its `JobObserver`: `Acked` on
//! success, `Retried` while attempts remain, `DeadLettered` when they are
//! exhausted — with the job's lane and kind.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use worklane::{
    Client, HandlerError, HandlerResult, Job, JobContext, JobEvent, JobObserver, JobOutcome,
    Worker, async_trait,
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
    async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
        Ok(())
    }
}

struct FailJob;
#[async_trait]
impl Job for FailJob {
    type Payload = Unit;
    type Output = ();
    const KIND: &'static str = "fail";
    async fn run(&self, _ctx: JobContext, _p: Unit) -> HandlerResult<()> {
        Err(HandlerError::from("boom"))
    }
}

#[derive(Default)]
struct CapturingObserver {
    events: Mutex<Vec<(String, String, JobOutcome)>>,
}

impl JobObserver for CapturingObserver {
    fn on_job_finished(&self, event: JobEvent<'_>) {
        self.events.lock().unwrap().push((
            event.lane.to_string(),
            event.kind.to_string(),
            event.outcome,
        ));
    }
}

#[tokio::test]
async fn reports_acked_on_success() {
    let broker = Arc::new(InMemoryBroker::new());
    let obs = Arc::new(CapturingObserver::default());
    let client = Client::new(broker.clone());

    client.enqueue::<OkJob>(Unit).await.unwrap();

    let mut worker = Worker::new(broker.clone()).with_observer(obs.clone());
    worker.register(OkJob).unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    let events = obs.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0],
        ("default".into(), "ok".into(), JobOutcome::Acked)
    );
}

#[tokio::test]
async fn reports_dead_lettered_when_attempts_exhausted() {
    let broker = Arc::new(InMemoryBroker::new());
    let obs = Arc::new(CapturingObserver::default());
    // One attempt only: the single failure exhausts attempts and dead-letters.
    let client = Client::new(broker.clone()).with_max_attempts(1);

    client.enqueue::<FailJob>(Unit).await.unwrap();

    let mut worker = Worker::new(broker.clone()).with_observer(obs.clone());
    worker.register(FailJob).unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    let events = obs.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].2, JobOutcome::DeadLettered);
}

#[tokio::test]
async fn reports_retried_while_attempts_remain() {
    let broker = Arc::new(InMemoryBroker::new());
    let obs = Arc::new(CapturingObserver::default());
    let client = Client::new(broker.clone()).with_max_attempts(3);

    client.enqueue::<FailJob>(Unit).await.unwrap();

    // Default retry policy delays the retry by ~1s, so `run_until_idle` resolves
    // the first attempt (a retry) and then stops — exactly one `Retried` event.
    let mut worker = Worker::new(broker.clone()).with_observer(obs.clone());
    worker.register(FailJob).unwrap();
    let worker = worker.build().unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), worker.run_until_idle()).await;

    let events = obs.events.lock().unwrap();
    assert_eq!(
        events.len(),
        1,
        "one attempt resolved before the delayed retry"
    );
    assert_eq!(events[0].2, JobOutcome::Retried);
}
