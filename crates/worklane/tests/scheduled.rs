//! `Client::enqueue_in` delays a job's visibility; `enqueue` is immediate. The
//! advance-and-become-visible timing is covered by the broker conformance suite's
//! timed tier; here we assert the client-facing behaviour over an in-memory
//! broker.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_core::Broker;
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

/// A job enqueued with a delay is not reservable yet, while an immediate enqueue
/// is.
#[tokio::test]
async fn enqueue_in_delays_visibility() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    // A long delay: the job exists but must not be visible now.
    client
        .enqueue_in::<OkJob>(Duration::from_secs(3600), Unit)
        .await
        .unwrap();
    assert!(
        broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .is_none(),
        "a delayed job must not be reservable before its delay elapses"
    );
    assert_eq!(broker.len(), 1, "the delayed job is stored, just hidden");

    // An immediate enqueue is reservable right away.
    client.enqueue::<OkJob>(Unit).await.unwrap();
    assert!(
        broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .is_some(),
        "a plain enqueue must be immediately reservable"
    );
}

/// A worker runs a delayed job only after its delay; `run_until_idle` before then
/// leaves it untouched.
#[tokio::test]
async fn delayed_job_not_run_until_due() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone());
    worker.register(OkJob).unwrap();

    client
        .enqueue_in::<OkJob>(Duration::from_secs(3600), Unit)
        .await
        .unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();
    assert_eq!(
        broker.len(),
        1,
        "a not-yet-due delayed job must remain unprocessed"
    );
}
