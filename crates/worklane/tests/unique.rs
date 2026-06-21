//! `Client::enqueue_unique` deduplicates by key while a live job holds it. The
//! full key lifecycle is covered by the broker conformance suite; here we assert
//! the client-facing behaviour over an in-memory broker.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use worklane::{Client, HandlerResult, Job, JobContext, async_trait};
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

/// Two `enqueue_unique` calls with the same key return the same id and leave one
/// live job.
#[tokio::test]
async fn enqueue_unique_dedups() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    let id1 = client.enqueue_unique::<OkJob>("k", Unit).await.unwrap();
    let id2 = client.enqueue_unique::<OkJob>("k", Unit).await.unwrap();
    assert_eq!(id1, id2, "same key dedups to the same job id");
    assert_eq!(broker.len(), 1, "only one live job exists for the key");

    // After that job is acked, the key is free again — a new job is created.
    let r = broker
        .reserve(&"default".parse().unwrap())
        .await
        .unwrap()
        .expect("a job");
    broker.ack(r.receipt).await.unwrap();
    let id3 = client.enqueue_unique::<OkJob>("k", Unit).await.unwrap();
    assert_ne!(id1, id3, "after ack the key is free; a new job is created");
}
