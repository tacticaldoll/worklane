//! Integration tests for the worklane core loop, exercising the spec scenarios.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use worklane::{
    Broker, Client, HandlerError, HandlerResult, Job, JobContext, JobId, NewJob, RetryPolicy,
    Worker, async_trait,
};
use worklane_memory::{InMemoryBroker, ManualClock};

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

/// Always fails.
struct FailJob;

#[async_trait]
impl Job for FailJob {
    type Payload = Unit;
    const KIND: &'static str = "fail";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult {
        Err(HandlerError::from("boom"))
    }
}

/// Defined but never registered with the worker, to exercise unknown kinds.
struct UnregisteredJob;

#[async_trait]
impl Job for UnregisteredJob {
    type Payload = Unit;
    const KIND: &'static str = "unregistered";
    async fn run(&self, _ctx: JobContext, _payload: Unit) -> HandlerResult {
        Ok(())
    }
}

/// Succeeds only if the payload round-tripped to the expected value.
#[derive(Serialize, Deserialize)]
struct Number {
    n: u64,
}

struct EchoJob;

#[async_trait]
impl Job for EchoJob {
    type Payload = Number;
    const KIND: &'static str = "echo";
    async fn run(&self, _ctx: JobContext, payload: Number) -> HandlerResult {
        if payload.n == 7 {
            Ok(())
        } else {
            Err(HandlerError::from(format!(
                "unexpected payload: {}",
                payload.n
            )))
        }
    }
}

#[tokio::test]
async fn happy_path_enqueue_reserve_ack() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone());
    worker.register(OkJob).unwrap();

    client.enqueue::<OkJob>(Unit).await.unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(broker.len(), 0, "acked job should be removed");
    assert!(broker.dead_letters().is_empty());
}

#[tokio::test]
async fn duplicate_kind_registration_is_rejected() {
    let broker = Arc::new(InMemoryBroker::new());
    let mut worker = Worker::new(broker.clone());
    worker.register(OkJob).unwrap();
    assert!(
        worker.register(OkJob).is_err(),
        "duplicate kind must be rejected"
    );
}

#[tokio::test]
async fn retry_increments_attempts_and_respects_delay() {
    let clock = Arc::new(ManualClock::new());
    let broker =
        Arc::new(InMemoryBroker::with_clock(clock.clone()).with_lease(Duration::from_secs(60)));
    let retry = RetryPolicy {
        base: Duration::from_secs(1),
        factor: 2,
        cap: Duration::from_secs(60),
    };
    let client = Client::new(broker.clone()).with_max_attempts(3);
    let worker = {
        let mut w = Worker::new(broker.clone()).with_retry_policy(retry);
        w.register(FailJob).unwrap();
        w
    };

    client.enqueue::<FailJob>(Unit).await.unwrap();

    // Attempt 1 -> retry after base (1s).
    assert!(worker.process_next().await.unwrap());
    assert_eq!(broker.len(), 1, "still live, scheduled for retry");
    assert!(
        !worker.process_next().await.unwrap(),
        "not visible until delay elapses"
    );

    clock.advance(Duration::from_secs(1));
    // Attempt 2 -> retry after base*2 (2s).
    assert!(worker.process_next().await.unwrap());
    assert!(!worker.process_next().await.unwrap());

    clock.advance(Duration::from_secs(2));
    // Attempt 3 -> attempts exhausted -> dead-letter.
    assert!(worker.process_next().await.unwrap());

    assert_eq!(broker.len(), 0);
    let dead = broker.dead_letters();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].envelope.attempts, 2, "incremented once per retry");
    assert!(dead[0].error.contains("boom"));
}

#[tokio::test]
async fn unknown_kind_dead_letters_and_loop_continues() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone());
    worker.register(OkJob).unwrap();

    // Unknown kind first, then a known job; the loop must process both.
    client.enqueue::<UnregisteredJob>(Unit).await.unwrap();
    client.enqueue::<OkJob>(Unit).await.unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(broker.len(), 0);
    let dead = broker.dead_letters();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].envelope.kind, "unregistered");
    assert!(dead[0].error.contains("unknown job kind"));
}

#[tokio::test]
async fn payload_round_trips() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let mut worker = Worker::new(broker.clone());
    worker.register(EchoJob).unwrap();

    client.enqueue::<EchoJob>(Number { n: 7 }).await.unwrap();
    worker.run_until_idle().await.unwrap();

    // Handler returned Ok only because it received n == 7.
    assert_eq!(broker.len(), 0);
    assert!(broker.dead_letters().is_empty());
}

#[tokio::test]
async fn corrupt_payload_dead_letters_without_panic() {
    let broker = Arc::new(InMemoryBroker::new());
    let mut worker = Worker::new(broker.clone());
    worker.register(EchoJob).unwrap();

    // Enqueue raw invalid bytes for the echo kind, bypassing the typed client.
    broker
        .enqueue(NewJob {
            kind: "echo".to_string(),
            payload: b"not json".to_vec(),
            max_attempts: 3,
        })
        .await
        .unwrap();

    worker.run_until_idle().await.unwrap();

    assert_eq!(broker.len(), 0);
    let dead = broker.dead_letters();
    assert_eq!(dead.len(), 1);
    assert!(dead[0].error.contains("serialization"));
}

#[tokio::test]
async fn lease_expiry_requeues() {
    let clock = Arc::new(ManualClock::new());
    let broker = InMemoryBroker::with_clock(clock.clone()).with_lease(Duration::from_secs(10));

    let id = broker
        .enqueue(NewJob {
            kind: "ok".to_string(),
            payload: b"null".to_vec(),
            max_attempts: 3,
        })
        .await
        .unwrap();

    let first = broker.reserve("default").await.unwrap().expect("a job");
    assert_eq!(first.id, id);
    assert!(
        broker.reserve("default").await.unwrap().is_none(),
        "leased, not reservable"
    );

    clock.advance(Duration::from_secs(11));
    let again = broker
        .reserve("default")
        .await
        .unwrap()
        .expect("requeued after lease");
    assert_eq!(again.id, id);
}

#[tokio::test]
async fn ack_unknown_job_is_an_error() {
    let broker = InMemoryBroker::new();
    assert!(
        broker.ack(JobId::new()).await.is_err(),
        "acking an unknown job must error, not silently succeed"
    );
}
