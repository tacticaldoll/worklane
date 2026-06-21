use serde::{Deserialize, Serialize};
use std::sync::Arc;
use worklane::{Broker, Canvas, ChordResults, Client, Job, JobContext};
use worklane_core::JobId;
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct DummyJob {
    data: String,
}

#[worklane::async_trait]
impl Job for DummyJob {
    type Payload = Self;
    type Output = ();
    const KIND: &'static str = "dummy";

    async fn run(
        &self,
        _ctx: JobContext,
        _payload: Self::Payload,
    ) -> worklane_core::HandlerResult<Self::Output> {
        Ok(())
    }
}

/// A chord callback context.
#[derive(Serialize, Deserialize)]
struct Ctx {
    tag: String,
}

/// A chord callback: its payload is `ChordResults<Ctx>` (context + dep outputs).
struct AggregateJob;

#[worklane::async_trait]
impl Job for AggregateJob {
    type Payload = ChordResults<Ctx>;
    type Output = ();
    const KIND: &'static str = "aggregate";

    async fn run(
        &self,
        _ctx: JobContext,
        _payload: Self::Payload,
    ) -> worklane_core::HandlerResult<Self::Output> {
        Ok(())
    }
}

#[tokio::test]
async fn test_build_continuation() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        1,
        0,
        "test".to_string(),
        None,
    );

    let payload = DummyJob {
        data: "test".to_string(),
    };

    // Call build_continuation
    let job_id = client
        .build_continuation::<DummyJob>(&ctx, payload)
        .unwrap()
        .enqueue()
        .await
        .unwrap();

    // Verify it was enqueued
    let lane = "default".parse().unwrap();
    let job = broker
        .reserve(&lane)
        .await
        .unwrap()
        .expect("job should be in queue");

    assert_eq!(job.envelope.id, job_id);
    assert_eq!(job.envelope.kind, DummyJob::KIND);

    // To verify the unique key was set correctly to `chain:{ctx.id}:{J::KIND}`,
    // we enqueue a dummy job with that explicit key. If the key is held, it will dedup
    // and return the existing `job_id`.
    let expected_key = format!("chain:{}:{}", ctx.id.clone(), DummyJob::KIND);
    let dup_id = client
        .enqueue_unique::<DummyJob>(
            expected_key,
            DummyJob {
                data: "dup".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        dup_id, job_id,
        "Unique key was not held or derived incorrectly"
    );
}

#[tokio::test]
async fn test_build_continuation_keyed() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    let payload = DummyJob {
        data: "test".to_string(),
    };
    let explicit_key = "chord:my-chord-id:callback".to_string();

    // Call build_continuation_keyed
    let job_id = client
        .build_continuation_keyed::<DummyJob>(explicit_key.clone(), payload)
        .unwrap()
        .enqueue()
        .await
        .unwrap();

    // Verify it was enqueued
    let lane = "default".parse().unwrap();
    let job = broker
        .reserve(&lane)
        .await
        .unwrap()
        .expect("job should be in queue");

    assert_eq!(job.envelope.id, job_id);
    assert_eq!(job.envelope.kind, DummyJob::KIND);

    // Verify the explicit key was held by enqueuing again
    let dup_id = client
        .enqueue_unique::<DummyJob>(
            explicit_key,
            DummyJob {
                data: "dup".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(dup_id, job_id, "Unique key was not held or set incorrectly");
}

/// At-least-once boundary (dedup window): if the parent job is redelivered and
/// continues again while the first continuation is still live (not yet acked),
/// the derived idempotency key dedups — only one continuation job exists.
#[tokio::test]
async fn build_continuation_dedups_while_continuation_is_live() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        3,
        0,
        "parent".to_string(),
        None,
    );

    let first = client
        .build_continuation::<DummyJob>(&ctx, DummyJob { data: "a".into() })
        .unwrap()
        .enqueue()
        .await
        .unwrap();
    // The parent is redelivered (at-least-once) and continues again with the
    // same ctx, while the first continuation is still live.
    let second = client
        .build_continuation::<DummyJob>(&ctx, DummyJob { data: "b".into() })
        .unwrap()
        .enqueue()
        .await
        .unwrap();

    assert_eq!(
        first, second,
        "a redelivered parent must dedup to the single live continuation"
    );

    let lane = "default".parse().unwrap();
    assert!(
        broker.reserve(&lane).await.unwrap().is_some(),
        "the one continuation is reservable"
    );
    assert!(
        broker.reserve(&lane).await.unwrap().is_none(),
        "the redelivery must not create a second continuation"
    );
}

/// At-least-once boundary (key released): once the continuation has completed
/// and been acked, its unique key is freed. A subsequent redelivery of the
/// parent then re-enqueues a fresh continuation — dedup is best-effort within
/// the live window, not exactly-once across the job's whole lifetime.
#[tokio::test]
async fn build_continuation_reruns_after_continuation_completes() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        3,
        0,
        "parent".to_string(),
        None,
    );

    let first = client
        .build_continuation::<DummyJob>(&ctx, DummyJob { data: "a".into() })
        .unwrap()
        .enqueue()
        .await
        .unwrap();

    // The continuation runs to completion: reserve + ack releases its key.
    let lane = "default".parse().unwrap();
    let r = broker
        .reserve(&lane)
        .await
        .unwrap()
        .expect("continuation must be reservable");
    assert_eq!(r.envelope.id, first);
    broker.ack(r.receipt).await.unwrap();

    // The parent is redelivered after the continuation completed. With the key
    // released, a fresh continuation is enqueued.
    let second = client
        .build_continuation::<DummyJob>(&ctx, DummyJob { data: "b".into() })
        .unwrap()
        .enqueue()
        .await
        .unwrap();
    assert_ne!(
        first, second,
        "after the continuation completed and freed its key, a redelivery must re-enqueue"
    );
    assert!(
        broker.reserve(&lane).await.unwrap().is_some(),
        "the re-enqueued continuation is reservable"
    );
}

/// A chord dependency carrying a `unique_key` is rejected before anything is
/// submitted: the atomic batch enqueue could deduplicate the member away, which
/// would leave the watcher with a dependency id that was never persisted (a
/// phantom that later falsely completes the chord.
#[tokio::test]
async fn chord_rejects_dependency_with_unique_key() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    let dep = client
        .build_job::<DummyJob>(DummyJob { data: "a".into() })
        .unwrap()
        .with_unique_key("dup");

    let result = client
        .chord::<AggregateJob, _>("chord-1".to_string(), vec![dep], Ctx { tag: "x".into() })
        .await;
    assert!(
        result.is_err(),
        "a chord dependency carrying a unique_key must be rejected"
    );

    let lane = "default".parse().unwrap();
    assert!(
        broker.reserve(&lane).await.unwrap().is_none(),
        "a rejected chord must submit neither dependencies nor the watcher"
    );
}

/// A chord with plain dependencies submits the members and the watcher in one
/// atomic batch, so each dependency id the watcher carries denotes a persisted
/// job.
#[tokio::test]
async fn chord_submits_dependencies_and_watcher() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    let dep_a = client
        .build_job::<DummyJob>(DummyJob { data: "a".into() })
        .unwrap();
    let dep_b = client
        .build_job::<DummyJob>(DummyJob { data: "b".into() })
        .unwrap();

    client
        .chord::<AggregateJob, _>(
            "chord-2".to_string(),
            vec![dep_a, dep_b],
            Ctx { tag: "x".into() },
        )
        .await
        .expect("a chord with plain dependencies must submit");

    // Two dependencies + one watcher, enqueued atomically on the default lane.
    let lane = "default".parse().unwrap();
    let mut count = 0;
    while broker.reserve(&lane).await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(
        count, 3,
        "the two dependencies and the watcher must all be enqueued"
    );
}
