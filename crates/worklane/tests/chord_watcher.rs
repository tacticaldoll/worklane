use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use worklane::{
    Broker, ChordResults, ChordWatcherJob, ChordWatcherPayload, Client, Job, JobContext,
};
use worklane_core::{JobId, Lane, NewJob, ResultStore};
use worklane_memory::InMemoryBroker;

struct DummyResultStore {
    data: Mutex<HashMap<JobId, Vec<u8>>>,
}

impl DummyResultStore {
    fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }
}

#[worklane::async_trait]
impl ResultStore for DummyResultStore {
    async fn store(&self, job_id: &JobId, result: &[u8]) -> worklane_core::Result<()> {
        self.data.lock().unwrap().insert(*job_id, result.to_vec());
        Ok(())
    }

    async fn get(&self, job_id: &JobId) -> worklane_core::Result<Option<Vec<u8>>> {
        Ok(self.data.lock().unwrap().get(job_id).cloned())
    }
}

impl DummyResultStore {
    /// Simulate a TTL eviction of an already-stored result.
    fn evict(&self, job_id: &JobId) {
        self.data.lock().unwrap().remove(job_id);
    }
}

async fn enqueue_live_dependency(broker: &InMemoryBroker, lane: &Lane) -> JobId {
    broker
        .enqueue(NewJob::new(lane.clone(), "dep", b"null".to_vec(), 1))
        .await
        .unwrap()
}

async fn complete_dependency(
    broker: &InMemoryBroker,
    result_store: &DummyResultStore,
    lane: &Lane,
    result: &[u8],
) -> JobId {
    let dep_id = enqueue_live_dependency(broker, lane).await;
    complete_existing_dependency(broker, result_store, lane, dep_id, result).await;
    dep_id
}

async fn complete_existing_dependency(
    broker: &InMemoryBroker,
    result_store: &DummyResultStore,
    lane: &Lane,
    dep_id: JobId,
    result: &[u8],
) {
    let reservation = broker
        .reserve(lane)
        .await
        .unwrap()
        .expect("dependency reservable");
    assert_eq!(reservation.envelope.id, dep_id);
    result_store.store(&dep_id, result).await.unwrap();
    broker.ack(reservation.receipt).await.unwrap();
}

#[derive(Serialize, Deserialize)]
struct CallbackJob {
    aggregated: bool,
}

#[worklane::async_trait]
impl Job for CallbackJob {
    type Payload = Self;
    type Output = ();
    const KIND: &'static str = "callback_job";

    async fn run(
        &self,
        _ctx: JobContext,
        _payload: Self::Payload,
    ) -> worklane_core::HandlerResult<Self::Output> {
        Ok(())
    }
}

#[tokio::test]
async fn test_chord_watcher_reschedule() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Arc::new(Client::new(broker.clone()));
    let result_store = Arc::new(DummyResultStore::new());

    // A still-running dependency is a *live* job in the broker (no result yet,
    // not dead-lettered). Enqueue it so `is_live` reports it pending.
    let dep_lane: Lane = "deps".parse().unwrap();
    let dep_id = enqueue_live_dependency(&broker, &dep_lane).await;

    let payload = ChordWatcherPayload::new(
        "test-chord".to_string(),
        vec![dep_id],
        "default".to_string(),
        CallbackJob::KIND.to_string(),
        worklane_core::to_payload(&CallbackJob { aggregated: true }).unwrap(),
        1,  // callback_max_attempts
        0,  // callback_priority
        0,  // poll_delay_secs: 0 so it's immediately reservable in the test
        10, // max_generations
    )
    .unwrap();

    let watcher = ChordWatcherJob {
        client: client.clone(),
        result_store: result_store.clone(),
    };

    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        1,
        0,
        "test".to_string(),
        None,
    );

    // Dependency has no result yet but is live. Run watcher.
    watcher.run(ctx.clone(), payload).await.unwrap();

    // It should have enqueued the next generation of the watcher
    let lane = "default".parse().unwrap();
    let reserved = broker
        .reserve(&lane)
        .await
        .unwrap()
        .expect("watcher should be rescheduled");
    assert_eq!(reserved.envelope.kind, ChordWatcherJob::KIND);

    // The unique key should be `cw:test-chord:2`.
    // Verify it by trying to enqueue another job with the same key.
    let dup_id = client
        .enqueue_unique::<CallbackJob>("cw:test-chord:2", CallbackJob { aggregated: false })
        .await
        .unwrap();
    assert_eq!(
        dup_id, reserved.envelope.id,
        "watcher did not hold the correct unique key"
    );
}

#[tokio::test]
async fn test_chord_watcher_success() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Arc::new(Client::new(broker.clone()));
    let result_store = Arc::new(DummyResultStore::new());

    let dep_lane: Lane = "deps".parse().unwrap();
    let dep_id = complete_dependency(&broker, &result_store, &dep_lane, &[1, 2, 3]).await;

    let payload = ChordWatcherPayload::new(
        "test-chord-success".to_string(),
        vec![dep_id],
        "default".to_string(),
        CallbackJob::KIND.to_string(),
        worklane_core::to_payload(&CallbackJob { aggregated: true }).unwrap(),
        1,  // callback_max_attempts
        0,  // callback_priority
        10, // poll_delay_secs
        10, // max_generations
    )
    .unwrap();

    let watcher = ChordWatcherJob {
        client: client.clone(),
        result_store: result_store.clone(),
    };

    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        1,
        0,
        "test".to_string(),
        None,
    );

    // All dependencies are present. Run watcher.
    watcher.run(ctx.clone(), payload).await.unwrap();

    // It should have enqueued the callback job
    let lane = "default".parse().unwrap();
    let reserved = broker
        .reserve(&lane)
        .await
        .unwrap()
        .expect("callback should be enqueued");
    assert_eq!(reserved.envelope.kind, CallbackJob::KIND);

    // The callback receives the aggregated results: the caller context plus each
    // dependency's output bytes, in dependency order.
    let delivered: ChordResults<CallbackJob> =
        worklane_core::from_payload(&reserved.envelope.payload).unwrap();
    assert!(
        delivered.context.aggregated,
        "the callback receives the caller context"
    );
    assert_eq!(
        delivered.results,
        vec![vec![1, 2, 3]],
        "the callback receives each dependency's output bytes"
    );

    // The unique key for the callback should be `chord:{chord_id}:callback`.
    // Verify it by trying to enqueue another job with the same key.
    let dup_id = client
        .enqueue_unique::<CallbackJob>(
            "chord:test-chord-success:callback",
            CallbackJob { aggregated: false },
        )
        .await
        .unwrap();
    assert_eq!(
        dup_id, reserved.envelope.id,
        "callback did not hold the correct unique key"
    );
}

/// A dependency observed complete in an early generation, then evicted from the
/// result store before a slower sibling completes, must NOT regress the chord:
/// completion detection is monotonic, so the chord still fires its callback.
#[tokio::test]
async fn test_chord_watcher_robust_to_result_eviction() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Arc::new(Client::new(broker.clone()));
    let result_store = Arc::new(DummyResultStore::new());

    let dep_lane: Lane = "deps".parse().unwrap();
    let dep_fast = complete_dependency(&broker, &result_store, &dep_lane, &[1]).await;
    // The slow dependency is still running in generation 1: a live job with no
    // result yet, so `is_live` reports it pending.
    let dep_slow = enqueue_live_dependency(&broker, &dep_lane).await;

    let payload = ChordWatcherPayload::new(
        "evict-chord".to_string(),
        vec![dep_fast, dep_slow],
        "default".to_string(),
        CallbackJob::KIND.to_string(),
        worklane_core::to_payload(&CallbackJob { aggregated: true }).unwrap(),
        1,  // callback_max_attempts
        0,  // callback_priority
        0,  // poll_delay_secs
        10, // max_generations
    )
    .unwrap();

    let watcher = ChordWatcherJob {
        client: client.clone(),
        result_store: result_store.clone(),
    };
    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        1,
        0,
        "test".to_string(),
        None,
    );
    let lane = "default".parse().unwrap();

    // Gen 1 captures dep_fast's value, dep_slow is still live → reschedules gen 2
    // carrying the captured value forward.
    watcher.run(ctx.clone(), payload).await.unwrap();
    let gen2 = broker
        .reserve(&lane)
        .await
        .unwrap()
        .expect("gen 2 watcher should be rescheduled");
    assert_eq!(gen2.envelope.kind, ChordWatcherJob::KIND);
    let gen2_payload: ChordWatcherPayload =
        worklane_core::from_payload(&gen2.envelope.payload).unwrap();
    assert_eq!(
        gen2_payload.dependencies(),
        vec![dep_fast, dep_slow],
        "the full dependency list is retained across generations"
    );
    assert_eq!(
        gen2_payload.collected(),
        vec![(dep_fast, vec![1])],
        "gen 2 carries the captured fast-dependency value forward"
    );

    // Now the fast dependency's result is evicted (TTL), and the slow one finishes.
    result_store.evict(&dep_fast);
    complete_existing_dependency(&broker, &result_store, &dep_lane, dep_slow, &[2]).await;

    // Gen 2 must NOT re-require the evicted dep_fast; it sees dep_slow present
    // and fires the callback.
    watcher.run(ctx.clone(), gen2_payload).await.unwrap();
    let reserved = broker
        .reserve(&lane)
        .await
        .unwrap()
        .expect("callback should be enqueued despite the eviction");
    assert_eq!(
        reserved.envelope.kind,
        CallbackJob::KIND,
        "the chord completed instead of regressing on the evicted result"
    );
    let dup_id = client
        .enqueue_unique::<CallbackJob>(
            "chord:evict-chord:callback",
            CallbackJob { aggregated: false },
        )
        .await
        .unwrap();
    assert_eq!(
        dup_id, reserved.envelope.id,
        "callback holds the stable chord callback key"
    );
}

#[tokio::test]
async fn test_chord_watcher_ignores_result_bytes_for_live_dependency() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Arc::new(Client::new(broker.clone()));
    let result_store = Arc::new(DummyResultStore::new());
    let dep_lane: Lane = "deps".parse().unwrap();
    let dep_id = enqueue_live_dependency(&broker, &dep_lane).await;
    result_store.store(&dep_id, &[9, 9, 9]).await.unwrap();

    let payload = ChordWatcherPayload::new(
        "ghost-chord".to_string(),
        vec![dep_id],
        "default".to_string(),
        CallbackJob::KIND.to_string(),
        worklane_core::to_payload(&CallbackJob { aggregated: true }).unwrap(),
        1,
        0,
        0,
        10,
    )
    .unwrap();
    let watcher = ChordWatcherJob {
        client: client.clone(),
        result_store: result_store.clone(),
    };
    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        1,
        0,
        "test".to_string(),
        None,
    );

    watcher.run(ctx, payload).await.unwrap();
    let reserved = broker
        .reserve(&"default".parse().unwrap())
        .await
        .unwrap()
        .expect("live dependency should reschedule watcher, not callback");
    assert_eq!(reserved.envelope.kind, ChordWatcherJob::KIND);
}

#[tokio::test]
async fn test_chord_watcher_rejects_malformed_payloads() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Arc::new(Client::new(broker.clone()));
    let result_store = Arc::new(DummyResultStore::new());
    let dep_lane: Lane = "deps".parse().unwrap();
    let dep_id = complete_dependency(&broker, &result_store, &dep_lane, &[1]).await;
    let unknown_id = JobId::new();

    let valid = ChordWatcherPayload::new(
        "bad-chord".to_string(),
        vec![dep_id],
        "default".to_string(),
        CallbackJob::KIND.to_string(),
        worklane_core::to_payload(&CallbackJob { aggregated: true }).unwrap(),
        1,
        0,
        0,
        10,
    )
    .unwrap();
    let watcher = ChordWatcherJob {
        client: client.clone(),
        result_store: result_store.clone(),
    };
    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        1,
        0,
        "test".to_string(),
        None,
    );

    let mut no_deps = serde_json::to_value(&valid).unwrap();
    no_deps["dependencies"] = serde_json::json!([]);
    let payload: ChordWatcherPayload = serde_json::from_value(no_deps).unwrap();
    let err = watcher.run(ctx.clone(), payload).await.unwrap_err();
    assert!(format!("{err}").contains("no dependencies"));

    let mut duplicate_deps = serde_json::to_value(&valid).unwrap();
    duplicate_deps["dependencies"] = serde_json::json!([dep_id, dep_id]);
    let payload: ChordWatcherPayload = serde_json::from_value(duplicate_deps).unwrap();
    let err = watcher.run(ctx.clone(), payload).await.unwrap_err();
    assert!(format!("{err}").contains("duplicate dependency"));

    let mut unknown_capture = serde_json::to_value(&valid).unwrap();
    unknown_capture["collected"] = serde_json::json!([[unknown_id, [7, 7]]]);
    let payload: ChordWatcherPayload = serde_json::from_value(unknown_capture).unwrap();
    let err = watcher.run(ctx, payload).await.unwrap_err();
    assert!(format!("{err}").contains("captured unknown dependency"));
}

/// A dependency that has been dead-lettered (exhausted its retries, so it never
/// stores a result) must fail the chord fast — naming the dependency — instead of
/// polling until `max_generations` is exhausted.
#[tokio::test]
async fn test_chord_watcher_fails_fast_on_dead_lettered_dependency() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Arc::new(Client::new(broker.clone()));
    let result_store = Arc::new(DummyResultStore::new());

    // Make a genuinely dead-lettered job: enqueue → reserve → fail. Its id is the
    // chord dependency, and it will never store a result.
    let dep_lane: worklane_core::Lane = "deps".parse().unwrap();
    let dep_id = broker
        .enqueue(worklane_core::NewJob::new(
            dep_lane.clone(),
            "dep",
            b"null".to_vec(),
            1,
        ))
        .await
        .unwrap();
    let r = broker
        .reserve(&dep_lane)
        .await
        .unwrap()
        .expect("dependency reservable");
    broker.fail(r.receipt, "boom".to_string()).await.unwrap();
    assert_eq!(
        broker.classify(dep_id).await.unwrap(),
        worklane_core::JobState::DeadLettered
    );

    let payload = ChordWatcherPayload::new(
        "dl-chord".to_string(),
        vec![dep_id],
        "default".to_string(),
        CallbackJob::KIND.to_string(),
        worklane_core::to_payload(&CallbackJob { aggregated: true }).unwrap(),
        1, // callback_max_attempts
        0, // callback_priority
        0, // poll_delay_secs
        // Large bound: a fail-fast must not depend on exhausting generations.
        10_000, // max_generations
    )
    .unwrap();

    let watcher = ChordWatcherJob {
        client: client.clone(),
        result_store: result_store.clone(),
    };
    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        1,
        0,
        "test".to_string(),
        None,
    );

    let err = watcher
        .run(ctx, payload)
        .await
        .expect_err("a dead-lettered dependency must fail the chord");
    let msg = format!("{err}");
    assert!(
        msg.contains("dead-lettered") && msg.contains(&dep_id.to_string()),
        "the error must name the dead-lettered dependency, got: {msg}"
    );

    // No callback and no next-generation watcher were enqueued on the callback lane.
    let lane = "default".parse().unwrap();
    assert!(
        broker.reserve(&lane).await.unwrap().is_none(),
        "a failed chord must not enqueue a callback or another watcher"
    );
}

/// A dependency that *succeeded* but whose result was evicted before the watcher
/// ever captured it must FAIL the chord: aggregation needs the value, so a
/// missing result cannot be aggregated. The broker reports it
/// `CompletedOrUnknown` (acked, no result), and the watcher fails naming the
/// dependency.
#[tokio::test]
async fn test_chord_watcher_fails_on_result_evicted_before_capture() {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Arc::new(Client::new(broker.clone()));
    let result_store = Arc::new(DummyResultStore::new());

    // Make a genuinely completed dependency: enqueue → reserve → ack. It stores
    // no result here (simulating a result already evicted before the first poll),
    // so the watcher sees: no result, not live (acked), not dead-lettered.
    let dep_lane: worklane_core::Lane = "deps".parse().unwrap();
    let dep_id = broker
        .enqueue(worklane_core::NewJob::new(
            dep_lane.clone(),
            "dep",
            b"null".to_vec(),
            1,
        ))
        .await
        .unwrap();
    let r = broker
        .reserve(&dep_lane)
        .await
        .unwrap()
        .expect("dependency reservable");
    broker.ack(r.receipt).await.unwrap();
    assert_eq!(
        broker.classify(dep_id).await.unwrap(),
        worklane_core::JobState::CompletedOrUnknown
    );

    let payload = ChordWatcherPayload::new(
        "evicted-chord".to_string(),
        vec![dep_id],
        "default".to_string(),
        CallbackJob::KIND.to_string(),
        worklane_core::to_payload(&CallbackJob { aggregated: true }).unwrap(),
        1, // callback_max_attempts
        0, // callback_priority
        0, // poll_delay_secs
        // A large bound: completion must not depend on polling generations.
        10_000, // max_generations
    )
    .unwrap();

    let watcher = ChordWatcherJob {
        client: client.clone(),
        result_store: result_store.clone(),
    };
    let ctx = JobContext::new(
        JobId::new(),
        "default".parse().unwrap(),
        1,
        1,
        0,
        "test".to_string(),
        None,
    );

    let err = watcher
        .run(ctx, payload)
        .await
        .expect_err("an evicted-before-capture result must fail the chord");
    let msg = format!("{err}");
    assert!(
        msg.contains(&dep_id.to_string()) && (msg.contains("evicted") || msg.contains("aggregate")),
        "the error must name the dependency and explain the missing result, got: {msg}"
    );

    // No callback (and no next-generation watcher) was enqueued.
    let lane = "default".parse().unwrap();
    assert!(
        broker.reserve(&lane).await.unwrap().is_none(),
        "a chord that cannot aggregate must not enqueue a callback"
    );
}
