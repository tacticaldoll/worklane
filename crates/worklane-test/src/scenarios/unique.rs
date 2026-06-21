use super::{job, lane};
use crate::BrokerContractHarness;
use worklane_core::Broker;

/// A unique key held by a live job deduplicates a second enqueue to the existing
/// job: same id, one live job.
pub async fn unique_enqueue_dedups_held_key<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let id1 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .unwrap();
    let id2 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .unwrap();
    assert_eq!(id1, id2, "a held key dedups to the existing job id");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the one live job is reservable"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "only one job exists for the key"
    );
}

/// Many concurrent *first-time* enqueues of the same unique key converge: every
/// call succeeds and returns one shared id, and exactly one live job exists.
/// This exercises the check-then-insert window a sequential dedup test cannot —
/// the key's uniqueness constraint, not the pre-read, must be the arbiter, and
/// the racers that lose must dedup rather than surface a duplicate-key error.
pub async fn concurrent_unique_enqueue_dedups<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let (a, c, d, e) = tokio::join!(
        b.enqueue(job("default").with_unique_key("k")),
        b.enqueue(job("default").with_unique_key("k")),
        b.enqueue(job("default").with_unique_key("k")),
        b.enqueue(job("default").with_unique_key("k")),
    );
    let ids: Vec<_> = [a, c, d, e]
        .into_iter()
        .map(|r| r.expect("a concurrent same-key enqueue must dedup, not error"))
        .collect();
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(
        unique.len(),
        1,
        "all concurrent same-key enqueues must dedup to one id, got {ids:?}"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the one deduped job is reservable"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "the race must not create a second job for the key"
    );
}

/// Acking a unique job releases its key; a later enqueue with that key is a new
/// job.
pub async fn unique_key_released_after_ack<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let id1 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    b.ack(r.receipt).await.unwrap();
    let id2 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .unwrap();
    assert_ne!(id1, id2, "after ack the key is free; a new job is created");
}

/// Failing a unique job releases its key; a later enqueue with that key is a new
/// job.
pub async fn unique_key_released_after_fail<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let id1 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    b.fail(r.receipt, "boom".to_string()).await.unwrap();
    let id2 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .unwrap();
    assert_ne!(id1, id2, "after fail the key is free; a new job is created");
}

/// Different unique keys are independent — both jobs are created.
pub async fn distinct_unique_keys_not_deduped<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let a = b
        .enqueue(job("default").with_unique_key("a"))
        .await
        .unwrap();
    let c = b
        .enqueue(job("default").with_unique_key("b"))
        .await
        .unwrap();
    assert_ne!(a, c, "distinct keys must not be deduplicated");
}

/// A `unique_key` is opaque application data: any characters are accepted and
/// still dedup correctly. The framework itself generates keys bearing `:` (chord
/// and chain idempotency keys, scheduled-fire keys), so a backend must not reject
/// or mangle them — this guards the redis key-scheme regression where `:` and
/// glob characters were wrongly rejected.
pub async fn unique_key_accepts_arbitrary_characters<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let key = "chord:abc-*?[]:42";
    let id1 = b
        .enqueue(job("default").with_unique_key(key))
        .await
        .unwrap();
    let id2 = b
        .enqueue(job("default").with_unique_key(key))
        .await
        .unwrap();
    assert_eq!(
        id1, id2,
        "an opaque key with `:`/glob chars must still dedup"
    );
    let other = b
        .enqueue(job("default").with_unique_key("chord:abc-*?[]:43"))
        .await
        .unwrap();
    assert_ne!(id1, other, "a distinct opaque key must not be deduplicated");
}

/// Jobs without a unique key are never deduplicated.
pub async fn no_unique_key_no_dedup<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let a = b.enqueue(job("default")).await.unwrap();
    let c = b.enqueue(job("default")).await.unwrap();
    assert_ne!(a, c, "keyless enqueues must each create a distinct job");
}
