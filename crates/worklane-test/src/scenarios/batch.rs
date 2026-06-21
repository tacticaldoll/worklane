use super::{job, lane};
use crate::BrokerContractHarness;
use worklane_core::Broker;

/// A successfully enqueued batch makes all jobs immediately visible.
pub async fn batch_all_visible<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let jobs = vec![job("default"), job("default"), job("default")];
    b.enqueue_batch(jobs).await.unwrap();

    let mut reserved = 0;
    while b.reserve(&lane("default")).await.unwrap().is_some() {
        reserved += 1;
    }
    assert_eq!(reserved, 3, "all jobs in the batch must be visible");
}

/// The returned JobId list preserves the order of the input batch.
pub async fn batch_preserves_order<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let mut j1 = job("default");
    j1.payload = b"1".to_vec();
    let mut j2 = job("default");
    j2.payload = b"2".to_vec();
    let mut j3 = job("default");
    j3.payload = b"3".to_vec();

    let jobs = vec![j1, j2, j3];
    let ids = b.enqueue_batch(jobs).await.unwrap();
    assert_eq!(ids.len(), 3);

    let mut found = vec![];
    while let Some(r) = b.reserve(&lane("default")).await.unwrap() {
        found.push((r.envelope.id, r.envelope.payload));
    }
    assert_eq!(found.len(), 3);

    // Each returned id maps to its corresponding payload (id↔payload integrity).
    for (expected_id, expected_payload) in
        ids.iter()
            .copied()
            .zip(vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()])
    {
        let matching = found
            .iter()
            .find(|(id, _)| *id == expected_id)
            .expect("id must exist");
        assert_eq!(
            matching.1, expected_payload,
            "id must map to the corresponding payload"
        );
    }

    // The returned id list is in input order, and (same lane, same visibility)
    // the jobs reserve back in that same FIFO order — pin both, not just the
    // id↔payload mapping.
    let found_ids: Vec<_> = found.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        found_ids, ids,
        "batch jobs must reserve in input order (returned-id order == reservation order)"
    );
    let found_payloads: Vec<_> = found.iter().map(|(_, p)| p.clone()).collect();
    assert_eq!(
        found_payloads,
        vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
        "batch payloads must reserve in input order"
    );
}

/// A batch deduplicates intra-batch collisions.
pub async fn batch_intra_unique_dedup<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let jobs = vec![
        job("default").with_unique_key("k"),
        job("default").with_unique_key("k"),
        job("default").with_unique_key("k"),
    ];
    let ids = b.enqueue_batch(jobs).await.unwrap();
    assert_eq!(ids.len(), 3);
    assert_eq!(ids[0], ids[1], "second job must dedup to the first");
    assert_eq!(ids[0], ids[2], "third job must dedup to the first");

    let r1 = b.reserve(&lane("default")).await.unwrap();
    assert!(r1.is_some(), "the deduped job is reservable");
    let r2 = b.reserve(&lane("default")).await.unwrap();
    assert!(r2.is_none(), "only one job was actually stored");
}

/// An empty batch does nothing and returns an empty list.
pub async fn batch_empty<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let ids = b.enqueue_batch(vec![]).await.unwrap();
    assert!(ids.is_empty(), "empty batch returns empty ids");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "no jobs created"
    );
}

/// Concurrent batches whose unique keys overlap in opposite order must not
/// deadlock: each batch completes and every shared key dedups to one live job.
/// A broker that locks unique keys per row in input order would deadlock (e.g.
/// Postgres aborts one with SQLSTATE 40P01); a correct broker acquires the
/// contended keys in a consistent order, or serializes batch writes entirely.
pub async fn batch_concurrent_overlapping_unique_no_deadlock<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    // Several rounds widen the window for a lock-ordering cycle on a backend
    // that would deadlock. Each round uses distinct keys listed in opposite
    // order by the two concurrent batches.
    const ROUNDS: usize = 8;
    for round in 0..ROUNDS {
        let ka = format!("a-{round}");
        let kb = format!("b-{round}");
        let batch1 = vec![
            job("default").with_unique_key(ka.clone()),
            job("default").with_unique_key(kb.clone()),
        ];
        let batch2 = vec![
            job("default").with_unique_key(kb.clone()),
            job("default").with_unique_key(ka.clone()),
        ];
        let (r1, r2) = tokio::join!(b.enqueue_batch(batch1), b.enqueue_batch(batch2));
        r1.expect("batch [a,b] must not deadlock against [b,a]");
        r2.expect("batch [b,a] must not deadlock against [a,b]");
    }
    // Each round's two keys dedup to one live job each across the two batches.
    let mut reserved = 0;
    while b.reserve(&lane("default")).await.unwrap().is_some() {
        reserved += 1;
    }
    assert_eq!(
        reserved,
        ROUNDS * 2,
        "each shared key must dedup to exactly one live job across the concurrent batches",
    );
}
