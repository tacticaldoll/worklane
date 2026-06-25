use super::{job, lane};
use crate::BrokerContractHarness;
use worklane_core::{BatchEnqueue, Broker};

/// Obtain the broker's [`BatchEnqueue`] capability for the batch suite.
///
/// These scenarios are the batch-enqueue capability battery: they run only for a
/// broker that provides the capability. Until the harness gates batteries by
/// capability presence (modular-conformance work), this asserts the broker under
/// test implements it.
fn batch_cap<B: Broker>(b: &B) -> &dyn BatchEnqueue {
    b.batch_enqueue()
        .expect("broker under the batch-enqueue suite must implement BatchEnqueue")
}

/// A successfully enqueued batch makes all jobs immediately visible.
pub async fn batch_all_visible<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let jobs = vec![job("default"), job("default"), job("default")];
    batch_cap(&*b).enqueue_batch(jobs).await.unwrap();

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
    let ids = batch_cap(&*b).enqueue_batch(jobs).await.unwrap();
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
    let ids = batch_cap(&*b).enqueue_batch(jobs).await.unwrap();
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
    let ids = batch_cap(&*b).enqueue_batch(vec![]).await.unwrap();
    assert!(ids.is_empty(), "empty batch returns empty ids");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "no jobs created"
    );
}

/// A batch mixing unique-key and plain jobs obeys the same contract as any
/// batch: the unique-key jobs dedup, every plain job is stored, and input order
/// is preserved. This guards a broker's batch fast/slow-path gate — a broker
/// that routes a *whole* batch to a no-dedup multi-row fast path must do so only
/// when no job carries a unique key. A gate that mis-fires on a mixed batch
/// would store the duplicate unique-key job instead of deduping it, which this
/// scenario catches.
pub async fn batch_mixed_unique_and_plain<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let mut p1 = job("default");
    p1.payload = b"p1".to_vec();
    let mut k2 = job("default").with_unique_key("k");
    k2.payload = b"k2".to_vec();
    let mut p3 = job("default");
    p3.payload = b"p3".to_vec();
    let mut k4 = job("default").with_unique_key("k");
    k4.payload = b"k4".to_vec();

    let ids = batch_cap(&*b)
        .enqueue_batch(vec![p1, k2, p3, k4])
        .await
        .unwrap();
    assert_eq!(
        ids.len(),
        4,
        "returned ids are 1:1 with the input, in order"
    );
    assert_eq!(ids[1], ids[3], "the two unique-key jobs dedup to one id");
    assert_ne!(ids[0], ids[1], "plain and unique-key jobs are distinct");
    assert_ne!(ids[0], ids[2], "the two plain jobs are distinct");

    let mut found = vec![];
    while let Some(r) = b.reserve(&lane("default")).await.unwrap() {
        found.push(r.envelope.id);
    }
    // Three live jobs — p1, the single deduped key job, p3 — the second key job
    // is a duplicate and is not stored.
    assert_eq!(found.len(), 3, "the duplicate unique-key job is not stored");
    assert_eq!(
        found,
        vec![ids[0], ids[1], ids[2]],
        "stored jobs reserve in input order (plain, deduped key, plain)"
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
        let bq = batch_cap(&*b);
        let (r1, r2) = tokio::join!(bq.enqueue_batch(batch1), bq.enqueue_batch(batch2));
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
