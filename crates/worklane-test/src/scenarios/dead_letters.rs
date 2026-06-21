use super::{dead_letter, job, lane};
use crate::BrokerContractHarness;
use worklane_core::{DeadLetterStore, NewJob};

/// A read returns a failed job's envelope and error, and is non-destructive: a
/// second read still returns it.
pub async fn read_returns_failed_job<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    dead_letter(b.as_ref(), job("critical"), "boom").await;
    let dead = b.read_dead_letters(&lane("critical"), 10).await.unwrap();
    assert_eq!(dead.len(), 1, "the failed job should be readable");
    assert_eq!(dead[0].error, "boom", "the read retains the error");
    assert_eq!(
        dead[0].envelope.lane, "critical",
        "the read retains the lane"
    );
    let again = b.read_dead_letters(&lane("critical"), 10).await.unwrap();
    assert_eq!(again.len(), 1, "the read must be non-destructive");
}

/// A read returns at most `limit` records when more jobs are dead-lettered.
pub async fn read_bounded_by_limit<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    for _ in 0..3 {
        dead_letter(b.as_ref(), job("default"), "boom").await;
    }
    let dead = b.read_dead_letters(&lane("default"), 2).await.unwrap();
    // With 3 records present and a limit of 2, a correct backend truncates to
    // exactly the limit. Asserting `== 2` (not just `<= 2`) rejects a backend
    // that returns zero — which would satisfy a `<= limit` check vacuously.
    assert_eq!(
        dead.len(),
        2,
        "a read must return exactly `limit` records when more exist, got {}",
        dead.len()
    );
}

/// A read is lane-scoped: dead letters on one lane are invisible to a read for
/// another lane.
pub async fn read_is_lane_scoped<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    dead_letter(b.as_ref(), job("a"), "boom").await;
    let other = b.read_dead_letters(&lane("b"), 10).await.unwrap();
    assert!(
        other.is_empty(),
        "a read for a different lane must not return the record"
    );
}

/// A read preserves the opaque envelope verbatim, including non-UTF-8 payloads.
pub async fn read_preserves_opaque_envelope<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let payload = vec![0u8, 159, 146, 150, 255, 0, 1, 2, 254];
    dead_letter(
        b.as_ref(),
        NewJob::new(lane("critical"), "send_email", payload.clone(), 7),
        "boom",
    )
    .await;
    let dead = b.read_dead_letters(&lane("critical"), 10).await.unwrap();
    assert_eq!(dead.len(), 1, "the failed job should be readable");
    assert_eq!(dead[0].envelope.kind, "send_email", "kind preserved");
    assert_eq!(
        dead[0].envelope.payload, payload,
        "payload bytes preserved verbatim"
    );
    assert_eq!(dead[0].envelope.max_attempts, 7, "max_attempts preserved");
}

/// Reading an empty dead-letter store returns no records.
pub async fn read_empty_store<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let dead = b.read_dead_letters(&lane("default"), 10).await.unwrap();
    assert!(dead.is_empty(), "an empty dead-letter store reads as empty");
}

/// The count equals the number of jobs dead-lettered on a lane.
pub async fn count_reflects_dead_lettered_jobs<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    for _ in 0..3 {
        dead_letter(b.as_ref(), job("critical"), "boom").await;
    }
    let count = b.count_dead_letters(&lane("critical")).await.unwrap();
    assert_eq!(count, 3, "the count must equal the number of dead letters");
}

/// The count is lane-scoped: dead letters on one lane do not count for another.
pub async fn count_is_lane_scoped<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    dead_letter(b.as_ref(), job("a"), "boom").await;
    let other = b.count_dead_letters(&lane("b")).await.unwrap();
    assert_eq!(other, 0, "a count for a different lane must be zero");
}

/// An empty dead-letter store counts zero.
pub async fn count_empty_store_is_zero<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let count = b.count_dead_letters(&lane("default")).await.unwrap();
    assert_eq!(count, 0, "an empty dead-letter store counts as zero");
}

/// The count is non-destructive: counting leaves every record readable and a
/// recount returns the same value.
pub async fn count_is_non_destructive<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    for _ in 0..2 {
        dead_letter(b.as_ref(), job("critical"), "boom").await;
    }
    let first = b.count_dead_letters(&lane("critical")).await.unwrap();
    assert_eq!(first, 2);
    let dead = b.read_dead_letters(&lane("critical"), 10).await.unwrap();
    assert_eq!(dead.len(), 2, "count must not remove records");
    let again = b.count_dead_letters(&lane("critical")).await.unwrap();
    assert_eq!(again, 2, "a recount must return the same value");
}

/// The count drops by one after a dead-lettered job is requeued.
pub async fn count_consistent_after_requeue<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let id = dead_letter(b.as_ref(), job("critical"), "boom").await;
    dead_letter(b.as_ref(), job("critical"), "boom").await;
    assert_eq!(b.count_dead_letters(&lane("critical")).await.unwrap(), 2);
    b.requeue(id).await.expect("requeue");
    assert_eq!(
        b.count_dead_letters(&lane("critical")).await.unwrap(),
        1,
        "requeue must drop the count by one"
    );
}

/// Purge removes every dead-letter record for a lane and reports the count; a
/// subsequent read and count see an empty store.
pub async fn purge_removes_lane_dead_letters<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    dead_letter(b.as_ref(), job("critical"), "boom").await;
    dead_letter(b.as_ref(), job("critical"), "boom").await;
    assert_eq!(b.count_dead_letters(&lane("critical")).await.unwrap(), 2);
    let removed = b.purge_dead_letters(&lane("critical")).await.unwrap();
    assert_eq!(removed, 2, "purge reports how many records it removed");
    assert_eq!(
        b.count_dead_letters(&lane("critical")).await.unwrap(),
        0,
        "the lane's dead-letter store is empty after purge"
    );
    assert!(
        b.read_dead_letters(&lane("critical"), 10)
            .await
            .unwrap()
            .is_empty(),
        "no records remain readable after purge"
    );
}

/// Purge is lane-scoped: it does not touch other lanes' dead letters.
pub async fn purge_is_lane_scoped<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    dead_letter(b.as_ref(), job("critical"), "boom").await;
    dead_letter(b.as_ref(), job("default"), "boom").await;
    let removed = b.purge_dead_letters(&lane("critical")).await.unwrap();
    assert_eq!(removed, 1);
    assert_eq!(
        b.count_dead_letters(&lane("default")).await.unwrap(),
        1,
        "another lane's dead letters are untouched"
    );
}

/// Purging an empty lane removes nothing and returns zero.
pub async fn purge_empty_lane_is_zero<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    assert_eq!(b.purge_dead_letters(&lane("default")).await.unwrap(), 0);
}

/// A read concurrent with a `requeue` of a record on the same lane must not fail:
/// the requeued record MAY be absent from the result (if the requeue removed it
/// before the read observed it), but the read SHALL still succeed and return the
/// remaining records. A `Mutex`/single-connection broker satisfies this by
/// construction; a pooled, networked broker must not let the concurrent removal
/// error or corrupt the read.
pub async fn read_succeeds_concurrent_with_requeue<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let id = dead_letter(b.as_ref(), job("critical"), "boom").await;
    dead_letter(b.as_ref(), job("critical"), "boom").await;
    let l = lane("critical");

    // Read and requeue in flight at once on the same lane.
    let (read, requeued) = tokio::join!(b.read_dead_letters(&l, 10), b.requeue(id));
    let dead = read.expect("a read concurrent with a requeue must still succeed");
    requeued.expect("requeue must succeed");
    // The requeued record may or may not have been observed by the read,
    // depending on who won the race — but the read must have returned the
    // surviving records without error.
    assert!(
        dead.len() == 1 || dead.len() == 2,
        "read must return the surviving records (1 if the requeue landed first, else 2), got {}",
        dead.len()
    );

    // Once both settle, the requeued record is live again and exactly the other
    // record remains dead-lettered.
    assert_eq!(
        b.count_dead_letters(&l).await.unwrap(),
        1,
        "after the requeue settles exactly one dead-letter remains"
    );
}
