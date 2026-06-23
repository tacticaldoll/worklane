use super::{dead_letter, job, lane};
use crate::BrokerContractHarness;
use worklane_core::{Broker, DeadLetterStore, Error, NewJob};

/// Requeue makes a dead-lettered job reservable on its original lane again and
/// removes it from the dead-letter store.
pub async fn requeue_makes_reservable_again<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let b = h.broker();
    let id = dead_letter(b.as_ref(), job("critical"), "boom").await;
    b.requeue(id).await.expect("a dead-lettered job requeues");
    let r = b.reserve(&lane("critical")).await.unwrap();
    assert!(
        r.is_some(),
        "a requeued job must be reservable on its original lane"
    );
    let dead = b.read_dead_letters(&lane("critical"), 10).await.unwrap();
    assert!(
        dead.is_empty(),
        "a requeued job must leave the dead-letter store"
    );
}

/// Requeue preserves the opaque envelope: the re-reserved job matches the
/// original payload, kind, and max_attempts.
pub async fn requeue_preserves_opaque_envelope<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let b = h.broker();
    let payload = vec![0u8, 159, 146, 150, 255, 0, 1, 2, 254];
    let id = dead_letter(
        b.as_ref(),
        NewJob::new(lane("critical"), "send_email", payload.clone(), 7),
        "boom",
    )
    .await;
    b.requeue(id).await.expect("requeue");
    let r = b
        .reserve(&lane("critical"))
        .await
        .unwrap()
        .expect("requeued job");
    assert_eq!(
        r.envelope.kind, "send_email",
        "kind preserved across requeue"
    );
    assert_eq!(
        r.envelope.payload, payload,
        "payload bytes preserved across requeue"
    );
    assert_eq!(
        r.envelope.max_attempts, 7,
        "max_attempts preserved across requeue"
    );
}

/// A dead-lettered job that carried a `unique_key` re-acquires it on requeue when
/// the key is free: a subsequent enqueue with that key deduplicates to the
/// requeued job. (The key was released when the job was dead-lettered, so requeue
/// must actively reclaim it.)
pub async fn requeue_reacquires_free_unique_key<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let b = h.broker();
    let id1 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .expect("enqueue with key");
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    b.fail(r.receipt, "boom".to_string()).await.unwrap();
    // The key is free now; requeue must re-acquire it for the revived job.
    b.requeue(id1).await.expect("requeue the dead job");
    let id2 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .expect("enqueue with same key");
    assert_eq!(
        id2, id1,
        "after requeue re-acquires the key, a same-key enqueue must dedup to the requeued job"
    );
}

/// Requeue is rejected when the dead-lettered job's `unique_key` is now held by
/// another live job (legitimately, since the key was released at fail). The
/// rejection is a `UniqueKeyHeld` error that leaves both the dead job and the
/// live holder untouched.
pub async fn requeue_conflicts_when_unique_key_held<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let b = h.broker();
    let id1 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .expect("enqueue with key");
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    b.fail(r.receipt, "boom".to_string()).await.unwrap();
    // Key released at fail → a new job legitimately claims it.
    let id2 = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .expect("enqueue with same key");
    assert_ne!(id2, id1, "after fail releases the key, a new job claims it");
    // Requeuing the original must now fail: its key is held.
    let err = b
        .requeue(id1)
        .await
        .expect_err("requeue must fail when the unique key is held");
    assert!(
        matches!(err, Error::UniqueKeyHeld(_)),
        "expected UniqueKeyHeld, got {err:?}"
    );
    // The live holder is unaffected and still the one reserved.
    let held = b
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("the key holder is still live");
    assert_eq!(
        held.envelope.id, id2,
        "the rejected requeue must not disturb the live key holder"
    );
}

/// Requeue is rejected when the dead-lettered job's id is already live again.
/// The dead record stays available for inspection and the live holder is not
/// disturbed.
pub async fn requeue_conflicts_when_job_id_is_live<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let b = h.broker();
    let id = dead_letter(b.as_ref(), job("requeue_id"), "boom").await;

    let mut live = NewJob::new(lane("requeue_id"), "live_holder", b"null".to_vec(), 3);
    live.id = id;
    b.enqueue(live)
        .await
        .expect("a completed/dead id may be reused by a new live job");

    let err = b
        .requeue(id)
        .await
        .expect_err("requeue must fail while the same job id is live");
    assert!(
        matches!(err, Error::LiveJobIdConflict(_)),
        "expected LiveJobIdConflict, got {err:?}"
    );

    let dead = b.read_dead_letters(&lane("requeue_id"), 10).await.unwrap();
    assert_eq!(
        dead.len(),
        1,
        "the rejected requeue must leave the dead-letter record intact"
    );
    assert_eq!(dead[0].envelope.id, id);

    let held = b
        .reserve(&lane("requeue_id"))
        .await
        .unwrap()
        .expect("the live holder remains reservable");
    assert_eq!(held.envelope.kind, "live_holder");
    assert!(
        b.reserve(&lane("requeue_id")).await.unwrap().is_none(),
        "the rejected requeue must not create another live lane member"
    );
}

/// Requeue of an unknown job id is rejected and changes nothing.
pub async fn requeue_unknown_rejected<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let b = h.broker();
    let id = dead_letter(b.as_ref(), job("critical"), "boom").await;
    b.requeue(worklane_core::JobId::new())
        .await
        .expect_err("requeue of an unknown id must be rejected");
    let dead = b.read_dead_letters(&lane("critical"), 10).await.unwrap();
    assert_eq!(
        dead.len(),
        1,
        "a rejected requeue must not change the dead-letter store"
    );
    assert_eq!(
        dead[0].envelope.id, id,
        "the existing dead-letter record is untouched"
    );
}
