use super::{job, lane};
use crate::BrokerContractHarness;
use worklane_core::{Broker, NewJob, QueueStats};

/// A schedule occurrence can only be claimed once, and only occurrences strictly
/// greater than the last claimed occurrence are accepted. When accepted, the
/// job is atomically enqueued.
pub async fn enqueue_scheduled_semantics<H: BrokerContractHarness>(h: &H) {
    let Some(store) = h.scheduled_store() else {
        return;
    };
    let b = h.broker();
    let sched = "test_sched_1";

    // First claim succeeds.
    let claimed = store
        .enqueue_scheduled(sched, 1000, job("default"))
        .await
        .expect("enqueue 1000");
    assert!(claimed, "first claim should succeed");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "job must be enqueued on success"
    );

    // Exact same claim fails.
    let claimed_again = store
        .enqueue_scheduled(sched, 1000, job("default"))
        .await
        .expect("enqueue 1000 again");
    assert!(
        !claimed_again,
        "second claim of same occurrence should fail"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "job must not be enqueued on failure"
    );

    // Older claim fails.
    let claimed_older = store
        .enqueue_scheduled(sched, 999, job("default"))
        .await
        .expect("enqueue 999");
    assert!(!claimed_older, "claim of older occurrence should fail");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "job must not be enqueued on failure"
    );

    // Newer claim succeeds.
    let claimed_newer = store
        .enqueue_scheduled(sched, 1001, job("default"))
        .await
        .expect("enqueue 1001");
    assert!(claimed_newer, "claim of newer occurrence should succeed");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "job must be enqueued on success"
    );

    // Large-magnitude occurrences must compare EXACTLY, not through an f64. Above
    // 2^53 a double rounds, so two adjacent i64 occurrences can collide on the
    // stored watermark. A backend that compared/stored the watermark as a
    // floating-point number would round both `i64::MAX - 1` and `i64::MAX` to
    // 2^63 and wrongly reject the strictly-greater second claim. (Regression
    // guard for the Redis watermark, which must use an order-preserving encoding.)
    let sched_hi = "test_sched_precision";
    let claimed_hi = store
        .enqueue_scheduled(sched_hi, i64::MAX - 1, job("default"))
        .await
        .expect("enqueue i64::MAX - 1");
    assert!(claimed_hi, "first large-occurrence claim should succeed");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the i64::MAX - 1 job must be enqueued"
    );

    let claimed_max = store
        .enqueue_scheduled(sched_hi, i64::MAX, job("default"))
        .await
        .expect("enqueue i64::MAX");
    assert!(
        claimed_max,
        "i64::MAX is strictly greater than i64::MAX - 1 and must be accepted; an \
         f64 watermark would round both to 2^63 and wrongly reject it"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the i64::MAX job must be enqueued"
    );

    // The recorded watermark is now i64::MAX: a repeat is not strictly greater.
    let claimed_max_again = store
        .enqueue_scheduled(sched_hi, i64::MAX, job("default"))
        .await
        .expect("enqueue i64::MAX again");
    assert!(
        !claimed_max_again,
        "a repeat claim of i64::MAX (the recorded occurrence) must fail"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "no job is enqueued for the rejected repeat claim"
    );
}

/// A schedule with no recorded occurrence accepts the first claim of any
/// occurrence value — including `0`, a negative timestamp, and `i64::MIN` — and
/// enqueues the job. A `0` (or any non-`i64::MIN`) sentinel would wrongly reject
/// some of these. Once an occurrence is recorded, only a strictly greater one
/// wins.
pub async fn enqueue_scheduled_initial_state<H: BrokerContractHarness>(h: &H) {
    let Some(store) = h.scheduled_store() else {
        return;
    };
    let b = h.broker();

    // First claim at occurrence 0 succeeds on a fresh schedule.
    let claimed_zero = store
        .enqueue_scheduled("sched_zero", 0, job("default"))
        .await
        .expect("enqueue 0");
    assert!(
        claimed_zero,
        "first claim of occurrence 0 should succeed (sentinel must not be 0)"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the occurrence-0 job must be enqueued"
    );

    // A subsequent claim at 0 (now the recorded occurrence) is not strictly
    // greater and must fail.
    let claimed_zero_again = store
        .enqueue_scheduled("sched_zero", 0, job("default"))
        .await
        .expect("enqueue 0 again");
    assert!(
        !claimed_zero_again,
        "a repeat claim of the recorded occurrence must fail"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "no job is enqueued for the rejected repeat claim"
    );

    // First claim of a negative occurrence on a fresh schedule also succeeds.
    let claimed_negative = store
        .enqueue_scheduled("sched_negative", -1000, job("default"))
        .await
        .expect("enqueue -1000");
    assert!(
        claimed_negative,
        "first claim of a negative occurrence should succeed on a fresh schedule"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the negative-occurrence job must be enqueued"
    );

    // First claim at i64::MIN on a fresh schedule succeeds: absence accepts any
    // first claim, matching the durable backends' unconditional first insert.
    let claimed_min = store
        .enqueue_scheduled("sched_min", i64::MIN, job("default"))
        .await
        .expect("enqueue i64::MIN");
    assert!(
        claimed_min,
        "first claim at i64::MIN should succeed on a fresh schedule"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the i64::MIN-occurrence job must be enqueued"
    );
}

/// Occurrences are Unix-second integer watermarks. The broker stores and compares
/// exactly the signed integer supplied by the scheduler; it does not interpret
/// the value through local time zones or calendar arithmetic.
pub async fn enqueue_scheduled_unix_second_watermark<H: BrokerContractHarness>(h: &H) {
    let Some(store) = h.scheduled_store() else {
        return;
    };
    let b = h.broker();
    let sched = "sched_unix_second_watermark";
    let first_unix_second = 1_700_000_000_i64;
    let next_minute = first_unix_second + 60;

    let claimed_first = store
        .enqueue_scheduled(sched, first_unix_second, job("default"))
        .await
        .expect("claim first Unix second");
    assert!(claimed_first, "first Unix-second occurrence should claim");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the first Unix-second job must be enqueued"
    );

    let claimed_older_calendar_neighbor = store
        .enqueue_scheduled(sched, first_unix_second - 1, job("default"))
        .await
        .expect("claim older Unix second");
    assert!(
        !claimed_older_calendar_neighbor,
        "an older Unix-second integer must not claim"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "the older Unix-second job must not be enqueued"
    );

    let claimed_next_minute = store
        .enqueue_scheduled(sched, next_minute, job("default"))
        .await
        .expect("claim next Unix-minute occurrence");
    assert!(
        claimed_next_minute,
        "a larger Unix-second integer must claim"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the next Unix-minute job must be enqueued"
    );
}

/// `remove_schedule` clears a schedule's occurrence watermark, so a later claim of
/// the same (or any) occurrence is accepted afresh — the decommission path. An
/// unknown schedule id removes nothing (idempotent).
pub async fn remove_schedule_resets_watermark<H: BrokerContractHarness>(h: &H) {
    let Some(store) = h.scheduled_store() else {
        return;
    };
    let b = h.broker();
    let sched = "test_sched_remove";

    // Claim 1000, then a repeat is rejected (watermark holds it).
    assert!(
        store
            .enqueue_scheduled(sched, 1000, job("default"))
            .await
            .unwrap()
    );
    assert!(b.reserve(&lane("default")).await.unwrap().is_some());
    assert!(
        !store
            .enqueue_scheduled(sched, 1000, job("default"))
            .await
            .unwrap()
    );

    // Decommission: removing the watermark lets the same occurrence claim again.
    store.remove_schedule(sched).await.unwrap();
    assert!(
        store
            .enqueue_scheduled(sched, 1000, job("default"))
            .await
            .unwrap(),
        "after remove_schedule, the watermark is gone and the claim is accepted afresh"
    );
    assert!(b.reserve(&lane("default")).await.unwrap().is_some());

    // Removing an unknown schedule is a no-op, not an error.
    store.remove_schedule("never_used_schedule").await.unwrap();
}

/// A schedule occurrence claimed successfully but carrying an existing unique key
/// deduplicates to the existing job: it returns true (the claim succeeded) but no
/// second job is created.
pub async fn enqueue_scheduled_unique_key_semantics<H: BrokerContractHarness>(h: &H) {
    let Some(store) = h.scheduled_store() else {
        return;
    };
    let b = h.broker();
    let sched = "test_sched_unique";

    // Enqueue a job with a unique key.
    let _ = b
        .enqueue(job("default").with_unique_key("k"))
        .await
        .unwrap();

    // Now claim a scheduled occurrence with the SAME unique key.
    // The claim MUST succeed, but the job MUST deduplicate (no second job).
    let claimed = store
        .enqueue_scheduled(sched, 1000, job("default").with_unique_key("k"))
        .await
        .expect("enqueue_scheduled 1000");
    assert!(
        claimed,
        "claim of a new occurrence should succeed even if the job deduplicates"
    );

    // We should only have ONE job in the queue, not two.
    let r1 = b.reserve(&lane("default")).await.unwrap();
    assert!(r1.is_some(), "the original job is reservable");
    let r2 = b.reserve(&lane("default")).await.unwrap();
    assert!(
        r2.is_none(),
        "no second job was created due to unique key deduplication"
    );
}

/// A successfully claimed scheduled occurrence still obeys live `JobId`
/// idempotency: if the supplied job id is already live, the claim succeeds but
/// the broker must not create a second live job or overwrite the existing one.
pub async fn enqueue_scheduled_dedups_live_job_id<H: BrokerContractHarness>(h: &H) {
    let Some(store) = h.scheduled_store() else {
        return;
    };
    let b = h.broker();
    let l = lane("scheduled_job_id");

    let first = NewJob::new(l.clone(), "original", b"null".to_vec(), 3);
    let id = first.id;
    b.enqueue(first).await.expect("enqueue original job");

    let mut duplicate = NewJob::new(l.clone(), "duplicate", b"{}".to_vec(), 9);
    duplicate.id = id;
    let claimed = store
        .enqueue_scheduled("test_sched_job_id", 1000, duplicate)
        .await
        .expect("scheduled duplicate-id claim");
    assert!(
        claimed,
        "the schedule occurrence is claimed even when the job id deduplicates"
    );

    assert_eq!(
        b.pending_count(&l).await.unwrap(),
        1,
        "a scheduled duplicate-id claim must not create a second live job"
    );
    let reserved = b.reserve(&l).await.unwrap().expect("original job remains");
    assert_eq!(
        reserved.envelope.kind, "original",
        "a scheduled duplicate-id claim must not overwrite the live job envelope"
    );
    assert_eq!(
        reserved.envelope.max_attempts, 3,
        "the original live envelope is preserved"
    );
    assert!(
        b.reserve(&l).await.unwrap().is_none(),
        "no second lane member may be left behind"
    );
}

/// Concurrent claims of the same schedule occurrence resolve to exactly one
/// winner: only one `enqueue_scheduled` returns `true`, and only one job is
/// enqueued. This is the HA guarantee: N instances racing the same occurrence
/// must not double-enqueue.
pub async fn concurrent_enqueue_scheduled_claims_once<H: BrokerContractHarness>(h: &H) {
    let Some(store) = h.scheduled_store() else {
        return;
    };
    let b = h.broker();
    let sched = "race_sched";
    let occurrence = 1000;

    let (a, c, d, e) = tokio::join!(
        store.enqueue_scheduled(sched, occurrence, job("default")),
        store.enqueue_scheduled(sched, occurrence, job("default")),
        store.enqueue_scheduled(sched, occurrence, job("default")),
        store.enqueue_scheduled(sched, occurrence, job("default")),
    );
    let claims: Vec<bool> = [a, c, d, e]
        .into_iter()
        .map(|r| r.expect("a concurrent claim must resolve, not error"))
        .collect();
    let winners = claims.iter().filter(|&&won| won).count();
    assert_eq!(
        winners, 1,
        "exactly one concurrent claim of the same occurrence must win, got {claims:?}"
    );

    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "the single winning claim must have enqueued its job"
    );
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "the race must not enqueue a second job for the same occurrence"
    );
}
