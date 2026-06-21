use super::{job, lane};
use crate::BrokerContractHarness;
use worklane_core::{Broker, QueueStats};

/// `pending_count` reports live (enqueued, not-yet-resolved) jobs on a lane: it
/// rises on enqueue, drops when a job is acked, and excludes dead-lettered jobs.
pub async fn pending_count_reflects_live_jobs<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let l = lane("pending_live");
    assert_eq!(
        b.pending_count(&l).await.unwrap(),
        0,
        "an empty lane has zero pending"
    );

    for _ in 0..3 {
        b.enqueue(job("pending_live")).await.unwrap();
    }
    assert_eq!(
        b.pending_count(&l).await.unwrap(),
        3,
        "three enqueued are pending"
    );

    // Ack one: it is no longer live.
    let r = b.reserve(&l).await.unwrap().expect("reservable");
    b.ack(r.receipt).await.unwrap();
    assert_eq!(
        b.pending_count(&l).await.unwrap(),
        2,
        "an acked job is no longer pending"
    );

    // Fail one to the dead-letter store: dead-lettered jobs are excluded.
    let r = b.reserve(&l).await.unwrap().expect("reservable");
    b.fail(r.receipt, "boom".to_string()).await.unwrap();
    assert_eq!(
        b.pending_count(&l).await.unwrap(),
        1,
        "a dead-lettered job is excluded from pending"
    );
}

/// `pending_count` is lane-scoped: jobs on other lanes do not contribute.
pub async fn pending_count_is_lane_scoped<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("pending_x")).await.unwrap();
    b.enqueue(job("pending_y")).await.unwrap();
    b.enqueue(job("pending_y")).await.unwrap();

    assert_eq!(b.pending_count(&lane("pending_x")).await.unwrap(), 1);
    assert_eq!(b.pending_count(&lane("pending_y")).await.unwrap(), 2);
    assert_eq!(
        b.pending_count(&lane("pending_z")).await.unwrap(),
        0,
        "a lane with no jobs has zero pending"
    );
}

/// An in-flight (reserved-but-unresolved) job still counts as pending — it is live
/// work, not yet done.
pub async fn pending_count_includes_in_flight<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let l = lane("pending_inflight");
    b.enqueue(job("pending_inflight")).await.unwrap();
    b.enqueue(job("pending_inflight")).await.unwrap();

    let _held = b.reserve(&l).await.unwrap().expect("reservable");
    assert_eq!(
        b.pending_count(&l).await.unwrap(),
        2,
        "a reserved (in-flight) job stays pending until acked or failed"
    );
}
