use super::{job, lane};
use crate::{BrokerContractHarness, TimedBrokerContractHarness};
use std::time::Duration;
use worklane_core::Broker;

/// Reserve hands out the highest-priority available job first, regardless of
/// enqueue order. Enqueuing the low-priority job first proves priority — not
/// FIFO — drives the choice.
pub async fn reserve_highest_priority_first<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let _low = b
        .enqueue(job("default").with_priority(1))
        .await
        .expect("enqueue low");
    let high = b
        .enqueue(job("default").with_priority(9))
        .await
        .expect("enqueue high");
    let r = b
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("a job must be reservable");
    assert_eq!(
        r.envelope.id, high,
        "the highest-priority job must be reserved first, not the oldest"
    );
}

/// Within a single priority, reserve hands out the oldest job first (FIFO). The
/// clock is advanced between enqueues so the two jobs carry distinct
/// `available_at` values, making the ordering deterministic on every backend
/// (including Redis, whose per-priority set orders by that timestamp).
pub async fn reserve_oldest_within_same_priority<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    let first = b
        .enqueue(job("default").with_priority(5))
        .await
        .expect("enqueue first");
    h.advance(Duration::from_secs(1));
    let second = b
        .enqueue(job("default").with_priority(5))
        .await
        .expect("enqueue second");

    let r1 = b
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("first job must be reservable");
    assert_eq!(
        r1.envelope.id, first,
        "same priority must reserve the oldest (FIFO) job first"
    );
    let r2 = b
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("second job must be reservable");
    assert_eq!(
        r2.envelope.id, second,
        "the newer same-priority job must be reserved second"
    );
}

/// Within a single priority *and* identical visibility time, reserve hands out
/// jobs in strict enqueue order (FIFO). Unlike
/// [`reserve_oldest_within_same_priority`], the clock is *not* advanced between
/// enqueues, so all jobs share one `available_at`: the contract's FIFO guarantee
/// — not the visibility timestamp — is what orders them. Three jobs make an
/// accidental pass under a random tiebreak (e.g. a backend ordering by a random
/// job id) vanishingly unlikely.
pub async fn reserve_fifo_within_identical_visibility<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    // No clock advance between enqueues: every job carries the same available_at,
    // so the broker cannot lean on distinct timestamps to order them.
    let mut order = Vec::new();
    for _ in 0..3 {
        order.push(
            b.enqueue(job("default").with_priority(5))
                .await
                .expect("enqueue"),
        );
    }

    for (i, expected) in order.iter().enumerate() {
        let r = b
            .reserve(&lane("default"))
            .await
            .unwrap()
            .expect("a job must be reservable");
        assert_eq!(
            r.envelope.id, *expected,
            "identical-visibility jobs must reserve in strict FIFO (enqueue) order; \
             position {i} did not match the enqueue order"
        );
        // Resolve it so the next reserve advances to the following job rather
        // than re-weighing a still-leased one.
        b.ack(r.receipt).await.expect("ack");
    }
}
