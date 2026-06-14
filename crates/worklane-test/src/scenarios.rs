//! The broker contract scenarios, one async function each. They are the
//! executable form of the broker spec; the macros in the crate root wrap each
//! as a `#[tokio::test]` over a caller-provided harness.

use std::time::Duration;

use worklane_core::{Broker, Error, NewJob, ReservationReceipt};

use crate::harness::{BrokerContractHarness, TimedBrokerContractHarness};

fn job(lane: &str) -> NewJob {
    NewJob::new(lane, "ok", b"null".to_vec(), 3)
}

// --- Required tier: no manual clock needed --------------------------------

/// Enqueue then reserve on the same lane returns the job.
pub async fn enqueue_then_reserve_same_lane<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b
        .reserve("default")
        .await
        .unwrap()
        .expect("enqueued job should be reservable");
    assert_eq!(r.envelope.lane, "default");
}

/// A reserve on one lane never returns a job from another lane.
pub async fn reserve_isolates_lanes<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("critical")).await.unwrap();
    assert!(
        b.reserve("default").await.unwrap().is_none(),
        "a different lane must not see the job"
    );
    let r = b
        .reserve("critical")
        .await
        .unwrap()
        .expect("the owning lane sees the job");
    assert_eq!(r.envelope.lane, "critical");
}

/// A leased job is not handed out a second time.
pub async fn reserve_does_not_double_hand_out<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let _first = b
        .reserve("default")
        .await
        .unwrap()
        .expect("first reserve gets the job");
    assert!(
        b.reserve("default").await.unwrap().is_none(),
        "a leased job must not be handed out again"
    );
}

/// Ack with the current receipt removes the job.
pub async fn ack_removes_job<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve("default").await.unwrap().expect("job");
    b.ack(r.receipt).await.unwrap();
    assert!(
        b.reserve("default").await.unwrap().is_none(),
        "an acked job must not be reservable again"
    );
}

/// Retry with zero delay increments attempts and the job is immediately
/// reservable again (the time-free probe of retry semantics).
pub async fn retry_zero_delay_increments_and_revisible<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve("default").await.unwrap().expect("job");
    assert_eq!(r.envelope.attempts, 0);
    b.retry(r.receipt, Duration::ZERO).await.unwrap();
    let r2 = b
        .reserve("default")
        .await
        .unwrap()
        .expect("zero-delay retry should be immediately reservable");
    assert_eq!(r2.envelope.attempts, 1, "retry must increment attempts");
}

/// Fail with the current receipt removes the live job; if the harness exposes
/// dead-letter inspection, the dead-letter content is asserted, otherwise that
/// assertion is visibly skipped.
pub async fn fail_removes_live_job_and_dead_letters<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("critical")).await.unwrap();
    let r = b.reserve("critical").await.unwrap().expect("job");
    b.fail(r.receipt, "boom".to_string()).await.unwrap();
    assert!(
        b.reserve("critical").await.unwrap().is_none(),
        "a failed job must not be reservable again"
    );
    match h.dead_letters(&b).await {
        Some(dead) => {
            assert_eq!(dead.len(), 1, "exactly one dead-letter expected");
            assert_eq!(dead[0].error, "boom");
            assert_eq!(
                dead[0].envelope.lane, "critical",
                "dead-letter retains lane"
            );
        }
        None => eprintln!(
            "SKIP fail_removes_live_job_and_dead_letters: harness exposes no dead-letter inspection; content not asserted"
        ),
    }
}

/// A receipt that was never issued by this broker is rejected as stale.
pub async fn unknown_receipt_rejected<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let err = b
        .ack(ReservationReceipt::new())
        .await
        .expect_err("an unknown receipt must not ack");
    assert!(
        matches!(err, Error::StaleReservation(_)),
        "an unknown receipt must be rejected as stale"
    );
}

/// Every envelope field survives storage and is returned unchanged by
/// `reserve`, including the opaque payload bytes (here non-UTF-8). An in-memory
/// broker satisfies this by identity; a durable broker via a storage round-trip.
pub async fn enqueue_preserves_envelope_fields<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    // Arbitrary, deliberately non-UTF-8 bytes.
    let payload = vec![0u8, 159, 146, 150, 255, 0, 1, 2, 254];
    b.enqueue(NewJob::new("critical", "send_email", payload.clone(), 7))
        .await
        .unwrap();
    let r = b
        .reserve("critical")
        .await
        .unwrap()
        .expect("enqueued job should be reservable");
    assert_eq!(r.envelope.lane, "critical", "lane must be preserved");
    assert_eq!(r.envelope.kind, "send_email", "kind must be preserved");
    assert_eq!(
        r.envelope.payload, payload,
        "payload bytes must survive storage verbatim"
    );
    assert_eq!(r.envelope.max_attempts, 7, "max_attempts must be preserved");
    assert_eq!(
        r.envelope.attempts, 0,
        "first reservation has zero prior attempts"
    );
}

// --- Timed tier: requires advancing the injected clock --------------------

/// Retry with a positive delay hides the job until the delay elapses, then
/// exposes it with incremented attempts.
pub async fn retry_delay_hides_then_exposes<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve("default").await.unwrap().expect("job");
    let delay = Duration::from_secs(5);
    b.retry(r.receipt, delay).await.unwrap();
    assert!(
        b.reserve("default").await.unwrap().is_none(),
        "job must be hidden before the retry delay elapses"
    );
    h.advance(delay);
    let r2 = b
        .reserve("default")
        .await
        .unwrap()
        .expect("job must be reservable after the retry delay");
    assert_eq!(r2.envelope.attempts, 1);
}

/// An expired receipt is rejected and does not mutate or remove the job.
pub async fn expired_receipt_rejected_without_mutation<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve("default").await.unwrap().expect("job");
    h.advance(h.lease());
    assert!(
        matches!(b.ack(r.receipt).await, Err(Error::StaleReservation(_))),
        "an expired receipt must be rejected"
    );
    let r2 = b
        .reserve("default")
        .await
        .unwrap()
        .expect("the job should requeue after lease expiry");
    assert_eq!(
        r2.envelope.attempts, 0,
        "a stale ack must not mutate the job"
    );
}

/// After the lease expires and the job is re-reserved, the first (superseded)
/// receipt is rejected while the current receipt resolves.
pub async fn superseded_receipt_rejected_current_resolves<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let first = b.reserve("default").await.unwrap().expect("first reserve");
    h.advance(h.lease());
    let second = b
        .reserve("default")
        .await
        .unwrap()
        .expect("re-reserve after lease expiry");
    assert!(
        matches!(b.ack(first.receipt).await, Err(Error::StaleReservation(_))),
        "the superseded receipt must be rejected"
    );
    b.ack(second.receipt)
        .await
        .expect("the current receipt must resolve");
    assert!(
        b.reserve("default").await.unwrap().is_none(),
        "the job is gone after a valid ack"
    );
}

/// A reservation conveys the broker's configured lease, so a caller can time a
/// heartbeat without reading the broker's clock.
pub async fn reservation_conveys_lease<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve("default").await.unwrap().expect("job");
    assert_eq!(
        r.lease,
        h.lease(),
        "the reservation must convey the broker's lease duration"
    );
}

/// Extending a held reservation keeps the job hidden past its original lease and
/// leaves it resolvable with the same receipt.
pub async fn extend_holds_past_original_lease<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve("default").await.unwrap().expect("job");
    // Extend before the lease expires, then advance to the original expiry: the
    // re-applied lease (measured from the extend) keeps the job held.
    h.advance(h.lease() / 2);
    b.extend(r.receipt)
        .await
        .expect("a current receipt must extend");
    h.advance(h.lease() / 2);
    assert!(
        b.reserve("default").await.unwrap().is_none(),
        "an extended job stays hidden past its original lease"
    );
    b.ack(r.receipt)
        .await
        .expect("the same receipt still resolves after an extend");
}

/// Extending after the lease has expired is rejected as stale and does not
/// mutate the job.
pub async fn extend_after_expiry_rejected<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve("default").await.unwrap().expect("job");
    h.advance(h.lease());
    assert!(
        matches!(b.extend(r.receipt).await, Err(Error::StaleReservation(_))),
        "extending an expired receipt must be rejected as stale"
    );
    let r2 = b
        .reserve("default")
        .await
        .unwrap()
        .expect("the job should requeue after lease expiry");
    assert_eq!(
        r2.envelope.attempts, 0,
        "a rejected extend must not mutate the job"
    );
}

/// After the lease expires and the job is re-reserved, the first (superseded)
/// receipt cannot extend, and the current reservation is unaffected.
pub async fn superseded_receipt_cannot_extend<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let first = b.reserve("default").await.unwrap().expect("first reserve");
    h.advance(h.lease());
    let second = b
        .reserve("default")
        .await
        .unwrap()
        .expect("re-reserve after lease expiry");
    assert!(
        matches!(
            b.extend(first.receipt).await,
            Err(Error::StaleReservation(_))
        ),
        "a superseded receipt must not extend"
    );
    b.ack(second.receipt)
        .await
        .expect("the current receipt must still resolve");
}
