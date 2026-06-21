use super::{job, lane};
use crate::BrokerContractHarness;
use std::time::Duration;
use worklane_core::{Broker, Error, NewJob, ReservationReceipt};

/// Enqueue then reserve on the same lane returns the job.
pub async fn enqueue_then_reserve_same_lane<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b
        .reserve(&lane("default"))
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
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "a different lane must not see the job"
    );
    let r = b
        .reserve(&lane("critical"))
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
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("first reserve gets the job");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "a leased job must not be handed out again"
    );
}

/// Ack with the current receipt removes the job.
pub async fn ack_removes_job<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    b.ack(r.receipt).await.unwrap();
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "an acked job must not be reservable again"
    );
}

/// Retry with zero delay increments attempts and the job is immediately
/// reservable again (the time-free probe of retry semantics).
pub async fn retry_zero_delay_increments_and_revisible<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    assert_eq!(r.envelope.attempts, 0);
    b.retry(r.receipt, Duration::ZERO).await.unwrap();
    let r2 = b
        .reserve(&lane("default"))
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
    let r = b.reserve(&lane("critical")).await.unwrap().expect("job");
    b.fail(r.receipt, "boom".to_string()).await.unwrap();
    assert!(
        b.reserve(&lane("critical")).await.unwrap().is_none(),
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
    b.enqueue(NewJob::new(
        lane("critical"),
        "send_email",
        payload.clone(),
        7,
    ))
    .await
    .unwrap();
    let r = b
        .reserve(&lane("critical"))
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
