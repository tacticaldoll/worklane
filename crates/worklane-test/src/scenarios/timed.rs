use super::{job, lane};
use crate::TimedBrokerContractHarness;
use std::time::Duration;
use worklane_core::{Broker, Error, NewJob};

/// Retry with a positive delay hides the job until the delay elapses, then
/// exposes it with incremented attempts.
pub async fn retry_delay_hides_then_exposes<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    let delay = Duration::from_secs(5);
    b.retry(r.receipt, delay).await.unwrap();
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "job must be hidden before the retry delay elapses"
    );
    h.advance(delay);
    let r2 = b
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("job must be reservable after the retry delay");
    assert_eq!(r2.envelope.attempts, 1);
}

/// An expired receipt is rejected and does not mutate or remove the job.
pub async fn expired_receipt_rejected_without_mutation<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    h.advance(h.lease());
    assert!(
        matches!(b.ack(r.receipt).await, Err(Error::StaleReservation(_))),
        "an expired receipt must be rejected"
    );
    let r2 = b
        .reserve(&lane("default"))
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
    let first = b
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("first reserve");
    h.advance(h.lease());
    let second = b
        .reserve(&lane("default"))
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
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "the job is gone after a valid ack"
    );
}

/// A reservation conveys the broker's configured lease, so a caller can time a
/// heartbeat without reading the broker's clock.
pub async fn reservation_conveys_lease<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
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
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    // Extend before the lease expires, then advance to the original expiry: the
    // re-applied lease (measured from the extend) keeps the job held.
    h.advance(h.lease() / 2);
    b.extend(r.receipt)
        .await
        .expect("a current receipt must extend");
    h.advance(h.lease() / 2);
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
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
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    h.advance(h.lease());
    assert!(
        matches!(b.extend(r.receipt).await, Err(Error::StaleReservation(_))),
        "extending an expired receipt must be rejected as stale"
    );
    let r2 = b
        .reserve(&lane("default"))
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
    let first = b
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("first reserve");
    h.advance(h.lease());
    let second = b
        .reserve(&lane("default"))
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

/// A job enqueued with a positive delay is hidden until the delay elapses, then
/// becomes reservable — the scheduled (delayed) enqueue primitive.
pub async fn delayed_enqueue_hidden_until_due<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    let delay = Duration::from_secs(5);
    b.enqueue(NewJob::new(lane("default"), "ok", b"null".to_vec(), 3).with_delay(delay))
        .await
        .unwrap();
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "a delayed job must be hidden before its delay elapses"
    );
    h.advance(delay);
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_some(),
        "a delayed job must be reservable after its delay elapses"
    );
}

/// A retry with an extreme delay saturates the job's visibility instead of
/// overflowing or panicking. The broker computes the next visibility as
/// `now + delay`; with `delay == Duration::MAX` that must saturate (not wrap or
/// panic), leaving the job hidden far in the future so a normal clock advance
/// does not expose it. Guards the broker's saturating visibility/lease math.
pub async fn retry_extreme_delay_saturates<H: TimedBrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    b.retry(r.receipt, Duration::MAX)
        .await
        .expect("retry with an extreme delay must saturate, not error or panic");
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "a job retried with an extreme delay is hidden immediately"
    );
    h.advance(Duration::from_secs(3600));
    assert!(
        b.reserve(&lane("default")).await.unwrap().is_none(),
        "a normal clock advance must not expose a job retried with an extreme delay"
    );
}
