use std::time::Duration;

use super::{job, lane};
use crate::BrokerContractHarness;
use worklane_core::Broker;

/// `defer` reschedules a reserved job to be visible again **without** advancing
/// `attempts` — unlike `retry`. With a zero delay the job is immediately
/// reservable again, and its `attempts` is unchanged.
pub async fn defer_reschedules_without_incrementing_attempts<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let l = lane("defer_attempts");
    b.enqueue(job("defer_attempts")).await.unwrap();

    let r = b.reserve(&l).await.unwrap().expect("reservable");
    assert_eq!(r.envelope.attempts, 0, "fresh job starts at zero attempts");

    b.defer(r.receipt, Duration::ZERO).await.unwrap();

    // Visible again immediately (zero delay) and still at zero attempts: defer did
    // not spend the retry budget.
    let r2 = b
        .reserve(&l)
        .await
        .unwrap()
        .expect("a zero-delay defer makes the job reservable again");
    assert_eq!(
        r2.envelope.attempts, 0,
        "defer must not increment attempts (unlike retry)"
    );
}

/// A receipt released by `defer` is no longer current, so deferring it again is
/// rejected as stale (it changes nothing) — matching `retry`/`ack` receipt
/// semantics.
pub async fn defer_rejects_a_stale_receipt<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let l = lane("defer_stale");
    b.enqueue(job("defer_stale")).await.unwrap();

    let r = b.reserve(&l).await.unwrap().expect("reservable");
    let receipt = r.receipt;
    b.defer(receipt, Duration::ZERO).await.unwrap();

    // The receipt was released by the defer above; using it again must be rejected.
    assert!(
        b.defer(receipt, Duration::ZERO).await.is_err(),
        "a superseded receipt must be rejected as stale"
    );
}
