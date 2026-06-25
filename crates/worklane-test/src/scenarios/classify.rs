use super::{dead_letter, job, lane};
use crate::BrokerContractHarness;
use worklane_core::{Broker, DeadLetterStore};

/// A dead-lettered job is reported dead-lettered.
pub async fn classify_dead_lettered_after_fail<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let id = dead_letter(b.as_ref(), job("critical"), "boom").await;
    assert_eq!(
        b.classify(id).await.unwrap(),
        worklane_core::JobState::DeadLettered,
        "a failed job must be reported dead-lettered"
    );
}

/// An acked job is not live and not dead-lettered (CompletedOrUnknown).
pub async fn classify_completed_or_unknown_for_acked<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let id = b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    b.ack(r.receipt).await.unwrap();
    assert_eq!(
        b.classify(id).await.unwrap(),
        worklane_core::JobState::CompletedOrUnknown,
        "an acked job is CompletedOrUnknown"
    );
}

/// An unknown id is CompletedOrUnknown.
pub async fn classify_completed_or_unknown_for_unknown<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    assert_eq!(
        b.classify(worklane_core::JobId::new()).await.unwrap(),
        worklane_core::JobState::CompletedOrUnknown,
        "an unknown id is CompletedOrUnknown"
    );
}

/// A pending job is reported live; it stays live while leased (in-flight).
pub async fn classify_live_for_pending_and_leased<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    let id = b.enqueue(job("default")).await.unwrap();
    assert_eq!(
        b.classify(id).await.unwrap(),
        worklane_core::JobState::Live,
        "a pending job is live"
    );
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    assert_eq!(
        b.classify(id).await.unwrap(),
        worklane_core::JobState::Live,
        "a leased (in-flight) job is still live"
    );
    // keep the receipt alive to the end of the scenario
    let _ = r;
}

/// A requeued job is live again on its original lane.
pub async fn classify_live_after_requeue<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let b = h.broker();
    let id = dead_letter(b.as_ref(), job("critical"), "boom").await;
    assert_eq!(
        b.classify(id).await.unwrap(),
        worklane_core::JobState::DeadLettered
    );
    b.requeue(id).await.unwrap();
    assert_eq!(
        b.classify(id).await.unwrap(),
        worklane_core::JobState::Live,
        "a requeued job is live again"
    );
}

/// The check is non-destructive: the record is still readable and a re-check
/// still reports it present.
pub async fn classify_is_non_destructive<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let b = h.broker();
    let id = dead_letter(b.as_ref(), job("critical"), "boom").await;
    assert_eq!(
        b.classify(id).await.unwrap(),
        worklane_core::JobState::DeadLettered
    );
    let dead = b.read_dead_letters(&lane("critical"), 10).await.unwrap();
    assert_eq!(dead.len(), 1, "the check must not remove the record");
    assert_eq!(
        b.classify(id).await.unwrap(),
        worklane_core::JobState::DeadLettered,
        "a re-check must still report it present"
    );
}
