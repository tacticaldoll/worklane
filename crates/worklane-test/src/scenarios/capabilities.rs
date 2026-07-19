use super::lane;
use crate::BrokerContractHarness;
use worklane_core::{Broker, DeadLetterStore, QueueStats};

/// A broker that implements dead-letter inspection must surface it through the
/// `Broker` accessor, and the returned handle must be wired to a working
/// implementation.
///
/// This guards the precise failure mode the capability-factory design introduces:
/// a broker that implements `DeadLetterStore` but forgets to override the
/// corresponding accessor would silently report `None` and be broken for every
/// real consumer while still satisfying the trait bound.
pub async fn dead_letter_accessors_present<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: DeadLetterStore,
{
    let broker = h.broker();

    let dead_letters = broker
        .dead_letter_store()
        .expect("dead_letter_store() accessor must return Some for a conforming broker");
    dead_letters
        .count_dead_letters(&lane("cap_accessors"))
        .await
        .expect("the dead-letter handle is wired to a working store");
}

/// A broker that implements queue-depth stats must surface them through the
/// `Broker` accessor, and the returned handle must be wired to a working
/// implementation.
pub async fn queue_stats_accessor_present<H>(h: &H)
where
    H: BrokerContractHarness,
    H::Broker: QueueStats,
{
    let broker = h.broker();

    let stats = broker
        .queue_stats()
        .expect("queue_stats() accessor must return Some for a conforming broker");
    stats
        .pending_count(&lane("cap_accessors"))
        .await
        .expect("the queue-stats handle is wired to a working store");
}
