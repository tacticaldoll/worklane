use super::lane;
use crate::BrokerContractHarness;
use worklane_core::Broker;

/// A broker that implements the optional capability traits must also surface them
/// through the `Broker` accessors, and the returned handles must be wired to a
/// working implementation.
///
/// This guards the precise failure mode the capability-factory design introduces:
/// a broker that implements `DeadLetterStore` / `QueueStats` but forgets to
/// override the corresponding accessor would silently report `None` and be broken
/// for every real consumer (CLI, metrics) while still satisfying the trait bound.
pub async fn capability_accessors_present<H: BrokerContractHarness>(h: &H) {
    let broker = h.broker();

    let dead_letters = broker
        .dead_letter_store()
        .expect("dead_letter_store() accessor must return Some for a conforming broker");
    dead_letters
        .count_dead_letters(&lane("cap_accessors"))
        .await
        .expect("the dead-letter handle is wired to a working store");

    let stats = broker
        .queue_stats()
        .expect("queue_stats() accessor must return Some for a conforming broker");
    stats
        .pending_count(&lane("cap_accessors"))
        .await
        .expect("the queue-stats handle is wired to a working store");
}
