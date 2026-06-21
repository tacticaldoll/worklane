use super::{job, lane};
use crate::BrokerContractHarness;
use std::time::Duration;
use worklane_core::{Broker, Error};

/// Concurrent reserves on one lane never hand the same job out twice. A
/// single-connection or `Mutex`-serialized broker satisfies this by
/// construction; a pooled, networked broker must satisfy it under genuinely
/// concurrent in-flight reserves (e.g. `FOR UPDATE SKIP LOCKED`).
pub async fn concurrent_reserve_no_double_handout<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    for _ in 0..4 {
        b.enqueue(job("default")).await.unwrap();
    }
    // Four reserves in flight at once on the same lane.
    let default = lane("default");
    let (r1, r2, r3, r4) = tokio::join!(
        b.reserve(&default),
        b.reserve(&default),
        b.reserve(&default),
        b.reserve(&default),
    );
    let ids: Vec<_> = [r1, r2, r3, r4]
        .into_iter()
        .filter_map(|r| r.unwrap().map(|res| res.envelope.id))
        .collect();
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "concurrent reserves must not hand the same job out twice"
    );
    assert_eq!(
        ids.len(),
        4,
        "four concurrent reserves over four visible jobs should each get a distinct job"
    );
}

/// Two acks racing with the same receipt remove the job at most once: exactly
/// one succeeds and the other is rejected as stale. A `Mutex`/single-connection
/// broker satisfies this by construction; an atomic-op broker (e.g. a Redis Lua
/// script) must satisfy it under genuinely concurrent resolutions.
pub async fn concurrent_ack_resolves_once<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    let receipt = r.receipt;
    let (a, c) = tokio::join!(b.ack(receipt), b.ack(receipt));
    let results = [a, c];
    let oks = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        oks, 1,
        "exactly one concurrent ack with the same receipt should succeed"
    );
    for r in &results {
        if let Err(e) = r {
            assert!(
                matches!(e, Error::StaleReservation(_)),
                "the losing concurrent ack must be a stale-reservation error"
            );
        }
    }
}

/// Two retries racing with the same receipt resolve the reservation at most
/// once: exactly one succeeds, the other is stale, and `attempts` rises by one.
pub async fn concurrent_retry_resolves_once<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("default")).await.unwrap();
    let r = b.reserve(&lane("default")).await.unwrap().expect("job");
    let receipt = r.receipt;
    let (a, c) = tokio::join!(
        b.retry(receipt, Duration::ZERO),
        b.retry(receipt, Duration::ZERO),
    );
    let results = [a, c];
    let oks = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        oks, 1,
        "exactly one concurrent retry with the same receipt should succeed"
    );
    for r in &results {
        if let Err(e) = r {
            assert!(
                matches!(e, Error::StaleReservation(_)),
                "the losing concurrent retry must be a stale-reservation error"
            );
        }
    }
    let r2 = b
        .reserve(&lane("default"))
        .await
        .unwrap()
        .expect("reservable again after a zero-delay retry");
    assert_eq!(
        r2.envelope.attempts, 1,
        "attempts must be incremented exactly once, not twice"
    );
}

/// Two fails racing with the same receipt resolve the reservation at most once:
/// exactly one succeeds, the other is stale, and the dead-letter store holds
/// exactly one record.
pub async fn concurrent_fail_resolves_once<H: BrokerContractHarness>(h: &H) {
    let b = h.broker();
    b.enqueue(job("critical")).await.unwrap();
    let r = b.reserve(&lane("critical")).await.unwrap().expect("job");
    let receipt = r.receipt;
    let (a, c) = tokio::join!(
        b.fail(receipt, "boom".to_string()),
        b.fail(receipt, "boom".to_string()),
    );
    let results = [a, c];
    let oks = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        oks, 1,
        "exactly one concurrent fail with the same receipt should succeed"
    );
    for r in &results {
        if let Err(e) = r {
            assert!(
                matches!(e, Error::StaleReservation(_)),
                "the losing concurrent fail must be a stale-reservation error"
            );
        }
    }
    if let Some(dead) = h.dead_letters(&b).await {
        assert_eq!(
            dead.len(),
            1,
            "a reservation must dead-letter exactly once, not twice"
        );
    }
}
