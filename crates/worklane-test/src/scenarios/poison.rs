use std::time::Duration;

use worklane_core::spi::MAX_DEAD_LETTER_SWEEP;
use worklane_core::{Broker, DeadLetterStore, NewJob};

use crate::{BrokerConfig, ConfigurableBrokerHarness};

use super::{job, lane};

/// The lease scenarios build with and advance past; shared with the config
/// default so the value has a single source.
const LEASE: Duration = BrokerConfig::DEFAULT_LEASE;
const MAX: u32 = 3;

/// A poison-pill job — one whose worker keeps crashing before it can ack, retry,
/// or fail — is redelivered unchanged forever under at-least-once delivery, since
/// `attempts` only advances on a handler decision. With `max_deliveries` set, the
/// broker bounds it: after the job has been delivered `max` times (each lease
/// simply expiring, as a crashed worker leaves it), the next reserve dead-letters
/// it instead of handing it out again.
pub async fn poison_delivery_bound_dead_letters<H: ConfigurableBrokerHarness>(h: &H) {
    let (b, clock) = h
        .build(
            BrokerConfig::new()
                .with_lease(LEASE)
                .with_max_deliveries(MAX),
        )
        .await;
    let poison = lane("poison");
    b.enqueue(job("poison")).await.unwrap();

    // Deliver it `max` times; each lease expires with no resolution.
    for n in 1..=MAX {
        let r = b.reserve(&poison).await.unwrap();
        assert!(
            r.is_some(),
            "delivery {n} of {MAX} must hand the job out (still within the bound)",
        );
        // Lease expires with no ack/retry/fail — the crashed-worker case.
        clock.advance(LEASE + Duration::from_millis(1));
    }

    // The next reserve must dead-letter the job rather than redeliver it.
    let r = b.reserve(&poison).await.unwrap();
    assert!(
        r.is_none(),
        "after {MAX} deliveries the poison job must not be handed out again",
    );
    assert_eq!(
        b.count_dead_letters(&poison).await.unwrap(),
        1,
        "the over-delivered job must be dead-lettered",
    );
}

/// The over-delivery reserve does not merely drop the job that hit its bound — it
/// MUST continue selecting the next eligible job on the lane, so a poison pill
/// cannot stall the good work queued behind it. A broker that dead-letters the
/// bound job but returns `None` (stalling the lane while another job is still
/// eligible) would pass [`poison_delivery_bound_dead_letters`] yet fail here.
///
/// Redelivery *ordering* is implementation-defined (see the broker spec), so this
/// does not assume which of the two crash-looping jobs is delivered on any given
/// pass. It pins the order-independent invariant: `reserve` returns `None` only
/// once *every* job on the lane has been dead-lettered — never while one is still
/// under its delivery bound.
pub async fn poison_skips_to_next_eligible_job<H: ConfigurableBrokerHarness>(h: &H) {
    let (b, clock) = h
        .build(
            BrokerConfig::new()
                .with_lease(LEASE)
                .with_max_deliveries(MAX),
        )
        .await;
    let l = lane("poison");
    // Two distinguishable jobs that both crash-loop (their leases just expire,
    // never acked), so both eventually hit the delivery bound.
    b.enqueue(job("poison")).await.unwrap();
    b.enqueue(NewJob::new(lane("poison"), "good_next", b"g".to_vec(), 3))
        .await
        .unwrap();

    // Reserve and let the lease expire, over and over. Between them the two jobs
    // have 2 * MAX deliveries to burn; a few extra passes cover the over-delivery
    // reserves that move a job to the dead store. Whenever `reserve` returns
    // `None`, BOTH jobs must already be dead-lettered — the lane must never go
    // empty while one is still eligible (the stall the prior helper couldn't catch).
    let mut delivered = 0u32;
    for _ in 0..(2 * MAX + 4) {
        match b.reserve(&l).await.unwrap() {
            Some(_) => {
                delivered += 1;
                clock.advance(LEASE + Duration::from_millis(1));
            }
            None => assert_eq!(
                b.count_dead_letters(&l).await.unwrap(),
                2,
                "reserve returned None, so every job on the lane must already be \
                 dead-lettered — it must not stall while one is still eligible",
            ),
        }
    }

    // Both crash-looping jobs are dead-lettered on their bound, and each was
    // delivered exactly `MAX` times first — the lane kept handing out work
    // (regardless of redelivery order) rather than stalling on the poison pill.
    assert_eq!(
        b.count_dead_letters(&l).await.unwrap(),
        2,
        "both crash-looping jobs are eventually dead-lettered on their bound",
    );
    assert_eq!(
        delivered,
        2 * MAX,
        "each job was delivered exactly MAX times before being dead-lettered",
    );
}

/// With a `unique_key`, the bound must release the key (as `fail` does) so a
/// fresh enqueue with that key creates a new job, while the dead-letter record
/// retains it for a later `requeue`.
pub async fn poison_delivery_bound_releases_unique_key<H: ConfigurableBrokerHarness>(h: &H) {
    let (b, clock) = h
        .build(
            BrokerConfig::new()
                .with_lease(LEASE)
                .with_max_deliveries(MAX),
        )
        .await;
    let l = lane("poison_uk");
    let first = b
        .enqueue(job("poison_uk").with_unique_key("k"))
        .await
        .unwrap();
    for _ in 1..=MAX {
        assert!(b.reserve(&l).await.unwrap().is_some());
        clock.advance(LEASE + Duration::from_millis(1));
    }
    // The over-delivery reserve dead-letters the holder and frees the key.
    assert!(b.reserve(&l).await.unwrap().is_none());

    // A new enqueue with the same key is no longer deduped to the dead job.
    let second = b
        .enqueue(job("poison_uk").with_unique_key("k"))
        .await
        .unwrap();
    assert_ne!(
        first, second,
        "the key must be released when the delivery bound dead-letters the job",
    );
}

/// A single `reserve` must not turn an arbitrarily long run of over-budget jobs
/// into an unbounded sweep: it dead-letters at most [`MAX_DEAD_LETTER_SWEEP`] of
/// them before yielding empty, and the next `reserve` continues where it left off
/// (bounded progress). Without the bound, a lane head full of expired-budget jobs
/// could make one `reserve` walk the entire backlog before returning. Pins the
/// cap's observable effect — the other poison/dead-letter scenarios only check
/// that dead-lettering *happens*, never how many a single reserve may move.
pub async fn poison_sweep_is_bounded_per_reserve<H: ConfigurableBrokerHarness>(h: &H) {
    let cap = u64::from(MAX_DEAD_LETTER_SWEEP);
    let total = cap + 2; // just over one cap, so a second sweep finishes the run
    let (b, clock) = h
        .build(BrokerConfig::new().with_lease(LEASE).with_max_deliveries(1))
        .await;
    let l = lane("sweep");

    // Enqueue `total` distinct jobs, then deliver each exactly once so every job
    // sits at its delivery bound (deliveries == max == 1). Each reserve hands out a
    // distinct still-visible job and leases it; after `total` reserves all are
    // leased, then one clock advance expires every lease at once.
    for _ in 0..total {
        b.enqueue(job("sweep")).await.unwrap();
    }
    for _ in 0..total {
        assert!(
            b.reserve(&l).await.unwrap().is_some(),
            "each freshly enqueued job is handed out once before hitting its bound",
        );
    }
    clock.advance(LEASE + Duration::from_millis(1));

    // Every job is now over budget. A single reserve sweeps them into the dead
    // store, but only up to the cap, then yields empty.
    assert!(
        b.reserve(&l).await.unwrap().is_none(),
        "a reserve facing only over-budget jobs must yield empty",
    );
    assert_eq!(
        b.count_dead_letters(&l).await.unwrap(),
        cap,
        "a single reserve must dead-letter at most MAX_DEAD_LETTER_SWEEP jobs",
    );

    // The next reserve continues the sweep rather than stalling or redoing work.
    assert!(b.reserve(&l).await.unwrap().is_none());
    assert_eq!(
        b.count_dead_letters(&l).await.unwrap(),
        total,
        "the following reserve sweeps the remainder — the cap bounds each call, \
         not the total progress",
    );
}

/// When `max_deliveries` is NOT configured (the default), redelivery is
/// unbounded: a job whose lease repeatedly expires with no resolution is handed
/// out again every time and is never dead-lettered on a delivery count. Pins the
/// negative half of the bounded-redelivery requirement that the bounded
/// scenarios cannot observe.
pub async fn redelivery_unbounded_by_default<H: ConfigurableBrokerHarness>(h: &H) {
    // Default config: no `max_deliveries`.
    let (b, clock) = h.build(BrokerConfig::new().with_lease(LEASE)).await;
    let l = lane("default");
    b.enqueue(job("default")).await.unwrap();

    // Expire the lease far more times than any bound would allow.
    for n in 1..=(MAX + 5) {
        let r = b.reserve(&l).await.unwrap();
        assert!(
            r.is_some(),
            "delivery {n} must keep handing the job out — redelivery is unbounded by default",
        );
        clock.advance(LEASE + Duration::from_millis(1));
        assert_eq!(
            b.count_dead_letters(&l).await.unwrap(),
            0,
            "a job must never be dead-lettered on its delivery count without a bound",
        );
    }
}
