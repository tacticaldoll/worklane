use std::time::Duration;

use worklane_core::{DeadLetterStore, RetentionPolicy};

use crate::{BrokerConfig, ConfigurableBrokerHarness};

use super::{dead_letter, job, lane};

const MAX_COUNT: u64 = 2;
const MAX_AGE: Duration = Duration::from_secs(60);

/// With no retention policy (the default), the dead-letter store is unbounded:
/// every dead-lettered job stays readable and counted, no matter how many a lane
/// accumulates. Pins the no-policy contract directly rather than relying on the
/// general dead-letter scenarios to imply it.
pub async fn retention_no_policy_retains_everything<H: ConfigurableBrokerHarness>(h: &H) {
    // Default config: no `RetentionPolicy`.
    let (b, _clock) = h.build(BrokerConfig::new()).await;
    let l = lane("retention_none");
    let n = 6;
    for i in 0..n {
        dead_letter(b.as_ref(), job("retention_none"), &format!("e{i}")).await;
    }
    assert_eq!(
        b.count_dead_letters(&l).await.unwrap(),
        n,
        "with no policy the count must equal every dead-lettered job"
    );
    let read = b.read_dead_letters(&l, n as usize + 10).await.unwrap();
    assert_eq!(
        read.len(),
        n as usize,
        "with no policy every dead-letter record must remain readable"
    );
}

/// `max_count` bounds the dead-letter store to the newest N records per lane, and
/// leaves other lanes untouched.
pub async fn retention_max_count_bounds_dead_letters<H: ConfigurableBrokerHarness>(h: &H) {
    let (b, _clock) = h
        .build(BrokerConfig::new().with_retention(RetentionPolicy::new().with_max_count(MAX_COUNT)))
        .await;
    let counted = lane("retention_count");
    for i in 0..(MAX_COUNT + 3) {
        dead_letter(b.as_ref(), job("retention_count"), &format!("e{i}")).await;
    }
    assert_eq!(
        b.count_dead_letters(&counted).await.unwrap(),
        MAX_COUNT,
        "max_count must bound the dead-letter store to the newest {MAX_COUNT}"
    );

    // The bound must keep the *newest* records, not merely some N: the three
    // oldest (`e0`,`e1`,`e2`) were evicted and `e3..=e{MAX_COUNT+2}` remain. A
    // backend that kept the oldest N would still pass the count check above, so
    // pin which records survived by their retained errors.
    let retained: std::collections::HashSet<String> = b
        .read_dead_letters(&counted, MAX_COUNT as usize + 10)
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.error)
        .collect();
    let expected: std::collections::HashSet<String> =
        (3..(MAX_COUNT + 3)).map(|i| format!("e{i}")).collect();
    assert_eq!(
        retained, expected,
        "max_count must retain the newest {MAX_COUNT} records and evict the oldest"
    );

    // A different lane carries its own count and is unaffected by this one's cap.
    let other = lane("retention_other");
    dead_letter(b.as_ref(), job("retention_other"), "x").await;
    assert_eq!(
        b.count_dead_letters(&other).await.unwrap(),
        1,
        "a different lane is not pruned by another lane's max_count"
    );
}

/// `max_age` drops a lane's aged dead-letter records when the lane next fails a
/// job.
pub async fn retention_max_age_drops_on_fail<H: ConfigurableBrokerHarness>(h: &H) {
    let (b, clock) = h
        .build(BrokerConfig::new().with_retention(RetentionPolicy::new().with_max_age(MAX_AGE)))
        .await;
    let aged = lane("retention_age");
    // Start at a nonzero base so the first record's timestamp is distinguishable
    // from the zero default of a freshly-migrated row.
    clock.advance(Duration::from_secs(1));
    dead_letter(b.as_ref(), job("retention_age"), "old").await;
    assert_eq!(
        b.count_dead_letters(&aged).await.unwrap(),
        1,
        "the first dead-letter is retained while still within max_age"
    );

    // Advance past max_age, then fail another job on the lane: the now-aged first
    // record is pruned, leaving only the fresh one.
    clock.advance(MAX_AGE + Duration::from_secs(1));
    dead_letter(b.as_ref(), job("retention_age"), "fresh").await;
    assert_eq!(
        b.count_dead_letters(&aged).await.unwrap(),
        1,
        "the aged record is dropped on the next fail, leaving only the fresh one"
    );
}

/// `max_age` pruning is lazy and per-lane: it fires only when a lane next fails
/// a job, so a quiet lane that stops failing retains its aged dead-letter records
/// indefinitely and still counts them — `max_age` is not a background TTL. Pins
/// the lingering half of the lazy contract that
/// [`retention_max_age_drops_on_fail`] (which always fails again) cannot observe.
pub async fn retention_max_age_idle_lane_lingers<H: ConfigurableBrokerHarness>(h: &H) {
    let (b, clock) = h
        .build(BrokerConfig::new().with_retention(RetentionPolicy::new().with_max_age(MAX_AGE)))
        .await;
    let idle = lane("retention_idle");
    clock.advance(Duration::from_secs(1));
    dead_letter(b.as_ref(), job("retention_idle"), "aged").await;

    // Age the record well past max_age, but never fail on this lane again.
    clock.advance(MAX_AGE * 10);

    // No background TTL: the aged record lingers and is still counted and read.
    assert_eq!(
        b.count_dead_letters(&idle).await.unwrap(),
        1,
        "an idle lane retains its aged dead-letter — max_age is not a background TTL"
    );
    assert_eq!(
        b.read_dead_letters(&idle, 10).await.unwrap().len(),
        1,
        "the aged record is still readable on a quiet lane"
    );

    // Pruning is per-lane: failing on a different lane does not prune the idle one.
    dead_letter(b.as_ref(), job("retention_other_idle"), "x").await;
    assert_eq!(
        b.count_dead_letters(&idle).await.unwrap(),
        1,
        "another lane's fail must not prune the idle lane's aged record"
    );
}

/// `max_age` and `max_count` configured together compose: a record is dropped
/// when it is either too old OR over the count cap. Exercises the interaction the
/// single-bound scenarios cannot, ensuring one bound does not disable the other.
pub async fn retention_max_age_and_count_combined<H: ConfigurableBrokerHarness>(h: &H) {
    let (b, clock) = h
        .build(
            BrokerConfig::new().with_retention(
                RetentionPolicy::new()
                    .with_max_age(MAX_AGE)
                    .with_max_count(MAX_COUNT),
            ),
        )
        .await;
    let l = lane("retention_combo");
    clock.advance(Duration::from_secs(1));

    // Three fresh dead-letters within max_age: the count cap (2) prunes the
    // oldest, proving max_count still bites when max_age would retain all three.
    for i in 0..3 {
        dead_letter(b.as_ref(), job("retention_combo"), &format!("c{i}")).await;
    }
    assert_eq!(
        b.count_dead_letters(&l).await.unwrap(),
        MAX_COUNT,
        "max_count must still bound the store when max_age alone would keep all records"
    );

    // Age every survivor past max_age, then fail once more: the age bound drops
    // the aged survivors, proving max_age still bites under a max_count cap. Only
    // the single fresh record remains.
    clock.advance(MAX_AGE + Duration::from_secs(1));
    dead_letter(b.as_ref(), job("retention_combo"), "fresh").await;
    let retained: std::collections::HashSet<String> = b
        .read_dead_letters(&l, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.error)
        .collect();
    assert_eq!(
        retained,
        std::collections::HashSet::from(["fresh".to_string()]),
        "max_age must still drop aged records under a max_count cap, leaving only the fresh one"
    );
}
