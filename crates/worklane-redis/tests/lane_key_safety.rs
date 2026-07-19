//! Redis-specific behavior: a lane that is a valid `Lane` but cannot be safely
//! embedded in the redis key scheme (it contains `:` or a glob metacharacter) is
//! rejected at every lane-to-key entry point, with no side effects. This is the
//! `add-redis-lane-key-safety` change — see the `broker` capability's
//! "Broker-specific lane rejection" requirement.
//!
//! Like the conformance suite, these require a reachable Redis via
//! `WORKLANE_REDIS_TEST_URL`; without it each test visibly skips.

use std::sync::atomic::{AtomicU64, Ordering};

use worklane_core::{Broker, DeadLetterStore, Lane, NewJob};
use worklane_redis::RedisBroker;

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_REDIS_TEST_URL").ok()
}

static NS_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_namespace() -> String {
    format!(
        "wltest-lks:{}:{}",
        std::process::id(),
        NS_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

async fn connect(url: &str) -> RedisBroker {
    RedisBroker::connect_with_namespace(url, &unique_namespace())
        .await
        .expect("connect to test redis")
}

fn job(lane: Lane) -> NewJob {
    NewJob::new(lane, "ok", b"null".to_vec(), 3)
}

/// A valid `Lane` that is unsafe in the redis key scheme.
fn unsafe_lane(name: &str) -> Lane {
    Lane::try_from(name).expect("portable Lane validation allows this name")
}

macro_rules! skip_without_redis {
    ($name:literal) => {
        match test_url() {
            Some(url) => url,
            None => {
                eprintln!(concat!(
                    "SKIP ",
                    $name,
                    ": set WORKLANE_REDIS_TEST_URL to run the redis lane-key-safety tests"
                ));
                return;
            }
        }
    };
}

#[tokio::test]
async fn enqueue_rejects_unsafe_lane() {
    let url = skip_without_redis!("enqueue_rejects_unsafe_lane");
    let b = connect(&url).await;
    for name in [":colon", "a:b", "glob*", "que?", "set[x]"] {
        let err = b.enqueue(job(unsafe_lane(name))).await;
        assert!(
            err.is_err(),
            "enqueue to redis-unsafe lane {name:?} must be rejected"
        );
    }
}

#[tokio::test]
async fn reserve_rejects_unsafe_lane() {
    let url = skip_without_redis!("reserve_rejects_unsafe_lane");
    let b = connect(&url).await;
    assert!(
        b.reserve(&unsafe_lane("a:b")).await.is_err(),
        "reserve on a redis-unsafe lane must be rejected"
    );
    assert!(
        b.reserve(&unsafe_lane("a*")).await.is_err(),
        "reserve on a glob-containing lane must be rejected"
    );
}

#[tokio::test]
async fn read_dead_letters_rejects_unsafe_lane() {
    let url = skip_without_redis!("read_dead_letters_rejects_unsafe_lane");
    let b = connect(&url).await;
    assert!(
        b.read_dead_letters(&unsafe_lane("a:b"), 10).await.is_err(),
        "read_dead_letters on a redis-unsafe lane must be rejected"
    );
}

#[tokio::test]
async fn rejection_has_no_side_effects_and_is_lane_local() {
    let url = skip_without_redis!("rejection_has_no_side_effects_and_is_lane_local");
    let b = connect(&url).await;
    let safe = Lane::try_from("default").unwrap();

    // A job on a safe lane.
    b.enqueue(job(safe.clone())).await.unwrap();
    // A rejected enqueue on an unsafe lane stores nothing and must not disturb it.
    assert!(b.enqueue(job(unsafe_lane("a:b"))).await.is_err());

    let r = b.reserve(&safe).await.unwrap();
    assert!(
        r.is_some(),
        "the safe lane's job survives a rejected enqueue on another lane"
    );
}

#[tokio::test]
async fn safe_lane_with_allowed_special_chars_round_trips() {
    let url = skip_without_redis!("safe_lane_with_allowed_special_chars_round_trips");
    let b = connect(&url).await;
    // Hyphens, underscores, dots, digits are not reserved — these stay usable.
    let lane = Lane::try_from("a-b_c.1").unwrap();

    let id = b.enqueue(job(lane.clone())).await.unwrap();
    let r = b
        .reserve(&lane)
        .await
        .unwrap()
        .expect("safe lane job is reservable");
    b.fail(r.receipt, "boom".to_string()).await.unwrap();

    let dead = b.read_dead_letters(&lane, 10).await.unwrap();
    assert_eq!(
        dead.len(),
        1,
        "dead-letter read works on a safe special lane"
    );
    assert_eq!(dead[0].envelope.id, id);

    b.requeue(id).await.unwrap();
    assert!(
        b.reserve(&lane).await.unwrap().is_some(),
        "requeue restores the job on its original safe lane"
    );
}

#[tokio::test]
async fn batch_enqueue_rejects_unsafe_lane_atomically() {
    let url = skip_without_redis!("batch_enqueue_rejects_unsafe_lane_atomically");
    let b = connect(&url).await;
    let safe = Lane::try_from("default").unwrap();

    let jobs = vec![
        job(safe.clone()),
        job(unsafe_lane("a:b")),
        job(safe.clone()),
    ];

    let err = b
        .batch_enqueue()
        .expect("Redis broker supports batch enqueue")
        .enqueue_batch(jobs)
        .await;
    assert!(err.is_err(), "batch with an unsafe lane must be rejected");

    let r = b.reserve(&safe).await.unwrap();
    assert!(
        r.is_none(),
        "the entire batch must roll back and leave no jobs if an unsafe lane aborts it"
    );
}
