//! Restart durability: a `PostgresBroker` whose handle is dropped and then
//! reopened against the same database and schema (simulating a process restart)
//! must still see its persisted jobs, honour their schedules, and reconstruct
//! its dead-letter store. This is the durable-broker arm of the broker spec's
//! restart-durability requirement.
//!
//! Requires a reachable Postgres: set `WORKLANE_POSTGRES_TEST_URL`. When unset
//! each test visibly skips so `cargo test` stays green without a database. Each
//! test pins a unique schema and shares it across both broker instances.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use worklane_core::{Broker, DeadLetterStore, NewJob};
use worklane_postgres::PostgresBroker;
use worklane_test::ManualClock;

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_POSTGRES_TEST_URL").ok()
}

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_schema() -> String {
    format!(
        "wlrestart_{}_{}",
        std::process::id(),
        SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn job() -> NewJob {
    NewJob::new("default".parse().unwrap(), "ok", b"null".to_vec(), 3)
}

/// A job enqueued before a restart is still reservable after reopening the same
/// schema with a fresh broker instance (and thus a fresh `WallClock`).
#[tokio::test]
async fn persisted_job_survives_restart() {
    let Some(url) = test_url() else {
        eprintln!("SKIP persisted_job_survives_restart: set WORKLANE_POSTGRES_TEST_URL");
        return;
    };
    let schema = unique_schema();
    {
        let broker = PostgresBroker::connect_with_schema(&url, &schema)
            .await
            .expect("open");
        broker.enqueue(job()).await.unwrap();
    } // broker dropped: simulate process exit

    let broker = PostgresBroker::connect_with_schema(&url, &schema)
        .await
        .expect("reopen");
    assert!(
        broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .is_some(),
        "a persisted job must survive a restart and remain reservable"
    );
}

/// A future retry delay persisted before a restart is honoured after it: the job
/// stays hidden until the delay elapses, then becomes reservable. A shared
/// `ManualClock` gives both broker instances the same stable epoch.
#[tokio::test]
async fn persisted_retry_delay_survives_restart() {
    let Some(url) = test_url() else {
        eprintln!("SKIP persisted_retry_delay_survives_restart: set WORKLANE_POSTGRES_TEST_URL");
        return;
    };
    let schema = unique_schema();
    let clock = Arc::new(ManualClock::new());
    let delay = Duration::from_secs(60);
    {
        let broker = PostgresBroker::connect_with_schema(&url, &schema)
            .await
            .expect("open")
            .with_clock(clock.clone());
        broker.enqueue(job()).await.unwrap();
        let r = broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .expect("job");
        broker.retry(r.receipt, delay).await.unwrap();
    }

    let broker = PostgresBroker::connect_with_schema(&url, &schema)
        .await
        .expect("reopen")
        .with_clock(clock.clone());
    assert!(
        broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .is_none(),
        "the retried job must stay hidden before its delay, even across a restart"
    );
    clock.advance(delay);
    assert!(
        broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .is_some(),
        "after the delay elapses the job is reservable again, across the restart"
    );
}

/// A dead-lettered job persisted before a restart is readable after reopening the
/// schema, and can be requeued back to its lane.
#[tokio::test]
async fn dead_letter_survives_restart_and_requeues() {
    let Some(url) = test_url() else {
        eprintln!("SKIP dead_letter_survives_restart_and_requeues: set WORKLANE_POSTGRES_TEST_URL");
        return;
    };
    let schema = unique_schema();
    let id = {
        let broker = PostgresBroker::connect_with_schema(&url, &schema)
            .await
            .expect("open");
        let id = broker.enqueue(job()).await.unwrap();
        let r = broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .expect("job");
        broker.fail(r.receipt, "boom".to_string()).await.unwrap();
        id
    };

    let broker = PostgresBroker::connect_with_schema(&url, &schema)
        .await
        .expect("reopen");
    let dead = broker
        .read_dead_letters(&"default".parse().unwrap(), 10)
        .await
        .unwrap();
    assert_eq!(
        dead.len(),
        1,
        "the dead-letter record must survive a restart"
    );
    assert_eq!(dead[0].error, "boom", "the error survives the restart");
    assert_eq!(
        dead[0].envelope.id, id,
        "the envelope id survives the restart"
    );

    broker.requeue(id).await.expect("requeue after restart");
    assert!(
        broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .is_some(),
        "a requeued job must be reservable after a restart"
    );
}
