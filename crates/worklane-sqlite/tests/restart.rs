//! Restart durability: a file-backed `SqliteBroker` reopened by a fresh broker
//! instance (simulating a process restart) must still see its persisted jobs and
//! honour their schedules. This is the property the default `WallClock` (Unix
//! epoch) provides and the old process-local `SystemClock` did not.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use worklane_core::{Broker, NewJob};
use worklane_sqlite::SqliteBroker;
use worklane_test::ManualClock;

/// A fresh temp DB path for `name`, with any stale files removed first.
fn temp_db(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "worklane-restart-{}-{}.db",
        name,
        std::process::id()
    ));
    for ext in ["db", "db-wal", "db-shm"] {
        let _ = std::fs::remove_file(path.with_extension(ext));
    }
    let _ = std::fs::remove_file(&path);
    path
}

fn job() -> NewJob {
    NewJob::new("default", "ok", b"null".to_vec(), 3)
}

/// A job enqueued before a restart is still reservable after reopening the same
/// database with a fresh broker (and thus a fresh `WallClock`).
#[tokio::test]
async fn persisted_job_survives_restart() {
    let path = temp_db("survive");
    {
        let broker = SqliteBroker::open(&path).expect("open");
        broker.enqueue(job()).await.unwrap();
    } // broker dropped: simulate process exit

    // "Restart": a brand-new broker instance over the same file.
    let broker = SqliteBroker::open(&path).expect("reopen");
    let reserved = broker.reserve("default").await.unwrap();
    assert!(
        reserved.is_some(),
        "a persisted job must survive a restart and remain reservable"
    );

    drop(broker);
    let _ = std::fs::remove_file(&path);
}

/// A future retry delay persisted before a restart is honoured after it: the job
/// stays hidden until the delay elapses, then becomes reservable. A shared
/// `ManualClock` gives both broker instances the same stable epoch, the property
/// `WallClock` guarantees in production, so the assertion is deterministic.
#[tokio::test]
async fn persisted_retry_delay_survives_restart() {
    let path = temp_db("retry");
    let clock = Arc::new(ManualClock::new());
    let delay = Duration::from_secs(60);
    {
        let broker = SqliteBroker::open(&path)
            .expect("open")
            .with_clock(clock.clone());
        broker.enqueue(job()).await.unwrap();
        let r = broker.reserve("default").await.unwrap().expect("job");
        broker.retry(r.receipt, delay).await.unwrap(); // available_at = now + delay
    }

    // Restart before the delay elapses (same stable epoch).
    let broker = SqliteBroker::open(&path)
        .expect("reopen")
        .with_clock(clock.clone());
    assert!(
        broker.reserve("default").await.unwrap().is_none(),
        "the retried job must stay hidden before its delay, even across a restart"
    );

    clock.advance(delay);
    assert!(
        broker.reserve("default").await.unwrap().is_some(),
        "after the delay elapses the job is reservable again, across the restart"
    );

    drop(broker);
    let _ = std::fs::remove_file(&path);
}
