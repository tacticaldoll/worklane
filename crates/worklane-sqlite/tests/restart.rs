//! Restart durability: a file-backed `SqliteBroker` reopened by a fresh broker
//! instance (simulating a process restart) must still see its persisted jobs and
//! honour their schedules. This is the property the default `WallClock` (Unix
//! epoch) provides and the old process-local `SystemClock` did not.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use worklane_core::{Broker, DeadLetterStore, NewJob};
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
    NewJob::new("default".parse().unwrap(), "ok", b"null".to_vec(), 3)
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
    let reserved = broker.reserve(&"default".parse().unwrap()).await.unwrap();
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
        let r = broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .expect("job");
        broker.retry(r.receipt, delay).await.unwrap(); // available_at = now + delay
    }

    // Restart before the delay elapses (same stable epoch).
    let broker = SqliteBroker::open(&path)
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

    drop(broker);
    let _ = std::fs::remove_file(&path);
}

/// A dead-lettered job persisted before a restart is readable after reopening
/// the database, and can be requeued back to its lane — the durable
/// reconstruction the dead-letter read/requeue contract requires.
#[tokio::test]
async fn dead_letter_survives_restart_and_requeues() {
    let path = temp_db("deadletter");
    let id = {
        let broker = SqliteBroker::open(&path).expect("open");
        let id = broker.enqueue(job()).await.unwrap();
        let r = broker
            .reserve(&"default".parse().unwrap())
            .await
            .unwrap()
            .expect("job");
        broker.fail(r.receipt, "boom".to_string()).await.unwrap();
        id
    }; // broker dropped: simulate process exit

    // "Restart": a brand-new broker instance over the same file.
    let broker = SqliteBroker::open(&path).expect("reopen");
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
    assert!(
        broker
            .read_dead_letters(&"default".parse().unwrap(), 10)
            .await
            .unwrap()
            .is_empty(),
        "a requeued job leaves the dead-letter store"
    );

    drop(broker);
    let _ = std::fs::remove_file(&path);
}
