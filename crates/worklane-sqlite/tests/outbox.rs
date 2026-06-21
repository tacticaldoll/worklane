//! `SqliteBroker::enqueue_with_tx` (Transactional Outbox): a job enqueued on a
//! caller-supplied transaction is visible to the broker **only after** the caller
//! commits, and is undone if the caller rolls back — so a business write and its
//! enqueue commit atomically.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use worklane_core::{Broker, Lane, NewJob};
use worklane_sqlite::SqliteBroker;
use worklane_sqlite::rusqlite::Connection;

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temp database path that deletes itself (and its WAL sidecars) on drop.
/// Mirrors the helper in `broker_contract_file.rs` (no `tempfile` dependency).
struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "wl-sqlite-outbox-{}-{}.db",
            std::process::id(),
            DB_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        TempDb { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut p = self.path.clone();
            if !suffix.is_empty() {
                p.set_file_name(format!(
                    "{}{suffix}",
                    self.path.file_name().unwrap().to_string_lossy()
                ));
            }
            let _ = std::fs::remove_file(&p);
        }
    }
}

fn job() -> NewJob {
    NewJob::new(Lane::default(), "email", b"{}".to_vec(), 3)
}

#[tokio::test]
async fn commit_makes_the_enqueue_visible() {
    let db = TempDb::new();
    let broker = SqliteBroker::open(&db.path).expect("open broker");

    // The application's own connection to the same database.
    let mut app = Connection::open(&db.path).expect("app connection");

    let id = {
        let tx = app.transaction().expect("begin app tx");
        // (the application would also write its business rows on `tx` here)
        let id = broker.enqueue_with_tx(&tx, job()).expect("enqueue_with_tx");
        tx.commit().expect("commit");
        id
    };

    let reserved = broker
        .reserve(&Lane::default())
        .await
        .expect("reserve")
        .expect("a committed enqueue must be reservable");
    assert_eq!(
        reserved.envelope.id, id,
        "the committed job is the one enqueued"
    );
}

#[tokio::test]
async fn rollback_undoes_the_enqueue() {
    let db = TempDb::new();
    let broker = SqliteBroker::open(&db.path).expect("open broker");

    let mut app = Connection::open(&db.path).expect("app connection");

    {
        let tx = app.transaction().expect("begin app tx");
        broker.enqueue_with_tx(&tx, job()).expect("enqueue_with_tx");
        // Drop `tx` without committing: it rolls back, as a failed business
        // transaction would.
    }

    assert!(
        broker
            .reserve(&Lane::default())
            .await
            .expect("reserve")
            .is_none(),
        "a rolled-back enqueue must leave no visible job"
    );
}
