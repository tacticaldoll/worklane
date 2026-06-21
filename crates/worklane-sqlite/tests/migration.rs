//! Schema baseline + version-gating, end-to-end through `SqliteBroker::open`.
//!
//! worklane is pre-1.0 with no in-place migration: a fresh database is created at
//! the baseline, reopening is idempotent, and a database from a different schema
//! generation is rejected (drop and recreate).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use worklane_core::{Broker, Lane, NewJob};
use worklane_sqlite::SqliteBroker;
use worklane_sqlite::rusqlite::Connection;

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "wl-sqlite-schema-{}-{}.db",
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

#[tokio::test]
async fn fresh_database_opens_and_works() {
    let db = TempDb::new();
    let broker = SqliteBroker::open(&db.path).expect("open fresh db");
    let id = broker
        .enqueue(NewJob::new(Lane::default(), "k", b"null".to_vec(), 3))
        .await
        .unwrap();
    let r = broker
        .reserve(&Lane::default())
        .await
        .unwrap()
        .expect("reservable");
    assert_eq!(r.envelope.id, id);
}

#[tokio::test]
async fn fresh_database_has_receipt_index() {
    let db = TempDb::new();
    SqliteBroker::open(&db.path).expect("open fresh db");
    let conn = Connection::open(&db.path).expect("raw open");
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master
                WHERE type = 'index' AND name = 'jobs_receipt'
            )",
            [],
            |r| r.get(0),
        )
        .expect("query sqlite_master");
    assert!(exists, "fresh baseline must include jobs_receipt index");
}

#[tokio::test]
async fn reopening_is_idempotent_and_preserves_jobs() {
    let db = TempDb::new();
    let id = {
        let broker = SqliteBroker::open(&db.path).expect("open #1");
        broker
            .enqueue(NewJob::new(Lane::default(), "k", b"null".to_vec(), 3))
            .await
            .unwrap()
    };
    // Reopen the same file: migrate is a no-op and the persisted job survives.
    let broker = SqliteBroker::open(&db.path).expect("reopen");
    let r = broker
        .reserve(&Lane::default())
        .await
        .unwrap()
        .expect("reservable after reopen");
    assert_eq!(r.envelope.id, id);
}

#[tokio::test]
async fn a_database_from_a_different_schema_generation_is_rejected() {
    let db = TempDb::new();
    // Create the baseline, then stamp a foreign version (e.g. an old ladder
    // version or a future one) directly.
    SqliteBroker::open(&db.path).expect("create baseline");
    {
        let conn = Connection::open(&db.path).expect("raw open");
        conn.pragma_update(None, "user_version", 99i64)
            .expect("stamp foreign version");
    }
    // Pre-1.0 there is no migration: opening it must error, not silently proceed.
    assert!(
        SqliteBroker::open(&db.path).is_err(),
        "a non-baseline schema version must be rejected"
    );
}
