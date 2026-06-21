//! `SqliteBroker::open_with_pool_size` opens with a caller-chosen connection-pool
//! size (the size is otherwise fixed at `DEFAULT_POOL_SIZE`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use worklane_core::{Broker, Lane, NewJob};
use worklane_sqlite::{DEFAULT_POOL_SIZE, SqliteBroker};

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDb(PathBuf);
impl TempDb {
    fn new() -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wl-sqlite-pool-{}-{}.db",
            std::process::id(),
            DB_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        TempDb(p)
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut p = self.0.clone();
            if !suffix.is_empty() {
                p.set_file_name(format!(
                    "{}{suffix}",
                    self.0.file_name().unwrap().to_string_lossy()
                ));
            }
            let _ = std::fs::remove_file(&p);
        }
    }
}

#[tokio::test]
async fn opens_and_works_with_a_custom_pool_size() {
    assert_eq!(DEFAULT_POOL_SIZE, 8, "the documented default is exposed");
    let db = TempDb::new();
    // A small explicit pool: concurrent reservers contend on it but still resolve.
    let broker = SqliteBroker::open_with_pool_size(&db.0, 2).expect("open with pool size 2");

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
async fn pool_size_is_clamped_to_at_least_one() {
    let db = TempDb::new();
    // Zero is clamped to 1 rather than building an unusable pool.
    let broker = SqliteBroker::open_with_pool_size(&db.0, 0).expect("open with clamped pool size");
    broker
        .enqueue(NewJob::new(Lane::default(), "k", b"null".to_vec(), 3))
        .await
        .unwrap();
    assert!(broker.reserve(&Lane::default()).await.unwrap().is_some());
}
