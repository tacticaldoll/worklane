//! Concurrent batch enqueue must not deadlock when two batches list overlapping
//! unique keys in opposite order. Before the fix the Postgres broker claimed the
//! `unique_keys` rows in caller order, so opposing batches could form a
//! lock-ordering cycle and deadlock (Postgres aborts one with SQLSTATE 40P01).
//! The fix acquires the keys via advisory locks in sorted order, so no cycle can
//! form. Requires `WORKLANE_POSTGRES_TEST_URL`; skips cleanly when unset.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use worklane_core::{Broker, NewJob};
use worklane_postgres::PostgresBroker;

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_POSTGRES_TEST_URL").ok()
}

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_schema() -> String {
    format!(
        "wlbatchdl_{}_{}",
        std::process::id(),
        SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn job_key(key: &str) -> NewJob {
    NewJob::new("default".parse().unwrap(), "ok", b"null".to_vec(), 3).with_unique_key(key)
}

/// Fifty rounds of two genuinely concurrent batches listing the same two keys in
/// opposite order. Every batch must succeed (no deadlock), and each round's two
/// keys must dedup to exactly one live job apiece.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_opposing_batches_do_not_deadlock() {
    let Some(url) = test_url() else {
        eprintln!(
            "SKIP concurrent_opposing_batches_do_not_deadlock: set WORKLANE_POSTGRES_TEST_URL"
        );
        return;
    };
    let schema = unique_schema();
    let broker = Arc::new(
        PostgresBroker::connect_with_schema(&url, &schema)
            .await
            .expect("open"),
    );

    const ROUNDS: usize = 50;
    for round in 0..ROUNDS {
        let ka = format!("a-{round}");
        let kb = format!("b-{round}");
        let (b1, b2) = (broker.clone(), broker.clone());
        let (ka1, kb1) = (ka.clone(), kb.clone());
        let (ka2, kb2) = (ka.clone(), kb.clone());
        let t1 =
            tokio::spawn(async move { b1.enqueue_batch(vec![job_key(&ka1), job_key(&kb1)]).await });
        let t2 =
            tokio::spawn(async move { b2.enqueue_batch(vec![job_key(&kb2), job_key(&ka2)]).await });
        let (r1, r2) = tokio::join!(t1, t2);
        r1.expect("task 1 panicked")
            .expect("batch [a,b] must not deadlock against [b,a]");
        r2.expect("task 2 panicked")
            .expect("batch [b,a] must not deadlock against [a,b]");
    }

    let mut reserved = 0;
    while broker
        .reserve(&"default".parse().unwrap())
        .await
        .unwrap()
        .is_some()
    {
        reserved += 1;
    }
    assert_eq!(
        reserved,
        ROUNDS * 2,
        "each shared key must dedup to exactly one live job across the concurrent batches",
    );
}
