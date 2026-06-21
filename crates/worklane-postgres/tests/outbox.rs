//! `PostgresBroker::enqueue_with_tx` (Transactional Outbox) against a live
//! Postgres. Requires `WORKLANE_POSTGRES_TEST_URL`; skips visibly otherwise so
//! `cargo test` stays green without a database.
//!
//! Verifies that a job enqueued on a *caller-supplied* transaction is visible to
//! the broker only after the caller commits, and is undone on rollback — so a
//! business write and its enqueue commit atomically.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use worklane_core::{Broker, Lane, NewJob};
use worklane_postgres::PostgresBroker;
use worklane_postgres::tokio_postgres::{self, NoTls};

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_url() -> Option<String> {
    std::env::var("WORKLANE_POSTGRES_TEST_URL").ok()
}

fn unique_schema() -> String {
    format!(
        "wl_outbox_{}_{}",
        std::process::id(),
        SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// A raw application connection to the same database (the broker's tables are
/// schema-qualified, so the caller's `search_path` is irrelevant).
async fn app_client(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect app client");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn job() -> NewJob {
    NewJob::new(Lane::default(), "email", b"{}".to_vec(), 3)
}

#[tokio::test]
async fn commit_makes_the_enqueue_visible() {
    let Some(url) = test_url() else {
        eprintln!("skip: set WORKLANE_POSTGRES_TEST_URL to run the postgres outbox test");
        return;
    };
    let broker = Arc::new(
        PostgresBroker::connect_with_schema(&url, &unique_schema())
            .await
            .expect("connect broker"),
    );
    let mut app = app_client(&url).await;

    let tx = app.transaction().await.expect("begin app tx");
    // (the application would also write its business rows on `tx` here)
    let id = broker
        .enqueue_with_tx(&tx, job())
        .await
        .expect("enqueue_with_tx");
    tx.commit().await.expect("commit");

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
    let Some(url) = test_url() else {
        eprintln!("skip: set WORKLANE_POSTGRES_TEST_URL to run the postgres outbox test");
        return;
    };
    let broker = Arc::new(
        PostgresBroker::connect_with_schema(&url, &unique_schema())
            .await
            .expect("connect broker"),
    );
    let mut app = app_client(&url).await;

    let tx = app.transaction().await.expect("begin app tx");
    broker
        .enqueue_with_tx(&tx, job())
        .await
        .expect("enqueue_with_tx");
    tx.rollback().await.expect("rollback"); // as a failed business tx would

    assert!(
        broker
            .reserve(&Lane::default())
            .await
            .expect("reserve")
            .is_none(),
        "a rolled-back enqueue must leave no visible job"
    );
}
