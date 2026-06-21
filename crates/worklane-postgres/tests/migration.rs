//! Schema baseline + version-gating against a live Postgres. Requires
//! `WORKLANE_POSTGRES_TEST_URL`; skips visibly otherwise.
//!
//! worklane is pre-1.0 with no in-place migration: a fresh schema is created at
//! the baseline, reconnecting is idempotent, and a schema from a different
//! generation is rejected (drop and recreate).

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
        "wl_schema_{}_{}",
        std::process::id(),
        SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

async fn raw_client(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn job() -> NewJob {
    NewJob::new(Lane::default(), "k", b"null".to_vec(), 3)
}

#[tokio::test]
async fn fresh_schema_opens_and_works() {
    let Some(url) = test_url() else {
        eprintln!("skip: set WORKLANE_POSTGRES_TEST_URL");
        return;
    };
    let broker = PostgresBroker::connect_with_schema(&url, &unique_schema())
        .await
        .expect("create baseline");
    let id = broker.enqueue(job()).await.unwrap();
    let r = broker
        .reserve(&Lane::default())
        .await
        .unwrap()
        .expect("reservable");
    assert_eq!(r.envelope.id, id);
}

#[tokio::test]
async fn reconnecting_is_idempotent_and_preserves_jobs() {
    let Some(url) = test_url() else {
        eprintln!("skip: set WORKLANE_POSTGRES_TEST_URL");
        return;
    };
    let schema = unique_schema();
    let id = {
        let broker = PostgresBroker::connect_with_schema(&url, &schema)
            .await
            .expect("connect #1");
        broker.enqueue(job()).await.unwrap()
    };
    // Reconnect to the same schema: init is a no-op and the job persists.
    let broker = Arc::new(
        PostgresBroker::connect_with_schema(&url, &schema)
            .await
            .expect("reconnect"),
    );
    let r = broker
        .reserve(&Lane::default())
        .await
        .unwrap()
        .expect("reservable after reconnect");
    assert_eq!(r.envelope.id, id);
}

#[tokio::test]
async fn a_schema_from_a_different_generation_is_rejected() {
    let Some(url) = test_url() else {
        eprintln!("skip: set WORKLANE_POSTGRES_TEST_URL");
        return;
    };
    let schema = unique_schema();
    PostgresBroker::connect_with_schema(&url, &schema)
        .await
        .expect("create baseline");
    // Stamp a foreign version directly.
    let client = raw_client(&url).await;
    client
        .execute(
            &format!("UPDATE \"{schema}\".meta SET schema_version = 99"),
            &[],
        )
        .await
        .expect("stamp foreign version");
    // Pre-1.0 has no migration: connecting must error, not silently proceed.
    assert!(
        PostgresBroker::connect_with_schema(&url, &schema)
            .await
            .is_err(),
        "a non-baseline schema version must be rejected"
    );
}
