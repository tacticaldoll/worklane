//! End-to-end Claim Check: a large payload is offloaded by the `Client`, stored
//! externally as a compact reference, transparently resolved by the `Worker`
//! before dispatch (the handler sees the full bytes), and its backing blob is
//! deleted once the job is acked.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use worklane::{Client, FilePayloadStore, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wl-cc-e2e-{}-{}",
            std::process::id(),
            DIR_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        TempDir(p)
    }
    /// Number of blob files currently in the store directory.
    fn blob_count(&self) -> usize {
        std::fs::read_dir(&self.0).map(|rd| rd.count()).unwrap_or(0)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Serialize, Deserialize)]
struct Blob {
    data: Vec<u8>,
}

/// Records the length of the payload it actually received, so the test can assert
/// the handler saw the *full* (resolved) bytes, not a reference.
struct VerifyJob {
    seen_len: Arc<AtomicUsize>,
}

#[async_trait]
impl Job for VerifyJob {
    type Payload = Blob;
    type Output = ();
    const KIND: &'static str = "verify";
    async fn run(&self, _ctx: JobContext, payload: Blob) -> HandlerResult<()> {
        self.seen_len.store(payload.data.len(), Ordering::SeqCst);
        // Confirm the bytes round-tripped intact, not just the length.
        if payload.data.iter().all(|&b| b == 42) {
            Ok(())
        } else {
            Err("payload corrupted".into())
        }
    }
}

#[tokio::test]
async fn large_payload_offloads_resolves_and_is_deleted_on_ack() {
    let dir = TempDir::new();
    let store = Arc::new(FilePayloadStore::open(&dir.0).unwrap());
    let broker = Arc::new(InMemoryBroker::new());

    let client = Client::new(broker.clone())
        .with_payload_store(store.clone())
        .with_offload_threshold(8);

    // A payload comfortably over the 8-byte threshold.
    let big = Blob {
        data: vec![42u8; 1000],
    };
    client.enqueue::<VerifyJob>(big).await.unwrap();

    // The payload was offloaded: exactly one blob now sits in the store, and the
    // queue holds only a reference.
    assert_eq!(
        dir.blob_count(),
        1,
        "the large payload must be offloaded to the store"
    );

    let seen_len = Arc::new(AtomicUsize::new(0));
    let mut worker = Worker::new(broker.clone()).with_payload_store(store.clone());
    worker
        .register(VerifyJob {
            seen_len: seen_len.clone(),
        })
        .unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    // The handler saw the full resolved payload.
    assert_eq!(
        seen_len.load(Ordering::SeqCst),
        1000,
        "the handler must receive the full resolved payload, not the reference"
    );
    // It succeeded (no dead letters), and the blob was deleted on ack.
    assert!(
        broker.dead_letters().is_empty(),
        "the job must succeed, not dead-letter"
    );
    assert_eq!(
        dir.blob_count(),
        0,
        "the blob must be deleted once the job is acked"
    );
}

#[tokio::test]
async fn small_payload_stays_inline_and_uses_no_blob() {
    let dir = TempDir::new();
    let store = Arc::new(FilePayloadStore::open(&dir.0).unwrap());
    let broker = Arc::new(InMemoryBroker::new());

    let client = Client::new(broker.clone())
        .with_payload_store(store.clone())
        .with_offload_threshold(64 * 1024);

    // Under the threshold: stays inline, no blob written.
    client
        .enqueue::<VerifyJob>(Blob {
            data: vec![42u8; 4],
        })
        .await
        .unwrap();
    assert_eq!(dir.blob_count(), 0, "a small payload must not be offloaded");

    let seen_len = Arc::new(AtomicUsize::new(usize::MAX));
    let mut worker = Worker::new(broker.clone()).with_payload_store(store.clone());
    worker
        .register(VerifyJob {
            seen_len: seen_len.clone(),
        })
        .unwrap();
    let worker = worker.build().unwrap();
    worker.run_until_idle().await.unwrap();

    assert_eq!(
        seen_len.load(Ordering::SeqCst),
        4,
        "the inline payload reaches the handler"
    );
    assert!(broker.dead_letters().is_empty());
}
