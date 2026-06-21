//! Claim Check support for the facade: a filesystem [`PayloadStore`] and a
//! [`ClaimCheck`] helper that offloads oversized payloads and resolves them back.
//!
//! Wire a `ClaimCheck` into a [`Client`](crate::Client) (offload on enqueue) and a
//! [`Worker`](crate::Worker) (resolve on dispatch, delete on ack) to move large
//! payloads out of the queue transparently. See the crate docs for
//! configuration and lifecycle guidance.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use worklane_core::{Error, JobId, PayloadStore, Result, claim_check};

/// The default offload threshold: payloads larger than this are stored externally.
/// 64 KiB comfortably inlines ordinary jobs while offloading genuinely large ones.
pub const DEFAULT_OFFLOAD_THRESHOLD: usize = 64 * 1024;

/// A filesystem-backed [`PayloadStore`] — the reference implementation.
///
/// Each blob is one file under a base directory, named by a random key. Blocking
/// filesystem calls run on Tokio's blocking pool (matching the durable backends),
/// so they never stall the async runtime. Suitable for a single host or a shared
/// volume; object-storage stores (S3, GCS) are separate implementations of the
/// same trait.
#[derive(Clone)]
pub struct FilePayloadStore {
    dir: Arc<PathBuf>,
}

impl FilePayloadStore {
    /// Open (creating it if absent) a payload store rooted at `dir`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::Broker(format!("payload store dir {dir:?}: {e}")))?;
        Ok(FilePayloadStore { dir: Arc::new(dir) })
    }

    /// Map a key to its on-disk path, **rejecting any key this store could not have
    /// minted**. `put` mints keys as `JobId` UUIDs, so a key that does not parse as
    /// one is not ours. This is the store's security boundary: `get`/`delete` keys
    /// come from a job's payload reference, which an untrusted producer controls —
    /// a forged key like `../../etc/passwd` or an absolute path would otherwise
    /// `join` outside the store directory (an absolute path replaces it entirely).
    /// A UUID contains only hex digits and hyphens — no path separator, no `..` —
    /// so a key that parses as one is provably safe to `join`.
    fn safe_path(&self, key: &str) -> Result<PathBuf> {
        JobId::from_str(key).map_err(|_| {
            Error::Broker(format!(
                "claim-check key {key:?} is not a valid store key (expected a UUID); \
                 refusing to map it to a filesystem path"
            ))
        })?;
        Ok(self.dir.join(key))
    }
}

#[async_trait]
impl PayloadStore for FilePayloadStore {
    async fn put(&self, payload: &[u8]) -> Result<String> {
        // A random, filesystem-safe key. `JobId` is a UUID (hyphenated hex), so it
        // never contains a path separator and cannot escape the base directory.
        let key = JobId::new().to_string();
        let path = self.safe_path(&key)?;
        let bytes = payload.to_vec();
        tokio::task::spawn_blocking(move || std::fs::write(&path, &bytes))
            .await
            .map_err(|e| Error::Broker(format!("payload store join: {e}")))?
            .map_err(|e| Error::Broker(format!("payload store write: {e}")))?;
        Ok(key)
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.safe_path(key)?;
        tokio::task::spawn_blocking(move || match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Broker(format!("payload store read: {e}"))),
        })
        .await
        .map_err(|e| Error::Broker(format!("payload store join: {e}")))?
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.safe_path(key)?;
        tokio::task::spawn_blocking(move || match std::fs::remove_file(&path) {
            // Absent is fine: delete is idempotent (a redelivered ack must not fail).
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Broker(format!("payload store delete: {e}"))),
        })
        .await
        .map_err(|e| Error::Broker(format!("payload store join: {e}")))?
    }
}

/// Offloads oversized payloads to a [`PayloadStore`] and resolves the references
/// back, implementing the Claim Check pattern.
///
/// - [`offload`](ClaimCheck::offload): on enqueue, replace a payload larger than
///   the threshold with a compact reference (small payloads pass through).
/// - [`fetch`](ClaimCheck::fetch): on dispatch, resolve a reference back to the
///   real bytes (non-references pass through).
/// - [`delete`](ClaimCheck::delete): after a job succeeds, drop the backing blob.
///
/// **Lifecycle / orphans.** A blob is deleted when its job is acked. A
/// *dead-lettered* job keeps its blob, so the dead letter stays inspectable and
/// requeueable — which means purging dead letters (or losing a job before ack)
/// can leave an orphan blob. Treat the store as needing an occasional sweep, the
/// same way the dead-letter store does.
#[derive(Clone)]
pub struct ClaimCheck<P: PayloadStore> {
    store: Arc<P>,
    threshold: usize,
}

impl<P: PayloadStore> ClaimCheck<P> {
    /// Build a claim check over `store` with the [`DEFAULT_OFFLOAD_THRESHOLD`].
    pub fn new(store: P) -> Self {
        ClaimCheck {
            store: Arc::new(store),
            threshold: DEFAULT_OFFLOAD_THRESHOLD,
        }
    }

    /// Set the offload threshold in bytes (builder style): payloads larger than
    /// this are offloaded; payloads at or below it stay inline.
    #[must_use = "this value must be used"]
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// If `payload` exceeds the threshold, store it and return a compact reference;
    /// otherwise return it unchanged. Used on the enqueue path.
    pub async fn offload(&self, payload: Vec<u8>) -> Result<Vec<u8>> {
        if payload.len() <= self.threshold {
            return Ok(payload);
        }
        let key = self.store.put(&payload).await?;
        Ok(claim_check::make_reference(&key))
    }

    /// If `payload` is a claim-check reference, fetch the real payload; otherwise
    /// return it unchanged. Errors if the reference is dangling (the blob is
    /// missing). Used on the dispatch path.
    pub async fn fetch(&self, payload: Vec<u8>) -> Result<Vec<u8>> {
        match claim_check::reference_key(&payload) {
            Some(key) => self.store.get(key).await?.ok_or_else(|| {
                Error::Handler(format!(
                    "claim-check payload {key} is missing from the store"
                ))
            }),
            None => Ok(payload),
        }
    }

    /// If `payload` is a reference, delete its backing blob (idempotent). Call
    /// after a job is acked; a dead-lettered job's blob is intentionally retained.
    pub async fn delete(&self, payload: &[u8]) -> Result<()> {
        if let Some(key) = claim_check::reference_key(payload) {
            self.store.delete(key).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            let mut p = std::env::temp_dir();
            p.push(format!(
                "wl-claimcheck-{}-{}",
                std::process::id(),
                DIR_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            TempDir(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn file_store_round_trips_and_deletes() {
        let dir = TempDir::new();
        let store = FilePayloadStore::open(&dir.0).unwrap();
        let key = store.put(b"hello").await.unwrap();
        assert_eq!(
            store.get(&key).await.unwrap().as_deref(),
            Some(&b"hello"[..])
        );
        store.delete(&key).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), None);
        // Deleting an absent key is a no-op, not an error.
        store.delete(&key).await.unwrap();
        // Getting an unknown (but well-formed) key is `None`, not an error.
        let absent = JobId::new().to_string();
        assert_eq!(store.get(&absent).await.unwrap(), None);
    }

    #[tokio::test]
    async fn forged_keys_cannot_escape_the_store_directory() {
        let dir = TempDir::new();
        let store = FilePayloadStore::open(&dir.0).unwrap();
        // A real minted key (a UUID) is accepted.
        let good = store.put(b"x").await.unwrap();
        assert!(store.get(&good).await.unwrap().is_some());

        // Anything that is not a minted UUID key is refused before it ever reaches
        // the filesystem — path traversal, absolute paths, nesting, empty.
        for forged in [
            "../../etc/passwd",
            "/etc/passwd",
            "..",
            "nested/key",
            "",
            "not-a-uuid",
        ] {
            let got = store.get(forged).await;
            assert!(
                got.is_err(),
                "get({forged:?}) must be rejected, not resolved"
            );
            let del = store.delete(forged).await;
            assert!(
                del.is_err(),
                "delete({forged:?}) must be rejected, not executed"
            );
        }
    }

    #[tokio::test]
    async fn small_payload_passes_through_inline() {
        let dir = TempDir::new();
        let cc = ClaimCheck::new(FilePayloadStore::open(&dir.0).unwrap()).with_threshold(8);
        let small = b"1234".to_vec();
        let out = cc.offload(small.clone()).await.unwrap();
        assert_eq!(out, small, "a payload at/under the threshold stays inline");
        assert_eq!(cc.fetch(out).await.unwrap(), small);
    }

    #[tokio::test]
    async fn large_payload_offloads_fetches_and_deletes() {
        let dir = TempDir::new();
        let cc = ClaimCheck::new(FilePayloadStore::open(&dir.0).unwrap()).with_threshold(8);
        let big = vec![7u8; 100];

        let reference = cc.offload(big.clone()).await.unwrap();
        assert!(
            worklane_core::claim_check::is_reference(&reference),
            "an over-threshold payload becomes a reference"
        );
        assert!(reference.len() < big.len(), "the reference is compact");

        assert_eq!(
            cc.fetch(reference.clone()).await.unwrap(),
            big,
            "fetch resolves it"
        );

        cc.delete(&reference).await.unwrap();
        let err = cc.fetch(reference).await.unwrap_err();
        assert!(
            matches!(err, Error::Handler(_)),
            "a dangling reference errors"
        );
    }
}
