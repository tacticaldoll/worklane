use async_trait::async_trait;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use std::time::Duration;
use worklane_core::{Error, JobId, Result, ResultStore};

/// A Redis-backed durable result store.
#[derive(Clone)]
pub struct RedisResultStore {
    conn: ConnectionManager,
    namespace: String,
    ttl: Option<Duration>,
}

impl RedisResultStore {
    /// Create a new result store from an existing connection manager.
    pub fn new(conn: ConnectionManager, namespace: &str) -> Self {
        Self {
            conn,
            namespace: namespace.to_string(),
            ttl: None,
        }
    }

    /// Set an optional time-to-live (TTL) for stored results, builder style.
    /// If configured, results will automatically expire and be deleted from Redis
    /// after this duration.
    #[must_use = "this value must be used"]
    pub fn with_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.ttl = ttl;
        self
    }

    fn key(&self, job_id: &JobId) -> String {
        format!("{}:result:{}", self.namespace, job_id)
    }
}

#[async_trait]
impl ResultStore for RedisResultStore {
    async fn store(&self, job_id: &JobId, result: &[u8]) -> Result<()> {
        let mut conn = self.conn.clone();
        let key = self.key(job_id);

        match self.ttl {
            Some(ttl) => {
                let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
                if ttl_ms > 0 {
                    let _: () = conn
                        .pset_ex(&key, result, ttl_ms)
                        .await
                        .map_err(|e| rs_err("redis pset_ex failed", e))?;
                } else {
                    let _: () = conn
                        .set(&key, result)
                        .await
                        .map_err(|e| rs_err("redis set failed", e))?;
                }
            }
            None => {
                let _: () = conn
                    .set(&key, result)
                    .await
                    .map_err(|e| rs_err("redis set failed", e))?;
            }
        }
        Ok(())
    }

    async fn get(&self, job_id: &JobId) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn.clone();
        let key = self.key(job_id);
        let data: Option<Vec<u8>> = conn
            .get(&key)
            .await
            .map_err(|e| rs_err("redis get failed", e))?;
        Ok(data)
    }
}

/// Build a `ResultStore` error for `op`, redacting any credential-bearing URL
/// the redis driver may echo before it enters `Error` and reaches logs.
fn rs_err(op: &str, e: redis::RedisError) -> Error {
    Error::ResultStore(worklane_core::redact_credentials(&format!("{op}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RedisBroker;

    #[tokio::test]
    async fn test_redis_store_and_get_with_ttl() {
        let Some(url) = std::env::var("WORKLANE_REDIS_TEST_URL").ok() else {
            eprintln!(
                "SKIP test_redis_store_and_get_with_ttl: set WORKLANE_REDIS_TEST_URL to run the redis TTL test"
            );
            return;
        };

        let namespace = format!(
            "wl_rs_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        );
        let broker = RedisBroker::connect_with_namespace(&url, &namespace)
            .await
            .unwrap();
        let store = broker.result_store().with_ttl(Some(Duration::from_secs(1)));

        let job_id = JobId::new();

        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved, None);

        let data = b"hello redis";
        store.store(&job_id, data).await.unwrap();

        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved.unwrap(), data);

        // Let TTL expire
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved, None);
    }

    #[tokio::test]
    async fn test_redis_store_with_subsecond_ttl_expires() {
        let Some(url) = std::env::var("WORKLANE_REDIS_TEST_URL").ok() else {
            eprintln!(
                "SKIP test_redis_store_with_subsecond_ttl_expires: set WORKLANE_REDIS_TEST_URL to run the redis TTL test"
            );
            return;
        };

        let namespace = format!(
            "wl_rs_subsec_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        );
        let broker = RedisBroker::connect_with_namespace(&url, &namespace)
            .await
            .unwrap();
        let store = broker
            .result_store()
            .with_ttl(Some(Duration::from_millis(50)));

        let job_id = JobId::new();
        store.store(&job_id, b"short lived").await.unwrap();
        assert_eq!(store.get(&job_id).await.unwrap().unwrap(), b"short lived");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(store.get(&job_id).await.unwrap(), None);
    }
}
