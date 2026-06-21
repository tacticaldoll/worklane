use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::{Arc, Mutex};
use worklane_core::{Error, JobId, Result, ResultStore};

use crate::conn::ConnPool;

/// A SQLite-backed durable result store.
///
/// Holds the same connection-pool shape as the broker (a file pool or a single
/// shared in-memory connection), so a store handed out by
/// [`SqliteBroker::result_store`](crate::SqliteBroker::result_store) is coherent
/// with that broker for *both* file and in-memory databases.
#[derive(Clone)]
pub struct SqliteResultStore {
    pool: ConnPool,
}

impl SqliteResultStore {
    /// Create a new result store from an existing connection wrapper. Sharing the
    /// same `Arc<Mutex<Connection>>` a broker uses makes the two coherent.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            pool: ConnPool::from_arc(conn),
        }
    }

    /// Build a result store over an existing connection pool (a broker's own), so
    /// they read and write the same database.
    pub(crate) fn from_pool(pool: ConnPool) -> Self {
        Self { pool }
    }

    /// Open (or create) a *standalone* result store backed by the database file at
    /// `path`.
    ///
    /// **Use a file path that the broker also opens** — the result store and the
    /// broker share data only through the same on-disk database. In particular,
    /// `":memory:"` opens a *private* in-memory database unique to this
    /// connection: a `SqliteBroker::open_in_memory()` broker cannot see results
    /// stored here, and vice-versa. To share a database (in-memory or file) with a
    /// broker, prefer
    /// [`SqliteBroker::result_store`](crate::SqliteBroker::result_store), which
    /// hands out a store over the broker's own pool/connection.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let mut conn = Connection::open(path).map_err(sql_err)?;
        crate::configure(&mut conn)?;
        crate::migrate(&conn)?;
        Ok(Self::new(Arc::new(Mutex::new(conn))))
    }

    /// Run a blocking closure against a connection on Tokio's blocking pool. The
    /// pool serializes (in-memory) or pools (file) exactly as the broker's does,
    /// so a shared connection behaves identically on both sides.
    async fn run<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || pool.with_conn(f))
            .await
            .map_err(|e| Error::ResultStore(format!("sqlite task join failed: {e}")))?
    }
}

#[async_trait]
impl ResultStore for SqliteResultStore {
    async fn store(&self, job_id: &JobId, result: &[u8]) -> Result<()> {
        let key = job_id.to_string();
        let blob = result.to_vec();
        self.run(move |conn| {
            conn.execute(
                "INSERT INTO results (job_id, blob) VALUES (?1, ?2) \
                 ON CONFLICT(job_id) DO UPDATE SET blob = excluded.blob",
                params![key, blob],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn get(&self, job_id: &JobId) -> Result<Option<Vec<u8>>> {
        let key = job_id.to_string();
        self.run(move |conn| {
            let blob: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT blob FROM results WHERE job_id = ?1",
                    params![key],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sql_err)?;
            Ok(blob)
        })
        .await
    }
}

fn sql_err(e: rusqlite::Error) -> Error {
    Error::ResultStore(worklane_core::redact_credentials(&e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_store_and_get() {
        let store = SqliteResultStore::open(":memory:").unwrap();
        let job_id = JobId::new();

        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved, None);

        let data = b"hello world";
        store.store(&job_id, data).await.unwrap();

        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved.unwrap(), data);

        let new_data = b"new data";
        store.store(&job_id, new_data).await.unwrap();

        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved.unwrap(), new_data);
    }
}
