//! Connection management for the SQLite backend.
//!
//! A file-backed broker uses an [`r2d2`] connection pool so reads run
//! concurrently: SQLite in WAL mode allows any number of concurrent readers
//! alongside a single writer, and per-connection `busy_timeout` makes a writer
//! that meets the write lock wait rather than fail. An in-memory broker keeps a
//! *single* connection behind a mutex instead: a private `:memory:` database is
//! visible only to the connection that opened it, and the shared-cache
//! alternative raises `SQLITE_LOCKED` (which `busy_timeout` does *not* retry, as
//! it only covers `SQLITE_BUSY`), so pooling it would be unsafe. In-memory
//! databases are ephemeral and test-oriented, so serial access is acceptable
//! there; the pool's concurrency is what matters for a durable file database.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use worklane_core::{Error, Result};

use crate::schema::init_connection;

/// The default file-pool size. Enough connections for several concurrent readers
/// plus the single writer without holding many file descriptors open.
pub const DEFAULT_POOL_SIZE: u32 = 8;

/// An r2d2 manager that opens `rusqlite` connections to a fixed file path and
/// applies the per-connection settings to each. Deliberately built on the
/// workspace `rusqlite` (not `r2d2_sqlite`, which links its own bundled SQLite)
/// so only one SQLite C library is in the build.
#[derive(Debug)]
pub(crate) struct SqliteManager {
    path: PathBuf,
}

impl r2d2::ManageConnection for SqliteManager {
    type Connection = Connection;
    type Error = rusqlite::Error;

    fn connect(&self) -> std::result::Result<Connection, rusqlite::Error> {
        let mut conn = Connection::open(&self.path)?;
        init_connection(&mut conn)?;
        Ok(conn)
    }

    fn is_valid(&self, conn: &mut Connection) -> std::result::Result<(), rusqlite::Error> {
        conn.execute_batch("SELECT 1")
    }

    fn has_broken(&self, _conn: &mut Connection) -> bool {
        false
    }
}

/// How a [`SqliteBroker`](crate::SqliteBroker) and its result store reach SQLite:
/// a shared file pool, or a single shared in-memory connection. Cloning shares
/// the underlying pool/connection (both variants hold an `Arc`), so a broker and
/// the result store it hands out talk to the same database.
#[derive(Clone)]
pub(crate) enum ConnPool {
    /// A file database: an r2d2 pool of WAL connections (concurrent reads).
    File(r2d2::Pool<SqliteManager>),
    /// An in-memory database: one connection, serialized by a mutex.
    Memory(Arc<Mutex<Connection>>),
}

impl ConnPool {
    /// Open a file-backed pool at `path`, run one-time configuration/migration on
    /// a checked-out connection, and return it. The migration closure runs once;
    /// every pooled connection is configured by [`SqliteManager::connect`].
    pub(crate) fn open_file(
        path: PathBuf,
        size: u32,
        migrate: impl FnOnce(&Connection) -> Result<()>,
    ) -> Result<Self> {
        let pool = r2d2::Pool::builder()
            .max_size(size)
            // Connections are local and cheap to validate; skip the per-checkout
            // round-trip, relying on `has_broken`/reconnect instead.
            .test_on_check_out(false)
            // Under heavy same-lane contention every pooled connection can be busy;
            // fail a checkout fast (seconds) rather than blocking on r2d2's 30s
            // default, so a saturated pool surfaces as a prompt error, not a stall.
            .connection_timeout(std::time::Duration::from_secs(5))
            .build(SqliteManager { path })
            .map_err(|e| Error::Broker(format!("sqlite pool build failed: {e}")))?;
        {
            let conn = pool
                .get()
                .map_err(|e| Error::Broker(format!("sqlite pool checkout failed: {e}")))?;
            migrate(&conn)?;
        }
        Ok(ConnPool::File(pool))
    }

    /// Wrap an already-configured, already-migrated in-memory connection.
    pub(crate) fn from_memory(conn: Connection) -> Self {
        ConnPool::Memory(Arc::new(Mutex::new(conn)))
    }

    /// Wrap an already-shared mutex-guarded connection (e.g. one a result store
    /// was constructed from directly), so it can travel through the same access
    /// path as a broker-shared pool.
    pub(crate) fn from_arc(conn: Arc<Mutex<Connection>>) -> Self {
        ConnPool::Memory(conn)
    }

    /// Run a blocking closure against a connection on Tokio's blocking pool.
    /// File: each call borrows a pooled connection, so calls run concurrently
    /// (reads truly in parallel under WAL). Memory: the mutex serializes calls.
    pub(crate) async fn run<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.with_conn(f))
            .await
            .map_err(|e| Error::Broker(format!("sqlite task join failed: {e}")))?
    }

    /// Run `f` against a connection on the current thread. The blocking checkout
    /// (file) or mutex lock (memory) means callers must already be off the async
    /// runtime — [`run`](Self::run) is the async wrapper; this also serves the
    /// few synchronous inspection helpers.
    pub(crate) fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        match self {
            ConnPool::File(pool) => {
                let conn = pool
                    .get()
                    .map_err(|e| Error::Broker(format!("sqlite pool checkout failed: {e}")))?;
                f(&conn)
            }
            ConnPool::Memory(mutex) => {
                // Recover from a poisoned mutex rather than propagating: every
                // operation commits or rolls back before returning, so a panic in
                // a prior closure cannot leave a partial transaction visible here.
                let guard = mutex.lock().unwrap_or_else(|e| e.into_inner());
                f(&guard)
            }
        }
    }
}
