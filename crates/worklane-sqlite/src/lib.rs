//! SQLite-backed durable [`Broker`](worklane_core::Broker) for `worklane`.
//!
//! Jobs are persisted in a SQLite database (in-memory or on disk) as a
//! serialized [`JobEnvelope`] blob plus a few denormalized index columns
//! (`lane`, `available_at`, `leased_until`, `receipt`). Reservation uses a
//! visibility lease exactly as the broker contract requires: a reserved job is
//! hidden for a lease duration and becomes visible again if it is not acked,
//! retried, or failed before the lease expires (at-least-once delivery).
//!
//! The synchronous `rusqlite` calls run on Tokio's blocking pool via
//! [`spawn_blocking`](tokio::task::spawn_blocking) over a single connection
//! behind a `Mutex`. Time comes from an injected [`Clock`] so lease and
//! visibility decisions are deterministic and the shared conformance suite can
//! drive them.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use worklane_core::{
    Broker, Clock, DeadLetter, Error, JobEnvelope, JobId, NewJob, Reservation, ReservationReceipt,
    Result, SystemClock,
};

/// The default visibility lease duration.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

/// Schema for the live job store and the dead-letter store. `seq` is the
/// implicit rowid, giving a stable FIFO order for `reserve`.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS jobs (
    seq          INTEGER PRIMARY KEY,
    receipt      TEXT,
    lane         TEXT    NOT NULL,
    available_at INTEGER NOT NULL,
    leased_until INTEGER,
    envelope     BLOB    NOT NULL
);
CREATE TABLE IF NOT EXISTS dead (
    seq      INTEGER PRIMARY KEY,
    envelope BLOB NOT NULL,
    error    TEXT NOT NULL
);";

/// A SQLite-backed broker.
pub struct SqliteBroker {
    conn: Arc<Mutex<Connection>>,
    clock: Arc<dyn Clock>,
    lease: Duration,
}

impl SqliteBroker {
    /// Open (or create) a broker backed by the database file at `path`, using
    /// the system clock and the default lease.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::from_connection(Connection::open(path).map_err(sql_err)?)
    }

    /// Open a broker backed by a private in-memory database, using the system
    /// clock and the default lease. Each call is an isolated database.
    pub fn open_in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory().map_err(sql_err)?)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.execute_batch(SCHEMA).map_err(sql_err)?;
        Ok(SqliteBroker {
            conn: Arc::new(Mutex::new(conn)),
            clock: Arc::new(SystemClock::new()),
            lease: DEFAULT_LEASE,
        })
    }

    /// Use a custom clock (e.g. a manual clock for tests), builder style.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Set the visibility lease duration, builder style.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// A snapshot of the dead-letter store, for inspection and tests. This is a
    /// per-implementation convenience, not part of the [`Broker`] contract.
    pub fn dead_letters(&self) -> Result<Vec<DeadLetter>> {
        let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
        let mut stmt = conn
            .prepare("SELECT envelope, error FROM dead ORDER BY seq")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |r| {
                let blob: Vec<u8> = r.get(0)?;
                let error: String = r.get(1)?;
                Ok((blob, error))
            })
            .map_err(sql_err)?;
        let mut out = Vec::new();
        for row in rows {
            let (blob, error) = row.map_err(sql_err)?;
            out.push(DeadLetter::new(decode_envelope(&blob)?, error));
        }
        Ok(out)
    }

    /// Run a blocking closure with the locked connection on Tokio's blocking
    /// pool, keeping synchronous SQLite calls off the async runtime threads.
    async fn run<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().expect("sqlite connection mutex poisoned");
            f(&guard)
        })
        .await
        .map_err(|e| Error::Broker(format!("sqlite task join failed: {e}")))?
    }
}

#[async_trait]
impl Broker for SqliteBroker {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let now = nanos(self.clock.now());
        self.run(move |conn| {
            let id = JobId::new();
            let envelope = JobEnvelope::new(id, job.lane, job.kind, job.payload, job.max_attempts);
            let blob = encode_envelope(&envelope)?;
            conn.execute(
                "INSERT INTO jobs (receipt, lane, available_at, leased_until, envelope) \
                 VALUES (NULL, ?1, ?2, NULL, ?3)",
                params![envelope.lane, now, blob],
            )
            .map_err(sql_err)?;
            Ok(id)
        })
        .await
    }

    async fn reserve(&self, lane: &str) -> Result<Option<Reservation>> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let lease_until = nanos(now_d + self.lease);
        let lane = lane.to_string();
        self.run(move |conn| {
            let receipt = ReservationReceipt::new();
            let key = receipt_key(&receipt)?;
            // Atomically lease the oldest visible job on the lane and return its
            // envelope. A job is visible when its scheduled time has arrived and
            // it is unleased or its lease has expired.
            let blob: Option<Vec<u8>> = conn
                .query_row(
                    "UPDATE jobs SET receipt = ?1, leased_until = ?2 \
                     WHERE seq = ( \
                         SELECT seq FROM jobs \
                         WHERE lane = ?3 AND available_at <= ?4 \
                           AND (leased_until IS NULL OR leased_until <= ?4) \
                         ORDER BY seq LIMIT 1 \
                     ) \
                     RETURNING envelope",
                    params![key, lease_until, lane, now],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_err)?;
            match blob {
                Some(b) => Ok(Some(Reservation::new(decode_envelope(&b)?, receipt))),
                None => Ok(None),
            }
        })
        .await
    }

    async fn ack(&self, receipt: ReservationReceipt) -> Result<()> {
        let now = nanos(self.clock.now());
        self.run(move |conn| {
            let (seq, _) = find_valid_row(conn, receipt, now)?;
            conn.execute("DELETE FROM jobs WHERE seq = ?1", params![seq])
                .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let available_at = nanos(now_d + delay);
        self.run(move |conn| {
            let (seq, blob) = find_valid_row(conn, receipt, now)?;
            let mut envelope = decode_envelope(&blob)?;
            envelope.attempts += 1;
            let new_blob = encode_envelope(&envelope)?;
            conn.execute(
                "UPDATE jobs SET envelope = ?1, available_at = ?2, leased_until = NULL, \
                 receipt = NULL WHERE seq = ?3",
                params![new_blob, available_at, seq],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()> {
        let now = nanos(self.clock.now());
        self.run(move |conn| {
            let (seq, blob) = find_valid_row(conn, receipt, now)?;
            conn.execute(
                "INSERT INTO dead (envelope, error) VALUES (?1, ?2)",
                params![blob, error],
            )
            .map_err(sql_err)?;
            conn.execute("DELETE FROM jobs WHERE seq = ?1", params![seq])
                .map_err(sql_err)?;
            Ok(())
        })
        .await
    }
}

/// Locate the row currently leased under `receipt` and confirm the lease is
/// still valid. Returns the row's `seq` and envelope bytes, or a stale-reservation
/// error when the receipt is unknown, superseded, or expired.
fn find_valid_row(
    conn: &Connection,
    receipt: ReservationReceipt,
    now: i64,
) -> Result<(i64, Vec<u8>)> {
    let key = receipt_key(&receipt)?;
    let row: Option<(i64, Option<i64>, Vec<u8>)> = conn
        .query_row(
            "SELECT seq, leased_until, envelope FROM jobs WHERE receipt = ?1",
            params![key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(sql_err)?;
    match row {
        Some((seq, Some(leased_until), blob)) if leased_until > now => Ok((seq, blob)),
        _ => Err(stale(receipt)),
    }
}

/// The opaque storage key for a receipt (its serialized form).
fn receipt_key(receipt: &ReservationReceipt) -> Result<String> {
    serde_json::to_string(receipt).map_err(json_err)
}

fn encode_envelope(envelope: &JobEnvelope) -> Result<Vec<u8>> {
    serde_json::to_vec(envelope).map_err(json_err)
}

fn decode_envelope(bytes: &[u8]) -> Result<JobEnvelope> {
    serde_json::from_slice(bytes).map_err(json_err)
}

/// Convert a clock duration to integer nanoseconds for storage, saturating at
/// `i64::MAX` (far beyond any realistic monotonic-since-epoch value).
fn nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

fn stale(receipt: ReservationReceipt) -> Error {
    Error::StaleReservation(format!("receipt {receipt:?} is not current"))
}

fn sql_err(e: rusqlite::Error) -> Error {
    Error::Broker(e.to_string())
}

fn json_err(e: serde_json::Error) -> Error {
    Error::Broker(e.to_string())
}
