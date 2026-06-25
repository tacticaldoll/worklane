//! SQLite-backed durable [`Broker`] for `worklane`.
//!
//! Depend on this crate when an embedded SQLite database is the right durable
//! store for a service. Application code still uses the `worklane` facade for
//! `Client` and `Worker`.
//!
//! Jobs are persisted in a SQLite database (in-memory or on disk) as a
//! serialized [`JobEnvelope`](worklane_core::JobEnvelope) blob plus a few
//! denormalized index columns (`lane`, `available_at`, `leased_until`,
//! `receipt`). Reservation uses a visibility lease exactly as the broker
//! contract requires: a reserved job is
//! hidden for a lease duration and becomes visible again if it is not acked,
//! retried, or failed before the lease expires (at-least-once delivery).
//!
//! The synchronous `rusqlite` calls run on Tokio's blocking pool via
//! [`spawn_blocking`](tokio::task::spawn_blocking). A file-backed broker holds an
//! [`r2d2`] connection pool so reads run concurrently (WAL: many readers + one
//! writer); an in-memory broker keeps a single mutex-guarded connection. Time
//! comes from an injected [`Clock`] so lease and
//! visibility decisions are deterministic and the shared conformance suite can
//! drive them. The default [`WallClock`] gives a restart-stable epoch and is
//! monotonic non-decreasing for the broker's lifetime, so a backward NTP step
//! cannot reorder visibility/lease keys or re-hide in-flight work. A large
//! forward step can still expire a live lease early, widening the at-least-once
//! redelivery window; a slewing time daemon (e.g. `chrony`) keeps forward
//! corrections gradual.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use worklane_core::spi::{
    MAX_DEAD_LETTER_SWEEP, decode_envelope, encode_envelope, nanos, receipt_key, stale,
};
use worklane_core::{
    BatchEnqueue, Broker, Clock, DeadLetter, Error, JobId, Lane, NewJob, Reservation,
    ReservationReceipt, Result, RetentionPolicy, UnboundedDlqWarning, WallClock,
};

mod conn;
use conn::ConnPool;
mod dead_letters;
use dead_letters::{dead_letter_seq, free_unique_key};
mod jobs;
pub use conn::DEFAULT_POOL_SIZE;
use jobs::{find_valid_row, insert_job};
mod schema;
use schema::{configure, migrate};
mod result_store;
pub use result_store::SqliteResultStore;
mod schedules;

/// Re-export of the underlying `rusqlite` so callers of
/// [`SqliteBroker::enqueue_with_tx`] name the exact `Transaction` type the broker
/// expects, without taking their own (possibly mismatched) `rusqlite` dependency.
pub use rusqlite;

/// The default visibility lease duration.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

/// A SQLite-backed broker.
pub struct SqliteBroker {
    pool: ConnPool,
    clock: Arc<dyn Clock>,
    lease: Duration,
    retention: RetentionPolicy,
    /// One-shot warning when dead-lettering under an unbounded retention policy.
    dlq_warning: UnboundedDlqWarning,
    /// Maximum times a job may be delivered before it is dead-lettered on the
    /// next reserve; `None` (default) means unbounded.
    max_deliveries: Option<u32>,
}

impl SqliteBroker {
    /// Open (or create) a broker backed by the database file at `path`, using
    /// the system clock and the default lease. Reads run concurrently across a
    /// connection pool (WAL).
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::open_with_pool_size(path, DEFAULT_POOL_SIZE)
    }

    /// As [`open`](Self::open), but with an explicit connection-pool size.
    ///
    /// The pool bounds how many broker operations touch the file concurrently.
    /// SQLite has a single writer (WAL), so writers still serialize — but a pool
    /// smaller than the number of concurrent in-flight broker calls makes the
    /// extra calls wait on a checkout (then on the per-connection `busy_timeout`),
    /// so under high worker concurrency size this to roughly that concurrency
    /// rather than leaving it at the default ([`DEFAULT_POOL_SIZE`]). `size` is
    /// clamped to at least 1. For heavy write concurrency prefer Postgres.
    ///
    /// [`DEFAULT_POOL_SIZE`]: crate::DEFAULT_POOL_SIZE
    pub fn open_with_pool_size(path: impl AsRef<std::path::Path>, size: u32) -> Result<Self> {
        let pool = ConnPool::open_file(path.as_ref().to_path_buf(), size.max(1), migrate)?;
        Ok(Self::with_pool(pool))
    }

    /// Open a broker backed by a private in-memory database, using the system
    /// clock and the default lease. Each call is an isolated database, served by a
    /// single connection.
    pub fn open_in_memory() -> Result<Self> {
        let mut conn = Connection::open_in_memory().map_err(sql_err)?;
        configure(&mut conn)?;
        migrate(&conn)?;
        Ok(Self::with_pool(ConnPool::from_memory(conn)))
    }

    fn with_pool(pool: ConnPool) -> Self {
        SqliteBroker {
            pool,
            // A durable broker needs a restart-stable epoch: WallClock measures
            // since UNIX_EPOCH, so persisted times survive a process restart.
            clock: Arc::new(WallClock::new()),
            lease: DEFAULT_LEASE,
            retention: RetentionPolicy::new(),
            dlq_warning: UnboundedDlqWarning::default(),
            max_deliveries: None,
        }
    }

    /// Use a custom clock (e.g. a manual clock for tests), builder style.
    #[must_use = "this value must be used"]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Set the visibility lease duration, builder style.
    #[must_use = "this value must be used"]
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Bound the dead-letter store with a [`RetentionPolicy`], enforced lazily on
    /// `fail` per lane (builder style). Defaults to unbounded.
    #[must_use = "this value must be used"]
    pub fn with_dead_letter_retention(mut self, policy: RetentionPolicy) -> Self {
        self.retention = policy;
        self
    }

    /// Bound how many times a job may be delivered before it is dead-lettered
    /// (builder style). Counts reservations, not handler failures, so it bounds a
    /// poison-pill job whose handler process crashes before acking, retrying, or
    /// failing (which never advances `attempts`). Defaults to unbounded. When
    /// also using `max_attempts`, set this above it so legitimate retries (each a
    /// redelivery) are not cut short.
    #[must_use = "this value must be used"]
    pub fn with_max_deliveries(mut self, max: u32) -> Self {
        self.max_deliveries = Some(max);
        self
    }

    /// Enqueue `job` on a caller-supplied transaction (the Transactional Outbox
    /// pattern), so a business write and its job enqueue commit **atomically**.
    ///
    /// Runs the same insert as [`enqueue`](Broker::enqueue) but against `tx` — the
    /// caller's own [`rusqlite::Transaction`] — instead of the broker's pool. The
    /// job becomes visible to workers only when the caller commits `tx`; if the
    /// caller rolls back (or drops `tx`), the enqueue is undone with the business
    /// write. This closes the dual-write gap where a job is enqueued but the
    /// business transaction later aborts (or vice-versa).
    ///
    /// `tx` must be on a connection to the **same database** the broker uses, so
    /// the `jobs`/`unique_keys` tables are present (a separate connection to the
    /// same file works under WAL). This is synchronous — the caller already owns
    /// the connection and decides where it runs (e.g. inside `spawn_blocking`).
    ///
    /// Unique-key dedup still applies within `tx`: if a live job already holds the
    /// key, its id is returned and no row is inserted.
    ///
    /// ```no_run
    /// # use worklane_sqlite::{SqliteBroker, rusqlite::Connection};
    /// # use worklane_core::{NewJob, Lane, to_payload};
    /// # fn demo(broker: &SqliteBroker, conn: &mut Connection, job: NewJob) -> worklane_core::Result<()> {
    /// let tx = conn.transaction().unwrap();
    /// // ... the application's own business writes on `tx` ...
    /// let _id = broker.enqueue_with_tx(&tx, job)?;
    /// tx.commit().unwrap(); // business write + enqueue commit together
    /// # Ok(())
    /// # }
    /// ```
    pub fn enqueue_with_tx(&self, tx: &rusqlite::Transaction<'_>, job: NewJob) -> Result<JobId> {
        let available_at = nanos(self.clock.now().saturating_add(job.delay));
        insert_job(tx, job, available_at)
    }

    /// A durable [`SqliteResultStore`] that shares this broker's database
    /// connection(s).
    ///
    /// Unlike [`SqliteResultStore::open`], which opens its own connection (and a
    /// *private* database for `":memory:"`), this hands out a store over the
    /// broker's own pool/connection, so results are coherent with the broker for
    /// both file and in-memory databases. The `results` table is created by the
    /// broker's own migration, so no extra setup is needed.
    pub fn result_store(&self) -> SqliteResultStore {
        SqliteResultStore::from_pool(self.pool.clone())
    }

    /// A snapshot of the dead-letter store, for inspection and tests. This is a
    /// per-implementation convenience, not part of the [`Broker`] contract.
    pub fn dead_letters(&self) -> Result<Vec<DeadLetter>> {
        self.pool.with_conn(|conn| {
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
        })
    }

    /// Run a blocking closure with a pooled connection on Tokio's blocking pool,
    /// keeping synchronous SQLite calls off the async runtime threads.
    async fn run<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        self.pool.run(f).await
    }
}

#[async_trait]
impl BatchEnqueue for SqliteBroker {
    async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        let now_d = self.clock.now();
        self.run(move |conn| {
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let mut ids = Vec::with_capacity(jobs.len());
            for job in jobs {
                let available_at = nanos(now_d.saturating_add(job.delay));
                ids.push(insert_job(&tx, job, available_at)?);
            }
            tx.commit().map_err(sql_err)?;
            Ok(ids)
        })
        .await
    }
}

#[async_trait]
impl Broker for SqliteBroker {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let available_at = nanos(self.clock.now().saturating_add(job.delay));
        self.run(move |conn| {
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let id = insert_job(&tx, job, available_at)?;
            tx.commit().map_err(sql_err)?;
            Ok(id)
        })
        .await
    }

    fn batch_enqueue(&self) -> Option<&dyn BatchEnqueue> {
        Some(self)
    }

    async fn reserve(&self, lane: &Lane) -> Result<Option<Reservation>> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let lease = self.lease;
        let lease_until = nanos(now_d.saturating_add(lease));
        let lane = lane.as_str().to_string();
        let max_deliveries = self.max_deliveries;
        let retention = self.retention;
        self.run(move |conn| {
            let receipt = ReservationReceipt::new();
            let key = receipt_key(&receipt)?;
            // A job is visible when its scheduled time has arrived and it is
            // unleased or its lease has expired. Every reserve bumps the chosen
            // job's `deliveries` count.
            match max_deliveries {
                // Fast path (unbounded): one atomic UPDATE...RETURNING leases the
                // oldest visible job and increments its delivery count.
                None => {
                    let blob: Option<Vec<u8>> = conn
                        .query_row(
                            "UPDATE jobs SET receipt = ?1, leased_until = ?2, \
                             deliveries = deliveries + 1 \
                             WHERE seq = ( \
                                 SELECT seq FROM jobs \
                                 WHERE lane = ?3 AND available_at <= ?4 \
                                   AND (leased_until IS NULL OR leased_until <= ?4) \
                                 ORDER BY priority DESC, available_at ASC, seq ASC LIMIT 1 \
                             ) \
                             RETURNING envelope",
                            params![key, lease_until, lane, now],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(sql_err)?;
                    match blob {
                        Some(b) => Ok(Some(Reservation::new(decode_envelope(&b)?, receipt, lease))),
                        None => Ok(None),
                    }
                }
                // Bounded path: in one IMMEDIATE transaction, pick the next visible
                // candidate; if it has already been delivered `max` times,
                // dead-letter it (a poison pill) and pick the next, else lease it
                // and bump its count. The transaction makes select-then-resolve
                // atomic so a concurrent reserver cannot interleave.
                Some(max) => {
                    let tx = conn.unchecked_transaction().map_err(sql_err)?;
                    // Bound the poison sweep per `reserve`: a large backlog of
                    // over-max jobs would otherwise be dead-lettered in one write
                    // transaction, and SQLite's single writer means that transaction
                    // blocks every other writer for its whole duration. After the cap
                    // we yield with no reservation; the next `reserve` resumes.
                    let mut swept = 0u32;
                    let outcome = loop {
                        let row: Option<(i64, i64, Vec<u8>)> = tx
                            .query_row(
                                "SELECT seq, deliveries, envelope FROM jobs \
                                 WHERE lane = ?1 AND available_at <= ?2 \
                                   AND (leased_until IS NULL OR leased_until <= ?2) \
                                 ORDER BY priority DESC, available_at ASC, seq ASC LIMIT 1",
                                params![lane, now],
                                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                            )
                            .optional()
                            .map_err(sql_err)?;
                        let Some((seq, deliveries, blob)) = row else {
                            break None;
                        };
                        if deliveries.saturating_add(1) > i64::from(max) {
                            dead_letter_seq(
                                &tx,
                                seq,
                                &blob,
                                format!("exceeded max deliveries ({max})"),
                                now,
                                &retention,
                            )?;
                            swept += 1;
                            if swept >= MAX_DEAD_LETTER_SWEEP {
                                break None;
                            }
                            continue;
                        }
                        tx.execute(
                            "UPDATE jobs SET receipt = ?1, leased_until = ?2, \
                             deliveries = deliveries + 1 WHERE seq = ?3",
                            params![key, lease_until, seq],
                        )
                        .map_err(sql_err)?;
                        break Some(Reservation::new(decode_envelope(&blob)?, receipt, lease));
                    };
                    tx.commit().map_err(sql_err)?;
                    Ok(outcome)
                }
            }
        })
        .await
    }

    async fn ack(&self, receipt: ReservationReceipt) -> Result<()> {
        let now = nanos(self.clock.now());
        self.run(move |conn| {
            // Drop the job and its uniqueness key atomically so a failure between
            // the two deletes can never leave an orphan `unique_keys` row.
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let (seq, _) = find_valid_row(&tx, receipt, now)?;
            free_unique_key(&tx, seq)?;
            tx.execute("DELETE FROM jobs WHERE seq = ?1", params![seq])
                .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        // `delay` is an arbitrary caller-supplied `Duration` (public `Broker::retry`
        // contract); saturate so a near-`Duration::MAX` delay cannot panic the
        // addition before `nanos` clamps to `i64::MAX`.
        let available_at = nanos(now_d.saturating_add(delay));
        self.run(move |conn| {
            // One IMMEDIATE transaction (the connection's default behavior): the
            // write lock is taken up front, so the `find_valid_row` read and the
            // UPDATE cannot interleave with another connection/process resolving the
            // same receipt — matching the atomicity of `ack`/`fail`. Without it a
            // concurrent resolve between the read and the UPDATE would silently drop
            // this retry (the UPDATE would match no row).
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let (seq, blob) = find_valid_row(&tx, receipt, now)?;
            let mut envelope = decode_envelope(&blob)?;
            // Saturate: `attempts` comes from the stored (possibly corrupted) blob;
            // a wrap at `u32::MAX` would silently reset the retry counter.
            envelope.attempts = envelope.attempts.saturating_add(1);
            let new_blob = encode_envelope(&envelope)?;
            tx.execute(
                "UPDATE jobs SET envelope = ?1, available_at = ?2, leased_until = NULL, \
                 receipt = NULL WHERE seq = ?3",
                params![new_blob, available_at, seq],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn defer(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let available_at = nanos(now_d.saturating_add(delay));
        self.run(move |conn| {
            // Same IMMEDIATE-transaction CAS as `retry`, but the envelope blob is
            // left untouched — `defer` reschedules without advancing `attempts`.
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let (seq, _blob) = find_valid_row(&tx, receipt, now)?;
            tx.execute(
                "UPDATE jobs SET available_at = ?1, leased_until = NULL, \
                 receipt = NULL WHERE seq = ?2",
                params![available_at, seq],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()> {
        let now = nanos(self.clock.now());
        let retention = self.retention;
        self.dlq_warning.warn_once(&retention);
        self.run(move |conn| {
            // Move to the dead-letter store, release the uniqueness key, and
            // delete the live job as one atomic unit: a partial failure must not
            // leave a job both dead-lettered and live, or leak a `unique_keys` row.
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let (seq, blob) = find_valid_row(&tx, receipt, now)?;
            // Move to the dead-letter store, release the uniqueness key (retaining
            // it on the dead record for a later requeue), delete the live job, and
            // enforce retention — all atomically. Shared with the delivery-bound
            // dead-lettering in `reserve`.
            dead_letter_seq(&tx, seq, &blob, error, now, &retention)?;
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn extend(&self, receipt: ReservationReceipt) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let lease_until = nanos(now_d.saturating_add(self.lease));
        self.run(move |conn| {
            let key = receipt_key(&receipt)?;
            // Single guarded re-lease: the `leased_until > now` predicate is the
            // same validity check `find_valid_row` applies, so an expired or
            // superseded receipt matches no row and is rejected as stale without
            // touching attempts or schedule.
            let changed = conn
                .execute(
                    "UPDATE jobs SET leased_until = ?1 WHERE receipt = ?2 AND leased_until > ?3",
                    params![lease_until, key, now],
                )
                .map_err(sql_err)?;
            if changed == 0 {
                return Err(stale(receipt));
            }
            Ok(())
        })
        .await
    }

    async fn classify(&self, id: JobId) -> Result<worklane_core::JobState> {
        let key = id.to_string();
        self.run(move |conn| {
            // Atomic check across both tables in a single query.
            let state: Option<i32> = conn
                .query_row(
                    "SELECT 1 FROM jobs WHERE id = ?1 UNION ALL SELECT 2 FROM dead WHERE id = ?1 LIMIT 1",
                    params![key],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sql_err)?;
            match state {
                Some(1) => Ok(worklane_core::JobState::Live),
                Some(2) => Ok(worklane_core::JobState::DeadLettered),
                _ => Ok(worklane_core::JobState::CompletedOrUnknown),
            }
        })
        .await
    }

    fn dead_letter_store(&self) -> Option<&dyn worklane_core::DeadLetterStore> {
        Some(self)
    }

    fn queue_stats(&self) -> Option<&dyn worklane_core::QueueStats> {
        Some(self)
    }

    fn scheduled_store(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn worklane_core::ScheduledStore>> {
        Some(self)
    }
}

#[async_trait]
impl worklane_core::DeadLetterStore for SqliteBroker {
    async fn read_dead_letters(&self, lane: &Lane, limit: usize) -> Result<Vec<DeadLetter>> {
        let lane = lane.as_str().to_string();
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.run(move |conn| {
            // Bounded, lane-scoped, non-destructive: a plain SELECT touches no
            // row. `seq` order is an unspecified implementation choice.
            let mut stmt = conn
                .prepare("SELECT envelope, error FROM dead WHERE lane = ?1 ORDER BY seq LIMIT ?2")
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![lane, limit], |r| {
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
        })
        .await
    }

    async fn count_dead_letters(&self, lane: &Lane) -> Result<u64> {
        let lane = lane.as_str().to_string();
        self.run(move |conn| {
            // Lane-scoped, non-destructive count.
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM dead WHERE lane = ?1",
                    params![lane],
                    |r| r.get(0),
                )
                .map_err(sql_err)?;
            Ok(u64::try_from(count).unwrap_or(0))
        })
        .await
    }

    async fn requeue(&self, id: JobId) -> Result<()> {
        let now = nanos(self.clock.now());
        let key = id.to_string();
        self.run(move |conn| {
            // Atomic move from the dead-letter store back to the live store; a
            // missing record selects 0 rows and is rejected with no writes.
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let row: Option<(Vec<u8>, String, Option<String>)> = tx
                .query_row(
                    "SELECT envelope, lane, unique_key FROM dead WHERE id = ?1",
                    params![key],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()
                .map_err(sql_err)?;
            let Some((blob, lane, unique_key)) = row else {
                return Err(Error::Broker(format!("no dead-letter record for job {id}")));
            };
            let live_id_held = tx
                .query_row("SELECT 1 FROM jobs WHERE id = ?1", params![key], |_| Ok(()))
                .optional()
                .map_err(sql_err)?
                .is_some();
            if live_id_held {
                return Err(Error::LiveJobIdConflict(format!(
                    "cannot requeue job {id}: a live job with the same id already exists"
                )));
            }
            // If the job held a unique key, re-acquire it for the requeued job —
            // unless another live job now holds it (the key was freed at fail
            // time, so this can happen). On conflict, reject with no writes.
            if let Some(uk) = &unique_key {
                let held = tx
                    .query_row(
                        "SELECT 1 FROM unique_keys WHERE unique_key = ?1",
                        params![uk],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(sql_err)?
                    .is_some();
                if held {
                    return Err(Error::UniqueKeyHeld(format!(
                        "cannot requeue job {id}: unique key {uk:?} is held by another live job"
                    )));
                }
            }
            // Re-insert the envelope verbatim, visible now on its original lane.
            let envelope = decode_envelope(&blob)?;
            let envelope_id = envelope.id.to_string();
            let inserted = tx
                .execute(
                    "INSERT OR IGNORE INTO jobs \
                     (id, receipt, lane, priority, available_at, leased_until, envelope) \
                     VALUES (?1, NULL, ?2, ?3, ?4, NULL, ?5)",
                    params![envelope_id, lane, envelope.priority, now, blob],
                )
                .map_err(sql_err)?;
            if inserted == 0 {
                return Err(Error::LiveJobIdConflict(format!(
                    "cannot requeue job {id}: a live job with the same id already exists"
                )));
            }
            if let Some(uk) = &unique_key {
                let seq = tx.last_insert_rowid();
                let inserted = tx
                    .execute(
                        "INSERT OR IGNORE INTO unique_keys (unique_key, seq) VALUES (?1, ?2)",
                        params![uk, seq],
                    )
                    .map_err(sql_err)?;
                if inserted == 0 {
                    return Err(Error::UniqueKeyHeld(format!(
                        "cannot requeue job {id}: unique key {uk:?} is held by another live job"
                    )));
                }
            }
            tx.execute("DELETE FROM dead WHERE id = ?1", params![key])
                .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn purge_dead_letters(&self, lane: &Lane) -> Result<u64> {
        let lane = lane.as_str().to_string();
        self.run(move |conn| {
            // Lane-scoped, destructive: delete every dead record for `lane`.
            let removed = conn
                .execute("DELETE FROM dead WHERE lane = ?1", params![lane])
                .map_err(sql_err)?;
            Ok(removed as u64)
        })
        .await
    }
}

#[async_trait]
impl worklane_core::QueueStats for SqliteBroker {
    async fn pending_count(&self, lane: &Lane) -> Result<u64> {
        let lane = lane.as_str().to_string();
        self.run(move |conn| {
            // Lane-scoped count of live jobs (in-flight and scheduled included).
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM jobs WHERE lane = ?1",
                    params![lane],
                    |r| r.get(0),
                )
                .map_err(sql_err)?;
            Ok(u64::try_from(count).unwrap_or(0))
        })
        .await
    }
}

/// Map a `rusqlite` error into a worklane [`Error`], scrubbing credentials first.
pub(crate) fn sql_err(e: rusqlite::Error) -> Error {
    // Consistent with the other backends: scrub any credential-bearing URL the
    // driver might echo before it enters `Error` and reaches logs/dead-letters.
    Error::Broker(worklane_core::redact_credentials(&e.to_string()))
}
