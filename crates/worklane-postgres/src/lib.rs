//! PostgreSQL-backed durable [`Broker`] for `worklane`.
//!
//! Depend on this crate when a service already runs PostgreSQL and needs jobs to
//! survive process restarts. Application code still uses the `worklane` facade
//! for `Client` and `Worker`.
//!
//! Jobs are persisted in Postgres as a serialized
//! [`JobEnvelope`](worklane_core::JobEnvelope) blob plus a few denormalized
//! index columns (`lane`, `available_at`, `leased_until`, `receipt`), exactly
//! as the SQLite broker does. The difference is concurrency: a
//! `deadpool-postgres` connection pool lets multiple reservers run genuinely
//! concurrent `reserve`s, and `reserve` uses `SELECT … FOR UPDATE SKIP LOCKED` so
//! each grabs a distinct visible job without blocking — satisfying the broker
//! contract's no-double-hand-out guarantee under real connection concurrency.
//!
//! Time comes from an injected [`Clock`] so lease and visibility decisions are
//! deterministic and the shared conformance suite can drive them; the default
//! [`WallClock`] gives a restart-stable epoch for durability across restarts.
//! `WallClock` is monotonic non-decreasing for the broker's lifetime, so a
//! backward NTP step cannot reorder visibility/lease keys or re-hide in-flight
//! work. A large forward step can still expire a live lease early and widen the
//! at-least-once redelivery (duplicate) window; a slewing time daemon (e.g.
//! `chrony`) keeps forward corrections gradual.
//!
//! Tables live in a configurable schema (default `public`), so several isolated
//! brokers can share one database — used by the conformance tests.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::Pool;
use worklane_core::spi::{
    MAX_DEAD_LETTER_SWEEP, classify_state, decode_envelope, encode_envelope, nanos, receipt_key,
    stale,
};
use worklane_core::{
    BatchEnqueue, Broker, Clock, DeadLetter, Error, JobId, Lane, NewJob, Reservation,
    ReservationReceipt, Result, RetentionPolicy, UnboundedDlqWarning, WallClock,
};

mod conn;
mod dead_letters;
mod ident;
mod jobs;
mod queries;
mod result_store;
mod schedules;
mod schema;
pub use result_store::PostgresResultStore;

/// Re-export of the underlying `tokio_postgres` so callers of
/// [`PostgresBroker::enqueue_with_tx`] name the exact `Transaction` type the
/// broker expects, without taking their own (possibly mismatched) dependency.
pub use tokio_postgres;

use dead_letters::dead_letter_seq;
use jobs::find_valid_row_locked;
use queries::Queries;

/// The default visibility lease duration.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

/// The default connection-pool size.
pub const DEFAULT_POOL_SIZE: usize = 10;

/// A PostgreSQL-backed broker.
pub struct PostgresBroker {
    pool: Pool,
    clock: Arc<dyn Clock>,
    lease: Duration,
    schema: crate::ident::SafeSchema,
    queries: Queries,
    retention: RetentionPolicy,
    /// One-shot warning when dead-lettering under an unbounded retention policy.
    dlq_warning: UnboundedDlqWarning,
    /// Maximum times a job may be delivered before it is dead-lettered on the
    /// next reserve; `None` (default) means unbounded.
    max_deliveries: Option<u32>,
}

impl PostgresBroker {
    /// Connect to Postgres at `url` and use the `public` schema, with the system
    /// (wall-clock) clock and the default lease.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_schema(url, "public").await
    }

    /// Connect to Postgres at `url` and use schema `schema` (created if absent),
    /// with the wall-clock clock, the default lease, and a [`DEFAULT_POOL_SIZE`]
    /// connection pool. A per-broker schema lets isolated brokers share one
    /// database.
    pub async fn connect_with_schema(url: &str, schema: &str) -> Result<Self> {
        Self::connect_with_pool(url, schema, DEFAULT_POOL_SIZE).await
    }

    /// As [`connect_with_schema`](Self::connect_with_schema), but with an explicit
    /// connection-pool size. Useful when many brokers share one server (e.g. the
    /// conformance tests) and a large pool per broker would exhaust connections.
    pub async fn connect_with_pool(url: &str, schema: &str, max_size: usize) -> Result<Self> {
        let pool = conn::build_pool(url, max_size)?;
        Self::from_pool(pool, schema).await
    }

    /// Connect over TLS (rustls) at `url`, using the `public` schema. Use this
    /// for a `sslmode=require` server; the URL is still plain `postgres://`.
    /// Requires the `tls` feature.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(url: &str) -> Result<Self> {
        Self::connect_tls_with_pool(url, "public", DEFAULT_POOL_SIZE).await
    }

    /// As [`connect_with_pool`](Self::connect_with_pool), but the pool negotiates
    /// TLS (rustls) using the system root certificates. Requires the `tls` feature.
    #[cfg(feature = "tls")]
    pub async fn connect_tls_with_pool(url: &str, schema: &str, max_size: usize) -> Result<Self> {
        let pool = conn::build_pool_tls(url, max_size)?;
        Self::from_pool(pool, schema).await
    }

    /// Finish broker construction from an already-built connection pool, shared
    /// by the plaintext and TLS connect paths.
    async fn from_pool(pool: Pool, schema: &str) -> Result<Self> {
        let schema = crate::ident::SafeSchema::new(schema)
            .ok_or_else(|| Error::Broker(format!("invalid schema name {schema:?}")))?;

        let broker = PostgresBroker {
            pool,
            clock: Arc::new(WallClock::new()),
            lease: DEFAULT_LEASE,
            queries: Queries::new(&schema),
            schema,
            retention: RetentionPolicy::new(),
            dlq_warning: UnboundedDlqWarning::default(),
            max_deliveries: None,
        };
        broker.init_schema().await?;
        Ok(broker)
    }

    /// Obtain a `PostgresResultStore` that shares this broker's connection pool.
    pub fn result_store(&self) -> PostgresResultStore {
        PostgresResultStore::from_safe(self.pool.clone(), self.schema.clone())
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
    /// caller's own [`tokio_postgres::Transaction`] — instead of a pooled
    /// connection. The job becomes visible to workers only when the caller commits
    /// `tx`; rolling back undoes the enqueue with the business write, closing the
    /// dual-write gap.
    ///
    /// `tx` must be on a connection to the **same database** the broker uses. The
    /// insert addresses fully schema-qualified table names, so the caller's
    /// `search_path` is irrelevant — but the broker's schema must exist (it is
    /// created on connect).
    ///
    /// Isolation: the unique-key dedup relies on `ON CONFLICT` arbitration, which
    /// is well-defined under `READ COMMITTED` (the default). Under `SERIALIZABLE`
    /// the caller's transaction may surface a serialization failure to retry, as
    /// any `SERIALIZABLE` transaction can.
    ///
    /// ```no_run
    /// # use worklane_postgres::{PostgresBroker, tokio_postgres::Transaction};
    /// # use worklane_core::NewJob;
    /// # async fn demo(broker: &PostgresBroker, tx: &Transaction<'_>, job: NewJob) -> worklane_core::Result<()> {
    /// // ... the application's own business writes on `tx` ...
    /// let _id = broker.enqueue_with_tx(tx, job).await?;
    /// // caller commits `tx`: business write + enqueue commit together
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_with_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        job: NewJob,
    ) -> Result<JobId> {
        let available_at = nanos(self.clock.now().saturating_add(job.delay));
        self.insert_job(tx, job, available_at).await
    }

    /// Qualified table name `"schema".name`. `schema` is validated on connect;
    /// `name` is a `'static` literal (see [`SafeSchema::qualify`]).
    fn table(&self, name: &'static str) -> String {
        self.schema.qualify(name)
    }

    /// A snapshot of the dead-letter store across all lanes, for tests. This is a
    /// per-implementation convenience, not part of the [`Broker`] contract.
    pub async fn dead_letters(&self) -> Result<Vec<DeadLetter>> {
        let client = self.client().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT envelope, error FROM {} ORDER BY seq",
                    self.table("dead")
                ),
                &[],
            )
            .await
            .map_err(pg_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let error: String = row.get(1);
            out.push(DeadLetter::new(decode_envelope(&blob)?, error));
        }
        Ok(out)
    }

    async fn client(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(|e| {
            // A deadpool checkout error can carry the connection config (DSN with
            // its password); it bypasses `pg_err`, so redact here too — the same
            // scrub the result store applies to its own checkout.
            Error::Broker(worklane_core::redact_credentials(&format!(
                "pool checkout failed: {e}"
            )))
        })
    }

    /// Take a transaction-scoped advisory lock that serialises contention on a
    /// `unique_key` *within this broker's schema*. Advisory locks are cluster-wide,
    /// not schema-scoped, so the schema is folded into the lock identity: two
    /// schema-isolated brokers sharing a `unique_key` string must not contend on
    /// the same lock (that would leak isolation into cross-tenant blocking). The
    /// two-key form `(hashtext(schema), hashtext(key))` namespaces the lock by
    /// schema without an in-band separator — Postgres `text` cannot carry a NUL
    /// and `unique_key` may contain arbitrary characters, so no separator string
    /// is collision-free. The lock only orders contention; the per-schema
    /// `unique_keys` INSERT still arbitrates dedup.
    async fn lock_unique_key(&self, tx: &tokio_postgres::Transaction<'_>, key: &str) -> Result<()> {
        let schema = self.schema.as_str();
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
            &[&schema, &key],
        )
        .await
        .map_err(pg_err)?;
        Ok(())
    }

    /// Begin a transaction at an explicit `READ COMMITTED` isolation level.
    ///
    /// The enqueue/dedup paths rely on `ON CONFLICT DO NOTHING` taking a row lock
    /// that blocks the losing inserter until the winner commits (the unique-key
    /// PRIMARY KEY is the serialization point). That argument holds only under
    /// `READ COMMITTED`; under `REPEATABLE READ`/`SERIALIZABLE` the loser would
    /// raise a serialization failure and break the all-or-nothing batch contract.
    /// Pinning the level here makes the dedup contract self-documenting and immune
    /// to a server-default change, rather than silently depending on it.
    async fn begin(
        client: &mut deadpool_postgres::Object,
    ) -> Result<deadpool_postgres::Transaction<'_>> {
        client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await
            .map_err(pg_err)
    }

    /// Insert one job into an open transaction, becoming visible at `available_at`,
    /// returning its id. A live job already holding the same uniqueness key wins:
    /// its id (from the existing envelope) is returned and the speculative row is
    /// discarded. The single insertion path shared by `enqueue` and
    /// `enqueue_batch`; both unify on deleting the lost-race row (equivalent to a
    /// rollback for a single job).
    async fn insert_job(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        job: NewJob,
        available_at: i64,
    ) -> Result<JobId> {
        let unique_key = job.unique_key.clone();
        // Deduplicate: a live job already holding this key wins; its id comes from
        // the existing job's envelope.
        if let Some(key) = &unique_key {
            let existing = tx
                .query_opt(
                    &format!(
                        "SELECT j.envelope FROM {uk} u JOIN {jobs} j ON j.seq = u.seq \
                         WHERE u.unique_key = $1",
                        uk = self.table("unique_keys"),
                        jobs = self.table("jobs"),
                    ),
                    &[key],
                )
                .await
                .map_err(pg_err)?;
            if let Some(row) = existing {
                let blob: Vec<u8> = row.get(0);
                return Ok(decode_envelope(&blob)?.id);
            }
        }
        let id = job.id;
        let envelope = job.into_envelope();
        let blob = encode_envelope(&envelope)?;
        // Idempotent on JobId: `ON CONFLICT (id)` (the UNIQUE index from schema
        // v12) makes a re-enqueue of an id a live job already holds a no-op —
        // `query_opt` returns no row — and we return that id instead of creating a
        // second job.
        let seq: i64 = match tx
            .query_opt(
                &format!(
                    "INSERT INTO {} (id, receipt, lane, priority, available_at, leased_until, envelope) \
                     VALUES ($1, NULL, $2, $3, $4, NULL, $5) ON CONFLICT (id) DO NOTHING RETURNING seq",
                    self.table("jobs")
                ),
                &[&envelope.id.to_string(), &envelope.lane.as_str(), &(envelope.priority as i16), &available_at, &blob],
            )
            .await
            .map_err(pg_err)?
        {
            Some(row) => row.get(0),
            None => return Ok(id),
        };
        if let Some(key) = &unique_key {
            // The unique-key PRIMARY KEY is the sole arbiter. The initial SELECT
            // can race (two enqueues both see no key under READ COMMITTED), so we
            // claim via `INSERT … ON CONFLICT DO NOTHING` and loop: one row
            // inserted ⇒ we own the key (done). Zero ⇒ someone holds it, so re-read
            // the holder and dedup to it — but the holder may have been ack'd/
            // fail'd in the meantime (its rows deleted, key freed), making the
            // re-read `None`; loop to re-attempt the claim instead of erroring on a
            // missing row (`query_opt`, not `query_one`).
            //
            // The loop normally settles in one or two turns (a contender blocks on
            // the holder's uncommitted row lock, then either claims or dedups once
            // it commits). It only re-spins when the holder vanished between the
            // failed insert and the re-read — a freed key — so a turn that neither
            // claims nor finds a holder means another transaction is churning the
            // same key (enqueue→resolve) in lockstep with us. That cannot recur
            // unboundedly in practice, but nothing in the protocol *guarantees*
            // progress, so cap the spins and surface contention as a retryable
            // broker error rather than pinning a connection forever.
            const MAX_CLAIM_ATTEMPTS: u32 = 16;
            let mut attempts = 0u32;
            loop {
                attempts += 1;
                if attempts > MAX_CLAIM_ATTEMPTS {
                    return Err(Error::Broker(format!(
                        "unique_key claim did not converge after {MAX_CLAIM_ATTEMPTS} attempts \
                         (sustained contention on the key); retry the enqueue"
                    )));
                }
                let inserted = tx
                    .execute(
                        &format!(
                            "INSERT INTO {} (unique_key, seq) VALUES ($1, $2) \
                             ON CONFLICT (unique_key) DO NOTHING",
                            self.table("unique_keys")
                        ),
                        &[key, &seq],
                    )
                    .await
                    .map_err(pg_err)?;
                if inserted == 1 {
                    break;
                }
                let existing = tx
                    .query_opt(
                        &format!(
                            "SELECT j.envelope FROM {uk} u JOIN {jobs} j ON j.seq = u.seq \
                             WHERE u.unique_key = $1",
                            uk = self.table("unique_keys"),
                            jobs = self.table("jobs"),
                        ),
                        &[key],
                    )
                    .await
                    .map_err(pg_err)?;
                if let Some(row) = existing {
                    // A live holder exists: discard our speculative job, dedup to it.
                    let existing_blob: Vec<u8> = row.get(0);
                    let existing_id = decode_envelope(&existing_blob)?.id;
                    tx.execute(
                        &format!("DELETE FROM {} WHERE seq = $1", self.table("jobs")),
                        &[&seq],
                    )
                    .await
                    .map_err(pg_err)?;
                    return Ok(existing_id);
                }
                // Holder vanished (resolved) → the key is free; retry the claim.
            }
        }
        Ok(id)
    }

    /// No-unique-key batch fast path: store an entire batch with one multi-row
    /// `UNNEST` insert, skipping the per-row dedup/claim machinery `insert_job`
    /// runs for unique keys. Every envelope is encoded up front, so an
    /// unencodable job returns `Err` before any row is written and the dropped
    /// transaction rolls the whole batch back (all-or-nothing). The bound arrays
    /// are built in input order and the statement pins `seq` assignment to that
    /// order (`WITH ORDINALITY … ORDER BY ord`), so the batch reserves back
    /// strict-FIFO. Returns the input ids in order; `ON CONFLICT (id) DO NOTHING`
    /// makes a re-enqueue of a live id a no-op while still returning that id,
    /// matching `insert_job`. Caller guarantees every job has `unique_key ==
    /// None`.
    async fn insert_batch_unnest(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        jobs: Vec<NewJob>,
        now_d: Duration,
    ) -> Result<Vec<JobId>> {
        let n = jobs.len();
        let mut ids = Vec::with_capacity(n);
        let mut id_strs: Vec<String> = Vec::with_capacity(n);
        let mut lanes: Vec<String> = Vec::with_capacity(n);
        let mut priorities: Vec<i16> = Vec::with_capacity(n);
        let mut available_ats: Vec<i64> = Vec::with_capacity(n);
        let mut envelopes: Vec<Vec<u8>> = Vec::with_capacity(n);
        for job in jobs {
            let available_at = nanos(now_d.saturating_add(job.delay));
            let envelope = job.into_envelope();
            let blob = encode_envelope(&envelope)?;
            id_strs.push(envelope.id.to_string());
            lanes.push(envelope.lane.as_str().to_string());
            priorities.push(envelope.priority as i16);
            available_ats.push(available_at);
            envelopes.push(blob);
            ids.push(envelope.id);
        }
        tx.execute(
            &self.queries.enqueue_batch_unnest,
            &[&id_strs, &lanes, &priorities, &available_ats, &envelopes],
        )
        .await
        .map_err(pg_err)?;
        Ok(ids)
    }
}

#[async_trait]
impl BatchEnqueue for PostgresBroker {
    async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        let now_d = self.clock.now();
        let mut client = self.client().await?;
        let tx = Self::begin(&mut client).await?;

        // Fast path: a batch with no unique keys needs no dedup arbitration, so
        // skip the per-row claim machinery and store the whole batch with one
        // multi-row UNNEST insert. An empty batch also lands here (all() is true
        // over no jobs) and inserts nothing. Any unique-key job routes to the
        // per-row path below, unchanged.
        if jobs.iter().all(|j| j.unique_key.is_none()) {
            let ids = self.insert_batch_unnest(&tx, jobs, now_d).await?;
            tx.commit().await.map_err(pg_err)?;
            return Ok(ids);
        }

        // Acquire the unique-key locks up front in a globally consistent (sorted)
        // order so two concurrent batches sharing keys in opposite order cannot
        // form a lock-ordering cycle and deadlock (SQLSTATE 40P01). The per-job
        // `unique_keys` INSERT below still arbitrates dedup; this only fixes the
        // ORDER in which concurrent transactions contend. Because every batch
        // acquires in sorted-key order, the wait-for graph cannot cycle (a hash
        // collision merely shares one lock — harmless). The insert loop is left
        // untouched, so `seq` assignment (and the input-order FIFO guarantee) is
        // preserved. Advisory locks are transaction-scoped and release on commit.
        let mut keys: Vec<&str> = jobs
            .iter()
            .filter_map(|j| j.unique_key.as_deref())
            .collect();
        keys.sort_unstable();
        keys.dedup();
        for key in keys {
            self.lock_unique_key(&tx, key).await?;
        }

        let mut ids = Vec::with_capacity(jobs.len());
        for job in jobs {
            let available_at = nanos(now_d.saturating_add(job.delay));
            ids.push(self.insert_job(&tx, job, available_at).await?);
        }
        tx.commit().await.map_err(pg_err)?;
        Ok(ids)
    }
}

#[async_trait]
impl Broker for PostgresBroker {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let available_at = nanos(self.clock.now().saturating_add(job.delay));
        let mut client = self.client().await?;
        let tx = Self::begin(&mut client).await?;
        let id = self.insert_job(&tx, job, available_at).await?;
        tx.commit().await.map_err(pg_err)?;
        Ok(id)
    }

    fn batch_enqueue(&self) -> Option<&dyn BatchEnqueue> {
        Some(self)
    }

    async fn reserve(&self, lane: &Lane) -> Result<Option<Reservation>> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let lease_until = nanos(now_d.saturating_add(self.lease));
        let receipt = ReservationReceipt::new();
        let key = receipt_key(&receipt)?;
        match self.max_deliveries {
            // Fast path (unbounded): one atomic UPDATE...RETURNING leases the
            // oldest visible job and increments its delivery count. FOR UPDATE SKIP
            // LOCKED lets concurrent reservers each take a distinct row, so a leased
            // job is never handed to a second reserve.
            None => {
                let client = self.client().await?;
                let row = client
                    .query_opt(
                        &self.queries.reserve,
                        &[&key, &lease_until, &lane.as_str(), &now],
                    )
                    .await
                    .map_err(pg_err)?;
                match row {
                    Some(r) => {
                        let blob: Vec<u8> = r.get(0);
                        Ok(Some(Reservation::new(
                            decode_envelope(&blob)?,
                            receipt,
                            self.lease,
                        )))
                    }
                    None => Ok(None),
                }
            }
            // Bounded path: in one transaction, pick the next visible candidate
            // (FOR UPDATE SKIP LOCKED, so concurrent reservers take distinct rows);
            // if it has already been delivered `max` times, dead-letter it (a poison
            // pill) and pick the next, else lease it and bump its count. The locked
            // select makes pick-then-resolve atomic so a concurrent reserver cannot
            // interleave on the same row.
            Some(max) => {
                let mut client = self.client().await?;
                let tx = Self::begin(&mut client).await?;
                let select = format!(
                    "SELECT seq, deliveries, envelope FROM {jobs} \
                     WHERE lane = $1 AND available_at <= $2 \
                       AND (leased_until IS NULL OR leased_until <= $2) \
                     ORDER BY priority DESC, available_at ASC, seq ASC \
                     FOR UPDATE SKIP LOCKED \
                     LIMIT 1",
                    jobs = self.table("jobs")
                );
                let lease_update = format!(
                    "UPDATE {jobs} SET receipt = $1, leased_until = $2, \
                     deliveries = deliveries + 1 WHERE seq = $3 RETURNING envelope",
                    jobs = self.table("jobs")
                );
                // Bound how many poison jobs a single `reserve` dead-letters before
                // returning empty-handed: a large backlog of over-max jobs would
                // otherwise dead-letter the whole batch inside one long-held write
                // transaction (the rows are `FOR UPDATE`-locked for its duration).
                // After the cap we yield with no reservation; the next `reserve`
                // resumes the sweep. Bounded progress beats one unbounded transaction.
                let mut swept = 0u32;
                let outcome = loop {
                    let candidate = tx
                        .query_opt(&select, &[&lane.as_str(), &now])
                        .await
                        .map_err(pg_err)?;
                    let Some(c) = candidate else {
                        break None;
                    };
                    let seq: i64 = c.get(0);
                    let deliveries: i64 = c.get(1);
                    let blob: Vec<u8> = c.get(2);
                    if deliveries.saturating_add(1) > i64::from(max) {
                        dead_letter_seq(
                            self,
                            &tx,
                            seq,
                            &blob,
                            format!("exceeded max deliveries ({max})"),
                            now,
                        )
                        .await?;
                        swept += 1;
                        if swept >= MAX_DEAD_LETTER_SWEEP {
                            break None;
                        }
                        continue;
                    }
                    let r = tx
                        .query_one(&lease_update, &[&key, &lease_until, &seq])
                        .await
                        .map_err(pg_err)?;
                    let leased_blob: Vec<u8> = r.get(0);
                    break Some(Reservation::new(
                        decode_envelope(&leased_blob)?,
                        receipt,
                        self.lease,
                    ));
                };
                tx.commit().await.map_err(pg_err)?;
                Ok(outcome)
            }
        }
    }

    async fn ack(&self, receipt: ReservationReceipt) -> Result<()> {
        let now = nanos(self.clock.now());
        let key = receipt_key(&receipt)?;
        let mut client = self.client().await?;
        // The guarded delete and the uniqueness-key release run in one transaction:
        // a crash between them must not leave an orphaned `unique_keys` row, which
        // would wedge every future enqueue of that key (its `seq` JOINs to no live
        // job). The `receipt`/`leased_until` predicate is the same validity check as
        // `find_valid_row`, so concurrent acks with the same receipt delete the row
        // at most once — the loser matches no row and is rejected as stale.
        // RETURNING `seq` lets the winner free its key in the same transaction.
        let tx = Self::begin(&mut client).await?;
        let row = tx
            .query_opt(&self.queries.ack_delete_returning_seq, &[&key, &now])
            .await
            .map_err(pg_err)?;
        match row {
            Some(r) => {
                let seq: i64 = r.get(0);
                tx.execute(&self.queries.delete_unique_by_seq, &[&seq])
                    .await
                    .map_err(pg_err)?;
                tx.commit().await.map_err(pg_err)?;
                Ok(())
            }
            // `tx` drops here → rollback; nothing was modified on this path.
            None => Err(stale(receipt)),
        }
    }

    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        // `delay` is an arbitrary caller-supplied `Duration` (public `Broker::retry`
        // contract); saturate so a near-`Duration::MAX` delay cannot panic the
        // addition before `nanos` clamps to `i64::MAX`.
        let available_at = nanos(now_d.saturating_add(delay));
        let mut client = self.client().await?;
        // A `FOR UPDATE` transaction: the row lock serializes concurrent
        // same-receipt retries, so the loser finds the receipt already cleared
        // and is rejected as stale — `attempts` is bumped exactly once.
        let tx = Self::begin(&mut client).await?;
        let (seq, blob) = find_valid_row_locked(&tx, self, receipt, now).await?;
        let mut envelope = decode_envelope(&blob)?;
        // Saturate: `attempts` comes from the stored (possibly corrupted) blob; a
        // wrap at `u32::MAX` would silently reset the retry counter and retry
        // forever. Reaching `u32::MAX` legitimately is impractical.
        envelope.attempts = envelope.attempts.saturating_add(1);
        let new_blob = encode_envelope(&envelope)?;
        tx.execute(
            &self.queries.retry_update,
            &[&new_blob, &available_at, &seq],
        )
        .await
        .map_err(pg_err)?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }

    async fn defer(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let available_at = nanos(now_d.saturating_add(delay));
        let mut client = self.client().await?;
        // Same `FOR UPDATE` CAS as `retry`, but the envelope is left untouched —
        // `defer` reschedules without advancing `attempts`.
        let tx = Self::begin(&mut client).await?;
        let (seq, _blob) = find_valid_row_locked(&tx, self, receipt, now).await?;
        tx.execute(
            &format!(
                "UPDATE {} SET available_at = $1, leased_until = NULL, receipt = NULL \
                 WHERE seq = $2",
                self.table("jobs")
            ),
            &[&available_at, &seq],
        )
        .await
        .map_err(pg_err)?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }

    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()> {
        let now = nanos(self.clock.now());
        self.dlq_warning.warn_once(&self.retention);
        let mut client = self.client().await?;
        // A `FOR UPDATE` transaction: the row lock serializes concurrent
        // same-receipt fails, so the loser finds the row already gone and is
        // rejected as stale — the dead-letter is written exactly once.
        let tx = Self::begin(&mut client).await?;
        let (seq, blob) = find_valid_row_locked(&tx, self, receipt, now).await?;
        // Move to the dead-letter store, release the uniqueness key (retaining it
        // on the dead record for a later requeue), delete the live job, and enforce
        // retention — all atomically. Shared with the delivery-bound dead-lettering
        // in `reserve`.
        dead_letter_seq(self, &tx, seq, &blob, error, now).await?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }

    async fn extend(&self, receipt: ReservationReceipt) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let lease_until = nanos(now_d.saturating_add(self.lease));
        let key = receipt_key(&receipt)?;
        let client = self.client().await?;
        // The same guard as the other resolutions: an expired or superseded
        // receipt matches no row and is rejected without touching the job.
        let changed = client
            .execute(&self.queries.extend, &[&lease_until, &key, &now])
            .await
            .map_err(pg_err)?;
        if changed == 0 {
            return Err(stale(receipt));
        }
        Ok(())
    }

    async fn classify(&self, id: JobId) -> Result<worklane_core::JobState> {
        let key = id.to_string();
        let client = self.client().await?;
        // Atomic check across both tables in a single query.
        let row = client
            .query_opt(
                &format!(
                    "SELECT 1 FROM {jobs} WHERE id = $1 UNION ALL SELECT 2 FROM {dead} WHERE id = $1 LIMIT 1",
                    jobs = self.table("jobs"),
                    dead = self.table("dead")
                ),
                &[&key],
            )
            .await
            .map_err(pg_err)?;
        let state: Option<i32> = row.map(|r| r.get(0));
        Ok(classify_state(state.map(i64::from)))
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
impl worklane_core::DeadLetterStore for PostgresBroker {
    async fn read_dead_letters(&self, lane: &Lane, limit: usize) -> Result<Vec<DeadLetter>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let client = self.client().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT envelope, error FROM {} WHERE lane = $1 ORDER BY seq LIMIT $2",
                    self.table("dead")
                ),
                &[&lane.as_str(), &limit],
            )
            .await
            .map_err(pg_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let error: String = row.get(1);
            out.push(DeadLetter::new(decode_envelope(&blob)?, error));
        }
        Ok(out)
    }

    async fn count_dead_letters(&self, lane: &Lane) -> Result<u64> {
        let client = self.client().await?;
        // Lane-scoped, non-destructive count.
        let row = client
            .query_one(
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE lane = $1",
                    self.table("dead")
                ),
                &[&lane.as_str()],
            )
            .await
            .map_err(pg_err)?;
        let count: i64 = row.get(0);
        Ok(u64::try_from(count).unwrap_or(0))
    }

    async fn requeue(&self, id: JobId) -> Result<()> {
        let now = nanos(self.clock.now());
        let key = id.to_string();
        let mut client = self.client().await?;
        let tx = Self::begin(&mut client).await?;
        // Atomic move from the dead-letter store back to the live store; a
        // missing record selects no row and is rejected with no writes.
        // `FOR UPDATE` locks the dead row so two concurrent `requeue(id)` calls
        // serialize: the loser blocks, then finds the row already gone and is
        // rejected — without it both could read the same record and each insert
        // a live job, duplicating one dead-letter into two live jobs.
        let row = tx
            .query_opt(
                &format!(
                    "SELECT envelope, lane, unique_key FROM {} WHERE id = $1 FOR UPDATE",
                    self.table("dead")
                ),
                &[&key],
            )
            .await
            .map_err(pg_err)?;
        let Some(row) = row else {
            return Err(Error::Broker(format!("no dead-letter record for job {id}")));
        };
        let blob: Vec<u8> = row.get(0);
        let lane: String = row.get(1);
        let unique_key: Option<String> = row.get(2);
        let live_id_held = tx
            .query_opt(
                &format!("SELECT 1 FROM {} WHERE id = $1", self.table("jobs")),
                &[&key],
            )
            .await
            .map_err(pg_err)?
            .is_some();
        if live_id_held {
            return Err(Error::LiveJobIdConflict(format!(
                "cannot requeue job {id}: a live job with the same id already exists"
            )));
        }
        // If the job held a unique key, re-acquire it — unless another live job now
        // holds it (the key was freed at fail time). On conflict, reject with no
        // writes: `tx` drops, rolling back, and the job stays dead-lettered.
        if let Some(uk) = &unique_key {
            let held = tx
                .query_opt(
                    &format!(
                        "SELECT 1 FROM {} WHERE unique_key = $1",
                        self.table("unique_keys")
                    ),
                    &[uk],
                )
                .await
                .map_err(pg_err)?
                .is_some();
            if held {
                return Err(Error::UniqueKeyHeld(format!(
                    "cannot requeue job {id}: unique key {uk:?} is held by another live job"
                )));
            }
        }
        let envelope = decode_envelope(&blob)?;
        let envelope_id = envelope.id.to_string();
        let seq: i64 = tx
            .query_one(
                &format!(
                    "INSERT INTO {} (id, receipt, lane, priority, available_at, leased_until, envelope) \
                     VALUES ($1, NULL, $2, $3, $4, NULL, $5) RETURNING seq",
                    self.table("jobs")
                ),
                &[&envelope_id, &lane, &(envelope.priority as i16), &now, &blob],
            )
            .await
            .map_err(pg_err)?
            .get(0);
        if let Some(uk) = &unique_key {
            // The unique-key PRIMARY KEY is the arbiter: the SELECT above is a
            // fast-path reject, but a concurrent `enqueue` can claim the key in the
            // window between it and here. `ON CONFLICT DO NOTHING` + a zero-row
            // check turns that lost race into the contractual `UniqueKeyHeld`
            // (rolling back via `tx` drop) instead of a raw duplicate-key
            // `Error::Broker`.
            let claimed = tx
                .execute(
                    &format!(
                        "INSERT INTO {} (unique_key, seq) VALUES ($1, $2) \
                         ON CONFLICT (unique_key) DO NOTHING",
                        self.table("unique_keys")
                    ),
                    &[uk, &seq],
                )
                .await
                .map_err(pg_err)?;
            if claimed == 0 {
                return Err(Error::UniqueKeyHeld(format!(
                    "cannot requeue job {id}: unique key {uk:?} is held by another live job"
                )));
            }
        }
        tx.execute(
            &format!("DELETE FROM {} WHERE id = $1", self.table("dead")),
            &[&key],
        )
        .await
        .map_err(pg_err)?;
        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }

    async fn purge_dead_letters(&self, lane: &Lane) -> Result<u64> {
        let client = self.client().await?;
        // Lane-scoped, destructive: delete every dead record for `lane`.
        let removed = client
            .execute(
                &format!("DELETE FROM {} WHERE lane = $1", self.table("dead")),
                &[&lane.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(removed)
    }
}

#[async_trait]
impl worklane_core::QueueStats for PostgresBroker {
    async fn pending_count(&self, lane: &Lane) -> Result<u64> {
        let client = self.client().await?;
        // Lane-scoped count of live jobs (in-flight and scheduled included).
        let row = client
            .query_one(
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE lane = $1",
                    self.table("jobs")
                ),
                &[&lane.as_str()],
            )
            .await
            .map_err(pg_err)?;
        let count: i64 = row.get(0);
        Ok(u64::try_from(count).unwrap_or(0))
    }
}

pub(crate) fn pg_err(e: tokio_postgres::Error) -> Error {
    // A connection error can echo the DSN (with its password); redact before the
    // string enters `Error` and flows on to logs and dead-letter reasons.
    Error::Broker(worklane_core::redact_credentials(&e.to_string()))
}
