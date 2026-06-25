//! Redis-backed durable [`Broker`] for `worklane`.
//!
//! Depend on this crate when a service already runs a single Redis node (or a
//! primary with replicas) and needs jobs to survive process restarts.
//! Application code still uses the `worklane` facade for `Client` and `Worker`.
//!
//! Redis has no row locks and no conditional multi-statement transactions, so
//! atomic reserve/resolve comes from **Lua scripts**: Redis runs each script to
//! completion single-threaded, which is what guarantees no-double-hand-out and
//! the receipt guards — the non-SQL answer to the broker design gate.
//!
//! **Single-node only — not Redis Cluster.** A single lifecycle operation
//! touches several keys (`ns:job:{id}`, `ns:lane:{lane}`, `ns:rcpt:{receipt}`,
//! `ns:unique:{key}`), and the scripts compute those key names inside Lua rather
//! than declaring them all in `KEYS[]`. On Redis Cluster those keys hash to
//! different slots, so the server rejects the `EVAL` with `CROSSSLOT`. This
//! broker targets a single Redis node (or a primary with replicas); it does not
//! support a clustered key space.
//!
//! **No key eviction.** The broker stores a job across coordinated keys: a lane
//! ZSET member, a job HASH, optional receipt and unique-key indexes, and
//! dead-letter indexes. Redis must be configured so worklane keys are not evicted
//! under memory pressure (for example `maxmemory-policy noeviction`). An
//! all-keys eviction policy can remove one side of those relationships and leave
//! stale lane or dead-letter members behind.
//!
//! Data model, under a key `namespace` (default `worklane`):
//! - `ns:lane:{lane}` — ZSET scored by *next-visible time* (`available_at`, or
//!   `leased_until` once leased so an expired lease reclaims). Each member is
//!   `<seq>:<id>`: the zero-padded enqueue sequence prefix makes equal-score
//!   (same-visibility) jobs sort by enqueue order — strict FIFO (implemented by
//!   the internal Lua script helpers).
//! - `ns:lane:{lane}:prios` — ZSET of the priority levels currently in use on the
//!   lane (member = score = priority). `reserve` sweeps these highest-first
//!   instead of all 0..255, pruning a level once its per-priority ZSET drains.
//! - `ns:job:{id}` — HASH: `envelope`, `lane`, `available_at`, `leased_until`,
//!   `receipt`, `attempts`, `deliveries`, `seq`. `attempts` is the source of truth
//!   (bumped with `HINCRBY`); it is patched into the opaque envelope on every read.
//!   `deliveries` counts reservations handed out (for the `max_deliveries` bound),
//!   distinct from `attempts`. `seq` lets the resolve scripts rebuild the
//!   lane-ZSET member from the job id.
//! - `ns:seq` — monotonic counter (`INCR`) handing out per-enqueue sequences.
//! - `ns:rcpt:{receipt}` — reverse index to the job id for receipt resolution.
//! - `ns:dead:{lane}` — ZSET of dead-letter members (`<seq>:<id>`) scored by
//!   `dead_at`, so count is `ZCARD`, age-prune is `ZREMRANGEBYSCORE`, and
//!   count-prune is `ZREMRANGEBYRANK`; `ns:dead:job:{id}` — HASH of
//!   the dead record (carries `member` so `requeue`/prune address the ZSET).
//!
//! All time is the injected [`Clock`], passed into every script as an argument
//! (never Redis `TIME`), so visibility is deterministic and the conformance
//! suite can advance a manual clock. The default [`WallClock`] gives a
//! restart-stable epoch and is monotonic non-decreasing for the broker's
//! lifetime, so a backward NTP step cannot reorder visibility/lease keys or
//! re-hide in-flight work. A large forward step can still expire a live lease
//! early and widen the at-least-once redelivery (duplicate-execution) window;
//! a slewing time daemon (e.g. `chrony`) keeps forward corrections gradual.
//!
//! Time values (`available_at`, `leased_until`) are integer nanoseconds but are
//! stored as ZSET scores, which are IEEE-754 doubles (53-bit integer mantissa).
//! Epoch-nanosecond magnitudes exceed 2^53, so scores lose sub-~256ns precision
//! and two jobs enqueued within that window collide on score. That no longer
//! perturbs ordering: a score tie falls through to the member's `<seq>:` prefix,
//! which orders by enqueue sequence (FIFO) regardless of the lost sub-256ns
//! difference. Sub-microsecond *visibility/scheduling* is still coarse — a job
//! due 100ns later may become visible in the same tick — but the *reserve order*
//! is exact.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use worklane_core::spi::{
    MAX_DEAD_LETTER_SWEEP, SCHEMA_VERSION, SchemaVersionCheck, check_schema_version,
    classify_state, decode_envelope, encode_envelope, nanos, receipt_key, stale,
};
use worklane_core::{
    BatchEnqueue, Broker, Clock, DeadLetter, Error, JobEnvelope, JobId, Lane, NewJob, Reservation,
    ReservationReceipt, Result, RetentionPolicy, UnboundedDlqWarning, WallClock,
};

mod result_store;
mod scripts;
pub use result_store::RedisResultStore;

/// The default visibility lease duration.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

/// The `(envelope, error, attempts)` fields of one dead-record hash, each
/// `Option` so a concurrent delete (nil reply) is tolerated rather than erroring.
type DeadFields = (Option<Vec<u8>>, Option<String>, Option<u32>);

/// A Redis-backed broker.
pub struct RedisBroker {
    conn: ConnectionManager,
    clock: Arc<dyn Clock>,
    lease: Duration,
    namespace: String,
    retention: RetentionPolicy,
    /// One-shot warning when dead-lettering under an unbounded retention policy.
    dlq_warning: UnboundedDlqWarning,
    /// Maximum times a job may be delivered before it is dead-lettered on the
    /// next reserve; `None` (default) means unbounded.
    max_deliveries: Option<u32>,
    /// Lifecycle Lua scripts, each built and SHA1-hashed once here and reused on
    /// every operation (the Redis analogue of the Postgres broker's precomputed
    /// `Queries`).
    scripts: scripts::Scripts,
}

impl RedisBroker {
    /// Connect to Redis at `url` using the `worklane` key namespace, the system
    /// (wall-clock) clock, and the default lease.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_namespace(url, "worklane").await
    }

    /// Connect to Redis at `url` using key prefix `namespace`, the wall-clock
    /// clock, and the default lease. A per-broker namespace lets isolated brokers
    /// share one server.
    pub async fn connect_with_namespace(url: &str, namespace: &str) -> Result<Self> {
        // The namespace is the prefix of every key (and of the `dead:job:*` SCAN
        // pattern). `:` is fine here — it only nests the prefix deeper and cannot
        // collide across namespaces — but an empty namespace or one bearing a glob
        // metacharacter would corrupt the scan, so reject those.
        if namespace.is_empty() {
            return Err(Error::Broker(
                "redis namespace must not be empty".to_string(),
            ));
        }
        if let Some(c) = namespace
            .chars()
            .find(|c| matches!(c, '*' | '?' | '[' | ']'))
        {
            return Err(Error::Broker(format!(
                "redis namespace {namespace:?} contains glob character {c:?}; \
                 avoid '*', '?', '[', ']'"
            )));
        }
        let client = redis::Client::open(url).map_err(redis_err)?;
        let conn = ConnectionManager::new(client).await.map_err(redis_err)?;
        let broker = RedisBroker {
            conn,
            clock: Arc::new(WallClock::new()),
            lease: DEFAULT_LEASE,
            namespace: namespace.to_string(),
            retention: RetentionPolicy::new(),
            dlq_warning: UnboundedDlqWarning::default(),
            max_deliveries: None,
            scripts: scripts::Scripts::new(),
        };
        broker.check_version().await?;
        Ok(broker)
    }

    /// Obtain a `RedisResultStore` that shares this broker's connection manager and namespace.
    pub fn result_store(&self) -> RedisResultStore {
        RedisResultStore::new(self.conn.clone(), &self.namespace)
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

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.namespace)
    }

    /// Stamp the schema version (if unset) and reject storage written under a
    /// newer, unknown version.
    async fn check_version(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let key = self.key("schema_version");
        let current: Option<i64> = conn.get(&key).await.map_err(redis_err)?;
        match check_schema_version(current) {
            SchemaVersionCheck::Fresh => {
                let _: () = conn.set(&key, SCHEMA_VERSION).await.map_err(redis_err)?;
            }
            SchemaVersionCheck::Match => {}
            // Redis is drain-don't-migrate: a store at any other version was written
            // under a different key layout that the current code would misread.
            // Reject (rather than bump the stamp) so the operator flushes the
            // namespace and re-enqueues. Pre-1.0 there is no in-place migration.
            SchemaVersionCheck::Mismatch(v) => {
                return Err(Error::Broker(format!(
                    "redis storage schema version {v} is not the supported baseline \
                     {SCHEMA_VERSION}; worklane is pre-1.0 and does not migrate redis storage \
                     in place — flush the namespace and re-enqueue (or upgrade worklane if this \
                     store is newer)"
                )));
            }
        }
        Ok(())
    }

    /// Decode the opaque envelope blob and override its `attempts` with the
    /// broker-managed counter (the source of truth).
    fn envelope_with_attempts(blob: &[u8], attempts: u32) -> Result<JobEnvelope> {
        let mut envelope = decode_envelope(blob)?;
        envelope.attempts = attempts;
        Ok(envelope)
    }

    /// A snapshot of the dead-letter store across **all** lanes, for inspection
    /// and tests. A per-implementation convenience, **not** part of the [`Broker`]
    /// contract.
    ///
    /// This is unbounded — it `SCAN`s the whole dead-letter keyspace — so it is for
    /// diagnostics, not a hot path. The bounded, contract production APIs are
    /// [`read_dead_letters`](worklane_core::DeadLetterStore::read_dead_letters) (lane-scoped,
    /// `limit`-bounded) and [`count_dead_letters`](worklane_core::DeadLetterStore::count_dead_letters).
    pub async fn dead_letters(&self) -> Result<Vec<DeadLetter>> {
        let mut conn = self.conn.clone();
        let pattern = self.key("dead:job:*");
        let mut keys: Vec<String> = Vec::new();
        {
            let mut iter = conn
                .scan_match::<_, String>(&pattern)
                .await
                .map_err(redis_err)?;
            while let Some(k) = iter.next_item().await {
                keys.push(k.map_err(redis_err)?);
            }
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        // Read every hash in ONE pipelined round-trip rather than N separate
        // HMGETs (the prior N+1). Each field is decoded as an `Option`: a
        // concurrent requeue/prune can `DEL` a hash between the SCAN and this read,
        // returning nil; the optional decode tolerates that race (a non-optional
        // decode would error the whole reply) and the row is skipped below —
        // matching the production `read_dead_letters` path.
        let mut pipe = redis::pipe();
        for k in &keys {
            pipe.cmd("HMGET")
                .arg(k)
                .arg("envelope")
                .arg("error")
                .arg("attempts");
        }
        let rows: Vec<DeadFields> = pipe.query_async(&mut conn).await.map_err(redis_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for (blob, error, attempts) in rows {
            // Skip a row whose hash was deleted by a concurrent requeue/prune.
            let (Some(blob), Some(error), Some(attempts)) = (blob, error, attempts) else {
                continue;
            };
            out.push(DeadLetter::new(
                Self::envelope_with_attempts(&blob, attempts)?,
                error,
            ));
        }
        Ok(out)
    }
}

#[async_trait]
impl BatchEnqueue for RedisBroker {
    async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }

        let mut invoker = self.scripts.enqueue_batch.arg(&self.namespace);

        for job in jobs {
            lane_key_segment(&job.lane)?;
            let available_at = nanos(self.clock.now().saturating_add(job.delay));
            let unique_key = job.unique_key.clone().unwrap_or_default();
            // Opaque, exact-match-only key segment — no key-safety check needed
            // (see `enqueue`).
            let id = job.id;
            let envelope = job.into_envelope();
            let blob = encode_envelope(&envelope)?;

            invoker.arg(id.to_string());
            invoker.arg(envelope.lane.as_str());
            invoker.arg(available_at);
            invoker.arg(blob);
            invoker.arg(unique_key);
            invoker.arg(envelope.priority);
        }

        let mut conn = self.conn.clone();
        let stored_blobs: Vec<Vec<u8>> =
            invoker.invoke_async(&mut conn).await.map_err(redis_err)?;

        let mut final_ids = Vec::with_capacity(stored_blobs.len());
        for blob in stored_blobs {
            final_ids.push(decode_envelope(&blob)?.id);
        }

        Ok(final_ids)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        // Reject a lane that cannot be safely embedded in a redis key before
        // storing anything.
        lane_key_segment(&job.lane)?;
        let available_at = nanos(self.clock.now().saturating_add(job.delay));
        let unique_key = job.unique_key.clone().unwrap_or_default();
        // `unique_key` is opaque application data and needs no key-safety check.
        // Unlike a lane (which collides across key families — `ns:dead:{lane}` vs
        // the `ns:dead:job:{id}` HASH) it appears only as the terminal segment of
        // `ns:unique:{key}` in exact-match GET/SET/DEL, never in a SCAN/glob
        // position, so no character can collide or be interpreted as a pattern.
        // The framework deliberately fills it with `:`-bearing values (fan-in and
        // sequence idempotency keys, scheduled-fire keys). Empty (no key) is unchanged.
        let id = job.id;
        let envelope = job.into_envelope();
        let blob = encode_envelope(&envelope)?;
        let mut conn = self.conn.clone();
        // The script stores the job, or — when the unique key is already held by a
        // live job — returns that job's envelope; either way we decode the id.
        let stored: Vec<u8> = self
            .scripts
            .enqueue
            .arg(&self.namespace)
            .arg(id.to_string())
            .arg(envelope.lane.as_str())
            .arg(available_at)
            .arg(blob)
            .arg(unique_key)
            .arg(envelope.priority)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(decode_envelope(&stored)?.id)
    }

    fn batch_enqueue(&self) -> Option<&dyn BatchEnqueue> {
        Some(self)
    }

    async fn reserve(&self, lane: &Lane) -> Result<Option<Reservation>> {
        let lane_seg = lane_key_segment(lane)?;
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let lease_until = nanos(now_d.saturating_add(self.lease));
        let receipt = ReservationReceipt::new();
        // Delivery bound: 0 means unbounded (the script never reads `deliveries`
        // beyond the always-on increment). When the bound dead-letters a poison
        // job it uses the same retention bounds as `fail` (0 = unbounded / no age
        // bound).
        let max = self.max_deliveries.unwrap_or(0);
        // 0 is the script's "unbounded / no bound" sentinel for both fields.
        let max_count = self.retention.keep_count().unwrap_or(0);
        let age_cutoff = self.retention.age_cutoff_nanos(now).unwrap_or(0);
        let has_age_bound = i64::from(self.retention.max_age.is_some());
        let mut conn = self.conn.clone();
        let res: Option<(Vec<u8>, u32)> = self
            .scripts
            .reserve
            .arg(&self.namespace)
            .arg(lane_seg)
            .arg(now)
            .arg(lease_until)
            .arg(receipt_key(&receipt)?)
            .arg(max)
            .arg(max_count)
            .arg(age_cutoff)
            .arg(has_age_bound)
            .arg(MAX_DEAD_LETTER_SWEEP)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        match res {
            Some((blob, attempts)) => Ok(Some(Reservation::new(
                Self::envelope_with_attempts(&blob, attempts)?,
                receipt,
                self.lease,
            ))),
            None => Ok(None),
        }
    }

    async fn ack(&self, receipt: ReservationReceipt) -> Result<()> {
        let now = nanos(self.clock.now());
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .scripts
            .ack
            .arg(&self.namespace)
            .arg(receipt_key(&receipt)?)
            .arg(now)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if ok == 1 { Ok(()) } else { Err(stale(receipt)) }
    }

    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        // `delay` is an arbitrary caller-supplied `Duration` (public `Broker::retry`
        // contract); saturate the addition so a near-`Duration::MAX` delay cannot
        // panic before `nanos` clamps to `i64::MAX`.
        let available_at = nanos(now_d.saturating_add(delay));
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .scripts
            .retry
            .arg(&self.namespace)
            .arg(receipt_key(&receipt)?)
            .arg(now)
            .arg(available_at)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if ok == 1 { Ok(()) } else { Err(stale(receipt)) }
    }

    async fn defer(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let available_at = nanos(now_d.saturating_add(delay));
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .scripts
            .defer
            .arg(&self.namespace)
            .arg(receipt_key(&receipt)?)
            .arg(now)
            .arg(available_at)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if ok == 1 { Ok(()) } else { Err(stale(receipt)) }
    }

    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()> {
        let now = nanos(self.clock.now());
        self.dlq_warning.warn_once(&self.retention);
        // Retention bounds for the prune step. Count still uses 0 as "unbounded",
        // but age uses an explicit flag because a real cutoff can be 0 when the
        // injected clock is near epoch.
        let max_count = self
            .retention
            .max_count
            .map(|c| i64::try_from(c).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let age_cutoff = self
            .retention
            .max_age
            .map(|a| now.saturating_sub(nanos(a)))
            .unwrap_or(0);
        let has_age_bound = i64::from(self.retention.max_age.is_some());
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .scripts
            .fail
            .arg(&self.namespace)
            .arg(receipt_key(&receipt)?)
            .arg(now)
            .arg(error)
            .arg(max_count)
            .arg(age_cutoff)
            .arg(has_age_bound)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if ok == 1 { Ok(()) } else { Err(stale(receipt)) }
    }

    async fn extend(&self, receipt: ReservationReceipt) -> Result<()> {
        let now_d = self.clock.now();
        let now = nanos(now_d);
        let lease_until = nanos(now_d.saturating_add(self.lease));
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .scripts
            .extend
            .arg(&self.namespace)
            .arg(receipt_key(&receipt)?)
            .arg(now)
            .arg(lease_until)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if ok == 1 { Ok(()) } else { Err(stale(receipt)) }
    }

    async fn classify(&self, id: JobId) -> Result<worklane_core::JobState> {
        // By-id, O(1): Evaluate both existence checks atomically in a Lua script
        // to prevent TOCTOU race conditions.
        let mut conn = self.conn.clone();
        let job_key = self.key(&format!("job:{id}"));
        let dead_key = self.key(&format!("dead:job:{id}"));

        let state: i64 = self
            .scripts
            .classify
            .key(job_key)
            .key(dead_key)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;

        Ok(classify_state(Some(state)))
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
impl worklane_core::DeadLetterStore for RedisBroker {
    async fn read_dead_letters(&self, lane: &Lane, limit: usize) -> Result<Vec<DeadLetter>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn.clone();
        let dead_key = self.key(&format!("dead:{}", lane_key_segment(lane)?));
        // Clamp before the `as isize` cast: a limit past `isize::MAX` (e.g.
        // `usize::MAX` meaning "all") would wrap negative and make `zrange`
        // silently drop the tail.
        let stop = (limit.min(isize::MAX as usize) as isize) - 1;
        // ZSET members are `<seq>:<id>` (fifo_member) ordered by (dead_at, seq),
        // so this reads the lane's dead letters oldest-first up to `limit`.
        let members: Vec<String> = conn.zrange(&dead_key, 0, stop).await.map_err(redis_err)?;
        if members.is_empty() {
            return Ok(Vec::new());
        }
        // One pipelined round-trip for every dead job rather than an HMGET per
        // id (an N+1 that scaled round-trips with the result size).
        let mut pipe = redis::pipe();
        for member in &members {
            let id = member.split_once(':').map_or(member.as_str(), |(_, id)| id);
            pipe.cmd("HMGET")
                .arg(self.key(&format!("dead:job:{id}")))
                .arg("envelope")
                .arg("error")
                .arg("attempts");
        }
        // Decode each field as an `Option`: a concurrent `requeue` can `DEL` a
        // dead-job HASH between the ZRANGE above and this HMGET, so every field
        // comes back nil. Decoding into non-optional `String`/`u32` would make the
        // whole reply fail with "Response type not string compatible" before any
        // per-row guard runs, so the optional decode is what actually tolerates
        // the race.
        let rows: Vec<DeadFields> = pipe.query_async(&mut conn).await.map_err(redis_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for (blob, error, attempts) in rows {
            // Skip a row whose HASH was deleted by a concurrent `requeue`: the
            // entry was just requeued and is no longer a dead letter — a benign
            // race, not a decode error. The SQL backends read a single-statement
            // snapshot and never see this; matching their tolerance here.
            let (Some(blob), Some(error), Some(attempts)) = (blob, error, attempts) else {
                continue;
            };
            out.push(DeadLetter::new(
                Self::envelope_with_attempts(&blob, attempts)?,
                error,
            ));
        }
        Ok(out)
    }

    async fn count_dead_letters(&self, lane: &Lane) -> Result<u64> {
        // O(1): the dead store is a ZSET kept in lockstep with the per-job hashes
        // (every add ZADDs+HSETs, every removal ZREMs+DELs), so `ZCARD` is the
        // exact live dead-letter count without a scan.
        let mut conn = self.conn.clone();
        let dead_key = self.key(&format!("dead:{}", lane_key_segment(lane)?));
        let count: u64 = conn.zcard(&dead_key).await.map_err(redis_err)?;
        Ok(count)
    }

    async fn requeue(&self, id: JobId) -> Result<()> {
        // No lane-key check needed: the REQUEUE script reads the lane from the
        // stored dead record, which could only have been written by an `enqueue`
        // that already passed `lane_key_segment` — so it is key-safe by
        // construction.
        let now = nanos(self.clock.now());
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .scripts
            .requeue
            .arg(&self.namespace)
            .arg(id.to_string())
            .arg(now)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        match ok {
            1 => Ok(()),
            // The script signals a unique-key conflict: the dead job's key is held
            // by another live job, so it was left dead-lettered.
            2 => Err(Error::UniqueKeyHeld(format!(
                "cannot requeue job {id}: its unique key is held by another live job"
            ))),
            3 => Err(Error::LiveJobIdConflict(format!(
                "cannot requeue job {id}: a live job with the same id already exists"
            ))),
            _ => Err(Error::Broker(format!("no dead-letter record for job {id}"))),
        }
    }

    async fn purge_dead_letters(&self, lane: &Lane) -> Result<u64> {
        // Lane-scoped, destructive: one atomic script drops every per-job dead
        // hash and the lane list. The lane segment is validated key-safe.
        let segment = lane_key_segment(lane)?;
        let mut conn = self.conn.clone();
        let removed: u64 = self
            .scripts
            .purge_dead
            .arg(&self.namespace)
            .arg(segment)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(removed)
    }
}

#[async_trait]
impl worklane_core::QueueStats for RedisBroker {
    async fn pending_count(&self, lane: &Lane) -> Result<u64> {
        // Sum ZCARD across the lane's per-priority ZSETs (in-flight and scheduled
        // jobs are still members). The lane segment is validated key-safe.
        let segment = lane_key_segment(lane)?;
        let mut conn = self.conn.clone();
        let count: u64 = self
            .scripts
            .pending_count
            .arg(&self.namespace)
            .arg(segment)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(count)
    }
}

/// Characters unsafe to embed verbatim in a Redis key segment: `:` is the
/// structural separator worklane uses to namespace keys (a lane carrying it could
/// masquerade as a deeper segment and collide — e.g. a lane `job:<id>` makes
/// `ns:dead:job:<id>` clash with the dead-job HASH key), and `* ? [ ]` are Redis
/// glob metacharacters, unsafe in any key or pattern position.
const LANE_KEY_UNSAFE: &[char] = &[':', '*', '?', '[', ']'];

/// The error for a `what` (e.g. `"lane"` or `"schedule id"`) whose value carries
/// the key-unsafe character `c`. Single-sourced so the lane and schedule-id entry
/// points cannot drift on wording.
fn key_unsafe_err(what: &str, value: &str, c: char) -> Error {
    Error::Broker(format!(
        "{what} {value:?} contains {c:?}, unsafe in a redis key; avoid ':' and the \
         glob characters '*', '?', '[', ']' on the redis broker"
    ))
}

/// Validate that a non-lane `value` (a schedule id) can be embedded verbatim in
/// this broker's keys, rejecting any [`LANE_KEY_UNSAFE`] character so it cannot
/// corrupt the `ns:...:{value}` key scheme. Lanes go through [`lane_key_segment`],
/// which applies the same charset via the shared core mechanism.
fn checked_key_segment<'a>(value: &'a str, what: &str) -> Result<&'a str> {
    match value.chars().find(|c| LANE_KEY_UNSAFE.contains(c)) {
        Some(c) => Err(key_unsafe_err(what, value, c)),
        None => Ok(value),
    }
}

/// Validate that `lane` can be embedded in this broker's keys and return its
/// string form. The authoritative lane lives in the job HASH, so the key
/// segment need not be reversible — only collision-free. Applies this broker's
/// [`LANE_KEY_UNSAFE`] charset through the shared core mechanism
/// ([`worklane_core::spi::reject_chars`]).
fn lane_key_segment(lane: &Lane) -> Result<&str> {
    worklane_core::spi::reject_chars(lane, LANE_KEY_UNSAFE)
        .map_err(|c| key_unsafe_err("lane", lane.as_str(), c))
}

fn redis_err(e: redis::RedisError) -> Error {
    // A connection error can echo the redis URL (with its password); redact
    // before the string enters `Error` and flows on to logs/dead-letters.
    Error::Broker(worklane_core::redact_credentials(&e.to_string()))
}

#[async_trait]
impl worklane_core::ScheduledStore for RedisBroker {
    async fn enqueue_scheduled(
        &self,
        schedule_id: &str,
        occurrence: i64,
        job: NewJob,
    ) -> Result<bool> {
        lane_key_segment(&job.lane)?;
        // The schedule id is interpolated into the key (KEYS[1]), so it must be
        // collision-free in the key scheme exactly as a lane is — the SQL
        // backends bind it as a parameter and need no such check.
        checked_key_segment(schedule_id, "schedule id")?;
        let key = self.key(&format!("schedule:{}", schedule_id));
        let available_at = nanos(self.clock.now().saturating_add(job.delay));
        let unique_key = job.unique_key.clone().unwrap_or_default();
        // Opaque, exact-match-only key segment — no key-safety check needed
        // (see `enqueue`).
        let id = job.id;
        let envelope = job.into_envelope();
        let blob = encode_envelope(&envelope)?;
        let mut conn = self.conn.clone();
        let ok: i64 = self
            .scripts
            .enqueue_scheduled
            .key(key)
            // Encode the i64 occurrence order-preservingly into a fixed-width
            // string (offset-binary: +2^63 maps i64→u64, then 20-digit
            // zero-padded) so the Lua watermark compare/store is exact for the
            // full i64 range instead of being rounded through an f64.
            .arg(format!(
                "{:020}",
                (occurrence as i128 + (1i128 << 63)) as u64
            ))
            .arg(&self.namespace)
            .arg(id.to_string())
            .arg(envelope.lane.as_str())
            .arg(available_at)
            .arg(blob)
            .arg(unique_key)
            .arg(envelope.priority)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(ok == 1)
    }

    async fn remove_schedule(&self, schedule_id: &str) -> Result<()> {
        checked_key_segment(schedule_id, "schedule id")?;
        let key = self.key(&format!("schedule:{schedule_id}"));
        let mut conn = self.conn.clone();
        // Idempotent: DEL of an absent key removes nothing.
        let _: i64 = conn.del(&key).await.map_err(redis_err)?;
        Ok(())
    }
}
