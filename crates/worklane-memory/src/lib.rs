//! In-memory [`Broker`] for `worklane`, for development and tests.
//!
//! Jobs live in process memory. Reservation uses a visibility lease: a reserved
//! job is hidden for a lease duration and becomes visible again if it is not
//! acked, retried, or failed before the lease expires (at-least-once delivery).
//! Time comes from a [`Clock`] seam so tests can advance it deterministically.
//!
//! Jobs are partitioned by lane: `reserve(lane)` only returns jobs enqueued to
//! that lane, and a lane no worker reserves retains its jobs indefinitely.
//!
//! This crate is best for examples, unit tests, and local development. Use
//! `worklane-sqlite`, `worklane-postgres`, or `worklane-redis` when jobs must
//! survive process restarts.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::spi::MAX_DEAD_LETTER_SWEEP;
use worklane_core::{
    BatchEnqueue, Broker, Clock, DeadLetter, Error, JobEnvelope, JobId, JobState, Lane, NewJob,
    Reservation, ReservationReceipt, Result, RetentionPolicy, SystemClock, UnboundedDlqWarning,
};

/// The default visibility lease duration (re-exported single source).
pub use worklane_core::spi::DEFAULT_LEASE;

struct StoredJob {
    envelope: JobEnvelope,
    /// When the job becomes visible for reservation.
    available_at: Duration,
    /// When the current lease expires, if the job is reserved.
    leased_until: Option<Duration>,
    /// The current receipt, if the job is reserved.
    receipt: Option<ReservationReceipt>,
    /// The uniqueness key this job holds, if any (freed on ack/fail).
    unique_key: Option<String>,
    /// How many times this job has been delivered (reserved and handed out).
    /// Distinct from `attempts` (handler failures): this advances on every
    /// reservation, even one the caller never resolves (e.g. a crashed worker),
    /// so it can bound poison-pill redelivery via `max_deliveries`.
    deliveries: u32,
}

/// A dead-letter record plus the `unique_key` the job held before it was
/// dead-lettered, retained so `requeue` can re-acquire the key. `DeadLetter`
/// (the public read shape) deliberately does not carry the key, so it is kept
/// alongside here.
struct DeadEntry {
    letter: DeadLetter,
    unique_key: Option<String>,
    /// When the job was dead-lettered, on the broker's clock — used to apply a
    /// [`RetentionPolicy`]'s `max_age` bound.
    dead_at: Duration,
}

/// Prune `lane`'s dead-letter records in `inner` to satisfy `policy`, given the
/// current time `now`. The `dead` vec is in dead-letter (sequence) order, so the
/// oldest records for a lane are the earliest ones encountered.
fn prune_dead(inner: &mut Inner, lane: &Lane, policy: &RetentionPolicy, now: Duration) {
    if policy.is_unbounded() {
        return;
    }
    if let Some(max_age) = policy.max_age {
        let cutoff = now.saturating_sub(max_age);
        inner
            .dead
            .retain(|d| d.letter.envelope.lane != *lane || d.dead_at >= cutoff);
    }
    if let Some(max_count) = policy.max_count {
        let total = inner
            .dead
            .iter()
            .filter(|d| d.letter.envelope.lane == *lane)
            .count() as u64;
        if total > max_count {
            let mut to_drop = total - max_count;
            inner.dead.retain(|d| {
                if d.letter.envelope.lane == *lane && to_drop > 0 {
                    to_drop -= 1;
                    false
                } else {
                    true
                }
            });
        }
    }
}

struct Inner {
    jobs: Vec<StoredJob>,
    dead: Vec<DeadEntry>,
    /// Live uniqueness keys → the job id holding them.
    unique: HashMap<String, JobId>,
    /// Schedule IDs → the largest Unix timestamp occurrence claimed so far.
    schedules: HashMap<String, i64>,
}

/// Reject a job whose payload exceeds the durable envelope-size cap, so an
/// over-cap job is refused at enqueue uniformly across backends rather than only
/// by the durable ones (which enforce it in `encode_envelope`). The in-memory
/// store keeps the typed envelope, not encoded bytes, so this is a fail-fast
/// payload-size guard approximating that exact encoded cap — the small fixed
/// envelope overhead is far under the multi-MB margin in practice. Checked before
/// any mutation so a batch fails atomically (no partial insert).
fn check_payload_size(job: &NewJob) -> Result<()> {
    if job.payload.len() > worklane_core::spi::MAX_ENVELOPE_BYTES {
        return Err(Error::Serialization(format!(
            "job payload is {} bytes, over the {}-byte limit",
            job.payload.len(),
            worklane_core::spi::MAX_ENVELOPE_BYTES
        )));
    }
    Ok(())
}

/// Insert one job, becoming visible at `now + job.delay`. A live job already
/// holding the same uniqueness key wins: its id is returned and nothing is
/// inserted. The single insertion path shared by `enqueue` and `enqueue_batch`.
fn insert_one(inner: &mut Inner, job: NewJob, now: Duration) -> JobId {
    let available_at = now.saturating_add(job.delay);
    // Idempotent on JobId: a live job already carrying this id wins, so a
    // re-enqueue of the same id returns it without creating a second job (the
    // broker's "no two live jobs share an id" invariant, enforced not just
    // conventional). Checked before the unique-key path so identity takes priority.
    if inner.jobs.iter().any(|j| j.envelope.id == job.id) {
        return job.id;
    }
    let unique_key = job.unique_key.clone();
    if let Some(key) = &unique_key {
        if let Some(&existing) = inner.unique.get(key) {
            return existing;
        }
    }
    let id = job.id;
    let envelope = job.into_envelope();
    if let Some(key) = &unique_key {
        inner.unique.insert(key.clone(), id);
    }
    inner.jobs.push(StoredJob {
        envelope,
        available_at,
        leased_until: None,
        receipt: None,
        unique_key,
        deliveries: 0,
    });
    id
}

/// Move the job at `idx` to the dead-letter store with `error`: release its live
/// uniqueness key (retaining it on the dead record for a later `requeue`), push
/// the dead record, and apply the retention policy. Shared by `fail` and the
/// `max_deliveries` bound in `reserve`.
fn dead_letter_at(
    inner: &mut Inner,
    idx: usize,
    error: String,
    now: Duration,
    retention: &RetentionPolicy,
) {
    let job = inner.jobs.remove(idx);
    let unique_key = job.unique_key;
    if let Some(key) = &unique_key {
        inner.unique.remove(key);
    }
    let lane = job.envelope.lane.clone();
    inner.dead.push(DeadEntry {
        letter: DeadLetter::new(job.envelope, error),
        unique_key,
        dead_at: now,
    });
    prune_dead(inner, &lane, retention, now);
}

/// An in-memory broker.
pub struct InMemoryBroker {
    inner: Mutex<Inner>,
    clock: Arc<dyn Clock>,
    lease: Duration,
    retention: RetentionPolicy,
    /// One-shot warning when dead-lettering under an unbounded retention policy.
    dlq_warning: UnboundedDlqWarning,
    /// Maximum times a job may be delivered before it is dead-lettered on the
    /// next reserve; `None` (default) means unbounded.
    max_deliveries: Option<u32>,
}

impl InMemoryBroker {
    /// Create a broker using the system clock and the default lease.
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock::new()))
    }

    /// Create a broker with a custom clock (e.g. a `ManualClock` for tests).
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        InMemoryBroker {
            inner: Mutex::new(Inner {
                jobs: Vec::new(),
                dead: Vec::new(),
                unique: HashMap::new(),
                schedules: HashMap::new(),
            }),
            clock,
            lease: DEFAULT_LEASE,
            retention: RetentionPolicy::new(),
            dlq_warning: UnboundedDlqWarning::default(),
            max_deliveries: None,
        }
    }

    /// Set the visibility lease duration (builder style).
    #[must_use = "this value must be used"]
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Bound how many times a job may be delivered before it is dead-lettered
    /// (builder style). This counts reservations, not handler failures, so it
    /// bounds a poison-pill job whose handler process crashes before acking,
    /// retrying, or failing (which never advances `attempts`). Defaults to
    /// unbounded. When also using `max_attempts`, set this above it so
    /// legitimate retries (each a redelivery) are not cut short.
    #[must_use = "this value must be used"]
    pub fn with_max_deliveries(mut self, max: u32) -> Self {
        self.max_deliveries = Some(max);
        self
    }

    /// Bound the dead-letter store with a [`RetentionPolicy`], enforced lazily on
    /// `fail` per lane (builder style). Defaults to unbounded.
    #[must_use = "this value must be used"]
    pub fn with_dead_letter_retention(mut self, policy: RetentionPolicy) -> Self {
        self.retention = policy;
        self
    }

    /// A snapshot of the dead-letter store, for inspection and tests.
    pub fn dead_letters(&self) -> Vec<DeadLetter> {
        self.lock().dead.iter().map(|d| d.letter.clone()).collect()
    }

    /// The number of live (non-dead-lettered) jobs.
    pub fn len(&self) -> usize {
        self.lock().jobs.len()
    }

    /// Whether there are no live jobs.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // Recover from a poisoned mutex rather than panicking: every method
        // mutates `Inner` synchronously and never holds the guard across an
        // await, so a panic elsewhere cannot leave a half-applied operation
        // visible. Propagating the poison would otherwise wedge the whole broker.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn find_current_receipt(
        inner: &mut Inner,
        receipt: ReservationReceipt,
        now: Duration,
    ) -> Result<usize> {
        let Some(pos) = inner
            .jobs
            .iter()
            .position(|job| job.receipt == Some(receipt))
        else {
            return Err(worklane_core::spi::stale(receipt));
        };

        if inner.jobs[pos]
            .leased_until
            .is_some_and(|until| until <= now)
        {
            inner.jobs[pos].leased_until = None;
            inner.jobs[pos].receipt = None;
            return Err(worklane_core::spi::stale(receipt));
        }

        Ok(pos)
    }
}

impl Default for InMemoryBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BatchEnqueue for InMemoryBroker {
    async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        // Validate every job before mutating so an over-cap job fails the whole
        // batch atomically (no partial insert), matching the durable backends.
        for job in &jobs {
            check_payload_size(job)?;
        }
        let now = self.clock.now();
        let mut inner = self.lock();
        let mut ids = Vec::with_capacity(jobs.len());
        for job in jobs {
            ids.push(insert_one(&mut inner, job, now));
        }
        Ok(ids)
    }
}

#[async_trait]
impl Broker for InMemoryBroker {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        check_payload_size(&job)?;
        let now = self.clock.now();
        let mut inner = self.lock();
        Ok(insert_one(&mut inner, job, now))
    }

    fn batch_enqueue(&self) -> Option<&dyn BatchEnqueue> {
        Some(self)
    }

    async fn reserve(&self, lane: &Lane) -> Result<Option<Reservation>> {
        let now = self.clock.now();
        let lease_until = now.saturating_add(self.lease);
        let mut inner = self.lock();

        for job in inner.jobs.iter_mut() {
            // Release any expired lease so the job is visible again.
            if let Some(until) = job.leased_until {
                if until <= now {
                    job.leased_until = None;
                    job.receipt = None;
                }
            }
        }

        // Pick the best visible job, then enforce the delivery bound. If the
        // chosen job has been delivered `max_deliveries` times already, dead-letter
        // it (a poison pill whose worker keeps crashing before resolving) and pick
        // the next one, so the bound never starves the lane of healthy jobs.
        let mut swept = 0u32;
        loop {
            // Consider jobs on the requested lane that are visible.
            // We pick the one with highest priority, then earliest available_at.
            let mut best_idx: Option<usize> = None;
            for (i, job) in inner.jobs.iter().enumerate() {
                if job.envelope.lane == *lane
                    && job.leased_until.is_none()
                    && job.available_at <= now
                {
                    if let Some(best) = best_idx {
                        let best_job = &inner.jobs[best];
                        if job.envelope.priority > best_job.envelope.priority
                            || (job.envelope.priority == best_job.envelope.priority
                                && job.available_at < best_job.available_at)
                        {
                            best_idx = Some(i);
                        }
                    } else {
                        best_idx = Some(i);
                    }
                }
            }

            let Some(idx) = best_idx else { return Ok(None) };

            if let Some(max) = self.max_deliveries {
                if inner.jobs[idx].deliveries.saturating_add(1) > max {
                    dead_letter_at(
                        &mut inner,
                        idx,
                        format!("exceeded max deliveries ({max})"),
                        now,
                        &self.retention,
                    );
                    swept += 1;
                    if swept >= MAX_DEAD_LETTER_SWEEP {
                        return Ok(None);
                    }
                    continue;
                }
            }

            let job = &mut inner.jobs[idx];
            job.deliveries = job.deliveries.saturating_add(1);
            let receipt = ReservationReceipt::new();
            job.leased_until = Some(lease_until);
            job.receipt = Some(receipt);
            return Ok(Some(Reservation::new(
                job.envelope.clone(),
                receipt,
                self.lease,
            )));
        }
    }

    async fn ack(&self, receipt: ReservationReceipt) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        let removed = inner.jobs.remove(pos);
        if let Some(key) = removed.unique_key {
            inner.unique.remove(&key);
        }
        Ok(())
    }

    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        let job = &mut inner.jobs[pos];
        // Saturate both: `attempts` comes from the stored envelope (a wrap at
        // `u32::MAX` would silently reset the retry counter), and `delay` is an
        // arbitrary caller-supplied `Duration` whose addition could otherwise panic.
        job.envelope.attempts = job.envelope.attempts.saturating_add(1);
        job.available_at = now.saturating_add(delay);
        job.leased_until = None;
        job.receipt = None;
        Ok(())
    }

    async fn defer(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        let job = &mut inner.jobs[pos];
        // Re-schedule without advancing `attempts` (backpressure, not a failure).
        job.available_at = now.saturating_add(delay);
        job.leased_until = None;
        job.receipt = None;
        Ok(())
    }

    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        self.dlq_warning.warn_once(&self.retention);
        // Release the live key, but retain its value on the dead record so a later
        // requeue can re-acquire it (the live slot is free in the meantime).
        dead_letter_at(&mut inner, pos, error, now, &self.retention);
        Ok(())
    }

    async fn extend(&self, receipt: ReservationReceipt) -> Result<()> {
        let now = self.clock.now();
        let lease_until = now.saturating_add(self.lease);
        let mut inner = self.lock();
        // The same validity check as every other resolution: an expired or
        // superseded receipt is rejected without touching the job.
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        inner.jobs[pos].leased_until = Some(lease_until);
        Ok(())
    }

    async fn classify(&self, id: JobId) -> Result<JobState> {
        let inner = self.lock();
        if inner.jobs.iter().any(|j| j.envelope.id == id) {
            Ok(JobState::Live)
        } else if inner.dead.iter().any(|d| d.letter.envelope.id == id) {
            Ok(JobState::DeadLettered)
        } else {
            Ok(JobState::CompletedOrUnknown)
        }
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
impl worklane_core::DeadLetterStore for InMemoryBroker {
    async fn read_dead_letters(&self, lane: &Lane, limit: usize) -> Result<Vec<DeadLetter>> {
        // Bounded, lane-scoped, non-destructive: clone up to `limit` records for
        // the lane, leaving the store untouched.
        Ok(self
            .lock()
            .dead
            .iter()
            .filter(|d| d.letter.envelope.lane == *lane)
            .take(limit)
            .map(|d| d.letter.clone())
            .collect())
    }

    async fn count_dead_letters(&self, lane: &Lane) -> Result<u64> {
        // Lane-scoped, non-destructive count over the in-memory dead-letter store.
        Ok(self
            .lock()
            .dead
            .iter()
            .filter(|d| d.letter.envelope.lane == *lane)
            .count() as u64)
    }

    async fn requeue(&self, id: JobId) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let Some(pos) = inner.dead.iter().position(|d| d.letter.envelope.id == id) else {
            return Err(Error::Broker(format!("no dead-letter record for job {id}")));
        };
        if inner.jobs.iter().any(|j| j.envelope.id == id) {
            return Err(Error::LiveJobIdConflict(format!(
                "cannot requeue job {id}: a live job with the same id already exists"
            )));
        }
        // If the job held a unique key, re-acquire it — unless another live job
        // now holds it (the key was freed at fail time). On conflict, reject with
        // no changes, leaving the dead record in place.
        if let Some(uk) = &inner.dead[pos].unique_key {
            if inner.unique.contains_key(uk) {
                return Err(Error::UniqueKeyHeld(format!(
                    "cannot requeue job {id}: unique key {uk:?} is held by another live job"
                )));
            }
        }
        // Move the envelope back to the live store, visible now on its original
        // lane, preserving every field (including attempts), and re-claim its key.
        let entry = inner.dead.remove(pos);
        let envelope = entry.letter.envelope;
        let unique_key = entry.unique_key;
        if let Some(key) = &unique_key {
            inner.unique.insert(key.clone(), id);
        }
        inner.jobs.push(StoredJob {
            envelope,
            available_at: now,
            leased_until: None,
            receipt: None,
            unique_key,
            // A requeue is a deliberate revival, so the delivery count starts
            // fresh — the operator gets a full `max_deliveries` budget again.
            deliveries: 0,
        });
        Ok(())
    }

    async fn purge_dead_letters(&self, lane: &Lane) -> Result<u64> {
        // Lane-scoped, destructive: drop every dead record for `lane`, returning
        // how many were removed.
        let mut inner = self.lock();
        let before = inner.dead.len();
        inner.dead.retain(|d| d.letter.envelope.lane != *lane);
        Ok((before - inner.dead.len()) as u64)
    }
}

#[async_trait]
impl worklane_core::QueueStats for InMemoryBroker {
    async fn pending_count(&self, lane: &Lane) -> Result<u64> {
        // Lane-scoped count of live jobs (every entry in `jobs`, leased or not, is
        // pending until acked or failed). Dead-lettered jobs live in `dead`.
        Ok(self
            .lock()
            .jobs
            .iter()
            .filter(|j| j.envelope.lane == *lane)
            .count() as u64)
    }
}

#[async_trait]
impl worklane_core::ScheduledStore for InMemoryBroker {
    async fn enqueue_scheduled(
        &self,
        schedule_id: &str,
        occurrence: i64,
        job: NewJob,
    ) -> Result<bool> {
        let now = self.clock.now();
        let mut inner = self.lock();
        // A schedule with no recorded occurrence accepts the first claim of any
        // occurrence value (including `0`, a negative timestamp, or `i64::MIN`);
        // once recorded, only a strictly greater occurrence wins. This mirrors
        // the durable backends, whose `INSERT ... ON CONFLICT DO UPDATE WHERE
        // existing < new` accepts a fresh schedule unconditionally. A `0`
        // sentinel here would wrongly reject a first claim at a non-positive
        // occurrence.
        let win = match inner.schedules.get(schedule_id).copied() {
            None => true,
            Some(last) => occurrence > last,
        };
        if win {
            inner.schedules.insert(schedule_id.to_string(), occurrence);
            insert_one(&mut inner, job, now);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn remove_schedule(&self, schedule_id: &str) -> Result<()> {
        // Idempotent: removing an unknown schedule_id is a no-op.
        self.lock().schedules.remove(schedule_id);
        Ok(())
    }
}
