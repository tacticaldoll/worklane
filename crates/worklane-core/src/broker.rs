use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::envelope::{DeadLetter, NewJob, Reservation, ReservationReceipt};
use crate::error::Result;
use crate::id::JobId;
use crate::lane::Lane;

/// The state of a job returned by `Broker::classify`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// The job is pending (visible now or delayed) or in-flight under a lease.
    Live,
    /// The job exhausted its attempts or was manually failed.
    DeadLettered,
    /// The job has been acked (completed successfully) or never existed.
    CompletedOrUnknown,
}

/// A backend-agnostic job store and lifecycle primitive.
///
/// A broker operates purely on opaque [`crate::JobEnvelope`]s; it never inspects or
/// deserializes payloads and knows nothing about Rust handler types.
///
/// # Interception / middleware
///
/// Cross-cutting concerns over the broker — audit logging, encryption of payloads
/// at rest, metrics, policy enforcement — are added by **wrapping** a broker, not
/// by changing this trait. Because `Broker` is an ordinary trait and the worker
/// and client hold an `Arc<dyn Broker>`, a decorator that wraps another broker is
/// itself a `Broker` and drops in transparently:
///
/// ```ignore
/// struct AuditBroker<B>(B);
/// #[async_trait]
/// impl<B: Broker> Broker for AuditBroker<B> {
///     async fn enqueue(&self, job: NewJob) -> Result<JobId> {
///         let id = self.0.enqueue(job).await?; // intercept …
///         tracing::info!(%id, "enqueued");      // … then delegate
///         Ok(id)
///     }
///     // forward the remaining methods to `self.0`
/// }
/// ```
///
/// This is the supported interception mechanism: it composes (stack several
/// wrappers), needs no change to any backend, and keeps this contract minimal.
/// The trade-off is that a decorator must forward every method it does not
/// override. See the `broker_middleware` example in the `worklane` crate for a
/// complete, runnable decorator.
#[async_trait]
pub trait Broker: Send + Sync {
    /// Store a new job and return its id. The id is assigned client-side (it is
    /// carried on [`NewJob`]); the broker persists and returns that id rather
    /// than minting a new one, which keeps enqueue atomic and replay-friendly.
    async fn enqueue(&self, job: NewJob) -> Result<JobId>;

    /// Store a batch of new jobs atomically and return their assigned ids in the
    /// same order.
    ///
    /// This method guarantees all-or-nothing insertion. If any job in the batch
    /// violates database constraints, no jobs are persisted.
    ///
    /// Unique key semantics:
    /// - **Inter-batch collision**: If a job's unique key matches an existing live job,
    ///   it returns the existing `JobId` without aborting the batch.
    /// - **Intra-batch collision**: Evaluated sequentially. If multiple jobs in the
    ///   batch share a unique key, the first one is inserted, and subsequent ones
    ///   return the `JobId` of the first.
    async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>>;

    /// Reserve at most one currently-visible job on `lane`, hiding it for a
    /// visibility lease. Returns `None` when no job is available.
    async fn reserve(&self, lane: &Lane) -> Result<Option<Reservation>>;

    /// Acknowledge a reservation as done, removing its job permanently.
    async fn ack(&self, receipt: ReservationReceipt) -> Result<()>;

    /// Increment the reserved job's attempts and schedule it to become visible
    /// again after `delay`.
    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()>;

    /// Release the reservation and schedule the job to become visible again after
    /// `delay`, **without** counting an attempt.
    ///
    /// Identical to [`retry`](Broker::retry) — releases the lease/receipt and sets
    /// the next visibility to `now + delay` — except it does **not** increment
    /// `attempts`, so it does not advance the job toward a `max_attempts`
    /// dead-letter threshold. It is for backpressure that is not the job's fault:
    /// a worker deferring a job because a dependency is unavailable (e.g. a tripped
    /// circuit breaker) must not burn the job's retry budget. Rejects an unknown,
    /// superseded, or expired receipt with
    /// [`Error::StaleReservation`](crate::Error::StaleReservation), changing
    /// nothing on rejection — exactly as `retry` does.
    async fn defer(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()>;

    /// Re-apply the broker's visibility lease to the job currently held under
    /// `receipt`, keeping it hidden from other [`reserve`](Broker::reserve)
    /// callers for a fresh lease measured from now. Used as a heartbeat so a
    /// still-running handler does not lose its reservation.
    ///
    /// Rejects a receipt that is unknown, superseded, or whose lease has already
    /// expired with [`Error::StaleReservation`](crate::Error::StaleReservation),
    /// and MUST NOT change the job's `attempts`, schedule, or visibility on that
    /// rejection. The lease duration is owned by the broker (as for `reserve`);
    /// `extend` takes no caller-supplied duration.
    async fn extend(&self, receipt: ReservationReceipt) -> Result<()>;

    /// Move the reserved job to the dead-letter store, retaining `error`.
    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()>;

    /// Classify the state of the job identified by `id`.
    ///
    /// Returns one of three states:
    /// - `Live`: pending or leased.
    /// - `DeadLettered`: permanently failed.
    /// - `CompletedOrUnknown`: acked or never enqueued.
    ///
    /// A bounded, by-id, non-destructive atomic point lookup. It does not scan
    /// the store. Together, this prevents TOCTOU races between liveness and
    /// dead-letter classification.
    ///
    /// See `openspec/specs/broker/spec.md` for semantic details on terminal states.
    async fn classify(&self, id: JobId) -> Result<JobState>;

    /// The dead-letter inspection and maintenance capability, if this broker
    /// provides one. Returns `None` by default; a broker that implements
    /// [`DeadLetterStore`] overrides this to return `Some(self)`.
    fn dead_letter_store(&self) -> Option<&dyn DeadLetterStore> {
        None
    }

    /// The queue-depth statistics capability, if this broker provides one.
    /// Returns `None` by default; a broker that implements [`QueueStats`]
    /// overrides this to return `Some(self)`.
    fn queue_stats(&self) -> Option<&dyn QueueStats> {
        None
    }

    /// The scheduled-enqueue capability, if this broker provides one. Returns
    /// `None` by default; a broker that implements [`ScheduledStore`] overrides
    /// this to return `Some(self)`. Takes `self: Arc<Self>` so the returned handle
    /// can be retained (for example by a recurring scheduler) beyond the call.
    fn scheduled_store(self: Arc<Self>) -> Option<Arc<dyn ScheduledStore>> {
        None
    }
}

/// Dead-letter inspection and maintenance — an optional [`Broker`] capability.
///
/// Obtained through [`Broker::dead_letter_store`]. A broker that retains dead-lettered
/// jobs in a readable store implements this trait and returns `Some(self)` from
/// the accessor; a broker without one returns `None` and is still a valid broker.
#[async_trait]
pub trait DeadLetterStore: Send + Sync {
    /// Read up to `limit` dead-letter records for `lane` in an unspecified order
    /// (implementations may return them oldest-first), without removing or
    /// mutating any of them.
    ///
    /// Each [`DeadLetter`] carries the preserved opaque [`crate::JobEnvelope`] (every
    /// field unchanged) and the error retained at [`fail`](Broker::fail) time.
    /// The read is lane-scoped (records for other lanes are not returned) and
    /// non-destructive: a subsequent read returns the same records.
    async fn read_dead_letters(&self, lane: &Lane, limit: usize) -> Result<Vec<DeadLetter>>;

    /// Count the dead-letter records for `lane`.
    ///
    /// Returns the number of dead-letter records currently held for `lane` as a
    /// `u64`. The count is lane-scoped (records for other lanes are not counted)
    /// and non-destructive (it neither removes nor mutates any record). Unlike
    /// [`read_dead_letters`](Self::read_dead_letters) it takes no `limit`: the
    /// value reflects every record present for the lane.
    async fn count_dead_letters(&self, lane: &Lane) -> Result<u64>;

    /// Move the dead-lettered job identified by `id` back to its original lane
    /// as a visible job, preserving every envelope field (including `attempts` —
    /// the broker imposes no retry policy), and remove it from the dead-letter
    /// store.
    ///
    /// If the job carried a `unique_key`, `requeue` re-acquires it for the revived
    /// job, so the key dedups enqueues again. Because the key was released when the
    /// job was dead-lettered, another live job may have claimed it meanwhile; in
    /// that case `requeue` fails with
    /// [`Error::UniqueKeyHeld`](crate::Error::UniqueKeyHeld) and leaves the
    /// dead-lettered job and the live holder untouched.
    ///
    /// Rejects an `id` that has no dead-letter record without changing any
    /// stored job or dead-letter record.
    async fn requeue(&self, id: JobId) -> Result<()>;

    /// Permanently remove **all** dead-letter records for `lane`, returning how
    /// many were removed.
    ///
    /// Lane-scoped (records for other lanes are untouched) and irreversible: the
    /// removed records are not requeued. It bounds the otherwise-unbounded growth
    /// of the dead-letter store, letting an operator drain a lane after
    /// inspecting (or [`requeue`](Self::requeue)ing) what it needs. Purging an
    /// empty lane removes nothing and returns `0`.
    async fn purge_dead_letters(&self, lane: &Lane) -> Result<u64>;
}

/// Queue-depth statistics — an optional [`Broker`] capability.
///
/// Obtained through [`Broker::queue_stats`]. A broker that can report its backlog
/// implements this trait and returns `Some(self)` from the accessor.
#[async_trait]
pub trait QueueStats: Send + Sync {
    /// Count the live jobs for `lane`: jobs enqueued but not yet acked or
    /// dead-lettered.
    ///
    /// Returns the lane's backlog as a `u64` — the primary input to queue-depth
    /// monitoring and autoscaling. Lane-scoped (jobs on other lanes are not
    /// counted) and non-destructive (it neither removes nor mutates any job).
    /// **Includes** jobs currently leased (in-flight) and jobs scheduled for a
    /// future `available_at`, since both are work not yet done; **excludes**
    /// dead-lettered jobs (see
    /// [`count_dead_letters`](DeadLetterStore::count_dead_letters)) and completed
    /// (acked) jobs, which no longer exist in the live store.
    async fn pending_count(&self, lane: &Lane) -> Result<u64>;
}

#[async_trait]
/// Optional schedule-claim store used by recurring schedulers.
///
/// Implemented by brokers that can atomically claim a schedule occurrence and
/// enqueue the corresponding job. The scheduler depends on this narrower
/// contract instead of adding recurring-schedule methods to [`Broker`].
pub trait ScheduledStore: Send + Sync {
    /// Atomically attempts to claim the schedule `schedule_id` at `occurrence` (a Unix
    /// timestamp). If this occurrence is strictly greater than the last recorded
    /// occurrence, the claim succeeds and `job` is enqueued. Returns `true` if this
    /// instance successfully claimed the occurrence and enqueued the job, or `false`
    /// if another instance already claimed this or a newer occurrence.
    async fn enqueue_scheduled(
        &self,
        schedule_id: &str,
        occurrence: i64,
        job: NewJob,
    ) -> Result<bool>;

    /// Remove the recorded occurrence watermark for `schedule_id`.
    ///
    /// Use when **decommissioning** a schedule, so its watermark does not linger
    /// in the store forever after the schedule stops running. After removal the
    /// schedule's next [`enqueue_scheduled`](ScheduledStore::enqueue_scheduled) is
    /// treated as a first claim again (any occurrence wins), so only call it for a
    /// schedule that is truly going away — not as a way to force a re-run, which
    /// would defeat the cross-instance dedup. Removing an unknown `schedule_id` is
    /// a no-op (idempotent).
    async fn remove_schedule(&self, schedule_id: &str) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NewJob, Reservation, ReservationReceipt};
    use async_trait::async_trait;

    /// A broker implementing only the core job lifecycle — no optional
    /// capability. The fact that this compiles is the contract's promise: a
    /// minimal broker need not implement dead-letter, stats, or scheduling.
    struct Minimal;

    #[async_trait]
    impl Broker for Minimal {
        async fn enqueue(&self, _job: NewJob) -> Result<JobId> {
            unimplemented!()
        }
        async fn enqueue_batch(&self, _jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
            unimplemented!()
        }
        async fn reserve(&self, _lane: &Lane) -> Result<Option<Reservation>> {
            unimplemented!()
        }
        async fn ack(&self, _receipt: ReservationReceipt) -> Result<()> {
            unimplemented!()
        }
        async fn retry(&self, _receipt: ReservationReceipt, _delay: Duration) -> Result<()> {
            unimplemented!()
        }
        async fn defer(&self, _receipt: ReservationReceipt, _delay: Duration) -> Result<()> {
            unimplemented!()
        }
        async fn extend(&self, _receipt: ReservationReceipt) -> Result<()> {
            unimplemented!()
        }
        async fn fail(&self, _receipt: ReservationReceipt, _error: String) -> Result<()> {
            unimplemented!()
        }
        async fn classify(&self, _id: JobId) -> Result<JobState> {
            unimplemented!()
        }
    }

    #[test]
    fn lifecycle_only_broker_reports_no_optional_capabilities() {
        let b = Minimal;
        assert!(
            b.dead_letter_store().is_none(),
            "default dead_letter_store accessor is None"
        );
        assert!(
            b.queue_stats().is_none(),
            "default queue_stats accessor is None"
        );
        assert!(
            Arc::new(Minimal).scheduled_store().is_none(),
            "default scheduled_store accessor is None"
        );
    }
}
