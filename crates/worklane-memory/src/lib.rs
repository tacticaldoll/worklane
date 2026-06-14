//! In-memory [`Broker`] for `worklane`, for development and tests.
//!
//! Jobs live in process memory. Reservation uses a visibility lease: a reserved
//! job is hidden for a lease duration and becomes visible again if it is not
//! acked, retried, or failed before the lease expires (at-least-once delivery).
//! Time comes from a [`Clock`] seam so tests can advance it deterministically.
//!
//! Jobs are partitioned by lane: `reserve(lane)` only returns jobs enqueued to
//! that lane, and a lane no worker reserves retains its jobs indefinitely.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::{
    Broker, Clock, DeadLetter, Error, JobEnvelope, JobId, NewJob, Reservation, ReservationReceipt,
    Result, SystemClock,
};

/// The default visibility lease duration.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

struct JobState {
    envelope: JobEnvelope,
    /// When the job becomes visible for reservation.
    available_at: Duration,
    /// When the current lease expires, if the job is reserved.
    leased_until: Option<Duration>,
    /// The current receipt, if the job is reserved.
    receipt: Option<ReservationReceipt>,
}

struct Inner {
    jobs: Vec<JobState>,
    dead: Vec<DeadLetter>,
}

/// An in-memory broker.
pub struct InMemoryBroker {
    inner: Mutex<Inner>,
    clock: Arc<dyn Clock>,
    lease: Duration,
}

impl InMemoryBroker {
    /// Create a broker using the system clock and the default lease.
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock::new()))
    }

    /// Create a broker with a custom clock (e.g. a [`ManualClock`] for tests).
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        InMemoryBroker {
            inner: Mutex::new(Inner {
                jobs: Vec::new(),
                dead: Vec::new(),
            }),
            clock,
            lease: DEFAULT_LEASE,
        }
    }

    /// Set the visibility lease duration (builder style).
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// A snapshot of the dead-letter store, for inspection and tests.
    pub fn dead_letters(&self) -> Vec<DeadLetter> {
        self.lock().dead.clone()
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
        self.inner.lock().expect("broker mutex poisoned")
    }

    fn stale(receipt: ReservationReceipt) -> Error {
        Error::StaleReservation(format!("receipt {receipt:?} is not current"))
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
            return Err(Self::stale(receipt));
        };

        if inner.jobs[pos]
            .leased_until
            .is_some_and(|until| until <= now)
        {
            inner.jobs[pos].leased_until = None;
            inner.jobs[pos].receipt = None;
            return Err(Self::stale(receipt));
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
impl Broker for InMemoryBroker {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let now = self.clock.now();
        let id = JobId::new();
        let envelope = JobEnvelope::new(id, job.lane, job.kind, job.payload, job.max_attempts);
        self.lock().jobs.push(JobState {
            envelope,
            available_at: now,
            leased_until: None,
            receipt: None,
        });
        Ok(id)
    }

    async fn reserve(&self, lane: &str) -> Result<Option<Reservation>> {
        let now = self.clock.now();
        let lease_until = now + self.lease;
        let mut inner = self.lock();

        for job in inner.jobs.iter_mut() {
            // Release any expired lease so the job is visible again.
            if let Some(until) = job.leased_until
                && until <= now
            {
                job.leased_until = None;
                job.receipt = None;
            }
        }

        // Only consider jobs on the requested lane; other lanes are isolated.
        let slot = inner.jobs.iter_mut().find(|job| {
            job.envelope.lane == lane && job.leased_until.is_none() && job.available_at <= now
        });

        match slot {
            Some(job) => {
                let receipt = ReservationReceipt::new();
                job.leased_until = Some(lease_until);
                job.receipt = Some(receipt);
                Ok(Some(Reservation::new(
                    job.envelope.clone(),
                    receipt,
                    self.lease,
                )))
            }
            None => Ok(None),
        }
    }

    async fn ack(&self, receipt: ReservationReceipt) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        inner.jobs.remove(pos);
        Ok(())
    }

    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        let job = &mut inner.jobs[pos];
        job.envelope.attempts += 1;
        job.available_at = now + delay;
        job.leased_until = None;
        job.receipt = None;
        Ok(())
    }

    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        let job = inner.jobs.remove(pos);
        inner.dead.push(DeadLetter::new(job.envelope, error));
        Ok(())
    }

    async fn extend(&self, receipt: ReservationReceipt) -> Result<()> {
        let now = self.clock.now();
        let lease_until = now + self.lease;
        let mut inner = self.lock();
        // The same validity check as every other resolution: an expired or
        // superseded receipt is rejected without touching the job.
        let pos = Self::find_current_receipt(&mut inner, receipt, now)?;
        inner.jobs[pos].leased_until = Some(lease_until);
        Ok(())
    }
}
