//! In-memory [`Broker`] for `worklane`, for development and tests.
//!
//! Jobs live in process memory. Reservation uses a visibility lease: a reserved
//! job is hidden for a lease duration and becomes visible again if it is not
//! acked, retried, or failed before the lease expires (at-least-once delivery).
//! Time comes from a [`Clock`] seam so tests can advance it deterministically.
//!
//! v0.1 uses a single logical lane; the `lane` argument is accepted but not yet
//! used to partition jobs.

mod clock;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use worklane_core::{Broker, DeadLetter, Error, JobEnvelope, JobId, NewJob, Result};

pub use clock::{Clock, ManualClock, SystemClock};

/// The default visibility lease duration.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

struct JobState {
    envelope: JobEnvelope,
    /// When the job becomes visible for reservation.
    available_at: Duration,
    /// When the current lease expires, if the job is reserved.
    leased_until: Option<Duration>,
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
        let envelope = JobEnvelope {
            id,
            kind: job.kind,
            payload: job.payload,
            attempts: 0,
            max_attempts: job.max_attempts,
        };
        self.lock().jobs.push(JobState {
            envelope,
            available_at: now,
            leased_until: None,
        });
        Ok(id)
    }

    async fn reserve(&self, _lane: &str) -> Result<Option<JobEnvelope>> {
        let now = self.clock.now();
        let lease_until = now + self.lease;
        let mut inner = self.lock();

        for job in inner.jobs.iter_mut() {
            // Release any expired lease so the job is visible again.
            if let Some(until) = job.leased_until
                && until <= now
            {
                job.leased_until = None;
            }
        }

        let slot = inner
            .jobs
            .iter_mut()
            .find(|job| job.leased_until.is_none() && job.available_at <= now);

        match slot {
            Some(job) => {
                job.leased_until = Some(lease_until);
                Ok(Some(job.envelope.clone()))
            }
            None => Ok(None),
        }
    }

    async fn ack(&self, id: JobId) -> Result<()> {
        let mut inner = self.lock();
        let pos = inner
            .jobs
            .iter()
            .position(|job| job.envelope.id == id)
            .ok_or_else(|| Error::Broker(format!("ack: unknown job {id}")))?;
        inner.jobs.remove(pos);
        Ok(())
    }

    async fn retry(&self, id: JobId, delay: Duration) -> Result<()> {
        let now = self.clock.now();
        let mut inner = self.lock();
        let job = inner
            .jobs
            .iter_mut()
            .find(|job| job.envelope.id == id)
            .ok_or_else(|| Error::Broker(format!("retry: unknown job {id}")))?;
        job.envelope.attempts += 1;
        job.available_at = now + delay;
        job.leased_until = None;
        Ok(())
    }

    async fn fail(&self, id: JobId, error: String) -> Result<()> {
        let mut inner = self.lock();
        let pos = inner
            .jobs
            .iter()
            .position(|job| job.envelope.id == id)
            .ok_or_else(|| Error::Broker(format!("fail: unknown job {id}")))?;
        let job = inner.jobs.remove(pos);
        inner.dead.push(DeadLetter {
            envelope: job.envelope,
            error,
        });
        Ok(())
    }
}
