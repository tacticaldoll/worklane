//! Broker middleware via the decorator pattern.
//!
//! `Broker` is an ordinary trait, so a cross-cutting concern (here: counting
//! every enqueue and fail) is added by *wrapping* a broker rather than changing
//! the `Broker` contract or any backend. The wrapper is itself a `Broker`, so the
//! `Client` and `Worker` accept it transparently as `Arc<dyn Broker>`. Stack
//! several wrappers to compose concerns (audit + encryption + metrics).
//!
//! The cost of this pattern is the boilerplate below: a decorator must forward
//! every method it does not override. That is deliberate — it keeps the broker
//! contract minimal and pushes interception entirely into user space.
//!
//! Run with: `cargo run -p worklane --example broker_middleware`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use worklane::{
    Broker, Client, HandlerResult, Job, JobContext, JobId, JobState, Lane, NewJob, Reservation,
    ReservationReceipt, Result, Worker, async_trait,
};
use worklane_memory::InMemoryBroker;

/// A decorator that records how many jobs were enqueued and failed, delegating
/// every operation to the wrapped broker.
struct AuditBroker<B> {
    // Held as `Arc<B>` so the `scheduled_store` accessor (which takes
    // `self: Arc<Self>`) can hand out the inner broker's scheduled store.
    inner: Arc<B>,
    enqueued: AtomicU64,
    failed: AtomicU64,
}

impl<B> AuditBroker<B> {
    fn new(inner: B) -> Self {
        AuditBroker {
            inner: Arc::new(inner),
            enqueued: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl<B: Broker> Broker for AuditBroker<B> {
    // --- intercepted methods -------------------------------------------------
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let id = self.inner.enqueue(job).await?;
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    async fn fail(&self, receipt: ReservationReceipt, error: String) -> Result<()> {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.inner.fail(receipt, error).await
    }

    // --- forwarded methods ---------------------------------------------------
    async fn enqueue_batch(&self, jobs: Vec<NewJob>) -> Result<Vec<JobId>> {
        self.inner.enqueue_batch(jobs).await
    }
    async fn reserve(&self, lane: &Lane) -> Result<Option<Reservation>> {
        self.inner.reserve(lane).await
    }
    async fn ack(&self, receipt: ReservationReceipt) -> Result<()> {
        self.inner.ack(receipt).await
    }
    async fn retry(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        self.inner.retry(receipt, delay).await
    }
    async fn defer(&self, receipt: ReservationReceipt, delay: Duration) -> Result<()> {
        self.inner.defer(receipt, delay).await
    }
    async fn extend(&self, receipt: ReservationReceipt) -> Result<()> {
        self.inner.extend(receipt).await
    }
    async fn classify(&self, id: JobId) -> Result<JobState> {
        self.inner.classify(id).await
    }

    // Optional capabilities are surfaced by delegating to the inner broker, so a
    // decorator preserves whatever the wrapped backend supports.
    fn dead_letter_store(&self) -> Option<&dyn worklane::DeadLetterStore> {
        self.inner.dead_letter_store()
    }
    fn scheduled_store(self: Arc<Self>) -> Option<Arc<dyn worklane::ScheduledStore>> {
        // The owned accessor delegates by cloning the inner `Arc<B>`; a complete
        // decorator must forward all three capability accessors, not just these.
        self.inner.clone().scheduled_store()
    }
    fn queue_stats(&self) -> Option<&dyn worklane::QueueStats> {
        self.inner.queue_stats()
    }
}

#[derive(Serialize, Deserialize)]
struct Greet {
    name: String,
}

struct GreetJob;

#[async_trait]
impl Job for GreetJob {
    type Payload = Greet;
    type Output = ();
    const KIND: &'static str = "greet";

    async fn run(&self, _ctx: JobContext, payload: Greet) -> HandlerResult<()> {
        if payload.name.is_empty() {
            // Exhaust the single attempt so this routes to `fail`, exercising the
            // intercepted failure path.
            return Err("empty name".into());
        }
        println!("hello, {}", payload.name);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Wrap the real broker once; everything downstream sees only `Arc<dyn Broker>`.
    let audit = Arc::new(AuditBroker::new(InMemoryBroker::new()));
    // One attempt per job, so an empty name dead-letters through the intercepted
    // `fail` in a single pass rather than retrying.
    let client = Client::new(audit.clone()).with_max_attempts(1);

    let mut worker = Worker::new(audit.clone());
    worker.register(GreetJob)?;

    client
        .enqueue::<GreetJob>(Greet {
            name: "world".to_string(),
        })
        .await?;
    client
        .enqueue::<GreetJob>(Greet {
            name: String::new(),
        })
        .await?;

    let worker = worker.build()?;
    worker.run_until_idle().await?;

    println!(
        "audit: {} enqueued, {} failed",
        audit.enqueued.load(Ordering::Relaxed),
        audit.failed.load(Ordering::Relaxed),
    );
    Ok(())
}
