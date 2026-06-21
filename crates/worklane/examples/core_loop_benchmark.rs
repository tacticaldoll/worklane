//! Minimal core-loop benchmark for the in-memory broker.
//!
//! Run with:
//! `cargo run -p worklane --example core_loop_benchmark --release -- 10000 64`

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use worklane::{Client, HandlerResult, Job, JobContext, Worker, async_trait};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct NoopPayload {
    value: u64,
}

struct NoopJob {
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl Job for NoopJob {
    type Payload = NoopPayload;
    type Output = ();

    const KIND: &'static str = "benchmark.noop";

    async fn run(&self, _ctx: JobContext, _payload: NoopPayload) -> HandlerResult<()> {
        self.completed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jobs = parse_u64_arg(1, 10_000);
    let concurrency = parse_usize_arg(
        2,
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
    )
    .max(1);

    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());
    let completed = Arc::new(AtomicUsize::new(0));

    let mut worker = Worker::new(broker);
    worker = worker.with_concurrency(concurrency);
    worker.register(NoopJob {
        completed: completed.clone(),
    })?;
    let worker = worker.build()?;

    let total_started = Instant::now();
    let enqueue_started = Instant::now();
    for value in 0..jobs {
        client.enqueue::<NoopJob>(NoopPayload { value }).await?;
    }
    let enqueue_elapsed = enqueue_started.elapsed();

    let drain_started = Instant::now();
    worker.run_until_idle().await?;
    let drain_elapsed = drain_started.elapsed();
    let total_elapsed = total_started.elapsed();
    let completed = completed.load(Ordering::Relaxed);

    println!("jobs: {jobs}");
    println!("concurrency: {concurrency}");
    println!(
        "enqueue: {:?} ({:.0} jobs/s)",
        enqueue_elapsed,
        rate(jobs, enqueue_elapsed)
    );
    println!(
        "drain: {:?} ({:.0} jobs/s)",
        drain_elapsed,
        rate(completed as u64, drain_elapsed)
    );
    println!(
        "total: {:?} ({:.0} jobs/s)",
        total_elapsed,
        rate(completed as u64, total_elapsed)
    );
    println!("completed: {completed}");

    Ok(())
}

fn parse_u64_arg(position: usize, default: u64) -> u64 {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn parse_usize_arg(position: usize, default: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn rate(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}
