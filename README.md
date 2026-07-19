# worklane

> Conformance-verified background jobs for Rust services.

`worklane` is a small, Rust-native async background job runner: enqueue typed
jobs and run workers with retries, ack/fail semantics, dead-lettering, and
pluggable brokers whose lifecycle behavior is verified by one shared
conformance suite.

> **Status: 0.1.0 baseline.** The core loop is solid across four brokers — in-memory,
> SQLite, PostgreSQL, and Redis — all passing a shared conformance suite: typed
> enqueue, lane partitioning, a long-running worker with bounded concurrency,
> lease renewal with a handler timeout, retry and an inspectable, replayable
> dead-letter store (read & requeue), scheduled (delayed) enqueue, unique-key
> deduplication, and handler panic isolation. The durable brokers survive
> process restarts and use a baseline storage schema. The public API favors
> additive evolution, though the `Broker` contract may still change before 1.0.

## What makes it different

Most "broker-agnostic" runners abstract at the **transport** layer: they unify
the *messaging* API across backends, but job *behavior* — acks, visibility
timeouts, retries, dead-lettering — is built on top and **varies by broker**,
which is why such tools accumulate a long tail of per-broker caveats.

`worklane` abstracts one layer up, at the **job lifecycle**. Every backend
implements the same minimal `Broker` contract and must pass **one shared
conformance suite** (`worklane-test`) — so behavior is *identical* across
backends, checked rather than assumed. The contract is deliberately small enough
that this uniformity is actually achievable.

```text
             the core Broker contract (job lifecycle)
      enqueue · reserve · ack · retry · fail · lease · classify
   + optional capabilities via accessors: dead-letter inspect/requeue
                          · batch · queue stats · scheduling
                               │ implemented by
      ┌──────────┬─────────────┼─────────────┬────────────────────┐
   in-memory   SQLite     PostgreSQL        Redis        (your own broker)
      └──────────┴─────────────┼─────────────┴────────────────────┘
                               │ every backend must pass
                 ┌─────────────▼──────────────┐
                 │  the SAME conformance suite │   ← the differentiator:
                 │      (worklane-test)        │     uniform behavior is
                 └─────────────────────────────┘     verified, not per-broker
                                                      caveats
```

So you pick the backend you already run (your SQL database, your Redis), switch
between them without behavior surprises, and — once the broker SPI is opened —
third parties can add a backend that *provably* behaves the same. It
intentionally does less (no exchange/routing model, no broad transport list) so
that what it does is small, typed, and conformance-checked.

Broker authors should start with the custom broker guide in
[`docs/custom-brokers.md`](docs/custom-brokers.md). The verified lifecycle is
summarized in [`docs/lifecycle-semantics.md`](docs/lifecycle-semantics.md), and
first-party capability coverage is tracked in
[`docs/broker-conformance-matrix.md`](docs/broker-conformance-matrix.md).

## When not to use worklane

`worklane` is not a general message bus, a Kafka-style event stream, or a
workflow engine at the broker layer. It does not promise exactly-once execution
or remove the need for idempotent handlers. Use it when you want typed
background jobs with verified lifecycle semantics across supported backends.

## Core loop

```text
typed payload -> envelope -> broker reserve -> dispatch by kind
              -> run handler -> ack / retry / fail / dead-letter
```

## Install

Add `worklane` plus the broker you want — each backend is its own crate, so you
depend only on the one you use:

```toml
[dependencies]
worklane = "0.1"
worklane-memory = "0.1"      # dev/tests; swap for a durable broker below
# worklane-sqlite   = "0.1"
# worklane-postgres = "0.1"
# worklane-redis    = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
```

`async_trait` is re-exported as `worklane::async_trait`, so you do not need to
depend on it directly.

## Quick start

```rust
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use worklane::{async_trait, Client, HandlerResult, Job, JobContext, Worker};
use worklane_memory::InMemoryBroker;

#[derive(Serialize, Deserialize)]
struct SendEmail { user_id: u64 }

struct SendEmailJob;

#[async_trait]
impl Job for SendEmailJob {
    type Payload = SendEmail;
    type Output = ();
    const KIND: &'static str = "send_email";
    async fn run(&self, _ctx: JobContext, payload: SendEmail) -> HandlerResult<()> {
        println!("sending email to user {}", payload.user_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Arc::new(InMemoryBroker::new());
    let client = Client::new(broker.clone());

    let mut worker = Worker::new(broker.clone());
    worker.register(SendEmailJob)?;

    client.enqueue::<SendEmailJob>(SendEmail { user_id: 42 }).await?;

    // Or use the builder for advanced options (delays, uniqueness, priority).
    // `build_job` serializes the payload eagerly, so it returns a `Result`.
    client.build_job::<SendEmailJob>(SendEmail { user_id: 43 })?
        .with_delay(std::time::Duration::from_secs(60))
        .with_unique_key("welcome_email_43")
        .with_priority(10)
        .enqueue()
        .await?;

    let worker = worker.build()?;
    worker.run_until_idle().await?;
    Ok(())
}
```

Run it with `cargo run -p worklane --example basic`.

> **Delivery is at-least-once.** A job may run more than once (after a lease
> expiry or a crash), so **handlers must be idempotent.**

For handlers that can legitimately run longer than the broker's visibility
lease, set `Worker::with_handler_timeout(..)`: the worker heartbeats to hold the
reservation while the handler runs within the timeout, and routes it to
retry/dead-letter if it exceeds it — so a slow handler keeps its lease and a
stuck one stays bounded.

## Choosing a broker

The quick start uses the in-memory broker. Every broker implements the same
`Broker` contract, so switching backends is a one-line change — the `Client` and
`Worker` code above is identical regardless of which you pick:

```rust
use std::sync::Arc;

// In-memory (dev/tests):
let broker = Arc::new(worklane_memory::InMemoryBroker::new());

// Durable — pick the store you already run (persists across restarts,
// creates its baseline schema on first open):
let broker = Arc::new(worklane_sqlite::SqliteBroker::open("jobs.db")?);
let broker = Arc::new(
    worklane_postgres::PostgresBroker::connect("postgres://localhost/app").await?,
);
let broker = Arc::new(worklane_redis::RedisBroker::connect("redis://localhost").await?);
```

## Failure, retries, and dead-letters

A handler signals failure by returning `Err(..)` (`HandlerError` is
`Box<dyn std::error::Error + Send + Sync>`). The worker retries with exponential
backoff up to the job's `max_attempts` (default 5), then moves it to the
dead-letter store:

```rust
async fn run(&self, _ctx: JobContext, payload: SendEmail) -> HandlerResult<()> {
    send_email(payload).await?; // any Err here -> retry, then dead-letter
    Ok(())
}
```

Set the limit per job with `.with_max_attempts(n)` on the builder (or
`Client::with_max_attempts(n)` for the client default). Inspect and replay the
dead-letter store with the `wl` CLI (`wl dead-letters list` / `wl dead-letters
requeue <id>`) or programmatically via `broker.dead_letter_store()` →
`DeadLetterStore::{read_dead_letters, requeue}`.

## Running a worker in production

The quick start calls `run_until_idle` (drain the lane, then return — handy for
scripts and tests). A long-running service calls `run(shutdown)`, which loops
until the `shutdown` future resolves, draining in-flight jobs first:

```rust
let mut worker = Worker::new(broker.clone())
    .with_lane("emails".parse()?) // this worker drains one lane
    .with_concurrency(16);        // up to 16 handlers at once
worker.register(SendEmailJob)?;

let worker = worker.build()?;
worker.run(async { tokio::signal::ctrl_c().await.ok(); }).await?;
```

Run one worker per lane to partition work across processes; enqueue to a
specific lane with `client.enqueue_to::<J>(lane, payload)` or the builder's
`.with_lane(..)`.

## Scheduling

To enqueue a job on a recurring schedule, add the `worklane-scheduler` crate,
register the job with a `Scheduler`, and run the daemon alongside your worker:

```rust
use worklane_scheduler::Scheduler;

let mut scheduler = Scheduler::new(broker.clone());
scheduler.schedule::<SendEmailJob>(
    "digest",
    "0 0 * * * *",
    SendEmail { user_id: 42 },
)?;
scheduler.run(shutdown).await?;
```

Cron expressions use the `cron` crate's seconds-first format, evaluated in UTC by
default; set a timezone (DST-aware) with `Scheduler::with_timezone(..)`. Missed
occurrences (while the scheduler was down) are not backfilled; pass
`schedule_unique` to make each fire idempotent via the unique-key dedup.

## Workspace

| Crate | Role |
|-------|------|
| `worklane` | Public-facing facade API |
| `worklane-core` | Traits, job model, envelope, errors |
| `worklane-memory` | In-memory broker for dev/tests |
| `worklane-sqlite` | Durable SQLite broker |
| `worklane-postgres` | Durable PostgreSQL broker (pooled, `FOR UPDATE SKIP LOCKED`) |
| `worklane-redis` | Durable Redis broker (atomic Lua scripts) |
| `worklane-pubsub` | Topic → lane fan-out built on the public API |
| `worklane-scheduler` | Recurring (cron) schedule daemon built on the public API |
| `worklane-otel` | OpenTelemetry trace-context propagation across the queue |
| `worklane-metrics` | Metrics-facade observer for worker outcomes and queue depth |
| `worklane-cli` | Operator CLI (`wl`) for inspecting and maintaining brokers |
| `worklane-test` | Reusable broker conformance suite |

## Results, Large Payloads, And Metrics

Durable result stores live alongside the SQLite, PostgreSQL, and Redis brokers
and implement `worklane_core::ResultStore`. Workers can store successful typed
outputs before acking, and clients retrieve them with `Client::get_result<T>`,
which is gated by broker lifecycle state so stale side-store bytes are not
reported as completed live or dead-lettered jobs.

Large payloads use the Claim Check pattern. Configure a `PayloadStore` on the
client and worker to offload payload bytes above a threshold; the broker carries
only a compact reference, and the worker resolves and deletes it after a
successful ack.

`worklane-metrics` provides an optional `metrics`-facade observer for job
attempt gauges, outcome counters, duration histograms, and queue-depth gauges.
Applications install their exporter of choice and wire the observer through
`Worker::with_observer`.

## Development

This project uses spec-driven development via
[OpenSpec](https://github.com/Fission-AI/OpenSpec). See [`AGENTS.md`](AGENTS.md)
for the workflow, `openspec/specs/` for the authoritative job-lifecycle
semantics, [`docs/development-flow.md`](docs/development-flow.md) for the
change/commit checklist, [`docs/release-checklist.md`](docs/release-checklist.md)
for crates.io release steps,
[`docs/lifecycle-semantics.md`](docs/lifecycle-semantics.md) for a readable
runtime semantics guide, [`docs/custom-brokers.md`](docs/custom-brokers.md) for
broker-author SPI and conformance wiring,
[`docs/broker-conformance-matrix.md`](docs/broker-conformance-matrix.md) for
first-party suite coverage,
[`docs/known-limitations.md`](docs/known-limitations.md) for support boundaries,
and [`BACKLOG.md`](BACKLOG.md) for deferred ideas.

**Running the broker tests against live services.** The durable-broker
(Postgres/Redis) conformance, poison, and retention tiers gate on
`WORKLANE_POSTGRES_TEST_URL` / `WORKLANE_REDIS_TEST_URL` and skip cleanly when
those are unset, so a plain `cargo test` needs no infrastructure. To run them,
start the bundled services (matching CI) and point the tests at them — `just
test-live` does both — or by hand:

```sh
docker compose up -d --wait          # local Postgres + Redis
set -a; source .env.example; set +a  # export the two test URLs
cargo test --workspace
```

The services and URLs are non-secret local endpoints defined in
[`docker-compose.yml`](docker-compose.yml) and [`.env.example`](.env.example).
A var that is set but unreachable makes its tests **fail** (not skip), so only
export them when the services are up.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
