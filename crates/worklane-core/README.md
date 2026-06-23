# worklane-core

The backend-agnostic **contract** for `worklane` typed background jobs: the job
model, the `Broker` trait brokers implement, and the shared error/retry types.
Zero internal dependencies — everything else in the workspace depends on this.

## What this crate is

`worklane-core` defines the *interface* between three groups, and nothing else:

- **Broker authors** — implement `Broker` and optional capability traits for a
  new backend (Postgres, Redis, ...). The `spi` module hands you the shared
  wire-format plumbing so backends cannot drift.
- **Integrators** — write metrics/tracing/middleware crates against the model
  (e.g. implement the `JobObserver` SPI) without pulling in the whole facade.
- **The `worklane` facade** — re-exports this crate and adds the worker, client,
  and concrete brokers on top.

If you are *using* jobs (enqueue + run handlers), depend on `worklane`, not this.
Depend on `worklane-core` directly only when you are extending the system.

## Key public API

- Job model: `JobId`, `Lane` / `LaneRegistry`, `NewJob`, `JobEnvelope`,
  `Reservation` / `ReservationReceipt`, `DeadLetter`, `Job` / `JobContext`.
- Contracts: `Broker` (the core store/lifecycle trait) plus its optional
  capability traits `BatchEnqueue`, `DeadLetterStore`, `QueueStats`, and
  `ScheduledStore`, discovered through `Broker::batch_enqueue` /
  `dead_letter_store` / `queue_stats` / `scheduled_store` accessors;
  `PayloadStore` (Claim Check); `ResultStore`.
- Telemetry SPI: `JobObserver`, `JobEvent`, `JobAttemptEvent`, `JobOutcome`.
- Policy & time: `RetryPolicy` (capped exponential backoff + deterministic
  jitter), `RetentionPolicy`, `Clock` / `SystemClock` / `WallClock`.
- `Error` / `Result`, `redact_credentials`, `spi::*` (broker-author plumbing).

## Implementing the contract (sketch)

```rust,ignore
use async_trait::async_trait;
use worklane_core::{Broker, NewJob, JobId, Lane, Result, Reservation};

struct MyBroker { /* ... */ }

#[async_trait]
impl Broker for MyBroker {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        // persist the job's envelope, keyed by lane / available_at ...
        todo!()
    }
    async fn reserve(&self, lane: &Lane) -> Result<Option<Reservation>> { todo!() }
    // ... the remaining lifecycle methods
}
```

Broker authors should use `spi::*` for shared backend decisions such as envelope
encoding, receipt keys, duration conversion, stale-reservation errors, redaction,
and validation helpers. See `docs/custom-brokers.md` in the workspace for the
conformance wiring and migration notes.

## Layering

`worklane-core` sits at the bottom: it never depends on a broker, a worker, or
the facade, so a third-party broker or integration links only this crate.

## License

Licensed under either of MIT or Apache-2.0 at your option.
