## Context

`Worker::run` ([crates/worklane/src/worker.rs](../../../crates/worklane/src/worker.rs))
is strictly sequential: the loop calls `process_next().await`, which reserves one
job and runs it to resolution before returning, so the loop is blocked for the
whole handler. A single worker therefore never overlaps handlers and never
re-reserves its own in-flight job.

The `Broker` contract is now durable-validated (step 3, `worklane-sqlite`), so
step 4 — bounded worker concurrency — is unblocked. It is the first place real
lease contention appears, and it is governed by *Least commitment* and the
*Change prioritization* rule (concurrency is scale-out, allowed now that the
contract is strong).

## Goals / Non-Goals

**Goals:**
- Run up to N handlers concurrently in `run`, bounded and back-pressure-free.
- Keep N=1 (the default) behaviourally identical to today.
- Drain all in-flight jobs on cooperative shutdown.
- No `Broker` trait change and no `worklane-core` change.

**Non-Goals:**
- Multi-lane / fair scheduling across lanes (a lane follow-up this unlocks but
  does not deliver).
- Lease extension/renewal (step 5) — this change only makes the lease-too-short
  problem observable.
- A broker connection pool (handlers run outside the broker lock; not needed).
- Eliminating the idle thundering-herd of N empty reserves (negligible here).

## Decisions

### 1. Model A — a pool of N independent loops, not a reserve+dispatch pool

Spawn N tasks, each running its own reserve→dispatch→resolve loop over a shared
`Arc<Worker>`. Each task holds at most one job, so **in-flight is bounded by N
automatically, with no queue and no semaphore**.

*Alternative considered — Model B (one reserve loop fanning jobs to a bounded
dispatch pool via `Semaphore` + `JoinSet`):* rejected. Its only advantage over A
is backpressure on an unbounded queue — but A has no queue (a task only reserves
when it is free), so B's semaphore solves a problem A does not have. A also
reuses the already-durable-validated `process`/`resolve` path verbatim. B's real
consumer is a future network broker where idle empty-reserves are expensive
(Decision 5); revisit B then.

### 2. True parallelism via `tokio::spawn` over `Arc<Worker>`

Each loop runs as a spawned task (real parallelism for CPU- and IO-bound
handlers), which requires `'static` + `Send`, so `run` takes `self: Arc<Self>`
(or an inner Arc). `Worker`'s fields already suit sharing: `broker:
Arc<dyn Broker>`, handlers are `Box<dyn Dispatch>` (`Send + Sync`) behind the
struct, and the rest are cheap and immutable during `run`.

*Alternative considered — `join_all` of N borrowing futures on one task:* gives
concurrency but not parallelism (a CPU-bound handler blocks the others on one
thread). For a job runner, true parallelism is the point; the `Arc<Worker>`
restructure is small.

### 3. `with_concurrency(n)`, default 1; only `run` is concurrent

Concurrency is an opt-in builder (`Worker::with_concurrency(n)`), defaulting to
1. At N=1 the pool is a single loop, behaviourally identical to today, so every
existing worker-spec scenario and facade test stays green unchanged.
`process_next` and `run_until_idle` remain sequential — they are the unit-test
primitives and the building block each loop calls.

### 4. Shutdown: fan one signal out to N loops, then drain

The public `run(shutdown: impl Future<Output = ()>)` signature is unchanged. A
single `impl Future` cannot be awaited by N tasks, so internally a small task
awaits it and flips a `tokio::sync::watch<bool>` that every loop observes (no new
dependency; `watch` latches, so a late-started loop still sees shutdown). Each
loop honours shutdown **between jobs** (never cancelling a running handler), so
on shutdown the loops stop reserving and `run` joins all N tasks — every
in-flight job runs to completion and resolves first. This is the N-job
generalization of today's cooperative, between-jobs shutdown.

### 5. No connection pool: handlers run outside the broker lock

`SqliteBroker` serializes on a single `Mutex<Connection>`, but the lock is held
only for the brief reserve/ack/retry/fail SQL — handlers run between, outside the
lock. So N handlers run fully in parallel and only their short broker calls
serialize; the single connection is sufficient. A pool stays deferred (its
consumer is a higher-throughput or network backend).

### 6. Error handling: fail-fast but drain

A non-stale fatal error from any loop flips the shutdown watch (stopping the
other loops from reserving), then `run` drains in-flight and returns the first
such error — the faithful generalization of today's `?` propagation. Stale
resolutions remain non-fatal and logged (existing `resolve` behaviour), which is
exactly what absorbs the redelivery a lease-too-short handler can cause.

*Alternative considered — resilient daemon (log fatal errors and keep going):*
arguably desirable but a separate behavioural decision and spec change; recorded
in BACKLOG, not folded in.

## Risks / Trade-offs

- **Lease-too-short under concurrency → duplicate execution** → This is the new
  surface, by design: a handler outliving its lease while a sibling has capacity
  is reserved again and runs twice (at-least-once). The loser's late resolution
  is rejected as stale and logged, so nothing crashes. Mitigation is step 5
  (lease extension); here it is specified as behaviour, not fixed.
- **Idle thundering-herd** → N idle loops each poll every interval = N empty
  reserves/interval. Negligible on in-memory/SQLite (brief, lock-serialized,
  returning `None`); recorded in BACKLOG. Only an expensive-reserve network
  broker would warrant a single-reserver design (Model B) or a shared idle wake.
- **`Arc<Worker>` restructure** → `run` moves from `&self` to `self: Arc<Self>`;
  small and additive, callers wrap in `Arc` (or a thin shim keeps `&self`
  ergonomics). N=1 equivalence guards against regression.
- **Handler thread-safety** → handlers already are `Send + Sync` and run with a
  per-job `JobContext`; shared mutable state is the handler author's
  responsibility, unchanged by this work.

## Migration Plan

Pre-release. Order: (1) add `concurrency` field + `with_concurrency`; (2)
restructure `run` to spawn N loops over `Arc<Worker>` with a `watch` shutdown
fan-out and drain-join; (3) confirm N=1 keeps every existing worker scenario
green; (4) add concurrency tests (bounded in-flight; drains N on shutdown;
lease-too-short redelivery is stale-rejected) on the in-memory broker with a
manual clock; (5) DoD; (6) record BACKLOG follow-ons. Rollback = revert.

## Open Questions

None blocking. Whether `run` should take `self: Arc<Self>` directly or keep
`&self` via an internal `Arc` clone is an implementation detail settled during
apply; the public surface added is just `with_concurrency`.
