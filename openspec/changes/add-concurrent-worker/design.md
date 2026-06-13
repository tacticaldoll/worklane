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
- Multi-core parallelism (this delivers in-task concurrency; a spawn-based
  parallel executor is deferred to BACKLOG).
- Multi-lane / fair scheduling across lanes (a lane follow-up this unlocks but
  does not deliver).
- Lease extension/renewal (step 5) — this change only makes the lease-too-short
  problem observable.
- A broker connection pool (handlers run outside the broker lock; not needed).
- Eliminating the idle thundering-herd of N empty reserves (negligible here).

## Decisions

### 1. Bounded in-flight with no queue (Model A shape)

Up to N `reserve → dispatch → resolve` units run at once; a new job is reserved
only when there is free capacity, so **in-flight is bounded by N automatically,
with no queue and no semaphore**. This reuses the already-durable-validated
`process`/`resolve` path verbatim.

*Alternative considered — Model B (one reserve loop fanning jobs to a bounded
dispatch pool via a `Semaphore`):* rejected. Its only advantage is backpressure
on an unbounded queue — but there is no queue here (we reserve only when free),
so the semaphore solves a problem this design does not have. B's real consumer
is a future network broker where idle empty-reserves are expensive (Decision 5).

### 2. In-task concurrency via `FuturesUnordered`, not spawned tasks

The N units are `self.process(reservation)` futures driven concurrently on
`run`'s own task with `futures_util::stream::FuturesUnordered`; `run` stays
`&self`. They interleave at await points — concurrency, not multi-core
parallelism.

*Why not `tokio::spawn` N tasks for true parallelism (the earlier draft):*
implementation showed it makes shutdown and draining **cross-task and
non-deterministic** — a shutdown fired from within a handler isn't observed
before the next reservation without scheduler hacks, and the existing poll-loop
tests (which assume a job drains within one task poll) break. Spawning also
forces `run` to `self: Arc<Self>`. In-task concurrency keeps `run(&self)`,
preserves every existing test verbatim, observes shutdown synchronously, and
drains deterministically. The cost is no CPU parallelism, which a *first*
concurrent worker does not need (background jobs are usually IO-bound, and the
lease contention step 4 targets comes from overlap, not from cores). Multi-core
parallelism — a spawn-based executor, or users running several `run()` futures
on a multi-thread runtime — is recorded in BACKLOG.

*Cost:* adds the `futures-util` dependency (home of `FuturesUnordered`); tokio
has no in-task equivalent. This supersedes the proposal's "no new dependency"
aim, accepted in exchange for the determinism and `&self` simplicity above.

### 3. `with_concurrency(n)`, default 1; only `run` is concurrent

Concurrency is an opt-in builder (`Worker::with_concurrency(n)`), defaulting to
1. At N=1 the pool is a single loop, behaviourally identical to today, so every
existing worker-spec scenario and facade test stays green unchanged.
`process_next` and `run_until_idle` remain sequential — they are the unit-test
primitives and the building block each loop calls.

### 4. Shutdown: polled in-task at the top of the loop, then drain

The public `run(shutdown: impl Future<Output = ()>)` signature is unchanged.
Because `run` is a single task, it polls the pinned shutdown future directly with
a non-blocking biased probe at the top of each loop iteration — so a signal
(including one fired from within a handler) is observed **between reservations**,
deterministically, with no cross-task latency. On shutdown the loop stops
reserving and awaits the `FuturesUnordered` to empty — every in-flight job runs
to completion and resolves before `run` returns. This is the N-job
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
restructure `run` to drive up to N `process` futures in-task via
`FuturesUnordered`, with a top-of-loop shutdown probe and a drain phase; (3)
confirm N=1 keeps every existing worker scenario green; (4) add concurrency tests
(bounded in-flight; drains N on shutdown; lease-too-short redelivery is
stale-rejected) on the in-memory broker with a manual clock; (5) DoD; (6) record
BACKLOG follow-ons. Rollback = revert.

## Open Questions

None blocking. Whether `run` should take `self: Arc<Self>` directly or keep
`&self` via an internal `Arc` clone is an implementation detail settled during
apply; the public surface added is just `with_concurrency`.
