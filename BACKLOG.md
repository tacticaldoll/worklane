# Worklane Backlog

Future features intentionally **excluded from v0.1** unless absolutely necessary.
Active work and the MVP are tracked as OpenSpec changes under `openspec/changes/`;
this file is the upstream idea list that feeds `/opsx:propose`.

## Near-term sequencing (foundations before polish)

The agreed order of foundational changes; each de-risks the next. Steps 1–4 have
shipped (details in `openspec/changes/archive/`); step 5 is the live next item.

1. ✓ **`add-broker-contract-tests`** (shipped as `establish-broker-contract`) —
   reusable broker-agnostic conformance suite derived from the broker spec;
   lifted the `Clock` seam into `worklane-core` for deterministic time.
2. ✓ **`add-worker-poll-loop`** — long-running daemon loop with cooperative
   shutdown, built on `process_next`; recorded (did not fix) lease-too-short /
   handler-too-long.
3. ✓ **`add-sqlite-broker`** (first durable broker) — ran the contract suite to
   validate the `Broker` trait *without changing it* (the decoupling milestone).
4. ✓ **`add-concurrent-worker`** — in-task bounded concurrency (`with_concurrency`);
   made lease-too-short real and tested (a handler outliving its lease is
   redelivered and runs twice, at-least-once).
5. ⏭ **lease extension / renewal** (next) — heartbeat to hold a reservation past
   the lease for long handlers. Adds a `Broker` trait method — the first change
   to the trait since it was durable-validated — and now has both a durable
   backend and concurrency to test real contention. See "Concurrent-worker
   follow-ups" below for the motivation surfaced by step 4.

## Deferred (post-v0.1)

- Redis broker
- Postgres broker
- NATS / SQS backend
- cron / scheduled jobs
- priority queue
- result backend
- dashboard
- workflow chaining
- batch jobs
- rate limiting
- per-job concurrency limit
- job cancellation
- unique jobs / deduplication
- OpenTelemetry integration
- CLI management tool
- admin web UI
- distributed scheduler

### Lane follow-ups (after `add-lane-partitioning`)

First-class lane assignment ships in the `add-lane-partitioning` change; these
extensions are intentionally deferred:

- per-call lane override (e.g. `enqueue_to(lane, …)`); v0.1 sets the lane
  per-client via `Client::with_lane`
- expose the lane to handlers via `JobContext.lane`
- a `Lane` newtype with validation / interning (v0.1 uses a bare `String`)
- lane typo protection / registration — until then, jobs on a lane no worker
  reserves accumulate silently (deliberately an operator responsibility)
- multi-lane worker / fair scheduling across lanes (real payoff needs the
  concurrent worker in step 4 above; sequential fair scheduling would be a toy)

### Durable-broker follow-ups (after `add-sqlite-broker`)

The first durable broker (`worklane-sqlite`) validated the `Broker` trait
unchanged; these refinements it surfaced are intentionally deferred until a real
consumer needs them:

- `reserve` ordering: the broker spec is silent on which of several visible
  same-lane jobs is returned. `worklane-sqlite` uses `ORDER BY seq` (FIFO) as an
  *unspecified* implementation choice; promoting strict FIFO to the contract is
  deferred because the deferred priority-queue feature would reorder.
- `JobEnvelope::from_stored` + a columnar schema: `worklane-sqlite` stores the
  envelope as a serde blob to avoid any core change. A columnar backend
  (Postgres) is the first real consumer that benefits from an additive
  envelope-reconstruction constructor and individually queryable columns.
- restart-durable clock: `SystemClock` is monotonic and process-local, so
  persisted absolute times are meaningless across a process restart. A
  production durable broker needs a stable wall-clock-epoch clock; the broker is
  correct with respect to whatever clock it is given.
- connection pool / concurrent connections: `worklane-sqlite` uses a single
  connection behind a `Mutex`. Real connection concurrency belongs with the
  concurrent-worker step.
- schema versioning via `PRAGMA user_version`, once the schema must evolve.

### Concurrent-worker follow-ups (after `add-concurrent-worker`)

`Worker::run` ships **in-task** bounded concurrency (up to N overlapping handler
futures on one task via `FuturesUnordered`). These were deliberately deferred:

- multi-core parallelism: in-task concurrency overlaps IO-bound handlers but does
  not use multiple cores. A spawn-based parallel executor (or simply running
  several `run()` futures on a multi-thread runtime) is deferred until a
  CPU-bound workload needs it; it was rejected for the first cut because spawning
  made shutdown/drain non-deterministic (see the change's design Decision 2).
- idle re-reserve cadence: moot for the single-task model, but a future
  spawn-based or network-broker design must avoid N idle workers each polling an
  empty lane (a single-reserver / shared idle wake).
- resilient daemon: `run` currently fails fast on a non-stale broker error
  (drains, then returns it). A "log and keep running" mode is a separate
  behavioural decision, deferred.
- lease extension / renewal (sequencing step 5): now motivated — under
  concurrency a handler outliving its lease is redelivered and runs twice
  (at-least-once); a heartbeat to hold the lease is the mitigation.
- multi-lane / fair scheduling across lanes: unlocked by concurrency (see the
  lane follow-ups above), still deferred.

## Guiding principle

Protect the core loop. Every **deferred** item above is out of scope until the
core enqueue → reserve → dispatch → ack / retry / fail / dead-letter loop is
solid. The near-term sequencing is the active path toward that solidity, not
deferred work.
