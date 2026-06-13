## Why

The `Worker` processes strictly one job at a time: `process_next().await` blocks
the loop until a job fully resolves, so a single worker can never overlap
handlers or re-reserve its own in-flight job. Now that the `Broker` contract is
durable-validated (Near-term sequencing step 3), step 4 raises throughput by
running up to N handlers concurrently — and is the first place real lease
contention appears: a handler that outlives its lease can be redelivered and run
again, which makes the deferred lease-too-short problem observable and motivates
step 5 (lease extension).

## What Changes

- Add bounded concurrency to `Worker::run`: up to N `reserve → dispatch →
  resolve` futures run **in-task** via a `futures_util::FuturesUnordered`, each
  holding at most one job, so in-flight is bounded by N with no extra queue. (An
  earlier draft spawned N tasks for true parallelism; implementation showed that
  made shutdown/drain non-deterministic and broke the existing poll-loop tests,
  so the model is in-task concurrency — see design Decision 2. Multi-core
  parallelism is deferred to BACKLOG.)
- Add `Worker::with_concurrency(n)` (builder); **default 1**, which stays
  strictly sequential and equivalent to today. `process_next` and
  `run_until_idle` remain sequential test primitives, unchanged; `run` keeps its
  `&self` signature.
- Keep the public `run(shutdown: impl Future)` signature. Because `run` is one
  task, it polls the shutdown future directly at the top of each loop, so a
  signal — including one fired from within a handler — is observed
  deterministically between reservations. Shutdown drains **all** in-flight jobs
  to resolution before `run` returns (the N-job generalization of today's
  cooperative shutdown).
- Error handling under concurrency: a non-stale fatal error from a job's
  resolution stops further reserving, drains in-flight, and returns the first
  error (fail-fast but drain) — the faithful generalization of today's `?`
  propagation.
- No `Broker` trait change and no `worklane-core` change: concurrent reserve is
  just N calls to the existing `reserve`. The SQLite connection needs no pool —
  handlers run outside the broker lock, so only the brief reserve/resolve calls
  serialize.

Deliberately **not** in scope (recorded, not built — *Least commitment*):
multi-lane / fair scheduling across lanes (a lane follow-up whose payoff this
change unlocks but does not deliver); lease extension/renewal (step 5); a
connection pool; and the idle "thundering-herd" of N empty reserves per poll
interval (negligible on in-memory/SQLite; only bites an expensive-reserve
network broker, which does not exist yet).

## Capabilities

### New Capabilities

<!-- None. Concurrency is a change to the existing worker capability, not a new
     backend-agnostic capability. -->

### Modified Capabilities

- `worker`: the processing loop becomes **bounded concurrent** (up to N jobs in
  flight; default 1 = today's sequential behaviour), the long-running `run` loop
  honours that bound, and cooperative shutdown drains **all** in-flight jobs.
  A behavioural note is added: under concurrency a handler that outlives its
  lease may be redelivered and run again (at-least-once), its late resolution
  rejected as stale and logged.

## Impact

- **`crates/worklane/src/worker.rs`:** `Worker::run` gains a concurrent pool;
  new `with_concurrency` builder + a concurrency field; `run` restructured to
  spawn N loops over `Arc<Worker>` and drain on shutdown.
- **Public API (additive):** `Worker::with_concurrency(n)`; `run`/`process_next`
  signatures unchanged.
- **Dependencies:** adds `futures-util` (for `FuturesUnordered`) to the facade.
- **`worklane-core` / `Broker` trait:** unchanged.
- **`openspec/specs/worker`:** modified requirements (processing loop, poll loop,
  cooperative shutdown) at sync time; no requirement removed.
- **Backlog:** record the idle thundering-herd and reaffirm multi-lane / lease
  extension / connection pool as the follow-ons this unlocks.
