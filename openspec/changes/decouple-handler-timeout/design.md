## Context

`Worker::run` reserves jobs and spawns each `ctx.process(..)` onto a `JoinSet`
(`crates/worklane/src/worker/mod.rs`), so jobs already run one-per-task. Inside
`process`, when a handler timeout or lease keepalive is configured,
`run_maintained` (`crates/worklane/src/worker/execution.rs`) runs the handler and
races a timeout `deadline` against it with `tokio::select!` **on that same task**,
while the heartbeat already runs on its own `tokio::spawn`ed task
(`AbortOnDrop`).

Because the handler and its timeout share one task, the timeout can only fire
when the handler yields at an `.await`. A CPU-bound or blocking handler that never
yields is never timed out; on a multi-thread runtime the independent heartbeat
keeps extending the lease, so the job is never redelivered and its `JoinSet` slot
is occupied forever. Enough such handlers wedge the worker silently. The current
`worker` spec documents this as an accepted limitation and calls the fix a
"separate, breaking change tracked in the backlog."

## Goals / Non-Goals

**Goals:**
- A configured handler timeout fires regardless of whether the handler yields
  (multi-thread runtime): the slot is freed and the job resolved via the failure
  path.
- No new public bound or API change; cooperative-handler and default
  (no-timeout) behavior unchanged.
- Correct the backlog's mistaken "breaking" rationale.

**Non-Goals:**
- Preempting a truly non-yielding CPU-bound handler (impossible under cooperative
  async scheduling — out of scope for every in-process async runner).
- Changing the heartbeat (already on its own task) or the reserve-loop per-job
  spawn.
- Any current-thread-runtime guarantee for non-yielding handlers (use
  `spawn_blocking`).

## Decisions

**D1 — Run the handler on its own task; race the timeout against its `JoinHandle`.**
`run_maintained` spawns the (owned) handler future with `tokio::spawn` and
`select!`s the timeout `deadline` against the handler task's `JoinHandle` instead
of against the handler future inline. The timeout arm then advances on
`run_maintained`'s task independently of whether the *handler* yields — **so long
as a runtime worker thread is free to poll `run_maintained`'s task** (see R1). On
timeout, abort the handler `JoinHandle` (signalling cooperative `Cancellation`
too) and return `TimedOut`; on completion, return the handler's result.
- *Making the handler future `'static`*: the current handler is
  `Next::new(&self.middleware, dispatch.as_ref())` — the middleware chain
  **borrows** `self.middleware` and the `&dyn Dispatch`. Spawning it requires
  refactoring `Next` (an internal `pub(super)` type) to **own** its `Arc`s, and
  owning the payload (`Cow::Owned`). This is an internal change, no public-API or
  bound impact (confirm during apply that `Next` has no external callers needing
  the borrowed form).
- *Alternative rejected*: `tokio::time::timeout(d, handler)` — identical to the
  current inline `select!`; same-task, cannot fire for a non-yielding handler.
- *Panic isolation*: a handler panic now surfaces as `JoinError::is_panic()` on
  the `JoinHandle` rather than via the inline `catch_unwind`; map it to
  `Error::Handler` so the existing panic→failure-path behavior and worker
  survival are preserved. The no-timeout path keeps running inline with
  `catch_unwind`, so two mechanisms coexist and both must be tested (see R3).

**D2 — This is non-breaking; no new bound is introduced.** Handlers are already
`Send + Sync + 'static`: `pub trait Job: Send + Sync + 'static`
(`worklane-core/src/job.rs:146`), `trait Dispatch: Send + Sync` + `#[async_trait]`
(Send futures), and `ctx.process(..)` is *already* `JoinSet::spawn`ed in the
reserve loop (which only compiles because the whole chain is `Send + 'static`).
Spawning the handler sub-future requires the same bounds, satisfied by owning its
captures (clone the middleware `Arc`s, own the payload). The backlog item that
deferred this as needing new `Send + 'static` bounds (a breaking change) is
therefore wrong; the bounds already exist publicly.

**D3 — Preserve the existing structure otherwise.** The reserve-loop per-job
`JoinSet::spawn`, the heartbeat task, the `AbortOnDrop` teardown, the
stale-rejection handling, panic catching, and circuit-breaker accounting are
unchanged. The no-timeout/no-keepalive path still runs the handler inline with no
heartbeat.

## Risks / Trade-offs

- **R1 — thread saturation still stalls timeouts** → the timeout fires only if a
  worker thread is free to poll `run_maintained`'s task. If non-yielding handlers
  occupy every worker thread (worker concurrency ≥ runtime worker threads, all
  blocked), no timeout can be polled until one frees. The decoupling shrinks the
  blast radius (one stuck handler no longer wedges a thread-rich runtime) but does
  **not** eliminate the stall under saturation. Mitigation: the real bound is
  `spawn_blocking`, which moves blocking work off the async worker threads
  entirely. Stated in the spec; do not over-promise a yield-independent timeout.
- **R3 — panic-isolation mechanism split** → spawned-path panics come back as
  `JoinError`, inline-path panics via `catch_unwind`. Mitigation: map both to
  `Error::Handler`; test panic isolation on both the timeout and no-timeout paths.
- **Cannot preempt a non-yielding CPU task** → document it. `JoinHandle::abort()`
  only cancels at the next yield point, so the orphaned task keeps burning a
  thread until it yields/returns even after the timeout fires. The win is that
  the *slot is freed and the job resolved* (no silent wedge), not preemption.
  `spawn_blocking` remains the answer for CPU-bound work. Recorded in the spec and
  `docs/known-limitations.md`.
- **Current-thread runtime** → a non-yielding handler still blocks the single
  executor, so the timeout/heartbeat cannot run. Out of scope; same guidance.
- **Aborting at a non-idempotent point** → the handler is dropped/aborted, never
  resumed; resolution uses the receipt CAS, so a stale resolution from a
  late-finishing orphan is already rejected (`StaleReservation`). No new hazard.
- **One extra task per maintained job** → negligible; jobs are already one task
  each, and the heartbeat is already a task.

### Competitive note (verified against apalis source)

apalis (the comparable Rust/tokio runner) runs jobs **inline** in a
`FuturesUnordered` on the worker's own task by default and applies timeouts via a
same-task `tower::timeout::TimeoutLayer`; a non-yielding handler stalls the whole
worker (all in-flight jobs + the worker-level keep-alive heartbeat) and the job is
only redelivered after a worker-orphan sweep (~300s). worklane already isolates
each job on its own task; this change makes the clean per-job timeout the
**default**, which in apalis requires manually composing `.parallelize(tokio::
spawn)` with an outer `.timeout()`.

## Migration Plan

Internal refactor; no migration. Roll back by reverting the change — behavior
returns to the documented same-task limitation. No data, schema, or API impact.

## Open Questions

None. The non-breaking property is proven from existing bounds (D2); the
preemption limit is a documented, accepted residual (it is inherent to
cooperative async, not specific to this design).
