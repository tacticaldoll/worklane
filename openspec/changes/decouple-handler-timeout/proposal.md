## Why

The worker's optional **handler timeout** is silently ineffective against the
very failure it names — a non-cooperative handler. Because the timeout deadline
is selected against the handler future on the *same task*, a CPU-bound or
blocking handler that never `.await`s never lets the timeout fire; on a
multi-thread runtime the heartbeat (already on its own task) keeps extending the
lease, so the job is never redelivered and its concurrency slot is occupied
indefinitely. Enough such handlers silently wedge the worker. A 0.2.1
stability-hardening release is the moment to fix it — and the fix is
**non-breaking** (handlers are already `Send + Sync + 'static`), correcting a
backlog item that wrongly deferred it as a breaking change.

## What Changes

- Decouple the handler onto its **own task** and race the configured handler
  timeout against that task's `JoinHandle`, so the timeout fires independently of
  whether the handler yields — **as long as a runtime worker thread is free to
  poll it** (saturating every worker thread with non-yielding handlers still
  stalls; `spawn_blocking` remains the real bound for CPU-bound work). (Internal
  refactor — handlers are already `Send + Sync + 'static` and the per-job
  `process` future is already spawned, so this introduces no new public bound.)
- On timeout: abort the handler task, free the concurrency slot, and resolve the
  job through the existing failure path (retry while attempts remain, otherwise
  dead-letter with a timeout error), recording the circuit breaker — even when
  the handler never yielded.
- Preserve the no-timeout/no-keepalive default (no heartbeat; rely on lease
  expiry) and the cooperative-handler behavior unchanged.
- Document the honest residual: `abort` cannot *preempt* a truly non-yielding
  CPU-bound task on the runtime (the orphaned task keeps running until it yields
  or ends), and on a current-thread runtime such a handler still blocks the
  executor — CPU-bound work still belongs in `tokio::task::spawn_blocking`. The
  change converts a *silent permanent wedge* into a *bounded timeout +
  redelivery*, not preemption.

## Capabilities

### New Capabilities

<!-- none -->

### Modified Capabilities

- `worker`: the **Bounded long-handler support** requirement changes so the
  handler timeout fires independently of handler yielding (handler on its own
  task; timeout races its `JoinHandle`), correcting the prior behavior where the
  timeout shared the handler's task and could not fire for a non-yielding
  handler. The accurate concurrency model (handler, timeout, and heartbeat each
  independent of handler yielding on a multi-thread runtime) is recorded, along
  with the current-thread / true-CPU-bound residual.

## Impact

- **Code**: `crates/worklane/src/worker/execution.rs` (`run_maintained` /
  `process`) — handler spawned on its own task, timeout races the `JoinHandle`,
  abort on timeout. The internal middleware chain (`Next`, `pub(super)`) is
  refactored to own its `Arc`s so the handler future is `'static`. No change to
  the `worker/mod.rs` reserve loop's per-job spawn, the heartbeat task, or the
  public `Worker` / `Job` API.
- **Behavior**: observable change — a non-yielding handler with a configured
  timeout is now failed/redelivered and its slot freed (previously: wedged
  indefinitely). Cooperative handlers and the default (no-timeout) path are
  unchanged.
- **Docs**: `docs/known-limitations.md` (clarify the residual), `BACKLOG.md`
  (remove the "structural handler decoupling" item and its incorrect
  breaking-change rationale), `CHANGELOG.md`.
- **Not breaking**: no dependency, schema, wire-format, or public-API change.
