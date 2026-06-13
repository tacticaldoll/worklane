## Context

`Worker` today exposes `process_next` (reserve + run one job → bool) and
`run_until_idle` (loop `process_next` until no currently-visible job). Neither can
run as a daemon: `run_until_idle` returns as soon as the lane has no *currently*
visible job, abandoning jobs scheduled for the future (pending retries). The
worker has no time dependency at all today. This change adds a long-running loop
and, with it, the worker's first need to *wait*.

`Clock` was lifted into `worklane-core` (with `now()` only) by
`establish-broker-contract`; the `Broker` trait and its conformance suite are
stable. This change is bounded to the `worklane` facade — `worklane-core` and
`worklane-memory` are untouched.

## Goals / Non-Goals

**Goals:**
- A long-running `Worker::run(shutdown)` usable as a daemon, built on the existing
  `process_next` primitive.
- Pending retries are eventually picked up (the liveness gap closes).
- Cooperative graceful shutdown: an in-flight job always resolves before return.
- Deterministic, fast tests with no real sleeping.

**Non-Goals:**
- Worker concurrency (still strictly one job at a time).
- Precise wakeup via a `next_available_at` `Broker` method (no trait change).
- Capped/exponential idle backoff.
- OS-signal (SIGTERM) wiring — that is the application's job.
- Lease extension/renewal.
- Adding `sleep` to the `Clock` trait.

## Decisions

### 1. Two kinds of time — the worker wait is not the broker clock

Broker *visibility* time (lease expiry, retry-delay maturation) is the broker's
contract, driven by the core `Clock` and already covered by the conformance
suite. The new *worker wait* (how long to pause between polls when idle) is a
separate concern. Poll-loop tests need to control only the worker wait: they use
**immediately-visible** jobs (enqueue makes a job visible now) and never re-assert
broker-time behaviour. So there is no "two clocks must stay in sync" problem — the
broker clock is irrelevant to these tests.

### 2. The worker waits via `tokio::time::sleep`; `Clock` is not involved

When idle, the worker sleeps with `tokio::time::sleep`. Rationale:
- *Don't reinvent tokio's timer.* Making a manual `Clock` awakenable on `advance`
  would re-implement tokio's timer wheel and wakers — rejected.
- *Least commitment.* A pluggable async-waiter seam has no consumer (the workspace
  is tokio-only), so it is not introduced.
- *Minimal contracts.* Waiting is a runtime concern, not a clock-contract concern,
  so `sleep` never goes on the `Clock` trait. `Clock` stays `now()`-only and the
  worker does not depend on `Clock` at all (a fixed interval needs only
  `sleep(d)`).

### 3. Layered API — `run` sits on `process_next`

```
process_next()      reserve + run ONE job -> bool        [primitive, kept]
run_until_idle()    while process_next() {}              [drain then stop, kept]
run(shutdown)       loop { drain; if shutdown break; sleep(interval) }  [daemon, NEW]
```

`run` drains currently-visible jobs as fast as they come (consecutive
`process_next` returning `true`), and only sleeps once a poll finds nothing.

### 4. Fixed poll interval, configurable

Idle wait is a fixed `poll_interval`, set by `Worker::with_poll_interval`
(default ~1s). Capped backoff (quieter when long-idle, at the cost of first-job
latency) and precise `next_available_at` wakeup are deferred.

### 5. Cooperative graceful shutdown

`run(&self, shutdown: impl Future<Output = ()>)`. The shutdown future is checked
only *between* jobs and during the idle wait — never mid-job. So an in-flight job
always runs to completion and resolves (ack/retry/fail) before `run` returns; no
job is orphaned to lease expiry by a graceful stop. Sketch:

```
tokio::pin!(shutdown);
loop {
    if process_next().await? { continue; }   // drained one; keep draining
    tokio::select! {
        _ = &mut shutdown => break,           // idle + asked to stop
        _ = tokio::time::sleep(poll_interval) => {} // idle; poll again
    }
}
```

A caller who instead hard-cancels (drops the `run` future) may drop a job
mid-execution; that job re-runs later under at-least-once delivery. The spec
states both paths.

## Risks / Trade-offs

- **Paused-time auto-advance spins an idle loop** → Under
  `#[tokio::test(start_paused = true)]`, virtual time auto-advances when the
  runtime is idle, so a `reserve→None→sleep` loop fast-forwards and burns real
  CPU. Mitigation: tests drive deterministically (spawn → enqueue an
  immediately-visible job → `tokio::time::advance(poll_interval)` → assert →
  signal shutdown) and wrap with `tokio::time::timeout` where needed. Documented
  as test discipline.
- **Fixed interval trades latency for simplicity** → A retry due in 1ms waits up
  to one `poll_interval`. Acceptable for v1; backoff and precise wakeup are
  deferred and additive (no rework of `run`'s shape).
- **Scope creep into concurrency** → Explicitly out; `run` stays strictly
  sequential, reusing `process_next` unchanged.

## Migration Plan

Additive, facade-only. Add `with_poll_interval` and `run` to `Worker`; keep
`process_next`/`run_until_idle`. No core/memory change, no data migration. The
example and README may later show a daemon-style `run`, but that is optional and
not required by this change.

## Open Questions

None blocking. `shutdown` is taken as `impl Future<Output = ()>` (works with a
`oneshot`, a `CancellationToken`'s `cancelled()`, or any future) rather than a
concrete type, to avoid coupling callers to a specific signalling primitive.
