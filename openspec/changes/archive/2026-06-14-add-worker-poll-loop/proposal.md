## Why

The worker can process jobs one at a time (`process_next`) or drain currently
visible jobs and stop (`run_until_idle`), but it cannot run as a long-lived
service. `run_until_idle` returns the moment no job is *currently* visible, so a
job scheduled for the future (a pending retry) is never picked up — a liveness
gap the code already flags as a planned follow-up. To use worklane as a daemon,
the worker needs a long-running loop that keeps polling and shuts down cleanly.

## What Changes

- Add `Worker::run(shutdown)` — a long-running loop layered on `process_next`:
  drain currently visible jobs, then wait a poll interval when idle, repeating
  until a shutdown signal. `process_next` and `run_until_idle` are retained; the
  three coexist as layers (one job / drain-then-stop / daemon).
- The worker waits when idle via `tokio::time::sleep`. The core `Clock` stays
  `now()`-only — waiting is a runtime concern, not part of the clock contract —
  and the worker does not depend on `Clock` at all.
- Idle wait is a **fixed interval**, configurable via `Worker::with_poll_interval`.
- **Cooperative graceful shutdown**: the shutdown signal is checked *between*
  jobs; an in-flight job always runs to completion and resolves (ack/retry/fail)
  before `run` returns. Hard-cancelling (dropping the `run` future) instead lets
  the in-flight job re-run later via at-least-once delivery.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `worker`: adds a `Long-running poll loop` requirement (a `run` that drains, then
  waits a poll interval when idle, until shutdown) and a `Cooperative shutdown`
  requirement (an in-flight job resolves before `run` returns).

## Impact

- `worklane` (facade): `Worker` gains `run(shutdown)` and `with_poll_interval`;
  needs tokio's `time` feature (already enabled in the workspace tokio dep).
- `worklane-core` / `worklane-memory`: **unchanged** — `Clock` is untouched, no
  `Broker` trait change.
- Deliberately deferred (Non-Goals): worker concurrency, `next_available_at`
  precise wakeup (a `Broker` trait change), capped backoff, OS-signal wiring,
  lease extension, and adding `sleep` to the `Clock` trait.
