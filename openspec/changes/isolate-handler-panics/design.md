## Context

`Worker::run` runs handler futures in a `FuturesUnordered` on one task. The
per-job lifecycle was just extracted into `worker/execution.rs` by
`bounded-long-handler-support`; the handler future is built there and either
awaited directly (no timeout) or raced against a heartbeat/timeout. Nothing
catches a panic: if `handler.run(..)` panics, the unwind propagates through
`in_flight.next().await` out of `run`, killing the worker task. Sibling
in-flight futures are dropped unresolved, and the panicking job — never acked,
retried, or failed — stays reserved until lease expiry, then redelivers and
panics again (a poison loop).

This change is worker-only: no `worklane-core` or `Broker` trait change.

## Goals / Non-Goals

**Goals:**
- A handler panic is contained and routed through the existing failure path
  (retry / dead-letter), exactly like a returned `Err`.
- A panic in one job never crashes the worker or abandons sibling in-flight jobs.
- Always-on, no configuration.

**Non-Goals:**
- Catching aborts under `panic = "abort"` (impossible by construction; documented).
- Catching panics outside the handler (e.g. inside broker calls) — a broker that
  panics is a broker bug, out of scope.
- A distinct `Error` variant for panics (reuse `Error::Handler`, per the
  precedent set by the timeout error in `bounded-long-handler-support`).

## Decisions

### Decision 1: catch at the handler future, via `catch_unwind` + `AssertUnwindSafe`

Wrap the dispatch future where it is built in `execution.rs`:

```rust
use futures_util::FutureExt;            // catch_unwind, map
use std::panic::AssertUnwindSafe;

let handler = AssertUnwindSafe(dispatch.dispatch(ctx, &envelope.payload))
    .catch_unwind()
    .map(|r| match r {
        Ok(res) => res,                              // handler returned (Ok/Err)
        Err(panic) => Err(Error::Handler(panic_message(panic))),
    });
```

`catch_unwind` turns the output from `Result<()>` into `Result<Result<()>,
Box<dyn Any + Send>>`; the `map` flattens it back to `Result<()>`, so both the
no-timeout path (`handler.await`) and `run_bounded` consume it unchanged. A
caught panic becomes `Err(Error::Handler(..))`, which `handle_failure` already
routes to retry/dead-letter.

`AssertUnwindSafe` is required because async futures are generally not
`UnwindSafe`. It is sound here: on a caught panic the job is discarded (failed
or retried) and we never observe the handler's possibly-inconsistent state, so
no broken invariant can leak forward.

### Decision 2: reuse `Error::Handler` with a best-effort panic message

`panic_message` downcasts the panic payload to `&str` then `String`, falling
back to a generic label:

```rust
fn panic_message(p: Box<dyn Any + Send>) -> String {
    let msg = p.downcast_ref::<&str>().map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "handler panicked".to_string());
    format!("handler panicked: {msg}")
}
```

No new `Error` variant: nothing branches on "panic vs error", and the failure
path is identical. Additive later if a consumer ever needs to distinguish.

### Decision 3: always-on, per-job

Containment is unconditional — a handler panic must never crash the worker.
Because it lives inside the per-job unit, each job's panic is isolated to that
job; the `FuturesUnordered` siblings and the `run` loop are untouched. This is
the payoff of the `execution.rs` seam: the change is a single wrap at the
handler-future site.

## Risks / Trade-offs

- [`panic = "abort"` defeats `catch_unwind`] → Out of scope and documented; under
  abort the process exits, which no library code can intercept.
- [`AssertUnwindSafe` could mask a genuinely unsafe-to-continue state] →
  Mitigated by discarding the job on panic; we never resume using the handler's
  state, and the worker continues only with fresh reservations.
- [A panic message may be a non-string payload] → `panic_message` falls back to a
  generic label; the job is still dead-lettered/retried correctly.

## Migration Plan

Purely additive and always-on; no API or config change. Existing behavior for
handlers that return `Err` is unchanged. Depends on the `worker/execution.rs`
seam from `bounded-long-handler-support` (this change stacks on it).

## Open Questions

None outstanding.
