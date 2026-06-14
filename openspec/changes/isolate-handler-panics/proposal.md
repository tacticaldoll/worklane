## Why

A job handler that **panics** (rather than returning `Err`) currently unwinds
out of `Worker::run`: it crashes the worker task, abandons every sibling
in-flight job (left unresolved, redelivered only later), and never dead-letters
the panicking job. A handler that panics deterministically therefore becomes a
poison loop — each redelivery crashes the worker again. The worker spec already
promises that an unknown kind "MUST NOT panic or stall the loop"; a panicking
handler breaks that same resilience for ordinary handler code.

## What Changes

- The worker SHALL catch a panic that unwinds out of a handler and treat it as a
  handler failure, routing the job through the existing failure path: retry
  while attempts remain, otherwise dead-letter with a panic error.
- A panic in one in-flight handler SHALL NOT crash the worker or abandon the
  other in-flight jobs; the worker keeps processing.
- This is always-on (no configuration): a handler panic must never take down the
  worker. It relies on the unwinding panic strategy (the default); under
  `panic = "abort"` the process aborts regardless, which is out of scope.

## Capabilities

### New Capabilities
<!-- None: this extends existing worker behavior. -->

### Modified Capabilities
- `worker`: add a **Handler panic isolation** requirement — a panicking handler
  is contained and dead-lettered/retried like any other failure, without
  crashing the worker or its sibling jobs.

## Impact

- `worklane`: the per-job execution unit (`worker/execution.rs`) wraps the
  handler future in `catch_unwind` and maps a caught panic to
  `Error::Handler`, which the existing failure path already handles. No public
  API change.
- No `worklane-core` change and **no `Broker` trait change**: panic isolation is
  entirely worker-side.
- Tests: a panicking handler is dead-lettered (and retried below max attempts),
  and a panic in one job does not stop concurrent siblings or the worker.
