## Why

Worklane's per-kind worker circuit breaker contains a pure sans-I/O state machine
(closed/open/half-open, admit-or-defer, record an outcome) inside its own hand-rolled
`BreakerState` enum, entangled with an ambient `Instant::now()` read and its own
`Mutex<HashMap<String, BreakerState>>` bookkeeping. Sigorta 0.1.1 (published to
crates.io, evidenced by exactly this mechanism) now owns that state machine as a pure,
`Instant`-explicit core, including the stale-probe bound this breaker's own `HalfOpen`
already required. Worklane can now adopt the published facade and retire the
hand-rolled duplicate.

## What Changes

- Add `sigorta = "0.1.1"` as a crates.io workspace dependency, consumed only from the
  `worklane` facade.
- Rewrite `CircuitBreaker`'s internals to hold a `Mutex<HashMap<String, sigorta::Sigorta>>`
  instead of `Mutex<HashMap<String, BreakerState>>`, delegating the closed/open/half-open
  transition rules to `sigorta::Sigorta::admit`/`record`.
- Preserve `CircuitBreakerPolicy`, `CircuitBreaker::new`, `admit(&self, kind: &str) ->
  Option<Duration>`, and `record(&self, kind: &str, success: bool)` exactly — no public
  API change, no call-site change in `execution.rs`.
- Keep the ambient clock read, the per-kind keying, and the `Mutex` in Worklane's own
  wrapper — Sigorta's core takes `now: Instant` explicitly and owns exactly one
  breaker's state; multi-instance keying and the clock are consumer concerns by design.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. The circuit breaker is private worker implementation with no formal capability
spec today (confirmed: no `circuit`/`breaker` mention anywhere in `openspec/specs/`).
This change preserves its existing observable behavior; it does not introduce a new
documented capability for a mechanical internal swap.

## Impact

The change touches the workspace dependency table, the `worklane` facade manifest,
`crates/worklane/src/worker/circuit_breaker.rs`, and `Cargo.lock`. It adds no
workspace dependency edge beyond the facade's existing `worklane-core` edge. It
changes no public API, `Broker`/`ResultStore` trait, backend, MSRV, or Tianheng law.
