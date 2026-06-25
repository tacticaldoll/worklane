## Why

`DEFAULT_LEASE` (`Duration::from_secs(30)`) is duplicated as a `pub const` in all
four backends (`worklane-memory`, `worklane-sqlite`, `worklane-postgres`,
`worklane-redis`) and again as `BrokerConfig::DEFAULT_LEASE` in the
`worklane-test` harness. Nothing forces the five copies to agree, so a change to
one backend's default visibility timeout silently diverges from the others and
from the conformance harness — the same drift surface the shipped
`cross-broker-decision-dedup` change closed for internal decisions. It was
deliberately left out of that change because, unlike those internal decisions,
`DEFAULT_LEASE` is a *user-facing* value each backend exports publicly, so its
lift needs a non-breaking re-export decision (made here).

## What Changes

- Define the canonical default lease once as `worklane_core::spi::DEFAULT_LEASE`,
  beside the other lifted shared backend defaults (`MAX_DEAD_LETTER_SWEEP`,
  `SCHEMA_VERSION`) — the default lease is a shared *backend decision*, so it shares
  their home for consistency.
- Each backend re-exports it (`pub use worklane_core::spi::DEFAULT_LEASE;`) in place
  of its own `pub const`, so existing public paths like
  `worklane_sqlite::DEFAULT_LEASE` keep resolving (non-breaking) and the value is
  single-sourced. (Re-export from a
  backend crate root is fine; the broker-extensibility rule forbids only *facade*
  re-export of `spi`.)
- `worklane-test`'s `BrokerConfig::DEFAULT_LEASE`, and the per-backend contract-test
  `TEST_LEASE` constants (`crates/worklane-*/tests/broker_contract*.rs`), are
  initialized from the core const so no copy of the default lease survives.
- Behaviour-preserving: the value stays `30s`. The only API change is **additive**
  (a new `pub const` in `spi`); nothing is removed or renamed.

## Capabilities

### New Capabilities
<!-- None. No new observable capability; a shared user-facing default is
     single-sourced behind unchanged public paths. -->

### Modified Capabilities
<!-- None. No requirement changes: the default visibility timeout stays 30s and
     every existing public path still resolves. Internal refactor, no delta spec
     (precedent: cross-broker-decision-dedup, redis-hotpath-script-cache,
     postgres-enqueue-batch-unnest — all archived --skip-specs). The conformance
     suite (lease/poison/timed scenarios) proves behaviour is preserved. -->

## Impact

- **Code:** `worklane-core` gains `pub const spi::DEFAULT_LEASE`. The four backends
  replace their local `pub const` with a `pub use` re-export; `worklane-test`'s
  `BrokerConfig::DEFAULT_LEASE` and the per-backend `TEST_LEASE` contract-test
  constants reference the core const. No facade change (no present consumer needs a
  facade-level re-export).
- **API:** additive only — a new `spi` `pub const`; all existing
  `worklane_<backend>::DEFAULT_LEASE` paths continue to resolve. Non-breaking.
- **Schema / wire format / `Broker` trait:** unchanged.
- **Specs:** none (no requirement change; grep of `openspec/specs/` confirms no
  normative mention pins the 30s default). Archived `--skip-specs`.
- **Tests:** existing lease/poison/timed conformance scenarios are the regression
  gate; the `worklane-governance` boundary check must still pass (backends already
  depend on `worklane-core`, so no new dependency edge).
- **Out of scope:** unrelated 30s values that are not the default lease (e.g. the
  worker circuit-breaker window, a bounded-handler test) are left untouched.
