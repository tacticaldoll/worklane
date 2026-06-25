## Context

The shipped `cross-broker-decision-dedup` change lifted four *internal* shared
decisions into `worklane-core` but explicitly deferred `DEFAULT_LEASE` because it
differs in kind: it is a value each backend crate **exports publicly**, so users
may reference `worklane_sqlite::DEFAULT_LEASE` (e.g. `with_lease(DEFAULT_LEASE * 2)`).
The five copies today: `worklane-memory`, `worklane-sqlite`, `worklane-postgres`,
`worklane-redis` (each `pub const DEFAULT_LEASE: Duration = Duration::from_secs(30)`)
and `worklane-test`'s `BrokerConfig::DEFAULT_LEASE`.

## Goals / Non-Goals

**Goals:**

- One source of truth for the default visibility-timeout value.
- Strictly non-breaking: every existing public path still resolves and the value
  stays `30s`.
- The test harness default is single-sourced from the broker default.

**Non-Goals:**

- No `Broker` trait / core job-trait / `JobEnvelope` / schema / wire change (the
  *Broker design gate* is not engaged).
- No facade (`worklane`) re-export — no present consumer needs a
  `worklane::DEFAULT_LEASE` (*least commitment*).
- No behaviour change and no spec delta.

## Decisions

### D1 — Canonical home: `worklane_core::spi::DEFAULT_LEASE`

Define the value once in `worklane_core::spi`, beside the other lifted shared
backend defaults from `cross-broker-decision-dedup` (`MAX_DEAD_LETTER_SWEEP`,
`SCHEMA_VERSION`). The default lease is a *shared backend decision* — every backend
defaults its visibility timeout to the same `30s` — which is exactly what `spi`
holds, and the `broker-extensibility` "Broker author SPI" requirement scopes `spi`
as "shared backend decisions". It serves all backends (not one), so the spec's
"backend-local helper is not promoted" carve-out does not apply.

*Why not the core root on a "user-facing" rationale:* an earlier draft placed it at
the root arguing it is user-facing. Review found **no user-facing consumer** — the
facade, the contract tests, and the examples never reference `DEFAULT_LEASE`; its
only readers are the backend constructors and the test harness. Absent a real
consumer, *least commitment* and consistency with the immediately-preceding
precedent both point to `spi`, not a speculative user-facing placement.

Backends re-export it at their own crate root, so `worklane_sqlite::DEFAULT_LEASE`
keeps resolving. This does not violate the spec's "SPI SHALL NOT be re-exported from
the `worklane` facade" rule — that rule is about the facade, not backend crates
(which already expose this symbol today).

*Alternatives rejected:* core root (no user-facing consumer justifies it; diverges
from the `spi` precedent); a facade-level `worklane::DEFAULT_LEASE` (no consumer —
*least commitment*); leaving it per-backend (the drift this change removes).

### D2 — Backends re-export via `pub use`, not a redefined const

Each backend replaces its `pub const DEFAULT_LEASE` with
`pub use worklane_core::spi::DEFAULT_LEASE;`. This keeps the existing public path
(`worklane_sqlite::DEFAULT_LEASE`, etc.) resolving — so it is non-breaking — while
making it literally *the* core item, not a copy. Internal uses (`lease: DEFAULT_LEASE`
in each `connect`) still resolve through the in-scope name.

*Alternatives rejected:* a redefined
`pub const DEFAULT_LEASE: Duration = worklane_core::spi::DEFAULT_LEASE;` (works and is
non-breaking, but re-declares a const whose only value is the core one — a `pub use`
is the more honest single source); removing the backend symbol entirely (breaking —
existing `worklane_<backend>::DEFAULT_LEASE` paths would stop resolving).

### D3 — All test-side lease defaults reference the core value

Two test-side copies must also be redirected so the change actually leaves *one*
source:

- `worklane-test`'s `BrokerConfig::DEFAULT_LEASE` is an associated const, so it
  cannot be a `pub use`; initialize it from the core value:
  `pub const DEFAULT_LEASE: Duration = worklane_core::spi::DEFAULT_LEASE;`.
- Each backend's contract test (`crates/worklane-*/tests/broker_contract*.rs`)
  defines `const TEST_LEASE: Duration = Duration::from_secs(30);` — five more copies
  of the default lease that drive the timed conformance tier (the regression gate
  this change leans on). Point each at `worklane_core::spi::DEFAULT_LEASE`.

Review caught that the original tasks missed the `TEST_LEASE` copies, which also
made the Definition-of-Done grep ("survives in exactly one place") self-
contradictory. Both are fixed here. Unrelated `Duration::from_secs(30)` values that
are *not* the default lease (the worker circuit-breaker window; a bounded-handler
test) stay as-is.

## Risks / Trade-offs

- **A `pub use` changes how the symbol is documented/linked.** → Re-exports are
  fully supported and documented by rustdoc; the doc gate (`-D missing_docs`) is
  satisfied by the core definition's doc comment. Low risk.
- **A downstream user pattern-matches the symbol's defining crate.** → Extremely
  unlikely for a `Duration` const; the path and the value are both preserved, which
  is all a caller can observe.
- **Governance edge.** → Backends already depend on `worklane-core`, so the `pub use`
  adds no new dependency edge; the `worklane-governance` check stays green
  (verified in tasks).

## Migration Plan

None — behaviour-preserving, value unchanged, all paths preserved. Rollback is a
plain revert. Verification is the existing `worklane-test` lease/poison/timed
conformance plus the full Definition of Done.
