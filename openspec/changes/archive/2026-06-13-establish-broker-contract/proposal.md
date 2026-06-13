## Why

The broker spec defines what a correct broker does (enqueue, lease, receipt,
retry, fail, dead-letter, lane isolation), but that contract lives only as prose
plus a set of broker-level tests that happen to sit in the `worklane` facade
crate — coupled to `InMemoryBroker` and not reusable. Before a second (durable)
broker exists, we want the contract to be **executable and implementation-
agnostic**, so any broker can *prove* it conforms by running one shared suite.

The deeper value is structural, not the tests themselves: building a reusable
suite forces a clean sort of the broker code into **what is contract** (every
implementation must honour it → lift to the shared core) versus **what is
implementation convenience** (`len`, `dead_letters`, manual-clock control → keep
behind the implementation, exposed through a test adapter). Doing this sort now,
while there is one broker and few call sites, is the cheapest it will ever be and
removes a class of future breaking changes (per the *Minimal contracts* and
*Least commitment* design principles in `AGENTS.md`).

## What Changes

- **Lift** the `Clock` trait and the production `SystemClock` from
  `worklane-memory` into `worklane-core` (move `now()` only — no async sleep yet;
  it has no consumer until the poll loop). Brokers derive time from an injectable
  clock so time-based semantics are deterministic across implementations.
- **Add** a publishable `worklane-test` crate holding a reusable broker
  conformance suite: a `BrokerContractHarness` adapter, shared async test
  functions split into a **required** tier and a deterministic-**timed** tier
  (two macros), and `ManualClock` (test-only time control, **sunk** here rather
  than placed in core).
- **Sink** implementation conveniences out of the contract: the suite observes a
  broker only through the `Broker` trait plus a per-implementation harness
  adapter; `len()`/`dead_letters()`/manual time control are never required on the
  `Broker` trait. Dead-letter inspection is capability-gated and visibly skipped
  when absent.
- **Relocate** the pure-broker scenarios currently in `crates/worklane/tests`
  into `worklane-memory`'s tests via the suite; keep Client/Worker integration
  tests in the facade.
- The `Broker` trait is **unchanged**.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `broker`: adds an `Injectable time source` requirement — a broker SHALL derive
  time-based decisions (visibility, lease expiry, retry scheduling) from an
  injectable clock, making its lease/visibility semantics deterministic, portable,
  and verifiable by the shared suite. This is the contract counterpart of lifting
  the `Clock` seam into `worklane-core`; it changes no observable runtime
  behaviour and no other requirement.

The separate *process* rule "broker implementations SHALL be verifiable by the
shared suite" is intentionally NOT a spec delta — it is deferred to an `AGENTS.md`
edit once the suite exists (*Separate knowledge by its kind*).

## Impact

- **`worklane-core`**: gains `Clock` + `SystemClock` (moved from
  `worklane-memory`); public surface grows by the time abstraction only.
- **`worklane-memory`**: imports `Clock`/`SystemClock` from core; its broker
  tests are replaced by invoking the shared suite; dev-depends on `worklane-test`.
- **`worklane-test`** (new crate): the harness + suite; depends on
  `worklane-core` only; dev-dependency of broker crates; publishable so
  third-party brokers can self-certify.
- **`worklane`** (facade): keeps only Client/Worker integration tests.
- No runtime dependency changes; no `Broker` trait change; no behaviour change.
- Noted but **not** acted on (bounded scope): `RetryPolicy` lives in
  `worklane-core` though only the worker uses it — a future sink candidate,
  recorded, not done here.
