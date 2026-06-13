## Context

The broker contract (enqueue, reserve, visibility lease, receipt validation,
retry, fail, dead-letter, lane isolation) is fully specified in
`openspec/specs/broker`, but its only executable form is a set of broker-level
tests living in `crates/worklane/tests/core_loop.rs` and `lane_partitioning.rs`
— in the *facade* crate, coupled to `InMemoryBroker`, and not reusable by a
future broker. The `Clock` seam those tests rely on (`ManualClock`) is private to
`worklane-memory`.

This change makes the contract executable and implementation-agnostic, and uses
that as the forcing function for a clean structural sort. It is governed by the
`AGENTS.md` design principles — *Minimal contracts*, *Least commitment*, and the
*Broker design gate* — and the `Broker` trait does not change.

## Goals / Non-Goals

**Goals:**
- A reusable, publishable conformance suite any `Broker` impl can run to prove it
  satisfies the broker contract.
- A clean sort: contract → lifted to `worklane-core`; implementation conveniences
  → kept behind a per-implementation adapter.
- Deterministic time-based scenarios via an injectable clock, without an
  in-memory-only assumption leaking into the contract.
- Fix the layering smell: broker scenarios live with the broker, not the facade.

**Non-Goals:**
- No new Redis/Postgres broker.
- No change to the `Broker` trait.
- No lease extension/renewal.
- No worker concurrency.
- No new or changed worker poll loop.
- Do NOT require every broker to provide dead-letter listing or a manual clock.
- Do NOT promote `InMemoryBroker` convenience details (`len`, `dead_letters`)
  into the public broker contract.

## Decisions

### 1. The change is a two-directional sort (lift / sink)

Everything currently lumped into `worklane-memory` is sorted by *kind*:

```
            worklane-memory (contract + convenience + private clock, mixed)
                              |
        ┌─────────────────────┴─────────────────────┐
     LIFT ↑ (shared contract → worklane-core)   SINK ↓ (impl/adapter)
   - Clock trait + SystemClock                  - ManualClock (test-only) → worklane-test
   - broker semantics → executable suite        - len() / dead_letters() / advance-time
                                                   → behind BrokerContractHarness (per-impl glue)
```

The suite proves the sort is correct; the sort (a minimal lifted contract plus
conveniences kept off the trait) is the value realised now.

### 2. Lift `Clock` to core — `now()` only, no async sleep

A reusable suite must drive each broker's notion of time, so the `Clock` trait
must be a shared contract: lift it (and the production `SystemClock`) into
`worklane-core`. Move only `now()`; an awaitable `sleep` has no consumer until
the poll loop, so per *Least commitment* it is not added here.

*SQL gate:* a durable broker computes visibility/lease from an injected `$now`
(`leased_until = $now + lease`), so an injectable clock is portable and even
avoids app/DB clock skew.

### 3. Sink `ManualClock` to `worklane-test`, not core

`ManualClock` is a test-control capability, not a production contract. Putting it
in core would ship test scaffolding in the core public surface. It lives in
`worklane-test` and implements core's `Clock`; brokers accept it through the
existing `with_clock(Arc<dyn Clock>)` path.

### 4. The suite asserts only what is observable; conveniences go behind an adapter

`len()` / `dead_letters()` are not on the `Broker` trait and must not be, so the
suite cannot use them directly. It observes a broker through the `Broker` trait
plus a small per-implementation adapter:

```rust
trait BrokerContractHarness {
    type Broker: Broker;
    async fn fresh_broker(&self) -> Arc<Self::Broker>;            // clean state per scenario
    async fn dead_letters(&self, b: &Self::Broker)
        -> Option<Vec<DeadLetter>>;                                // None = capability absent
}
```

`fresh_broker` is per-scenario isolation (trivial for in-memory; a clean
schema/table for a durable broker — the real glue point). Dead-letter inspection
is capability-gated via `Option`; when absent the relevant assertion is **visibly
skipped** (`eprintln!`), never silently green. `live_len` is dropped (YAGNI — the
first scenarios verify removal via `reserve` returning `None`).

### 5. Required vs Timed tiers, split by two macros at the call site

Two macros so capability is declared where the broker is wired, avoiding a
misleading green for skipped time scenarios:

- `broker_contract_required!{ harness }` — every broker; time-free.
- `broker_contract_timed!{ harness }` — only brokers that can advance time.

The boundary uses `delay = 0` as the time-free probe: `retry(receipt, ZERO)`
verifies `attempts + 1` and immediate re-reservability (required); `retry(>0)`
verifies hidden-before / visible-after (timed). The timed harness variant adds
`advance_time(delta)`.

### 6. Relocate broker scenarios; keep integration in the facade

Pure-broker scenarios move from `crates/worklane/tests` into
`worklane-memory`'s tests (invoking the suite). Client/Worker integration tests
(happy path, retry via worker, unknown kind, stale-resolution non-fatal,
duplicate registration, payload round-trip) stay in the facade.

### 7. One spec delta (injectable time source); conformance rule deferred to governance

The single behavioural-contract change is that a broker derives time from an
injectable clock — the contract counterpart of lifting `Clock` into core. It is
added to `openspec/specs/broker` as the `Injectable time source` requirement, and
the suite's timed tier is its regression test. No observable runtime behaviour and
no other requirement changes.

The developer-facing "broker implementations SHALL be verifiable by the suite"
rule is *process*, not behaviour, and is deferred to a separate `AGENTS.md` edit
once the suite exists (*Separate knowledge by its kind*) — it is deliberately not
a spec delta.

## Risks / Trade-offs

- **New crate adds workspace ceremony** → Justified: `worklane-test` has a sharp
  boundary (conformance kit) and must be a separate, publishable crate so other
  broker crates can dev-depend on it without a dependency cycle (`worklane-test →
  worklane-core`; brokers `dev → worklane-test`).
- **Suite/spec drift** → Each test is named after its broker-spec scenario;
  keep them aligned at sync time. The suite is the executable mirror of the spec.
- **Capability-gated skips hide coverage** → Mitigated by the two-macro split
  (call-site opt-in) and explicit skip notices; no silent green.
- **Refactor creep** → Bounded to the three moves the suite needs (lift clock,
  adapter sort, relocate tests). `RetryPolicy`'s mis-placement in core is recorded
  as a future sink candidate, not done here (*Least commitment*).

## Migration Plan

Pre-release, no persisted state. Order: (1) lift `Clock`/`SystemClock` to core and
update `worklane-memory` imports; (2) add `worklane-test` with `ManualClock`,
harness, and the two macros; (3) port the broker scenarios into
`worklane-memory`'s tests via the suite and trim the facade tests. Existing tests
guard behaviour throughout; the suite becomes the permanent guard. Rollback =
revert the commits.

## Open Questions

None blocking. The conformance-as-governance rule and the lift/sink pattern's
promotion to a named `AGENTS.md` principle are deliberately deferred until this
change validates them (the `Pending` marker already in `AGENTS.md`).
