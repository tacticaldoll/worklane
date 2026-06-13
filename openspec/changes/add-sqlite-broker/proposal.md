## Why

The broker contract is fully specified (`openspec/specs/broker`) and executable
as the `worklane-test` conformance suite, but it has only ever been validated
against `InMemoryBroker` — an implementation that shares the contract's own
in-process, single-`Vec` assumptions. Per the **Broker design gate**, the
`Broker` trait must not be treated as stable until a *durable* backend with a
different storage paradigm (rows, SQL, transactions) passes the suite **without
changing the trait**. This change is that decoupling milestone (Near-term
sequencing step 3): the first real proof that the contract is portable, not
accidentally shaped for memory.

## What Changes

- Add a new `worklane-sqlite` crate implementing the existing `Broker` trait on
  top of SQLite (via `rusqlite`, `bundled` feature — no system `libsqlite3`).
- Persist jobs as a serialized `JobEnvelope` blob plus a few denormalized index
  columns (`lane`, `available_at`, `leased_until`, `receipt`); `reserve` is a
  single atomic `UPDATE … RETURNING`. This reuses the envelope's existing serde
  "on-the-wire" form and requires **zero changes to `worklane-core` code** — the
  `Broker` trait and every public type stay one line unchanged (the decoupling
  tripwire; a forced trait change would mean the contract is wrong, not the impl).
- Close a real gap a durable backend forces open: the `broker` spec's
  *Backend-agnostic payloads* requirement forbids the broker from *reading* the
  payload but never requires it to *preserve* it. **MODIFY** that requirement so
  every envelope field — including the opaque `payload` bytes — is returned
  unchanged across a storage round-trip, and add one shared-suite scenario both
  brokers pass (in-memory trivially; SQLite via serde round-trip). The `Broker`
  trait is unaffected.
- Provide `BrokerContractHarness` + `TimedBrokerContractHarness` glue and run
  `broker_contract_required!` and `broker_contract_timed!` against fresh
  in-memory SQLite databases (perfect per-scenario isolation, no temp files).
- Derive all time decisions from an injected `Clock` (the spec's *Injectable
  time source* requirement), so the timed tier advances deterministically.
- Wire the crate into the workspace (`workspace.dependencies`) alongside
  `worklane-memory`.

Deliberately **not** in scope (recorded, not built — *Least commitment*):
`next_available_at` precise wakeup and lease extension (both would add a trait
method → would trip the tripwire); a connection pool (a step-4 concurrency
concern); a columnar schema + a `JobEnvelope::from_stored` constructor (its
first real consumer is the future Postgres broker); restart-durable wall-clock
epoch handling.

## Capabilities

### New Capabilities

<!-- None. A second Broker implementation conforms to the existing `broker`
     contract; it introduces no new backend-agnostic capability of its own. -->

### Modified Capabilities

- `broker`: **MODIFY** the existing *Backend-agnostic payloads* requirement to
  close a gap it leaves open — it forbids the broker from *reading* the payload
  but never requires it to *preserve* it. Add the obligation that every envelope
  field, including the opaque `payload` bytes, is returned unchanged across a
  storage round-trip. Trivially true for in-memory; a genuine obligation for the
  first serializing backend. The `Broker` trait is unaffected.

<!-- Deliberately NOT folded in (recorded in BACKLOG instead): `reserve` is
     silent on which of several visible same-lane jobs is returned. In-memory
     picks FIFO via Vec order; SQLite must write an explicit ORDER BY. Speccing
     strict FIFO now would pre-commit the contract against the backlogged
     priority-queue feature, so per Least commitment it stays a recorded
     observation, not a requirement.

     Likewise the process rule this milestone surfaces ("a durable backend
     validates the trait; broker impls are verified by the suite") is
     governance, not behaviour, so it goes to AGENTS.md — consistent with how
     `establish-broker-contract` deferred its conformance-as-governance rule. -->

## Impact

- **New crate:** `crates/worklane-sqlite/` (`worklane-sqlite`), depending on
  `worklane-core` + `async-trait` + `rusqlite` (+ `serde_json`, `tokio` for
  `spawn_blocking`); dev-depending on `worklane-test` + `tokio`.
- **New dependency:** `rusqlite` (with `bundled`) enters the workspace; it
  compiles SQLite from source (adds a C build step to CI).
- **Workspace manifest:** `worklane-sqlite` added to `[workspace.dependencies]`.
- **`worklane-core`:** code unchanged — the explicit, verifiable success
  criterion (the `Broker` trait and every public type one line unchanged).
- **`worklane-test`:** one new required-tier scenario (envelope fidelity) added
  to `broker_contract_required!`; both existing brokers must keep passing.
- **`openspec/specs/broker`:** one MODIFIED requirement (Backend-agnostic
  payloads, gaining the preservation obligation) at sync time; nothing added or
  removed.
- **Governance:** an `AGENTS.md` note recording that the `Broker` trait has now
  been validated against a durable backend without change.
- **Docs/backlog:** record the deferred `from_stored`/columnar schema and the
  restart-epoch boundary as future items.
