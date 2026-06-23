## Context

`worklane` 0.1.0 proves the same lifecycle behavior across the in-memory,
SQLite, PostgreSQL, and Redis brokers with `worklane-test`. The public broker
surface still mixes mandatory lifecycle operations with optional operational
capabilities, and the conformance suite is easier for first-party crates to
consume than for external broker authors to understand.

This change is the 0.2.0 extension foundation: keep the lifecycle contract
small, split optional capabilities into explicit traits, and document how a
broker author proves compatibility. It intentionally uses the existing
first-party brokers as the validation set rather than adding a new durable
backend.

## Goals / Non-Goals

**Goals:**

- Define a minimal lifecycle broker contract for enqueue, reserve, resolve,
  lease extension, classification, and defer semantics.
- Move optional behavior behind explicit capability traits and modular
  conformance suites.
- Make `worklane_core::spi` the documented broker-author helper surface.
- Make `worklane-test` consumable by private and third-party broker authors.
- Publish lifecycle semantics, custom broker conformance, and conformance
  matrix documentation.
- Preserve all shipped lifecycle semantics across first-party brokers.

**Non-Goals:**

- Add NATS, SQS, MySQL, AMQP, or another durable backend.
- Add a dashboard or web UI.
- Add rate limiting, workflow/saga primitives, or exactly-once execution.
- Change job lifecycle semantics merely to fit an implementation convenience.
- Require every broker to support every optional capability.

## Decisions

### Split lifecycle core from optional capabilities

The shared broker core will contain only operations every backend must honor for
the lifecycle loop: enqueue, reserve, ack, retry, defer, extend, fail, and
classify. Optional surfaces such as batch enqueue, dead-letter inspection,
queue-depth stats, scheduled enqueue, and result storage will live behind
separate capability traits.

This is a breaking pre-1.0 API change, but it reduces the long-term contract to
the minimum every backend can implement uniformly. SQL implementations can map
the core to transactional row updates with receipt checks; Redis can map the
same core to atomic scripts; in-memory can implement the same semantics without
leaking in-memory-only conveniences.

Alternatives considered:

- Keep the current unified `Broker` trait. Rejected because every third-party
  implementation would inherit optional methods and conformance expectations
  that may not match its storage.
- Add another higher-level facade over the current trait. Rejected because it
  would preserve the oversized implementor contract underneath.
- Make optional operations best-effort methods on the core trait. Rejected
  because best-effort methods blur portability claims.

### Make conformance modular by capability

`worklane-test` will expose a mandatory lifecycle suite and optional suites for
capabilities. A broker that only implements the lifecycle core can prove that
core; a broker that also implements dead-letter inspection, scheduled enqueue,
stats, batch enqueue, or result storage can opt into those suites.

The suite must report skipped optional capability suites explicitly through the
author's test wiring rather than silently passing. This keeps compatibility
claims precise: "passes lifecycle core" is different from "passes lifecycle
core plus scheduled enqueue and dead-letter inspection."

Alternatives considered:

- Keep one required monolithic suite. Rejected because it forces optional
  capabilities onto every broker and makes partial, honest compatibility hard.
- Let broker authors hand-pick individual scenarios. Rejected because it makes
  compatibility claims non-comparable.

### Treat `worklane_core::spi` as broker-author API

The SPI remains outside the `worklane` facade and is documented as an extension
surface for broker authors. Shared helpers such as envelope encoding, receipt
key encoding, clock duration conversion, stale receipt errors, redaction, and
lane name validation belong here when they encode decisions every backend must
share.

Implementation-specific conveniences stay in the backend crates. A helper is
eligible for SPI only when at least two backend implementations need the same
decision, or when divergence would break the storage or conformance contract.

Alternatives considered:

- Move all backend helpers into SPI immediately. Rejected because it would
  promote unproven implementation details.
- Keep SPI undocumented. Rejected because external broker authors would copy
  first-party internals and drift.

### Document the lifecycle instead of introducing new behavior

The lifecycle semantics guide and conformance matrix are documentation outputs,
not new runtime behavior. They summarize the existing OpenSpec requirements and
link to them as the source of truth. The custom broker conformance guide explains
test wiring and compatibility claims without weakening the specs.

Alternatives considered:

- Put all lifecycle explanation only in README. Rejected because the README
  should stay concise.
- Create a second normative contract in prose docs. Rejected because OpenSpec
  remains the behavioral source of truth.

## Risks / Trade-offs

- **Breaking API churn** -> Keep the change pre-1.0, document migration notes,
  and update every first-party broker in the same change.
- **Capability split creates too many tiny traits** -> Split only along existing
  optional behavior with real consumers and conformance scenarios.
- **Conformance claims become confusing** -> Publish a matrix that separates
  lifecycle core from each optional capability.
- **SPI becomes a dumping ground** -> Require each new SPI helper to encode a
  shared backend decision or storage-contract invariant.
- **Docs drift from specs** -> Lifecycle guides link back to OpenSpec and avoid
  restating behavior as an independent contract.

## Migration Plan

1. Introduce the new core lifecycle trait and capability traits in
   `worklane-core`.
2. Update first-party broker implementations to implement the split traits.
3. Update `worklane-test` harnesses and macros to run mandatory and optional
   suites separately.
4. Update `worklane`, `worklane-scheduler`, `worklane-cli`, and examples to use
   the split traits or capability accessors.
5. Add migration notes for direct `Broker` implementers and custom broker
   authors.
6. Add lifecycle semantics, custom broker conformance, and conformance matrix
   documentation.
7. Run the full Definition of Done, including the first-party conformance suites.

Rollback is source-level: revert the split before publishing 0.2.0. After
publication, restore compatibility only through additive adapters or a documented
follow-up breaking release.

## Open Questions

- Exact trait names are implementation-level and may be finalized during apply,
  but the capability boundaries are fixed by the delta specs before apply.
- The result-store suite already exists separately; apply should decide whether
  the conformance matrix presents it under broker capabilities or adjacent
  storage capabilities.
