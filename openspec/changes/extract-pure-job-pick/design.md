## Context

`reserve`'s inline scan picks the highest-priority, earliest-available visible job
on a lane. It is pure (reads `&[StoredJob]`, `&Lane`, `Duration`; no I/O, no
mutation) but has no name and no direct test coverage — only exercised indirectly
through the full async `Broker::reserve` contract tests.

This is deliberately **not** treated as a new family-member extraction (unlike
`lengkap` or `sigorta`): the decision is stateless per call, and the SQL/Redis
backends already express the equivalent ordering natively (SQL `ORDER BY`, a Lua
script), so there is no second Rust implementation this would deduplicate. This is
a local clarity/testability refactor, not a reusable mechanism being distilled.

## Goals / Non-Goals

**Goals:** name the selection rule, make it directly unit-testable without the
`Mutex`/`Clock`/async machinery, with zero behavior change.

**Non-Goals:** extracting a new crate; touching the delivery-bound sweep, the
dead-letter side effect, or lease/receipt assignment; changing the public `Broker`
trait or any other backend.

## Decisions

### D1 — A private free function, not a new type

`pick_best` takes `&[StoredJob]` directly rather than introducing a `JobView`
wrapper type: `StoredJob` is already private to this crate, so there is no
encapsulation reason to add one, and doing so would be surface for a concern that
does not exist (no second caller needs a decoupled view type).

## Risks / Trade-offs

- **None of substance.** This is a same-module, zero-behavior-change refactor;
  the existing `Broker` contract test suite (`tests/broker_contract.rs`,
  `tests/configured_contract.rs`) already exercises `reserve` end-to-end and is the
  regression backstop.
