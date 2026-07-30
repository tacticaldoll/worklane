## Context

`FanInWatcherJob::run` currently performs two responsibilities in one async
loop:

- Worklane observes durable job state, reads result bytes, persists checkpoint
  state, schedules polling, reports domain errors, and enqueues the callback.
- An in-memory decision tracks fixed ordered requirements, captures values
  monotonically, and selects pending, ready, or impossible.

Lengkap 0.1.0 implements only the second responsibility. A local Worklane spike
proved behavioral fit against Lengkap commit
`f4e42ea4dc964174f1934c13a14969146140d1dd`; both product crates are now
published with Worklane's Rust 1.85 MSRV.

## Goals / Non-Goals

**Goals:**

- adopt the published Lengkap facade through the existing workspace dependency
  convention;
- preserve fan-in lifecycle behavior, result ordering, wire shape, and public
  Worklane API;
- keep every observation, persistence operation, timer, and reaction in
  Worklane;
- preserve the prior private checkpoint tuple ordering in addition to slot
  identity;
- promote the spike evidence through normal OpenSpec and integration
  governance.

**Non-Goals:**

- change callback delivery, timeout, result retention, or malformed-payload
  semantics;
- add Worklane vocabulary, serialization, async, storage, or I/O to Lengkap;
- change `Broker`, `ResultStore`, any backend, or the Tianheng constitution;
- expose Lengkap types through Worklane's public API;
- add another completion mode or generic workflow abstraction.

## Decisions

### Depend on the published facade

Declare `lengkap = "0.1.0"` in `[workspace.dependencies]` and consume it only
from the `worklane` facade. The facade is Lengkap's curated entrypoint and was
verified from crates.io on Rust 1.85. Depending directly on
`lengkap-contract` was rejected because application consumers should validate
the intended public surface. A path dependency was rejected now that the
artifact is published because it would make Worklane packaging non-portable.

This adds an external dependency but no new workspace edge. The active
Tianheng facade boundary continues to observe only `worklane-core` among
workspace dependencies.

### Adapt the existing checkpoint without changing its wire shape

Restore a Lengkap `Assembly<Vec<u8>>` from the watcher's existing
`dependencies` and `collected` fields. Slot index is dependency order.
`Assembly::into_slots` maps back to the same `(JobId, Vec<u8>)` payload shape;
Lengkap owns neither serialization nor durable checkpoint representation.

When checkpointing a pending assembly, retain previously captured tuples in
their prior order and append newly captured values in dependency order. A
canonical slot-order rewrite was rejected because it would introduce an
unnecessary observable change to the private serialized payload across
generations.

### Translate observations into pure findings

Only unresolved slots are observed:

| Worklane observation | Lengkap input |
| --- | --- |
| `Live` | no finding |
| `CompletedOrUnknown` plus result bytes | `Produced(bytes)` |
| `CompletedOrUnknown` without bytes | `Impossible(MissingResult(id))` |
| `DeadLettered` | `Impossible(DeadLettered(id))` |

Worklane reacts to `Pending` by enforcing the generation bound and enqueueing
the next watcher, to `Ready` by constructing and enqueueing the callback, and
to `Impossible` by producing the existing domain-specific error. Lengkap
structural errors become internal fan-in inconsistency errors because payload
validation and adapter slot construction make them unreachable.

Keeping the manual mutable result vector was rejected because it continues to
mix completion policy into the I/O loop and leaves the pure governance boundary
implicit.

### Reuse the conformance evidence

The existing watcher integration suite remains the primary behavioral
equivalence proof. A focused unit test verifies checkpoint restoration,
preservation of prior tuple order, and dependency-ordered ready output. The
formal branch additionally proves Cargo resolves both Lengkap crates from
crates.io rather than an adjacent path.

## Risks / Trade-offs

- **A dependency update changes the decision contract.** → Keep the initial
  dependency on the compatible 0.1 line and review future lockfile updates
  against the workflow scenarios.
- **Checkpoint conversion changes ordering.** → Derive slots only from
  dependency order, preserve existing tuple order, and test partial round trips
  plus ready output order.
- **Multiple impossible observations select a different failure.** → Lengkap
  selects the lowest slot, matching the watcher's prior dependency-order scan.
- **Produced bytes are read before another slot proves impossible.** → No
  callback or next generation is persisted after an impossible decision,
  preserving externally observable behavior.
- **Tianheng does not govern external dependencies or private runtime
  behavior.** → Keep those effects explicit in OpenSpec and verify the existing
  facade boundary after implementation.

## Migration Plan

Recreate the proven source and tests from the local spike on a fresh branch
from `main`, replacing only its path dependency with the crates.io version.
Sync and archive this formal change, run the complete local gates, open a PR,
wait for CI, and squash-merge to `main`.

Rollback before merge is a branch deletion. After merge, revert the adoption in
a normal reviewed change; no durable payload migration is required because the
watcher payload and result ordering do not change.

## Open Questions

None.
