## Why

Worklane's fan-in watcher contains a pure fixed all-of completion decision
inside its async broker loop. The local spike proved that published Lengkap
0.1.0 can own that decision without changing lifecycle behavior, so Worklane
can now adopt the crates.io facade and make the governance boundary real.

## What Changes

- Add `lengkap = "0.1.0"` as a crates.io workspace dependency used by the
  `worklane` facade.
- Restore existing watcher checkpoints into a Lengkap assembly.
- Map broker and result-store observations into Lengkap findings.
- Keep checkpoint persistence, polling, failure reactions, and callback
  enqueueing in Worklane.
- Preserve the public API, serialized watcher payload, dependency ordering, and
  prior private checkpoint tuple ordering.
- Promote the verified local spike through the complete OpenSpec, CI, PR, and
  squash-merge flow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `workflow`: Require the fan-in completion decision to use a sans-I/O all-of
  boundary while preserving Worklane's existing lifecycle semantics.

## Impact

The change touches the workspace dependency table, the `worklane` facade
manifest, private fan-in watcher implementation and tests, Cargo.lock, and
OpenSpec artifacts. It adds no workspace dependency edge beyond the facade's
existing `worklane-core` edge and changes no public API, broker or result-store
trait, serialized payload shape, backend, MSRV, or Tianheng law.
