## 1. Dependency

- [ ] 1.1 Add `sigorta = "0.1.1"` to `[workspace.dependencies]` in the workspace
      root `Cargo.toml`.
- [ ] 1.2 Add `sigorta = { workspace = true }` to `crates/worklane/Cargo.toml`'s
      `[dependencies]`.

## 2. Implementation

- [ ] 2.1 Replace `BreakerState` and its `Default` impl with
      `sigorta::Sigorta` as the map's value type.
- [ ] 2.2 Rewrite `admit` to look up or insert a fresh `Sigorta` via the map's
      `entry` API, call `Sigorta::admit(now)`, and map `Admitted`/`Probing` to
      `None`, `Rejected { retry_after, .. }` to `Some(retry_after)`.
- [ ] 2.3 Rewrite `record` to look up or insert a fresh `Sigorta`, call
      `Sigorta::record(event, now)` with `event` mapped from `success: bool`,
      and store the result back.
- [ ] 2.4 Remove the now-dead `cooldown_end` helper (Sigorta computes this
      internally).
- [ ] 2.5 Update the module doc comment: it described `BreakerState` as
      Worklane's own sum type; it is now Sigorta's.

## 3. Verification

- [ ] 3.1 Run all five existing unit tests unmodified; all must pass with no
      changes to their assertions.
- [ ] 3.2 Run the full Definition of Done from the workspace root.
- [ ] 3.3 Confirm `cargo run -p worklane-governance -- check --manifest-path
      Cargo.toml` still passes (the facade boundary governs workspace
      dependencies only, unaffected by an external crate).

## 4. Sync and archive

- [ ] 4.1 No capability spec changes to sync (see `specs/README.md`).
- [ ] 4.2 Archive this change to `openspec/changes/archive/YYYY-MM-DD-adopt-sigorta-for-circuit-breaker/`.
