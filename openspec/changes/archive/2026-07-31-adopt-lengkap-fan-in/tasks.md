## 1. Registry Dependency

- [x] 1.1 Add published `lengkap` 0.1.0 to the workspace dependency table and
  the `worklane` facade manifest.
- [x] 1.2 Resolve Cargo.lock from crates.io and verify no path source remains.

## 2. Fan-In Decision Adapter

- [x] 2.1 Restore existing captured values into fixed Lengkap dependency slots.
- [x] 2.2 Map unresolved broker observations to produced or impossible findings
  while leaving live dependencies pending.
- [x] 2.3 React to pending, ready, and impossible decisions with existing
  Worklane-owned checkpoint, scheduling, callback, and error behavior.
- [x] 2.4 Preserve prior checkpoint tuple order while appending new captures in
  dependency order.

## 3. Equivalence Evidence

- [x] 3.1 Add a checkpoint round-trip unit test that proves slot identity,
  prior tuple order, and ready result order.
- [x] 3.2 Run the focused fan-in watcher and workflow integration suites.

## 4. Verification

- [x] 4.1 Run build, test, clippy, and format Definition of Done gates.
- [x] 4.2 Run Rust 1.85 all-targets, cargo-deny, Tianheng, OpenSpec, and
  workspace package verification gates.
- [x] 4.3 Record promotion evidence from the published registry dependency.

## 5. Specification Integration

- [x] 5.1 Sync the workflow delta into the living specification.
- [x] 5.2 Archive the completed adoption change.
- [x] 5.3 Update BACKLOG.md with the ✓ shipped status after archiving.
