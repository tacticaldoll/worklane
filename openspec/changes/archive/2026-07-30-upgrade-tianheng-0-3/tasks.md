## 1. Dependency Upgrade

- [x] 1.1 Record the Tianheng 0.1.10 canonical JSON boundary projection.
- [x] 1.2 Upgrade the workspace Tianheng dependency and lockfile to 0.3.
- [x] 1.3 Apply only source-compatibility changes required by Tianheng 0.3.

## 2. Governance Compatibility

- [x] 2.1 Add regression coverage for all eight normalized boundary records.
- [x] 2.2 Verify clean, enforced-violation, and invalid-runner reaction classes.
- [x] 2.3 Verify the governance crate still has no workspace dependencies.

## 3. Quality Gates

- [x] 3.1 Run the governance check and compare the canonical after-state
  projection with the recorded before-state.
- [x] 3.2 Run the Rust 1.85 compatibility check.
- [x] 3.3 Run `cargo build`, `cargo test`,
  `cargo clippy --all-targets -- -D warnings`, and
  `cargo fmt --all --check`.
- [x] 3.4 Validate the OpenSpec change and confirm no product behavior or
  accepted law changed.

## 4. Integration

- [x] 4.1 Update BACKLOG.md with the ✓ shipped status after archiving.
