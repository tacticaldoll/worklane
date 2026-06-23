## 1. Broker Contract Split

- [ ] 1.1 Define the minimal lifecycle broker trait and optional capability
  traits in `worklane-core`.
- [ ] 1.2 Move optional surfaces for batch enqueue, dead-letter inspection,
  queue stats, scheduled enqueue, and result storage behind explicit capability
  boundaries.
- [ ] 1.3 Document `worklane_core::spi` as the broker-author helper surface and
  keep it out of the `worklane` facade.
- [ ] 1.4 Add migration notes for direct broker implementers and direct trait
  users.

## 2. First-Party Broker Migration

- [ ] 2.1 Update `worklane-memory` to implement the split lifecycle and optional
  capability traits.
- [ ] 2.2 Update `worklane-sqlite` to implement the split lifecycle and optional
  capability traits.
- [ ] 2.3 Update `worklane-postgres` to implement the split lifecycle and
  optional capability traits.
- [ ] 2.4 Update `worklane-redis` to implement the split lifecycle and optional
  capability traits.
- [ ] 2.5 Update `worklane`, `worklane-scheduler`, `worklane-cli`, examples, and
  tests to use the split contract.

## 3. Modular Conformance

- [ ] 3.1 Split `worklane-test` broker scenarios into mandatory lifecycle and
  optional capability suites.
- [ ] 3.2 Update harness traits and macros so brokers can run lifecycle-only
  conformance or opt into capability suites.
- [ ] 3.3 Update all first-party broker contract tests to enumerate the shared
  lifecycle and capability suites from a single source.
- [ ] 3.4 Ensure omitted optional capability suites are visible in test wiring,
  documentation, or the conformance matrix.

## 4. Documentation

- [ ] 4.1 Add a lifecycle semantics guide that summarizes OpenSpec behavior
  without creating a second contract.
- [ ] 4.2 Add a custom broker conformance guide for wiring a private or
  third-party broker into `worklane-test`.
- [ ] 4.3 Add a broker conformance matrix distinguishing lifecycle conformance
  from optional capability support.
- [ ] 4.4 Update README, architecture docs, and crate docs to point broker
  authors to the SPI and conformance guide.

## 5. Verification

- [ ] 5.1 Run `cargo fmt --all --check`.
- [ ] 5.2 Run `cargo build`.
- [ ] 5.3 Run `cargo test`.
- [ ] 5.4 Run `cargo clippy --all-targets -- -D warnings`.
- [ ] 5.5 Verify the lifecycle guide, conformance guide, and conformance matrix
  match the delta specs.
- [ ] 5.6 Update BACKLOG.md with the ✓ shipped status after archiving.
