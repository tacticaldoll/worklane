## 1. Core definition

- [x] 1.1 Add `pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);` to
      `worklane_core::spi` (beside `MAX_DEAD_LETTER_SWEEP`), with a doc comment
      naming it the default reservation lease / visibility timeout and the
      broker-author audience (CI docs gate is `-D missing_docs`).
- [x] 1.2 `cargo test -p worklane-core` + `cargo doc -p worklane-core` green; commit
      `feat(default-lease-single-source): add spi::DEFAULT_LEASE`.

## 2. Backends re-export the core const (D2)

- [x] 2.1 `worklane-memory`: replace the local `pub const DEFAULT_LEASE` with
      `pub use worklane_core::spi::DEFAULT_LEASE;`; confirm internal uses resolve.
- [x] 2.2 `worklane-sqlite`: same.
- [x] 2.3 `worklane-postgres`: same.
- [x] 2.4 `worklane-redis`: same.
- [x] 2.5 `cargo build --workspace` green (existing
      `worklane_<backend>::DEFAULT_LEASE` paths still resolve); commit
      `refactor(default-lease-single-source): re-export core DEFAULT_LEASE`.

## 3. Test-side defaults reference the core value (D3)

- [ ] 3.1 `worklane-test`: initialize `BrokerConfig::DEFAULT_LEASE` from
      `worklane_core::spi::DEFAULT_LEASE` instead of a `Duration::from_secs(30)`
      literal.
- [ ] 3.2 Redirect each backend contract test's `const TEST_LEASE`
      (`crates/worklane-{memory,sqlite,postgres,redis}/tests/broker_contract*.rs`)
      to `worklane_core::spi::DEFAULT_LEASE`.
- [ ] 3.3 `cargo test --workspace` green; commit
      `refactor(default-lease-single-source): source test lease defaults from core`.

## 4. Verification (Definition of Done)

- [ ] 4.1 `cargo build`, `cargo fmt --all --check`,
      `cargo clippy --all-targets -- -D warnings`, `cargo deny check` clean.
- [ ] 4.2 `RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps`
      clean (the new `spi` `pub const` is documented).
- [ ] 4.3 `cargo test --workspace` green, including live Postgres + Redis
      (lease/poison/timed conformance is the regression gate).
- [ ] 4.4 `cargo run -p worklane-governance -- check --manifest-path Cargo.toml`
      clean (no new cross-crate edge).
- [ ] 4.5 Grep confirms the default-lease value `Duration::from_secs(30)` survives
      in exactly one place (`worklane-core/src/spi.rs`); every `DEFAULT_LEASE` and
      `TEST_LEASE` is a re-export of or reference to it. Unrelated 30s values (worker
      circuit-breaker window, bounded-handler test) are explicitly not lease
      defaults and stay.

## 5. Archive bookkeeping

- [ ] 5.1 Archive with
      `openspec archive default-lease-single-source --skip-specs` (no delta spec);
      commit `chore(openspec): archive default-lease-single-source`.
- [ ] 5.2 Update `BACKLOG.md`: move the deferred "Lift `DEFAULT_LEASE` to core" item
      to *Shipped* (note the core-root home + per-backend `pub use`).
