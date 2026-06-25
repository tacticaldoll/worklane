## 1. Core shared surface (worklane-core `spi`)

All new items are `pub` (cross-crate sharing) and live in `worklane_core::spi`
with broker-author audience docs; they are NOT re-exported from the `worklane`
facade. Every new `pub` item MUST carry a doc comment (the CI docs gate is
`-D missing_docs`).

- [ ] 1.1 Add `MAX_DEAD_LETTER_SWEEP` const to `spi` (single source of truth for
      the per-reserve dead-letter sweep bound), documented.
- [ ] 1.2 Add a classify helper in `spi` mapping `Option<i64>` → `JobState`
      (`1 → Live`, `2 → DeadLettered`, else/`None` → `CompletedOrUnknown`),
      documented; unit-test all arms incl. `None`.
- [ ] 1.3 Add `SCHEMA_VERSION` const + a match-vs-reject *decision* helper to `spi`
      (given the stored `Option<i64>`, report match vs. mismatch). **No message
      string in core** — remediation text stays per-backend (D3). Documented;
      unit-test match / mismatch / absent.
- [ ] 1.4 Add a retention computation method on the core retention surface
      returning the age-cutoff instant and the count-keep bound from a
      `RetentionPolicy` + `now` (D4). Documented; unit-test bounded/unbounded.
- [ ] 1.5 `cargo test -p worklane-core` green; commit
      `feat(cross-broker-decision-dedup): add shared core decisions to spi`.

## 2. Sweep-bound conformance gate (D6) — add on CURRENT code first

- [ ] 2.1 Add a `worklane-test` scenario: enqueue more than the cap of
      expired/poison jobs on one lane; assert a single `reserve` dead-letters a
      bounded number and yields empty, AND a subsequent `reserve` continues the
      sweep (bounded-progress). Runs across all four backends.
- [ ] 2.2 Run it against unmodified code (green = it pins existing behaviour);
      commit `test(cross-broker-decision-dedup): pin dead-letter sweep bound`.

## 3. Adopt the sweep cap (D2)

- [ ] 3.1 `worklane-memory`: replace local `MAX_DEAD_LETTER_SWEEP` with the core
      const.
- [ ] 3.2 `worklane-sqlite`: replace local `const` with the core const.
- [ ] 3.3 `worklane-postgres`: replace local `const` with the core const.
- [ ] 3.4 `worklane-redis`: pass the core const into `RESERVE` as `ARGV[10]`;
      replace the `local sweep_cap = 128` literal with `tonumber(ARGV[10])`; add
      the 10th `.arg()` at the call site IN THE SAME EDIT.
- [ ] 3.5 `cargo test --workspace` green (the D6 scenario is the regression gate
      for 3.4); commit
      `refactor(cross-broker-decision-dedup): share dead-letter sweep cap`.

## 4. Adopt the classify mapping (D1)

- [ ] 4.1 `worklane-sqlite` / `worklane-postgres`: call the core helper, adding
      `.map(i64::from)` to widen the `i32` status column (do NOT change the column
      read type). `worklane-redis`: wrap its bare `i64` in `Some(..)` and call the
      helper. Remove the three hand-rolled `1/2/_` matches.
- [ ] 4.2 `cargo test --workspace` green (classify conformance covers
      Live/DeadLettered/CompletedOrUnknown on each backend); commit
      `refactor(cross-broker-decision-dedup): share classify mapping`.

## 5. Adopt the schema-version const + decision (D3)

- [ ] 5.1 `worklane-sqlite` / `worklane-postgres`: keep the dialect read/write
      (`PRAGMA user_version` / meta row) AND each backend's own remediation
      message; call the core decision helper and drop the local `SCHEMA_VERSION`
      const + inline policy.
- [ ] 5.2 `worklane-redis`: route the `ns:schema_version` baseline check through
      the core const + decision, keeping the Redis "flush / re-enqueue" message.
- [ ] 5.3 Point `worklane-redis/tests/migration.rs` `BASELINE_VERSION` at the core
      `SCHEMA_VERSION` so no fifth hand-synced copy survives.
- [ ] 5.4 `cargo test --workspace` green, INCLUDING `migration.rs` for all three
      backends (the Redis test pins its message wording); commit
      `refactor(cross-broker-decision-dedup): share schema-version policy`.

## 6. Adopt the retention computation (D4)

- [ ] 6.1 `worklane-sqlite`, `worklane-postgres`, and the Redis reserve path
      (`worklane-redis/src/lib.rs` cutoff feeding `RESERVE`) use the core retention
      method for the age-cutoff + keep-count; each keeps its own `DELETE`/sweep.
      Removes all three verbatim copies.
- [ ] 6.2 `cargo test --workspace` green; commit
      `refactor(cross-broker-decision-dedup): share retention prune math`.

## 7. Docs

- [ ] 7.1 Scan `docs/lifecycle-semantics.md`, `docs/architecture.md`,
      `docs/broker-conformance-matrix.md`, `docs/custom-brokers.md` for any
      "each backend implements X" claim about classify / sweep / schema versioning
      that is now centralized; update only stale claims (likely none).
- [ ] 7.2 Confirm every new `pub` `spi` item (1.1–1.4) has rustdoc (gate in 8.2).

## 8. Verification (Definition of Done)

- [ ] 8.1 `cargo build`, `cargo fmt --all --check`,
      `cargo clippy --all-targets -- -D warnings`, `cargo deny check` clean.
- [ ] 8.2 `RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace
      --no-deps` clean (CI docs gate — must pass with the new `pub` items).
- [ ] 8.3 `cargo test --workspace` green, including live Postgres + Redis
      (`WORKLANE_POSTGRES_TEST_URL` / `WORKLANE_REDIS_TEST_URL` set) — D3's
      Postgres/Redis reject paths only run with live DBs.
- [ ] 8.4 `cargo run -p worklane-governance -- check --manifest-path Cargo.toml`
      clean (no new cross-crate edge; confirm the helpers use the core `Error`,
      not a backend error type).
- [ ] 8.5 Confirm no `Broker` trait / core job-trait / `JobEnvelope` / on-disk
      schema / wire-format change. Grep the four backends + tests for any remaining
      copy of each lifted decision (sweep cap, classify match, `SCHEMA_VERSION`
      incl. the migration test const). Confirm `DEFAULT_LEASE` was intentionally
      left (out of scope).

## 9. Archive bookkeeping

- [ ] 9.1 Archive with `openspec archive cross-broker-decision-dedup --skip-specs`
      (no delta spec); commit
      `chore(openspec): archive cross-broker-decision-dedup`.
- [ ] 9.2 Update `BACKLOG.md`: move the "Cross-broker logic dedup" item to
      *Shipped* (note what was lifted), AND add a new deferred item to lift
      `DEFAULT_LEASE` (needs a re-export/deprecation API decision).
