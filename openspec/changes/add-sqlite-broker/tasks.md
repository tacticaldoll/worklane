## 1. Scaffold the crate

- [x] 1.1 Create `crates/worklane-sqlite/` with `Cargo.toml`: package metadata
      inheriting `version`/`edition`/`license`/`authors` from the workspace;
      deps `worklane-core`, `async-trait`, `rusqlite` (features `["bundled"]`),
      `serde_json`, `tokio`; dev-deps `worklane-test`, `tokio`.
- [x] 1.2 Add `rusqlite` (with `bundled`) to root `[workspace.dependencies]`
      and add the `worklane-sqlite = { path = ... }` workspace entry.
- [x] 1.3 Confirm the crate is picked up by the `crates/*` workspace glob and
      `cargo build -p worklane-sqlite` compiles an empty `lib.rs`.

## 2. Implement `SqliteBroker`

- [x] 2.1 Define the broker struct (`Arc<Mutex<rusqlite::Connection>>`,
      `Arc<dyn Clock>`, `lease: Duration`) and the schema DDL constant
      (`jobs` + `dead` tables per design.md; `CREATE TABLE IF NOT EXISTS`).
- [x] 2.2 Constructors mirroring `InMemoryBroker`: `open(path)`,
      `open_in_memory()`, `with_clock(...)`, `with_lease(Duration)` builder, and
      `DEFAULT_LEASE`; run the schema DDL at construction.
- [x] 2.3 Time + receipt/id helpers: `Duration` ⇄ `i64` nanos
      (`as_nanos`/`from_nanos`), receipt ⇄ `TEXT`, envelope ⇄ `BLOB` via
      `serde_json`. A `stale()` error helper matching `InMemoryBroker`.
- [x] 2.4 `enqueue`: serialize a fresh `JobEnvelope` (attempts 0), insert with
      `available_at = now`, NULL lease/receipt; return the `JobId`.
- [x] 2.5 `reserve`: the single atomic `UPDATE … RETURNING envelope` (lane +
      `available_at <= now` + `(leased_until IS NULL OR leased_until <= now)`,
      `ORDER BY seq LIMIT 1`); set new receipt + `leased_until = now + lease`;
      return `Reservation` or `None`.
- [x] 2.6 Receipt validation shared by ack/retry/fail: a row with this receipt
      exists AND `leased_until > now`, else `Error::StaleReservation` (covers
      expired, superseded, unknown).
- [x] 2.7 `ack`: delete the validated row.
- [x] 2.8 `retry`: validate; deserialize blob, `attempts += 1`, reserialize;
      set `available_at = now + delay`, clear lease/receipt.
- [x] 2.9 `fail`: validate; move the row's envelope + error into `dead`.
- [x] 2.10 Wrap each `Broker` method body in `spawn_blocking` over the cloned
      `Arc<Mutex<Connection>>`; map `rusqlite`/`serde_json` errors to
      `Error::Broker`.
- [x] 2.11 Add a `dead_letters()` inspection method (reads the `dead` table)
      for the harness — a per-impl convenience, not on the `Broker` trait.

## 3. Add the envelope-fidelity scenario to the shared suite

- [x] 3.1 In `worklane-test` `scenarios.rs`, add a required-tier
      `enqueue_preserves_envelope_fields` scenario: enqueue a job with a
      distinctive `kind`, non-UTF-8 `payload` bytes, and a specific
      `max_attempts`; reserve it; assert `lane`, `kind`, `payload`, and
      `max_attempts` equal the enqueued values and `attempts == 0`.
- [x] 3.2 Register it in the `broker_contract_required!` macro (`lib.rs`).
- [x] 3.3 Confirm `cargo test -p worklane-memory` still passes (the in-memory
      broker satisfies fidelity trivially) — guards against an accidental
      contract that only the new backend meets.

## 4. Run the shared conformance suite against SQLite

- [x] 4.1 Add `tests/broker_contract.rs`: a `SqliteHarness` (fresh
      `open_in_memory()` per scenario) implementing `BrokerContractHarness`
      (`broker()`, `dead_letters()` → `Some(...)`).
- [x] 4.2 Add a `TimedSqliteHarness` (manual clock + known lease) implementing
      `TimedBrokerContractHarness` (`advance`, `lease`).
- [x] 4.3 Invoke `broker_contract_required!(SqliteHarness::new())` and
      `broker_contract_timed!(TimedSqliteHarness::new())`; confirm all eleven
      scenarios pass (eight required incl. fidelity + three timed).

## 5. Verify the decoupling tripwire + DoD

- [x] 5.1 Confirm `git diff` touches no file under `crates/worklane-core/` —
      the `Broker` trait and every public type are one line unchanged.
- [x] 5.2 Definition of Done: `cargo build`, `cargo test`,
      `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all --check` all
      green across the workspace.

## 6. Record governance + deferrals

- [x] 6.1 `AGENTS.md` (Broker design gate): note the `Broker` trait has now been
      validated against a durable backend (`worklane-sqlite`) without change.
- [x] 6.2 `BACKLOG.md`: record the deferred items this milestone surfaced —
      `reserve` ordering / strict-FIFO-per-lane (left unspecified to avoid
      pre-committing against the backlogged priority queue),
      `JobEnvelope::from_stored` + columnar schema (first consumer: Postgres
      broker), the restart-durable wall-clock epoch boundary, the connection
      pool, and `PRAGMA user_version` schema versioning.
