## 1. Implement the script cache

- [x] 1.1 Add a `Scripts` struct in `crates/worklane-redis/src/scripts.rs` (the
      Redis analogue of Postgres `Queries`) holding one prebuilt `redis::Script`
      per operation: enqueue, enqueue_batch, reserve, ack, retry, defer, fail,
      extend, requeue, purge_dead, pending_count, enqueue_scheduled, and classify
      — built once in `Scripts::new` from the existing `scripts::*` body
      functions. The previously inline `classify` literal in `lib.rs` is pulled
      into `scripts.rs` (new `CLASSIFY` const) so all script text lives in one
      place.
- [x] 1.2 Build the `Scripts` value once in the `RedisBroker` constructor
      (`connect_with_namespace`) and store it on the struct; keep `scripts::*` as
      the single source of script text (the constructor calls them, nothing else
      does).
- [x] 1.3 Replace every `redis::Script::new(...)` call site in
      `crates/worklane-redis/src/lib.rs` (all 13 sites, incl. the enqueue family,
      pending_count, and the inline classify) with a reference to the cached
      `self.scripts.x`. No body, key layout, or `KEYS`/`ARGV` change.

## 2. Verify behavior is unchanged

- [x] 2.1 Run the `worklane-test` Redis conformance suite against a live Redis
      and confirm it passes unchanged (this is the regression gate). Passed:
      `broker_contract` 77/77, plus configured/lane-safety/migration/restart/
      result-store suites, 0 failed / 0 ignored, against single-node `redis:7`.
- [x] 2.2 Run `cargo clippy`/`cargo test` for the workspace; confirm no new
      warnings and no public API signature change.
- [x] 2.3 Capture a brief before/after micro-measurement of a Redis hot-path op
      (e.g. reserve+ack drain) and note it in the PR description; baseline for
      reference is ~28,486 jobs/s reserve+ack drain on single-node localhost.
      Measured with a throwaway sequential reserve+ack drain harness (20k jobs ×
      5 rounds, single-node `redis:7` on localhost): before (per-call
      `format!`+SHA1) best 3,432 jobs/s / median ~3,125; after (cached `Script`)
      best 3,632 jobs/s / median ~3,535 — a consistent but modest gain because a
      single-connection drain is round-trip-latency bound, so per-call CPU is a
      small fraction. The saving is larger under concurrency/pipelining (the
      regime of the ~28,486 jobs/s baseline), where the removed allocation +
      SHA1 actually competes with throughput. Harness was throwaway and not
      committed (the change is a clean refactor, not a new bench artifact).

## 3. Docs and backlog

- [x] 3.1 Update `BACKLOG.md`: add this change to **Shipped** with a ✓ entry
      describing the Redis hot-path script-cache (precompute once, mirror the
      Postgres `Queries` pattern).
- [x] 3.2 Position the remaining perf/risk scan findings in `BACKLOG.md` as
      future ideas (NOT implemented here):
      - **P2 — Postgres `enqueue_batch` no-unique-key UNNEST fast path**: for
        batches without unique keys, skip the per-row dedup machinery and use a
        single multi-row `UNNEST` insert; measured insert-shape ceiling ~15,450
        jobs/s vs the current ~5,500, ~2.8× headroom. Unique-key rows keep the
        existing per-row claim path.
      - **P3 — quantified idle-poll tax**: 16 idle workers issue ~4,000
        empty-`reserve` queries/s on Postgres (~87,000/s on Redis). Document in
        `docs/known-limitations.md` as the cost of poll-based design;
        explicitly DO NOT add LISTEN/NOTIFY — it reintroduces the commit
        serialization that worklane deliberately avoids. Mitigation is worker
        idle backoff.
      - **R1 — pull the parked "Adversarial conformance" item's clock-skew +
        fault-injection slices forward**: `ManualClock` has no `set`/rewind, and
        the duplicate-window-widening on a forward clock step (documented in all
        three durable brokers) is untested.
      - **R2 — make SQLite `insert_job` dedup defensive**: use
        `INSERT ... ON CONFLICT (unique_key) DO NOTHING` + re-read to match the
        Postgres claim loop, rather than relying solely on the single-writer
        invariant.
- [x] 3.3 Correct the stale duplication counts already in `BACKLOG.md`
      (the "Cross-broker logic dedup" item): `MAX_DEAD_LETTER_SWEEP` has **4**
      copies, not 3 — the fourth is the Redis Lua literal `sweep_cap = 128` in
      `crates/worklane-redis/src/scripts.rs`; the `i64 → JobState` classify
      mapping has **3** integer-mapping copies, not 4 (the memory broker returns
      `JobState` directly and is structurally different).
