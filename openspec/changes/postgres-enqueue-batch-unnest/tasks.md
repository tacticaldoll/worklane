## 1. Precompute the UNNEST statement

- [x] 1.1 Add an `enqueue_batch_unnest: String` field to the `Queries` struct in
      `crates/worklane-postgres/src/queries.rs`, built once in `Queries::new`
      from the schema-qualified `jobs` table. The statement MUST pin input-order
      FIFO with `WITH ORDINALITY` + `ORDER BY` (a plain `UNNEST` does not
      guarantee `BIGSERIAL seq` is assigned in input order): `INSERT INTO {jobs}
      (id, receipt, lane, priority, available_at, leased_until, envelope) SELECT
      id, NULL, lane, priority, available_at, NULL, envelope FROM
      UNNEST($1::text[], $2::text[], $3::int2[], $4::int8[], $5::bytea[]) WITH
      ORDINALITY AS t(id, lane, priority, available_at, envelope, ord) ORDER BY
      t.ord ON CONFLICT (id) DO NOTHING`. Confirm column order and types match
      the schema (`priority int2`, `available_at int8`, `envelope bytea`).
- [x] 1.2 Update the `Queries` module doc that says the batch insert "cannot be
      precomputed" — true for the per-row loop, no longer true for the
      fixed-shape no-unique-key UNNEST statement.

## 2. Implement the no-unique-key fast path

- [x] 2.1 In `PostgresBroker::enqueue_batch` (`crates/worklane-postgres/src/lib.rs`),
      after opening the transaction, branch on
      `jobs.iter().all(|j| j.unique_key.is_none())`. When true, delegate to a new
      `insert_batch_unnest` helper; otherwise fall through to the existing
      advisory-lock-sorted per-row `insert_job` loop, unchanged. `enqueue_batch`
      stays a readable dispatcher (open tx → predicate → fast/slow → commit).
- [x] 2.2 Add the private `insert_batch_unnest(&self, tx, jobs, now) ->
      Result<Vec<JobId>>` helper, mirroring `insert_job`'s role. It builds five
      input-order parallel vectors — `ids` (`String`), `lanes` (`String`),
      `priorities` (`i16`), `available_ats` (`i64` from
      `nanos(now.saturating_add(job.delay))` per job), and `envelopes` (`Vec<u8>`
      from `encode_envelope`) — consuming each `NewJob` into its envelope.
      Building all blobs up front means an unencodable job returns `Err` before
      any insert, so the dropped transaction rolls the whole batch back
      (all-or-nothing).
- [x] 2.3 In the helper, issue the single `self.queries.enqueue_batch_unnest`
      statement binding the five arrays, and return the `ids` in input order
      (independent of `RETURNING`, matching `insert_job`'s same-id-on-conflict
      semantics). Verify the empty-batch case (all-true predicate over no jobs)
      binds empty arrays, inserts nothing, and returns an empty vec — matching
      `batch_empty` and current behavior.

## 3. Guard the gate boundary (new conformance scenario)

- [x] 3.1 Add a `batch_mixed_unique_and_plain` scenario to
      `crates/worklane-test/src/scenarios/batch.rs` and register it in the batch
      battery: a single batch mixing unique-key and plain jobs (e.g. `[plain,
      key("k"), plain, key("k")]`) MUST still dedup the unique-key jobs to one
      live job and preserve input order. This pins the `all(unique_key.is_none())`
      gate so a mixed batch can never silently take the dedup-skipping fast path,
      and it guards every broker, not just Postgres. (The existing
      `batch_all_visible` / `batch_preserves_order` / `batch_empty` already cover
      the all-plain fast path; the dedup/concurrent scenarios cover the all-key
      slow path — only the mixed boundary is untested today.)

## 4. Verify behavior is unchanged

- [x] 4.1 Run the `worklane-test` Postgres conformance suite against a live
      Postgres and confirm it passes unchanged — the mandatory lifecycle battery
      plus the `BatchEnqueue` capability battery: atomic all-or-nothing insert,
      strict input-order FIFO on reserve, intra-batch unique-key dedup,
      concurrent overlapping batches (no deadlock), unencodable-lane whole-batch
      rollback, and the new mixed-batch scenario. This is the regression gate.
- [x] 4.2 Run `cargo clippy` and `cargo test` for the workspace; confirm no new
      warnings and no public API / `Broker` / `BatchEnqueue` signature change.
- [x] 4.3 Capture a before/after micro-measurement of a no-unique-key
      `enqueue_batch` using the repaired `bench/` head-to-head harness (its
      `wl_bulk` path is exactly no-unique-key `enqueue_batch`), not a fresh
      throwaway. Reference: current real path ≈ 5,400 jobs/s; isolated UNNEST
      ceiling ≈ 31,000 jobs/s. Note the result in the PR description.

## 5. Docs and backlog

- [ ] 5.1 Move the **P2** entry in `BACKLOG.md` from positioned-future to a
      ✓-shipped entry describing the no-unique-key UNNEST fast path (gated on
      `all unique_key == None`, mixed batches keep the per-row claim path,
      `WITH ORDINALITY` for FIFO, behavior-preserving, conformance suite as the
      gate including the new mixed-batch scenario).
- [ ] 5.2 Archive the change with `openspec archive postgres-enqueue-batch-unnest
      --skip-specs` (no spec delta — behavior-preserving, the existing `broker`
      spec already fixes the contract).
