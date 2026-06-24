## Why

`PostgresBroker::enqueue_batch` inserts every job through the shared
`insert_job` path: per job it runs a dedup `SELECT`, an `INSERT … ON CONFLICT
(id)`, and — for unique-key jobs — a claim loop. That per-row machinery exists
to arbitrate `unique_key` deduplication, but the common batch-throughput case
has **no unique keys at all**, and there it is pure overhead: N round-trips of
dedup logic where one multi-row insert would do. The isolated insert-shape
ceiling measures ~31,000 jobs/s for a single multi-row `UNNEST` insert versus
~13,300 jobs/s for the per-row loop, and the real `enqueue_batch` path (with its
dedup SELECT/claim work) sits at ~5,400 jobs/s — roughly **2.3× headroom** for
the no-unique-key case left on the table. This is the Postgres analogue of the
already-shipped Redis hot-path script cache: a behavior-preserving throughput
fix on a durable broker's write path.

## What Changes

- Add a fast path to `PostgresBroker::enqueue_batch`: when **every** job in the
  batch has `unique_key == None`, skip the per-row dedup/claim machinery and
  issue a single multi-row `INSERT … SELECT FROM UNNEST(…) ON CONFLICT (id) DO
  NOTHING` for the whole batch.
- Batches containing **any** unique-key job keep the existing per-row claim path
  unchanged (advisory-lock-sorted, `insert_job` per row) — the fast path is a
  pure addition gated on the no-unique-key predicate, not a replacement.
- Preserve all observable batch semantics exactly: input-order `seq` assignment
  (strict FIFO on reserve), `JobId` idempotency via `ON CONFLICT (id) DO
  NOTHING`, all-or-nothing atomicity within the transaction, and per-job
  `available_at` (delay) handling.
- No `Broker`/`BatchEnqueue` trait, public API, schema, or wire-format change —
  this is an internal write-path optimization.

## Capabilities

### New Capabilities

- _(none)_ — no new lifecycle behavior is introduced.

### Modified Capabilities

- _(none)_ — behavior-preserving internal optimization. The existing
  `broker` spec already fixes the contract this path must satisfy (atomic
  all-or-nothing batch enqueue, strict input-order FIFO, batch unique-key
  deduplication, and per-lane rollback on an unencodable lane); the fast path
  must continue to satisfy every one of those requirements unchanged. The
  `worklane-test` Postgres batch-enqueue conformance battery is the regression
  guard, so there is no `openspec/specs/` delta.

## Impact

- **Code**: `crates/worklane-postgres/src/lib.rs` (`enqueue_batch`), and the
  precomputed `queries.rs` `Queries` struct if the `UNNEST` statement is
  precomputed there for consistency with the existing hot-statement pattern.
- **APIs**: none. No public signature change; `Broker`/`BatchEnqueue` contract
  unchanged.
- **Dependencies**: none. Uses existing `tokio-postgres` array binding.
- **Verification**: the `worklane-test` Postgres conformance suite (the
  mandatory lifecycle battery plus the `BatchEnqueue` capability battery —
  atomicity, FIFO, intra-batch and concurrent-overlap dedup, lane rollback)
  must pass unchanged; a before/after micro-measurement documents the win.
- **Docs**: `BACKLOG.md` (move P2 from positioned-future to a ✓-shipped entry).
