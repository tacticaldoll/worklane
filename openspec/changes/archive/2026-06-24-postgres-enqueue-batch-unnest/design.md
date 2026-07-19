## Context

`PostgresBroker::enqueue_batch` (`crates/worklane-postgres/src/lib.rs`) opens one
transaction, sorts and advisory-locks the batch's unique keys, then loops
calling `insert_job` once per job. `insert_job` is the single insertion path
shared with the singular `enqueue`: for each job it runs a dedup `SELECT` (when
the job has a `unique_key`), an `INSERT … ON CONFLICT (id) DO NOTHING RETURNING
seq`, and — for unique-key jobs — a `unique_keys` claim loop with up to 16
re-spins. That machinery is necessary to arbitrate `unique_key` deduplication
under READ COMMITTED, where the initial `SELECT` can race.

But the dedup work is conditional on `unique_key` being present, and the common
batch-throughput workload has none. For such a batch the loop degrades to N
sequential `INSERT … RETURNING` round-trips inside the transaction, when a single
multi-row insert would store the whole batch in one statement.

Measured insert-shape ceilings (isolated from dedup/FIFO logic, single-node
`postgres:16` on localhost, 10k rows, chunk 500): per-row loop `INSERT` ≈ 13,300
jobs/s; single multi-row `UNNEST` `INSERT` ≈ 31,000 jobs/s. The real
`enqueue_batch` path (carrying the dedup `SELECT`/claim work) sits at ≈ 5,400
jobs/s. The no-unique-key case therefore has ≈ 2.3× headroom against the loop
ceiling, and more against the current real path.

The sibling Redis broker just shipped the analogous hot-path win (the
`redis-hotpath-script-cache` change). This is the Postgres write-path analogue.

## Goals / Non-Goals

**Goals:**

- For a batch in which **every** job has `unique_key == None`, store the whole
  batch with a single multi-row `INSERT … SELECT FROM UNNEST(…) ON CONFLICT (id)
  DO NOTHING`, skipping the per-row dedup `SELECT`/claim machinery entirely.
- Preserve every observable batch guarantee the `broker` spec fixes: atomic
  all-or-nothing insertion, strict input-order FIFO (`seq` assigned in input
  order), `JobId` idempotency on re-enqueue, per-job `available_at` (delay), and
  whole-batch rollback if any job is unencodable.
- Keep the two durable brokers structurally consistent by precomputing the
  static `UNNEST` statement in the existing `Queries` precompute, mirroring the
  reserve/resolve hot statements.

**Non-Goals:**

- No change to the unique-key path. A batch containing **any** unique-key job
  keeps the existing advisory-lock-sorted, per-row `insert_job` loop verbatim.
- No `Broker`/`BatchEnqueue` trait, public API, schema, or wire-format change.
- Not touching the singular `enqueue` path, the Redis/SQLite brokers, or the
  other BACKLOG scan findings (P3 idle-poll tax).

**Considered fold-ins, deliberately excluded** (kept out to keep the diff
surgical and behavior-preserving):

- _Precomputing the slow-path `insert_job` SQL into `Queries`:_ `insert_job`
  still `format!`s its dedup `SELECT`, `INSERT … ON CONFLICT (id)`, and
  `unique_keys` claim statements per call. That is the cold path (not the
  measured bottleneck) and is shared with singular `enqueue`; precomputing it
  would expand this diff into the dedup machinery this change deliberately
  leaves untouched. Left as a separate future cleanup.
- _A symmetric multi-row fast path for the SQLite batch loop:_ SQLite is
  single-writer and fsync-bound, its batch throughput was not benchmarked, and
  the win here is statement-count amortization that Postgres round-trips make
  expensive. Without a SQLite measurement this is unjustified; recorded as a
  future cross-broker symmetry item, not folded in.

## Decisions

**Decision: gate the fast path on `jobs.iter().all(|j| j.unique_key.is_none())`.**
The fast path's correctness depends on no dedup arbitration being needed; a
single unique-key job in the batch reintroduces the claim requirement, so a
mixed batch takes the existing per-row path unchanged. The predicate is a cheap
linear scan over a batch already held in memory.

- _Alternative — split the batch into a unique-key sub-batch (per-row) and a
  no-key sub-batch (UNNEST) within one transaction:_ rejected for this change.
  It complicates `seq`/FIFO ordering across the two sub-inserts and the dedup
  interleaving, for a workload (mixed batches) that is not the measured
  hot case. Can be revisited if a real consumer shows mixed batches dominate.

**Decision: single `INSERT … SELECT FROM UNNEST(…) WITH ORDINALITY … ORDER BY
ord ON CONFLICT (id) DO NOTHING`, binding column-parallel arrays.** Build five
parallel arrays in input order — `id`, `lane`, `priority`, `available_at`,
`envelope` — and bind them as Postgres array parameters. `receipt` and
`leased_until` are inserted `NULL` (a fresh, unleased job), matching `insert_job`.

The FIFO guarantee is the load-bearing subtlety. `seq` is `BIGSERIAL PRIMARY
KEY`, and `reserve` orders identical-priority/visibility jobs by `seq ASC`, so
strict input-order FIFO requires the `seq` sequence (`nextval`) to be assigned in
input order. A plain `INSERT … SELECT FROM UNNEST(a, b, …)` does **not**
guarantee this: without an `ORDER BY`, Postgres may assign serial values in any
row-production order the planner chooses (e.g. a parallel or reordered scan), and
the SQL standard makes no ordering promise for an `INSERT … SELECT` absent an
explicit sort. Small conformance batches almost never trigger a reordering plan,
so a plain `UNNEST` would pass `batch_preserves_order` yet could silently break
FIFO for large production batches. The statement therefore uses `UNNEST(…) WITH
ORDINALITY AS t(id, lane, priority, available_at, envelope, ord)` and `ORDER BY
t.ord`, which pins the rows fed to the sequence to input (ordinality) order. The
`ORDER BY` cost is negligible for realistic batch sizes. (The throwaway
insert-shape ceiling harness used plain `UNNEST` — it only measured throughput,
not ordering — so the ~31k jobs/s ceiling still applies; `WITH ORDINALITY` adds
no row-count-dependent cost.)

**Decision: extract the fast path into an `insert_batch_unnest` helper.** Rather
than inlining the array-build + insert into `enqueue_batch`, factor it into a
private method mirroring `insert_job`'s role, so `enqueue_batch` stays a readable
dispatcher: open tx → predicate → `insert_batch_unnest` (fast) or the
advisory-lock-sorted per-row loop (slow) → commit. This keeps the two paths
visually parallel and confines the new code to one named unit.

**Decision: keep `ON CONFLICT (id) DO NOTHING` and return all input ids in input
order.** `insert_job` returns the same `id` on an `(id)` conflict (a re-enqueue
of an id a live job already holds is a no-op). The fast path matches that: it
returns the batch's input ids in order regardless of whether any row was a
no-op, so `JobId` idempotency is preserved without inspecting `RETURNING`.
Freshly generated ids make a collision vanishingly rare, but the clause keeps the
contract identical.

**Decision: precompute the `UNNEST` statement in `Queries`.** Unlike the prior
per-row batch insert (whose SQL row count varied with the batch), the `UNNEST`
statement is fixed for any batch size, so it belongs with the other precomputed
hot statements (`reserve`, `retry_update`, …) rather than being `format!`-ed per
call. Add an `enqueue_batch_unnest` field to `Queries`, built once from the
schema, and update the `Queries` module doc that currently states batch insert
"cannot be precomputed" (true for the per-row loop, no longer for the no-key
fast path).

**Decision: build all envelope blobs before issuing the insert.** Encoding every
job's envelope up front means an unencodable job returns `Err` before any row is
written; the transaction is dropped uncommitted, so the whole batch rolls back —
satisfying the "unencodable lane/job rolls back the entire batch" scenario
exactly as the per-row path does (which fails mid-loop and never commits).

## Risks / Trade-offs

- [FIFO ordering drift — the UNNEST insert assigns `seq` out of input order] →
  mitigated structurally by `WITH ORDINALITY … ORDER BY ord` (see Decisions),
  which guarantees serial assignment in input order rather than relying on
  incidental scan order. The Postgres FIFO conformance scenario
  (`batch_preserves_order` / `reserve` is FIFO for identical priority and
  visibility) is the regression gate; note it is a *small* batch and would not
  by itself catch a plan-dependent reordering, which is exactly why the ordering
  is pinned in SQL rather than left to the planner.
- [Gate-boundary regression — a mixed batch (some unique-key, some not)
  wrongly taking the fast path would skip dedup] → the `all(unique_key.is_none())`
  predicate routes any unique-key-bearing batch to the slow path. There is
  currently no conformance scenario for a *mixed* batch, so the new branch
  boundary is untested; this change adds one (a mixed batch must still dedup its
  unique-key jobs and preserve order), guarding the predicate for every broker.
- [A behavioral divergence between the two paths for some edge — empty batch,
  duplicate ids within one no-key batch] → Empty batch: `all()` is true on an
  empty iterator, the arrays are empty, `UNNEST` inserts zero rows, and an empty
  id vec is returned — identical to the current early no-op. Duplicate ids
  within a batch are not produced by the typed client (ids are freshly
  generated) and `ON CONFLICT (id) DO NOTHING` keeps the first; called out so
  the conformance run is read with this in mind.
- [Mixed batches see no speedup] → accepted and intentional; they retain exact
  current behavior. The fast path targets the measured hot case, not all
  batches.
- [Subtle refactor error caching the wrong column order in the bound arrays] →
  the `worklane-test` Postgres `BatchEnqueue` battery (atomicity, FIFO,
  intra-batch and concurrent-overlap dedup, lane rollback) is the regression
  gate and must pass unchanged; a column-order mistake fails FIFO or atomicity.

## Migration Plan

Pure in-process write-path optimization — no schema, data, or wire-format
change. Deploys with a normal release; rollback is reverting the commit. No
migration steps and no operator action. Existing rows and in-flight batches are
unaffected (the statement targets the same table and columns).
