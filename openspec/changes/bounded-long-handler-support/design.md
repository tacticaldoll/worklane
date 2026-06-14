## Context

Concurrency (`add-concurrent-worker`) made lease-too-short real and tested: a
handler outliving its lease is redelivered and runs twice, and its later
resolution is stale-rejected. Backlog step 5 (lease renewal) is the planned
mitigation. Exploration surfaced that a bare heartbeat is unsafe: a *hung*
handler — today self-limiting via lease-expiry → redelivery → attempt
exhaustion → dead-letter — would be held forever by an unconditional heartbeat
and never dead-lettered. So the heartbeat must be bounded by a handler timeout,
and the two ship together.

This is the **first deliberate change to the `Broker` trait** since
`add-sqlite-broker` durable-validated it. AGENTS.md treats that validation as
the point at which a required trait change is made "while it is still cheap"; the
Broker design gate (answerable as SQL/Redis) governs the new method.

Current per-job logic (`process`, `handle_failure`, `resolve`) and the loop
(`run`) are tangled in one `worker.rs`. Every part of this change lands in the
per-job path, which makes it the first real consumer of a per-job execution
seam.

## Goals / Non-Goals

**Goals:**
- Let a legitimately slow handler complete once without redelivery, when the
  operator opts in via a handler timeout.
- Keep a stuck/hung handler mortal: bounded by the timeout, then retried or
  dead-lettered.
- Add `Broker::extend` in a shape both the in-memory and SQLite brokers satisfy
  through the shared conformance suite, with the SQLite path expressible as a
  single guarded `UPDATE`.
- Extract a `worker/execution.rs` per-job unit, shaped by this feature as its
  first consumer.

**Non-Goals:**
- Unbounded heartbeat (heartbeat without a timeout) — deliberately not offered;
  it reintroduces the immortal-hang failure mode.
- Per-job (vs per-worker) timeout configuration — deferred until a caller needs
  it; v0.1 sets one timeout per worker.
- Cancelling/aborting the timed-out handler future mid-flight — see Decision 4.
- Multi-core/spawn-based execution — unchanged; this stays in-task.

## Decisions

### Decision 1: `extend(receipt)` re-applies the broker's lease; no caller duration

`extend` takes only the receipt and re-applies the broker's own configured lease
from `now`, mirroring `reserve`. Rationale: lease duration is broker-owned
(`with_lease`), whereas `retry(receipt, delay)` takes a delay because backoff is
the *worker's* policy (`RetryPolicy`). Keeping `extend` duration-free preserves
that split and keeps the contract minimal.

*Alternative considered:* `extend(receipt, lease)` with a caller-supplied
duration. Rejected: no present consumer needs a per-call lease, and it would let
callers set lease policy the broker is supposed to own.

**Broker design gate (SQL answer):**
```sql
UPDATE jobs SET leased_until = :now + :lease
WHERE receipt = :receipt AND leased_until > :now;   -- 0 rows ⇒ StaleReservation
```
This is the exact guard `ack`/`retry`/`fail` already use via `find_valid_row`
(`leased_until > now`). Redis: reset the key TTL / sorted-set score under the
same receipt guard. Both satisfy the gate with no full scan and no live
references.

### Decision 2: surface the lease on `Reservation`, worker heartbeats at `lease/2`

The worker cannot read the broker's injected `Clock`, so it cannot know when a
lease expires unless the broker tells it. Add `Reservation.lease: Duration`
(additive; `Reservation` is `#[non_exhaustive]`). The worker schedules each
heartbeat at `lease/2`, giving one guaranteed extend before expiry even under
scheduling jitter.

*Alternative considered:* `extend` returns the remaining lease so the worker
self-paces (supports variable-lease brokers). Rejected for now: the first
heartbeat still needs the lease from `reserve`, so `Reservation.lease` is
required regardless; current brokers use a fixed lease, so returning remaining
buys nothing yet. Recorded in BACKLOG if a variable-lease broker appears.

### Decision 3: heartbeat lives inside the per-job unit, not the loop

Each in-flight unit becomes `reserve → race(dispatch, heartbeat-tick,
timeout-deadline) → resolve`. The heartbeat timer (`sleep(lease/2)`) and the
timeout deadline (`sleep(handler_timeout)`) are selected against the handler
future *within* the unit, so the receipt stays local and the outer `run` loop
(`FuturesUnordered`, shutdown, poll) is unchanged. A fast handler wins the race
before the first tick, so heartbeating costs nothing for fast jobs.

This is why the extraction rides here: `worker/execution.rs` owns the per-job
state machine; `worker/mod.rs` keeps orchestration. Structure only — no
behavioral requirement describes module layout.

### Decision 4: on timeout, stop holding and fail; do not abort the handler

At the timeout the worker stops heartbeating and resolves via the failure path
(`retry`/`fail`). The handler future is dropped at end of unit; we do not
`catch_unwind` or force-abort a running handler here (handler panics are a
separate, later change). If a dropped/abandoned handler had already lost its
lease, its (absent) resolution is moot; if a heartbeat was rejected as stale
mid-run, the unit stops extending and the eventual resolution is stale-rejected
and logged — identical to today's at-least-once handling.

### Decision 5: fixed `lease/2` heartbeat fraction, not configurable

The worker heartbeats at a fixed `lease/2`. Rationale: it guarantees one extend
before expiry under normal jitter, and it is implementation-level — the spec
only requires the worker to "periodically extend," so the fraction can change
later without a spec or contract change. No present workload asks for a
configurable fraction.

*Alternative considered:* a `with_heartbeat_fraction` knob. Deferred (least
commitment): add it when a real workload shows `lease/2` is wrong. Revisitable
during apply if the code argues otherwise.

### Decision 6: a timeout is reported via `Error::Handler`, not a new variant

A timed-out handler is failed with `Error::Handler("handler timed out after …")`,
reusing the existing variant. Rationale: it flows through the unchanged failure
path (retry/dead-letter), and nothing branches on a distinct timeout error, so a
new `Error` variant would widen the public enum with no consumer. The spec only
requires "a timeout error," so this is implementation-level.

*Alternative considered:* a distinct `Error::Timeout`. Deferred until a caller
needs to branch on it (additive then, since `Error` is `#[non_exhaustive]`).
Revisitable during apply.

## Risks / Trade-offs

- [A timed-out handler may still be running after we route the job to retry] →
  Accepted: at-least-once already permits a second run; the timeout bounds the
  *hold*, not the OS thread. The redelivered attempt is a normal duplicate.
- [`lease/2` heartbeat adds broker calls for genuinely long handlers] →
  Bounded and opt-in: only handlers that outlive `lease/2` ever heartbeat, only
  when a timeout is configured. Fast handlers pay nothing.
- [Adding a required `Broker::extend` breaks external implementors] → Acceptable:
  trait was durable-validated precisely to make this first change cheap; only
  in-repo implementors exist. An ADR records the trait change.
- [Clock skew between heartbeat scheduling (tokio timer) and broker lease
  (injected clock)] → `lease/2` margin absorbs normal jitter; a pathological
  stall still degrades safely to redelivery (at-least-once), never to a lost job.

## Migration Plan

Additive and opt-in. Existing callers pass no handler timeout and see unchanged
behavior. Brokers must implement `extend` (the only breaking step), validated by
re-running the `worklane-test` conformance suite against both brokers. An ADR
under `docs/adr/` records the first post-validation `Broker` trait change.

## Open Questions

None outstanding. The two prior design questions (heartbeat fraction, timeout
error shape) are resolved as Decisions 5 and 6 — both implementation-level with a
documented default, revisitable during apply.
