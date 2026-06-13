## Context

`lane` is half-present in worklane today. The broker spec's `Enqueue` scenario
already reads "enqueued to a lane ... `reserve` on that lane shall return it",
the `Broker::reserve(lane)` signature takes a lane, and `Worker::with_lane`
exists. But `Client` has no way to set a lane, `NewJob` has no `lane` field, and
`InMemoryBroker::reserve(_lane)` ignores the argument and serves from one global
pool. So the in-memory broker silently under-delivers the broker contract.

This change makes lane first-class end to end (enqueue → store → reserve) with
the smallest possible surface, and uses that surface to exercise the two
governance rules just added to AGENTS.md (`#[non_exhaustive]` evolution and the
Broker SQL/Redis design gate).

## Goals / Non-Goals

**Goals:**
- A job can be enqueued to a named lane; default is `"default"`.
- `reserve(lane)` returns only jobs from that lane; other lanes cannot be stolen.
- The lane travels with the job through reserve and into dead-lettering.
- No change to the `Broker` trait signatures (`reserve` already takes a lane).

**Non-Goals:**
- Priority, fairness, or any cross-lane scheduling.
- Worker concurrency or multi-lane workers.
- Durable brokers.
- Per-call lane override (`enqueue_to`), `JobContext.lane`, a `Lane` newtype, and
  lane registration / typo protection — all recorded in BACKLOG.md lane
  follow-ups.

## Decisions

### 1. `lane` lives on `JobEnvelope`, not only `NewJob`

`reserve` must filter stored jobs by lane, so the lane has to be part of what the
broker stores — i.e. on the envelope. Putting it there also means a `DeadLetter`
(which wraps the envelope) retains the lane for free, and a future durable broker
gets a natural `lane` column. There is no persisted data yet, so the schema cost
is zero now and only grows later.

*Alternative — lane only on `NewJob` + broker-internal state:* keeps `JobEnvelope`
minimal, but forces dead-letter to carry lane separately and gives durable
brokers nothing to map. Rejected; the envelope is the right home.

This makes `lane` part of the durable, on-the-wire job contract (per the AGENTS.md
"API stability and evolution" rule). The added fields land on `#[non_exhaustive]`
types so this and future fields stay additive.

### 2. Routing by call, not by job type

The lane is chosen at enqueue time (via the client), not declared on the `Job`
trait as a `const LANE`. A lane is an operational routing concern — the same job
type may run on different lanes in different deployments — so baking it into the
type would be wrong and less flexible.

*Alternative — `const LANE` on `Job`:* simpler to read at the call site but
couples job identity to deployment routing. Rejected.

### 3. Per-client lane, not per-call

`Client::with_lane(lane)` is a consuming builder, symmetric with the existing
`with_max_attempts`. One client targets one lane; enqueuing to several lanes
means constructing several clients. This is the minimal, consistent surface.

*Alternative — `enqueue_to(lane, payload)` per call:* more flexible but adds API
surface before it is needed. Deferred to BACKLOG.md.

### 4. `DEFAULT_LANE = "default"` is a single shared constant

The enqueue side and the reserve side MUST agree on the default lane, or jobs
enqueued with the default would be invisible to a default worker. One
`DEFAULT_LANE` constant is the single source of truth for both `Client` and
`Worker`, and the specs state the default explicitly.

### 5. Bare `String` / `&str`, no `Lane` newtype yet

`lane` is a `String` on the types and `&str` on `reserve`. A `Lane` newtype would
buy validation/interning room, but that is speculative now; `#[non_exhaustive]`
on the carrying types preserves the option to introduce it without breaking
callers. Deferred to BACKLOG.md.

## Risks / Trade-offs

- **Typo'd or unworked lane silently accumulates jobs** → Deliberate: lanes are
  arbitrary strings with no registration, so a job on a lane no worker reserves
  stays enqueued forever. Specified as an explicit operator-responsibility
  scenario rather than hidden, and `len()`/`is_empty()` on the in-memory broker
  let tests and operators observe the backlog.
- **Adding `lane` breaks existing `NewJob`/`JobEnvelope` literals** → Mitigated by
  `#[non_exhaustive]` and by the project being pre-release (0.0.1) with the only
  in-tree constructors under our control.
- **Broker portability** → The SQL/Redis design gate passes: lane-scoped reserve
  is `WHERE lane = $1 AND visible ... FOR UPDATE SKIP LOCKED`. No in-memory-only
  assumption is introduced and the `Broker` trait is unchanged.

## Migration Plan

Pre-release, single repo, no persisted state. Land core type changes
(`NewJob`/`JobEnvelope` gain `lane`, both `#[non_exhaustive]`), then the broker
filter, then the client builder, in one change. No data migration; no rollback
beyond reverting the commit(s).

## Open Questions

None — the four micro-decisions above are settled. `JobContext.lane`, per-call
override, and the `Lane` newtype are intentionally deferred, not open.
