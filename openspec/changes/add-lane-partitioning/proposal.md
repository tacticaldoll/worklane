## Why

The broker spec already promises lane partitioning — its `Enqueue` scenario says
"a job is enqueued to a lane ... a `reserve` on **that lane** shall return it" —
but the implementation does not deliver it: `Client` cannot choose a lane,
`NewJob` carries no lane, and `InMemoryBroker::reserve(_lane)` ignores its
argument entirely. The in-memory broker therefore violates the current broker
contract, and the `lane` already threaded through `Worker::with_lane` is inert.
This change makes lane a first-class, honoured concept — closing a
specified-but-unimplemented capability (AGENTS.md prioritization tier 2).

## What Changes

- Add a first-class `lane` to enqueue: `Client::with_lane(lane)` (builder,
  symmetric with `with_max_attempts`), defaulting to `DEFAULT_LANE = "default"`.
- Add `lane` to `NewJob` and to `JobEnvelope`. Placing it on the envelope (not
  only `NewJob`) lets `reserve` filter by it, lets dead-letters retain the lane
  for free (a `DeadLetter` already wraps the envelope), and gives future durable
  brokers a natural column — with zero migration cost while nothing is persisted
  yet. **BREAKING** to the in-progress `JobEnvelope`/`NewJob` constructors;
  mitigated by marking these types `#[non_exhaustive]` per AGENTS.md.
- Make the in-memory broker partition visible jobs by lane: `reserve(lane)`
  returns only jobs enqueued to that lane; jobs on other lanes are never stolen.
- Workers reserve only their configured lane (no cross-lane scan, no fairness).
- Out of scope (deferred to BACKLOG.md lane follow-ups): priority, concurrency,
  durable brokers, per-call lane override, exposing lane via `JobContext`, and a
  `Lane` newtype.

## Capabilities

### New Capabilities
<!-- none — this change completes an existing concept across existing specs -->

### Modified Capabilities
- `client`: `Typed enqueue` — the client submits a `NewJob` that carries a
  `lane`, configured per-client via `with_lane`, defaulting to `"default"`.
- `job-model`: `Opaque job envelope` — the `JobEnvelope` field set gains `lane`.
- `broker`: `Enqueue` — the stored envelope retains the lane it was enqueued to;
  `Backend-agnostic payloads` — `lane` joins the allowed envelope fields; plus a
  new `Lane-scoped reserve` requirement: `reserve(lane)` returns only that lane's
  jobs, other lanes cannot steal them, and a lane no worker reserves accumulates
  jobs indefinitely (a deliberate operator responsibility).

## Impact

- `worklane-core`: `NewJob` and `JobEnvelope` gain `lane: String`; both made
  `#[non_exhaustive]`. No change to the `Broker` trait signatures (`reserve`
  already takes `&str lane`).
- `worklane-memory`: `reserve` filters candidate jobs by lane; stored job state
  keys off the envelope's lane.
- `worklane`: `Client` gains a default lane + `with_lane`; `enqueue` sets it on
  `NewJob`. `DEFAULT_LANE` becomes the single shared default for both the enqueue
  and reserve sides.
- Docs/examples: README quick-start unaffected (default lane); no new deps.
