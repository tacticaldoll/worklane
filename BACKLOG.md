# Worklane Backlog

Future features intentionally **excluded from v0.1** unless absolutely necessary.
Active work and the MVP are tracked as OpenSpec changes under `openspec/changes/`;
this file is the upstream idea list that feeds `/opsx:propose`.

## Deferred (post-v0.1)

- Redis broker
- Postgres broker
- NATS / SQS backend
- cron / scheduled jobs
- priority queue
- result backend
- dashboard
- workflow chaining
- batch jobs
- rate limiting
- per-job concurrency limit
- job cancellation
- unique jobs / deduplication
- OpenTelemetry integration
- CLI management tool
- admin web UI
- distributed scheduler

### Lane follow-ups (after `add-lane-partitioning`)

First-class lane assignment ships in the `add-lane-partitioning` change; these
extensions are intentionally deferred:

- per-call lane override (e.g. `enqueue_to(lane, …)`); v0.1 sets the lane
  per-client via `Client::with_lane`
- expose the lane to handlers via `JobContext.lane`
- a `Lane` newtype with validation / interning (v0.1 uses a bare `String`)
- lane typo protection / registration — until then, jobs on a lane no worker
  reserves accumulate silently (deliberately an operator responsibility)
- multi-lane worker / fair scheduling across lanes

## Guiding principle

Protect the core loop. Everything above is out of scope until the core
enqueue → reserve → dispatch → ack / retry / fail / dead-letter loop is solid.
