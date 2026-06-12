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
- multiple queues / lanes
- job cancellation
- unique jobs / deduplication
- lease receipt tokens (validate ack / retry / fail against the current reservation; needed for concurrent workers and durable brokers)
- OpenTelemetry integration
- CLI management tool
- admin web UI
- distributed scheduler

## Guiding principle

Protect the core loop. Everything above is out of scope until the core
enqueue → reserve → dispatch → ack / retry / fail / dead-letter loop is solid.
