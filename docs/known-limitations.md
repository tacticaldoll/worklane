# Known Limitations

This page summarizes first-release support boundaries for adopters. The
authoritative lifecycle behavior remains in `openspec/specs/`.

## Broker Support Matrix

| Capability | Memory | SQLite | PostgreSQL | Redis |
| --- | --- | --- | --- | --- |
| Broker conformance suite | Yes | Yes | Yes | Yes |
| Durable across restart | No | Yes | Yes | Yes |
| Result store | In process | Yes | Yes | Yes |
| Dead-letter read / requeue | Yes | Yes | Yes | Yes |
| Scheduled enqueue store | Yes | Yes | Yes | Yes |
| CLI dead-letter workflows | No | Yes | Yes | Yes |
| External service required | No | No | Yes | Yes |
| Cluster / distributed store | N/A | N/A | Native DB | Single node |

## Choosing A Broker

- Use `worklane-memory` for examples, unit tests, and local development. It is
  not durable.
- Use `worklane-sqlite` when a local embedded database is enough and operational
  simplicity matters.
- Use `worklane-postgres` when PostgreSQL is already part of the service stack
  and concurrent workers need durable row-lock semantics.
- Use `worklane-redis` only with a single Redis node or primary / replica setup.
  Redis Cluster is not supported.

## Limitations And Handling

### At-Least-Once Delivery

Jobs may run more than once after lease expiry, process failure, timeout,
panic, or stale resolution. Handlers must be idempotent.

Handling: make side effects idempotent with application-level keys, database
constraints, or external idempotency tokens.

### Poll-Based Idle Load

worklane delivery is poll-based: a worker asks for work by calling
`Broker::reserve`, and when a lane is empty that call still costs a real query
or round-trip to the store. Idle workers therefore generate background load that
scales with worker count and poll rate. As a characterization of the per-call
cost, 16 consumers spinning `reserve` on an empty lane (single-node services on
localhost) sustain roughly 2,500 empty reserves/s on Postgres and roughly 96,000
on Redis — these are the raw round-trip rates, not the steady-state load of a
default worker (see Handling). This is the deliberate cost of not using a
push/notify delivery mechanism: worklane keeps `reserve` uniform across every
backend and avoids the commit serialization that a notify-based path (such as
Postgres `LISTEN`/`NOTIFY`) would reintroduce. Adding `LISTEN`/`NOTIFY` is a
non-goal for this reason, not an oversight.

Handling: `Worker::run` already paces idle polling. It waits `poll_interval`
(default 1s) between empty polls and applies exponential idle backoff up to
`idle_backoff_cap`, so a steady-state idle worker polls on the order of once per
second — far below the raw rates above. Tune with `Worker::with_poll_interval`
and `Worker::with_idle_backoff`, and reduce idle worker count or lengthen the
interval for cost-sensitive deployments.

### Blocking Handlers And Handler Timeout

Handlers must be **cooperatively async**. The worker runs each handler on its own
task, so a configured handler timeout fires independently of whether the handler
yields — it bounds even a non-yielding handler, failing/redelivering the job and
freeing its concurrency slot — **provided a worker thread is free to poll the
timeout**. What the timeout cannot do is *preempt* a handler that never yields:
the orphaned task keeps running until it yields or returns. And if non-yielding
handlers occupy every worker thread (or on a current-thread runtime), nothing is
left to poll the timeout until one frees.

Handling: run blocking or CPU-bound work off the async task with
`tokio::task::spawn_blocking` (or a dedicated thread) and `.await` its result.
That is the only way to fully remove the dependence on yielding — it keeps the
async worker threads free for the heartbeat, the timeout, and other jobs.

### Pre-1.0 API Evolution

This is a `0.x` baseline. Public types are designed for additive evolution, but
the `Broker` contract may still change before 1.0.

Handling: application code should use the `worklane` facade where possible.
Broker implementors should depend on `worklane-test` and expect some contract
movement before 1.0.

### Redis Cluster

`worklane-redis` is single-node only. Lua scripts touch coordinated keys that
are not declared as a complete Redis Cluster `KEYS[]` set, so Cluster rejects
the operation with `CROSSSLOT` instead of silently corrupting data.

Handling: run Redis as a single node or primary / replica deployment. Treat
Cluster support as a future data-model redesign, not a configuration switch.

### Redis Eviction

The Redis broker stores a job across multiple coordinated keys. An all-keys
eviction policy can remove one side of those relationships.

Handling: configure Redis so worklane keys are not evicted under memory
pressure, such as `maxmemory-policy noeviction`.

### Scheduler Missed Occurrences

`worklane-scheduler` does not backfill missed recurring occurrences while the
scheduler is down.

Handling: use `schedule_unique` for idempotent fires, run multiple scheduler
instances with identical schedule definitions for HA, and model catch-up work as
application jobs when catch-up is required.

### Metrics Export

`worklane-metrics` records through the `metrics` facade but does not install or
run an exporter.

Handling: install the application's exporter of choice, such as a Prometheus
recorder, and attach `MetricsObserver` to the worker.

### OpenTelemetry Scope

`worklane-otel` propagates trace context through job metadata. It does not
install a tracer provider or decide span naming policy for the application.

Handling: configure OpenTelemetry in the application and call the crate's
inject / extract helpers at enqueue and dispatch boundaries.

### Deferred Backends

NATS, SQS, and other backends are not part of the first release.

Handling: use the shipped brokers or implement a custom broker against
`worklane-core::Broker` and validate it with `worklane-test`.

### Benchmark Data

The first release includes a minimal in-memory core-loop benchmark, but not a
durable-broker benchmark matrix.

Handling: evaluate with the broker and workload shape that matches the service.
See [`benchmarks.md`](benchmarks.md) for the runnable benchmark entry point.
Future benchmark work should cover enqueue / reserve throughput, contention,
and latency under durable brokers.

### Live Service Tests

PostgreSQL and Redis live-service tests skip when their environment variables
are unset.

Handling: set `WORKLANE_POSTGRES_TEST_URL` and `WORKLANE_REDIS_TEST_URL` only
when the services are running. If the variables are set but unreachable, tests
fail.
