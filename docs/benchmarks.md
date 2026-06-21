# Benchmarks

The first release includes a minimal stable-Rust benchmark entry point for the
local in-memory core loop.

Run:

```sh
cargo run -p worklane --example core_loop_benchmark --release -- 10000 64
```

Arguments:

- `10000` is the number of jobs to enqueue and drain.
- `64` is worker concurrency.

Example output:

```text
jobs: 10000
concurrency: 64
enqueue: 12.3ms (813008 jobs/s)
drain: 34.5ms (289855 jobs/s)
total: 46.8ms (213675 jobs/s)
completed: 10000
```

This benchmark measures:

- typed payload serialization through `Client`
- in-memory broker enqueue and reserve
- worker dispatch, handler execution, ack, and idle drain
- bounded worker concurrency

It does not measure:

- SQLite, PostgreSQL, or Redis durability cost
- network latency
- result-store writes
- payload offload
- real handler work

Use it as a repeatable smoke benchmark and local comparison point. Durable
broker performance should be measured with the backing store, network, schema,
and workload shape used by the application.
