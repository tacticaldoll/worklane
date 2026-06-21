# worklane-sqlite

SQLite-backed durable `Broker` for [worklane] — embedded and small services.

Jobs persist in SQLite (file or in-memory) as an opaque `JobEnvelope` blob plus
denormalized index columns. Reservation uses a visibility lease (at-least-once).
Synchronous `rusqlite` calls run on Tokio's blocking pool; a file broker holds an
`r2d2` WAL pool (concurrent readers, one writer), an in-memory broker a single
mutex-guarded connection.

## When to pick this broker

Pick it for a single service that wants durability without running a server:
desktop apps, edge, small backends. For heavy write concurrency or multiple
processes, prefer `worklane-postgres`. The in-memory `worklane-memory` broker is
better for tests.

## Construction

```rust,ignore
use worklane_sqlite::SqliteBroker;

let broker = SqliteBroker::open("jobs.db")?;          // file (WAL pool)
let broker = SqliteBroker::open_in_memory()?;          // private in-memory
let broker = broker.with_lease(/* Duration */).with_max_deliveries(5);
```

## Features

- Implements the `worklane-core` `Broker` contract.
- Transactional Outbox: `enqueue_with_tx(&Transaction, job)` commits a job
  atomically with your business write (re-exports `rusqlite`).
- Durable `SqliteResultStore` via `broker.result_store()`.
- Pre-1.0 schema is a frozen baseline (no in-place migration; drop & recreate).

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
