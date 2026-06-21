# worklane-postgres

PostgreSQL-backed durable `Broker` for [worklane] — production services.

Jobs persist in Postgres as an opaque `JobEnvelope` blob plus denormalized index
columns, like the SQLite broker. The difference is concurrency: a
`deadpool-postgres` pool plus `SELECT … FOR UPDATE SKIP LOCKED` in `reserve` lets
many reservers grab distinct jobs without blocking — the contract's
no-double-hand-out guarantee under real connection concurrency.

## When to pick this broker

Pick it when you already run Postgres and need durable jobs across restarts with
high write/worker concurrency or multiple processes. For embedded single-process
use, `worklane-sqlite` is lighter; for tests, `worklane-memory`.

## Construction

```rust,ignore
use worklane_postgres::PostgresBroker;

let broker = PostgresBroker::connect("postgres://user:pw@host/db").await?;
let broker = PostgresBroker::connect_with_schema(&url, "worklane").await?;
let broker = broker.with_lease(/* Duration */).with_max_deliveries(5);
```

Tables live in a configurable schema (default `public`), so isolated brokers can
share one database.

## Features

- Implements the `worklane-core` `Broker` contract.
- Transactional Outbox: `enqueue_with_tx(&Transaction, job)` (re-exports
  `tokio_postgres`); `READ COMMITTED` isolation pinned for dedup correctness.
- Durable `PostgresResultStore` via `broker.result_store()`.
- Pre-1.0 schema baseline (no in-place migration; drop & recreate).

## Connection security

Plaintext by default. Enable the `tls` feature for rustls-encrypted connections
(system root certificates), then connect with `PostgresBroker::connect_tls(url)`:

```toml
worklane-postgres = { version = "0.1", features = ["tls"] }
```

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
