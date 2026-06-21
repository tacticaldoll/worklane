# worklane-memory

In-memory `Broker` for [worklane] — for development, tests, and examples.

Jobs live in process memory behind a mutex. Reservation uses a visibility lease
(at-least-once delivery): a reserved job is hidden for the lease and becomes
visible again if not acked/retried/failed before it expires. Time comes from an
injected `Clock`, so tests can advance it deterministically.

## When to pick this broker

Use it for unit tests, examples, and local development. Nothing survives a
process restart — for durability use `worklane-sqlite`, `worklane-postgres`, or
`worklane-redis`, which expose the same `Broker` contract.

## Construction

```rust,ignore
use worklane_memory::InMemoryBroker;
use std::time::Duration;

let broker = InMemoryBroker::new()
    .with_lease(Duration::from_secs(30))
    .with_max_deliveries(5);
```

A `ManualClock` can be injected via `InMemoryBroker::with_clock(...)`.

## Features

- Implements the `worklane-core` `Broker` contract (and `ScheduledStore`).
- Lane partitioning, unique-key dedup, dead-letter store with `RetentionPolicy`.
- Poison-pill bound via `with_max_deliveries`.
- Zero heavy dependencies (only `worklane-core` + `async-trait`).

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
