# worklane-redis

Single-node Redis-backed durable `Broker` for [worklane] — production services.

Redis has no row locks or conditional multi-statement transactions, so atomic
reserve/resolve is built on **Lua scripts**: Redis runs each to completion
single-threaded, giving no-double-hand-out and the receipt guards. Jobs are
stored across coordinated keys (lane ZSETs, a job HASH, receipt/unique indexes,
dead-letter indexes) under a configurable namespace.

## When to pick this broker

Pick it when you already run a single Redis node (or a primary + replicas) and
want durable jobs with low-latency reservation. **Single-node only — not Redis
Cluster** (a lifecycle op touches multiple keys that would hash to different
slots → `CROSSSLOT`). Configure `maxmemory-policy noeviction` so worklane keys
are never evicted. For clustered/HA-sharded needs, use `worklane-postgres`.

## Construction

```rust,ignore
use worklane_redis::RedisBroker;

let broker = RedisBroker::connect("redis://host:6379").await?;
let broker = RedisBroker::connect_with_namespace(&url, "worklane").await?;
let broker = broker.with_lease(/* Duration */).with_max_deliveries(5);
```

## Features

- Implements the `worklane-core` `Broker` contract (and `ScheduledStore`).
- Durable `RedisResultStore` via `broker.result_store()`, with optional TTL.
- Lane/schedule-id key-safety validation; opaque unique keys allowed.
- Pre-1.0 layout baseline (drain the namespace before upgrading).

## Connection security

Plaintext by default. Enable the `tls` feature for rustls support, then connect
with a `rediss://` URL:

```toml
worklane-redis = { version = "0.1", features = ["tls"] }
```

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
