# worklane-scheduler

Optional recurring (cron) scheduler for [worklane]. Enqueues a templated job
through any worklane broker every time a cron schedule becomes due.

**Layer:** ecosystem crate above `worklane-core`. Depends only on `worklane-core`
(+ `cron`/`chrono`/`chrono-tz`). Add it only when you need *recurring* enqueue;
one-shot delayed enqueue is already in the core client (`Client::enqueue_in`).

## Usage

```rust,ignore
use worklane_scheduler::Scheduler;

// `new` takes any broker that supports scheduling (Arc<dyn Broker>) and
// obtains its scheduled-store capability; it errors if the broker has none.
// Use `Scheduler::with_scheduled_store(store)` if you already hold the store.
let mut scheduler = Scheduler::new(broker)? // Arc<dyn Broker>
    .with_timezone(chrono_tz::America::New_York);

// cron: "sec min hour dom month dow [year]" — seconds first.
scheduler.schedule::<SendDigest>("daily-digest", "0 30 9 * * *", payload)?;

// Run until a shutdown future resolves.
scheduler.run(shutdown_signal).await?;
```

## High availability

Multiple instances coordinate via the atomic `ScheduledStore::enqueue_scheduled`, so each
occurrence fires at most once cluster-wide. **Every instance must define each
schedule with the same `(id, cron, timezone)`** — the dedup key is computed from
them; divergence double-fires. The scheduler cannot enforce this (it sees only
itself); it is an operator contract.

Cron is UTC by default; `with_timezone` interprets fields in an IANA zone
(DST-aware) for schedules registered after the call. Requires an epoch-based
`Clock` (`WallClock`, the default).

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
