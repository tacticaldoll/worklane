# worklane-pubsub

Topic-to-lane fan-out for [worklane].

A lightweight `Publisher` that maps a semantic topic (e.g. `"user.created"`) to
one or more worker `Lane`s and atomically fans a single payload out to all of
them. Opt-in: the core job loop has no exchange or routing model — this adds one
without changing core semantics.

## When to use it

You think in events/topics rather than lanes, and one event should enqueue the
same job onto several lanes (email + crm + analytics) in one call.

## How it plugs in

A thin wrapper around the facade `Client`. Register routes, then build and
publish typed jobs; publishing fans out to every lane on the topic.

```rust,ignore
use worklane::{Client, Lane};
use worklane_pubsub::Publisher;

let publisher = Publisher::new(client)
    .route("user.created", [Lane::try_from("email")?, Lane::try_from("crm")?]);

let ids = publisher
    .build_publish::<MyEvent>("user.created", payload)?
    .with_priority(5)
    .publish()
    .await?;   // Vec<JobId>, one per lane
```

A topic with no registered lanes publishes nothing and returns `Ok(vec![])`.

## Layer

Sits on the `worklane` facade: it composes `Client`/`JobBuilder` and the typed
`Job` trait. Application-level, above core.

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
