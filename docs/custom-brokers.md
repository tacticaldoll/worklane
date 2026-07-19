# Custom Brokers

This guide is for broker authors implementing a private or third-party backend.
Application users should normally depend on the `worklane` facade and one broker
crate instead.

## Implement The Lifecycle First

Start with `worklane_core::Broker`. The core trait is only the job lifecycle:
enqueue, reserve, ack, retry, defer, extend, fail, and classify. A lifecycle-only
broker is valid and can run the mandatory conformance suite.

Optional operations live behind explicit capability traits:

- `BatchEnqueue`
- `DeadLetterStore`
- `QueueStats`
- `ScheduledStore`

Implement a capability only when the backend can provide the same observable
semantics as the first-party brokers. Then override the matching `Broker`
accessor to return `Some(self)` or `Some(Arc<dyn ScheduledStore>)`. Leaving the
default `None` makes the omission explicit.

Result storage uses `ResultStore` beside the broker. It is storage-adjacent and
is not reached through `Arc<dyn Broker>`.

## Broker SPI

`worklane_core::spi` is the broker-author helper surface. Use it for shared
decisions that all durable backends must make the same way:

- envelope encoding and decoding
- reservation receipt key encoding
- duration conversion to stored integer units
- stale-reservation error construction
- credential redaction
- backend name and lane-key validation helpers

SPI items are intentionally not re-exported from the `worklane` facade. If a
helper only serves one backend's local implementation convenience, keep it in
that backend crate instead of promoting it to SPI.

## Conformance Wiring

Add `worklane-test` as a dev-dependency and provide a harness:

```rust,ignore
use std::sync::Arc;
use worklane_test::{BrokerContractHarness, contract_tests};

struct MyHarness {
    broker: Arc<MyBroker>,
}

impl MyHarness {
    fn new() -> Self {
        Self { broker: Arc::new(MyBroker::new()) }
    }
}

#[async_trait::async_trait]
impl BrokerContractHarness for MyHarness {
    type Broker = MyBroker;

    fn broker(&self) -> Arc<MyBroker> {
        self.broker.clone()
    }
}

macro_rules! emit_lifecycle {
    ($($name:ident),* $(,)?) => {
        $(worklane_test::contract_tests!(MyHarness::new(); $name);)*
    };
}

worklane_test::for_each_lifecycle_scenario!(emit_lifecycle);
```

Run optional batteries only for capabilities your broker exposes:

```rust,ignore
worklane_test::for_each_batch_enqueue_scenario!(emit_lifecycle);
worklane_test::for_each_dead_letter_scenario!(emit_lifecycle);
worklane_test::for_each_queue_stats_scenario!(emit_lifecycle);
worklane_test::for_each_scheduled_scenario!(emit_lifecycle);
```

If a broker needs deterministic time, implement `TimedBrokerContractHarness` and
run `for_each_timed_scenario!`. If it supports configurable bounded redelivery
or dead-letter retention, implement `ConfigurableBrokerHarness` and run
`for_each_configured_scenario!`.

A broker that omits an optional capability should make that omission visible in
its test wiring:

```rust,ignore
worklane_test::omitted_capability_test!(
    scheduled_enqueue_omitted,
    "scheduled enqueue"
);
```

A failure in the shared suite means the broker behavior must be fixed. Do not
weaken the shared lifecycle contract to fit one implementation.

## Compatibility Claims

State exactly which suites pass. For example:

```text
passes lifecycle, timed, batch enqueue, dead-letter, queue stats
does not support scheduled enqueue
```

Passing the lifecycle suite does not imply optional capability support. Claiming
an optional capability without passing its suite is not a valid compatibility
claim.

## Migration From The Old Broker Trait

Before 0.2, direct implementers of `Broker` implemented `enqueue_batch` on the
core trait. In the split contract:

1. Remove `enqueue_batch` from the `Broker` impl.
2. Implement `BatchEnqueue` for the broker when atomic batch insertion is
   supported.
3. Override `Broker::batch_enqueue` to return `Some(self)`.
4. Update direct callers to request the capability and handle `None`.

Decorator brokers should forward optional capabilities deliberately. A decorator
that wants to preserve batch enqueue can implement `BatchEnqueue` itself and
return `Some(self)` only when its wrapped broker's `batch_enqueue()` accessor is
present.
