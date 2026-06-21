# worklane-test

Reusable **broker conformance suite** for [worklane]. A third-party broker author
adds this as a `dev-dependency` and runs the same contract tests the first-party
brokers (SQLite, Postgres, Redis) must pass.

**Layer:** test-support crate above `worklane-core` (its only dependency besides
`async-trait`/`tokio`). Runtime application code must not depend on it.

## Usage

Implement the harness adapter for your broker, then let the single-source drivers
generate the tests:

```rust,ignore
use std::sync::Arc;
use worklane_test::{BrokerContractHarness, contract_tests, for_each_required_scenario};

struct MyHarness { broker: Arc<MyBroker> }

#[async_trait::async_trait]
impl BrokerContractHarness for MyHarness {
    type Broker = MyBroker;
    fn broker(&self) -> Arc<MyBroker> { self.broker.clone() }
    // Override dead_letters / scheduled_store only if your broker exposes them.
}

// Emit one #[tokio::test] per scenario.
macro_rules! emit { ($($n:ident),* $(,)?) =>
    { $(worklane_test::contract_tests!(MyHarness::new(); $n);)* } }
for_each_required_scenario!(emit);
```

Three tiers: `for_each_required_scenario!` (every broker),
`for_each_timed_scenario!` (brokers with an advanceable clock — also implement
`TimedBrokerContractHarness`), and `for_each_configured_scenario!`
(poison/retention — implement `ConfigurableBrokerHarness`). Result stores use
`result_store_contract!`. `ManualClock` is re-exported for timed tiers.

## Stability

The harness traits and the `for_each_*` drivers are the stable surface. The
individual `scenarios::*` function paths are public so the macros can name them;
new scenarios may be added between minor versions.

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
