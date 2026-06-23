## Why

`worklane` already has a verified job-lifecycle contract across first-party
brokers, but that contract is still easier for maintainers to use than for
external broker authors to implement. The 0.2.0 opportunity is to turn
conformance-verified behavior from an internal quality gate into a clear
extension story: a broker author should know which lifecycle surface is required,
which capabilities are optional, and how to prove compatibility.

## What Changes

- **BREAKING**: Split the current broker surface into a minimal lifecycle
  contract plus explicit optional capability traits, so implementations can
  support enqueue / reserve / resolve without carrying unrelated optional
  operations.
- Publish a broker-author SPI policy around `worklane_core::spi`, including
  which helpers are intended for backend authors and which public APIs remain
  application-facing.
- Make `worklane-test` usable as a broker-author conformance suite, with clear
  mandatory lifecycle scenarios and optional capability suites.
- Add a custom broker conformance guide that explains how to wire a private or
  third-party broker into the test suite and what passing means.
- Add lifecycle semantics documentation and a conformance matrix that make the
  verified behavior legible to users and operators.
- Keep dashboards, new durable backends, rate limiting, and workflow/saga
  primitives out of this change unless they are required to validate the broker
  extension contract.

## Capabilities

### New Capabilities

- `broker-extensibility`: Defines the public broker-author extension model,
  including the minimal lifecycle contract, optional capability boundaries, SPI
  policy, and conformance-suite expectations.

### Modified Capabilities

- `broker`: Narrows the shared broker contract into a lifecycle core plus
  optional capability traits while preserving the existing lifecycle semantics.
- `baseline-documentation`: Adds lifecycle semantics, custom broker
  conformance, and conformance matrix documentation as part of the stable
  project documentation set.

## Impact

- Affects `worklane-core` broker traits and SPI modules.
- Affects all first-party broker crates: `worklane-memory`, `worklane-sqlite`,
  `worklane-postgres`, and `worklane-redis`.
- Affects `worklane-test` by making conformance suites modular by lifecycle core
  and optional capability.
- Affects `worklane`, `worklane-scheduler`, `worklane-cli`, and any code that
  calls optional broker accessors directly.
- Requires migration notes because this is a pre-1.0 breaking API change for
  custom broker implementers and direct `Broker` trait users.
