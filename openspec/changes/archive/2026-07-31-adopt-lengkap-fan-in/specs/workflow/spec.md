## ADDED Requirements

### Requirement: Fan-in completion uses a sans-I/O decision boundary

The fan-in watcher SHALL map its existing persisted captures and newly observed
dependency states into a fixed ordered all-of assembly. The decision mechanism
SHALL own only monotonic capture and the `Pending`, `Ready`, or `Impossible`
decision. Worklane SHALL retain ownership of broker classification, result
reads, checkpoint serialization, generation limits, polling, error messages,
and callback enqueueing.

#### Scenario: Live dependency remains pending

- **WHEN** an unresolved dependency is classified `Live`
- **THEN** Worklane supplies no finding for its slot
- **THEN** a pending decision carries captured sibling values into the next
  watcher generation

#### Scenario: Completed dependency produces its result

- **WHEN** an unresolved dependency is classified `CompletedOrUnknown`
- **AND** its result bytes are present
- **THEN** Worklane supplies a produced finding for that dependency's slot
- **THEN** a ready decision returns all result bytes in dependency order

#### Scenario: Dead-lettered dependency is impossible

- **WHEN** an unresolved dependency is classified `DeadLettered`
- **THEN** Worklane supplies an impossible finding containing that dependency
  identity
- **THEN** Worklane fails the fan-in without enqueueing a callback or watcher

#### Scenario: Completed result missing before capture is impossible

- **WHEN** an unresolved dependency is classified `CompletedOrUnknown`
- **AND** its result bytes are absent
- **THEN** Worklane supplies an impossible missing-result finding
- **THEN** Worklane's failure names the affected dependency

#### Scenario: Stale result bytes belong to live work

- **WHEN** an unresolved dependency is classified `Live` while stale bytes
  exist in the result store
- **THEN** Worklane does not read or capture those bytes
- **THEN** the assembly remains pending for that slot

#### Scenario: Captured value survives checkpoint round trip

- **WHEN** a pending assembly is exported into the watcher payload and restored
  in a later generation
- **THEN** every captured value retains its original dependency slot
- **THEN** prior checkpoint tuple order is preserved
- **THEN** Worklane does not observe that dependency again

#### Scenario: Decision core remains free of Worklane effects

- **WHEN** the watcher observes dependencies and reacts to a decision
- **THEN** no broker, result store, payload, timer, async, or callback operation
  is delegated to the decision core
