## ADDED Requirements

### Requirement: Preserve the accepted governance projection

The governance runner SHALL project the same eight enforced crate boundaries
after upgrading Tianheng. Each boundary SHALL retain its kind, target, rule
parameters, severity, and complete accepted reason.

#### Scenario: Canonical JSON projection remains equivalent

- **WHEN** an operator runs `worklane-governance list --format json`
- **THEN** the projection contains exactly the accepted contract-root, broker,
  conformance-suite, governor, and facade boundaries
- **AND** every boundary retains its pre-upgrade rule parameters, `enforce`
  severity, and complete reason

#### Scenario: Projection formatting changes without semantic drift

- **WHEN** Tianheng changes JSON whitespace or boundary ordering
- **THEN** compatibility is determined from normalized boundary records
- **AND** the projection is accepted only if all eight semantic records remain
  equivalent

#### Scenario: Projection loses or changes accepted law

- **WHEN** the upgraded runner omits, adds, weakens, retargets, or changes the
  reason of an accepted boundary
- **THEN** verification fails
- **AND** the upgrade SHALL NOT amend project law to make the comparison pass

### Requirement: Preserve governance reaction classes

The governance runner SHALL preserve Tianheng's observable reaction classes:
clean or advisory results exit with code 0, enforced violations exit with code
1, and configuration, scan, usage, or harness failures exit with code 2.

#### Scenario: Governed workspace is clean

- **WHEN** the runner checks the conforming worklane workspace
- **THEN** it exits with code 0
- **AND** it reports no enforced violation

#### Scenario: Fixture workspace violates an enforced boundary

- **WHEN** the runner checks an isolated fixture in which a governed crate has
  a forbidden workspace dependency
- **THEN** it exits with code 1
- **AND** the reaction identifies the violated boundary

#### Scenario: Runner receives an invalid governance target

- **WHEN** the runner checks an isolated invalid manifest, configuration, or
  unsupported invocation
- **THEN** it exits with code 2
- **AND** the result is not reported as architectural drift

### Requirement: Preserve compatibility and scope

The upgrade SHALL retain the workspace Rust 1.85 MSRV and SHALL NOT change
product APIs, broker lifecycle behavior, accepted law, or governance
dependencies on workspace crates.

#### Scenario: Build with the declared minimum Rust version

- **WHEN** the workspace is checked with Rust 1.85 after dependency resolution
- **THEN** the governance runner and its selected Tianheng dependencies compile

#### Scenario: Governance runner dependency graph is checked

- **WHEN** the upgraded workspace dependency graph is inspected
- **THEN** `worklane-governance` depends on Tianheng
- **AND** it has no dependency on another workspace crate

#### Scenario: Upgrade would require broader product or law changes

- **WHEN** Tianheng 0.3 cannot preserve the current runner contract without
  changing a product API, broker behavior, or accepted boundary
- **THEN** implementation pauses
- **AND** the broader change is not included in this upgrade
