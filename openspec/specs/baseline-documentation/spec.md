# baseline-documentation Specification

## Purpose

Define how the repository presents the current system as a clean baseline.
Durable behavior belongs in `openspec/specs/`; stable architecture and operator
guidance belong in current project documentation; deferred work belongs in
`BACKLOG.md`.

## Requirements

### Requirement: Clean baseline documentation

The repository SHALL present the current system as a clean baseline. Durable
behavioral truth SHALL live in `openspec/specs/`; stable architecture and
operator guidance SHALL live in current project documentation; deferred work
SHALL live in `BACKLOG.md`. Descriptive decision-record history SHALL NOT be
shipped as part of the baseline documentation set.

#### Scenario: Behavioral truth lives in specs

- **WHEN** a reader needs the current behavior of a public capability
- **THEN** the relevant file under `openspec/specs/` SHALL describe the behavior
- **AND** the reader SHALL NOT need decision-record history to understand the
  contract

#### Scenario: Operator guidance is current

- **WHEN** a backend has a required deployment assumption
- **THEN** stable project documentation or code docs SHALL state the assumption
- **AND** the statement SHALL be understandable without external decision
  records

#### Scenario: Decision-record history is absent from baseline

- **WHEN** the baseline documentation is inspected
- **THEN** descriptive decision-record files SHALL NOT be present
- **AND** no stable document SHALL require a decision-record file to understand
  current behavior

### Requirement: Historical language is removed from docs

Documentation and code docs SHALL describe current behavior directly. They MUST
NOT rely on timeline narration or decision-record numbering, unless that wording
is part of an external citation or compatibility notice required by a public
API.

#### Scenario: Public docs describe current behavior

- **WHEN** README, architecture docs, crate docs, and public API docs are read
- **THEN** they SHALL describe what the system does now
- **AND** they SHALL NOT explain behavior primarily by contrasting it with an
  earlier implementation

#### Scenario: Deferred work remains in backlog

- **WHEN** a document mentions future work or an intentionally omitted feature
- **THEN** that information SHALL be recorded in `BACKLOG.md`
- **AND** it SHALL NOT be kept as decision-record narrative

### Requirement: Baseline Metadata Is Current

The repository SHALL keep metadata, README tables, architecture docs, and code
docs current with the clean baseline. They MUST NOT present current behavior as
an upgrade from an earlier unreleased version.

#### Scenario: OpenSpec context is not historical

- **WHEN** OpenSpec project context is read
- **THEN** it SHALL describe the current system scope without obsolete version
  timeline notes

#### Scenario: Crate inventory includes shipped crates

- **WHEN** the README or architecture docs list workspace crates
- **THEN** shipped crates such as metrics, payload storage, durable brokers, CLI,
  scheduler, pub/sub, and telemetry SHALL be represented accurately

#### Scenario: Public API docs contain no baseline deprecations

- **WHEN** public code docs are built with warnings denied
- **THEN** no API SHALL be deprecated solely as a pre-initial-baseline migration
  path

### Requirement: Verified Release Packaging

The repository SHALL treat verified Cargo package creation as a release
readiness gate for every publishable workspace crate. The gate MUST run
`cargo package --workspace` without `--no-verify`, so Cargo assembles each
package, rewrites path dependencies as registry dependencies, unpacks the
tarball, and compiles it as a downstream registry consumer would.

The CI package gate MUST NOT rely on `cargo package --workspace --no-verify` as
the only release packaging check. Workspace crate dependency versions MUST be
chosen so same-release packages resolve to compatible APIs during package
verification. The CI package gate MUST use an isolated package target directory
or another clean verification environment so stale local package artifacts
cannot make the gate pass or fail incorrectly.

#### Scenario: Package verification compiles all published crates

- **WHEN** the release packaging gate runs from the workspace root
- **THEN** `cargo package --workspace` SHALL complete successfully
- **AND** every publishable workspace crate SHALL be verified from its packaged
  tarball

#### Scenario: No-verify packaging is insufficient

- **WHEN** CI checks release packaging for the baseline
- **THEN** the package job SHALL NOT use `--no-verify` as its only packaging
  command
- **AND** a package that assembles but fails tarball compilation SHALL fail CI

#### Scenario: Registry-style dependencies remain compatible

- **WHEN** Cargo verifies a packaged workspace crate after rewriting path
  dependencies to registry dependencies
- **THEN** the resolved same-release workspace dependencies SHALL expose the APIs
  required by the packaged crate
- **AND** verification SHALL NOT depend on unpublished path-only behavior

#### Scenario: Stale package artifacts do not affect CI

- **WHEN** CI runs the package verification gate
- **THEN** it SHALL verify packages in an isolated package target directory or
  equivalent clean environment
- **AND** stale artifacts from prior packaging attempts SHALL NOT determine the
  result

### Requirement: Warning-Free Public Rustdoc

The repository SHALL keep public Rust documentation warning-free for the
published workspace crates. CI MUST run
`RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps`
and the same command with `--all-features` so broken, ambiguous, private,
undocumented, or otherwise warning-producing rustdoc output fails before
release.

Public documentation MUST link only to resolvable public items or use plain text
for implementation details that are not part of the public API.

#### Scenario: Public documentation builds cleanly

- **WHEN** CI builds public Rust documentation for the workspace
- **THEN** `RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace
  --no-deps` SHALL complete successfully
- **AND** `RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace
  --no-deps --all-features` SHALL complete successfully
- **AND** docs.rs-facing documentation SHALL NOT contain rustdoc warnings

#### Scenario: Broken links fail CI

- **WHEN** a public doc comment links to an unresolved item
- **THEN** the documentation gate SHALL fail
- **AND** the item SHALL be fixed by linking to a resolvable public path or by
  making the reference plain text

#### Scenario: Private implementation details are not public links

- **WHEN** a public doc comment mentions a private implementation detail
- **THEN** it SHALL NOT link to a private item
- **AND** rustdoc private-intra-doc-link warnings SHALL fail the documentation
  gate

#### Scenario: Ambiguous links are disambiguated

- **WHEN** a public doc link could resolve to more than one item kind
- **THEN** the link SHALL use rustdoc disambiguation such as `enum@` or another
  explicit public path
- **AND** rustdoc ambiguity warnings SHALL fail the documentation gate

### Requirement: Warning-Free Lints

The repository SHALL keep workspace Rust lints warning-free in CI. CI MUST run
clippy for default features and all features so optional public surfaces are not
allowed to drift outside lint coverage before release.

#### Scenario: Default-feature linting is clean

- **WHEN** CI runs the lint gate
- **THEN** `cargo clippy --workspace --all-targets` SHALL complete successfully

#### Scenario: All-feature linting is clean

- **WHEN** CI runs the lint gate
- **THEN** `cargo clippy --workspace --all-targets --all-features` SHALL
  complete successfully

### Requirement: Verified MSRV

The repository SHALL validate its declared minimum supported Rust version
(MSRV) in CI. Because workspace crates declare `rust-version = "1.85"`, CI MUST
include a Rust 1.85.0 job that compiles the workspace targets before the
baseline is treated as release-ready.

The MSRV gate MUST be separate from latest-stable quality gates so a newer
stable compiler cannot mask accidental use of language features or dependency
APIs unavailable on the declared MSRV.

#### Scenario: Declared MSRV compiles workspace targets

- **WHEN** CI runs the MSRV gate
- **THEN** it SHALL use Rust 1.85.0
- **AND** `cargo check --workspace --all-targets` SHALL complete successfully
- **AND** `cargo check --workspace --all-targets --all-features` SHALL complete
  successfully

#### Scenario: Latest stable does not substitute for MSRV

- **WHEN** latest-stable build, test, clippy, or docs jobs pass
- **THEN** the release readiness decision SHALL still require the Rust 1.85.0
  MSRV job to pass

#### Scenario: MSRV drift fails visibly

- **WHEN** code or dependency usage requires a compiler newer than Rust 1.85.0
- **THEN** the MSRV gate SHALL fail
- **AND** the project SHALL either restore compatibility or deliberately update
  the declared MSRV in a separate change

### Requirement: Public Release Support Files

The repository SHALL include public release support files before the baseline is
published to crates.io. These files MUST give adopters, vulnerability reporters,
and contributors enough current information to evaluate and interact with the
project without relying on private context.

The first public changelog entry MUST list the shipped `0.1.0` capabilities and
compatibility notes directly. It MUST NOT delegate the feature set entirely to
the README.

The repository MUST include a security policy with a vulnerability reporting
path, a contribution guide aligned with OpenSpec and the Definition of Done, a
code of conduct, and GitHub issue / pull request templates.

#### Scenario: Changelog describes the first release

- **WHEN** a reader opens `CHANGELOG.md` for version `0.1.0`
- **THEN** the entry SHALL list the major shipped capabilities directly
- **AND** it SHALL include compatibility or pre-1.0 stability notes relevant to
  adopters

#### Scenario: Vulnerability reporters have a path

- **WHEN** a security researcher wants to report a vulnerability
- **THEN** `SECURITY.md` SHALL describe the supported versions and reporting
  path
- **AND** it SHALL tell reporters not to disclose exploit details in a public
  issue

#### Scenario: Contributors can prepare a valid change

- **WHEN** a contributor wants to propose or implement a change
- **THEN** `CONTRIBUTING.md` SHALL point to OpenSpec and the repository
  Definition of Done
- **AND** the pull request template SHALL ask for the related OpenSpec change
  and verification commands

#### Scenario: Issues capture release-relevant context

- **WHEN** a user opens a bug report or feature request
- **THEN** the issue templates SHALL request enough environment, broker,
  reproduction, and contract-impact context for maintainers to triage it

### Requirement: Published Crate Audience Positioning

Each publishable workspace crate SHALL identify its intended audience and role
from the package metadata or crate-level documentation. A reader landing on an
individual crates.io or docs.rs page MUST be able to tell whether the crate is
for application runtime use, broker implementation, optional integration,
operator tooling, shared contracts, or conformance testing.

`worklane-test` MUST be positioned as a reusable conformance suite for broker
implementors, not as a normal application runtime dependency.

#### Scenario: Application users choose the facade and broker crates

- **WHEN** an application developer reads the package descriptions or crate docs
- **THEN** `worklane` SHALL be identifiable as the facade crate
- **AND** broker crates SHALL be identifiable as the direct dependencies for
  their backing stores

#### Scenario: Extension crates are opt-in

- **WHEN** a reader evaluates scheduler, pub/sub, telemetry, or metrics crates
- **THEN** the docs or metadata SHALL identify them as optional integrations or
  layers
- **AND** the reader SHALL NOT need to depend on them for the core job loop

#### Scenario: Broker authors understand worklane-test

- **WHEN** a broker implementor reads `worklane-test`
- **THEN** the crate SHALL be described as a dev-dependency conformance suite
- **AND** it SHALL state that it is used to prove `Broker` contract compliance

### Requirement: First Release Checklist

The repository SHALL include a release checklist before publishing the baseline
workspace crates to crates.io. The checklist MUST document external blockers,
local gates, dry-run steps, publish order, and post-publish verification.

The checklist MUST treat crates.io name availability as a hard pre-publish
blocker because crate names cannot be reclaimed by this repository after another
publisher owns them.

#### Scenario: Release operator checks external blockers

- **WHEN** a maintainer prepares the first crates.io release
- **THEN** the checklist SHALL require checking all `worklane` and `worklane-*`
  crate names on crates.io before publishing
- **AND** it SHALL identify name ownership as a hard blocker

#### Scenario: Publish order is explicit

- **WHEN** a maintainer publishes workspace crates
- **THEN** the checklist SHALL list a dependency-safe publish order
- **AND** it SHALL account for crates that are needed by package verification or
  downstream workspace crates

#### Scenario: Release gates are command-oriented

- **WHEN** a maintainer follows the checklist
- **THEN** it SHALL include the concrete local verification commands
- **AND** it SHALL include dry-run and post-publish verification steps

### Requirement: Known Limitations And Support Matrix

The repository SHALL document first-release support boundaries in a public
document linked from the README. The document MUST include a backend / feature
support matrix and known limitations with practical handling guidance where
available.

The limitations document MUST NOT contradict the authoritative behavior specs in
`openspec/specs/`. Detailed lifecycle semantics SHALL remain in OpenSpec; the
limitations document SHALL summarize release-facing adoption boundaries.

#### Scenario: Adopters can compare brokers

- **WHEN** a reader evaluates which broker crate to use
- **THEN** the limitations document SHALL include a matrix that compares the
  first-party brokers across shipped capabilities
- **AND** the matrix SHALL identify notable operational constraints

#### Scenario: Known limitations are visible

- **WHEN** a reader evaluates production readiness
- **THEN** the limitations document SHALL list known release boundaries such as
  at-least-once delivery, Redis Cluster support, pre-1.0 API evolution, and
  deferred backends or benchmark data
- **AND** each limitation SHALL include handling guidance when available

#### Scenario: README links the limitations

- **WHEN** a reader starts from the README
- **THEN** the reader SHALL be able to reach the known-limitations document
  directly

### Requirement: Minimal Benchmark Entry Point

The repository SHALL include a minimal, repeatable benchmark entry point for
the first release. The benchmark MUST be runnable on stable Rust without adding
new release-risk dependencies, and it MUST document what it measures and what it
does not measure.

#### Scenario: Maintainer runs the core-loop benchmark

- **WHEN** a maintainer runs the documented benchmark command
- **THEN** it SHALL enqueue and drain a configurable number of typed jobs
- **AND** it SHALL print throughput and completion-count output

#### Scenario: Benchmark scope is explicit

- **WHEN** a reader evaluates the benchmark documentation
- **THEN** the document SHALL state that the benchmark measures the local
  in-memory core loop
- **AND** it SHALL not imply durable broker performance guarantees

### Requirement: Public API Documentation And Unsafe Policy

Published workspace crates SHALL make public API documentation coverage and
unsafe-code policy explicit before release. Library crate roots MUST warn on
missing public docs and forbid unsafe code. Public items exposed by binary crate
documentation SHOULD be documented well enough for rustdoc missing-doc probes to
pass.

Release verification MUST include a rustdoc missing-doc probe so newly exposed
public items without docs are visible before publishing.

#### Scenario: Missing public docs are visible

- **WHEN** rustdoc is run with missing-doc warnings denied for the workspace
- **THEN** published crates SHALL document their public API items sufficiently
  for the command to complete
- **AND** newly exposed undocumented public items SHALL fail the probe

#### Scenario: Unsafe code is forbidden

- **WHEN** workspace crates are compiled
- **THEN** crate roots SHALL forbid unsafe code
- **AND** accidental unsafe blocks SHALL fail compilation

### Requirement: Large Module Responsibilities Are Split

Large modules SHALL be split by current responsibility while preserving public
API paths. The split MUST be mechanical from a caller perspective and MUST NOT
change documented behavior.

#### Scenario: Facade module split preserves callers

- **WHEN** downstream code imports public `worklane` facade types
- **THEN** the imports SHALL continue to compile after client and worker internals
  are split into focused modules

#### Scenario: Durable broker split preserves behavior

- **WHEN** SQLite, Postgres, and Redis conformance tests run after module splits
- **THEN** the brokers SHALL preserve the same public behavior

### Requirement: Lifecycle semantics guide

The repository SHALL provide stable documentation that summarizes the verified
job lifecycle semantics for users and operators. The guide SHALL cover enqueue,
delayed visibility, reserve, lease expiry, stale resolution, ack, retry, defer,
extend, fail, dead-lettering, uniqueness, scheduling, and at-least-once
execution. The guide SHALL link to OpenSpec as the source of truth and SHALL NOT
replace the normative specs.

#### Scenario: Reader needs lifecycle behavior

- **WHEN** a reader wants to understand how worklane handles reservation,
  retry, failure, or dead-lettering
- **THEN** stable project documentation SHALL provide a lifecycle semantics
  guide
- **AND** the guide SHALL link to the relevant OpenSpec capabilities

#### Scenario: Guide avoids a second contract

- **WHEN** lifecycle behavior is described in the guide
- **THEN** it SHALL be presented as a readable summary of OpenSpec requirements
- **AND** it SHALL NOT introduce behavior that is absent from the specs

#### Scenario: At-least-once boundary is documented

- **WHEN** the guide describes delivery guarantees
- **THEN** it SHALL state that execution is at-least-once
- **AND** it SHALL state that handlers must be idempotent

### Requirement: Broker conformance matrix

The repository SHALL provide a conformance matrix that distinguishes the
mandatory lifecycle suite from optional capability suites for each supported
broker. The matrix SHALL identify which suites pass for in-memory, SQLite,
PostgreSQL, and Redis brokers.

#### Scenario: Supported broker matrix is read

- **WHEN** a user reads the conformance matrix
- **THEN** they SHALL be able to identify which lifecycle and optional
  capability suites each supported broker passes
- **AND** optional capability support SHALL be distinguishable from lifecycle
  support

#### Scenario: Optional capability is absent

- **WHEN** a broker does not support an optional capability
- **THEN** the matrix SHALL mark that capability as absent or not applicable
- **AND** it SHALL NOT imply that passing the lifecycle suite provides that
  capability

#### Scenario: Matrix is updated after conformance changes

- **WHEN** a broker gains or loses a conformance-tested capability
- **THEN** the conformance matrix SHALL be updated in the same change

### Requirement: Custom broker guide is stable documentation

The repository SHALL provide stable documentation for custom broker authors that
explains the broker SPI, conformance-suite wiring, compatibility claims, and
common portability pitfalls. Deferred or speculative backend work SHALL remain
in `BACKLOG.md`.

#### Scenario: Broker author looks for extension docs

- **WHEN** a reader wants to implement a custom broker
- **THEN** stable documentation SHALL direct them to the SPI and conformance
  guide
- **AND** they SHALL NOT need to copy implementation details from a first-party
  broker crate to understand the supported extension path

#### Scenario: Speculative backend is mentioned

- **WHEN** documentation mentions a backend that is not implemented
- **THEN** that backend SHALL remain documented as deferred work in `BACKLOG.md`
- **AND** stable custom-broker documentation SHALL focus on the extension
  contract rather than promising that backend
