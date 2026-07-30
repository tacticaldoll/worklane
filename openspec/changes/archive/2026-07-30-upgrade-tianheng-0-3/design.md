## Context

`worklane-governance` is a dependency-independent binary that declares eight
accepted crate-graph boundaries through Tianheng. The workspace currently uses
Tianheng 0.1.10, while the current governance workflow supports Tianheng 0.3.x.
The upgrade must preserve the accepted Constitution, its reasons, and the CLI
reaction contract. This is dependency maintenance, not authority to introduce
new architectural law.

The before-state is the canonical output of:

```bash
cargo run -p worklane-governance -- list --format json
```

It contains eight enforced crate boundaries: the contract root, four brokers,
the conformance suite, the governance runner, and the facade.

## Goals / Non-Goals

**Goals:**

- Move the workspace dependency to Tianheng 0.3.
- Preserve all eight accepted boundaries exactly in the canonical JSON
  projection.
- Preserve clean, violation, and invalid-configuration exit classifications.
- Keep the workspace MSRV at Rust 1.85.
- Pass the complete repository Definition of Done.

**Non-Goals:**

- Adding, removing, weakening, or retargeting Constitution boundaries.
- Adding a sans-I/O purity boundary before a concrete pure decision component
  exists.
- Extracting a broker lifecycle state machine.
- Adopting Pacta, Shaahid, Suunta, or Cadw.
- Changing any public API or broker lifecycle behavior.

## Decisions

### Pin the workspace to the Tianheng 0.3 compatibility line

Set the workspace dependency to `0.3.0` and let Cargo select compatible patch
releases. Tianheng 0.3 retains Rust 1.85 compatibility, so the upgrade does not
require an MSRV change.

An exact version pin was rejected because this workspace convention uses Cargo
compatibility requirements and patch releases may contain compatible fixes.
Retaining 0.1.10 was rejected because it leaves the project outside the
supported activation workflow.

### Treat the canonical projection as the compatibility oracle

Capture the normalized before-state projection, upgrade the dependency, and
compare the after-state boundary records by kind, target, rule, parameters,
severity, and complete reason. Ordering and JSON whitespace are not themselves
law, but all eight semantic records are.

Relying only on compilation was rejected because the API can compile while
projection semantics drift. Editing `AGENTS.md` or Constitution reasons to
match changed output was rejected because this change has no law-amendment
authority.

### Keep compatibility edits inside the governance runner

If Tianheng 0.3 changes source APIs, make the smallest adaptation in
`crates/worklane-governance`. Do not add workspace dependencies, change target
sets, or introduce new observation dimensions.

Creating a new shared governance abstraction was rejected because there is one
consumer and no repeated local pattern to justify it.

### Verify reaction classes without changing accepted law

Run the project-native clean check against the workspace. Exercise enforced
violation and invalid-configuration behavior through isolated fixture
workspaces or existing Tianheng-compatible test seams; do not temporarily
weaken or rewrite the accepted Constitution.

Treating upstream exit-code documentation alone as sufficient was rejected
because the adopter runner and argument forwarding are part of the local
integration.

## Risks / Trade-offs

- **Tianheng 0.3 source compatibility differs from 0.1.10** → Limit edits to
  mechanical runner adaptation and compare the complete canonical projection.
- **A transitive dependency raises the effective MSRV** → Run the Rust 1.85
  compatibility check in addition to the normal Definition of Done.
- **Fixture checks accidentally become alternate law sources** → Use fixtures
  only to observe reaction classes; keep the real Constitution as the sole
  projection authority.
- **New 0.3 capabilities invite speculative governance** → Add no new
  boundary, baseline, semantic scan, runtime probe, or sans-I/O profile in this
  change.

## Migration Plan

1. Record the Tianheng 0.1.10 canonical JSON projection.
2. Update the workspace dependency and lockfile to Tianheng 0.3.
3. Apply the smallest runner compatibility edits, if required.
4. Compare the after-state projection and execute reaction-class checks.
5. Run the repository Definition of Done and Rust 1.85 compatibility check.

Rollback consists of restoring the dependency declaration, lockfile, and any
mechanical runner adaptation. There is no persisted data or public API
migration.

## Open Questions

None.
