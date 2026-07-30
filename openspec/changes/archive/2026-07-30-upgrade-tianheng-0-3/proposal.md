## Why

The governance runner is pinned to Tianheng 0.1.10, while the current supported
governance workflow and sans-I/O observation profiles require Tianheng 0.3.x.
Upgrade the runner now so existing architectural law remains supported without
changing or expanding the accepted constitution.

## What Changes

- Upgrade the workspace Tianheng dependency from 0.1.10 to the compatible 0.3
  release.
- Adapt `worklane-governance` only where Tianheng 0.3 requires source-level
  compatibility changes.
- Prove that the canonical law projection still contains the same eight
  enforced crate boundaries with unchanged reasons and parameters.
- Prove that the governance check retains its clean, violation, and
  configuration-error reaction contract.
- Keep lifecycle decision-kernel extraction and adoption of Pacta, Shaahid,
  Suunta, or Cadw out of scope.

## Capabilities

### New Capabilities

- `governance-runner-compatibility`: Defines the observable projection and
  reaction behavior the workspace governance runner must preserve across
  Tianheng upgrades.

### Modified Capabilities

None.

## Impact

The workspace dependency declaration, lockfile, and possibly
`crates/worklane-governance` compatibility code are affected. No product API,
broker lifecycle behavior, accepted boundary, baseline, or minimum supported
Rust version changes.
