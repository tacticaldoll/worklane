# Tianheng 0.1.10 Before-State

Recorded from:

```bash
cargo run -p worklane-governance -- list --format json
```

The canonical projection reported Constitution `worklane` with these normalized
enforced crate-boundary records:

- `worklane-core`, allowing no workspace dependencies:
  "worklane-core is the portable contract root; it must not depend on any other
  workspace crate".
- `worklane-memory`, `worklane-sqlite`, `worklane-postgres`, and
  `worklane-redis`, each allowing only `worklane-core`:
  "brokers must stay substitutable: depend only on worklane-core, never on
  another broker or the facade".
- `worklane-test`, allowing only `worklane-core`:
  "the conformance suite must assert only through the contract: depend on
  worklane-core alone, never on a concrete broker".
- `worklane-governance`, allowing no workspace dependencies:
  "the governance gate must stay independent of the graph it judges: depend
  only on tianheng, never on a workspace crate".
- `worklane`, allowing only `worklane-core`:
  "the facade stays broker-agnostic and thin: depend only on worklane-core among
  workspace crates; bring your own broker".

Every record had:

- kind `crate`;
- rule `restrict workspace dependencies to`;
- severity `enforce`; and
- no anchor or additional scan-depth parameter.
