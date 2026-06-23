## 1. Public API + module rename (code)

- [x] 1.1 Rename the source module `crates/worklane/src/canvas.rs` →
      `workflow.rs`; update `mod canvas` → `mod workflow` and every
      `crate::canvas::` → `crate::workflow::` (lib.rs, client.rs, client_builder.rs).
- [x] 1.2 Rename public types: `Canvas`→`Workflow`, `ChordResults`→`FanInResults`,
      `ChordPolicy`→`FanInPolicy`, `ChordWatcherJob`→`FanInWatcherJob`,
      `ChordWatcherPayload`→`FanInWatcherPayload`; update the `worklane` facade
      `pub use` exports (keep the `#[doc(hidden)]` on the watcher types).
- [x] 1.3 Rename methods `Client::chord`→`Client::fan_in`,
      `Client::chord_with_policy`→`Client::fan_in_with_policy`; update all call
      sites/usages workspace-wide.
- [x] 1.4 Rename the private `chord_id` field → `fanin_id` and the
      `chord_results_payload` helper → `fan_in_results_payload`.
- [x] 1.5 Rename the canvas test files: `tests/canvas.rs`→`tests/workflow.rs`,
      `tests/chord_watcher.rs`→`tests/fan_in_watcher.rs` (and the Celery-vocabulary
      test/fn names inside them).

## 2. Internal references

- [x] 2.1 Rename unique-key prefixes: `chain:{id}:{kind}`→`sequence:{id}:{kind}`,
      `chord:{id}:callback`→`fanin:{id}:callback`, `cw:{id}:{gen}`→`fiw:{id}:{gen}`.
- [x] 2.2 Rename the durable watcher job kind `"worklane:chord_watcher"` →
      `"worklane:fan_in_watcher"` (note the in-flight caveat in design.md).
- [x] 2.3 Rename "chord"/"chain"/"group" wording in error strings, the
      `FanInPolicy.poll_delay_secs` message, and code comments to
      fan-in/sequence/fan-out; remove every `Celery`/"Celery-style topologies"
      mention from Rustdoc/comments.
- [x] 2.4 Update incidental references in `crates/worklane-test/src/scenarios/
      unique.rs` (the `chord:`-style example key) and the comment in
      `crates/worklane-redis/src/lib.rs`.

## 3. Specs

- [x] 3.1 Sync the terminology delta into `openspec/specs/workflow-canvas/spec.md`
      (restate every requirement in fan-in/fan-out vocabulary; behavior
      unchanged).
- [x] 3.2 Rename the capability: `git mv openspec/specs/workflow-canvas
      openspec/specs/workflow` and change the spec title to `# Workflow` (final
      post-sync step, per design.md).
- [x] 3.3 Fix incidental non-normative references in
      `openspec/specs/result-backend/spec.md`, `openspec/specs/broker/spec.md`
      (the `chord:`-style example key), and `openspec/specs/payload-store/spec.md`
      (the "fan-out or chord enqueue" line).

## 4. Docs / config

- [x] 4.1 Remove Celery from `README.md` (the "Celery, via Kombu" paragraph, the
      diagram caption, and "not a Celery clone") and `docs/architecture.md`; the
      technical points stand without it.
- [x] 4.2 Remove/neutralize the Celery line in `openspec/config.yaml` ("Inspired
      by Celery … do NOT treat it as a Celery clone") and rename "workflow canvas
      (chains / chords)" to fan-in/fan-out vocabulary.
- [x] 4.4 Update `AGENTS.md` (the "client, canvas" facade-module mention →
      "workflow").
- [x] 4.5 Fix any Celery/chord references in `BACKLOG.md`.

## 5. Version + changelog

- [x] 5.1 Bump the workspace version `0.1.0` → `0.2.0` in `Cargo.toml`
      (`[workspace.package]` and internal `[workspace.dependencies]` pins).
- [x] 5.2 Add a `CHANGELOG.md` 0.2.0 entry documenting the breaking rename with
      the migration map (do not rewrite older entries).

## 6. Boundaries (must hold)

- [x] 6.1 No behavior change: every `workflow` capability WHEN/THEN is preserved;
      the delta is vocabulary-only.
- [x] 6.2 No `Broker`/`worklane-core` change; the rename is confined to the
      `worklane` facade, specs, and docs.
- [x] 6.3 No perf/worker change is folded in.
- [x] 6.4 Live-surface grep gate: no stray `Celery`, `Chord`, `Canvas`, `chord`,
      `canvas`, or `chain:`/`cw:` prefix remains in code, specs, or active docs
      (excluding historical `CHANGELOG.md` entries, `docs/design/
      bench-apalis-handoff.md`, `openspec/changes/`, and the gitignored `bench/`).

## 7. Verification & close-out

- [x] 7.1 Definition of Done: `cargo build`, `cargo test`, `cargo clippy
      --all-targets -- -D warnings`, `cargo fmt --all --check` all pass.
- [x] 7.2 Full conformance: run the Postgres + Redis conformance suites against
      live containers (a rename must not alter broker behavior) plus the full
      `cargo test` workspace run.
- [x] 7.3 Update `BACKLOG.md` with the ✓ shipped status after archiving — N/A:
      this is a clean-room rename with no corresponding BACKLOG feature item to
      mark shipped.
