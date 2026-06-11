# 1. Adopt OpenSpec for spec-driven development

Date: 2026-06-11

## Status

Accepted

## Context

worklane must stay portable across multiple AI coding agents, and the hard part
of the project is deciding job-lifecycle semantics, not writing Rust. We need a
single, version-controlled source of truth for those semantics that any agent
can read and follow.

## Decision

Use [OpenSpec](https://github.com/Fission-AI/OpenSpec) for spec-driven
development. `openspec/specs/` is the authoritative source of truth for system
behavior; `openspec/changes/` holds active change proposals. A committed,
agent-agnostic `AGENTS.md` is the entry-point meta-guideline that points every
agent to OpenSpec. Per-agent command shims (e.g. `.claude/`) are generated
per-clone and are not committed.

## Consequences

- Lifecycle semantics are written by hand as requirements/scenarios in
  `openspec/specs/`, not auto-generated.
- Contributors run `openspec init --tools <agent>` after cloning to get their
  own shims.
- Spec Kit's `SPEC.md` convention is superseded by `openspec/specs/`.
