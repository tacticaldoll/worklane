//! Executable architectural governance for the worklane workspace.
//!
//! `AGENTS.md` states two load-bearing crate-graph invariants in prose — the
//! portability of `worklane-core` (the Broker design gate) and the
//! substitutability of the durable backends. This binary declares them as a
//! `modou` [`Constitution`] so CI reacts when the graph drifts, instead of the
//! rules living only as English a reviewer has to remember.
//!
//! ```text
//! cargo run -p worklane-governance -- check
//! ```
//!
//! Exit codes come from `modou`: `0` clean, `1` an enforced boundary was
//! breached, `2` a configuration error.

use modou::prelude::*;
use std::process::ExitCode;

/// Every crate in the worklane workspace.
///
/// New crates are added here so the `worklane-core` and backend boundaries below
/// automatically forbid depending on them without anyone editing the rules.
const WORKSPACE_CRATES: &[&str] = &[
    "worklane-core",
    "worklane",
    "worklane-memory",
    "worklane-sqlite",
    "worklane-postgres",
    "worklane-redis",
    "worklane-scheduler",
    "worklane-pubsub",
    "worklane-otel",
    "worklane-metrics",
    "worklane-cli",
    "worklane-test",
    "worklane-governance",
];

/// The internal crates `package` must not depend on: every workspace crate
/// except itself and those in `allowed`.
fn forbidden_internal(package: &str, allowed: &[&str]) -> Vec<String> {
    WORKSPACE_CRATES
        .iter()
        .filter(|c| **c != package && !allowed.contains(*c))
        .map(|c| (*c).to_string())
        .collect()
}

/// The worklane workspace constitution.
fn constitution() -> Constitution {
    // worklane-core is the portable contract root: traits, job model, envelope,
    // errors. If it depended on any other workspace crate the Broker contract
    // would stop being backend-agnostic. (AGENTS.md: Broker design gate /
    // Minimal contracts.)
    let mut c = Constitution::new("worklane").boundary(
        CrateBoundary::crate_("worklane-core")
            .forbid_dependency_on(forbidden_internal("worklane-core", &[]))
            .because(
                "worklane-core is the portable contract root; it must not depend \
                 on any other workspace crate",
            ),
    );

    // Durable backends stay interchangeable only if none reaches into another
    // backend or the facade — each depends on worklane-core alone. Depending on
    // worklane-test (the conformance suite, a dev-dependency) is allowed: it is
    // the mechanism that proves substitutability, not a breach of it.
    // (AGENTS.md: backends are interchangeable when they pass the same
    // behavioral conformance suite.)
    for backend in ["worklane-sqlite", "worklane-postgres", "worklane-redis"] {
        c = c.boundary(
            CrateBoundary::crate_(backend)
                .forbid_dependency_on(forbidden_internal(
                    backend,
                    &["worklane-core", "worklane-test"],
                ))
                .because(
                    "durable backends must stay substitutable: depend only on \
                     worklane-core, never on another backend or the facade",
                ),
        );
    }

    c
}

fn main() -> ExitCode {
    modou::run(&constitution(), std::env::args())
}
