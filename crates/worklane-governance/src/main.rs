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

/// The worklane workspace constitution.
///
/// Both boundaries govern *workspace* dependencies, whose membership `modou`
/// derives from `cargo metadata`. A newly added workspace crate is therefore
/// governed by default — there is no hand-maintained crate list to forget to
/// update.
fn constitution() -> Constitution {
    Constitution::new("worklane")
        // worklane-core is the portable contract root: traits, job model,
        // envelope, errors. If it depended on any other workspace crate the
        // Broker contract would stop being backend-agnostic. (AGENTS.md: Broker
        // design gate / Minimal contracts.)
        .boundary(
            CrateBoundary::crate_("worklane-core")
                .forbid_all_workspace_dependencies()
                .because(
                    "worklane-core is the portable contract root; it must not \
                     depend on any other workspace crate",
                ),
        )
        // Durable backends stay interchangeable only if none reaches into
        // another backend or the facade — each may depend on worklane-core
        // alone among workspace crates.
        .boundary(backend_boundary("worklane-sqlite"))
        .boundary(backend_boundary("worklane-postgres"))
        .boundary(backend_boundary("worklane-redis"))
}

/// A durable backend may depend on only `worklane-core` among workspace crates.
///
/// The rule governs normal `[dependencies]` only, so the dev-dependency on
/// `worklane-test` — the conformance suite that *proves* substitutability — is
/// allowed without being listed: it is the mechanism, not a breach. (AGENTS.md:
/// backends are interchangeable when they pass the same behavioral conformance
/// suite.)
fn backend_boundary(backend: &str) -> CrateBoundary {
    CrateBoundary::crate_(backend)
        .restrict_workspace_dependencies_to(["worklane-core"])
        .because(
            "durable backends must stay substitutable: depend only on \
             worklane-core, never on another backend or the facade",
        )
}

fn main() -> ExitCode {
    modou::run(&constitution(), std::env::args())
}
