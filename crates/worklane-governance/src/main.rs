//! Executable architectural governance for the worklane workspace.
//!
//! `AGENTS.md` states two load-bearing crate-graph invariants in prose — the
//! portability of `worklane-core` (the Broker design gate) and the
//! substitutability of the brokers. This binary declares them as a
//! `tianheng` [`Constitution`] so CI reacts when the graph drifts, instead of the
//! rules living only as English a reviewer has to remember.
//!
//! ```text
//! cargo run -p worklane-governance -- check
//! ```
//!
//! Exit codes come from `tianheng`: `0` clean, `1` an enforced boundary was
//! breached, `2` a configuration error.

use std::process::ExitCode;
use tianheng::prelude::*;

/// The worklane workspace constitution.
///
/// Both boundaries govern *workspace* dependencies, whose membership `tianheng`
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
        // Every broker stays interchangeable only if none reaches into another
        // broker or the facade — each may depend on worklane-core alone among
        // workspace crates. Substitutability is about passing the same
        // conformance suite, not about durability, so the in-memory reference
        // (worklane-memory) is governed identically to the durable backends.
        .boundary(broker_boundary("worklane-memory"))
        .boundary(broker_boundary("worklane-sqlite"))
        .boundary(broker_boundary("worklane-postgres"))
        .boundary(broker_boundary("worklane-redis"))
        // The shared conformance suite proves substitutability only because it
        // asserts through the contract alone: it must depend on worklane-core
        // and nothing else among workspace crates, with each backend supplying
        // its adapter via a dev-dependency. A normal dependency on a concrete
        // broker would make the suite backend-specific and void the proof.
        // (AGENTS.md: Minimal contracts — assert only through the contract plus
        // a per-implementation adapter.)
        .boundary(
            CrateBoundary::crate_("worklane-test")
                .restrict_workspace_dependencies_to(["worklane-core"])
                .because(
                    "the conformance suite must assert only through the \
                     contract: depend on worklane-core alone, never on a \
                     concrete broker",
                ),
        )
        // The gate must stay independent of the graph it judges: a governor
        // that imported the crates it scores would entangle its verdict with
        // its subject, and a change to the governed graph could break the gate
        // itself. It depends only on tianheng (external), never on a workspace
        // crate. (rust.yml: a dependency-free gate.)
        .boundary(
            CrateBoundary::crate_("worklane-governance")
                .forbid_all_workspace_dependencies()
                .because(
                    "the governance gate must stay independent of the graph it \
                     judges: depend only on tianheng, never on a workspace crate",
                ),
        )
}

/// A broker may depend on only `worklane-core` among workspace crates.
///
/// The rule governs normal `[dependencies]` only, so the dev-dependency on
/// `worklane-test` — the conformance suite that *proves* substitutability — is
/// allowed without being listed: it is the mechanism, not a breach. (AGENTS.md:
/// brokers are interchangeable when they pass the same behavioral conformance
/// suite.)
fn broker_boundary(broker: &str) -> CrateBoundary {
    CrateBoundary::crate_(broker)
        .restrict_workspace_dependencies_to(["worklane-core"])
        .because(
            "brokers must stay substitutable: depend only on worklane-core, \
             never on another broker or the facade",
        )
}

fn main() -> ExitCode {
    tianheng::run(&constitution(), std::env::args())
}
