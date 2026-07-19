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

// The canonical boundary reasons — the load-bearing 母則 each rule carries.
// `tianheng` foregrounds `.because(...)` in its projections (`list --format
// markdown`) precisely so an agent reading the law internalises the reason, not
// just the mechanical rule. These consts are the single source: each is the
// argument to one `.because(...)` below *and* the string a drift test asserts
// appears verbatim in `AGENTS.md` §Boundary enforcement, so the constitution
// and its prose projection cannot diverge silently.
const CORE_REASON: &str = "worklane-core is the portable contract root; it must \
    not depend on any other workspace crate";
const BROKER_REASON: &str = "brokers must stay substitutable: depend only on \
    worklane-core, never on another broker or the facade";
const TEST_REASON: &str = "the conformance suite must assert only through the \
    contract: depend on worklane-core alone, never on a concrete broker";
const GOVERNANCE_REASON: &str = "the governance gate must stay independent of \
    the graph it judges: depend only on tianheng, never on a workspace crate";
const FACADE_REASON: &str = "the facade stays broker-agnostic and thin: depend \
    only on worklane-core among workspace crates; bring your own broker";

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
                .because(CORE_REASON),
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
                .because(TEST_REASON),
        )
        // The gate must stay independent of the graph it judges: a governor
        // that imported the crates it scores would entangle its verdict with
        // its subject, and a change to the governed graph could break the gate
        // itself. It depends only on tianheng (external), never on a workspace
        // crate. (rust.yml: a dependency-free gate.)
        .boundary(
            CrateBoundary::crate_("worklane-governance")
                .forbid_all_workspace_dependencies()
                .because(GOVERNANCE_REASON),
        )
        // The facade is the thin public surface over the contract (worker,
        // client, workflow built on worklane-core). It stays broker-agnostic —
        // bring your own broker — so it must not pull a broker (or any other
        // workspace crate) in: depend on worklane-core alone among workspace
        // crates. Users compose a broker crate separately. (AGENTS.md: layout.)
        .boundary(
            CrateBoundary::crate_("worklane")
                .restrict_workspace_dependencies_to(["worklane-core"])
                .because(FACADE_REASON),
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
        .because(BROKER_REASON)
}

fn main() -> ExitCode {
    tianheng::run(&constitution(), std::env::args())
}

/// The constitution and its prose projection are two faces of one law. The
/// machine face is enforced by `tianheng` over `cargo metadata`; the prose face
/// lives in `AGENTS.md` §Boundary enforcement, where an agent reads it before
/// touching the graph. They share one source — the `*_REASON` consts above — so
/// this guard turns drift between them into a CI reaction: if a `.because(...)`
/// reason is reworded without updating the prose (or vice versa), the test
/// fails until the single source is restored. Whitespace is normalised because
/// `AGENTS.md` wraps at ~80 columns, so a reason spans several lines there while
/// the const is one logical string.
#[cfg(test)]
mod tests {
    use super::*;

    const AGENTS_MD: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../AGENTS.md"));

    #[test]
    fn boundary_reasons_are_projected_into_agents_md() {
        let prose: String = AGENTS_MD.split_whitespace().collect::<Vec<_>>().join(" ");
        for reason in [
            CORE_REASON,
            BROKER_REASON,
            TEST_REASON,
            GOVERNANCE_REASON,
            FACADE_REASON,
        ] {
            assert!(
                prose.contains(reason),
                "AGENTS.md §Boundary enforcement has drifted from the \
                 constitution: it must carry this boundary reason verbatim \
                 (whitespace-insensitive). Add or restore it:\n  {reason}",
            );
        }
    }
}
