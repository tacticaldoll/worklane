use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// An opt-in bound on how many dead-letter records a broker retains.
///
/// Dead-lettered jobs otherwise accumulate until [`purge_dead_letters`] is called
/// manually, so a forgotten purge lets the dead store grow without bound. A
/// `RetentionPolicy` lets a broker cap that growth by count, age, or both. Both
/// bounds are optional; the default (neither set) means *unlimited* — exactly the
/// behavior of a broker with no policy configured.
///
/// Enforcement is **write-driven and lazy**: a broker prunes the affected lane on
/// [`fail`] (when a dead record is written), scoped to that lane. A consequence is
/// that [`max_age`](RetentionPolicy::max_age) is only applied when a lane next
/// fails a job — a lane that stops failing may retain aged records until its next
/// failure or an explicit [`purge_dead_letters`]. [`max_count`] is unaffected by
/// this, since the dead store only grows on a write.
///
/// [`purge_dead_letters`]: crate::DeadLetterStore::purge_dead_letters
/// [`fail`]: crate::Broker::fail
/// [`max_count`]: RetentionPolicy::max_count
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Drop dead-letter records older than this age (measured by the broker's
    /// injected clock). `None` means no age bound.
    pub max_age: Option<Duration>,
    /// Retain at most this many of the most-recently dead-lettered records per
    /// lane, dropping older ones by dead-letter (failure) sequence. `None` means
    /// no count bound.
    pub max_count: Option<u64>,
}

impl RetentionPolicy {
    /// An unbounded policy (retain everything) — the default.
    pub fn new() -> Self {
        RetentionPolicy::default()
    }

    /// Bound the dead store to records younger than `age` (builder style).
    #[must_use = "this value must be used"]
    pub fn with_max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }

    /// Bound the dead store to at most `count` records per lane (builder style).
    #[must_use = "this value must be used"]
    pub fn with_max_count(mut self, count: u64) -> Self {
        self.max_count = Some(count);
        self
    }

    /// Whether the policy imposes no bound (both fields `None`), in which case a
    /// broker can skip all pruning work.
    pub fn is_unbounded(&self) -> bool {
        self.max_age.is_none() && self.max_count.is_none()
    }
}

/// A one-shot operator warning that a broker is dead-lettering under an unbounded
/// [`RetentionPolicy`], so its dead-letter store will grow without limit.
///
/// The default is deliberately unbounded (see [`RetentionPolicy`]) — a broker must
/// never silently delete a dead-lettered job an operator may still want to
/// inspect or requeue. The cost of that safe default is that a forgotten
/// `with_dead_letter_retention`/`purge_dead_letters` lets the dead store grow
/// unbounded. This makes the silent failure mode audible: the first time a job is
/// dead-lettered with no policy configured, the broker logs one `tracing` warning.
/// It is silent for a bounded policy and warns at most once per broker instance,
/// so it never floods logs on a hot failure path. It changes no behaviour — only
/// observability.
///
/// A broker embeds one of these and calls [`warn_once`](Self::warn_once) on its
/// dead-letter (`fail`) path.
#[derive(Debug, Default)]
pub struct UnboundedDlqWarning(AtomicBool);

impl UnboundedDlqWarning {
    /// Emit the warning if `policy` is unbounded and it has not already fired for
    /// this instance. Returns whether it warned on this call — `true` exactly once
    /// per instance, and only for an unbounded policy (so callers can test the
    /// once-semantics without capturing log output).
    pub fn warn_once(&self, policy: &RetentionPolicy) -> bool {
        if !policy.is_unbounded() {
            return false;
        }
        // `swap` returning `true` means a prior call already warned.
        if self.0.swap(true, Ordering::Relaxed) {
            return false;
        }
        tracing::warn!(
            "dead-lettering with no RetentionPolicy configured: the dead-letter \
             store will grow without bound. Configure \
             `with_dead_letter_retention(..)` to cap it, or call \
             `purge_dead_letters` periodically."
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warns_exactly_once_for_an_unbounded_policy() {
        let warning = UnboundedDlqWarning::default();
        let policy = RetentionPolicy::new(); // unbounded
        assert!(warning.warn_once(&policy), "first unbounded fail must warn");
        assert!(
            !warning.warn_once(&policy),
            "a second call must not warn again (no log flooding on a hot path)"
        );
    }

    #[test]
    fn never_warns_for_a_bounded_policy() {
        let warning = UnboundedDlqWarning::default();
        let bounded = RetentionPolicy::new().with_max_count(1000);
        assert!(
            !warning.warn_once(&bounded),
            "a configured retention policy must stay silent"
        );
        // ...and staying silent must not consume the one-shot: an unbounded policy
        // on the same instance still warns. (Policy is fixed per broker in
        // practice; this just proves the guard is keyed on actually warning.)
        assert!(warning.warn_once(&RetentionPolicy::new()));
    }
}
