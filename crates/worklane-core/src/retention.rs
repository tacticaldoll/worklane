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

    /// The dead-letter age cutoff, in integer nanoseconds, for a prune happening
    /// at `now` (also nanoseconds): records dead-lettered before this instant are
    /// over the [`max_age`](RetentionPolicy::max_age) bound. `None` if the policy
    /// sets no age bound.
    ///
    /// A `max_age` exceeding `now` yields a negative cutoff; a `dead_at < cutoff`
    /// delete then matches nothing (stored timestamps are non-negative), which is
    /// the intended "too early to expire anything" behaviour. The subtraction only
    /// truly saturates at the `i64` bound, where `nanos` has already clamped an
    /// enormous `max_age` to `i64::MAX`.
    ///
    /// Shared so every backend computes the same cutoff and only its own
    /// dialect-specific delete differs.
    pub fn age_cutoff_nanos(&self, now: i64) -> Option<i64> {
        self.max_age
            .map(|age| now.saturating_sub(crate::spi::nanos(age)))
    }

    /// The per-lane keep bound as an `i64` row limit, saturating at `i64::MAX`.
    /// `None` if the policy sets no [`max_count`](RetentionPolicy::max_count)
    /// bound. Shared so backends agree on the saturation behaviour.
    pub fn keep_count(&self) -> Option<i64> {
        self.max_count
            .map(|count| i64::try_from(count).unwrap_or(i64::MAX))
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

    #[test]
    fn age_cutoff_and_keep_count_are_none_when_unbounded() {
        let p = RetentionPolicy::new();
        assert_eq!(p.age_cutoff_nanos(1_000), None);
        assert_eq!(p.keep_count(), None);
    }

    #[test]
    fn age_cutoff_subtracts_max_age_and_floors_at_zero() {
        let p = RetentionPolicy::new().with_max_age(Duration::from_nanos(300));
        assert_eq!(p.age_cutoff_nanos(1_000), Some(700));
        // A max_age older than `now` yields a negative cutoff; a `dead_at < cutoff`
        // delete then matches nothing (timestamps are non-negative). This mirrors
        // the prior per-backend `now.saturating_sub(...)` behaviour exactly.
        assert_eq!(p.age_cutoff_nanos(100), Some(-200));
    }

    #[test]
    fn keep_count_saturates_at_i64_max() {
        assert_eq!(
            RetentionPolicy::new().with_max_count(5).keep_count(),
            Some(5)
        );
        assert_eq!(
            RetentionPolicy::new().with_max_count(u64::MAX).keep_count(),
            Some(i64::MAX)
        );
    }
}
