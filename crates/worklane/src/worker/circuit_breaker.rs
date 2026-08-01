//! A per-kind circuit breaker for the worker.
//!
//! When a job kind's handler fails repeatedly — typically because a dependency it
//! calls is down — continuing to run (and retry, and eventually dead-letter) every
//! job of that kind wastes work and floods the dead-letter store. The breaker
//! trips after a threshold of consecutive failures and, for a cooldown, short-
//! circuits *dispatch* of that kind: the worker defers each reserved job
//! ([`Broker::defer`](worklane_core::Broker::defer)) **without** spending its retry
//! budget, so a long outage cannot exhaust `max_attempts` and dead-letter the
//! backlog. When the cooldown elapses the next job is let through as a probe; its
//! success closes the breaker, its failure re-opens it.
//!
//! The closed/open/half-open transition rules themselves — including bounding how
//! long a single probe may stay outstanding before a fresh one replaces it — are
//! [`sigorta::Sigorta`]'s: a sans-I/O core with no ambient clock read. This module
//! owns everything Sigorta deliberately does not: the wall clock, the per-kind
//! keying, and the policy values.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sigorta::{Decision, Event, Sigorta};

/// Tuning for a [`CircuitBreaker`].
#[derive(Debug, Clone)]
pub struct CircuitBreakerPolicy {
    /// Consecutive handler failures of one kind that trip its breaker.
    pub failure_threshold: u32,
    /// How long a tripped breaker stays open before letting a probe through.
    pub open_duration: Duration,
}

impl Default for CircuitBreakerPolicy {
    fn default() -> Self {
        CircuitBreakerPolicy {
            failure_threshold: 5,
            open_duration: Duration::from_secs(30),
        }
    }
}

/// Per-kind circuit-breaker state, shared across a worker's in-flight tasks.
///
/// State is per-worker and in-process (a fresh worker starts closed); it uses a
/// monotonic [`Instant`] clock, independent of the broker's time source.
pub struct CircuitBreaker {
    policy: CircuitBreakerPolicy,
    states: Mutex<HashMap<String, Sigorta>>,
}

impl CircuitBreaker {
    /// Create a breaker with the given policy.
    pub fn new(policy: CircuitBreakerPolicy) -> Self {
        CircuitBreaker {
            policy,
            states: Mutex::new(HashMap::new()),
        }
    }

    /// Decide whether to admit a job of `kind` for dispatch. `None` admits it
    /// (closed, or *the* half-open probe); `Some(delay)` defers it for `delay`
    /// without spending an attempt (open, or a probe already in flight).
    pub fn admit(&self, kind: &str) -> Option<Duration> {
        let now = Instant::now();
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let entry = states.entry(kind.to_string()).or_insert_with(|| {
            Sigorta::new(self.policy.failure_threshold, self.policy.open_duration)
        });
        match entry.admit(now) {
            Decision::Admitted(next) | Decision::Probing(next) => {
                *entry = next;
                None
            }
            Decision::Rejected { core, retry_after } => {
                *entry = core;
                Some(retry_after)
            }
        }
    }

    /// Record a handler outcome for `kind`. Success closes the breaker; a failure
    /// trips a closed breaker once the threshold run is reached, and re-opens it
    /// immediately on a failed half-open probe.
    pub fn record(&self, kind: &str, success: bool) {
        let now = Instant::now();
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let entry = states.entry(kind.to_string()).or_insert_with(|| {
            Sigorta::new(self.policy.failure_threshold, self.policy.open_duration)
        });
        let event = if success {
            Event::Success
        } else {
            Event::Failure
        };
        *entry = entry.record(event, now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn breaker(threshold: u32, open: Duration) -> CircuitBreaker {
        CircuitBreaker::new(CircuitBreakerPolicy {
            failure_threshold: threshold,
            open_duration: open,
        })
    }

    #[test]
    fn opens_after_threshold_consecutive_failures() {
        let cb = breaker(2, Duration::from_secs(60));
        assert!(cb.admit("k").is_none(), "starts closed");
        cb.record("k", false);
        assert!(cb.admit("k").is_none(), "one failure is below threshold");
        cb.record("k", false);
        assert!(cb.admit("k").is_some(), "threshold reached → open");
    }

    #[test]
    fn success_resets_the_failure_run() {
        let cb = breaker(2, Duration::from_secs(60));
        cb.record("k", false);
        cb.record("k", true); // resets
        cb.record("k", false);
        assert!(
            cb.admit("k").is_none(),
            "a success between failures must reset the run, so one more failure is not a trip"
        );
    }

    #[test]
    fn only_one_probe_is_admitted_when_the_cooldown_elapses() {
        // `open_duration` is both the cooldown and the half-open probe window, so
        // pick a real (non-zero) duration: trip the breaker, let the cooldown pass,
        // then verify exactly one probe is admitted while the window is live.
        let cb = breaker(1, Duration::from_millis(40));
        cb.record("k", false); // Open { ~40ms }
        assert!(cb.admit("k").is_some(), "still cooling down → deferred");
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            cb.admit("k").is_none(),
            "cooldown elapsed → first caller admitted as the probe"
        );
        assert!(
            cb.admit("k").is_some(),
            "a second caller while the probe is in flight is deferred, not admitted"
        );
    }

    #[test]
    fn a_failed_probe_reopens_and_a_successful_probe_closes() {
        let cb = breaker(1, Duration::from_millis(20));
        cb.record("k", false); // open
        std::thread::sleep(Duration::from_millis(30));
        assert!(cb.admit("k").is_none(), "probe admitted after cooldown");

        // Probe fails → re-open immediately (not just at threshold).
        cb.record("k", false);
        assert!(
            cb.admit("k").is_some(),
            "a failed probe re-opens the breaker"
        );

        // Let it cool, admit a probe, and have it succeed → closed.
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            cb.admit("k").is_none(),
            "probe admitted after second cooldown"
        );
        cb.record("k", true);
        assert!(
            cb.admit("k").is_none(),
            "a successful probe closes the breaker"
        );
    }

    #[test]
    fn breakers_are_per_kind() {
        let cb = breaker(1, Duration::from_secs(60));
        cb.record("a", false);
        assert!(cb.admit("a").is_some());
        assert!(cb.admit("b").is_none(), "kind b is unaffected");
    }
}
