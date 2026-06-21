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

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

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

/// The explicit state of one kind's breaker.
///
/// Modeling the three states as a sum type (rather than a `(failures, Option<t>)`
/// pair) makes the illegal combinations unrepresentable and gives `HalfOpen` a
/// home: the old design let *every* job deferred during the cooldown through the
/// instant it elapsed (a thundering herd onto a dependency that may still be
/// down). `HalfOpen` admits exactly one probe and holds the rest.
enum BreakerState {
    /// Healthy. `failures` counts the current run of consecutive failures.
    Closed { failures: u32 },
    /// Tripped. Every job is deferred until `until`.
    Open { until: Instant },
    /// The cooldown has elapsed and a single probe has been admitted; other jobs
    /// are deferred until the probe reports back. `probe_expires` bounds the wait
    /// so a probe that never reports (e.g. its worker died) cannot wedge the kind
    /// — once it lapses, the next caller becomes a fresh probe.
    HalfOpen { probe_expires: Instant },
}

impl Default for BreakerState {
    fn default() -> Self {
        BreakerState::Closed { failures: 0 }
    }
}

/// Per-kind circuit-breaker state, shared across a worker's in-flight tasks.
///
/// State is per-worker and in-process (a fresh worker starts closed); it uses a
/// monotonic [`Instant`] clock, independent of the broker's time source.
pub struct CircuitBreaker {
    policy: CircuitBreakerPolicy,
    states: Mutex<HashMap<String, BreakerState>>,
}

impl CircuitBreaker {
    /// Create a breaker with the given policy.
    pub fn new(policy: CircuitBreakerPolicy) -> Self {
        CircuitBreaker {
            policy,
            states: Mutex::new(HashMap::new()),
        }
    }

    /// `open_duration` from now, saturating so an extreme policy cannot panic on
    /// `Instant` overflow.
    fn cooldown_end(&self, now: Instant) -> Instant {
        now.checked_add(self.policy.open_duration).unwrap_or(now)
    }

    /// Decide whether to admit a job of `kind` for dispatch. `None` admits it
    /// (closed, or *the* half-open probe); `Some(delay)` defers it for `delay`
    /// without spending an attempt (open, or a probe already in flight).
    ///
    /// This is a state transition, not a pure read: when the cooldown elapses it
    /// moves `Open → HalfOpen` and admits the caller as the single probe; further
    /// callers stay deferred until the probe resolves via [`record`](Self::record)
    /// or its window lapses.
    pub fn admit(&self, kind: &str) -> Option<Duration> {
        let now = Instant::now();
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let state = states.entry(kind.to_string()).or_default();
        match state {
            BreakerState::Closed { .. } => None,
            BreakerState::Open { until } => {
                if now < *until {
                    Some(*until - now)
                } else {
                    // Cooldown elapsed: this caller is the probe; hold the rest.
                    *state = BreakerState::HalfOpen {
                        probe_expires: self.cooldown_end(now),
                    };
                    None
                }
            }
            BreakerState::HalfOpen { probe_expires } => {
                if now < *probe_expires {
                    Some(*probe_expires - now)
                } else {
                    // The in-flight probe never reported back within its window;
                    // admit a fresh probe rather than wedging the kind forever.
                    *state = BreakerState::HalfOpen {
                        probe_expires: self.cooldown_end(now),
                    };
                    None
                }
            }
        }
    }

    /// Record a handler outcome for `kind`. Success closes the breaker; a failure
    /// trips a closed breaker once the threshold run is reached, and re-opens it
    /// immediately on a failed half-open probe.
    pub fn record(&self, kind: &str, success: bool) {
        let now = Instant::now();
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let state = states.entry(kind.to_string()).or_default();
        if success {
            *state = BreakerState::Closed { failures: 0 };
            return;
        }
        match state {
            BreakerState::Closed { failures } => {
                let n = failures.saturating_add(1);
                *state = if n >= self.policy.failure_threshold {
                    BreakerState::Open {
                        until: self.cooldown_end(now),
                    }
                } else {
                    BreakerState::Closed { failures: n }
                };
            }
            // A half-open probe failed (or a stray failure arrived while open):
            // (re-)open for a fresh cooldown.
            BreakerState::Open { .. } | BreakerState::HalfOpen { .. } => {
                *state = BreakerState::Open {
                    until: self.cooldown_end(now),
                };
            }
        }
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
