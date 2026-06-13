use std::time::{Duration, Instant};

/// A monotonic time source, abstracted so brokers derive time-based decisions
/// (visibility, lease expiry, retry scheduling) from an injectable clock rather
/// than reading wall-clock time directly.
///
/// `now` returns the elapsed time since an arbitrary epoch; only differences are
/// meaningful.
pub trait Clock: Send + Sync {
    /// The current time as a duration since the clock's epoch.
    fn now(&self) -> Duration;
}

/// A real clock backed by [`Instant`].
pub struct SystemClock {
    start: Instant,
}

impl SystemClock {
    /// Create a system clock anchored at the current instant.
    pub fn new() -> Self {
        SystemClock {
            start: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }
}
