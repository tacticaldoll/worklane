use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A monotonic time source, abstracted so tests can advance time deterministically.
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

/// A manually-advanced clock for deterministic tests.
pub struct ManualClock {
    elapsed: Mutex<Duration>,
}

impl ManualClock {
    /// Create a manual clock starting at zero.
    pub fn new() -> Self {
        ManualClock {
            elapsed: Mutex::new(Duration::ZERO),
        }
    }

    /// Advance the clock by `delta`.
    pub fn advance(&self, delta: Duration) {
        let mut elapsed = self.elapsed.lock().expect("clock poisoned");
        *elapsed += delta;
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.elapsed.lock().expect("clock poisoned")
    }
}
