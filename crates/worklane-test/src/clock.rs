use std::sync::Mutex;
use std::time::Duration;

use worklane_core::Clock;

/// A manually-advanced [`Clock`] for deterministic broker tests.
///
/// Test-only time control lives here rather than in `worklane-core`: it is a
/// testing capability, not part of the production contract.
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
