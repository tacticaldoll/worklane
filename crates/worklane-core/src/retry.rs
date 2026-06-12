use std::time::Duration;

/// Computes the delay before a failed job is retried, using capped exponential
/// backoff: `delay = min(base * factor^attempts, cap)`.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// The base delay for the first retry.
    pub base: Duration,
    /// The exponential growth factor.
    pub factor: u32,
    /// The maximum delay.
    pub cap: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            base: Duration::from_secs(1),
            factor: 2,
            cap: Duration::from_secs(60),
        }
    }
}

impl RetryPolicy {
    /// The retry delay after the given number of `attempts` already made.
    pub fn delay_for(&self, attempts: u32) -> Duration {
        let multiplier = self.factor.checked_pow(attempts).unwrap_or(u32::MAX);
        let delay = self.base.saturating_mul(multiplier);
        delay.min(self.cap)
    }
}
