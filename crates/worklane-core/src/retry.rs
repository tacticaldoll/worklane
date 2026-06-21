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
    /// Jitter fraction in `0.0..=1.0` (default `0.0` = none). When > 0, a retry
    /// delay computed via [`delay_for_seeded`](RetryPolicy::delay_for_seeded) is
    /// shortened by up to this fraction (so the delay lands in
    /// `[delay·(1-jitter), delay]`), decorrelating jobs that fail in lockstep — a
    /// downstream outage that fails many jobs at once no longer retries them in a
    /// synchronized wave. The reduction is a deterministic function of the per-job
    /// seed (no RNG), so it stays reproducible. `delay_for` ignores it.
    pub jitter: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            base: Duration::from_secs(1),
            factor: 2,
            cap: Duration::from_secs(60),
            jitter: 0.0,
        }
    }
}

impl RetryPolicy {
    /// The retry delay after the given number of `attempts` already made.
    ///
    /// `attempts` is the count of retries *already* performed, so the first
    /// retry passes `0` and gets `base` (`base * factor^0`), the second passes
    /// `1` and gets `base * factor`, and so on. The result is always clamped to
    /// `cap`.
    ///
    /// The growth is computed in `u128` nanoseconds so a large `base` or
    /// `factor` cannot collapse the curve via the old `u32` multiplier ceiling;
    /// any overflow saturates straight to `cap` instead of wrapping.
    pub fn delay_for(&self, attempts: u32) -> Duration {
        let multiplier = u128::from(self.factor)
            .checked_pow(attempts)
            .unwrap_or(u128::MAX);
        let scaled = self.base.as_nanos().saturating_mul(multiplier);
        let capped = scaled.min(self.cap.as_nanos());
        // `capped <= cap.as_nanos() <= u64::MAX as u128` for any real `cap`, but
        // clamp before the cast so a pathological `Duration` can never truncate.
        Duration::from_nanos(capped.min(u128::from(u64::MAX)) as u64).min(self.cap)
    }

    /// As [`delay_for`](RetryPolicy::delay_for), but applies [`jitter`] using
    /// `seed` (typically a hash of the job id) so jobs that fail together spread
    /// their retries instead of firing in a synchronized wave. With `jitter == 0`
    /// this is exactly `delay_for`. The spread is a deterministic function of
    /// `(seed, attempts)` — no RNG — so it is reproducible; the result stays in
    /// `[delay·(1 - jitter), delay]` and clamped to `cap`.
    ///
    /// [`jitter`]: RetryPolicy::jitter
    pub fn delay_for_seeded(&self, attempts: u32, seed: u64) -> Duration {
        let base = self.delay_for(attempts);
        // Reject non-positive AND non-finite jitter up front. A plain `<= 0.0`
        // guard lets `NaN` slip through (every comparison with NaN is false),
        // and `NaN.clamp(0,1)` stays NaN, so `base * (1 - NaN*frac)` is NaN and
        // `NaN as u64 == 0` — collapsing every retry to a zero delay and
        // bypassing the cap, the exact thundering-herd jitter is meant to avoid.
        // Treat any non-finite (NaN/±inf) or non-positive jitter as "no jitter".
        if !(self.jitter.is_finite() && self.jitter > 0.0) {
            return base;
        }
        let jitter = self.jitter.clamp(0.0, 1.0);
        // splitmix64 finalizer over (seed, attempts) → a well-spread fraction in
        // [0, 1), deterministic and RNG-free.
        let mut z = seed ^ u64::from(attempts).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let frac = (z >> 11) as f64 / (1u64 << 53) as f64;
        let scaled = base.as_nanos() as f64 * (1.0 - jitter * frac);
        Duration::from_nanos(scaled as u64).min(self.cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            base: Duration::from_secs(1),
            factor: 2,
            cap: Duration::from_secs(60),
            jitter: 0.0,
        }
    }

    #[test]
    fn first_retry_uses_base() {
        // `attempts = 0` is the first retry: base * factor^0 = base.
        assert_eq!(policy().delay_for(0), Duration::from_secs(1));
    }

    #[test]
    fn grows_geometrically() {
        assert_eq!(policy().delay_for(1), Duration::from_secs(2));
        assert_eq!(policy().delay_for(2), Duration::from_secs(4));
        assert_eq!(policy().delay_for(5), Duration::from_secs(32));
    }

    #[test]
    fn clamps_to_cap() {
        // factor^6 = 64s > 60s cap.
        assert_eq!(policy().delay_for(6), Duration::from_secs(60));
        // Large attempt counts saturate to cap, never wrap or collapse.
        assert_eq!(policy().delay_for(1_000), Duration::from_secs(60));
        assert_eq!(policy().delay_for(u32::MAX), Duration::from_secs(60));
    }

    #[test]
    fn zero_jitter_equals_delay_for() {
        let p = policy(); // jitter 0.0
        for attempts in 0..8 {
            assert_eq!(
                p.delay_for_seeded(attempts, 0xDEAD_BEEF),
                p.delay_for(attempts),
                "jitter == 0 must be identical to delay_for"
            );
        }
    }

    #[test]
    fn jitter_stays_within_band_and_is_deterministic() {
        let p = RetryPolicy {
            base: Duration::from_secs(10),
            factor: 2,
            cap: Duration::from_secs(600),
            jitter: 0.5,
        };
        let nominal = p.delay_for(3); // 80s, well under cap
        let lo = nominal.mul_f64(0.5); // delay·(1 - jitter)
        for seed in [1u64, 42, 999, u64::MAX, 0] {
            let d = p.delay_for_seeded(3, seed);
            assert!(
                d >= lo && d <= nominal,
                "jittered delay {d:?} outside [{lo:?}, {nominal:?}]"
            );
            // Deterministic: same (attempts, seed) → same delay.
            assert_eq!(d, p.delay_for_seeded(3, seed));
        }
        // Different seeds spread (not all identical) — decorrelation.
        let a = p.delay_for_seeded(3, 1);
        let b = p.delay_for_seeded(3, 2);
        assert_ne!(a, b, "distinct seeds should jitter differently");
    }

    #[test]
    fn non_finite_jitter_is_treated_as_no_jitter() {
        // A NaN/inf jitter must not collapse the delay to zero or bypass the
        // cap: it falls back to the plain backoff curve, identical to delay_for.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let p = RetryPolicy {
                base: Duration::from_secs(2),
                factor: 2,
                cap: Duration::from_secs(60),
                jitter: bad,
            };
            for attempts in 0..6 {
                assert_eq!(
                    p.delay_for_seeded(attempts, 0xABCD),
                    p.delay_for(attempts),
                    "non-finite jitter ({bad}) must behave as no jitter"
                );
            }
        }
    }

    #[test]
    fn large_base_is_not_capped_below_its_value() {
        // A base larger than the old u32 multiplier ceiling must still grow
        // correctly rather than collapsing back toward `base`.
        let p = RetryPolicy {
            base: Duration::from_secs(10),
            factor: 10,
            cap: Duration::from_secs(1_000_000),
            jitter: 0.0,
        };
        assert_eq!(p.delay_for(0), Duration::from_secs(10));
        assert_eq!(p.delay_for(1), Duration::from_secs(100));
        assert_eq!(p.delay_for(2), Duration::from_secs(1_000));
        assert_eq!(p.delay_for(100), Duration::from_secs(1_000_000));
    }
}
