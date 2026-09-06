//! Seeded fault injection for deterministic simulation.
//!
//! Every fault decision is a draw from the sim's single seeded RNG stream
//! ([`SimRng`]), consumed in a fixed program order — so the fault schedule
//! is a pure function of the seed. The same seed replays the exact same
//! crashes, rejections, delays, and duplicates.
//!
//! Fault model (all probabilities are independent per-event draws):
//!
//! | Fault | Effect |
//! |---|---|
//! | `crash_prob` | the target actor panics mid-message (real `catch_unwind` recovery: state → `Failed`, mailbox drained, worker survives) |
//! | `send_reject_prob` | the "transport" rejects the send before delivery (message lost; models fail-fast `try_send` semantics) |
//! | `delay_prob` | delivery postponed by `delay_ticks_min..=delay_ticks_max` simulated ticks |
//! | `duplicate_prob` | the message is delivered a second time later (at-least-once transport) |
//!
//! System messages (`Start`) are exempt: fault injection targets
//! application traffic, keeping the identity ledger clean.

use rand_xoshiro::rand_core::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;

/// Fault probabilities for a simulation run.
///
/// `FaultConfig::default()` injects nothing (pure scheduling interleaving).
/// Use [`FaultConfig::chaos`] for a sensible all-faults-on baseline, or set
/// fields directly.
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Probability that an application message send is rejected by the
    /// transport (message lost, sender notified — models fail-fast
    /// `try_send`).
    pub send_reject_prob: f64,
    /// Probability that a delivery is delayed by
    /// `delay_ticks_min..=delay_ticks_max` simulated ticks.
    pub delay_prob: f64,
    /// Minimum delay in ticks.
    pub delay_ticks_min: u64,
    /// Maximum delay in ticks (inclusive).
    pub delay_ticks_max: u64,
    /// Probability that a message is duplicated by the transport
    /// (at-least-once delivery: processed count may exceed one).
    pub duplicate_prob: f64,
    /// Probability that processing an application message crashes the actor
    /// (panic inside the executor; the runtime's real panic containment
    /// runs).
    pub crash_prob: f64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            send_reject_prob: 0.0,
            delay_prob: 0.0,
            delay_ticks_min: 1,
            delay_ticks_max: 8,
            duplicate_prob: 0.0,
            crash_prob: 0.0,
        }
    }
}

impl FaultConfig {
    /// All-faults-on baseline with the given per-message crash rate
    /// (e.g. `0.01` = 1% of application messages crash their actor).
    pub fn chaos(crash_prob: f64) -> Self {
        Self {
            send_reject_prob: 0.01,
            delay_prob: 0.05,
            delay_ticks_min: 1,
            delay_ticks_max: 8,
            duplicate_prob: 0.02,
            crash_prob,
        }
    }

    /// True when no faults are configured (exactly-once mode).
    pub fn is_fault_free(&self) -> bool {
        self.send_reject_prob == 0.0
            && self.delay_prob == 0.0
            && self.duplicate_prob == 0.0
            && self.crash_prob == 0.0
    }
}

/// The sim's single seeded RNG stream.
///
/// Everything nondeterministic in the real runtime — which task runs next,
/// whether a fault fires, what the workload does — is drawn from this one
/// xoshiro256++ stream in a fixed program order. That is the whole
/// determinism contract: one stream, one order, one seed.
pub(crate) struct SimRng {
    inner: Xoshiro256PlusPlus,
}

impl SimRng {
    /// Seed the stream.
    pub fn new(seed: u64) -> Self {
        Self {
            inner: Xoshiro256PlusPlus::seed_from_u64(seed),
        }
    }

    /// Next raw `u64`.
    pub fn next_u64(&mut self) -> u64 {
        self.inner.next_u64()
    }

    /// Uniform `f64` in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        // 53 random mantissa bits: (x >> 11) * 2^-53 ∈ [0, 1).
        ((self.next_u64() >> 11) as f64) * (1.0 / (1u64 << 53) as f64)
    }

    /// Uniform `u64` in `[0, n)` (`n > 0`); multiply-shift, no modulo bias.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        let wide = (self.next_u64() as u128 * n as u128) >> 64;
        wide as u64
    }

    /// Uniform `u64` in `[lo, hi]` (inclusive).
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.below(hi - lo + 1)
    }

    /// Draw an event with probability `p`.
    ///
    /// Short-circuiting on `p <= 0` (no draw consumed) is part of the fixed
    /// program order — the stream position is a deterministic function of
    /// the config, not of runtime state.
    pub fn roll(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        self.next_f64() < p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = SimRng::new(12345);
        let mut b = SimRng::new(12345);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn next_f64_in_range() {
        let mut rng = SimRng::new(7);
        for _ in 0..10_000 {
            let x = rng.next_f64();
            assert!((0.0..1.0).contains(&x), "out of range: {x}");
        }
    }

    #[test]
    fn below_bounded_and_covers() {
        let mut rng = SimRng::new(99);
        let mut seen = [false; 8];
        for _ in 0..10_000 {
            let v = rng.below(8);
            assert!(v < 8);
            seen[v as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "all buckets hit");
    }

    #[test]
    fn roll_zero_never_true_and_is_deterministic() {
        let mut a = SimRng::new(5);
        let mut b = SimRng::new(5);
        for _ in 0..100 {
            assert!(!a.roll(0.0));
            assert_eq!(a.next_u64(), b.next_u64(), "zero-prob draws consume nothing");
        }
        assert!((0..100).filter(|_| a.roll(1.0)).count() == 100);
    }

    #[test]
    fn range_inclusive() {
        let mut rng = SimRng::new(11);
        for _ in 0..1000 {
            assert_eq!(rng.range(3, 3), 3);
            let v = rng.range(2, 5);
            assert!((2..=5).contains(&v));
        }
    }
}
