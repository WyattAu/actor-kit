//! Virtual time for deterministic simulation.
//!
//! [`SimClock`] replaces `std::time` / `tokio::time` on the simulation path.
//! Simulated time starts at microsecond zero and moves **only** when the sim
//! explicitly calls [`SimClock::advance`] — never by wall-clock elapse,
//! never by sleeping. Combined with the single-threaded sim runner this
//! makes time a pure function of the seed: two runs with the same seed
//! observe identical clock readings at identical scheduling steps.
//!
//! The clock is `Send + Sync` (atomic backing) because executor trait
//! objects it may travel with require it, but the sim runner is
//! single-threaded by design: determinism requires it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Saturating microsecond count of a duration (Duration carries u128).
fn micros_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// A point in simulated time (microsecond resolution).
///
/// Opaque count of simulated microseconds since the sim's epoch. Orders
/// lexicographically; arithmetic saturates instead of panicking (a sim that
/// advances ~584 million years in one step has bigger problems).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SimInstant {
    micros: u64,
}

impl SimInstant {
    /// The sim's epoch (zero).
    pub const EPOCH: Self = Self { micros: 0 };

    /// Raw microseconds since the sim epoch.
    pub fn as_micros(self) -> u64 {
        self.micros
    }

    /// Amount of simulated time from `earlier` to `self` (saturating).
    pub fn duration_since(self, earlier: Self) -> Duration {
        Duration::from_micros(self.micros.saturating_sub(earlier.micros))
    }
}

impl std::ops::Add<Duration> for SimInstant {
    type Output = SimInstant;
    fn add(self, rhs: Duration) -> SimInstant {
        SimInstant {
            micros: self.micros.saturating_add(micros_of(rhs)),
        }
    }
}

impl std::ops::Sub for SimInstant {
    type Output = Duration;
    fn sub(self, rhs: SimInstant) -> Duration {
        self.duration_since(rhs)
    }
}

/// Virtual clock: `now()` advances only via [`SimClock::advance`].
///
/// No thread, no reactor, no timer wheel — "sleeping" in the sim means
/// recording a future tick with the runner and letting it advance the clock.
#[derive(Debug)]
pub struct SimClock {
    micros: AtomicU64,
}

impl SimClock {
    /// Create a clock at simulated time zero.
    pub fn new() -> Self {
        Self::with_start(0)
    }

    /// Create a clock starting at `micros` simulated microseconds.
    pub fn with_start(micros: u64) -> Self {
        Self {
            micros: AtomicU64::new(micros),
        }
    }

    /// Current simulated instant.
    pub fn now(&self) -> SimInstant {
        SimInstant {
            micros: self.micros.load(Ordering::Relaxed),
        }
    }

    /// Advance simulated time by `duration` and return the new instant.
    ///
    /// This is the *only* way time moves in a simulation.
    pub fn advance(&self, duration: Duration) -> SimInstant {
        let micros = micros_of(duration);
        let prev = self.micros.fetch_add(micros, Ordering::Relaxed);
        SimInstant {
            micros: prev.saturating_add(micros),
        }
    }

    /// Simulated time elapsed since the epoch.
    pub fn elapsed(&self) -> Duration {
        Duration::from_micros(self.micros.load(Ordering::Relaxed))
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_moves_only_when_advanced() {
        let clock = SimClock::new();
        assert_eq!(clock.now(), SimInstant::EPOCH);
        assert_eq!(
            clock.now(),
            SimInstant::EPOCH,
            "reading must not advance time"
        );
        let t = clock.advance(Duration::from_millis(5));
        assert_eq!(t.as_micros(), 5_000);
        assert_eq!(clock.now().as_micros(), 5_000);
    }

    #[test]
    fn instant_arithmetic_saturates() {
        let t = SimInstant { micros: u64::MAX };
        let later = t + Duration::from_secs(1_000);
        assert_eq!(later.as_micros(), u64::MAX);
        assert_eq!(
            later.duration_since(SimInstant::EPOCH),
            Duration::from_micros(u64::MAX)
        );
    }

    #[test]
    fn duration_since_and_sub() {
        let a = SimInstant { micros: 1_000 };
        let b = SimInstant { micros: 1_500 };
        assert_eq!(b - a, Duration::from_micros(500));
        assert_eq!(a.duration_since(b), Duration::ZERO, "saturating");
    }
}
