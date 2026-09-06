//! Deterministic simulation seed sweep.
//!
//! Runs the chaotic sim across many seeds, asserting the runtime invariants
//! for every trace. Any failure prints its seed — replay it with
//! `Sim::with_config(seed, config)`.

#![cfg(feature = "sim")]

use actor_kit::sim::{FaultConfig, Sim, SimConfig};

/// The chaotic configuration used by the in-crate invariant checks.
fn chaos_config(seed: u64) -> SimConfig {
    let mut config = SimConfig::default();
    config.messages = 512;
    config.faults = FaultConfig::chaos(0.02);
    let _ = seed; // seed is passed to Sim::with_config
    config
}

#[test]
fn sim_invariants_hold_across_500_seeds() {
    for seed in 0..500u64 {
        let mut sim = Sim::with_config(seed, chaos_config(seed));
        let outcome = sim.run_to_quiescence();
        assert!(
            outcome.quiesced,
            "seed {seed}: sim did not quiesce — replay with Sim::with_config({seed}, ..)"
        );
        // The per-seed invariant checks (restart budgets, delivery accounting)
        // run inside `run_to_quiescence` and panic with the seed attached.
    }
}

#[test]
fn sim_determinism_same_seed_same_trace() {
    // Determinism contract: identical seed + workload ⇒ identical trace.
    for seed in [7u64, 42, 9_999] {
        let mut a = Sim::with_config(seed, chaos_config(seed));
        let out_a = a.run_to_quiescence();
        let mut b = Sim::with_config(seed, chaos_config(seed));
        let out_b = b.run_to_quiescence();
        assert_eq!(
            a.trace_hash(),
            b.trace_hash(),
            "seed {seed}: traces diverge — determinism contract violated"
        );
        assert_eq!(out_a.stats.processed, out_b.stats.processed);
    }
}

#[test]
fn sim_delivery_accounting_closes_across_seeds() {
    // Nothing may vanish: every injected message ends up processed, rejected,
    // or dropped. (Duplicates can inflate `processed`, and blocked messages
    // resolve by quiescence, so the honest closure is an inequality.)
    for seed in 500..560u64 {
        let mut sim = Sim::with_config(seed, chaos_config(seed));
        let outcome = sim.run_to_quiescence();
        let s = &outcome.stats;
        assert!(
            s.processed + s.rejected + s.dropped >= s.injected,
            "seed {seed}: messages vanished — injected {} vs processed {} + rejected {} + dropped {}",
            s.injected,
            s.processed,
            s.rejected,
            s.dropped
        );
    }
}

/// Long-range sweep used by the nightly `sim-sweep` workflow. Ignored by
/// default; `--ignored` runs it. Seed range comes from the environment so
/// the workflow can shard the sweep.
#[test]
#[ignore]
fn sim_long_range_seed_sweep() {
    let start: u64 = std::env::var("SEED_START")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let end: u64 = std::env::var("SEED_END")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(start + 499);
    for seed in start..=end {
        let mut sim = Sim::with_config(seed, chaos_config(seed));
        let outcome = sim.run_to_quiescence();
        assert!(
            outcome.quiesced,
            "seed {seed}: did not quiesce — replay with Sim::with_config({seed}, ..)"
        );
    }
}
