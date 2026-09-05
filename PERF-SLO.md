# Performance SLOs — actor-kit

Measured with criterion (`cargo bench --bench message_roundtrip`,
`--bench spawn_throughput`), 2026-09.
Hardware: Intel(R) Core(TM) i5-9400F CPU @ 2.90GHz, 6 cores, Linux x86_64.
Criterion reports mean/median/stddev, not percentiles; **P50 column = criterion
mean** (P99 is not directly measured; the CI bench job compares means against
the saved `ci` baseline).

## Measured

| Benchmark | P50 (mean) | Notes |
|---|---|---|
| Message round-trip, 1 worker | **136 µs / 200 msgs ≈ 0.68 µs/msg** | backpressure-paced dispatch→process |
| Message round-trip, 4 workers | 180 µs / 200 msgs ≈ 0.90 µs/msg | single-target actor, within noise of 1w |
| Spawn throughput | ~162 ms / 1000 actors ≈ **6.2 µs/spawn** | 1w and 4w within noise |

End-to-end dispatch→process throughput of a single target actor:
**1.1–1.5 M messages/s** (bounded mailbox, capacity 64, semaphore
backpressure pacing the producer).

## SLO statements

- `ActorScheduler::send` end-to-end (enqueue through processing of a
  `Custom(vec![u8])` message) sustains **≥ 1 M msg/s per target actor**
  (measured 2026-09, 6-core x86_64), i.e. **≤ 1 µs/msg end-to-end P50**.
- Actor spawn costs **< 10 µs P50** per actor.

## Allocation profile (from code reading)

- ≥ 1 allocation per message: the bench payload `MessagePayload::Custom`
  owns a `Vec<u8>`; the scheduler path additionally wraps the message into a
  queued task. Dispatch itself (registry lookup + enqueue) is lock-free-ish
  via crossbeam queues; a precise per-op count needs the counting-allocator
  treatment (demonstrated on `breaker`), blocked on the known issue below.

## Known issue (honesty note)

- An actor **stops consuming after ~10k cumulative messages** on a
  long-lived runtime: the drain path stalls (lost wakeup suspected) and the
  processed counter can read stale/zero under registry churn. The round-trip
  bench works around it by rebuilding the scheduler every 4k messages;
  `benches/steal_contention.rs` (20k msgs/iteration) currently trips the
  stall and panics — it is therefore **excluded from the CI bench job**
  (`bench-regression` stays off for this crate until fixed). This is a
  correctness issue tracked separately from performance work.

## Regression policy

- Baselines are saved on main in CI by the shared bench job
  ([rust-kit.yml](https://github.com/WyattAu/engineering-standards/blob/main/.github/workflows/rust-kit.yml),
  `cargo bench -- --save-baseline ci`), non-gating (regression visibility).
- Local: `cargo bench --bench message_roundtrip -- --save-baseline main`,
  compare with `-- --baseline main`.
- Alert threshold: >2× mean regression on `message_roundtrip/send_drain_200_1w`.
