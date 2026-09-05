# Performance SLOs — actor-kit

Measured with criterion (`cargo bench --bench message_roundtrip`,
`--bench spawn_throughput`, `--bench steal_contention`), re-measured 2026-09
after the 0.1.1 drain-stall fix. Hardware: Intel(R) Core(TM) i5-9400F CPU @
2.90GHz, 6 cores, Linux x86_64.
Criterion reports mean/median/stddev, not percentiles; **P50 column = criterion
mean** (P99 is not directly measured; the CI bench job compares means against
the saved `ci` baseline).

## Measured

| Benchmark | P50 (mean) | Notes |
|---|---|---|
| Message round-trip, 1 worker | 2.50 ms / 200 msgs ≈ **12.5 µs/msg** | cap-64 mailbox, producer backpressured (parks when full); dominated by semaphore park/unpark wakeup |
| Message round-trip, 4 workers | 633 µs / 200 msgs ≈ **3.2 µs/msg** | same, 4 workers release capacity sooner |
| Uncontended dispatch, 1 worker | ≈ 2.5 µs/msg (~0.39 M msg/s) | cap-10k mailbox, 200-msg bursts never fill it |
| Uncontended dispatch, 4 workers | ≈ 1.3 µs/msg (~0.76 M msg/s) | same, 4 workers |
| Spawn throughput | ~580 ms / 1000 actors ≈ **580 µs/spawn** | dominated by mailbox preallocation (~640 KB zeroed for the default 10 000-message ArrayQueue); scales ~linearly with `mailbox_config.capacity` |
| Steal contention, 2 producers × 10k | 25.8 ms / 20k msgs ≈ **776k msg/s** | backpressured sends, 16 targets |
| Steal contention, 8 producers × 10k | 60.6 ms / 80k msgs ≈ **1.32 M msg/s** | backpressured sends, 16 targets |

The round-trip bench now runs a single long-lived runtime for the whole
criterion run (≈ 300k–2M+ cumulative messages per target) with no degradation;
previously an actor stalled after ~capacity cumulative messages (see
"History" below).

## SLO statements

- Sustained delivery: **N total messages across M actors complete without
  capacity exhaustion** for any N (delivery is bounded only by producer
  throughput). Regression test:
  `tests/stall_regression.rs::sustained_delivery_across_50k_messages_does_not_stall`
  (4 actors × 12 500 messages, cap-64 mailboxes, 10 consecutive rounds —
  500k cumulative messages — 10/10 green).
- Uncontended dispatch (`send` into a mailbox with free capacity) sustains
  **≥ 0.7 M msg/s per target actor** with ≥ 4 workers (measured ~0.76 M,
  2026-09, 6-core x86_64).
- Backpressured delivery (producer parked on a full mailbox) is bounded by
  semaphore wakeup latency: **≤ 15 µs/msg P50 at 1 worker, ≤ 5 µs/msg at 4
  workers** for a single target.
- Actor spawn costs **≤ 1 ms P50 per actor** at the default mailbox capacity
  (allocation-bound; proportional to `mailbox_config.capacity`).

## Allocation profile

- ≥ 1 allocation per message: the bench payload `MessagePayload::Custom`
  owns a `Vec<u8>`; the scheduler path additionally wraps the message into a
  queued task and parks a clone in the target mailbox (bounded by capacity;
  the slot is released when the worker consumes the message). Dispatch itself
  (registry lookup + enqueue) is lock-free-ish via crossbeam queues; a precise
  per-op count needs the counting-allocator treatment (demonstrated on
  `breaker`).
- Per spawn: one `ArrayQueue<Message>` + one `Semaphore` sized by
  `mailbox_config.capacity` — with the 10 000-message default this is
  ~640 KB zeroed per actor and dominates spawn cost (~580 µs).

## History: the 0.1.0 drain stall (fixed)

0.1.0 stalled after ~`mailbox_capacity` cumulative messages per actor: every
`Mailbox::send`/`try_send` acquired one capacity semaphore permit and
`forget()` it, but the worker consumed the `Task` copy from the work queue and
never popped the mailbox, so permits never returned on the happy path. At
`capacity` cumulative sends the semaphore exhausted and further sends blocked
forever (default capacity 10 000 — the "~10k cumulative" threshold).
Independently, `SchedulerConfig::mailbox_config` was never wired into the
registry (every mailbox silently used the 10 000 default, masking the true
threshold). Fixed in 0.1.1: the worker releases the mailbox slot (permit +
size) when it processes the message; the config knob is honored;
`steal_contention` runs clean and `bench-regression` is re-enabled in CI.

## Regression policy

- Baselines are saved on main in CI by the shared bench job
  ([rust-kit.yml](https://github.com/WyattAu/engineering-standards/blob/main/.github/workflows/rust-kit.yml),
  `cargo bench -- --save-baseline ci`), non-gating (regression visibility).
- Local: `cargo bench --bench message_roundtrip -- --save-baseline main`,
  compare with `-- --baseline main`.
- Alert threshold: >2× mean regression on `message_roundtrip/send_drain_200_1w`.
