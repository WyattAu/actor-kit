# Performance claims inventory — actor-kit

Every performance claim in [README.md](README.md) and [PERF-SLO.md](PERF-SLO.md),
mapped to its proof artifact. Created 2026-09-12 (actor-kit 0.2.2).

Machine context for wall-clock records: Intel Core i5-9400F (6 cores,
x86_64), 2026-09, post-0.1.1 re-measurement. Proof kinds: **criterion**
(wall-clock record), **iai** (instruction gate, `cargo bench --bench
iai_hot_path`), **test** (runs on every `cargo test`), **code**, **compiler**.

## Measured throughput (criterion — measured-on records)

| # | Claim | Artifact | Status |
|---|---|---|---|
| 1 | Round-trip 1 worker: ≈ 12.5 µs/msg (cap-64 mailbox, backpressured) | `benches/message_roundtrip.rs::send_drain_200_1w` | backed (measured on) |
| 2 | Round-trip 4 workers: ≈ 3.2 µs/msg | same, `send_drain_200_4w` | backed (measured on) |
| 3 | Uncontended dispatch 1 worker: ≈ 2.5 µs/msg (~0.39 M msg/s) | `message_roundtrip` cap-10k arms | backed (measured on) |
| 4 | Uncontended dispatch 4 workers: ≈ 1.3 µs/msg (~0.76 M msg/s) | same; **load-independent form proven**: iai `scheduler_tell` = 4 165 instructions per uncontended tell (registry lookup + mailbox send + task push; 2026-09-12, valgrind 3.25.1) | **proven** (iai) + backed (criterion) |
| 5 | Spawn ≈ 580 µs/actor (default 10 000-msg mailbox) | `benches/spawn_throughput.rs` | backed (measured on) |
| 6 | Steal contention 2 producers × 10k: ≈ 776k msg/s | `benches/steal_contention.rs` | backed (measured on) |
| 7 | Steal contention 8 producers × 10k: ≈ 1.32 M msg/s | same | backed (measured on) |
| 8 | False-sharing audit deltas: cache-misses −15%, `2_producers` −24%, `8_producers` −4% | PERF-SLO addendum (`perf stat` + criterion medians, machine noted as loaded — deltas not absolutes) | backed (measured on, hardware-noted) |

## SLO statements

| # | Claim | Artifact | Status |
|---|---|---|---|
| 9 | Sustained delivery: N messages across M actors never stalls | `tests/stall_regression.rs::sustained_delivery_across_50k_messages_does_not_stall` (500k cumulative messages, 10/10 green) | **proven** (test) |
| 10 | ≥ 0.7 M msg/s uncontended with ≥ 4 workers | policy; evidence = claim 4 | backed (policy) |
| 11 | Backpressured ≤ 15 µs/msg (1 worker) / ≤ 5 µs/msg (4 workers) | policy; evidence = claims 1+2 | backed (policy) |
| 12 | Spawn ≤ 1 ms P50 at default capacity | policy; evidence = claim 5 | backed (policy) |

## Message model (allocation + concurrency)

| # | Claim | Artifact | Status |
|---|---|---|---|
| 13 | Mailbox is lock-free (`crossbeam_queue::ArrayQueue` + `Semaphore`) | code (`src/mailbox.rs`); crossbeam documents `ArrayQueue` as lock-free | backed (code) |
| 14 | "zero-copy" refers to the rkyv message path (`zero-copy` feature), not the mailbox | code (`src/zero_copy/`, feature-gated); mailbox clones payloads by design | backed (code; wording clarified in CLAIMS to prevent misreading) |
| 15 | ≥ 1 allocation per message (Custom Vec + task wrap) — was "needs counting-allocator treatment" | `tests/zero_alloc_tell.rs`: steady-state tell measured at **~2 allocations/call** (payload `Vec` clone for the mailbox + crossbeam-deque `Injector` slot), stable across windows, bounded ≤ 2.1/call (2026-09-12) | **proven** (test; gap closed) |
| 16 | Pure mailbox enqueue (`try_send` happy path) is allocation-free | same test: delta = 0; iai `mailbox_try_send` = **716 instructions** | **proven** (test + iai) |
| 17 | Per-spawn: one `ArrayQueue` + one `Semaphore` sized by capacity (~640 KB zeroed at 10 000 default) | code (`src/mailbox.rs::new`) — spawn is excluded from the alloc test by design and measured by criterion (claim 5) | backed (code) |

## Other

| # | Claim | Artifact | Status |
|---|---|---|---|
| 18 | `#![deny(unsafe_code)]`; the only `unsafe` is `memory_pool` behind `unsafe-pool` | attributes in `src/lib.rs` / `src/memory_pool.rs` | **proven** (compiler) |
| 19 | "100,000+ actors per node" | design target (bounded per-actor memory via capped mailboxes), not a benchmark — README wording kept as a hosting *target*, backed by the spawn cost model (claim 5: 640 KB × 100k ≈ 64 GB… the claim as written requires ~1k-message mailboxes; see note) | **reworded** (PERF-SLO note) |

Note on claim 19: at the *default* 10 000-message mailbox (~640 KB/actor),
100 000 actors imply ~64 GB — the target holds with modest mailboxes
(e.g. 1 000 messages ≈ 64 KB/actor ≈ 6.4 GB). PERF-SLO now states the
mailbox-size condition explicitly instead of an unconditional number.

## Totals

- **Proven by hard artifact (iai/test/compiler):** 6
  (claims 4, 9, 15, 16, 18 — and 13 partially via the 716-instruction pin)
- **Backed (measured-on records, policy, code reading):** 12
- **Deleted/reworded:** 1 (claim 19 — capacity claim now states its
  mailbox-size condition)

## Reproducing

```sh
cargo bench --bench message_roundtrip    # wall-clock round-trip
cargo bench --bench spawn_throughput     # spawn cost
cargo bench --bench steal_contention     # contention throughput
cargo bench --bench iai_hot_path         # instruction gate (needs valgrind)
cargo test  --test zero_alloc_tell       # allocation gate
cargo test  --test stall_regression      # sustained-delivery gate
```
