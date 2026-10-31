# Performance claims inventory: actor-kit
## Measured throughput (criterion: measured-on records)
| 8 | False-sharing audit deltas: cache-misses −15%, `2_producers` −24%, `8_producers` −4% | PERF-SLO addendum (`perf stat` + criterion medians, machine noted as loaded; deltas not absolutes) | backed (measured on, hardware-noted) |
| 15 | ≥ 1 allocation per message (Custom Vec + task wrap); was "needs counting-allocator treatment" | `tests/zero_alloc_tell.rs`: steady-state tell measured at **~2 allocations/call** (payload `Vec` clone for the mailbox + crossbeam-deque `Injector` slot), stable across windows, bounded ≤ 2.1/call (2026-09-12) | **proven** (test; gap closed) |
| 17 | Per-spawn: one `ArrayQueue` + one `Semaphore` sized by capacity (~640 KB zeroed at 10 000 default) | code (`src/mailbox.rs::new`); spawn is excluded from the alloc test by design and measured by criterion (claim 5) | backed (code) |
| 19 | "100,000+ actors per node" | design target (bounded per-actor memory via capped mailboxes), not a benchmark; README wording kept as a hosting *target*, backed by the spawn cost model (claim 5: 640 KB × 100k ≈ 64 GB… the claim as written requires ~1k-message mailboxes; see note) | **reworded** (PERF-SLO note) |
100 000 actors imply ~64 GB; the target holds with modest mailboxes
  (claims 4, 9, 15, 16, 18; and 13 partially via the 716-instruction pin)
- **Deleted/reworded:** 1 (claim 19; capacity claim now states its