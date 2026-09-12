# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

## [Unreleased]

## [0.2.2] - 2026-09-12

### Added

- **Claims proof-back** ([CLAIMS.md](CLAIMS.md)): every numeric performance
  claim in README/PERF-SLO mapped to its proof artifact.
- `tests/zero_alloc_tell.rs` — counting-allocator proof of the allocation
  profile (previously "needs counting-allocator treatment"): steady-state
  tell measures ~2 allocations/call (payload `Vec` clone + crossbeam-deque
  `Injector` slot), bounded and stable; pure mailbox `try_send` proven
  allocation-free.
- `benches/iai_hot_path.rs` — iai-callgrind instruction gate for the tell
  hot path: `mailbox_try_send` (716 instructions) and full `scheduler_tell`
  dispatch (4 165 instructions); runs without worker threads so counts are
  reproducible (CI-gated; needs valgrind to run locally).

### Changed

- PERF-SLO.md allocation profile upgraded from code reading to "proven";
  the "100,000+ actors per node" claim now states its mailbox-size
  condition (~64 KB/actor at 1 000-message mailboxes ≈ 6.4 GB for 100k
  actors) instead of an unconditional number.

## [0.2.1] - 2026-09-11

### Fixed

- 22-gate quality audit pass: documentation completeness
  (README badges, REQUIREMENTS/THREAT-MODEL coverage) and
  feature-gated test hygiene.

## [0.1.1] - 2026-09-05

### Fixed
- **Message drain stall after sustained delivery**: an actor stopped consuming
  after ~`mailbox_capacity` cumulative messages. Every `send` permanently
  consumed one mailbox capacity permit (acquired + `forget()`), and the
  scheduler never released it — the worker processes the `Task` copy from the
  work queue, not the mailbox copy, so permits (and mailbox slots) leaked
  until the semaphore exhausted and all further sends blocked forever
  (`src/scheduler.rs`, `process_single_message`). The worker now releases the
  mailbox slot when it consumes a message
  (`tests/stall_regression.rs` covers 500k cumulative messages, 10/10 green).
- `SchedulerConfig::mailbox_config` was a dead knob: the registry silently
  built every actor mailbox with the 10 000-message default, masking the stall
  threshold. The configured capacity is now honored
  (`ActorRegistry::with_mailbox_config`).
- Lost-wakeup race in `Mailbox::recv`: the `Notify` interest is now registered
  before the emptiness check, so a concurrent send cannot be missed.
- `benches/message_roundtrip.rs` no longer rebuilds the runtime every 4k
  messages (workaround for the stall); `benches/steal_contention.rs` uses
  backpressured `send` instead of fail-fast `try_send().unwrap()` and runs
  clean. CI `bench-regression` re-enabled.

### Changed
- Re-measured and re-stated performance SLOs (see PERF-SLO.md); the previous
  numbers were taken against an effectively unbounded mailbox and are not
  comparable.

## [0.1.0] - 2026-09-05

### Added
- Initial public release.
