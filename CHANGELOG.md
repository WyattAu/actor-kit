# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

## [Unreleased]

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
