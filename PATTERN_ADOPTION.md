# PATTERN_ADOPTION

actor-kit's relationship to the WyattAu pattern canon
(`WyattAu/pattern-standards`). One row per pattern with a real
relationship; non-applicable patterns are omitted per
`references/adoption-format.md` rule 6.

| Pattern | Status | Evidence |
|---|---|---|
| T11 | adopted | src/scheduler.rs:314 (stop() joins every worker handle; actors are registry entries, never detached tasks) |
| T12 | adopted | src/supervisor.rs:55 (OneForOne/OneForAll/RestForOne with restart windows) + src/scheduler.rs:661 (catch_unwind contains handler panics; actor marked Failed, mailbox drained) |
| T16 | adopted | src/mailbox.rs:76 (bounded ArrayQueue plus Semaphore capacity; backpressure flag at the 0.8 threshold) |
| T20 | partial | src/scheduler.rs:42 (single-owner local deques with cross-worker stealing; the global injector stays multi-producer) |
| T33 | refused | R6 (no double-apply failure mode in scope; src/rpc.rs correlation IDs match replies, they do not dedupe applies) |
| T36 | adopted | src/sim/mod.rs:436 (seeded, single-threaded simulation; same seed reproduces the trace) + tests/sim.rs |
| P29 | adopted | src/sim/faults.rs:29 (crash, reject, delay, duplicate fault draws) |
| P64 | adopted | tests/proptest.rs:28 (property battery including exactly-once accounting) |
