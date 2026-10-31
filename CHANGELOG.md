Changelog](https://keepachangelog.com/); versions follow [semver](https://semver.org).
  from a permanent deadlock into a real suspension; and exposed what the
  itself queued behind the suspension; so `pause()` was a permanent
  The fix was on `main` but never released, so `0.2.3`; the version
  `estate-integration` pins; still deadlocks. A downstream consumer reading
- `tests/config_matrix.rs`; per-knob behavior matrix for all 16 config
  (no reader), `ChildSpec::significant` (gates only a log line); pinned
- `tests/zero_alloc_tell.rs`; counting-allocator proof of the allocation
- `benches/iai_hot_path.rs`; iai-callgrind instruction gate for the tell
  scheduler never released it; the worker processes the `Task` copy from the