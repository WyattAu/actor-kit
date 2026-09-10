# Threat Model — actor-kit

Reference: STRIDE. Scope: the crate's public API surface (scheduler, handles,
mailboxes, supervision, RPC module). Trust boundaries: (1) bytes entering
`RpcRequest::from_bytes` / `RpcResponse::from_bytes` / zero-copy `rkyv`
decoding, (2) concurrent callers and workers sharing one `ActorScheduler`,
(3) the in-process caller itself (actor-kit is not network-exposed; it
inherits the process's trust domain).

## Assets

| ID | Asset | Example |
|----|-------|---------|
| A1 | Availability of the scheduler (100k+ actors stay responsive) | Flooded mailbox or worker starvation stalls all message delivery |
| A2 | Integrity of queued messages | Corruption between send and handler execution |
| A3 | Spoofed message provenance | A message claiming a `sender` it did not originate |

## STRIDE Analysis

| # | Threat | Category | Surface | Mitigation | Verifying test |
|---|--------|----------|---------|------------|----------------|
| T1 | Unbounded memory growth via message flood | DoS | `Mailbox::send` / `ActorHandle::send` | Hard-bounded `ArrayQueue` + `Semaphore`; backpressure threshold (default 80%) blocks or rejects senders; a hot actor cannot grow the queue without bound | `check_backpressure` (`src/mailbox.rs`), `all_messages_delivered` (`tests/proptest.rs`); `tests/stall_regression.rs` |
| T2 | Worker starvation / scheduler stall | DoS | `ActorScheduler` | Fixed worker pool with priority tiers + work stealing; deterministic sim module replays fault-injected schedules to prove quiescence | `chaotic_sim_quiesces_with_restart_accounting` (`tests/sim.rs`), `all_messages_delivered` |
| T3 | Restart storm (crash loop) | DoS | `Supervisor`, `SupervisorTree` | max-restarts-within-window rate limiting, escalation actions, degradation controller | `crash_then_supervised_restart_then_roundtrip`, `armed_crash_panics_once_then_disarms`, `check_restart_allowed`, `restarts_respect_max_restarts` (`tests/proptest.rs`) |
| T4 | Forged message provenance (`Message.sender`) | Spoofing | `Message`, `ActorContext::send` | **Not mitigated** — `sender` is plain data; `ActorContext` stamps the actor's own ID, but any code holding a `ActorHandle`/registry can construct arbitrary `Message` values. Documented residual risk: authentication belongs to the transport above this crate | Code review; no authenticity check exists (API surface audit) |
| T5 | Forged RPC envelope accepted | Spoofing | `RpcRequest::from_bytes`, `RpcEnvelope` | **Not mitigated** — deserialization validates structure only; `from`/`correlation_id` are trusted bytes. Documented: RPC is for trusted in-process/protected-channel use | `rpc::from_bytes` returns `Err` on structurally invalid input; see `tests/integration.rs` RPC roundtrips |
| T6 | Malicious `rkyv` bytes cause UB | Tampering/DoS | `zero_copy` module (`rkyv` feature) | `rkyv::access` performs full runtime archive validation before any access; failures map to `Error::serialization`, never raw memory reinterpretation | `decode_rkyv_ref` error path (`src/zero_copy/rkyv_impl.rs:64`); roundtrip tests in `tests/integration.rs` |
| T7 | `unsafe` memory-pool corruption | Tampering | `memory_pool` (opt-in `unsafe-pool` feature) | The crate's only `unsafe` is isolated behind the loudly documented `unsafe-pool` feature; default builds carry `#![deny(unsafe_code)]` | Not covered by a dedicated test — documented residual risk (review-gated SAFETY comments) |
| T8 | Secret/payload leakage via `Debug` | Info disclosure | `Message`, `MessagePayload::Custom(Vec<u8>)` | **Not mitigated** — derived `Debug` renders full payload bytes; callers must not log messages. Documented residual risk | Code review |

## Repudiation

The runtime keeps no audit trail of message delivery, spawn, or kill events.
`SchedulerStats`/`RegistryStats` are point-in-time counters only. Attribution
of actions to actors is impossible after the fact — accepted, because the
crate assumes a single trust domain (see scope).

## Out of Scope

- Network transport security: actor-kit never opens sockets; who may send
  messages is decided by whoever exposes the process.
- Caller-side priority abuse: any sender may mark messages
  `Priority::Critical`; there is no per-sender priority quota. A trusted
  in-process caller is assumed.
- `sim` module: test-only by construction (seeded single-threaded scheduler).

## Residual Risks

- **R1 (Medium, accepted):** No message authenticity (T4/T5). Any component
  in the process can impersonate any actor or forge RPC envelopes. Rationale:
  actor-kit is an in-process runtime; adding MACs would push transport
  concerns into a layer that deliberately has none. Mitigation for exposed
  deployments: authenticate and authorize at the network boundary before
  bytes reach `from_bytes`/`send`.
- **R2 (Low, accepted):** `Priority` is caller-asserted, so a buggy or
  hostile component can starve `Normal` traffic with `Critical` floods.
  Backpressure (T1) still bounds memory; only ordering fairness is affected.
- **R3 (Low, accepted):** `ActorId::new()` uses UUID v4 from the OS RNG —
  unguessable in practice, but IDs are capability-ish tokens handed out
  freely; holding an ID grants no capability by itself (send goes through
  the scheduler), so the exposure is naming only.
