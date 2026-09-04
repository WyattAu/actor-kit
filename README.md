# actor-kit

A work-stealing actor runtime for Rust with OTP-style supervision trees, built
for hosting **100,000+ actors per node** with efficient load balancing and
bounded, backpressured mailboxes.

```toml
[dependencies]
actor-kit = "0.1"
```

## Quick example

```rust
use actor_kit::{ActorBuilder, ActorScheduler, MessagePayload, SchedulerConfig};
use std::sync::Arc;

fn main() -> actor_kit::Result<()> {
    // 4-worker work-stealing scheduler.
    let scheduler = Arc::new(ActorScheduler::new(SchedulerConfig::new().workers(4)));
    scheduler.start()?;

    let handle = ActorBuilder::new().name("my-actor").spawn(&scheduler)?;

    actor_kit::rt().block_on(async {
        handle.start().await?;                       // Start signal
        handle.send(MessagePayload::Custom(vec![1, 2, 3])).await?;
        assert!(handle.is_running());
        Ok::<(), actor_kit::Error>(())
    })?;

    scheduler.stop();
    Ok(())
}
```

Supervision (Erlang/OTP style, data-driven):

```rust
use actor_kit::{ChildSpec, RestartPolicy, SupervisionStrategy, SupervisorTree, ExitReason};
use std::time::Duration;

let mut tree = SupervisorTree::new(SupervisionStrategy::one_for_one(5, Duration::from_secs(60)));
let root = tree.root();

let child = tree.start_child_under(
    root,
    ChildSpec::new("worker-1").restart_policy(RestartPolicy::Permanent),
)?;

// ... on crash:
tree.handle_child_exit(root, "worker-1", ExitReason::Error("boom".into())).await?;
// child is now Restarting; the supervisor counted it against max_restarts.
```

## How it differs from task-per-actor runtimes

In `ractor`/`kameo`, each actor *is* a tokio task; scheduling is the async
runtime polling frames, and mailboxes are MPSC channels. `actor-kit` decouples
actors from tasks:

- **Actors are registry entries** (ID + state + bounded mailbox), not tasks.
- **A fixed pool of OS workers** pulls from a priority injector → local FIFO
  deque → global injector → **steals** from other workers (crossbeam deques),
  with spin/sleep backoff when idle.
- **Mailboxes backpressure**: lock-free `crossbeam_queue::ArrayQueue` +
  tokio `Semaphore`, backpressure flag at 80% of capacity (configurable).
  `send` waits, `try_send` fails fast — a hot actor can never grow unbounded.
- **Supervision is data**: OneForOne / OneForAll / RestForOne /
  SimpleOneForOne, Permanent / Transient / Temporary restart policies,
  max-restarts-per-window rate limiting, escalation actions
  (Escalate / ShutdownNode / GiveUp), hierarchical `SupervisorTree`s.
  You drive exits explicitly; there is no hidden per-supervisor task.

## Honest comparison

| | actor-kit | ractor | kameo | actix (actors) |
|---|---|---|---|---|
| Execution model | fixed worker pool + work stealing | 1 tokio task/actor | 1 tokio task/actor | arbiter-thread actors |
| Supervision strategies | OneForOne/OneForAll/RestForOne/Simple + trees, restart window, escalation | one-for-one/all (supervisor actors) | experimental supervision | none (lifecycle hooks only) |
| Backpressure | bounded ArrayQueue + Semaphore, threshold flag | unbounded MPSC (bounded variant exists) | bounded, backpressured | bounded |
| Message typing | binary payloads + typed RPC (serde) | typed enums | typed messages | typed messages |
| Handler shape | sync state-machine step | async fn | async fn | async fn |
| `.await` I/O mid-handler | no (use RPC to an I/O actor) | yes | yes | yes |
| Panic containment | worker survives; actor → Failed, mailbox drained | task-level | task-level | arbiter-dependent |

**When to prefer actor-kit:** very many cheap actors, predictable per-actor
memory, OTP fault-tolerance semantics, CPU-light handlers where task-per-actor
overhead and unbounded mailboxes hurt.

**When to prefer ractor/kameo:** handlers that need to await I/O directly,
rich typed message enums without a serde RPC layer, first-party ecosystem.

## Feature flags

| feature | enables |
|---|---|
| `std` *(default)* | passthrough; the runtime is std-based |
| `serde` | typed RPC (`rpc`), serde impls, wire serialization (bincode) |
| `zero-copy` | rkyv zero-copy message path (`zero_copy`) |
| `unsafe-pool` | bump-allocator memory pool (`memory_pool`) |
| `full` | all of the above |

### Unsafe-code policy

The crate carries `#![deny(unsafe_code)]`. The **only** `unsafe` code lives in
`memory_pool` behind the opt-in `unsafe-pool` feature: a raw-pointer bump
arena with a hand-rolled free list and manual `Send`/`Sync` impls, each with
SAFETY comments and documented invariants (module docs). Everything else —
scheduler, mailboxes, supervision, RPC, zero-copy serialization — is 100% safe.

## Resource policy hook

Spawn/send admission is pluggable via the `ResourcePolicy` trait
(`actor_kit::policy`), replacing a hard dependency on any specific quota
system. Default is `NoopPolicy` (admit all):

```rust
use actor_kit::policy::ResourcePolicy;
use actor_kit::Result;

struct MaxActors(usize, std::sync::atomic::AtomicUsize);

impl ResourcePolicy for MaxActors {
    fn admit_actor(&self) -> Result<()> {
        let prev = self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if prev >= self.0 { /* reject */ }
        Ok(())
    }
    fn release_actor(&self) { self.1.fetch_sub(1, std::sync::atomic::Ordering::SeqCst); }
}
```

## Benchmarks

```sh
cargo bench
```

Three criterion suites: `spawn_throughput`, `message_roundtrip`,
`steal_contention`. Numbers are machine-dependent; run your own.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option.
