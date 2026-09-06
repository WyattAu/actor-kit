//! # actor-kit
//!
//! A work-stealing actor runtime with OTP-style supervision, designed for
//! hosting 100,000+ actors per node with efficient load balancing.
//!
//! ## How this differs from task-per-actor runtimes
//!
//! Most Rust actor frameworks (`ractor`, `kameo`) run each actor as its own
//! tokio task: the async runtime multiplexes tasks onto its worker threads, and
//! "scheduling" is just task polling. `actor-kit` takes a different approach:
//!
//! - **Actors are not tasks.** An actor is a registry entry (ID + state +
//!   bounded mailbox). Work arrives as `Task` items on crossbeam deques.
//! - **A fixed pool of OS worker threads** pulls work from (1) a priority
//!   injector, (2) their local FIFO deque, (3) a global injector, then
//!   (4) steals from other workers — in that order. Idle workers back off
//!   (spin, then sleep) instead of burning a reactor tick per actor.
//! - **Per-actor mailbox with hard backpressure.** Bounded lock-free
//!   `ArrayQueue` + tokio `Semaphore`; senders block or fail once the
//!   mailbox crosses its backpressure threshold (default 80% of capacity).
//!   A hot actor cannot grow an unbounded task queue.
//! - **Supervision is data, not tasks.** Erlang/OTP strategies
//!   (OneForOne / OneForAll / RestForOne / SimpleOneForOne), restart
//!   policies (Permanent / Transient / Temporary), max-restarts-within-window
//!   rate limiting, escalation, and hierarchical [`SupervisorTree`]s are
//!   plain data structures you drive explicitly — no hidden per-supervisor
//!   tokio tasks, no implicit restart side effects.
//!
//! The trade-offs are honest: message handlers here are state-machine steps
//! (state lives in the registry, not in an async frame), so actors that need
//! to `.await` I/O mid-handler are a better fit for task-per-actor frameworks.
//! This runtime shines when you have very many mostly-CPU-light actors,
//! want predictable memory per actor, and need OTP-style fault tolerance
//! semantics with real work stealing.
//!
//! ## Example
//!
//! ```no_run
//! use actor_kit::{ActorBuilder, ActorScheduler, MessagePayload, SchedulerConfig};
//! use std::sync::Arc;
//!
//! # fn main() -> actor_kit::Result<()> {
//! // Create and start a 4-worker work-stealing scheduler.
//! let scheduler = Arc::new(ActorScheduler::new(SchedulerConfig::new().workers(4)));
//! scheduler.start()?;
//!
//! // Spawn an actor and start it.
//! let handle = ActorBuilder::new().name("my-actor").spawn(&scheduler)?;
//! # actor_kit::rt().block_on(async {
//! handle.start().await?;
//!
//! // Send messages (backpressured, bounded mailbox).
//! handle.send(MessagePayload::Custom(vec![1, 2, 3])).await?;
//! assert!(handle.is_running());
//! # Ok::<(), actor_kit::Error>(()) }).unwrap();
//! # scheduler.stop();
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature flags
//!
//! - `std` *(default)* — passthrough; the runtime is std-based.
//! - `serde` — typed RPC (`rpc` module), serde impls on public types.
//! - `zero-copy` — rkyv-backed zero-copy message path (`zero_copy` module).
//! - `unsafe-pool` — bump-allocator memory pool (`memory_pool` module).
//!   **The only `unsafe` code in the crate lives behind this flag.**
//! - `sim` — deterministic simulation testing (`sim` module): a seeded
//!   single-threaded scheduler replacement, virtual clock, fault injection,
//!   and seed-replay traces. TigerBeetle-style DST; see the `sim` module docs.
//! - `full` — all of the above.
//!
//! The crate carries `#![deny(unsafe_code)]`; `memory_pool` (behind
//! `unsafe-pool`) is the single, loudly documented exception.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
mod executor;
mod handle;
mod mailbox;
#[cfg(feature = "unsafe-pool")]
pub mod memory_pool;
pub mod policy;
pub mod queue;
mod registry;
#[cfg(feature = "serde")]
pub mod rpc;
mod scheduler;
#[cfg(feature = "sim")]
pub mod sim;
pub mod supervisor;
#[cfg(feature = "zero-copy")]
pub mod zero_copy;

pub use error::{Error, Result};
pub use executor::{ActorExecutor, ExecutionResult, NullExecutor};
pub use handle::{ActorBuilder, ActorHandle};
pub use mailbox::{Mailbox, MailboxConfig};
pub use policy::{NoopPolicy, ResourcePolicy};
pub use queue::{create_local_queue, PriorityQueue, WorkQueue, WorkStealer};
pub use registry::{ActorRegistry, ActorState, RegistryStats};
#[cfg(feature = "serde")]
pub use rpc::{
    process_rpc_message, RpcClient, RpcEnvelope, RpcError, RpcHandler, RpcMessage, RpcRegistry,
    RpcRequest, RpcResponse,
};
pub use scheduler::{
    ActorScheduler, SchedulerConfig, SchedulerStats, StealerRegistry, WorkerStatsInfo,
};
pub use supervisor::{
    ActorConfig, ChildSpec, ChildState, DegradationController, DegradationDecision,
    EscalationAction, ExitReason, FnResourceMonitor, PressureLevel, ResourceMonitor, RestartPolicy,
    StaticResourceMonitor, SupervisedChild, SupervisionStrategy, Supervisor, SupervisorError,
    SupervisorHandle, SupervisorStats, SupervisorTree, SupervisorTreeStats,
};

use std::sync::Arc;
use uuid::Uuid;

/// Unique identifier for an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorId(pub Uuid);

impl ActorId {
    /// Generate a new random actor ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ActorId {
    fn default() -> Self {
        Self::new()
    }
}

/// Priority level for actor messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Priority {
    /// Low priority (background tasks)
    Low = 0,
    /// Normal priority (default)
    #[default]
    Normal = 1,
    /// High priority (time-sensitive)
    High = 2,
    /// Critical priority (system messages)
    Critical = 3,
}

/// A message sent to an actor.
#[derive(Debug, Clone)]
pub struct Message {
    /// Sender actor ID (None for system messages)
    pub sender: Option<ActorId>,
    /// Message type/payload
    pub payload: MessagePayload,
    /// Message priority
    pub priority: Priority,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            sender: None,
            payload: MessagePayload::Empty,
            priority: Priority::Normal,
        }
    }
}

/// Payload of a message.
#[derive(Debug, Clone, Default)]
pub enum MessagePayload {
    /// Start the actor
    Start,
    /// Stop the actor
    Stop,
    /// Custom binary payload
    Custom(Vec<u8>),
    /// System signal
    Signal(Signal),
    /// Default (empty)
    #[default]
    Empty,
}

/// System signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// Pause execution
    Pause,
    /// Resume execution
    Resume,
    /// Restart the actor
    Restart,
}

/// Trait for actor behavior.
pub trait Actor: Send + Sync + 'static {
    /// Handle a message.
    fn handle(
        &mut self,
        msg: Message,
        ctx: &ActorContext,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Context provided to actors during message handling.
pub struct ActorContext {
    /// Actor's own ID
    pub id: ActorId,
    /// Scheduler handle for spawning work
    scheduler: Arc<ActorScheduler>,
}

impl ActorContext {
    /// Send a message to another actor.
    pub async fn send(&self, target: ActorId, payload: MessagePayload) -> Result<()> {
        self.scheduler
            .send(
                target,
                Message {
                    sender: Some(self.id),
                    payload,
                    priority: Priority::Normal,
                },
            )
            .await
    }

    /// Send a high-priority message to another actor.
    pub async fn send_high(&self, target: ActorId, payload: MessagePayload) -> Result<()> {
        self.scheduler
            .send(
                target,
                Message {
                    sender: Some(self.id),
                    payload,
                    priority: Priority::High,
                },
            )
            .await
    }
}

/// Internal helper: a minimal single-threaded runtime for doctests/examples.
#[doc(hidden)]
pub fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build runtime")
}
