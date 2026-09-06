//! Deterministic Simulation Testing (DST) for actor-kit.
//!
//! TigerBeetle/VOPR-style testing applied to this actor runtime: the entire
//! world — scheduling decisions, faults, time — is driven by a single seeded
//! RNG on a single thread, so **same seed ⇒ identical execution trace**.
//!
//! # Why a custom sim
//!
//! The work-stealing scheduler is exactly what a generic tokio simulator
//! cannot model: actors here are *not* tokio tasks, so there is no task
//! polling surface to intercept. The interesting scheduling surface is
//! "which ready task does the pool run next", and this module models that
//! decision directly (see [`SchedulingPolicy`]) while running every chosen
//! task through the **real** production pipeline — real [`ActorRegistry`],
//! real `Mailbox`es with their permit accounting, real panic containment via
//! [`crate::ActorScheduler`]'s processing path. Simulation code decides
//! *what runs next*; production code decides *what running it does*. There
//! is no model of the runtime to drift from the runtime.
//!
//! Not simulated (and covered elsewhere): the lock-free internals of the
//! crossbeam deques and the mailbox `ArrayQueue`/`Semaphore` race windows —
//! micro-interleavings are loom territory; macro behaviors (ordering,
//! backpressure, faults, supervision) are sim territory. See the README's
//! "Deterministic Simulation Testing" section.
//!
//! # What runs where
//!
//! | Concern | Real code (under test) | Sim code |
//! |---|---|---|
//! | registry, actor state | `ActorRegistry` | — |
//! | mailbox + permit accounting | `Mailbox` | — |
//! | processing, panic containment, slot release | scheduler pipeline (`sim_process_task` seam) | — |
//! | supervision decisions, restart accounting | `SupervisorTree` | — |
//! | which task runs next | — | seeded RNG + policy |
//! | faults (crash / reject / delay / duplicate) | — | seeded RNG draws |
//! | time | — | [`SimClock`] |
//! | workload | — | seeded message generator |
//!
//! # Determinism contract
//!
//! `Sim::new(seed)` builds the world from a fixed config; every "random"
//! event draws from one xoshiro256++ stream in a fixed program order. No
//! wall clock, no OS threads, no `Uuid::new_v4` (actor IDs derive from
//! ordinals), no hash-map iteration order on any decision path. Same seed +
//! same config ⇒ byte-identical [`TraceOp`] log, witnessed by
//! [`SimOutcome::trace_hash`].
//!
//! # Invariants audited on every run
//!
//! 1. **Slot/permit conservation** (the 0.1.0 drain-stall bug class): for
//!    every actor, at every step,
//!    `mailbox.len() == delivered − processed − dropped − queued`.
//!    A leaked mailbox permit or slot unbalances this within one step.
//! 2. **Delivery accounting**: at quiescence every injected message is
//!    either processed or provably lost (crash drain / dead target /
//!    fault rejection). With `duplicate_prob == 0` every message is
//!    processed at most once (exactly-once); with duplicates enabled,
//!    at-least-once over non-lost messages.
//! 3. **Termination** (the stall bug's ghost): `run_to_quiescence` must
//!    reach quiescence within the step budget — sustained workloads
//!    complete.
//! 4. **Restart accounting**: supervised restarts never exceed
//!    `max_restarts` (cross-checked against the supervisor's own
//!    accounting); beyond it the actor is permanently dead and never
//!    processes again.
//!
//! # Example
//!
//! ```
//! use actor_kit::sim::{FaultConfig, SchedulingPolicy, Sim, SimConfig};
//!
//! fn chaos_config() -> SimConfig {
//!     let mut c = SimConfig::default();
//!     c.actors = 4;
//!     c.messages = 512;
//!     c.mailbox.capacity = 64;
//!     c.faults = FaultConfig::chaos(0.01);
//!     c.policy = SchedulingPolicy::Random;
//!     c
//! }
//!
//! let outcome = Sim::with_config(42, chaos_config()).run_to_quiescence();
//! assert!(outcome.quiesced, "workload must drain");
//! let replay = Sim::with_config(42, chaos_config()).run_to_quiescence();
//! assert_eq!(outcome.trace_hash, replay.trace_hash, "same seed ⇒ same trace");
//! ```

use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::executor::ActorExecutor;
use crate::queue::Task;
use crate::registry::ActorState;
use crate::supervisor::{
    ChildSpec, ExitReason, RestartPolicy, SupervisionStrategy, SupervisorTree,
};
use crate::{ActorId, ActorRegistry, MailboxConfig, Message, MessagePayload, Priority};

pub mod clock;
pub mod executor;
pub mod faults;
pub mod scheduler;

pub use clock::{SimClock, SimInstant};
pub use executor::{ExecRecord, ExecTag, SimExecutor, SIM_CRASH_MESSAGE};
pub use faults::FaultConfig;
pub use scheduler::SchedulingPolicy;

use faults::SimRng;
use scheduler::{ReadySet, SimTask};

/// Default step budget for [`Sim::run_to_quiescence`].
pub const DEFAULT_STEP_BUDGET: u64 = 4_000_000;

/// Simulated tick length: one scheduling round advances the virtual clock by
/// this much. Only tick *counts* matter (delays are tick quantities).
const TICK: Duration = Duration::from_millis(1);

/// Gap in ticks between a delivery and its duplicate copy.
const DUPLICATE_GAP_TICKS: u64 = 2;

/// Deterministic sim identity base: actor IDs derive from `(ordinal,
/// incarnation)` — no OS randomness on the sim path.
const SIM_ID_BASE: u128 = 0xA77C_70B1_5160_0000;

/// Derive the deterministic actor ID for `ordinal`'s `incarnation`.
pub(crate) fn sim_actor_id(ordinal: u64, incarnation: u64) -> ActorId {
    ActorId(Uuid::from_u128(
        SIM_ID_BASE | ((ordinal as u128) << 64) | incarnation as u128,
    ))
}

/// Workload + world configuration for a simulation run.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Number of actors spawned (each supervised, `Permanent` restarts).
    pub actors: u64,
    /// Application messages the workload injects (identities `0..messages`).
    pub messages: u64,
    /// Mailbox config for every actor (small capacities exercise
    /// backpressure; default 64).
    pub mailbox: MailboxConfig,
    /// Task-selection policy (the modeled scheduler).
    pub policy: SchedulingPolicy,
    /// Fault injection schedule.
    pub faults: FaultConfig,
    /// `max_restarts` for the root supervisor's one-for-one strategy.
    /// `0` means unlimited (the supervisor's convention).
    pub max_restarts: u32,
    /// Workload injection rate in messages per simulated tick.
    pub injection_rate: f64,
    /// Task-processing draws per tick (scheduling decisions per round).
    pub processing_per_tick: u64,
    /// Blocked-delivery retries per tick (round-robin rotation).
    pub blocked_retries_per_tick: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            actors: 4,
            messages: 256,
            mailbox: MailboxConfig {
                capacity: 64,
                priority_queue: true,
                backpressure_threshold: 0.8,
            },
            policy: SchedulingPolicy::Random,
            faults: FaultConfig::default(),
            max_restarts: 3,
            injection_rate: 4.0,
            processing_per_tick: 4,
            blocked_retries_per_tick: 64,
        }
    }
}

/// Why a send was rejected (transport semantics of the sim).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Fault injection rejected the send (models a fail-fast transport).
    Fault,
    /// Target actor does not exist or is not running.
    DeadTarget,
}

/// One entry in the deterministic operation trace.
///
/// Identities are sim ordinals and message numbers — never `ActorId` — so
/// the trace (and its hash) is a pure function of the seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceOp {
    /// Actor spawned (registry entry + supervisor child created).
    Spawn {
        /// Actor ordinal.
        ord: u64,
    },
    /// Workload injected a message.
    Send {
        /// Message identity.
        mid: u64,
        /// Target actor ordinal.
        to: u64,
    },
    /// Workload injected a batch.
    BatchSend {
        /// First message identity.
        first: u64,
        /// Batch size.
        len: u64,
        /// Target actor ordinal.
        to: u64,
    },
    /// Send rejected (message lost — at-most-once transport semantics).
    Reject {
        /// Message identity.
        mid: u64,
        /// Target actor ordinal.
        to: u64,
        /// Why the send was rejected.
        reason: RejectReason,
    },
    /// Batch delivered to the target's mailbox + ready set.
    Deliver {
        /// First message identity.
        first: u64,
        /// Batch size.
        len: u64,
        /// Target actor ordinal.
        to: u64,
    },
    /// Delivery hit a full mailbox; parked for retry (backpressure event).
    Blocked {
        /// First message identity.
        first: u64,
        /// Batch size.
        len: u64,
        /// Target actor ordinal.
        to: u64,
    },
    /// The runtime processed a message (executor observation).
    Process {
        /// Message identity.
        mid: u64,
        /// Target actor ordinal.
        to: u64,
    },
    /// The runtime processed a system message.
    ProcessSystem {
        /// Target actor ordinal.
        to: u64,
    },
    /// Actor crashed mid-message; the runtime's panic containment drained
    /// `dropped` buffered artifacts.
    Crash {
        /// Actor ordinal.
        to: u64,
        /// Artifacts destroyed by the containment drain.
        dropped: u64,
    },
    /// A queued task targeted a dead actor and was discarded.
    Discard {
        /// First message identity.
        first: u64,
        /// Batch size.
        len: u64,
        /// Target actor ordinal.
        to: u64,
    },
    /// Crash reported to the supervisor: `allowed` = restart, else
    /// escalation.
    Restart {
        /// Actor ordinal.
        to: u64,
        /// Whether the supervisor granted a restart.
        allowed: bool,
    },
    /// Actor escalated past `max_restarts`: permanently killed.
    Kill {
        /// Actor ordinal.
        to: u64,
        /// Artifacts destroyed by the final mailbox clear.
        dropped: u64,
    },
}

impl TraceOp {
    /// Stable little-endian encoding for hashing (tag byte + fields).
    fn encode_into(&self, buf: &mut Vec<u8>) {
        fn push(buf: &mut Vec<u8>, tag: u8, fields: &[u64]) {
            buf.push(tag);
            for field in fields {
                buf.extend_from_slice(&field.to_le_bytes());
            }
        }
        match self {
            TraceOp::Spawn { ord } => push(buf, 0, &[*ord]),
            TraceOp::Send { mid, to } => push(buf, 1, &[*mid, *to]),
            TraceOp::BatchSend { first, len, to } => push(buf, 2, &[*first, *len, *to]),
            TraceOp::Reject { mid, to, reason } => push(
                buf,
                3,
                &[*mid, *to, match reason {
                    RejectReason::Fault => 0,
                    RejectReason::DeadTarget => 1,
                }],
            ),
            TraceOp::Deliver { first, len, to } => push(buf, 4, &[*first, *len, *to]),
            TraceOp::Blocked { first, len, to } => push(buf, 5, &[*first, *len, *to]),
            TraceOp::Process { mid, to } => push(buf, 6, &[*mid, *to]),
            TraceOp::ProcessSystem { to } => push(buf, 7, &[*to]),
            TraceOp::Crash { to, dropped } => push(buf, 8, &[*to, *dropped]),
            TraceOp::Discard { first, len, to } => push(buf, 9, &[*first, *len, *to]),
            TraceOp::Restart { to, allowed } => push(buf, 10, &[*to, u64::from(*allowed)]),
            TraceOp::Kill { to, dropped } => push(buf, 11, &[*to, *dropped]),
        }
    }
}

/// Aggregate counters for a simulation run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SimStats {
    /// Workload messages injected.
    pub injected: u64,
    /// Sends rejected by fault or dead target (message-level).
    pub rejected: u64,
    /// Deliveries accepted into a mailbox (duplicates included).
    pub delivered: u64,
    /// Application messages the runtime processed.
    pub processed: u64,
    /// Injected mid-message crashes.
    pub crashed: u64,
    /// Supervised restarts granted.
    pub restarted: u64,
    /// Actors permanently killed after escalation.
    pub escalated: u64,
    /// Lost artifacts: crash-drained mailbox copies, discarded tasks for
    /// dead actors, restart-dropped mail.
    pub dropped: u64,
    /// Deliveries parked on a full mailbox (events, including retries).
    pub blocked: u64,
    /// Deliveries that succeeded after having been parked at least once.
    pub unblocked: u64,
    /// Delayed deliveries that landed.
    pub delayed: u64,
    /// Duplicate copies delivered.
    pub duplicates: u64,
    /// System (`Start`) messages processed.
    pub system_processed: u64,
}

/// Result of a simulation run.
#[derive(Debug, Clone)]
pub struct SimOutcome {
    /// Seed the run was built from.
    pub seed: u64,
    /// True when the world drained: no pending mail, mailboxes empty.
    pub quiesced: bool,
    /// Scheduling rounds executed.
    pub steps: u64,
    /// FNV-1a hash of the full operation trace — the determinism witness.
    pub trace_hash: u64,
    /// Number of trace operations.
    pub trace_ops: usize,
    /// Aggregate counters.
    pub stats: SimStats,
}

/// Per-actor ledger used by the conservation audit.
#[derive(Debug, Default, Clone)]
struct ActorLedger {
    /// Mailbox copies accepted (duplicates included).
    delivered: u64,
    /// Messages processed per executor observation.
    consumed: u64,
    /// Lost artifacts (crash drains, discarded tasks, restart drops).
    dropped: u64,
    /// Application messages currently sitting in queued tasks.
    queued: u64,
    /// Current incarnation counter.
    incarnation: u64,
    /// Registered (not escalated/killed).
    alive: bool,
}

/// A delivery attempt payload (may carry a batch).
#[derive(Debug, Clone)]
struct DeliverPayload {
    ord: u64,
    msgs: Vec<Message>,
    mids: Vec<u64>,
    was_blocked: bool,
}

/// Time-ordered delayed delivery (min-heap by `(at, seq)`).
#[derive(Debug, Clone)]
struct DeliverLater {
    at: u64,
    seq: u64,
    payload: DeliverPayload,
}

impl PartialEq for DeliverLater {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}
impl Eq for DeliverLater {}
impl PartialOrd for DeliverLater {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DeliverLater {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; reverse for min-heap by (at, seq).
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

/// A deterministic simulation of the actor runtime.
///
/// See the [module docs](self) for the design, the invariants, and the
/// seed-replay workflow.
pub struct Sim {
    seed: u64,
    config: SimConfig,
    rng: SimRng,
    clock: Arc<SimClock>,
    registry: Arc<ActorRegistry>,
    executor: Arc<SimExecutor>,
    executor_dyn: Arc<dyn ActorExecutor>,
    tree: SupervisorTree,
    root: ActorId,
    /// Current incarnation ID per actor ordinal.
    actors: Vec<ActorId>,
    /// Actor ID bits → ordinal for *current* incarnations (point lookups
    /// only; iteration order is never consulted).
    ordinals: BTreeMap<u128, u64>,
    ledger: Vec<ActorLedger>,
    ready: ReadySet,
    delayed: BinaryHeap<DeliverLater>,
    blocked: VecDeque<DeliverPayload>,
    next_seq: u64,
    next_mid: u64,
    /// Per-mid processed counts (identity audit).
    mid_times: Vec<u32>,
    /// Per-mid lost flags (identity audit).
    mid_lost: Vec<bool>,
    tick: u64,
    trace: Vec<TraceOp>,
    stats: SimStats,
}

impl Sim {
    /// Build a sim with the default configuration and the given seed.
    pub fn new(seed: u64) -> Sim {
        Self::with_config(seed, SimConfig::default())
    }

    /// Build a sim with an explicit configuration and seed.
    pub fn with_config(seed: u64, config: SimConfig) -> Sim {
        let mailbox_config = config.mailbox.clone();
        let n_actors = config.actors;
        let n_messages = config.messages;
        let registry = Arc::new(ActorRegistry::with_mailbox_config(mailbox_config));
        let executor = Arc::new(SimExecutor::new());
        let executor_dyn: Arc<dyn ActorExecutor> = executor.clone();
        let tree = SupervisorTree::new(SupervisionStrategy::one_for_one(
            config.max_restarts,
            // Restart windows use the supervisor's wall clock; a window far
            // larger than any sim's *real* duration keeps accounting
            // deterministic (see README: sim time ≠ supervisor window time).
            Duration::from_secs(3600),
        ));
        let root = tree.root();

        let mut sim = Sim {
            seed,
            config,
            rng: SimRng::new(seed),
            clock: Arc::new(SimClock::new()),
            registry,
            executor,
            executor_dyn,
            tree,
            root,
            actors: Vec::with_capacity(n_actors as usize),
            ordinals: BTreeMap::new(),
            ledger: Vec::with_capacity(n_actors as usize),
            ready: ReadySet::new(),
            delayed: BinaryHeap::new(),
            blocked: VecDeque::new(),
            next_seq: 0,
            next_mid: 0,
            mid_times: vec![0; n_messages as usize],
            mid_lost: vec![false; n_messages as usize],
            tick: 0,
            trace: Vec::new(),
            stats: SimStats::default(),
        };

        for ord in 0..n_actors {
            sim.spawn_actor(ord);
        }
        sim
    }

    /// Mutable access to the run configuration.
    ///
    /// World-shape fields (`actors`, `messages`) are captured at
    /// construction; changing them afterwards has no effect.
    pub fn config_mut(&mut self) -> &mut SimConfig {
        &mut self.config
    }

    /// The virtual clock (observes simulated time).
    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// The real registry backing this simulation.
    pub fn registry(&self) -> &Arc<ActorRegistry> {
        &self.registry
    }

    /// The sim executor (observation oracle).
    pub fn executor(&self) -> &SimExecutor {
        &self.executor
    }

    /// The full operation trace of the run so far.
    pub fn trace(&self) -> &[TraceOp] {
        &self.trace
    }

    /// FNV-1a hash of the operation trace — the determinism witness.
    pub fn trace_hash(&self) -> u64 {
        let mut buf = Vec::with_capacity(self.trace.len() * 40);
        for op in &self.trace {
            op.encode_into(&mut buf);
        }
        fnv1a(&buf)
    }

    /// Counters so far.
    pub fn stats(&self) -> SimStats {
        self.stats
    }

    /// Spawn actor `ord`: registry entry + supervisor child + `Start` mail.
    fn spawn_actor(&mut self, ord: u64) {
        let id = sim_actor_id(ord, 0);
        let name = actor_name(ord, 0);
        self.registry
            .register_named(id, Some(name))
            .expect("sim actor ids are unique");
        self.tree
            .start_child_under(
                self.root,
                ChildSpec::new(supervisor_name(ord)).restart_policy(RestartPolicy::Permanent),
            )
            .expect("sim child names are unique");
        self.actors.push(id);
        self.ordinals.insert(id.0.as_u128(), ord);
        self.ledger.push(ActorLedger {
            incarnation: 0,
            alive: true,
            ..ActorLedger::default()
        });
        self.trace.push(TraceOp::Spawn { ord });
        // System Start: no fault rolls (system messages are reliable).
        let start = Message {
            sender: None,
            payload: MessagePayload::Start,
            priority: Priority::Normal,
        };
        self.deliver_or_block(ord, vec![start], Vec::new(), 0);
    }

    /// Route a delivery into mailbox + ready set, postponing by `delay` if
    /// nonzero (delayed deliveries land in the timer heap).
    fn deliver_or_block(&mut self, ord: u64, msgs: Vec<Message>, mids: Vec<u64>, delay: u64) {
        if delay > 0 {
            self.next_seq += 1;
            self.delayed.push(DeliverLater {
                at: self.tick + delay,
                seq: self.next_seq,
                payload: DeliverPayload {
                    ord,
                    msgs,
                    mids,
                    was_blocked: false,
                },
            });
            return;
        }
        self.attempt_delivery(ord, msgs, mids, false);
    }

    /// One delivery attempt against the live world.
    ///
    /// Mirrors `ActorScheduler::send`/`send_batch` semantics: state check →
    /// bounded mailbox push (blocking modeled as a retry park) →
    /// priority-routed task push. Batches are atomic: fully delivered or
    /// parked.
    fn attempt_delivery(
        &mut self,
        ord: u64,
        msgs: Vec<Message>,
        mids: Vec<u64>,
        was_blocked: bool,
    ) {
        let id = self.actors[ord as usize];
        let state = self.registry.get_state(&id);
        if matches!(
            state,
            None | Some(ActorState::Stopped) | Some(ActorState::Failed)
        ) {
            // At-most-once across death: pending mail to a dead actor is lost.
            for &mid in &mids {
                self.mark_lost(mid);
                self.stats.rejected += 1;
                self.trace.push(TraceOp::Reject {
                    mid,
                    to: ord,
                    reason: RejectReason::DeadTarget,
                });
            }
            return;
        }
        let mailbox = self
            .registry
            .get_mailbox(&id)
            .expect("live ord has a mailbox");

        // Atomic batch precondition (see module docs: models `send_batch`
        // as all-or-park; the real `send_batch` can strand partial copies on
        // a mid-batch full — a documented sharp edge the sim deliberately
        // does not replicate).
        if mailbox.remaining_capacity() < msgs.len() {
            self.stats.blocked += 1;
            if let Some(&first) = mids.first() {
                // Trace the park event once (retries re-park silently —
                // otherwise sustained-backpressure runs bloat the trace).
                if !was_blocked {
                    self.trace.push(TraceOp::Blocked {
                        first,
                        len: mids.len() as u64,
                        to: ord,
                    });
                }
            }
            self.blocked.push_back(DeliverPayload {
                ord,
                msgs,
                mids,
                was_blocked: true,
            });
            return;
        }

        for msg in &msgs {
            mailbox
                .try_send(msg.clone())
                .expect("capacity precondition holds");
        }
        let priority = msgs
            .iter()
            .map(|m| m.priority)
            .max()
            .unwrap_or(Priority::Normal);
        let task = Task {
            actor_id: id,
            message: msgs[0].clone(),
            priority,
            additional_messages: msgs[1..].to_vec(),
        };

        let ledger = &mut self.ledger[ord as usize];
        ledger.delivered += msgs.len() as u64;
        ledger.queued += mids.len() as u64;
        self.stats.delivered += msgs.len() as u64;
        if was_blocked {
            self.stats.unblocked += 1;
        }

        if let Some(&first) = mids.first() {
            self.trace.push(TraceOp::Deliver {
                first,
                len: mids.len() as u64,
                to: ord,
            });
        }
        self.ready.push(SimTask {
            ord,
            task,
            mids,
        });
    }

    /// Inject this tick's workload (fixed draw order). The message budget
    /// is the identity-ledger size fixed at construction.
    fn inject_workload(&mut self) {
        let budget = self.mid_times.len() as u64;
        if self.next_mid >= budget {
            return;
        }
        // Poisson-ish: integer part of the rate plus a fractional roll.
        let rate = self.config.injection_rate.max(0.0);
        let base = rate as u64;
        let frac = rate - base as f64;
        let mut attempts = base + u64::from(frac > 0.0 && self.rng.roll(frac));
        attempts = attempts.max(1); // always progress toward the budget
        for _ in 0..attempts {
            if self.next_mid >= budget {
                return;
            }
            self.inject_one();
        }
    }

    /// Inject a single send event (possibly a small batch).
    fn inject_one(&mut self) {
        let budget = self.mid_times.len() as u64;
        let to = self.rng.below(self.config.actors);
        let remaining = budget - self.next_mid;

        // Fixed draw order: batch → priority → reject → delay → duplicate.
        let mut len = 1usize;
        if remaining > 1 && self.rng.roll(1.0 / 8.0) {
            // 1/8 of events are batches of 2..=3 (exercises the batch path).
            let max = remaining.min(3);
            len = self.rng.range(2, max) as usize;
        }
        let priority = if self.rng.roll(0.1) {
            Priority::High
        } else {
            Priority::Normal
        };
        let first = self.next_mid;
        self.next_mid += len as u64;
        self.stats.injected += len as u64;
        let mids: Vec<u64> = (first..first + len as u64).collect();

        if len > 1 {
            self.trace.push(TraceOp::BatchSend {
                first,
                len: len as u64,
                to,
            });
        } else {
            self.trace.push(TraceOp::Send { mid: first, to });
        }

        // Fault roll 1: transport rejection (message lost).
        if self.rng.roll(self.config.faults.send_reject_prob) {
            for &mid in &mids {
                self.mark_lost(mid);
                self.stats.rejected += 1;
                self.trace.push(TraceOp::Reject {
                    mid,
                    to,
                    reason: RejectReason::Fault,
                });
            }
            return;
        }

        // Fault roll 2: delayed delivery.
        let delay = if self.rng.roll(self.config.faults.delay_prob) {
            self.rng.range(
                self.config.faults.delay_ticks_min,
                self.config.faults.delay_ticks_max,
            )
        } else {
            0
        };

        // Fault roll 3: duplicate (second copy lands later).
        let duplicate = self.rng.roll(self.config.faults.duplicate_prob);

        let msgs: Vec<Message> = mids
            .iter()
            .map(|mid| Message {
                sender: None,
                payload: MessagePayload::Custom(mid.to_le_bytes().to_vec()),
                priority,
            })
            .collect();
        self.deliver_or_block(to, msgs.clone(), mids.clone(), delay);
        if duplicate {
            self.stats.duplicates += 1;
            self.deliver_or_block(to, msgs, mids, delay + DUPLICATE_GAP_TICKS);
        }
    }

    /// Deliver everything due this tick.
    fn deliver_due(&mut self) {
        while let Some(later) = self.delayed.peek() {
            if later.at > self.tick {
                break;
            }
            let later = self.delayed.pop().expect("peeked");
            self.stats.delayed += 1;
            self.attempt_delivery(
                later.payload.ord,
                later.payload.msgs,
                later.payload.mids,
                later.payload.was_blocked,
            );
        }
    }

    /// Retry parked deliveries: rotation prevents starvation (the popped
    /// entry is either delivered or pushed to the back).
    fn retry_blocked(&mut self) {
        let mut attempts = self
            .config
            .blocked_retries_per_tick
            .min(self.blocked.len() as u64);
        while attempts > 0 {
            attempts -= 1;
            let payload = self.blocked.pop_front().expect("checked non-empty");
            self.attempt_delivery(
                payload.ord,
                payload.msgs,
                payload.mids,
                payload.was_blocked,
            );
        }
    }

    /// Run up to `steps` scheduling rounds.
    ///
    /// Each round: advance the virtual clock one tick, inject workload, land
    /// due and unblocked deliveries, then make `processing_per_tick` seeded
    /// task-selection decisions, each executed through the real processing
    /// pipeline. Invariants are audited every round; a violation panics with
    /// the seed for replay.
    pub fn run(&mut self, steps: u64) -> SimOutcome {
        let mut executed = 0u64;
        for _ in 0..steps {
            if self.is_quiescent() {
                break;
            }
            executed += 1;
            self.tick += 1;
            self.clock.advance(TICK);

            self.inject_workload();
            self.deliver_due();
            self.retry_blocked();

            for _ in 0..self.config.processing_per_tick {
                if let Some(sim_task) = self
                    .ready
                    .pick(self.config.policy, &mut self.rng, self.tick)
                {
                    self.process_chosen(sim_task);
                } else {
                    break;
                }
            }

            self.audit_counts();
        }

        let quiesced = self.is_quiescent();
        if quiesced {
            self.audit_identities();
        }
        SimOutcome {
            seed: self.seed,
            quiesced,
            steps: executed,
            trace_hash: self.trace_hash(),
            trace_ops: self.trace.len(),
            stats: self.stats,
        }
    }

    /// Run until quiescence or the step budget ([`DEFAULT_STEP_BUDGET`]).
    ///
    /// `outcome.quiesced == false` is a **stall finding**: the workload
    /// cannot drain — exactly how the 0.1.0 drain-stall bug reproduces
    /// (permit exhaustion makes delivery permanently fail).
    pub fn run_to_quiescence(&mut self) -> SimOutcome {
        self.run(DEFAULT_STEP_BUDGET)
    }

    /// Process one chosen task through the real pipeline.
    fn process_chosen(&mut self, sim_task: SimTask) {
        let ord = sim_task.ord();
        let id = sim_task.task.actor_id;
        let app_len = sim_task.app_len() as u64;
        self.ledger[ord as usize].queued -= app_len;

        let state = self.registry.get_state(&id);
        if !matches!(
            state,
            Some(ActorState::Creating) | Some(ActorState::Running)
        ) {
            // Dead target: the real pipeline drops such tasks silently
            // (`process_single_message` matches non-viable states with
            // `_ => {}`); account the artifacts as lost and move on.
            for &mid in &sim_task.mids {
                self.mark_lost(mid);
            }
            if let Some(&first) = sim_task.mids.first() {
                self.trace.push(TraceOp::Discard {
                    first,
                    len: app_len,
                    to: ord,
                });
            }
            self.executor.arm_crash(false);
            return;
        }

        // Fault: crash mid-message (application messages only).
        let crash = app_len > 0 && self.rng.roll(self.config.faults.crash_prob);
        self.executor.arm_crash(crash);

        // Panic containment mirrors the real runtime: production actors run
        // inside tokio tasks, so an actor panic kills the task — not the
        // process. The sim is single-threaded, so the injected crash is
        // contained here and reconciled by `settle_crash` below. Records
        // observed before the fault are still drained; the worker counter is
        // not trusted past the unwind point.
        let registry = &self.registry;
        let executor_dyn = &self.executor_dyn;
        let process = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::ActorScheduler::sim_process_task(registry, sim_task.task, Some(executor_dyn))
        }));
        let processed = match process {
            Ok(n) => n,
            Err(payload) => {
                // Silence the expected injected-crash payload (it is the
                // `SIM_CRASH_MESSAGE` string, not a real bug).
                if payload
                    .downcast_ref::<&str>()
                    .is_none_or(|m| !m.contains(SIM_CRASH_MESSAGE))
                    && payload
                        .downcast_ref::<String>()
                        .is_none_or(|m| !m.contains(SIM_CRASH_MESSAGE))
                {
                    std::panic::resume_unwind(payload);
                }
                0
            }
        };
        let records = self.executor.drain_records();
        self.executor.arm_crash(false);
        if !crash {
            debug_assert_eq!(
                processed as usize,
                records.len(),
                "worker counter and observation records must agree"
            );
        }

        for record in records {
            let record_ord = self
                .ordinals
                .get(&record.actor.0.as_u128())
                .copied()
                .expect("records only reference current incarnations");
            self.ledger[record_ord as usize].consumed += 1;
            match record.tag {
                ExecTag::Custom(mid) => {
                    self.mid_times[mid as usize] = self.mid_times[mid as usize].saturating_add(1);
                    self.stats.processed += 1;
                    self.trace.push(TraceOp::Process {
                        mid,
                        to: record_ord,
                    });
                }
                ExecTag::System => {
                    self.stats.system_processed += 1;
                    self.trace.push(TraceOp::ProcessSystem { to: record_ord });
                }
            }
        }

        if crash {
            self.stats.crashed += 1;
            self.settle_crash(ord, &sim_task.mids);
            self.handle_restart(ord);
        }
    }

    /// Reconcile the ledger after a crash: the runtime marked the actor
    /// `Failed` and drained its mailbox. Destroyed copies = every delivered
    /// copy not yet consumed (the drain removes them all).
    fn settle_crash(&mut self, ord: u64, task_mids: &[u64]) {
        let id = self.actors[ord as usize];
        let mailbox_len = self
            .registry
            .get_mailbox(&id)
            .map(|m| m.len())
            .unwrap_or(0) as u64;
        let ledger = &mut self.ledger[ord as usize];
        let residual = ledger
            .delivered
            .saturating_sub(ledger.consumed)
            .saturating_sub(ledger.dropped);
        let destroyed = residual.saturating_sub(mailbox_len);
        ledger.dropped += destroyed;
        self.stats.dropped += destroyed;

        // Batch remainder: members of the crashed task that never executed.
        for &mid in task_mids {
            if self.mid_times[mid as usize] == 0 {
                self.mark_lost(mid);
            }
        }
        self.trace.push(TraceOp::Crash {
            to: ord,
            dropped: destroyed,
        });
    }

    /// Report the crash to the real supervisor; restart or escalate.
    fn handle_restart(&mut self, ord: u64) {
        let name = supervisor_name(ord);
        let result = futures::executor::block_on(self.tree.handle_child_exit(
            self.root,
            &name,
            ExitReason::Error(SIM_CRASH_MESSAGE.to_string()),
        ));
        match result {
            Ok(()) => {
                self.stats.restarted += 1;
                self.trace.push(TraceOp::Restart { to: ord, allowed: true });
                self.respawn(ord);
            }
            Err(_) => {
                self.trace
                    .push(TraceOp::Restart { to: ord, allowed: false });
                self.kill(ord);
            }
        }
    }

    /// Respawn a restarted actor under a fresh deterministic identity.
    fn respawn(&mut self, ord: u64) {
        {
            let ledger = &mut self.ledger[ord as usize];
            ledger.incarnation += 1;
        }
        let inc = self.ledger[ord as usize].incarnation;

        let old = self.actors[ord as usize];
        // Old incarnation: mailbox already drained by crash containment;
        // unregister so stale tasks/blocked mail targeting it resolve as
        // dead-target rejections instead of piling up.
        let _ = self.registry.unregister(&old);
        self.ordinals.remove(&old.0.as_u128());
        if let Some(supervisor) = self.tree.get_supervisor_mut(&self.root) {
            let _ = supervisor.mark_child_running(&supervisor_name(ord));
        }

        let id = sim_actor_id(ord, inc);
        self.registry
            .register_named(id, Some(actor_name(ord, inc)))
            .expect("fresh incarnation id is unique");
        self.actors[ord as usize] = id;
        self.ordinals.insert(id.0.as_u128(), ord);
        self.trace.push(TraceOp::Spawn { ord });

        let start = Message {
            sender: None,
            payload: MessagePayload::Start,
            priority: Priority::Normal,
        };
        self.deliver_or_block(ord, vec![start], Vec::new(), 0);
    }

    /// Escalation: permanent death (mirrors `ActorScheduler::kill`).
    fn kill(&mut self, ord: u64) {
        let id = self.actors[ord as usize];
        let dropped = self
            .registry
            .get_mailbox(&id)
            .map(|m| {
                let n = m.len() as u64;
                m.clear();
                n
            })
            .unwrap_or(0);
        let ledger = &mut self.ledger[ord as usize];
        ledger.dropped += dropped;
        ledger.alive = false;
        self.stats.dropped += dropped;
        self.stats.escalated += 1;
        let _ = self.registry.unregister(&id);
        self.ordinals.remove(&id.0.as_u128());
        self.trace.push(TraceOp::Kill { to: ord, dropped });
        // Pending tasks for this ord resolve as discards (dead target).
    }

    fn mark_lost(&mut self, mid: u64) {
        if let Some(slot) = self.mid_lost.get_mut(mid as usize) {
            *slot = true;
        }
    }

    /// True when nothing can ever run or arrive again.
    fn is_quiescent(&self) -> bool {
        self.next_mid >= self.mid_times.len() as u64
            && self.delayed.is_empty()
            && self.blocked.is_empty()
            && self.ready.is_empty()
            && self.ready.total_queued_msgs() == 0
            && self.ledger.iter().zip(self.actors.iter()).all(|(led, id)| {
                !led.alive
                    || self
                        .registry
                        .get_mailbox(id)
                        .map(|m| m.is_empty())
                        .unwrap_or(true)
            })
    }

    /// Invariant 1 (slot/permit conservation) + restart accounting, per tick.
    ///
    /// Identity: every delivered mailbox copy is removed exactly once — by
    /// consumption (`consumed`) or by a drain (`dropped`). A permit/slot
    /// leak unbalances this within one step.
    fn audit_counts(&self) {
        for (ord, led) in self.ledger.iter().enumerate() {
            if !led.alive {
                continue;
            }
            let Some(mailbox) = self.registry.get_mailbox(&self.actors[ord]) else {
                continue;
            };
            let expected = led
                .delivered
                .saturating_sub(led.consumed)
                .saturating_sub(led.dropped);
            assert_eq!(
                mailbox.len() as u64,
                expected,
                "seed {}: slot/permit conservation violated for actor {ord} \
                 (delivered {}, consumed {}, dropped {}, queued {}, actual len {})",
                self.seed,
                led.delivered,
                led.consumed,
                led.dropped,
                led.queued,
                mailbox.len()
            );
        }
        if self.config.max_restarts > 0 {
            let restarts = self
                .tree
                .get_supervisor(&self.root)
                .expect("root exists")
                .count_children()
                .total_restarts;
            // `max_restarts` is the supervisor's PER-CHILD budget (enforced
            // by `check_restart_allowed` against each child's own restart
            // history); `total_restarts` is the aggregate across all children.
            // The correct aggregate bound is therefore per-child budget ×
            // child count. An escalation also permanently stops the child, so
            // in practice the bound is tighter — this is the loose ceiling.
            let budget = u64::from(self.config.max_restarts) * self.config.actors;
            assert!(
                restarts <= budget,
                "seed {}: aggregate restarts {restarts} exceed per-child budget {} × {} actors",
                self.seed,
                self.config.max_restarts,
                self.config.actors
            );
            // Escalation discipline: a child that exhausted its budget must
            // never be restarted again — every Restart trace op after its
            // budget is exhausted must be `allowed: false`.
            let per_child_allowed = self.trace.iter().fold(
                std::collections::HashMap::new(),
                |mut acc: std::collections::HashMap<u64, u32>, op| {
                    if let TraceOp::Restart { to, allowed } = op {
                        let entry = acc.entry(*to).or_insert(0);
                        if *allowed {
                            *entry += 1;
                            assert!(
                                *entry <= self.config.max_restarts,
                                "seed {}: child {to} restarted {} times, exceeding max_restarts {}",
                                self.seed,
                                entry,
                                self.config.max_restarts
                            );
                        }
                    }
                    acc
                },
            );
            debug_assert!(per_child_allowed.len() <= self.config.actors as usize);
        }
    }

    /// Invariant 2 (delivery accounting) — audited at quiescence.
    fn audit_identities(&self) {
        for (mid, &times) in self
            .mid_times
            .iter()
            .take(self.next_mid as usize)
            .enumerate()
        {
            let lost = self.mid_lost[mid];
            assert!(
                times >= 1 || lost,
                "seed {}: message {mid} neither processed nor accounted lost",
                self.seed
            );
            if self.config.faults.duplicate_prob == 0.0 {
                assert!(
                    times <= 1,
                    "seed {}: message {mid} processed {times}× (exactly-once violated)",
                    self.seed
                );
            }
        }
    }
}

/// FNV-1a 64-bit (stable across platforms and rustc versions).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Registry name for an actor incarnation (unique per incarnation).
fn actor_name(ord: u64, inc: u64) -> String {
    format!("sim-{ord}-v{inc}")
}

/// Supervisor child name (stable across restarts — the supervisor keys on it).
fn supervisor_name(ord: u64) -> String {
    format!("sim-{ord}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_known_vectors() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn sim_actor_ids_deterministic_and_unique() {
        assert_eq!(sim_actor_id(3, 0), sim_actor_id(3, 0));
        assert_ne!(sim_actor_id(1, 0), sim_actor_id(2, 0));
        assert_ne!(sim_actor_id(1, 0), sim_actor_id(1, 1));
    }

    #[test]
    fn fault_free_sim_quiesces_exactly_once() {
        let mut config = SimConfig::default();
        config.actors = 4;
        config.messages = 256;
        let mut sim = Sim::with_config(1, config);
        let outcome = sim.run_to_quiescence();
        assert!(outcome.quiesced, "fault-free workload must drain");
        assert_eq!(outcome.stats.injected, 256);
        assert_eq!(outcome.stats.processed, 256, "exactly-once end to end");
        assert_eq!(outcome.stats.rejected + outcome.stats.dropped, 0);
        // Every actor processed its Start.
        assert_eq!(outcome.stats.system_processed, 4);
    }

    #[test]
    fn deterministic_same_seed_same_hash() {
        let build = || {
            let mut config = SimConfig::default();
            config.messages = 128;
            config.faults = FaultConfig::chaos(0.05);
            Sim::with_config(777, config)
        };
        let a = build().run_to_quiescence();
        let b = build().run_to_quiescence();
        assert_eq!(a.trace_hash, b.trace_hash);
        assert_eq!(a.trace_ops, b.trace_ops);
        assert_eq!(a.stats, b.stats);
    }

    #[test]
    fn chaotic_sim_quiesces_with_restart_accounting() {
        let mut config = SimConfig::default();
        config.messages = 512;
        config.faults = FaultConfig::chaos(0.02);
        let mut sim = Sim::with_config(9, config);
        let outcome = sim.run_to_quiescence();
        assert!(outcome.quiesced);
        assert!(outcome.stats.crashed > 0, "chaos config must crash actors");
        assert!(outcome.stats.processed > 0);
    }
}
