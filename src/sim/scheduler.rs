//! Deterministic task-selection model for the simulation.
//!
//! The real runtime's worker loop selects work in a fixed order — priority
//! injector → local deque → global injector → steal from peers — across N
//! preemptively scheduled OS threads. The *invariant* part of that order
//! (priority first) and the *arbitrary* part (which ready task any worker
//! happens to grab, in which interleaving) are separated here:
//!
//! - the priority set always drains first, exactly as every real worker
//!   iteration checks the priority queue before anything else;
//! - the ordinary ready set abstracts (global injector ∪ local deques ∪
//!   stealing): under [`SchedulingPolicy::Random`] the seeded RNG picks
//!   *any* element — modeling the worst-case reordering real threads and
//!   steal batches can produce. Token-steal decisions become RNG decisions.
//!
//! There are no threads, no locks, and no real time here: a ready set is
//! plain `VecDeque` state advanced one deterministic `pick` at a time.

use std::collections::{BTreeMap, VecDeque};

use crate::queue::Task;
use crate::{ActorId, Priority};

use super::faults::SimRng;

/// Map key for an actor: the `u128` UUID bits (`ActorId` does not impl
/// `Ord`, and the sim must not perturb the public API to get one).
fn actor_key(id: &ActorId) -> u128 {
    id.0.as_u128()
}

/// How the sim picks the next runnable task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchedulingPolicy {
    /// Priority set first, then FIFO — mirrors a single worker's selection
    /// order exactly (the deterministic baseline).
    #[default]
    Fifo,
    /// Priority set first, then a uniform-random element of the ready set —
    /// models arbitrary multi-worker/stealing reordering (worst case).
    Random,
    /// Priority set first, then the oldest task of the least-recently-served
    /// actor — deterministic per-actor fairness.
    RoundRobin,
}

/// One schedulable unit: the runtime's [`Task`] plus the sim's parallel
/// message-identity ledger (`mids`) and actor ordinal for the audit.
///
/// `mids[i]` corresponds to `task.message` when `i == 0` and to
/// `task.additional_messages[i - 1]` afterwards; empty for system messages.
pub(crate) struct SimTask {
    /// Actor ordinal (stable across incarnations/restarts).
    pub ord: u64,
    pub task: Task,
    pub mids: Vec<u64>,
}

impl SimTask {
    /// Number of application messages in this task.
    pub fn app_len(&self) -> usize {
        self.mids.len()
    }

    /// The task's actor ordinal.
    pub fn ord(&self) -> u64 {
        self.ord
    }
}

/// Model of the runtime's ready sets (priority injector + the pooled
/// global/local/steal work).
pub(crate) struct ReadySet {
    priority: VecDeque<SimTask>,
    ready: VecDeque<SimTask>,
    /// Actor → tick it was last served (RoundRobin bookkeeping).
    last_served: BTreeMap<u128, u64>,
    /// Actor → number of queued messages (audit side ledger).
    queued_msgs: BTreeMap<u128, u64>,
}

impl ReadySet {
    pub fn new() -> Self {
        Self {
            priority: VecDeque::new(),
            ready: VecDeque::new(),
            last_served: BTreeMap::new(),
            queued_msgs: BTreeMap::new(),
        }
    }

    /// Push a task, routing by priority exactly like `ActorScheduler::send`
    /// (`>= High` → priority injector, else the ordinary pool).
    pub fn push(&mut self, sim_task: SimTask) {
        *self
            .queued_msgs
            .entry(actor_key(&sim_task.task.actor_id))
            .or_insert(0) += sim_task.app_len() as u64;
        if sim_task.task.priority >= Priority::High {
            self.priority.push_back(sim_task);
        } else {
            self.ready.push_back(sim_task);
        }
    }

    /// Remove and return the next task per policy. `tick` stamps the
    /// RoundRobin bookkeeping.
    pub fn pick(
        &mut self,
        policy: SchedulingPolicy,
        rng: &mut SimRng,
        tick: u64,
    ) -> Option<SimTask> {
        // Priority always first: every real worker iteration checks the
        // priority queue before local/global/steal. This is a real runtime
        // guarantee, not a policy choice.
        let mut picked = match self.priority.pop_front() {
            Some(t) => t,
            None => match policy {
                SchedulingPolicy::Fifo => match self.ready.pop_front() {
                    Some(t) => t,
                    None => return None,
                },
                SchedulingPolicy::Random => {
                    let len = self.ready.len();
                    if len == 0 {
                        return None;
                    }
                    let idx = rng.below(len as u64) as usize;
                    self.ready.remove(idx)?
                }
                SchedulingPolicy::RoundRobin => {
                    // Oldest task of the least-recently-served actor (ties
                    // broken by queue position — deterministic).
                    let mut best: Option<(u64, usize)> = None; // (last_served, idx)
                    for (idx, t) in self.ready.iter().enumerate() {
                        let served = self
                            .last_served
                            .get(&actor_key(&t.task.actor_id))
                            .copied()
                            .unwrap_or(0);
                        if best.map_or(true, |(bs, _)| served < bs) {
                            best = Some((served, idx));
                        }
                    }
                    let idx = best?.1;
                    self.ready.remove(idx)?
                }
            },
        };

        self.last_served.insert(actor_key(&picked.task.actor_id), tick);
        if let Some(q) = self.queued_msgs.get_mut(&actor_key(&picked.task.actor_id)) {
            *q = q.saturating_sub(picked.app_len() as u64);
        }
        picked
            .task
            .additional_messages
            .shrink_to_fit();
        Some(picked)
    }

    /// Total queued messages (both sets); a quiescence cross-check.
    pub fn total_queued_msgs(&self) -> u64 {
        self.queued_msgs.values().sum()
    }

    /// True when nothing is queued anywhere.
    pub fn is_empty(&self) -> bool {
        self.priority.is_empty() && self.ready.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, MessagePayload};
    use uuid::Uuid;

    /// Deterministic test ID (production path: `super::sim_actor_id`).
    fn test_id(n: u64) -> ActorId {
        ActorId(Uuid::from_u128(0xA77C_70B1_5160_0000 + n as u128))
    }

    fn task_for(id: ActorId, prio: Priority, tag: u8) -> SimTask {
        SimTask {
            ord: 0,
            task: Task {
                actor_id: id,
                message: Message {
                    sender: None,
                    payload: MessagePayload::Custom(vec![tag]),
                    priority: prio,
                },
                priority: prio,
                additional_messages: Vec::new(),
            },
            mids: vec![tag as u64],
        }
    }

    #[test]
    fn priority_drains_before_ready_under_all_policies() {
        let a = test_id(1);
        let b = test_id(2);
        for policy in [
            SchedulingPolicy::Fifo,
            SchedulingPolicy::Random,
            SchedulingPolicy::RoundRobin,
        ] {
            let mut set = ReadySet::new();
            let mut rng = SimRng::new(1);
            set.push(task_for(a, Priority::Normal, 1));
            set.push(task_for(b, Priority::Critical, 2));
            let first = set.pick(policy, &mut rng, 1).unwrap();
            assert_eq!(first.task.priority, Priority::Critical);
            let second = set.pick(policy, &mut rng, 2).unwrap();
            assert_eq!(second.task.priority, Priority::Normal);
            assert!(set.pick(policy, &mut rng, 3).is_none());
        }
    }

    #[test]
    fn fifo_preserves_order() {
        let a = test_id(1);
        let mut set = ReadySet::new();
        let mut rng = SimRng::new(1);
        set.push(task_for(a, Priority::Normal, 10));
        set.push(task_for(a, Priority::Normal, 11));
        assert_eq!(
            set.pick(SchedulingPolicy::Fifo, &mut rng, 1).unwrap().mids[0],
            10
        );
        assert_eq!(
            set.pick(SchedulingPolicy::Fifo, &mut rng, 2).unwrap().mids[0],
            11
        );
    }

    #[test]
    fn round_robin_serves_actors_fairly() {
        let a = test_id(1);
        let b = test_id(2);
        let mut set = ReadySet::new();
        let mut rng = SimRng::new(1);
        set.push(task_for(a, Priority::Normal, 1));
        set.push(task_for(a, Priority::Normal, 2));
        set.push(task_for(b, Priority::Normal, 3));
        set.push(task_for(b, Priority::Normal, 4));
        let seq: Vec<u64> = (0..4)
            .map(|t| {
                set.pick(SchedulingPolicy::RoundRobin, &mut rng, t + 1)
                    .unwrap()
                    .mids[0]
            })
            .collect();
        // Strict alternation once every actor has been served once:
        // a₁ b₁ a₂ b₂.
        assert_eq!(
            &seq[..],
            &[1, 3, 2, 4],
            "round-robin must alternate actors: got {seq:?}"
        );
        assert_eq!(set.total_queued_msgs(), 0);
    }
}
