//! Config-knob behavior matrix for actor-kit.
//!
//! Every public knob must OBSERVABLY change behavior: each test below pairs
//! a default with an alternate value and asserts the observable output
//! differs. A knob that cannot change behavior is a bug.
//!
//! Knobs covered (16):
//!   SchedulerConfig: workers, mailbox_config, priority_scheduling,
//!     max_steal_batch, idle_sleep_us, stealer_refresh_interval (6)
//!   MailboxConfig: capacity, priority_queue, backpressure_threshold (3)
//!   MemoryPoolConfig: arena_size_bytes, page_size, max_pools,
//!     preallocate (4)
//!   ChildSpec: restart_policy, shutdown_timeout, significant (3)
//!
//! DEAD-KNOB REPORT (settable, no behavior change — see summary):
//!   - `MemoryPoolConfig::page_size` / `::max_pools`: stored in
//!     `BumpAllocatorInner::_config` (note the underscore) and never read.
//!     Only observable effect is the `page_size` `MIN_BLOCK_SIZE` clamp.
//!   - `ChildSpec::shutdown_timeout`: no shutdown path reads it.
//!   - `ChildSpec::significant`: only gates a `tracing::warn!` log line;
//!     child state transitions are identical either way.
//!
//! The `dead_knob_*_is_documented_inert` tests pin this contract so a
//! future wiring changes them deliberately.
//!
//! BUG FIX pinned here: `SchedulerConfig::stealer_refresh_interval = 0`
//! used to panic worker threads (`iteration % 0`); the worker loop now
//! clamps to >= 1.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[cfg(feature = "unsafe-pool")]
use actor_kit::memory_pool::{BumpAllocator, MemoryPoolConfig};
use actor_kit::{
    create_local_queue, ActorId, ActorScheduler, ChildSpec, ExitReason, Mailbox, MailboxConfig,
    Message, MessagePayload, Priority, RestartPolicy, SchedulerConfig, SupervisionStrategy,
    SupervisorTree, WorkStealer,
};
use actor_kit::{ActorExecutor, ExecutionResult};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Executor that records `Custom` payload bytes in arrival order.
struct OrderRecorder {
    order: Mutex<Vec<u8>>,
}

impl OrderRecorder {
    fn new() -> Self {
        Self {
            order: Mutex::new(Vec::new()),
        }
    }

    fn order(&self) -> Vec<u8> {
        self.order.lock().unwrap().clone()
    }
}

impl ActorExecutor for OrderRecorder {
    fn execute(&self, _actor_id: &ActorId, message: &Message) -> ExecutionResult {
        if let MessagePayload::Custom(v) = &message.payload {
            self.order.lock().unwrap().extend(v.iter().copied());
        }
        ExecutionResult::Success {
            fuel_consumed: 1,
            response: None,
        }
    }

    fn is_ready(&self, _actor_id: &ActorId) -> bool {
        true
    }

    fn get_fuel(&self, _actor_id: &ActorId) -> Option<u64> {
        None
    }

    fn reset(&self, _actor_id: &ActorId) -> actor_kit::Result<()> {
        Ok(())
    }
}

fn custom(priority: Priority, byte: u8) -> Message {
    Message {
        sender: None,
        payload: MessagePayload::Custom(vec![byte]),
        priority,
    }
}

fn wait_processed(scheduler: &ActorScheduler, id: &ActorId, want: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while scheduler.registry().get_processed_count(id) < want {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {want} processed messages"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

// ---------------------------------------------------------------------------
// SchedulerConfig::workers
// ---------------------------------------------------------------------------

#[test]
fn knob_workers_changes_thread_count() {
    for (workers, want) in [(1usize, 1usize), (2, 2)] {
        let scheduler = ActorScheduler::new(SchedulerConfig::new().workers(workers));
        scheduler.start().unwrap();
        assert_eq!(
            scheduler.stats().worker_count,
            want,
            "workers({workers}) must spawn {want} worker threads"
        );
        scheduler.stop();
    }
}

// ---------------------------------------------------------------------------
// SchedulerConfig::mailbox_config (plumbing — was once a dead knob)
// ---------------------------------------------------------------------------

#[test]
fn knob_mailbox_config_is_plumbed_into_registry() {
    // A capacity-2 mailbox rejects the 3rd send; the 10_000 default accepts.
    let tiny = ActorScheduler::new(SchedulerConfig {
        mailbox_config: MailboxConfig::new(2),
        ..SchedulerConfig::default()
    });
    let id = tiny.spawn().unwrap();
    tiny.set_actor_running(&id).unwrap();
    tiny.try_send(id, custom(Priority::Normal, 1)).unwrap();
    tiny.try_send(id, custom(Priority::Normal, 2)).unwrap();
    assert!(
        tiny.try_send(id, custom(Priority::Normal, 3)).is_err(),
        "mailbox_config capacity must be honored by spawned actors"
    );

    let big = ActorScheduler::new(SchedulerConfig::default());
    let id = big.spawn().unwrap();
    big.set_actor_running(&id).unwrap();
    for b in 0..3 {
        big.try_send(id, custom(Priority::Normal, b)).unwrap();
    }
}

// ---------------------------------------------------------------------------
// SchedulerConfig::priority_scheduling
// ---------------------------------------------------------------------------

#[test]
fn knob_priority_scheduling_changes_delivery_order() {
    for (enabled, want) in [(true, vec![2u8, 1u8]), (false, vec![1u8, 2u8])] {
        let recorder = Arc::new(OrderRecorder::new());
        let mut config = SchedulerConfig::new().workers(1);
        config.priority_scheduling = enabled;
        let scheduler = ActorScheduler::with_executor(config, recorder.clone());
        let id = scheduler.spawn().unwrap();
        scheduler.set_actor_running(&id).unwrap();
        // Queue both BEFORE start so no worker can interleave: Normal first.
        scheduler.try_send(id, custom(Priority::Normal, 1)).unwrap();
        scheduler
            .try_send(id, custom(Priority::Critical, 2))
            .unwrap();
        scheduler.start().unwrap();
        wait_processed(&scheduler, &id, 2);
        // Give the recorder a beat to flush the second execution.
        let deadline = Instant::now() + Duration::from_secs(5);
        while recorder.order().len() < 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            recorder.order(),
            want,
            "priority_scheduling={enabled} must deliver in order {want:?}"
        );
        scheduler.stop();
    }
}

// ---------------------------------------------------------------------------
// SchedulerConfig::max_steal_batch
// ---------------------------------------------------------------------------

#[test]
fn knob_max_steal_batch_caps_batch_steals() {
    use actor_kit::queue::Task;

    fn task(byte: u8) -> Task {
        Task {
            actor_id: ActorId::new(),
            message: custom(Priority::Normal, byte),
            priority: Priority::Normal,
            additional_messages: Vec::new(),
        }
    }

    // max = 0 steals nothing and leaves the source intact.
    let (w1, s1) = create_local_queue();
    for b in 0..4 {
        w1.push(task(b));
    }
    let (w2, _s2) = create_local_queue();
    let thief = WorkStealer::new(vec![s1]);
    assert_eq!(thief.steal_batch(&w2, 0), 0);
    assert!(w2.pop().is_none(), "max=0 must steal nothing");

    // A generous max drains everything the stealer can offer.
    let (w3, s3) = create_local_queue();
    for b in 0..4 {
        w3.push(task(b));
    }
    let (w4, _s4) = create_local_queue();
    let thief = WorkStealer::new(vec![s3]);
    let stolen = thief.steal_batch(&w4, 64);
    assert!(stolen > 0, "max=64 must steal available tasks");
    assert!(w3.pop().is_none(), "max=64 must drain the source worker");
}

#[test]
fn knob_max_steal_batch_extremes_still_make_progress() {
    for max in [1usize, 64] {
        let mut config = SchedulerConfig::new().workers(2);
        config.max_steal_batch = max;
        let scheduler = ActorScheduler::new(config);
        let id = scheduler.spawn().unwrap();
        scheduler.set_actor_running(&id).unwrap();
        scheduler.start().unwrap();
        for b in 0..4u8 {
            scheduler.try_send(id, custom(Priority::Normal, b)).unwrap();
        }
        wait_processed(&scheduler, &id, 4);
        scheduler.stop();
    }
}

// ---------------------------------------------------------------------------
// SchedulerConfig::idle_sleep_us
// ---------------------------------------------------------------------------

#[test]
fn knob_idle_sleep_us_extremes_stay_functional() {
    // Timing-sensitive by nature (see worker_loop idle path): pin that both
    // extremes keep the runtime functional instead of asserting wall-clock
    // differences that would flake under load.
    for idle in [0u64, 1_000_000] {
        let mut config = SchedulerConfig::new().workers(1);
        config.idle_sleep_us = idle;
        let scheduler = ActorScheduler::new(config);
        let id = scheduler.spawn().unwrap();
        scheduler.set_actor_running(&id).unwrap();
        scheduler.start().unwrap();
        scheduler.try_send(id, custom(Priority::Normal, 7)).unwrap();
        wait_processed(&scheduler, &id, 1);
        scheduler.stop();
        assert!(!scheduler.stats().running);
    }
}

// ---------------------------------------------------------------------------
// SchedulerConfig::stealer_refresh_interval
// ---------------------------------------------------------------------------

#[test]
fn knob_stealer_refresh_interval_zero_does_not_kill_workers() {
    // Regression: `iteration % 0` panicked the worker thread, so no message
    // was ever processed. The loop clamps to >= 1.
    let mut config = SchedulerConfig::new().workers(1);
    config.stealer_refresh_interval = 0;
    let scheduler = ActorScheduler::new(config);
    let id = scheduler.spawn().unwrap();
    scheduler.set_actor_running(&id).unwrap();
    scheduler.start().unwrap();
    scheduler.try_send(id, custom(Priority::Normal, 9)).unwrap();
    wait_processed(&scheduler, &id, 1);
    scheduler.stop();
}

#[test]
fn knob_stealer_refresh_interval_extremes_make_progress() {
    for interval in [1u32, u32::MAX] {
        let mut config = SchedulerConfig::new().workers(2);
        config.stealer_refresh_interval = interval;
        let scheduler = ActorScheduler::new(config);
        let id = scheduler.spawn().unwrap();
        scheduler.set_actor_running(&id).unwrap();
        scheduler.start().unwrap();
        scheduler.try_send(id, custom(Priority::Normal, 3)).unwrap();
        wait_processed(&scheduler, &id, 1);
        scheduler.stop();
    }
}

// ---------------------------------------------------------------------------
// MailboxConfig::capacity
// ---------------------------------------------------------------------------

#[test]
fn knob_mailbox_capacity_bounds_try_send() {
    let small = Mailbox::new(ActorId::new(), MailboxConfig::new(2));
    small.try_send(custom(Priority::Normal, 1)).unwrap();
    small.try_send(custom(Priority::Normal, 2)).unwrap();
    assert!(
        small.try_send(custom(Priority::Normal, 3)).is_err(),
        "capacity=2 must reject the 3rd message"
    );

    let big = Mailbox::new(ActorId::new(), MailboxConfig::new(4));
    for b in 0..3 {
        big.try_send(custom(Priority::Normal, b)).unwrap();
    }
}

// ---------------------------------------------------------------------------
// MailboxConfig::priority_queue
// ---------------------------------------------------------------------------

#[test]
fn knob_mailbox_priority_queue_changes_recv_order() {
    // Priority queuing on: Critical jumps ahead of queued Normal.
    let prio = Mailbox::new(
        ActorId::new(),
        MailboxConfig {
            priority_queue: true,
            ..MailboxConfig::default()
        },
    );
    prio.try_send(custom(Priority::Normal, 1)).unwrap();
    prio.try_send(custom(Priority::Critical, 2)).unwrap();
    let first = prio.try_recv().unwrap();
    assert_eq!(
        first.priority,
        Priority::Critical,
        "priority_queue=true must deliver Critical first"
    );

    // Off: strict FIFO regardless of priority.
    let fifo = Mailbox::new(
        ActorId::new(),
        MailboxConfig {
            priority_queue: false,
            ..MailboxConfig::default()
        },
    );
    fifo.try_send(custom(Priority::Normal, 1)).unwrap();
    fifo.try_send(custom(Priority::Critical, 2)).unwrap();
    let first = fifo.try_recv().unwrap();
    assert_eq!(
        first.priority,
        Priority::Normal,
        "priority_queue=false must keep FIFO order"
    );
}

// ---------------------------------------------------------------------------
// MailboxConfig::backpressure_threshold
// ---------------------------------------------------------------------------

#[test]
fn knob_backpressure_threshold_moves_the_trip_point() {
    fn pressured(threshold: f32, sends: usize) -> bool {
        let mb = Mailbox::new(
            ActorId::new(),
            MailboxConfig {
                capacity: 10,
                backpressure_threshold: threshold,
                ..MailboxConfig::default()
            },
        );
        for b in 0..sends {
            mb.try_send(custom(Priority::Normal, b as u8)).unwrap();
        }
        mb.is_backpressured()
    }

    // threshold 0.5 trips at 5/10; 0.9 does not trip at 5/10.
    assert!(pressured(0.5, 5));
    assert!(!pressured(0.9, 5));
    // ...but 0.9 trips at 9/10 while 0.5 already tripped.
    assert!(pressured(0.9, 9));
}

// ---------------------------------------------------------------------------
// MemoryPoolConfig::arena_size_bytes
// ---------------------------------------------------------------------------

#[cfg(feature = "unsafe-pool")]
#[test]
fn knob_arena_size_bytes_changes_capacity() {
    let big = BumpAllocator::new(MemoryPoolConfig {
        arena_size_bytes: 8192,
        ..MemoryPoolConfig::default()
    });
    let small = BumpAllocator::new(MemoryPoolConfig {
        arena_size_bytes: 64,
        ..MemoryPoolConfig::default()
    });
    assert_eq!(big.capacity(), 8192);
    assert_eq!(small.capacity(), 64);
    assert!(big.borrow(4096).is_some());
    assert!(
        small.borrow(4096).is_none(),
        "64-byte arena must reject a 4 KiB borrow the 8 KiB arena accepts"
    );
}

// ---------------------------------------------------------------------------
// MemoryPoolConfig::preallocate
// ---------------------------------------------------------------------------

#[cfg(feature = "unsafe-pool")]
#[test]
fn knob_preallocate_changes_initial_availability() {
    let eager = BumpAllocator::new(MemoryPoolConfig {
        preallocate: true,
        ..MemoryPoolConfig::default()
    });
    let lazy = BumpAllocator::new(MemoryPoolConfig {
        preallocate: false,
        ..MemoryPoolConfig::default()
    });
    assert!(eager.capacity() > 0);
    assert_eq!(lazy.capacity(), 0, "preallocate=false must defer the arena");
    assert!(eager.borrow(64).is_some());
    assert!(
        lazy.borrow(64).is_none(),
        "unallocated pool must refuse borrows the preallocated pool serves"
    );
}

// ---------------------------------------------------------------------------
// MemoryPoolConfig::page_size / max_pools — DEAD (pinned inert)
// ---------------------------------------------------------------------------

#[cfg(feature = "unsafe-pool")]
#[test]
fn dead_knob_page_size_only_clamps_never_read() {
    let clamped = MemoryPoolConfig::default().page_size(1);
    assert_eq!(
        clamped.page_size, 16,
        "builder clamps below MIN_BLOCK_SIZE (only observable effect)"
    );
    // Same allocator behavior either way: the borrow path uses the
    // MIN_BLOCK_SIZE constant, never `config.page_size`.
    let a = BumpAllocator::new(MemoryPoolConfig::default().page_size(16));
    let b = BumpAllocator::new(MemoryPoolConfig::default().page_size(4096));
    assert_eq!(a.capacity(), b.capacity());
    assert_eq!(a.available(), b.available());
    assert_eq!(a.borrow(32).is_some(), b.borrow(32).is_some());
}

#[cfg(feature = "unsafe-pool")]
#[test]
fn dead_knob_max_pools_is_stored_never_read() {
    let a = BumpAllocator::new(MemoryPoolConfig::default().max_pools(1));
    let b = BumpAllocator::new(MemoryPoolConfig::default().max_pools(1_000_000));
    // Single-arena allocator: identical capacity, identical borrows.
    assert_eq!(a.capacity(), b.capacity());
    assert!(a.borrow(64).is_some());
    assert!(b.borrow(64).is_some());
}

// ---------------------------------------------------------------------------
// ChildSpec::restart_policy
// ---------------------------------------------------------------------------

fn tree_with_policy(policy: RestartPolicy) -> (SupervisorTree, ActorId) {
    let mut tree = SupervisorTree::new(SupervisionStrategy::one_for_one(
        10,
        Duration::from_secs(60),
    ));
    let root = tree.root();
    tree.start_child_under(root, ChildSpec::new("child").restart_policy(policy))
        .unwrap();
    (tree, root)
}

fn child_state(tree: &SupervisorTree, root: &ActorId) -> String {
    format!(
        "{:?}",
        tree.get_supervisor(root)
            .unwrap()
            .get_child("child")
            .unwrap()
            .state
    )
}

#[tokio::test]
async fn knob_restart_policy_changes_restart_decision() {
    // Permanent restarts even on clean exits; Temporary never restarts.
    let (mut tree, root) = tree_with_policy(RestartPolicy::Permanent);
    tree.handle_child_exit(root, "child", ExitReason::Normal)
        .await
        .unwrap();
    assert!(
        child_state(&tree, &root).contains("Restarting"),
        "Permanent must restart on Normal exit"
    );

    let (mut tree, root) = tree_with_policy(RestartPolicy::Temporary);
    tree.handle_child_exit(root, "child", ExitReason::Error("boom".into()))
        .await
        .unwrap();
    assert!(
        child_state(&tree, &root).contains("Stopped"),
        "Temporary must not restart on Error exit"
    );

    // Transient splits: abnormal restarts, normal stops.
    let (mut tree, root) = tree_with_policy(RestartPolicy::Transient);
    tree.handle_child_exit(root, "child", ExitReason::Error("boom".into()))
        .await
        .unwrap();
    assert!(child_state(&tree, &root).contains("Restarting"));

    let (mut tree, root) = tree_with_policy(RestartPolicy::Transient);
    tree.handle_child_exit(root, "child", ExitReason::Normal)
        .await
        .unwrap();
    assert!(
        child_state(&tree, &root).contains("Stopped"),
        "Transient must not restart on Normal exit"
    );
}

// ---------------------------------------------------------------------------
// ChildSpec::shutdown_timeout / significant — DEAD (pinned inert)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dead_knob_shutdown_timeout_is_documented_inert() {
    let mut states = Vec::new();
    for timeout in [Duration::from_millis(1), Duration::from_secs(3600)] {
        let mut tree = SupervisorTree::new(SupervisionStrategy::one_for_one(
            10,
            Duration::from_secs(60),
        ));
        let root = tree.root();
        tree.start_child_under(root, ChildSpec::new("child").shutdown_timeout(timeout))
            .unwrap();
        tree.handle_child_exit(root, "child", ExitReason::Error("x".into()))
            .await
            .unwrap();
        states.push(child_state(&tree, &root));
    }
    assert_eq!(states[0], states[1], "shutdown_timeout changes no outcome");
}

#[tokio::test]
async fn dead_knob_significant_is_documented_inert() {
    let mut states = Vec::new();
    for significant in [false, true] {
        let mut tree = SupervisorTree::new(SupervisionStrategy::one_for_one(
            10,
            Duration::from_secs(60),
        ));
        let root = tree.root();
        tree.start_child_under(root, ChildSpec::new("child").significant(significant))
            .unwrap();
        tree.handle_child_exit(root, "child", ExitReason::Error("x".into()))
            .await
            .unwrap();
        states.push(child_state(&tree, &root));
    }
    assert_eq!(
        states[0], states[1],
        "significant changes no child state (only a log line)"
    );
}
