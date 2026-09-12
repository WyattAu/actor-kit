// Zero-allocation hot-path gate: a counting global allocator proves the
// PERF-SLO.md allocation-profile claims for the steady-state tell path
// (spawn excluded — all fixtures are built before the counter snapshot).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

//! Allocation counter tests for actor-kit's message path.
//!
//! PERF-SLO.md claims, from code reading:
//!
//! - **≥ 1 allocation per message** on the scheduler dispatch path: the
//!   bench payload (`MessagePayload::Custom`) owns a `Vec<u8>`, the send
//!   clones it once for the mailbox and moves the original into a queued
//!   `Task` — measured here as exactly ~2 per tell (the Vec clone +
//!   crossbeam-deque `Injector` slot), stable across windows;
//! - the dispatch itself (registry lookup + mailbox enqueue + task push)
//!   is crossbeam lock-free queues — the pure mailbox enqueue (`try_send`)
//!   is **allocation-free** (proven here);
//! - per-spawn allocation is the preallocated `ArrayQueue` + `Semaphore`
//!   (excluded here: spawn happens in the warm-up, before counting).
//!
//! No scheduler workers are started — the process is single-threaded
//! during counting, so background processing cannot pollute the global
//! counter. The iai-callgrind gate (CI-only; requires valgrind) pins the
//! instruction cost of the same paths.

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use actor_kit::{
    ActorBuilder, ActorScheduler, MailboxConfig, Message, MessagePayload, Priority, SchedulerConfig,
};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocations() -> usize {
    ALLOCATIONS.load(Ordering::Relaxed)
}

/// Drive `fut` to completion on the current thread without a runtime: the
/// uncontended send completes on its first poll (semaphore permits are
/// available), so the noop-waker poll measures exactly the crate's path.
fn block_once<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("uncontended send must complete on its first poll"),
    }
}

fn fixture() -> (Arc<ActorScheduler>, actor_kit::ActorHandle) {
    let mut cfg = SchedulerConfig::new().workers(1);
    cfg.mailbox_config = MailboxConfig::new(4096);
    let scheduler = Arc::new(ActorScheduler::new(cfg));
    // NOTE: `start()` (worker threads) deliberately not called — counting
    // requires a single-threaded process.
    let handle = ActorBuilder::new()
        .name("alloc-probe")
        .spawn(&scheduler)
        .unwrap();
    scheduler.set_actor_running(&handle.id()).unwrap();
    (scheduler, handle)
}

fn msg(i: u8) -> Message {
    Message {
        sender: None,
        payload: MessagePayload::Custom(vec![i]),
        priority: Priority::Normal,
    }
}

/// One sequential test: the allocation counter is process-global.
#[test]
fn tell_path_allocation_profile() {
    let (scheduler, handle) = fixture();

    // --- Warm-up: first sends touch lazy queue/registry state (and the
    // spawn above allocates the mailbox) — all before counting. 100+
    // warm-up sends also cycle crossbeam-deque's block allocation so the
    // measured windows land in steady state. ---
    for i in 0..128u8 {
        block_once(scheduler.send(handle.id(), msg(i))).unwrap();
    }

    // --- Steady-state tell: the per-call allocation count is pinned.
    //
    // Expected ~2/call: 1 = the `Custom` `Vec<u8>` clone parked in the
    // mailbox (the original moves into the `Task`), 1 = crossbeam-deque
    // `Injector` slot per pushed task. PERF-SLO's claim is "≥ 1 per
    // message"; the windows must also be identical to each other (the
    // path is deterministic) and small (catches accidental per-send
    // `format!`/`String` regressions).
    let window = |scheduler: &Arc<ActorScheduler>, handle: &actor_kit::ActorHandle| {
        let before = allocations();
        const ITERATIONS: usize = 100;
        for i in 0..ITERATIONS {
            block_once(scheduler.send(handle.id(), msg(i as u8))).unwrap();
        }
        allocations() - before
    };
    let window_a = window(&scheduler, &handle);
    let window_b = window(&scheduler, &handle);
    assert!(
        window_a >= 100,
        "each tell must allocate ≥ 1 (the mailbox Vec clone): got {window_a}/100"
    );
    assert!(
        window_a <= 210,
        "steady-state tell allocates ~2/call (Vec clone + injector slot); \
         got {window_a}/100 — a regression slipped in"
    );
    assert!(
        (window_a as isize - window_b as isize).abs() <= 2,
        "allocation count must be stable across windows (crossbeam block \
         boundaries add ±1): {window_a} vs {window_b}"
    );
    // Measured 2026-09-12: ~2 allocations per tell (the `Custom` Vec clone
    // parked in the mailbox + the crossbeam-deque `Injector` slot per
    // pushed task), with ±1 jitter from crossbeam block allocation
    // boundaries. The bounds above are the gate: a new `String`/`format!`
    // on the path, or a queue change, moves the count and fails.

    // --- Pure mailbox enqueue (`try_send`) is allocation-free. ---
    let mailbox = scheduler.registry().get_mailbox(&handle.id()).unwrap();
    let message = msg(255); // built before the snapshot
    let before = allocations();
    mailbox.try_send(message).unwrap();
    assert_eq!(
        allocations(),
        before,
        "mailbox try_send happy path must not allocate"
    );
}

// Sanity guard: if this fails, the assertions above prove nothing (the
// counter would be broken, not the path miraculously cheap).
#[test]
fn allocation_counter_sanity() {
    let before = allocations();
    std::hint::black_box(format!("fresh-{before}"));
    assert!(
        allocations() > before,
        "format! must allocate (counter sanity check)"
    );
}
