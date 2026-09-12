// iai-callgrind benchmarks run once under Valgrind on fixed inputs; the
// harness measures instruction counts, so there is no "expected failure"
// recovery path — a panic aborts the run visibly, which is what we want.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

//! Deterministic regression gate for the tell hot path behind PERF-SLO.md
//! (uncontended dispatch ≈ 1.3 µs/msg wall-clock at 4 workers).
//!
//! The setup deliberately never calls `scheduler.start()`: no worker
//! threads exist, so the measured region is exactly the crate's dispatch
//! path (registry lookup → mailbox enqueue → task push) on the benchmarked
//! thread — background stealing would make instruction counts
//! irreproducible.
//!
//! - `mailbox_try_send` — pure mailbox enqueue (crossbeam `ArrayQueue` +
//!   semaphore fast path).
//! - `scheduler_tell` — full dispatch: registry lookup + `Mailbox::send`
//!   (first-poll complete) + `Task` push onto the global injector.
//!
//! Criterion owns the wall-clock numbers (`benches/message_roundtrip.rs`);
//! this file is the pass/fail gate. Locally requires `valgrind`; without
//! it, compile-check only: `cargo bench --no-run --bench iai_hot_path`.

use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use actor_kit::{
    ActorBuilder, ActorScheduler, Mailbox, MailboxConfig, Message, MessagePayload, Priority,
    Result, SchedulerConfig,
};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};

/// Poll a ready-on-first-poll future without a runtime: uncontended sends
/// complete on the first poll, so the noop-waker poll is the crate's path
/// only — no tokio plumbing in the count.
fn block_once<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("uncontended send must complete on its first poll"),
    }
}

fn msg(i: u8) -> Message {
    Message {
        sender: None,
        payload: MessagePayload::Custom(vec![i]),
        priority: Priority::Normal,
    }
}

fn setup_scheduler() -> (Arc<ActorScheduler>, actor_kit::ActorHandle) {
    let mut cfg = SchedulerConfig::new().workers(1);
    cfg.mailbox_config = MailboxConfig::new(4096);
    let scheduler = Arc::new(ActorScheduler::new(cfg));
    // No `start()`: zero worker threads (see module docs).
    let handle = ActorBuilder::new()
        .name("iai-probe")
        .spawn(&scheduler)
        .unwrap();
    scheduler.set_actor_running(&handle.id()).unwrap();
    (scheduler, handle)
}

fn setup_mailbox() -> Arc<Mailbox> {
    let (scheduler, handle) = setup_scheduler();
    scheduler.registry().get_mailbox(&handle.id()).unwrap()
}

// Pure mailbox enqueue: the lock-free ArrayQueue push + semaphore fast path.
#[library_benchmark]
#[bench::steady_state(setup = setup_mailbox)]
fn mailbox_try_send(mailbox: Arc<Mailbox>) -> Result<()> {
    black_box(mailbox.try_send(msg(1)).map_err(|(_, e)| e))
}

// Full dispatch hot path: registry lookup + mailbox send + task push.
#[library_benchmark]
#[bench::steady_state(setup = setup_scheduler)]
fn scheduler_tell(env: (Arc<ActorScheduler>, actor_kit::ActorHandle)) -> Result<()> {
    let (scheduler, handle) = env;
    black_box(block_once(scheduler.send(handle.id(), msg(2))))
}

library_benchmark_group!(
    name = iai_hot_path;
    benchmarks =
        mailbox_try_send,
        scheduler_tell
);

main!(library_benchmark_groups = iai_hot_path);
