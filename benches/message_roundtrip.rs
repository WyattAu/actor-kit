//! Message round-trip: send 200-message bursts to a live actor through a
//! bounded mailbox (capacity 64); semaphore backpressure paces the producer
//! at the actor's true consumption rate, so the measured time is the
//! end-to-end dispatch→process hot path.
//!
//! The scheduler+actor are rebuilt every ~4_000 messages: a long-lived actor
//! currently stalls after ~10k cumulative messages (see PERF-SLO.md), and
//! periodic rebuilds keep every measurement on a healthy, warm runtime.
//! Rebuild cost is amortized across ~20 iterations (~1 ms per iteration,
//! dominated by the send loop itself).

use actor_kit::{ActorBuilder, ActorScheduler, MailboxConfig, Message, MessagePayload, Priority, SchedulerConfig};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::cell::RefCell;
use std::sync::Arc;

const BATCH: u64 = 200;
const REBUILD_AFTER_BATCHES: u64 = 20; // 4k msgs per runtime lifetime

struct Rt {
    scheduler: Arc<ActorScheduler>,
    handle: actor_kit::ActorHandle,
    rt: tokio::runtime::Runtime,
}

fn spawn_runtime(workers: usize) -> Rt {
    let mut cfg = SchedulerConfig::new().workers(workers);
    cfg.mailbox_config = MailboxConfig::new(64);
    let scheduler = Arc::new(ActorScheduler::new(cfg));
    scheduler.start().unwrap();
    let handle = ActorBuilder::new()
        .name("bench-target")
        .spawn(&scheduler)
        .unwrap();
    scheduler.set_actor_running(&handle.id()).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(handle.start()).unwrap();
    Rt {
        scheduler,
        handle,
        rt,
    }
}

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_roundtrip");

    for workers in [1, 4] {
        group.throughput(Throughput::Elements(BATCH));
        group.bench_function(format!("send_drain_{BATCH}_{workers}w"), |b| {
            let state: RefCell<Option<(Rt, u64)>> = RefCell::new(None);
            b.iter(|| {
                if state.borrow().is_none() {
                    state.borrow_mut().replace((spawn_runtime(workers), 0));
                }
                let (mut rt, batches) = state.borrow_mut().take().unwrap();
                let elapsed = rt.rt.block_on(async {
                    let t0 = std::time::Instant::now();
                    for i in 0..BATCH {
                        rt.scheduler
                            .send(
                                rt.handle.id(),
                                Message {
                                    sender: None,
                                    payload: MessagePayload::Custom(vec![i as u8]),
                                    priority: Priority::Normal,
                                },
                            )
                            .await
                            .unwrap();
                    }
                    t0.elapsed()
                });
                let batches = batches + 1;
                if batches >= REBUILD_AFTER_BATCHES {
                    rt.scheduler.stop();
                } else {
                    state.borrow_mut().replace((rt, batches));
                }
                elapsed
            });
            let last = state.borrow_mut().take();
            if let Some((rt, _)) = last {
                rt.scheduler.stop();
            }
        });
    }

    group.finish();
}

criterion_group!(benches, bench_roundtrip);
criterion_main!(benches);
