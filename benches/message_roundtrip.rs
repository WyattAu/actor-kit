//! Message round-trip: send 200-message bursts to a live actor through a
//! bounded mailbox (capacity 64); semaphore backpressure paces the producer
//! at the actor's true consumption rate, so the measured time is the
//! end-to-end dispatch→process hot path.
//!
//! A single long-lived runtime serves the whole benchmark: since 0.1.1 the
//! mailbox permit consumed per send is released when the scheduler processes
//! the message (previously permits leaked and the actor stalled after
//! ~capacity cumulative messages, worked around here by rebuilding the
//! runtime every 4k messages). A full criterion run sustains millions of
//! cumulative messages through one actor.

use actor_kit::{
    ActorBuilder, ActorScheduler, MailboxConfig, Message, MessagePayload, Priority, SchedulerConfig,
};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;

const BATCH: u64 = 200;

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
            let rt = spawn_runtime(workers);
            b.iter(|| {
                rt.rt.block_on(async {
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
                })
            });
            rt.scheduler.stop();
        });
    }

    group.finish();
}

criterion_group!(benches, bench_roundtrip);
criterion_main!(benches);
