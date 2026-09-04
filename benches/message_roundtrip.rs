//! Message roundtrip latency: send a message and wait for the ack via a
//! spin-wait on the actor's processed count.

use actor_kit::{ActorBuilder, ActorScheduler, Message, MessagePayload, Priority, SchedulerConfig};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::time::Duration;

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_roundtrip");

    for workers in [1, 4] {
        group.bench_function(format!("send_recv_1k_{workers}w"), |b| {
            let scheduler = Arc::new(ActorScheduler::new(SchedulerConfig::new().workers(workers)));
            scheduler.start().unwrap();

            let handle = ActorBuilder::new()
                .name("bench-target")
                .spawn(&scheduler)
                .unwrap();
            scheduler.set_actor_running(&handle.id()).unwrap();
            // Start the actor.
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.start())
                .unwrap();

            b.iter_custom(|iters| {
                let start = std::time::Instant::now();
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    for i in 0..iters {
                        scheduler
                            .send(
                                handle.id(),
                                Message {
                                    sender: None,
                                    payload: MessagePayload::Custom(vec![i as u8]),
                                    priority: Priority::Normal,
                                },
                            )
                            .await
                            .unwrap();
                    }
                });
                // Wait for the workers to drain all iters messages.
                let deadline = std::time::Instant::now() + Duration::from_secs(30);
                while scheduler.registry().get_processed_count(&handle.id()) < iters {
                    if std::time::Instant::now() > deadline {
                        panic!("roundtrip bench timed out");
                    }
                    std::thread::sleep(Duration::from_micros(50));
                }
                start.elapsed()
            });

            scheduler.stop();
        });
    }

    group.finish();
}

criterion_group!(benches, bench_roundtrip);
criterion_main!(benches);
