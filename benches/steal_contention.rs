//! Steal contention: many producers hammering a shared work-stealing pool
//! from multiple OS threads while workers steal from each other.

use actor_kit::{ActorScheduler, Message, MessagePayload, Priority, SchedulerConfig};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

fn bench_steal_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("steal_contention");

    for producers in [2, 8] {
        group.bench_function(format!("{producers}_producers_x_10k_msgs"), |b| {
            let scheduler = Arc::new(ActorScheduler::new(SchedulerConfig::new().workers(4)));
            scheduler.start().unwrap();

            let mut targets = Vec::new();
            for _ in 0..16 {
                let id = scheduler.spawn().unwrap();
                scheduler.set_actor_running(&id).unwrap();
                targets.push(id);
            }

            b.iter(|| {
                let mut handles = Vec::new();
                for p in 0..producers {
                    let scheduler = Arc::clone(&scheduler);
                    let targets = targets.clone();
                    handles.push(std::thread::spawn(move || {
                        // Backpressured sends: `try_send` is fail-fast and a
                        // burst can transiently fill a target's mailbox
                        // (producer 0 concentrates on one target by index);
                        // awaiting `send` paces the producer instead.
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                        rt.block_on(async {
                            for i in 0..10_000usize {
                                let target = targets[(p * i) % targets.len()];
                                scheduler
                                    .send(
                                        target,
                                        Message {
                                            sender: None,
                                            payload: MessagePayload::Custom(vec![
                                                (i % 256) as u8
                                            ]),
                                            priority: Priority::Normal,
                                        },
                                    )
                                    .await
                                    .unwrap();
                            }
                        });
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
            });

            scheduler.stop();
        });
    }

    group.finish();
}

criterion_group!(benches, bench_steal_contention);
criterion_main!(benches);
