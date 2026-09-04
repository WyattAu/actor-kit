//! Spawn throughput: how fast can we register actors on the scheduler.

use actor_kit::{ActorScheduler, SchedulerConfig};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

fn bench_spawn(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawn");

    for workers in [1, 4] {
        group.throughput(criterion::Throughput::Elements(1));
        group.bench_function(format!("spawn_1000_actors_{workers}w"), |b| {
            let scheduler = Arc::new(ActorScheduler::new(SchedulerConfig::new().workers(workers)));
            scheduler.start().unwrap();
            b.iter(|| {
                for _ in 0..1000 {
                    criterion::black_box(scheduler.spawn().unwrap());
                }
            });
            scheduler.stop();
        });
    }

    group.finish();
}

criterion_group!(benches, bench_spawn);
criterion_main!(benches);
