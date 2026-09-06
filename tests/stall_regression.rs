//! Regression test for the sustained-delivery drain stall (fix in 0.1.1).
//!
//! Symptom on 0.1.0: an actor stops consuming after `mailbox_capacity`
//! cumulative messages. `Mailbox::send`/`try_send` acquire (and forget) one
//! semaphore permit per message, but the scheduler never popped the mailbox
//! copy when it processed the corresponding `Task` from the work queue, so
//! permits were never returned on the happy path. After `capacity` cumulative
//! sends the semaphore was exhausted and every subsequent `send` blocked
//! forever — the "message drain stall" (default capacity 10_000, hence the
//! ~10k threshold in PERF-SLO.md).
//!
//! This test sends far more than the capacity with continuous backpressure
//! and requires every message to be processed within a hard timeout.

use actor_kit::{
    ActorBuilder, ActorScheduler, MailboxConfig, Message, MessagePayload, Priority, SchedulerConfig,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ACTORS: u64 = 4;
const MSGS_PER_ACTOR: u64 = 12_500; // 50k total >> any mailbox capacity
const MAILBOX_CAP: usize = 64;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// N total messages across M actors complete without capacity exhaustion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_delivery_across_50k_messages_does_not_stall() {
    // 10 rounds: fresh runtime each round, exercising sustained delivery on a
    // long-lived actor well past the old ~10k stall threshold.
    for round in 0..10 {
        run_one_round(round).await;
    }
}

async fn run_one_round(round: u64) {
    let mut cfg = SchedulerConfig::new().workers(2);
    cfg.mailbox_config = MailboxConfig::new(MAILBOX_CAP);
    let scheduler = Arc::new(ActorScheduler::new(cfg));
    scheduler.start().unwrap();

    let mut handles = Vec::new();
    for i in 0..ACTORS {
        let handle = ActorBuilder::new()
            .name(format!("stall-reg-{round}-{i}"))
            .spawn(&scheduler)
            .unwrap();
        scheduler.set_actor_running(&handle.id()).unwrap();
        handle.start().await.unwrap();
        handles.push(handle);
    }

    let total = ACTORS * MSGS_PER_ACTOR;

    let drained = tokio::time::timeout(DRAIN_TIMEOUT, async {
        for (k, handle) in handles.iter().enumerate() {
            for i in 0..MSGS_PER_ACTOR {
                scheduler
                    .send(
                        handle.id(),
                        Message {
                            sender: None,
                            payload: MessagePayload::Custom(vec![((k as u64 + i) % 256) as u8]),
                            priority: Priority::Normal,
                        },
                    )
                    .await
                    .unwrap_or_else(|e| panic!("round {round}: send failed: {e}"));
            }
        }

        // All sends accepted; every message must be processed.
        let deadline = Instant::now() + DRAIN_TIMEOUT;
        loop {
            let processed: u64 = handles
                .iter()
                .map(|h| scheduler.registry().get_processed_count(&h.id()))
                .sum();
            if processed >= total + ACTORS {
                // +ACTORS: one Start message per actor.
                break processed;
            }
            assert!(
                Instant::now() < deadline,
                "round {round}: stall — processed {} / {} within timeout",
                processed,
                total + ACTORS
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;

    match drained {
        Ok(processed) => assert!(processed >= total + ACTORS),
        Err(_) => {
            let processed: u64 = handles
                .iter()
                .map(|h| scheduler.registry().get_processed_count(&h.id()))
                .sum();
            let mailbox_lens: Vec<usize> = handles.iter().map(|h| h.mailbox_size()).collect();
            panic!(
                "round {round}: TIMED OUT after {DRAIN_TIMEOUT:?} — stall reproduced \
                 (processed {processed}/{}; mailbox sizes {mailbox_lens:?})",
                total + ACTORS
            );
        }
    }

    scheduler.stop();
}
