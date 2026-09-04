//! Property-based tests.
//!
//! 1. Mailbox delivery: for N actors × M messages, every message is
//!    delivered at-least-once and accounted for exactly (mailbox is a lossless
//!    bounded queue under backpressured sends: senders wait until there is
//!    capacity, so nothing is dropped).
//! 2. Supervisor restart accounting: restarts within the rate-limit window
//!    never exceed `max_restarts`; the (N+1)-th crash is escalated as an
//!    error.

use actor_kit::{ActorId, Mailbox, MailboxConfig, Message, MessagePayload, Priority};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use std::sync::Arc;
use std::time::Duration;

fn msg(i: u64) -> Message {
    Message {
        sender: Some(ActorId::default()),
        payload: MessagePayload::Custom(i.to_le_bytes().to_vec()),
        priority: Priority::Normal,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// N actors, M messages each: every sent message is received; per-actor
    /// delivery counts are exact (at-least-once with zero duplication).
    #[test]
    fn all_messages_delivered(n_actors in 1usize..=8, m_messages in 1usize..=200) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mailboxes: Vec<Arc<Mailbox>> = (0..n_actors)
                .map(|_| {
                    Arc::new(Mailbox::new(
                        ActorId::new(),
                        MailboxConfig::new(m_messages.max(1)),
                    ))
                })
                .collect();

            // Send M messages to every actor concurrently; the bounded
            // mailboxes backpressure senders instead of dropping.
            let mut senders = Vec::new();
            for mb in &mailboxes {
                let mb = Arc::clone(mb);
                senders.push(tokio::spawn(async move {
                    for i in 0..m_messages as u64 {
                        mb.send(msg(i)).await.expect("send should not fail");
                    }
                }));
            }
            for s in senders {
                s.await.unwrap();
            }

            // Receive everything and account for it.
            for mb in &mailboxes {
                let mut seen = vec![0u64; m_messages];
                let mut received = 0usize;
                while received < m_messages {
                    let m = mb.recv().await;
                    if let MessagePayload::Custom(bytes) = &m.payload {
                        let mut arr = [0u8; 8];
                        arr.copy_from_slice(bytes);
                        let idx = u64::from_le_bytes(arr) as usize;
                        seen[idx] += 1;
                    }
                    received += 1;
                }
                prop_assert!(seen.iter().all(|&c| c == 1), "exactly-once accounting violated");
                assert!(mb.is_empty());
            }
            Ok::<(), TestCaseError>(())
        })
        .unwrap();
    }

    /// Restart count respects max_restarts within the window: with a
    /// one-for-one(max_restarts = r, within = 1h) supervisor, exactly
    /// `min(k, r)` restarts happen for k crashes; crash k > r is an error.
    /// (`r == 0` is the runtime's "unlimited" convention.)
    #[test]
    fn restarts_respect_max_restarts(k in 1u32..=50u32, r in 0u32..=10u32) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut supervisor =
                actor_kit::Supervisor::new(actor_kit::SupervisionStrategy::one_for_one(
                    r,
                    Duration::from_secs(3600),
                ));
            supervisor
                .start_child(actor_kit::ChildSpec::new("child").restart_policy(actor_kit::RestartPolicy::Permanent))
                .unwrap();

            for crash in 1..=k {
                let result = supervisor
                    .handle_child_exit("child", actor_kit::ExitReason::Error("crash".into()))
                    .await;
                if r != 0 && crash > r {
                    prop_assert!(result.is_err(), "restart {crash} should exceed max_restarts={r}");
                    break;
                }
                prop_assert!(result.is_ok(), "restart {crash} should be allowed");
            }

            let expected = if r == 0 { k } else { k.min(r) };
            prop_assert_eq!(supervisor.count_children().total_restarts, expected as u64);
            Ok::<(), TestCaseError>(())
        })
        .unwrap();
    }
}
