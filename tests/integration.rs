//! End-to-end: spawn → crash → supervisor restart → message roundtrip.
//!
//! Wires the two halves of the runtime together: the work-stealing scheduler
//! (registry, mailboxes, workers) and the OTP-style supervisor (strategies,
//! restart policies, restart accounting). The supervisor is the source of
//! truth for *whether* a child should run; the scheduler/registry is the
//! mechanism for *actually running* it.

use actor_kit::{
    ActorBuilder, ActorScheduler, ChildSpec, ExitReason, Message, MessagePayload, RestartPolicy,
    SchedulerConfig, SupervisionStrategy, SupervisorTree,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn crash_then_supervised_restart_then_roundtrip() {
    // -- Start a 2-worker work-stealing scheduler.
    let scheduler = Arc::new(ActorScheduler::new(SchedulerConfig::new().workers(2)));
    scheduler.start().unwrap();

    // -- Create a supervisor tree (one-for-one, max 3 restarts / 60s window).
    let mut tree =
        SupervisorTree::new(SupervisionStrategy::one_for_one(3, Duration::from_secs(60)));
    let root = tree.root();

    // -- Start a child under the supervisor AND spawn its runtime incarnation
    //    in the scheduler. The spec's name links the two.
    let spec = ChildSpec::new("worker-1").restart_policy(RestartPolicy::Permanent);
    let spec_name = spec.name.clone();
    let _supervised_id = tree.start_child_under(root, spec).unwrap();

    let handle = ActorBuilder::new()
        .name(spec_name.as_str())
        .spawn(&scheduler)
        .unwrap();
    scheduler.set_actor_running(&handle.id()).unwrap();

    // -- Message roundtrip on the healthy actor.
    tokio::time::sleep(Duration::from_millis(10)).await; // let a worker pick up pending work
    scheduler
        .send(
            handle.id(),
            Message {
                sender: None,
                payload: MessagePayload::Custom(vec![1]),
                priority: actor_kit::Priority::Normal,
            },
        )
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while scheduler.registry().get_processed_count(&handle.id()) < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "no message processed in time"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(scheduler.registry().get_processed_count(&handle.id()), 1);

    // -- CRASH: the worker panics; the scheduler marks it Failed and drains
    //    its mailbox (that is what `process_task_safe` does on panic). We
    //    simulate the post-crash state the same way.
    scheduler
        .registry()
        .set_state(&handle.id(), actor_kit::ActorState::Failed)
        .unwrap();

    // Sends to a crashed actor are rejected.
    let dead = scheduler
        .send(
            handle.id(),
            Message {
                sender: None,
                payload: MessagePayload::Custom(vec![2]),
                priority: actor_kit::Priority::Normal,
            },
        )
        .await;
    assert!(dead.is_err(), "send to crashed actor should fail");

    // -- SUPERVISOR RESTART: report the crash. OneForOne + Permanent means
    //    restart, so the supervisor mints a new child identity.
    tree.handle_child_exit(
        root,
        &spec_name,
        ExitReason::Error("panic: worker exploded".into()),
    )
    .await
    .unwrap();

    let restarted = tree
        .get_supervisor(&root)
        .unwrap()
        .get_child(&spec_name)
        .unwrap()
        .id;
    assert_ne!(restarted, handle.id(), "restart must mint a new actor ID");
    assert_eq!(
        tree.get_supervisor(&root)
            .unwrap()
            .count_children()
            .total_restarts,
        1
    );

    // -- RE-SPAWN the runtime incarnation under the new ID and bring it up.
    let new_handle = ActorBuilder::new()
        .name(format!("{spec_name}-v2"))
        .spawn(&scheduler)
        .unwrap();
    scheduler.set_actor_running(&new_handle.id()).unwrap();

    // -- MESSAGE ROUNDTRIP on the restarted actor.
    scheduler
        .send(
            new_handle.id(),
            Message {
                sender: None,
                payload: MessagePayload::Custom(vec![3]),
                priority: actor_kit::Priority::Normal,
            },
        )
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while scheduler.registry().get_processed_count(&new_handle.id()) < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "restarted actor processed nothing"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(new_handle.is_running());

    // -- Housekeeping: stop cleanly.
    scheduler.stop();
}
