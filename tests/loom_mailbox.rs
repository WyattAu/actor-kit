//! Loom model tests for the mailbox protocol (run with
//! `RUSTFLAGS="--cfg loom" cargo test --release --features loom loom --
//! --test-threads=1`).
//!
//! The models live in [`actor_kit::loom_mailbox`]: a loom-native double of
//! the mailbox's crate-owned discipline (permit/size accounting, the
//! enable-before-check recv handshake) with the trusted third-party
//! internals (tokio Semaphore/Notify, crossbeam ArrayQueue) replaced by
//! loom stand-ins. See that module's docs for the precise mapping.
//!
//! Scenarios per invariant:
//! * capacity gate — `loom_mailbox_capacity_gate_two_senders`,
//!   `loom_mailbox_permit_conservation_roundtrip`
//! * permit/size conservation & no loss/duplication —
//!   `loom_mailbox_permit_conservation_roundtrip`,
//!   `loom_mailbox_two_senders_receiver_drains_via_handshake`
//! * no lost wakeup — `loom_mailbox_recv_no_lost_wakeup`,
//!   `loom_mailbox_two_senders_receiver_drains_via_handshake`

#![cfg(loom)]

#[test]
fn loom_mailbox_capacity_gate_two_senders() {
    actor_kit::loom_mailbox::model_capacity_gate_two_senders();
}

#[test]
fn loom_mailbox_permit_conservation_roundtrip() {
    actor_kit::loom_mailbox::model_permit_conservation_roundtrip();
}

#[test]
fn loom_mailbox_recv_no_lost_wakeup() {
    actor_kit::loom_mailbox::model_recv_no_lost_wakeup();
}

#[test]
fn loom_mailbox_two_senders_receiver_drains_via_handshake() {
    actor_kit::loom_mailbox::model_two_senders_receiver_drains_via_handshake();
}

// --- Non-vacuousness proof (seeded race) --------------------------------
//
// Uncomment and run: the model MUST FAIL — loom reports the receiver
// parked forever ("deadlock detected") in exactly the interleaving the
// crate's enable-before-check ordering forecloses. It exercises the
// deliberately broken recv variant in
// `loom_mailbox::model_broken_recv_loses_wakeup`, which checks queue
// emptiness *before* registering with the wakeup gate: receiver checks
// empty → sender pushes + notifies (no waiter registered) → receiver
// registers → parks with no future notification. If this test ever
// *passes*, the wakeup models above are vacuous and must be fixed.
//
// #[test]
// #[should_panic(expected = "deadlock")]
// fn loom_broken_recv_loses_wakeup_is_caught() {
//     actor_kit::loom_mailbox::model_broken_recv_loses_wakeup();
// }
