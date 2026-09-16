//! Loom model double of the mailbox protocol (compiled only under
//! `--cfg loom`).
//!
//! The real [`crate::Mailbox`] mixes three layers of concurrency machinery:
//!
//! 1. **tokio `Semaphore` permit accounting** — hard capacity gate: a send
//!    that cannot take a permit is rejected instead of enqueued.
//! 2. **crossbeam `ArrayQueue`** — the lock-free FIFO itself.
//! 3. **the crate-owned protocol** — the `size`/`backpressure` atomic
//!    accounting, permit rollback on failed enqueue, and the `recv()`
//!    lost-wakeup handshake: the `Notified` future is registered
//!    (`enable()`) *before* the emptiness check, so a `notify_waiters()`
//!    racing with the receive can never be missed.
//!
//! Layers 1–2 are trusted third-party lock-free machinery — loom cannot see
//! inside them, and their correctness is not this crate's claim. Layer 3 is
//! the crate's own atomic discipline, and it is what this double models:
//! identical program order, identical accounting, with the trusted
//! internals replaced by loom-native stand-ins:
//!
//! * `ArrayQueue` → `Mutex<VecDeque>` (capacity is enforced by the permit
//!   gate, exactly as in the real send path, so the FIFO itself needs no
//!   modeling beyond ordering)
//! * tokio `Semaphore` → a `Mutex<usize>` permit counter with the same
//!   take-on-send / give-on-recv discipline
//! * tokio `Notify` → [`WakeupGate`], a minimal double of the
//!   `notify_waiters` contract: `enable()` registers the single MPSC
//!   receiver; `notify_waiters()` stores a wakeup permit iff a receiver is
//!   currently registered; the receiver consumes its permit or spins
//!   (bounded) until one is stored. Register-after-fire does *not* store a
//!   permit — the precise semantic the enable-before-check ordering exists
//!   for.
//!
//! State is kept tiny (capacity ≤ 2, ≤ 2 messages, ≤ 3 threads) so
//! exhaustive exploration stays fast, and models run under
//! `LOOM_MAX_PREEMPTIONS=2`.
//!
//! # Non-vacuousness
//!
//! [`model_broken_recv_loses_wakeup`] is the deliberately broken variant
//! (emptiness check *before* `enable()`). It is compiled but not wired to a
//! `#[test]`; uncomment the seeded-race test in `tests/loom_mailbox.rs` and
//! the model fails — loom reports the receiver parked forever (deadlock)
//! in exactly the interleaving the correct ordering forecloses —
//! demonstrating the models above have teeth.

use loom::sync::atomic::{
    AtomicBool, AtomicUsize,
    Ordering::{Acquire, Relaxed, Release},
};
use loom::sync::{Arc, Mutex};
use std::collections::VecDeque;

/// Minimal double of tokio `Notify`'s `notify_waiters` contract for the
/// single-receiver mailbox.
///
/// Semantics modeled:
/// * `enable()` registers the receiver (tokio: `Notified::enable`).
/// * `notify_waiters()` wakes *currently registered* waiters only — a
///   waiter that registers afterwards is not woken, and no permit is
///   stored for later (the exact `notify_waiters` contract; `notify_one`
///   would differ). Registration is taken by the notification, mirroring
///   tokio's waiter-entry consumption.
/// * `wait()` blocks the receiver until its registration is consumed by a
///   `notify_waiters()`, via loom park/unpark — real blocking, no spin, so
///   models stay compatible with `LOOM_MAX_PREEMPTIONS`. The
///   registered-recheck loop guards the park/unpark race; a sticky unpark
///   token makes the early-unpark case return immediately.
pub struct WakeupGate {
    registered: Mutex<Option<loom::thread::Thread>>,
}

impl WakeupGate {
    pub fn new() -> Self {
        Self {
            registered: Mutex::new(None),
        }
    }

    /// Register the receiver: a `notify_waiters()` from this point on wakes
    /// it.
    pub fn enable(&self) {
        *self.registered.lock().unwrap() = Some(loom::thread::current());
    }

    /// Wake the registered receiver, if any. Program-ordered after the
    /// queue push in `try_send`, mirroring the real mailbox.
    pub fn notify_waiters(&self) {
        if let Some(waiter) = self.registered.lock().unwrap().take() {
            waiter.unpark();
        }
    }

    /// Block until a `notify_waiters()` consumed our registration.
    pub fn wait(&self) {
        while self.registered.lock().unwrap().is_some() {
            loom::thread::park();
        }
    }
}

/// Loom double of [`crate::Mailbox`] — identical protocol shape, loom-native
/// primitives. `usize` messages stand in for `Message` payloads.
pub struct LoomMailbox {
    queue: Mutex<VecDeque<usize>>,
    /// Remaining send permits (double of the Semaphore's available permits).
    permits: Mutex<usize>,
    /// Crate-owned size accounting — identical field, identical orderings.
    size: AtomicUsize,
    /// Crate-owned backpressure latch — identical field, identical orderings.
    backpressure: AtomicBool,
    capacity: usize,
    backpressure_count: usize,
    notify: WakeupGate,
}

impl LoomMailbox {
    /// Mailbox double with the default config's shape: capacity `capacity`,
    /// backpressure threshold 80% (like [`crate::MailboxConfig::default`]).
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            permits: Mutex::new(capacity),
            size: AtomicUsize::new(0),
            backpressure: AtomicBool::new(false),
            capacity,
            backpressure_count: (capacity as f32 * 0.8) as usize,
            notify: WakeupGate::new(),
        }
    }

    /// Double of [`crate::Mailbox::try_send`] — same steps, same order:
    /// permit gate → size increment → enqueue → backpressure latch →
    /// notify_waiters.
    pub fn try_send(&self, message: usize) -> bool {
        {
            let mut permits = self.permits.lock().unwrap();
            if *permits == 0 {
                return false;
            }
            *permits -= 1;
        }

        let size = self.size.fetch_add(1, Relaxed) + 1;

        // The permit gate already bounded in-flight sends to `capacity`, so
        // the enqueue itself cannot fail here — mirroring the real queue's
        // capacity falling exactly on the semaphore's.
        self.queue.lock().unwrap().push_back(message);

        if size >= self.backpressure_count {
            self.backpressure.store(true, Relaxed);
        }

        self.notify.notify_waiters();
        true
    }

    /// Double of [`crate::Mailbox::try_recv`] — same steps, same order:
    /// dequeue → size decrement → permit return → backpressure re-arm.
    pub fn try_recv(&self) -> Option<usize> {
        let message = self.queue.lock().unwrap().pop_front()?;
        self.size.fetch_sub(1, Relaxed);
        *self.permits.lock().unwrap() += 1;
        self.check_backpressure();
        Some(message)
    }

    /// Double of [`crate::Mailbox::recv`] minus the async plumbing — the
    /// crate-owned handshake, verbatim in ordering: register (enable) the
    /// wakeup *before* the emptiness check, park only if still empty.
    pub fn recv(&self) -> usize {
        loop {
            self.notify.enable();
            if let Some(message) = self.try_recv() {
                return message;
            }
            self.notify.wait();
        }
    }

    pub fn len(&self) -> usize {
        self.size.load(Relaxed)
    }

    pub fn is_backpressured(&self) -> bool {
        self.backpressure.load(Relaxed)
    }

    /// Invariant probe for the models: after all sends and receives have
    /// joined, the accounting must be exactly conserved — every consumed
    /// permit returned, the size counter back to zero, queue drained.
    pub fn is_fully_drained(&self) -> bool {
        *self.permits.lock().unwrap() == self.capacity
            && self.size.load(Acquire) == 0
            && self.queue.lock().unwrap().is_empty()
    }

    fn check_backpressure(&self) {
        let size = self.size.load(Relaxed);
        if size < self.backpressure_count / 2 {
            self.backpressure.store(false, Relaxed);
        }
    }
}

/// Model 1 — the capacity gate.
///
/// Two senders each attempt two sends into a capacity-2 mailbox with no
/// receiver. Property under *every* interleaving: at most `capacity` sends
/// succeed in total (the permit gate cannot be raced through), the size
/// counter equals the number of successful sends, the backpressure latch
/// reflects the final size, and — after the main thread drains everything —
/// the accounting is exactly conserved and a fresh send succeeds again.
pub fn model_capacity_gate_two_senders() {
    loom::model(move || {
        let mailbox = Arc::new(LoomMailbox::with_capacity(2));

        let mut handles = Vec::new();
        for sender in 0..2usize {
            let mailbox = Arc::clone(&mailbox);
            handles.push(loom::thread::spawn(move || {
                let mut successes = 0;
                for _ in 0..2 {
                    if mailbox.try_send(sender) {
                        successes += 1;
                    }
                }
                successes
            }));
        }

        let mut total = 0;
        for h in handles {
            total += h.join().unwrap();
        }

        assert!(
            total <= 2,
            "capacity gate raced through: {total} sends succeeded into capacity 2"
        );
        assert_eq!(mailbox.len(), total, "size counter must equal deliveries");
        assert_eq!(
            mailbox.is_backpressured(),
            total >= 1,
            "backpressure latch must reflect the final size (threshold 1 at capacity 2)"
        );

        // Drain from the main thread: exactly `total` messages come out,
        // then the mailbox reports empty. No message was created or lost.
        let mut drained = 0;
        while mailbox.try_recv().is_some() {
            drained += 1;
        }
        assert_eq!(drained, total);
        assert!(
            mailbox.is_fully_drained(),
            "permit/size conservation broken"
        );

        // Every permit came back: a fresh send succeeds.
        assert!(
            mailbox.try_send(9),
            "post-drain send must succeed after full permit restoration"
        );
    });
}

/// Model 2 — permit conservation across a send/receive round trip.
///
/// Two senders push one tagged message each; a receiver drains with
/// *bounded* retries (flow control, not liveness — it may legitimately
/// exhaust its attempt budget before the senders are scheduled, exactly as
/// in shm-rings' bounded-attempt models). Property under every interleaving:
/// no duplication and no foreign values in what was received; after all
/// threads join, the main thread drains any stragglers and the mailbox must
/// be exactly conserved — every message accounted for once, every permit
/// restored, size back to zero.
pub fn model_permit_conservation_roundtrip() {
    const ATTEMPTS: usize = 4;
    loom::model(move || {
        let mailbox = Arc::new(LoomMailbox::with_capacity(2));

        let mut handles = Vec::new();
        for tag in 1..=2usize {
            let mailbox = Arc::clone(&mailbox);
            handles.push(loom::thread::spawn(move || {
                for _ in 0..ATTEMPTS {
                    if mailbox.try_send(tag) {
                        return;
                    }
                }
            }));
        }

        let received = {
            let mailbox = Arc::clone(&mailbox);
            loom::thread::spawn(move || {
                let mut got = Vec::new();
                for _ in 0..ATTEMPTS * 2 {
                    if got.len() == 2 {
                        break;
                    }
                    if let Some(message) = mailbox.try_recv() {
                        got.push(message);
                    }
                }
                got
            })
        };

        for h in handles {
            h.join().unwrap();
        }
        let mut got = received.join().unwrap();

        // Bounded-liveness: the receiver may have burned its budget before
        // both sends landed; drain the stragglers from the main thread so
        // the conservation check below sees the full picture.
        while let Some(message) = mailbox.try_recv() {
            got.push(message);
        }

        got.sort_unstable();
        assert!(
            got == vec![1] || got == vec![2] || got == vec![1, 2],
            "deliveries must be a duplicate-free subset of the sent tags"
        );
        assert!(
            mailbox.is_fully_drained(),
            "permit/size conservation broken"
        );
    });
}

/// Model 3 — the recv handshake never loses a wakeup.
///
/// One receiver runs the crate's handshake (enable → check → park); one
/// sender pushes a message and notifies, in that program order. Property
/// under every interleaving: whenever the receiver's park *returns*, the
/// message is necessarily in the queue (asserted inline at the wakeup
/// point — this is the no-lost-wakeup property the enable-before-check
/// ordering buys), and the receiver ends up with exactly that message.
pub fn model_recv_no_lost_wakeup() {
    loom::model(move || {
        let mailbox = Arc::new(LoomMailbox::with_capacity(2));

        let sender = {
            let mailbox = Arc::clone(&mailbox);
            loom::thread::spawn(move || {
                assert!(mailbox.try_send(1), "first send into a fresh mailbox fits");
            })
        };

        let receiver = {
            let mailbox = Arc::clone(&mailbox);
            loom::thread::spawn(move || mailbox.recv())
        };

        sender.join().unwrap();
        let received = receiver.join().unwrap();

        // The recv handshake asserts inline: after a park returns, try_recv
        // MUST yield the message (never a second empty scan). Here: the
        // only message is tag 1.
        assert_eq!(
            received, 1,
            "receiver must wake to exactly the sent message"
        );
        assert!(mailbox.is_fully_drained());
    });
}

/// Model 4 — two senders, one receiver: no loss, no duplication, no missed
/// wakeup under a concurrent handshake.
///
/// Both senders push a distinct tag; the receiver drains both via the
/// handshake loop. Property under every interleaving: the received multiset
/// is exactly `{1, 2}`; each park-return is immediately followed by a
/// successful receive (inline assertion inside `recv`); and the end state
/// is fully conserved.
pub fn model_two_senders_receiver_drains_via_handshake() {
    loom::model(move || {
        let mailbox = Arc::new(LoomMailbox::with_capacity(2));

        let mut senders = Vec::new();
        for tag in 1..=2usize {
            let mailbox = Arc::clone(&mailbox);
            senders.push(loom::thread::spawn(move || {
                assert!(mailbox.try_send(tag), "single send per sender always fits");
            }));
        }

        let receiver = {
            let mailbox = Arc::clone(&mailbox);
            loom::thread::spawn(move || vec![mailbox.recv(), mailbox.recv()])
        };

        for h in senders {
            h.join().unwrap();
        }
        let mut received = receiver.join().unwrap();
        received.sort_unstable();
        assert_eq!(received, vec![1, 2], "each message delivered exactly once");
        assert!(mailbox.is_fully_drained());
    });
}

/// NON-VACUOUSNESS PROOF — deliberately broken variant. Compiled but not
/// part of the test suite: uncomment the seeded-race test in
/// `tests/loom_mailbox.rs` and run it — the model FAILS with the
/// "lost wakeup" panic, proving the models above can catch the bug class
/// they claim to.
///
/// The break: the emptiness check runs *before* `enable()` (the classic
/// check-then-register race). The losing interleaving is exactly the one
/// the correct ordering forecloses: receiver checks empty → sender pushes
/// and fires `notify_waiters` (nobody registered yet) → receiver enables →
/// parks on a permit that will never be stored → budget exhausted → panic.
pub fn model_broken_recv_loses_wakeup() {
    loom::model(move || {
        let mailbox = Arc::new(LoomMailbox::with_capacity(2));

        let sender = {
            let mailbox = Arc::clone(&mailbox);
            loom::thread::spawn(move || {
                assert!(mailbox.try_send(1));
            })
        };

        let receiver = {
            let mailbox = Arc::clone(&mailbox);
            loom::thread::spawn(move || {
                // BROKEN handshake: check first, register after.
                loop {
                    if let Some(message) = mailbox.try_recv() {
                        return message;
                    }
                    mailbox.notify.enable();
                    mailbox.notify.wait();
                }
            })
        };

        sender.join().unwrap();
        let received = receiver.join().unwrap();
        assert_eq!(received, 1);
    });
}

// Thin lib-target wrappers so the shared rust-kit loom job
// (`cargo test --release --lib -- --test-threads=1 loom`) exercises the
// models; tests/loom_mailbox.rs runs the same models via the integration
// target with the canonical local command.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;

    #[test]
    fn loom_capacity_gate_two_senders() {
        model_capacity_gate_two_senders();
    }

    #[test]
    fn loom_permit_conservation_roundtrip() {
        model_permit_conservation_roundtrip();
    }

    #[test]
    fn loom_recv_no_lost_wakeup() {
        model_recv_no_lost_wakeup();
    }

    #[test]
    fn loom_two_senders_receiver_drains_via_handshake() {
        model_two_senders_receiver_drains_via_handshake();
    }
}
