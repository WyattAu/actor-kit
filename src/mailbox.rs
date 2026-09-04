//! MPSC mailbox for actors with bounded capacity and backpressure.
//!
//! Each actor has its own mailbox for receiving messages.
//! Uses lock-free `crossbeam_queue::ArrayQueue` for the message queues
//! and `tokio::sync::Notify` for efficient async waiting.

use crossbeam_queue::ArrayQueue;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore};

use crate::Error;
use crate::{ActorId, Message, Priority};

/// Configuration for an actor mailbox.
#[derive(Debug, Clone)]
pub struct MailboxConfig {
    /// Maximum capacity of the mailbox
    pub capacity: usize,
    /// Enable priority queuing
    pub priority_queue: bool,
    /// Backpressure threshold (percentage of capacity)
    pub backpressure_threshold: f32,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            priority_queue: true,
            backpressure_threshold: 0.8,
        }
    }
}

impl MailboxConfig {
    /// Create a new mailbox config with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            ..Default::default()
        }
    }

    /// Calculate the backpressure threshold in message count.
    pub fn backpressure_count(&self) -> usize {
        (self.capacity as f32 * self.backpressure_threshold) as usize
    }
}

/// MPSC mailbox for an actor.
///
/// Uses lock-free bounded queues internally.
/// Backpressure is coordinated via a semaphore; async wakeup via `Notify`.
pub struct Mailbox {
    /// Actor ID this mailbox belongs to
    actor_id: ActorId,
    /// Configuration
    config: MailboxConfig,
    /// Lock-free message queue
    queue: ArrayQueue<Message>,
    /// Current queue size
    size: AtomicUsize,
    /// Semaphore for bounded capacity
    semaphore: Arc<Semaphore>,
    /// Whether the mailbox is in backpressure state
    backpressure: AtomicBool,
    /// Lock-free critical priority queue
    critical_queue: ArrayQueue<Message>,
    /// Notify waiters when a message is enqueued
    notify: Arc<Notify>,
}

impl Mailbox {
    /// Create a new mailbox.
    pub fn new(actor_id: ActorId, config: MailboxConfig) -> Self {
        let capacity = config.capacity;
        Self {
            actor_id,
            config,
            queue: ArrayQueue::new(capacity),
            size: AtomicUsize::new(0),
            semaphore: Arc::new(Semaphore::new(capacity)),
            backpressure: AtomicBool::new(false),
            critical_queue: ArrayQueue::new(capacity),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Try to send a message to the mailbox (non-blocking).
    pub fn try_send(&self, message: Message) -> Result<(), (Message, Error)> {
        let semaphore = self.semaphore.clone();
        let permit = semaphore.try_acquire_owned();
        match permit {
            Ok(permit) => {
                permit.forget();
            }
            Err(_) => {
                return Err((
                    message,
                    Error::resource_exhausted(format!(
                        "mailbox for actor {:?} is full",
                        self.actor_id
                    )),
                ));
            }
        }

        let size = self.size.fetch_add(1, Ordering::Relaxed) + 1;

        if let Err(rejected) =
            if self.config.priority_queue && message.priority == Priority::Critical {
                self.critical_queue.push(message)
            } else {
                self.queue.push(message)
            }
        {
            self.size.fetch_sub(1, Ordering::Relaxed);
            self.semaphore.add_permits(1);
            return Err((
                rejected,
                Error::resource_exhausted(format!(
                    "mailbox for actor {:?} internal queue is full",
                    self.actor_id
                )),
            ));
        }

        if size >= self.config.backpressure_count() {
            self.backpressure.store(true, Ordering::Relaxed);
        }

        self.notify.notify_waiters();

        Ok(())
    }

    /// Send a message to the mailbox (async, waits if full).
    pub async fn send(&self, message: Message) -> crate::Result<()> {
        let semaphore = self.semaphore.clone();
        let permit = semaphore
            .acquire()
            .await
            .map_err(|e| Error::resource_exhausted(e.to_string()))?;
        permit.forget();

        let size = self.size.fetch_add(1, Ordering::Relaxed) + 1;

        if if self.config.priority_queue && message.priority == Priority::Critical {
            self.critical_queue.push(message)
        } else {
            self.queue.push(message)
        }
        .is_err()
        {
            self.size.fetch_sub(1, Ordering::Relaxed);
            self.semaphore.add_permits(1);
            return Err(Error::resource_exhausted(format!(
                "mailbox for actor {:?} internal queue is full",
                self.actor_id
            )));
        }

        if size >= self.config.backpressure_count() {
            self.backpressure.store(true, Ordering::Relaxed);
        }

        self.notify.notify_waiters();

        Ok(())
    }

    /// Receive a message from the mailbox (non-blocking).
    pub fn try_recv(&self) -> Option<Message> {
        if self.config.priority_queue {
            if let Some(msg) = self.critical_queue.pop() {
                self.size.fetch_sub(1, Ordering::Relaxed);
                self.semaphore.add_permits(1);
                self.check_backpressure();
                return Some(msg);
            }
        }

        if let Some(msg) = self.queue.pop() {
            self.size.fetch_sub(1, Ordering::Relaxed);
            self.semaphore.add_permits(1);
            self.check_backpressure();
            return Some(msg);
        }

        None
    }

    /// Receive a message from the mailbox (async, waits if empty).
    ///
    /// Uses `tokio::sync::Notify` for efficient wakeup instead of busy-polling.
    pub async fn recv(&self) -> Message {
        loop {
            if let Some(msg) = self.try_recv() {
                return msg;
            }
            self.notify.notified().await;
        }
    }

    /// Get the current queue size.
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Check if the mailbox is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if the mailbox is in backpressure state.
    pub fn is_backpressured(&self) -> bool {
        self.backpressure.load(Ordering::Relaxed)
    }

    /// Get the mailbox capacity.
    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    /// Get remaining capacity.
    pub fn remaining_capacity(&self) -> usize {
        self.config.capacity.saturating_sub(self.len())
    }

    /// Clear all messages from the mailbox.
    pub fn clear(&self) {
        let mut total = 0;

        while self.critical_queue.pop().is_some() {
            total += 1;
        }
        while self.queue.pop().is_some() {
            total += 1;
        }

        if total > 0 {
            self.size.fetch_sub(total, Ordering::Relaxed);
            self.semaphore.add_permits(total);
        }
        self.backpressure.store(false, Ordering::Relaxed);
    }

    fn check_backpressure(&self) {
        let size = self.size.load(Ordering::Relaxed);
        if size < (self.config.backpressure_count() / 2) {
            self.backpressure.store(false, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MessagePayload;

    #[test]
    fn test_mailbox_basic() {
        let mailbox = Mailbox::new(ActorId::new(), MailboxConfig::default());

        let msg = Message {
            sender: None,
            payload: MessagePayload::Start,
            priority: Priority::Normal,
        };

        mailbox.try_send(msg).unwrap();
        assert_eq!(mailbox.len(), 1);

        let received = mailbox.try_recv();
        assert!(received.is_some());
        assert!(mailbox.is_empty());
    }

    #[test]
    fn test_mailbox_backpressure() {
        let config = MailboxConfig {
            capacity: 10,
            backpressure_threshold: 0.8,
            ..Default::default()
        };
        let mailbox = Mailbox::new(ActorId::new(), config);

        for i in 0..8 {
            let msg = Message {
                sender: None,
                payload: MessagePayload::Custom(vec![i as u8]),
                priority: Priority::Normal,
            };
            mailbox.try_send(msg).unwrap();
        }

        assert!(mailbox.is_backpressured());
    }

    #[test]
    fn test_mailbox_priority() {
        let config = MailboxConfig {
            priority_queue: true,
            ..Default::default()
        };
        let mailbox = Mailbox::new(ActorId::new(), config);

        let normal_msg = Message {
            sender: None,
            payload: MessagePayload::Custom(vec![1]),
            priority: Priority::Normal,
        };
        let critical_msg = Message {
            sender: None,
            payload: MessagePayload::Custom(vec![2]),
            priority: Priority::Critical,
        };

        mailbox.try_send(normal_msg).unwrap();
        mailbox.try_send(critical_msg).unwrap();

        let first = mailbox.try_recv().unwrap();
        assert_eq!(first.priority, Priority::Critical);
    }

    #[tokio::test]
    async fn test_mailbox_async() {
        let mailbox = Mailbox::new(ActorId::new(), MailboxConfig::default());

        let msg = Message {
            sender: None,
            payload: MessagePayload::Start,
            priority: Priority::Normal,
        };

        mailbox.send(msg).await.unwrap();
        let received = mailbox.recv().await;
        assert!(matches!(received.payload, MessagePayload::Start));
    }

    #[tokio::test]
    async fn test_recv_waits_then_wakes() {
        let mailbox = Arc::new(Mailbox::new(ActorId::new(), MailboxConfig::default()));
        let mailbox2 = mailbox.clone();

        let handle = tokio::spawn(async move { mailbox2.recv().await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let msg = Message {
            sender: None,
            payload: MessagePayload::Custom(vec![42]),
            priority: Priority::Normal,
        };
        mailbox.try_send(msg).unwrap();

        let received = handle.await.expect("recv task should not panic");
        assert!(matches!(received.payload, MessagePayload::Custom(_)));
    }

    #[test]
    fn test_mailbox_clear() {
        let config = MailboxConfig {
            capacity: 10,
            priority_queue: true,
            ..Default::default()
        };
        let mailbox = Mailbox::new(ActorId::new(), config);

        for i in 0..5 {
            let msg = Message {
                sender: None,
                payload: MessagePayload::Custom(vec![i]),
                priority: Priority::Normal,
            };
            mailbox.try_send(msg).unwrap();
        }
        let critical_msg = Message {
            sender: None,
            payload: MessagePayload::Custom(vec![99]),
            priority: Priority::Critical,
        };
        mailbox.try_send(critical_msg).unwrap();

        assert_eq!(mailbox.len(), 6);
        mailbox.clear();
        assert!(mailbox.is_empty());
        assert!(!mailbox.is_backpressured());
    }
}
