//! Lock-free work queue with work stealing support.
//!
//! Uses crossbeam-deque for efficient work stealing between workers.

use crossbeam_deque::{Injector, Steal, Stealer, Worker};

use crate::{ActorId, Message, Priority};

/// A task to be executed by a worker.
///
/// When `additional_messages` is non-empty the task represents a batch: the
/// worker processes `message` first, then each entry in `additional_messages`.
/// This avoids pushing N separate `Task`s onto the work-stealing queue.
#[derive(Debug)]
pub struct Task {
    /// Target actor ID
    pub actor_id: ActorId,
    /// Message to process
    pub message: Message,
    /// Priority level
    pub priority: Priority,
    /// Extra messages that belong to the same batch (processed after `message`).
    pub additional_messages: Vec<Message>,
}

/// Global work queue for task injection.
pub struct WorkQueue {
    /// Global injector for new tasks
    injector: Injector<Task>,
    /// Number of workers
    worker_count: std::sync::atomic::AtomicUsize,
}

impl WorkQueue {
    /// Create a new work queue.
    pub fn new() -> Self {
        Self {
            injector: Injector::new(),
            worker_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Push a task to the global queue.
    pub fn push(&self, task: Task) {
        self.injector.push(task);
    }

    /// Push multiple tasks to the global queue.
    pub fn push_batch(&self, tasks: Vec<Task>) {
        for task in tasks {
            self.injector.push(task);
        }
    }

    /// Steal a task from the global queue.
    pub fn steal_global(&self) -> Option<Task> {
        loop {
            match self.injector.steal() {
                Steal::Success(task) => return Some(task),
                Steal::Empty => return None,
                Steal::Retry => continue,
            }
        }
    }

    /// Get the global injector reference.
    pub fn injector(&self) -> &Injector<Task> {
        &self.injector
    }

    /// Register a new worker.
    pub fn register_worker(&self) {
        self.worker_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Unregister a worker.
    pub fn unregister_worker(&self) {
        self.worker_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Default for WorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a new local worker queue and return both the worker and stealer.
pub fn create_local_queue() -> (Worker<Task>, Stealer<Task>) {
    let worker = Worker::new_fifo();
    let stealer = worker.stealer();
    (worker, stealer)
}

/// Work stealer for stealing from other workers.
pub struct WorkStealer {
    /// Stealers for all worker queues
    stealers: Vec<Stealer<Task>>,
    /// Index for round-robin stealing
    index: std::sync::atomic::AtomicUsize,
}

impl WorkStealer {
    /// Create a new work stealer with the given stealers.
    pub fn new(stealers: Vec<Stealer<Task>>) -> Self {
        Self {
            stealers,
            index: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Steal a task from another worker using work stealing.
    pub fn steal(&self) -> Option<Task> {
        if self.stealers.is_empty() {
            return None;
        }

        let start = self
            .index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let len = self.stealers.len();

        for i in 0..len {
            let idx = (start + i) % len;
            loop {
                match self.stealers[idx].steal() {
                    Steal::Success(task) => {
                        return Some(task);
                    }
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }
        }
        None
    }

    /// Steal multiple tasks (batch steal).
    pub fn steal_batch(&self, dest: &Worker<Task>, max: usize) -> usize {
        if self.stealers.is_empty() {
            return 0;
        }

        let start = self
            .index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let len = self.stealers.len();
        let mut stolen = 0;

        for i in 0..len {
            if stolen >= max {
                break;
            }
            let idx = (start + i) % len;
            loop {
                match self.stealers[idx].steal_batch_and_pop(dest) {
                    Steal::Success(task) => {
                        dest.push(task);
                        stolen += 1;
                    }
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }
        }
        stolen
    }
}

/// Priority-aware work queue wrapper.
pub struct PriorityQueue {
    /// Critical priority tasks
    critical: Injector<Task>,
    /// High priority tasks
    high: Injector<Task>,
    /// Normal priority tasks
    normal: Injector<Task>,
    /// Low priority tasks
    low: Injector<Task>,
}

impl PriorityQueue {
    /// Create a new priority queue.
    pub fn new() -> Self {
        Self {
            critical: Injector::new(),
            high: Injector::new(),
            normal: Injector::new(),
            low: Injector::new(),
        }
    }

    /// Push a task with the given priority.
    pub fn push(&self, task: Task) {
        match task.priority {
            Priority::Critical => self.critical.push(task),
            Priority::High => self.high.push(task),
            Priority::Normal => self.normal.push(task),
            Priority::Low => self.low.push(task),
        }
    }

    /// Pop the highest priority task available.
    pub fn pop(&self) -> Option<Task> {
        fn try_steal(injector: &Injector<Task>) -> Option<Task> {
            loop {
                match injector.steal() {
                    Steal::Success(task) => return Some(task),
                    Steal::Empty => return None,
                    Steal::Retry => continue,
                }
            }
        }

        try_steal(&self.critical)
            .or_else(|| try_steal(&self.high))
            .or_else(|| try_steal(&self.normal))
            .or_else(|| try_steal(&self.low))
    }
}

impl Default for PriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MessagePayload;

    #[test]
    fn test_work_queue_basic() {
        let queue = WorkQueue::new();
        let task = Task {
            actor_id: ActorId::new(),
            message: Message {
                sender: None,
                payload: MessagePayload::Start,
                priority: Priority::Normal,
            },
            priority: Priority::Normal,
            additional_messages: Vec::new(),
        };

        queue.push(task);
        let stolen = queue.steal_global();
        assert!(stolen.is_some());
    }

    #[test]
    fn test_local_queue() {
        let (worker, _stealer) = create_local_queue();
        let task = Task {
            actor_id: ActorId::new(),
            message: Message {
                sender: None,
                payload: MessagePayload::Start,
                priority: Priority::Normal,
            },
            priority: Priority::Normal,
            additional_messages: Vec::new(),
        };

        worker.push(task);
        assert_eq!(worker.len(), 1);
        let popped = worker.pop();
        assert!(popped.is_some());
        assert!(worker.is_empty());
    }

    #[test]
    fn test_priority_queue() {
        let queue = PriorityQueue::new();

        let low_task = Task {
            actor_id: ActorId::new(),
            message: Message {
                sender: None,
                payload: MessagePayload::Start,
                priority: Priority::Low,
            },
            priority: Priority::Low,
            additional_messages: Vec::new(),
        };
        let high_task = Task {
            actor_id: ActorId::new(),
            message: Message {
                sender: None,
                payload: MessagePayload::Start,
                priority: Priority::High,
            },
            priority: Priority::High,
            additional_messages: Vec::new(),
        };

        queue.push(low_task);
        queue.push(high_task);

        let first = queue.pop().unwrap();
        assert_eq!(first.priority, Priority::High);

        let second = queue.pop().unwrap();
        assert_eq!(second.priority, Priority::Low);
    }
}
