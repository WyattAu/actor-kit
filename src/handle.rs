//! Actor handle for sending messages and querying state.
//!
//! Provides a reference to an actor for interaction.

use std::sync::Arc;

use crate::{ActorId, ActorScheduler, ActorState, Message, MessagePayload, Priority};

/// Handle to an actor for sending messages and querying state.
#[derive(Clone)]
pub struct ActorHandle {
    /// Actor ID
    id: ActorId,
    /// Scheduler reference
    scheduler: Arc<ActorScheduler>,
}

impl ActorHandle {
    /// Create a new actor handle.
    pub(crate) fn new(id: ActorId, scheduler: Arc<ActorScheduler>) -> Self {
        Self { id, scheduler }
    }

    /// Get the actor ID.
    pub fn id(&self) -> ActorId {
        self.id
    }

    /// Send a message to this actor.
    pub async fn send(&self, payload: MessagePayload) -> crate::Result<()> {
        self.scheduler
            .send(
                self.id,
                Message {
                    sender: None,
                    payload,
                    priority: Priority::Normal,
                },
            )
            .await
    }

    /// Send a message with a specific priority.
    pub async fn send_with_priority(
        &self,
        payload: MessagePayload,
        priority: Priority,
    ) -> crate::Result<()> {
        self.scheduler
            .send(
                self.id,
                Message {
                    sender: None,
                    payload,
                    priority,
                },
            )
            .await
    }

    /// Send a message from another actor.
    pub async fn send_from(&self, sender: ActorId, payload: MessagePayload) -> crate::Result<()> {
        self.scheduler
            .send(
                self.id,
                Message {
                    sender: Some(sender),
                    payload,
                    priority: Priority::Normal,
                },
            )
            .await
    }

    /// Send a start signal to the actor.
    pub async fn start(&self) -> crate::Result<()> {
        self.send(MessagePayload::Start).await
    }

    /// Send a stop signal to the actor.
    pub async fn stop(&self) -> crate::Result<()> {
        self.send(MessagePayload::Stop).await
    }

    /// Send a pause signal to the actor.
    pub async fn pause(&self) -> crate::Result<()> {
        self.send(MessagePayload::Signal(crate::Signal::Pause))
            .await
    }

    /// Send a resume signal to the actor.
    pub async fn resume(&self) -> crate::Result<()> {
        self.send(MessagePayload::Signal(crate::Signal::Resume))
            .await
    }

    /// Send a restart signal to the actor.
    pub async fn restart(&self) -> crate::Result<()> {
        self.send(MessagePayload::Signal(crate::Signal::Restart))
            .await
    }

    /// Query the actor's state.
    pub fn state(&self) -> Option<ActorState> {
        self.scheduler.registry().get_state(&self.id)
    }

    /// Check if the actor is running.
    pub fn is_running(&self) -> bool {
        self.state() == Some(ActorState::Running)
    }

    /// Check if the actor is stopped.
    pub fn is_stopped(&self) -> bool {
        self.state() == Some(ActorState::Stopped)
    }

    /// Check if the actor is suspended.
    pub fn is_suspended(&self) -> bool {
        self.state() == Some(ActorState::Suspended)
    }

    /// Check if the actor is in backpressure.
    pub fn is_backpressured(&self) -> bool {
        self.scheduler
            .registry()
            .get_mailbox(&self.id)
            .map(|m| m.is_backpressured())
            .unwrap_or(false)
    }

    /// Get the mailbox size.
    pub fn mailbox_size(&self) -> usize {
        self.scheduler
            .registry()
            .get_mailbox(&self.id)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Get the number of messages processed by this actor.
    pub fn processed_count(&self) -> u64 {
        self.scheduler.registry().get_processed_count(&self.id)
    }
}

impl std::fmt::Debug for ActorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorHandle")
            .field("id", &self.id)
            .field("state", &self.state())
            .finish()
    }
}

impl std::hash::Hash for ActorHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for ActorHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ActorHandle {}

/// Builder for creating actors.
pub struct ActorBuilder {
    name: Option<String>,
    priority: Priority,
}

impl ActorBuilder {
    /// Create a new actor builder.
    pub fn new() -> Self {
        Self {
            name: None,
            priority: Priority::Normal,
        }
    }

    /// Set the actor name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the default priority for this actor's messages.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Spawn the actor with the given scheduler.
    pub fn spawn(self, scheduler: &Arc<ActorScheduler>) -> crate::Result<ActorHandle> {
        let id = scheduler.spawn_named(self.name)?;
        Ok(ActorHandle::new(id, scheduler.clone()))
    }
}

impl Default for ActorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_actor_handle_send() {
        let scheduler = Arc::new(ActorScheduler::new(Default::default()));
        scheduler.start().unwrap();

        let handle = ActorBuilder::new()
            .name("test-actor")
            .spawn(&scheduler)
            .unwrap();

        assert_eq!(handle.state(), Some(ActorState::Creating));

        handle.start().await.unwrap();
    }
}
