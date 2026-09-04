//! Erlang-style supervisor trees for hierarchical actor supervision.
//!
//! This module implements the supervisor pattern from Erlang/OTP, providing
//! fault-tolerant actor management with automatic restart policies and
//! hierarchical supervision trees.
//!
//! # Overview
//!
//! Supervisors manage child actors with configurable restart strategies:
//!
//! - **One-for-one**: Restart only the failed child
//! - **One-for-all**: Restart all children when one fails
//! - **Rest-for-one**: Restart failed child and those started after it
//! - **Simple one-for-one**: No restart, just acknowledge failure
//!
//! # Example
//!
//! ```ignore
//! use actor_kit::supervisor::{
//!     SupervisorTree, SupervisionStrategy, ChildSpec, RestartPolicy,
//! };
//! use std::time::Duration;
//!
//! // Create a supervisor tree with one-for-one strategy
//! let mut tree = SupervisorTree::new(
//!     SupervisionStrategy::one_for_one(5, Duration::from_secs(60))
//! );
//!
//! // Start a child actor under the root supervisor
//! let child_id = tree.start_child_under(
//!     tree.root(),
//!     ChildSpec {
//!         name: "worker-1".into(),
//!         actor_config: Default::default(),
//!         restart_policy: RestartPolicy::Permanent,
//!         shutdown_timeout: Duration::from_secs(30),
//!         significant: false,
//!     },
//! )?;
//! ```

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{ActorId, Signal};
use crate::{Error, Result};

/// Maximum restart count threshold before considering it excessive.
const MAX_RESTART_THRESHOLD: u32 = 100;

/// How a supervisor should handle child failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisionStrategy {
    /// Restart only the failed child.
    ///
    /// When a child crashes, only that child is restarted.
    /// Other children are unaffected.
    OneForOne {
        /// Maximum number of restarts allowed within the time window.
        max_restarts: u32,
        /// Time window for counting restarts.
        within: Duration,
    },
    /// Restart all children when one fails.
    ///
    /// When a child crashes, all children are terminated and restarted.
    /// Use when children are tightly coupled.
    OneForAll {
        /// Maximum number of restarts allowed within the time window.
        max_restarts: u32,
        /// Time window for counting restarts.
        within: Duration,
    },
    /// Restart failed child and those started after it.
    ///
    /// When a child crashes, that child and all children started after it
    /// are terminated and restarted. Children started before are unaffected.
    /// Use for dependent chains of actors.
    RestForOne {
        /// Maximum number of restarts allowed within the time window.
        max_restarts: u32,
        /// Time window for counting restarts.
        within: Duration,
    },
    /// Don't restart, just acknowledge failure.
    ///
    /// No automatic restart is performed. Useful for dynamic, temporary
    /// children where failures are expected.
    SimpleOneForOne,
}

impl SupervisionStrategy {
    /// Create a one-for-one strategy.
    pub fn one_for_one(max_restarts: u32, within: Duration) -> Self {
        Self::OneForOne {
            max_restarts,
            within,
        }
    }

    /// Create a one-for-all strategy.
    pub fn one_for_all(max_restarts: u32, within: Duration) -> Self {
        Self::OneForAll {
            max_restarts,
            within,
        }
    }

    /// Create a rest-for-one strategy.
    pub fn rest_for_one(max_restarts: u32, within: Duration) -> Self {
        Self::RestForOne {
            max_restarts,
            within,
        }
    }

    /// Create a simple one-for-one strategy.
    pub fn simple_one_for_one() -> Self {
        Self::SimpleOneForOne
    }

    /// Get the max restarts limit for this strategy
    #[cfg(test)]
    pub(crate) fn max_restarts(&self) -> Option<u32> {
        match self {
            Self::OneForOne { max_restarts, .. }
            | Self::OneForAll { max_restarts, .. }
            | Self::RestForOne { max_restarts, .. } => Some(*max_restarts),
            Self::SimpleOneForOne => None,
        }
    }

    fn within(&self) -> Option<Duration> {
        match self {
            Self::OneForOne { within, .. }
            | Self::OneForAll { within, .. }
            | Self::RestForOne { within, .. } => Some(*within),
            Self::SimpleOneForOne => None,
        }
    }
}

impl Default for SupervisionStrategy {
    fn default() -> Self {
        Self::one_for_one(5, Duration::from_secs(60))
    }
}

/// Restart policy for a supervised child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestartPolicy {
    /// Always restart the child, regardless of exit reason.
    #[default]
    Permanent,
    /// Restart only on abnormal termination (error, killed, signaled).
    Transient,
    /// Never restart the child.
    Temporary,
}

/// Why an actor terminated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitReason {
    /// Graceful shutdown completed successfully.
    Normal,
    /// Supervisor requested shutdown.
    Shutdown,
    /// Received a termination signal.
    Signaled(Signal),
    /// Actor failed with an error.
    Error(String),
    /// Actor was forcefully killed.
    Killed,
}

impl ExitReason {
    /// Check if this is a normal termination.
    pub fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }

    /// Check if this is an abnormal termination (should trigger restart for Transient).
    pub fn is_abnormal(&self) -> bool {
        !self.is_normal()
    }
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::Shutdown => write!(f, "shutdown"),
            Self::Signaled(signal) => write!(f, "signaled({:?})", signal),
            Self::Error(msg) => write!(f, "error({})", msg),
            Self::Killed => write!(f, "killed"),
        }
    }
}

/// Specification for a supervised child actor.
#[derive(Debug, Clone)]
pub struct ChildSpec {
    /// Unique name for the child within this supervisor.
    pub name: String,
    /// Startup and behavioral configuration passed to the child actor on spawn,
    /// such as mailbox capacity, dispatch priority, and initialization parameters.
    pub actor_config: ActorConfig,
    /// When to restart the child.
    pub restart_policy: RestartPolicy,
    /// How long to wait for graceful shutdown before force-killing.
    pub shutdown_timeout: Duration,
    /// If true, abnormal termination escalates to parent supervisor.
    pub significant: bool,
}

impl ChildSpec {
    /// Create a new child spec with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            actor_config: ActorConfig::default(),
            restart_policy: RestartPolicy::default(),
            shutdown_timeout: Duration::from_secs(30),
            significant: false,
        }
    }

    /// Set the restart policy.
    pub fn restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.restart_policy = policy;
        self
    }

    /// Set the shutdown timeout.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Mark this child as significant (termination escalates).
    pub fn significant(mut self, significant: bool) -> Self {
        self.significant = significant;
        self
    }
}

/// Startup and behavioral configuration for a child actor, controlling parameters
/// such as mailbox capacity, dispatch priority, and initialization arguments.
#[derive(Debug, Clone, Default)]
pub struct ActorConfig {
    /// Reserved for additional configuration options (e.g., mailbox bounds,
    /// dispatch priority, custom metadata) in future releases.
    _future: (),
}

/// State of a supervised child.
#[derive(Debug, Clone)]
pub enum ChildState {
    /// Child is running normally.
    Running,
    /// Child is being stopped.
    Stopping,
    /// Child has stopped.
    Stopped {
        /// Why the child stopped.
        reason: ExitReason,
    },
    /// Child is being restarted.
    Restarting {
        /// Current restart attempt number.
        attempt: u32,
    },
}

impl ChildState {
    /// Check if the child is in an active state.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Running | Self::Restarting { .. })
    }
}

/// A child actor being supervised.
#[derive(Debug, Clone)]
pub struct SupervisedChild {
    /// Actor ID.
    pub id: ActorId,
    /// Child specification.
    pub spec: ChildSpec,
    /// Current state.
    pub state: ChildState,
    /// Order in which this child was started (for rest-for-one).
    pub start_order: u32,
}

/// Statistics about a supervisor's children.
#[derive(Debug, Clone, Default)]
pub struct SupervisorStats {
    /// Total number of children.
    pub total: usize,
    /// Number of running children.
    pub running: usize,
    /// Number of stopped children.
    pub stopped: usize,
    /// Number of restarting children.
    pub restarting: usize,
    /// Total restarts across all children.
    pub total_restarts: u64,
}

/// Action to take when max restarts are exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EscalationAction {
    /// Escalate to parent supervisor.
    #[default]
    Escalate,
    /// Shutdown the entire node.
    ShutdownNode,
    /// Ignore and stop restarting.
    GiveUp,
}

/// Error type for supervisor operations.
#[derive(Debug, Clone)]
pub enum SupervisorError {
    /// Child with this name already exists.
    ChildExists(String),
    /// Child with this name not found.
    ChildNotFound(String),
    /// Maximum restarts exceeded.
    MaxRestartsExceeded {
        /// Child name.
        name: String,
        /// Restart count in window.
        count: u32,
        /// Max allowed.
        max: u32,
    },
    /// Supervisor not found.
    SupervisorNotFound(ActorId),
    /// Invalid operation for current state.
    InvalidState {
        /// Child name.
        name: String,
        /// Current state.
        state: String,
        /// Attempted operation.
        operation: String,
    },
    /// Parent supervisor ID is invalid.
    InvalidParent(ActorId),
}

impl std::fmt::Display for SupervisorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChildExists(name) => write!(f, "child '{}' already exists", name),
            Self::ChildNotFound(name) => write!(f, "child '{}' not found", name),
            Self::MaxRestartsExceeded { name, count, max } => {
                write!(
                    f,
                    "child '{}' exceeded max restarts ({}/{})",
                    name, count, max
                )
            }
            Self::SupervisorNotFound(id) => write!(f, "supervisor {:?} not found", id),
            Self::InvalidState {
                name,
                state,
                operation,
            } => {
                write!(
                    f,
                    "invalid operation '{}' on child '{}' in state '{}'",
                    operation, name, state
                )
            }
            Self::InvalidParent(id) => write!(f, "invalid parent supervisor {:?}", id),
        }
    }
}

impl From<SupervisorError> for Error {
    fn from(e: SupervisorError) -> Error {
        Error::actor(e.to_string())
    }
}

/// A supervisor that manages child actors.
#[derive(Debug)]
pub struct Supervisor {
    /// Supervisor's own ID.
    id: ActorId,
    /// Supervision strategy.
    strategy: SupervisionStrategy,
    /// Child actors by name.
    children: HashMap<String, SupervisedChild>,
    /// Restart timestamps per child for rate limiting.
    restart_counts: HashMap<String, Vec<Instant>>,
    /// Parent supervisor ID (None for root).
    parent: Option<ActorId>,
    /// Next start order number.
    next_start_order: u32,
    /// Total restarts across all children.
    total_restarts: u64,
    /// Escalation action when max restarts exceeded.
    escalation_action: EscalationAction,
}

impl Supervisor {
    /// Create a new supervisor with the given strategy.
    pub fn new(strategy: SupervisionStrategy) -> Self {
        Self {
            id: ActorId::new(),
            strategy,
            children: HashMap::new(),
            restart_counts: HashMap::new(),
            parent: None,
            next_start_order: 0,
            total_restarts: 0,
            escalation_action: EscalationAction::default(),
        }
    }

    /// Create a new supervisor with a parent.
    pub fn with_parent(strategy: SupervisionStrategy, parent: ActorId) -> Self {
        Self {
            id: ActorId::new(),
            strategy,
            children: HashMap::new(),
            restart_counts: HashMap::new(),
            parent: Some(parent),
            next_start_order: 0,
            total_restarts: 0,
            escalation_action: EscalationAction::default(),
        }
    }

    /// Get the supervisor's ID.
    pub fn id(&self) -> ActorId {
        self.id
    }

    /// Get the parent supervisor ID.
    pub fn parent(&self) -> Option<ActorId> {
        self.parent
    }

    /// Set the escalation action.
    pub fn set_escalation_action(&mut self, action: EscalationAction) {
        self.escalation_action = action;
    }

    /// Start a new child actor.
    pub fn start_child(&mut self, spec: ChildSpec) -> Result<ActorId> {
        if self.children.contains_key(&spec.name) {
            return Err(SupervisorError::ChildExists(spec.name.clone()).into());
        }

        let id = ActorId::new();
        let start_order = self.next_start_order;
        self.next_start_order = self.next_start_order.saturating_add(1);

        let child = SupervisedChild {
            id,
            spec,
            state: ChildState::Running,
            start_order,
        };

        let name = child.spec.name.clone();
        self.children.insert(name, child);

        Ok(id)
    }

    /// Stop a child actor gracefully.
    pub fn stop_child(&mut self, name: &str) -> Result<()> {
        let child = self
            .children
            .get_mut(name)
            .ok_or_else(|| SupervisorError::ChildNotFound(name.to_string()))?;

        match &child.state {
            ChildState::Running | ChildState::Restarting { .. } => {
                child.state = ChildState::Stopping;
                Ok(())
            }
            ChildState::Stopping => Ok(()),
            ChildState::Stopped { .. } => Ok(()),
        }
    }

    /// Terminate a child actor with a specific reason.
    pub fn terminate_child(&mut self, name: &str, reason: ExitReason) -> Result<()> {
        let child = self
            .children
            .get_mut(name)
            .ok_or_else(|| SupervisorError::ChildNotFound(name.to_string()))?;

        child.state = ChildState::Stopped { reason };
        Ok(())
    }

    /// Get a list of all children.
    pub fn which_children(&self) -> Vec<&SupervisedChild> {
        self.children.values().collect()
    }

    /// Get statistics about the supervisor.
    pub fn count_children(&self) -> SupervisorStats {
        let mut running = 0;
        let mut stopped = 0;
        let mut restarting = 0;

        for child in self.children.values() {
            match &child.state {
                ChildState::Running => running += 1,
                ChildState::Stopped { .. } => stopped += 1,
                ChildState::Restarting { .. } => restarting += 1,
                ChildState::Stopping => stopped += 1,
            }
        }

        SupervisorStats {
            total: self.children.len(),
            running,
            stopped,
            restarting,
            total_restarts: self.total_restarts,
        }
    }

    /// Handle a child exit event.
    pub async fn handle_child_exit(&mut self, name: &str, reason: ExitReason) -> Result<()> {
        let child = self
            .children
            .get(name)
            .ok_or_else(|| SupervisorError::ChildNotFound(name.to_string()))?;

        let should_restart = match child.spec.restart_policy {
            RestartPolicy::Permanent => true,
            RestartPolicy::Transient => reason.is_abnormal(),
            RestartPolicy::Temporary => false,
        };

        let is_significant = child.spec.significant && reason.is_abnormal();

        if is_significant {
            if let Some(parent_id) = self.parent {
                tracing::warn!(
                    "Significant child '{}' exited, escalating to parent {:?}",
                    name,
                    parent_id
                );
            }
        }

        if !should_restart {
            if let Some(child) = self.children.get_mut(name) {
                child.state = ChildState::Stopped { reason };
            }
            return Ok(());
        }

        match &self.strategy {
            SupervisionStrategy::SimpleOneForOne => {
                if let Some(child) = self.children.get_mut(name) {
                    child.state = ChildState::Stopped { reason };
                }
                Ok(())
            }
            SupervisionStrategy::OneForOne {
                max_restarts,
                within,
            } => {
                self.restart_one_for_one(name, reason, *max_restarts, *within)
                    .await
            }
            SupervisionStrategy::OneForAll {
                max_restarts,
                within,
            } => {
                self.restart_one_for_all(name, reason, *max_restarts, *within)
                    .await
            }
            SupervisionStrategy::RestForOne {
                max_restarts,
                within,
            } => {
                self.restart_rest_for_one(name, reason, *max_restarts, *within)
                    .await
            }
        }
    }

    async fn restart_one_for_one(
        &mut self,
        name: &str,
        reason: ExitReason,
        max_restarts: u32,
        within: Duration,
    ) -> Result<()> {
        if !self.check_restart_allowed(name, max_restarts, within) {
            return self.handle_max_restarts_exceeded(name, max_restarts);
        }

        self.record_restart(name);
        self.total_restarts = self.total_restarts.saturating_add(1);

        if let Some(child) = self.children.get_mut(name) {
            let attempt = self
                .restart_counts
                .get(name)
                .map(|v| v.len() as u32)
                .unwrap_or(1);
            tracing::info!(
                "Restarting child '{}' (attempt {}) after {:?}",
                name,
                attempt,
                reason
            );
            child.state = ChildState::Restarting { attempt };
            child.id = ActorId::new();
        }

        Ok(())
    }

    async fn restart_one_for_all(
        &mut self,
        name: &str,
        reason: ExitReason,
        max_restarts: u32,
        within: Duration,
    ) -> Result<()> {
        if !self.check_restart_allowed(name, max_restarts, within) {
            return self.handle_max_restarts_exceeded(name, max_restarts);
        }

        self.record_restart(name);
        self.total_restarts = self.total_restarts.saturating_add(1);

        tracing::info!(
            "Restarting all children after '{}' exited with {:?}",
            name,
            reason
        );

        for child in self.children.values_mut() {
            let attempt = self
                .restart_counts
                .get(&child.spec.name)
                .map(|v| v.len() as u32)
                .unwrap_or(1);
            child.state = ChildState::Restarting { attempt };
            child.id = ActorId::new();
        }

        Ok(())
    }

    async fn restart_rest_for_one(
        &mut self,
        name: &str,
        _reason: ExitReason,
        max_restarts: u32,
        within: Duration,
    ) -> Result<()> {
        if !self.check_restart_allowed(name, max_restarts, within) {
            return self.handle_max_restarts_exceeded(name, max_restarts);
        }

        let failed_order = self
            .children
            .get(name)
            .map(|c| c.start_order)
            .ok_or_else(|| SupervisorError::ChildNotFound(name.to_string()))?;

        self.record_restart(name);
        self.total_restarts = self.total_restarts.saturating_add(1);

        tracing::info!(
            "Restarting child '{}' and siblings started after it (rest-for-one)",
            name
        );

        for child in self.children.values_mut() {
            if child.start_order >= failed_order {
                let attempt = self
                    .restart_counts
                    .get(&child.spec.name)
                    .map(|v| v.len() as u32)
                    .unwrap_or(1);
                child.state = ChildState::Restarting { attempt };
                child.id = ActorId::new();
            }
        }

        Ok(())
    }

    fn check_restart_allowed(&self, name: &str, max_restarts: u32, within: Duration) -> bool {
        if max_restarts == 0 {
            return true;
        }

        let now = Instant::now();
        let restarts = self.restart_counts.get(name);

        if let Some(timestamps) = restarts {
            let recent = timestamps
                .iter()
                .filter(|&&t| now.duration_since(t) < within)
                .count();

            let recent_u32 = u32::try_from(recent).unwrap_or(MAX_RESTART_THRESHOLD);
            recent_u32 < max_restarts
        } else {
            true
        }
    }

    fn record_restart(&mut self, name: &str) {
        let now = Instant::now();
        let timestamps = self.restart_counts.entry(name.to_string()).or_default();
        timestamps.push(now);

        if let Some(within) = self.strategy.within() {
            timestamps.retain(|&t| now.duration_since(t) < within);
        }
    }

    fn handle_max_restarts_exceeded(&mut self, name: &str, max_restarts: u32) -> Result<()> {
        let count = self
            .restart_counts
            .get(name)
            .map(|v| u32::try_from(v.len()).unwrap_or(MAX_RESTART_THRESHOLD))
            .unwrap_or(0);

        tracing::error!(
            "Child '{}' exceeded max restarts ({}/{})",
            name,
            count,
            max_restarts
        );

        if let Some(child) = self.children.get_mut(name) {
            child.state = ChildState::Stopped {
                reason: ExitReason::Error(format!(
                    "max restarts exceeded ({}/{})",
                    count, max_restarts
                )),
            };
        }

        match self.escalation_action {
            EscalationAction::Escalate => {
                if let Some(parent_id) = self.parent {
                    tracing::warn!("Escalating max restarts failure to parent {:?}", parent_id);
                }
                Err(SupervisorError::MaxRestartsExceeded {
                    name: name.to_string(),
                    count,
                    max: max_restarts,
                }
                .into())
            }
            EscalationAction::ShutdownNode => {
                tracing::error!("Initiating node shutdown due to max restarts exceeded");
                Err(SupervisorError::MaxRestartsExceeded {
                    name: name.to_string(),
                    count,
                    max: max_restarts,
                }
                .into())
            }
            EscalationAction::GiveUp => {
                tracing::warn!("Giving up on child '{}' after max restarts", name);
                Ok(())
            }
        }
    }

    /// Remove a child from supervision.
    pub fn remove_child(&mut self, name: &str) -> Result<SupervisedChild> {
        self.children
            .remove(name)
            .ok_or_else(|| SupervisorError::ChildNotFound(name.to_string()).into())
    }

    /// Get a child by name.
    pub fn get_child(&self, name: &str) -> Option<&SupervisedChild> {
        self.children.get(name)
    }

    /// Get a child by name mutably.
    pub fn get_child_mut(&mut self, name: &str) -> Option<&mut SupervisedChild> {
        self.children.get_mut(name)
    }

    /// Check if a child exists.
    pub fn has_child(&self, name: &str) -> bool {
        self.children.contains_key(name)
    }

    /// Get the number of children.
    pub fn child_count(&self) -> usize {
        self.children.len()
    }

    /// Mark a restarting child as running.
    pub fn mark_child_running(&mut self, name: &str) -> Result<()> {
        let child = self
            .children
            .get_mut(name)
            .ok_or_else(|| SupervisorError::ChildNotFound(name.to_string()))?;

        match &child.state {
            ChildState::Restarting { .. } | ChildState::Running => {
                child.state = ChildState::Running;
                Ok(())
            }
            ChildState::Stopping | ChildState::Stopped { .. } => {
                Err(SupervisorError::InvalidState {
                    name: name.to_string(),
                    state: format!("{:?}", child.state),
                    operation: "mark_running".to_string(),
                }
                .into())
            }
        }
    }
}

/// Hierarchical supervisor tree structure.
pub struct SupervisorTree {
    /// Root supervisor ID.
    root: ActorId,
    /// All supervisors by ID.
    supervisors: HashMap<ActorId, Supervisor>,
    /// Parent-child relationships for supervisors.
    supervisor_children: HashMap<ActorId, Vec<ActorId>>,
}

impl SupervisorTree {
    /// Create a new supervisor tree with a root supervisor.
    pub fn new(root_strategy: SupervisionStrategy) -> Self {
        let root_supervisor = Supervisor::new(root_strategy);
        let root_id = root_supervisor.id();

        let mut supervisors = HashMap::new();
        supervisors.insert(root_id, root_supervisor);

        let mut supervisor_children = HashMap::new();
        supervisor_children.insert(root_id, Vec::new());

        Self {
            root: root_id,
            supervisors,
            supervisor_children,
        }
    }

    /// Get the root supervisor ID.
    pub fn root(&self) -> ActorId {
        self.root
    }

    /// Add a new supervisor under a parent.
    pub fn add_supervisor(
        &mut self,
        parent: ActorId,
        strategy: SupervisionStrategy,
    ) -> Result<ActorId> {
        if !self.supervisors.contains_key(&parent) {
            return Err(SupervisorError::SupervisorNotFound(parent).into());
        }

        let supervisor = Supervisor::with_parent(strategy, parent);
        let id = supervisor.id();

        self.supervisors.insert(id, supervisor);
        self.supervisor_children.insert(id, Vec::new());

        if let Some(children) = self.supervisor_children.get_mut(&parent) {
            children.push(id);
        }

        Ok(id)
    }

    /// Start a child actor under a specific supervisor.
    pub fn start_child_under(&mut self, supervisor: ActorId, spec: ChildSpec) -> Result<ActorId> {
        let sup = self
            .supervisors
            .get_mut(&supervisor)
            .ok_or(SupervisorError::SupervisorNotFound(supervisor))?;

        sup.start_child(spec)
    }

    /// Get a supervisor by ID.
    pub fn get_supervisor(&self, id: &ActorId) -> Option<&Supervisor> {
        self.supervisors.get(id)
    }

    /// Get a supervisor by ID mutably.
    pub fn get_supervisor_mut(&mut self, id: &ActorId) -> Option<&mut Supervisor> {
        self.supervisors.get_mut(id)
    }

    /// Terminate a subtree starting from a given supervisor.
    pub async fn terminate_tree(&mut self, root_id: &ActorId) -> Result<()> {
        if !self.supervisors.contains_key(root_id) {
            return Err(SupervisorError::SupervisorNotFound(*root_id).into());
        }

        let to_terminate = self.collect_subtree(root_id);

        for id in to_terminate.iter().rev() {
            if let Some(supervisor) = self.supervisors.get(id) {
                let child_names: Vec<String> = supervisor
                    .which_children()
                    .iter()
                    .map(|c| c.spec.name.clone())
                    .collect();

                for name in child_names {
                    if let Some(sup) = self.supervisors.get_mut(id) {
                        let _ = sup.terminate_child(&name, ExitReason::Shutdown);
                    }
                }
            }
        }

        for id in &to_terminate {
            if *id != self.root {
                self.supervisors.remove(id);
                self.supervisor_children.remove(id);

                for children in self.supervisor_children.values_mut() {
                    children.retain(|child_id| child_id != id);
                }
            }
        }

        Ok(())
    }

    fn collect_subtree(&self, root_id: &ActorId) -> Vec<ActorId> {
        let mut result = Vec::new();
        self.collect_subtree_recursive(root_id, &mut result);
        result
    }

    fn collect_subtree_recursive(&self, id: &ActorId, result: &mut Vec<ActorId>) {
        result.push(*id);

        if let Some(children) = self.supervisor_children.get(id) {
            for child in children {
                self.collect_subtree_recursive(child, result);
            }
        }
    }

    /// Handle a child exit in the appropriate supervisor.
    pub async fn handle_child_exit(
        &mut self,
        supervisor_id: ActorId,
        child_name: &str,
        reason: ExitReason,
    ) -> Result<()> {
        let supervisor = self
            .supervisors
            .get_mut(&supervisor_id)
            .ok_or(SupervisorError::SupervisorNotFound(supervisor_id))?;

        supervisor.handle_child_exit(child_name, reason).await
    }

    /// Get statistics for the entire tree.
    pub fn tree_stats(&self) -> SupervisorTreeStats {
        let mut total_children = 0;
        let mut running_children = 0;
        let mut stopped_children = 0;
        let mut restarting_children = 0;
        let mut total_restarts = 0;

        for supervisor in self.supervisors.values() {
            let sup_stats = supervisor.count_children();
            total_children += sup_stats.total;
            running_children += sup_stats.running;
            stopped_children += sup_stats.stopped;
            restarting_children += sup_stats.restarting;
            total_restarts += sup_stats.total_restarts;
        }

        SupervisorTreeStats {
            supervisor_count: self.supervisors.len(),
            total_children,
            running_children,
            stopped_children,
            restarting_children,
            total_restarts,
        }
    }

    /// Get the depth of the tree.
    pub fn tree_depth(&self) -> usize {
        self.calculate_depth(&self.root, 0)
    }

    fn calculate_depth(&self, id: &ActorId, current_depth: usize) -> usize {
        let children = self.supervisor_children.get(id);
        let child_depths: Vec<usize> = children
            .map(|c| {
                c.iter()
                    .map(|child_id| self.calculate_depth(child_id, current_depth + 1))
                    .collect()
            })
            .unwrap_or_default();

        child_depths.into_iter().max().unwrap_or(current_depth)
    }
}

/// Statistics for the entire supervisor tree.
#[derive(Debug, Clone, Default)]
pub struct SupervisorTreeStats {
    /// Number of supervisors.
    pub supervisor_count: usize,
    /// Total children across all supervisors.
    pub total_children: usize,
    /// Running children.
    pub running_children: usize,
    /// Stopped children.
    pub stopped_children: usize,
    /// Restarting children.
    pub restarting_children: usize,
    /// Total restarts.
    pub total_restarts: u64,
}

/// Thread-safe wrapper for supervisor operations.
#[derive(Clone)]
pub struct SupervisorHandle {
    inner: Arc<RwLock<Supervisor>>,
}

impl SupervisorHandle {
    /// Create a new handle wrapping a supervisor.
    pub fn new(supervisor: Supervisor) -> Self {
        Self {
            inner: Arc::new(RwLock::new(supervisor)),
        }
    }

    /// Get the supervisor ID.
    pub fn id(&self) -> ActorId {
        self.inner.read().id()
    }

    /// Start a child actor.
    pub fn start_child(&self, spec: ChildSpec) -> Result<ActorId> {
        self.inner.write().start_child(spec)
    }

    /// Stop a child actor.
    pub fn stop_child(&self, name: &str) -> Result<()> {
        self.inner.write().stop_child(name)
    }

    /// Get child statistics.
    pub fn count_children(&self) -> SupervisorStats {
        self.inner.read().count_children()
    }

    /// Get all children.
    pub fn which_children(&self) -> Vec<SupervisedChild> {
        self.inner
            .read()
            .which_children()
            .into_iter()
            .cloned()
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Graceful Degradation
// ---------------------------------------------------------------------------

/// Resource pressure level reported by a [`ResourceMonitor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum PressureLevel {
    /// System is operating within normal resource bounds.
    #[default]
    Normal,
    /// Resource usage is elevated; non-essential work should be throttled.
    Elevated,
    /// Resources are critically constrained; spawn new work is rejected.
    Critical,
}

impl std::fmt::Display for PressureLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::Elevated => write!(f, "elevated"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

/// Trait for monitoring system resource pressure.
///
/// Implementations report the current [`PressureLevel`] so that supervisors
/// can make graceful degradation decisions.
pub trait ResourceMonitor: Send + Sync {
    /// Return the current resource pressure level.
    fn current_pressure(&self) -> PressureLevel;
}

/// A static resource monitor that always reports the same pressure level.
///
/// Useful for testing and for nodes where resource monitoring is delegated
/// to an external system.
pub struct StaticResourceMonitor {
    level: PressureLevel,
}

impl StaticResourceMonitor {
    /// Create a monitor that always reports `level`.
    pub fn new(level: PressureLevel) -> Self {
        Self { level }
    }
}

impl ResourceMonitor for StaticResourceMonitor {
    fn current_pressure(&self) -> PressureLevel {
        self.level
    }
}

/// A resource monitor that reports `Elevated` when a closure returns `true`
/// and `Critical` when a second closure returns `true`.
pub struct FnResourceMonitor {
    elevated_fn: Box<dyn Fn() -> bool + Send + Sync>,
    critical_fn: Box<dyn Fn() -> bool + Send + Sync>,
}

impl FnResourceMonitor {
    /// Create a function-based resource monitor.
    pub fn new(
        elevated_fn: Box<dyn Fn() -> bool + Send + Sync>,
        critical_fn: Box<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        Self {
            elevated_fn,
            critical_fn,
        }
    }
}

impl ResourceMonitor for FnResourceMonitor {
    fn current_pressure(&self) -> PressureLevel {
        if (self.critical_fn)() {
            PressureLevel::Critical
        } else if (self.elevated_fn)() {
            PressureLevel::Elevated
        } else {
            PressureLevel::Normal
        }
    }
}

/// Result of a graceful degradation decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradationDecision {
    /// Allow the operation to proceed normally.
    Allow,
    /// Throttle: reduce batch sizes or concurrency.
    Throttle,
    /// Reject: the operation cannot proceed under current pressure.
    Reject,
}

impl DegradationDecision {
    /// Returns `true` if the operation is allowed (possibly throttled).
    pub fn is_allowed(self) -> bool {
        matches!(self, Self::Allow | Self::Throttle)
    }
}

/// Graceful degradation controller that wraps a [`ResourceMonitor`] and
/// makes admission-control decisions.
pub struct DegradationController<M: ResourceMonitor> {
    monitor: M,
    elevated_batch_factor: f64,
}

impl<M: ResourceMonitor> DegradationController<M> {
    /// Create a new controller with the given resource monitor.
    ///
    /// `elevated_batch_factor` is multiplied by the normal batch size when
    /// pressure is elevated (e.g. `0.5` halves the batch).
    pub fn new(monitor: M, elevated_batch_factor: f64) -> Self {
        Self {
            monitor,
            elevated_batch_factor: elevated_batch_factor.clamp(0.0, 1.0),
        }
    }

    /// Return the current pressure level.
    pub fn pressure(&self) -> PressureLevel {
        self.monitor.current_pressure()
    }

    /// Return a reference to the underlying monitor.
    pub fn monitor(&self) -> &M {
        &self.monitor
    }

    /// Return the elevated batch factor.
    pub fn elevated_batch_factor(&self) -> f64 {
        self.elevated_batch_factor
    }

    /// Decide whether a new child actor spawn should be admitted.
    pub fn admit_spawn(&self) -> DegradationDecision {
        match self.monitor.current_pressure() {
            PressureLevel::Normal => DegradationDecision::Allow,
            PressureLevel::Elevated => DegradationDecision::Throttle,
            PressureLevel::Critical => DegradationDecision::Reject,
        }
    }

    /// Compute the effective batch size given normal and current pressure.
    pub fn effective_batch_size(&self, normal: usize) -> usize {
        match self.monitor.current_pressure() {
            PressureLevel::Normal => normal,
            PressureLevel::Elevated => {
                let scaled = (normal as f64 * self.elevated_batch_factor) as usize;
                scaled.max(1)
            }
            PressureLevel::Critical => 1,
        }
    }

    /// Decide whether a non-essential actor should be paused.
    pub fn should_pause_non_essential(&self) -> bool {
        match self.monitor.current_pressure() {
            PressureLevel::Normal => false,
            PressureLevel::Elevated => true,
            PressureLevel::Critical => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supervision_strategy_constructors() {
        let s1 = SupervisionStrategy::one_for_one(5, Duration::from_secs(60));
        assert_eq!(s1.max_restarts(), Some(5));
        assert_eq!(s1.within(), Some(Duration::from_secs(60)));

        let s2 = SupervisionStrategy::one_for_all(3, Duration::from_secs(300));
        assert_eq!(s2.max_restarts(), Some(3));

        let s3 = SupervisionStrategy::rest_for_one(10, Duration::from_secs(120));
        assert_eq!(s3.max_restarts(), Some(10));

        let s4 = SupervisionStrategy::simple_one_for_one();
        assert_eq!(s4.max_restarts(), None);
    }

    #[test]
    fn test_exit_reason_checks() {
        assert!(ExitReason::Normal.is_normal());
        assert!(!ExitReason::Normal.is_abnormal());

        assert!(!ExitReason::Killed.is_normal());
        assert!(ExitReason::Killed.is_abnormal());

        assert!(ExitReason::Error("test".to_string()).is_abnormal());
        assert!(ExitReason::Shutdown.is_abnormal());
        assert!(ExitReason::Signaled(Signal::Pause).is_abnormal());
    }

    #[test]
    fn test_child_spec_builder() {
        let spec = ChildSpec::new("test-child")
            .restart_policy(RestartPolicy::Transient)
            .shutdown_timeout(Duration::from_secs(60))
            .significant(true);

        assert_eq!(spec.name, "test-child");
        assert_eq!(spec.restart_policy, RestartPolicy::Transient);
        assert_eq!(spec.shutdown_timeout, Duration::from_secs(60));
        assert!(spec.significant);
    }

    #[test]
    fn test_supervisor_start_child() {
        let mut supervisor = Supervisor::new(SupervisionStrategy::default());

        let spec = ChildSpec::new("child-1");
        let id = supervisor.start_child(spec).unwrap();

        assert!(supervisor.has_child("child-1"));
        assert_eq!(supervisor.get_child("child-1").map(|c| c.id), Some(id));
        assert_eq!(supervisor.child_count(), 1);
    }

    #[test]
    fn test_supervisor_duplicate_child() {
        let mut supervisor = Supervisor::new(SupervisionStrategy::default());

        let spec1 = ChildSpec::new("child-1");
        let _ = supervisor.start_child(spec1).unwrap();

        let spec2 = ChildSpec::new("child-1");
        let result = supervisor.start_child(spec2);

        assert!(result.is_err());
    }

    #[test]
    fn test_supervisor_stats() {
        let mut supervisor = Supervisor::new(SupervisionStrategy::default());

        supervisor.start_child(ChildSpec::new("child-1")).unwrap();
        supervisor.start_child(ChildSpec::new("child-2")).unwrap();
        supervisor.start_child(ChildSpec::new("child-3")).unwrap();

        let stats = supervisor.count_children();
        assert_eq!(stats.total, 3);
        assert_eq!(stats.running, 3);
        assert_eq!(stats.stopped, 0);
    }

    #[tokio::test]
    async fn test_supervisor_stop_child() {
        let mut supervisor = Supervisor::new(SupervisionStrategy::default());

        supervisor.start_child(ChildSpec::new("child-1")).unwrap();
        supervisor.stop_child("child-1").unwrap();

        let child = supervisor.get_child("child-1").unwrap();
        matches!(
            child.state,
            ChildState::Stopping | ChildState::Stopped { .. }
        );
    }

    #[tokio::test]
    async fn test_supervisor_terminate_child() {
        let mut supervisor = Supervisor::new(SupervisionStrategy::default());

        supervisor.start_child(ChildSpec::new("child-1")).unwrap();
        supervisor
            .terminate_child("child-1", ExitReason::Error("test error".to_string()))
            .unwrap();

        let child = supervisor.get_child("child-1").unwrap();
        match &child.state {
            ChildState::Stopped { reason } => {
                assert!(matches!(reason, ExitReason::Error(_)));
            }
            _ => panic!("Expected stopped state"),
        }
    }

    #[tokio::test]
    async fn test_supervisor_handle_exit_permanent_restart() {
        let mut supervisor =
            Supervisor::new(SupervisionStrategy::one_for_one(5, Duration::from_secs(60)));

        supervisor
            .start_child(ChildSpec::new("child-1").restart_policy(RestartPolicy::Permanent))
            .unwrap();

        supervisor
            .handle_child_exit("child-1", ExitReason::Error("crashed".to_string()))
            .await
            .unwrap();

        let child = supervisor.get_child("child-1").unwrap();
        match &child.state {
            ChildState::Restarting { attempt } => {
                assert_eq!(*attempt, 1);
            }
            _ => panic!("Expected restarting state"),
        }

        let stats = supervisor.count_children();
        assert_eq!(stats.total_restarts, 1);
    }

    #[tokio::test]
    async fn test_supervisor_handle_exit_temporary_no_restart() {
        let mut supervisor =
            Supervisor::new(SupervisionStrategy::one_for_one(5, Duration::from_secs(60)));

        supervisor
            .start_child(ChildSpec::new("child-1").restart_policy(RestartPolicy::Temporary))
            .unwrap();

        supervisor
            .handle_child_exit("child-1", ExitReason::Error("crashed".to_string()))
            .await
            .unwrap();

        let child = supervisor.get_child("child-1").unwrap();
        match &child.state {
            ChildState::Stopped { .. } => {}
            _ => panic!("Expected stopped state"),
        }
    }

    #[tokio::test]
    async fn test_supervisor_handle_exit_transient_normal_no_restart() {
        let mut supervisor =
            Supervisor::new(SupervisionStrategy::one_for_one(5, Duration::from_secs(60)));

        supervisor
            .start_child(ChildSpec::new("child-1").restart_policy(RestartPolicy::Transient))
            .unwrap();

        supervisor
            .handle_child_exit("child-1", ExitReason::Normal)
            .await
            .unwrap();

        let child = supervisor.get_child("child-1").unwrap();
        match &child.state {
            ChildState::Stopped { reason } => {
                assert!(reason.is_normal());
            }
            _ => panic!("Expected stopped state"),
        }
    }

    #[tokio::test]
    async fn test_supervisor_handle_exit_transient_abnormal_restart() {
        let mut supervisor =
            Supervisor::new(SupervisionStrategy::one_for_one(5, Duration::from_secs(60)));

        supervisor
            .start_child(ChildSpec::new("child-1").restart_policy(RestartPolicy::Transient))
            .unwrap();

        supervisor
            .handle_child_exit("child-1", ExitReason::Killed)
            .await
            .unwrap();

        let child = supervisor.get_child("child-1").unwrap();
        matches!(child.state, ChildState::Restarting { .. });
    }

    #[tokio::test]
    async fn test_supervisor_max_restarts_exceeded() {
        let mut supervisor =
            Supervisor::new(SupervisionStrategy::one_for_one(2, Duration::from_secs(60)));

        supervisor
            .start_child(ChildSpec::new("child-1").restart_policy(RestartPolicy::Permanent))
            .unwrap();

        supervisor
            .handle_child_exit("child-1", ExitReason::Error("crash".to_string()))
            .await
            .unwrap();
        supervisor
            .handle_child_exit("child-1", ExitReason::Error("crash".to_string()))
            .await
            .unwrap();

        let result = supervisor
            .handle_child_exit("child-1", ExitReason::Error("crash".to_string()))
            .await;
        assert!(result.is_err());

        let child = supervisor.get_child("child-1").unwrap();
        matches!(child.state, ChildState::Stopped { .. });
    }

    #[tokio::test]
    async fn test_supervisor_give_up_escalation() {
        let mut supervisor =
            Supervisor::new(SupervisionStrategy::one_for_one(1, Duration::from_secs(60)));
        supervisor.set_escalation_action(EscalationAction::GiveUp);

        supervisor
            .start_child(ChildSpec::new("child-1").restart_policy(RestartPolicy::Permanent))
            .unwrap();

        supervisor
            .handle_child_exit("child-1", ExitReason::Error("crash".to_string()))
            .await
            .unwrap();

        let result = supervisor
            .handle_child_exit("child-1", ExitReason::Error("crash".to_string()))
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_supervisor_tree_creation() {
        let tree = SupervisorTree::new(SupervisionStrategy::default());

        assert!(tree.get_supervisor(&tree.root()).is_some());
        assert_eq!(tree.tree_depth(), 0);
    }

    #[test]
    fn test_supervisor_tree_add_supervisor() {
        let mut tree = SupervisorTree::new(SupervisionStrategy::default());
        let root = tree.root();

        let child_sup = tree
            .add_supervisor(
                root,
                SupervisionStrategy::one_for_all(3, Duration::from_secs(60)),
            )
            .unwrap();

        assert!(tree.get_supervisor(&child_sup).is_some());
        assert_eq!(tree.tree_depth(), 1);
        assert_eq!(tree.tree_stats().supervisor_count, 2);
    }

    #[test]
    fn test_supervisor_tree_start_child() {
        let mut tree = SupervisorTree::new(SupervisionStrategy::default());
        let root = tree.root();

        let child_id = tree
            .start_child_under(root, ChildSpec::new("child-1"))
            .unwrap();

        let supervisor = tree.get_supervisor(&root).unwrap();
        let child = supervisor.get_child("child-1").unwrap();
        assert_eq!(child.id, child_id);
    }

    #[test]
    fn test_supervisor_tree_invalid_parent() {
        let mut tree = SupervisorTree::new(SupervisionStrategy::default());

        let fake_id = ActorId::new();
        let result = tree.add_supervisor(fake_id, SupervisionStrategy::default());

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_supervisor_tree_terminate() {
        let mut tree = SupervisorTree::new(SupervisionStrategy::default());
        let root = tree.root();

        let sup1 = tree
            .add_supervisor(root, SupervisionStrategy::default())
            .unwrap();
        let sup2 = tree
            .add_supervisor(sup1, SupervisionStrategy::default())
            .unwrap();

        tree.start_child_under(root, ChildSpec::new("child-1"))
            .unwrap();
        tree.start_child_under(sup1, ChildSpec::new("child-2"))
            .unwrap();
        tree.start_child_under(sup2, ChildSpec::new("child-3"))
            .unwrap();

        assert_eq!(tree.tree_stats().supervisor_count, 3);
        assert_eq!(tree.tree_stats().total_children, 3);

        tree.terminate_tree(&sup1).await.unwrap();

        assert_eq!(tree.tree_stats().supervisor_count, 1);
        assert_eq!(tree.tree_stats().total_children, 1);
    }

    #[test]
    fn test_supervisor_tree_stats() {
        let mut tree = SupervisorTree::new(SupervisionStrategy::default());
        let root = tree.root();

        tree.start_child_under(root, ChildSpec::new("child-1"))
            .unwrap();
        tree.start_child_under(root, ChildSpec::new("child-2"))
            .unwrap();

        let sup1 = tree
            .add_supervisor(root, SupervisionStrategy::default())
            .unwrap();
        tree.start_child_under(sup1, ChildSpec::new("child-3"))
            .unwrap();

        let stats = tree.tree_stats();
        assert_eq!(stats.supervisor_count, 2);
        assert_eq!(stats.total_children, 3);
        assert_eq!(stats.running_children, 3);
    }

    #[test]
    fn test_supervisor_handle() {
        let supervisor = Supervisor::new(SupervisionStrategy::default());
        let handle = SupervisorHandle::new(supervisor);

        let id = handle.id();
        assert_ne!(id, ActorId::new());

        handle.start_child(ChildSpec::new("child-1")).unwrap();

        let stats = handle.count_children();
        assert_eq!(stats.total, 1);

        let children = handle.which_children();
        assert_eq!(children.len(), 1);
    }

    #[test]
    fn test_child_state_is_active() {
        assert!(ChildState::Running.is_active());
        assert!(ChildState::Restarting { attempt: 1 }.is_active());
        assert!(!ChildState::Stopping.is_active());
        assert!(!ChildState::Stopped {
            reason: ExitReason::Normal
        }
        .is_active());
    }

    #[test]
    fn test_supervisor_remove_child() {
        let mut supervisor = Supervisor::new(SupervisionStrategy::default());

        supervisor.start_child(ChildSpec::new("child-1")).unwrap();
        assert_eq!(supervisor.child_count(), 1);

        let removed = supervisor.remove_child("child-1").unwrap();
        assert_eq!(removed.spec.name, "child-1");
        assert_eq!(supervisor.child_count(), 0);

        let result = supervisor.remove_child("nonexistent");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_one_for_all_strategy() {
        let mut supervisor =
            Supervisor::new(SupervisionStrategy::one_for_all(5, Duration::from_secs(60)));

        supervisor.start_child(ChildSpec::new("child-1")).unwrap();
        supervisor.start_child(ChildSpec::new("child-2")).unwrap();
        supervisor.start_child(ChildSpec::new("child-3")).unwrap();

        supervisor
            .handle_child_exit("child-2", ExitReason::Error("crashed".to_string()))
            .await
            .unwrap();

        for name in ["child-1", "child-2", "child-3"] {
            let child = supervisor.get_child(name).unwrap();
            matches!(child.state, ChildState::Restarting { .. });
        }
    }

    #[tokio::test]
    async fn test_rest_for_one_strategy() {
        let mut supervisor = Supervisor::new(SupervisionStrategy::rest_for_one(
            5,
            Duration::from_secs(60),
        ));

        supervisor.start_child(ChildSpec::new("child-1")).unwrap();
        supervisor.start_child(ChildSpec::new("child-2")).unwrap();
        supervisor.start_child(ChildSpec::new("child-3")).unwrap();

        supervisor
            .handle_child_exit("child-2", ExitReason::Error("crashed".to_string()))
            .await
            .unwrap();

        let child1 = supervisor.get_child("child-1").unwrap();
        matches!(child1.state, ChildState::Running);

        for name in ["child-2", "child-3"] {
            let child = supervisor.get_child(name).unwrap();
            matches!(child.state, ChildState::Restarting { .. });
        }
    }

    #[tokio::test]
    async fn test_simple_one_for_one_strategy() {
        let mut supervisor = Supervisor::new(SupervisionStrategy::simple_one_for_one());

        supervisor
            .start_child(ChildSpec::new("child-1").restart_policy(RestartPolicy::Permanent))
            .unwrap();

        supervisor
            .handle_child_exit("child-1", ExitReason::Error("crashed".to_string()))
            .await
            .unwrap();

        let child = supervisor.get_child("child-1").unwrap();
        match &child.state {
            ChildState::Stopped { .. } => {}
            _ => panic!("Expected stopped state for simple_one_for_one"),
        }
    }

    // -----------------------------------------------------------------------
    // Graceful Degradation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_pressure_level_ordering() {
        assert!(PressureLevel::Normal < PressureLevel::Elevated);
        assert!(PressureLevel::Elevated < PressureLevel::Critical);
    }

    #[test]
    fn test_pressure_level_display() {
        assert_eq!(format!("{}", PressureLevel::Normal), "normal");
        assert_eq!(format!("{}", PressureLevel::Elevated), "elevated");
        assert_eq!(format!("{}", PressureLevel::Critical), "critical");
    }

    #[test]
    fn test_static_resource_monitor() {
        let normal = StaticResourceMonitor::new(PressureLevel::Normal);
        assert_eq!(normal.current_pressure(), PressureLevel::Normal);

        let critical = StaticResourceMonitor::new(PressureLevel::Critical);
        assert_eq!(critical.current_pressure(), PressureLevel::Critical);
    }

    #[test]
    fn test_fn_resource_monitor() {
        let monitor = FnResourceMonitor::new(Box::new(|| true), Box::new(|| false));
        assert_eq!(monitor.current_pressure(), PressureLevel::Elevated);

        let monitor2 = FnResourceMonitor::new(Box::new(|| true), Box::new(|| true));
        assert_eq!(monitor2.current_pressure(), PressureLevel::Critical);

        let monitor3 = FnResourceMonitor::new(Box::new(|| false), Box::new(|| false));
        assert_eq!(monitor3.current_pressure(), PressureLevel::Normal);
    }

    #[test]
    fn test_degradation_controller_admit_spawn() {
        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Normal), 0.5);
        assert_eq!(controller.admit_spawn(), DegradationDecision::Allow);

        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Elevated), 0.5);
        assert_eq!(controller.admit_spawn(), DegradationDecision::Throttle);

        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Critical), 0.5);
        assert_eq!(controller.admit_spawn(), DegradationDecision::Reject);
    }

    #[test]
    fn test_degradation_controller_batch_size() {
        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Normal), 0.5);
        assert_eq!(controller.effective_batch_size(100), 100);

        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Elevated), 0.5);
        assert_eq!(controller.effective_batch_size(100), 50);
        assert_eq!(controller.effective_batch_size(1), 1);

        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Critical), 0.5);
        assert_eq!(controller.effective_batch_size(100), 1);
    }

    #[test]
    fn test_degradation_controller_pause_non_essential() {
        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Normal), 0.5);
        assert!(!controller.should_pause_non_essential());

        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Elevated), 0.5);
        assert!(controller.should_pause_non_essential());

        let controller =
            DegradationController::new(StaticResourceMonitor::new(PressureLevel::Critical), 0.5);
        assert!(controller.should_pause_non_essential());
    }

    #[test]
    fn test_degradation_decision_is_allowed() {
        assert!(DegradationDecision::Allow.is_allowed());
        assert!(DegradationDecision::Throttle.is_allowed());
        assert!(!DegradationDecision::Reject.is_allowed());
    }
}
