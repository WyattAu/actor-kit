//! Actor executor abstraction.
//!
//! Provides the executor interface the scheduler uses to run actor messages
//! through pluggable backends (native handlers, WASM hosts, test doubles).
//! The upstream `WasmActorExecutor` was excised with the WASM engine; the
//! trait below is its stable seam.

use crate::error::Result;
use crate::{ActorId, Message};

/// Result of executing an actor task.
#[derive(Debug)]
pub enum ExecutionResult {
    /// Execution completed successfully
    Success {
        /// Fuel consumed during execution
        fuel_consumed: u64,
        /// Optional response payload
        response: Option<Vec<u8>>,
    },
    /// Execution ran out of fuel
    FuelExhausted {
        /// Fuel that was attempted to be consumed
        requested: u64,
    },
    /// Execution failed with an error
    Failed {
        /// Error message
        error: String,
    },
    /// Actor not found or not initialized
    NotReady,
}

/// Trait for executing actor messages.
///
/// Implementations handle the actual invocation of actor code,
/// whether through WASM, native code, or other mechanisms.
pub trait ActorExecutor: Send + Sync {
    /// Execute a message for an actor.
    ///
    /// # Arguments
    /// * `actor_id` - The target actor's ID
    /// * `message` - The message to process
    ///
    /// # Returns
    /// The result of execution
    fn execute(&self, actor_id: &ActorId, message: &Message) -> ExecutionResult;

    /// Check if an actor is ready to execute.
    fn is_ready(&self, actor_id: &ActorId) -> bool;

    /// Get the fuel consumption for an actor.
    fn get_fuel(&self, actor_id: &ActorId) -> Option<u64>;

    /// Reset an actor's execution state.
    fn reset(&self, actor_id: &ActorId) -> Result<()>;
}

/// Null executor for testing.
///
/// Does nothing but track calls.
pub struct NullExecutor {
    call_count: std::sync::atomic::AtomicU64,
}

impl NullExecutor {
    /// Create a new null executor.
    pub fn new() -> Self {
        Self {
            call_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get the number of calls made.
    pub fn call_count(&self) -> u64 {
        self.call_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for NullExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorExecutor for NullExecutor {
    fn execute(&self, _actor_id: &ActorId, _message: &Message) -> ExecutionResult {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ExecutionResult::Success {
            fuel_consumed: 0,
            response: None,
        }
    }

    fn is_ready(&self, _actor_id: &ActorId) -> bool {
        true
    }

    fn get_fuel(&self, _actor_id: &ActorId) -> Option<u64> {
        Some(1_000_000)
    }

    fn reset(&self, _actor_id: &ActorId) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MessagePayload, Priority};

    #[test]
    fn test_null_executor() {
        let executor = NullExecutor::new();
        let actor_id = ActorId::new();
        let message = Message {
            sender: None,
            payload: MessagePayload::Start,
            priority: Priority::Normal,
        };

        let result = executor.execute(&actor_id, &message);
        assert!(matches!(result, ExecutionResult::Success { .. }));
        assert_eq!(executor.call_count(), 1);
    }

    #[test]
    fn test_null_executor_is_ready() {
        let executor = NullExecutor::new();
        let actor_id = ActorId::new();
        assert!(executor.is_ready(&actor_id));
    }

    #[test]
    fn test_execution_result_debug() {
        let result = ExecutionResult::Success {
            fuel_consumed: 100,
            response: Some(vec![1, 2, 3]),
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("Success"));
    }
}
