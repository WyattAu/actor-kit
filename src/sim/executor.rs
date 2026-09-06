//! The sim's executor: observation oracle + crash injector.
//!
//! Installed as the [`ActorExecutor`] for every simulated actor, this is the
//! single observation point for "what did the runtime actually process":
//! every `execute` call the real processing pipeline makes is recorded with
//! the target actor and a message identity tag. It is also the fault
//! injector for `crash_prob`: when the fault schedule arms it, the next
//! `execute` panics — inside the *real* `catch_unwind` of the real
//! processing pipeline, so crash containment (state → `Failed`, mailbox
//! drain, worker survival) is production code under test, not simulation
//! code.
//!
//! Only application messages (`Custom` payloads with an 8-byte
//! little-endian identity) carry an identity; system messages
//! (`Start`/`Stop`/signals) are recorded as [`ExecTag::System`] and never
//! crash.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::executor::{ActorExecutor, ExecutionResult};
use crate::{ActorId, Message, MessagePayload};

/// Sentinel text carried by every sim-injected crash payload, so test panic
/// hooks can filter injected crashes from real ones.
pub const SIM_CRASH_MESSAGE: &str = "actor-kit-sim: injected actor crash";

/// Identity tag recorded for one processed message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecTag {
    /// Application message with sim identity `u64` (payload `Custom(8le)`).
    Custom(u64),
    /// System message (`Start`/`Stop`/`Signal`/other shapes).
    System,
}

/// One observed processing event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecRecord {
    /// Actor the message was delivered to.
    pub actor: ActorId,
    /// Identity tag of the processed message.
    pub tag: ExecTag,
}

/// Observation + crash-injection executor (see module docs).
pub struct SimExecutor {
    crash_next: AtomicBool,
    records: Mutex<Vec<ExecRecord>>,
}

impl SimExecutor {
    /// Create a disarmed executor.
    pub fn new() -> Self {
        Self {
            crash_next: AtomicBool::new(false),
            records: Mutex::new(Vec::new()),
        }
    }

    /// Arm (or disarm) a crash on the next `execute` call.
    ///
    /// The sim runner calls this immediately before handing the chosen task
    /// to the real processing pipeline; the very next `execute` — and only
    /// that one — panics.
    pub(crate) fn arm_crash(&self, crash: bool) {
        self.crash_next.store(crash, Ordering::Release);
    }

    /// Drain all records observed so far (the runner reconciles per step).
    pub(crate) fn drain_records(&self) -> Vec<ExecRecord> {
        std::mem::take(&mut self.records.lock().unwrap())
    }

    /// Total records observed since creation (monotonic).
    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

impl Default for SimExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorExecutor for SimExecutor {
    fn execute(&self, actor_id: &ActorId, message: &Message) -> ExecutionResult {
        if self.crash_next.swap(false, Ordering::AcqRel) {
            panic!("{}", SIM_CRASH_MESSAGE);
        }
        let tag = match &message.payload {
            MessagePayload::Custom(bytes) if bytes.len() == 8 => {
                let arr: [u8; 8] = bytes.as_slice().try_into().expect("8-byte payload");
                ExecTag::Custom(u64::from_le_bytes(arr))
            }
            _ => ExecTag::System,
        };
        self.records.lock().unwrap().push(ExecRecord {
            actor: *actor_id,
            tag,
        });
        ExecutionResult::Success {
            fuel_consumed: 0,
            response: None,
        }
    }

    fn is_ready(&self, _actor_id: &ActorId) -> bool {
        true
    }

    fn get_fuel(&self, _actor_id: &ActorId) -> Option<u64> {
        Some(u64::MAX)
    }

    fn reset(&self, _actor_id: &ActorId) -> crate::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Priority;
    use std::sync::Arc;
    use uuid::Uuid;

    fn custom_msg(mid: u64) -> Message {
        Message {
            sender: None,
            payload: MessagePayload::Custom(mid.to_le_bytes().to_vec()),
            priority: Priority::Normal,
        }
    }

    #[test]
    fn records_identities_and_system_messages() {
        let ex = SimExecutor::new();
        let actor = ActorId(Uuid::from_u128(1));
        let _ = ex.execute(&actor, &custom_msg(42));
        let _ = ex.execute(
            &actor,
            &Message {
                sender: None,
                payload: MessagePayload::Start,
                priority: Priority::High,
            },
        );
        let records = ex.drain_records();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0],
            ExecRecord {
                actor,
                tag: ExecTag::Custom(42)
            }
        );
        assert_eq!(
            records[1],
            ExecRecord {
                actor,
                tag: ExecTag::System
            }
        );
        assert_eq!(ex.record_count(), 0, "drained");
    }

    #[test]
    fn armed_crash_panics_once_then_disarms() {
        let ex = Arc::new(SimExecutor::new());
        let actor = ActorId(Uuid::from_u128(2));
        ex.arm_crash(true);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = ex.execute(&actor, &custom_msg(1));
        }));
        assert!(caught.is_err(), "armed execute must panic");
        // Disarmed by the panic: next execute succeeds and records.
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = ex.execute(&actor, &custom_msg(1));
        }));
        assert!(caught.is_ok(), "crash is single-shot");
        assert_eq!(ex.drain_records().len(), 1, "only the post-crash record");
    }
}
