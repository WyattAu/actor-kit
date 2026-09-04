//! Pluggable admission-control hook for spawns and message sends.
//!
//! This replaces the upstream `tenant::quota::QuotaEnforcer` coupling: instead
//! of the scheduler depending on a concrete multi-tenant quota type, it depends
//! on this trait. Hosts that need per-tenant quotas, per-actor memory budgets,
//! or rate limiting implement [`ResourcePolicy`] and install it via
//! [`ActorScheduler::with_resource_policy`](crate::ActorScheduler::with_resource_policy).
//!
//! The default policy is [`NoopPolicy`], which admits everything.

use crate::Result;

/// Admission control for actor spawns and message sends.
///
/// Called synchronously on the hot path (`spawn`, `send`, `send_batch`), so
/// implementations must be cheap and non-blocking.
///
/// All methods have default implementations that admit everything, so
/// policies only need to override what they enforce.
pub trait ResourcePolicy: Send + Sync + 'static {
    /// Decide whether a new actor may be spawned.
    ///
    /// Return `Err(reason)` to reject the spawn; the reason is surfaced in the
    /// [`Error::ResourceExhausted`](crate::Error::ResourceExhausted) message.
    fn admit_actor(&self) -> Result<()> {
        Ok(())
    }

    /// Notify the policy that an actor was killed/released.
    ///
    /// Called from `ActorScheduler::kill`. Use to free any per-actor budget.
    fn release_actor(&self) {}

    /// Decide whether a send of `count` messages may proceed.
    ///
    /// `count == 1` for single sends, `> 1` for batch sends.
    fn admit_message(&self, count: usize) -> Result<()> {
        let _ = count;
        Ok(())
    }
}

/// The default policy: admit all spawns and sends.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPolicy;

impl ResourcePolicy for NoopPolicy {}
