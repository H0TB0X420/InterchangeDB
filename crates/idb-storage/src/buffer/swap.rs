//! Runtime policy swap types.
//!
//! Defines [`SwapMode`] for choosing cold vs warm swap, and [`SwapResult`]
//! for reporting what happened during a swap.

use std::time::Duration;

/// How to swap eviction policies at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapMode {
    /// Fresh start — new policy has no knowledge of access patterns.
    Cold,

    /// Transfer state — new policy inherits "hot page" knowledge from old policy.
    ///
    /// Uses [`PolicyState`](super::replacer::PolicyState) to transfer access
    /// pattern data between policies.
    Warm,
}

/// Result of a policy swap operation.
#[derive(Debug, Clone)]
pub struct SwapResult {
    /// Name of the old eviction policy.
    pub old_policy: &'static str,

    /// Name of the new eviction policy.
    pub new_policy: &'static str,

    /// Whether this was a cold or warm swap.
    pub mode: SwapMode,

    /// Number of hot page entries transferred (0 for cold swap).
    pub pages_transferred: usize,

    /// Number of frames re-registered with the new policy.
    pub frames_registered: usize,

    /// Wall-clock time the swap took (while holding the replacer lock).
    pub swap_duration: Duration,
}
