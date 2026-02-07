//! Eviction policy trait and state transfer types.
//!
//! This module defines the `EvictionPolicy` trait that all replacer
//! implementations must implement. The trait enables:
//!
//! 1. **Uniform interface**: BufferPoolManager can work with any policy
//! 2. **Runtime swapping**: Policies can be swapped via trait objects (`Box<dyn EvictionPolicy>`)
//! 3. **Warm state transfer**: `PolicyState` allows transferring "hot page" knowledge between policies

use std::collections::HashMap;
use std::time::Instant;

use crate::common::{FrameId, PageId};

/// State that can be transferred between eviction policies during a warm swap.
///
/// When swapping policies at runtime, we can either:
/// - **Cold swap**: Start fresh (new policy has no knowledge of access patterns)
/// - **Warm swap**: Transfer state (new policy inherits knowledge of "hot" pages)
///
/// `PolicyState` captures the essential information that any policy can use:
/// the pages that have been frequently/recently accessed.
#[derive(Debug, Clone)]
pub struct PolicyState {
    /// Pages ranked by "hotness" (access frequency/recency).
    ///
    /// Each entry is `(PageId, score)` where higher scores indicate hotter pages.
    /// The exact meaning of the score depends on the source policy:
    /// - FIFO: position in queue (lower = older = colder)
    /// - LRU: recency rank (higher = more recent = hotter)
    /// - LRU-K: inverse of K-distance (higher = hotter)
    pub hot_pages: Vec<(PageId, u64)>,

    /// Policy-specific metadata for advanced state transfer.
    ///
    /// Allows policies to export/import additional data that doesn't fit
    /// the `hot_pages` model. Keys are policy-defined strings.
    pub metadata: HashMap<String, Vec<u8>>,

    /// When this state was captured.
    ///
    /// Used to detect stale state (e.g., if swap is delayed).
    pub captured_at: Instant,

    /// Name of the policy that exported this state.
    pub source_policy: String,
}

impl PolicyState {
    /// Create a new PolicyState.
    pub fn new(source_policy: impl Into<String>) -> Self {
        Self {
            hot_pages: Vec::new(),
            metadata: HashMap::new(),
            captured_at: Instant::now(),
            source_policy: source_policy.into(),
        }
    }

    /// Check if this state is stale (older than the given threshold).
    ///
    /// Stale state may not accurately reflect current access patterns.
    pub fn is_stale(&self, max_age: std::time::Duration) -> bool {
        self.captured_at.elapsed() > max_age
    }

    /// Get the top N hottest pages.
    ///
    /// Returns pages sorted by descending hotness score.
    pub fn top_hot_pages(&self, n: usize) -> Vec<PageId> {
        let mut sorted = self.hot_pages.clone();
        sorted.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by score descending
        sorted.into_iter().take(n).map(|(page_id, _)| page_id).collect()
    }
}

/// Trait for buffer pool eviction policies.
///
/// An eviction policy decides which page to remove from the buffer pool
/// when space is needed for a new page. Different policies have different
/// trade-offs:
///
/// | Policy | Strengths | Weaknesses |
/// |--------|-----------|------------|
/// | FIFO   | Simple, predictable | No recency awareness |
/// | Clock  | Cheap approximation of LRU | Still scan-vulnerable |
/// | LRU    | Good for temporal locality | Scan-vulnerable |
/// | LRU-K  | Scan-resistant | More complex, memory overhead |
/// | 2Q     | Scan-resistant, simple | Fixed queue ratios |
/// | ARC    | Self-tuning, scan-resistant | Most complex |
///
/// # Implementing a new policy
///
/// ```ignore
/// use interchangedb::buffer::replacer::{EvictionPolicy, PolicyState};
///
/// struct MyReplacer { /* ... */ }
///
/// impl EvictionPolicy for MyReplacer {
///     fn name(&self) -> &'static str { "my-policy" }
///
///     fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
///         // Track that this frame/page was accessed
///     }
///
///     fn evict(&mut self) -> Option<FrameId> {
///         // Return the frame to evict, or None if all frames are pinned
///     }
///
///     // ... implement remaining methods
/// }
/// ```
pub trait EvictionPolicy: Send {
    /// Human-readable name of this policy (e.g., "fifo", "lru", "lru-k").
    fn name(&self) -> &'static str;

    /// Record that a frame was accessed.
    ///
    /// Called by the buffer pool manager whenever a page is fetched or created.
    /// The policy uses this to track access patterns.
    ///
    /// # Arguments
    /// * `frame_id` - The frame slot that was accessed
    /// * `page_id` - The page stored in that frame (needed for some policies like LRU-K)
    fn record_access(&mut self, frame_id: FrameId, page_id: PageId);

    /// Set whether a frame is evictable.
    ///
    /// A frame is evictable when its pin count is 0 (no one is using it).
    /// Pinned frames (pin_count > 0) must never be evicted.
    ///
    /// # Arguments
    /// * `frame_id` - The frame to update
    /// * `evictable` - `true` if the frame can be evicted, `false` if pinned
    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool);

    /// Select and remove the next frame to evict.
    ///
    /// Returns `Some(frame_id)` of the victim frame, or `None` if no frames
    /// are evictable (all are pinned).
    ///
    /// The chosen frame is removed from the policy's tracking structures.
    fn evict(&mut self) -> Option<FrameId>;

    /// Remove a frame from the policy entirely.
    ///
    /// Called when a page is deleted from the buffer pool. The frame should
    /// be removed from all internal tracking structures.
    fn remove(&mut self, frame_id: FrameId);

    /// Number of evictable frames currently tracked.
    fn size(&self) -> usize;

    /// Export current state for warm swap.
    ///
    /// Captures the policy's knowledge of access patterns in a format that
    /// other policies can import. This enables preserving "hot page" knowledge
    /// when switching policies at runtime.
    fn export_state(&self) -> PolicyState;

    /// Import state from another policy (warm swap).
    ///
    /// Uses the provided state to initialize this policy's internal structures.
    /// The policy should interpret the `hot_pages` scores appropriately for
    /// its own algorithm.
    ///
    /// # Arguments
    /// * `state` - State exported from another policy
    fn import_state(&mut self, state: &PolicyState);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_policy_state_new() {
        let state = PolicyState::new("test-policy");
        assert_eq!(state.source_policy, "test-policy");
        assert!(state.hot_pages.is_empty());
        assert!(state.metadata.is_empty());
    }

    #[test]
    fn test_policy_state_is_stale() {
        let state = PolicyState::new("test");

        // Freshly created state should not be stale
        assert!(!state.is_stale(Duration::from_secs(1)));

        // Sleep briefly and check with a very short threshold
        thread::sleep(Duration::from_millis(10));
        assert!(state.is_stale(Duration::from_millis(5)));
        assert!(!state.is_stale(Duration::from_secs(1)));
    }

    #[test]
    fn test_policy_state_top_hot_pages() {
        let mut state = PolicyState::new("test");

        // Add pages with various scores (not in order)
        state.hot_pages.push((PageId::new(1), 10));
        state.hot_pages.push((PageId::new(2), 50));
        state.hot_pages.push((PageId::new(3), 30));
        state.hot_pages.push((PageId::new(4), 20));
        state.hot_pages.push((PageId::new(5), 40));

        // Top 3 should be pages 2, 5, 3 (scores 50, 40, 30)
        let top3 = state.top_hot_pages(3);
        assert_eq!(top3.len(), 3);
        assert_eq!(top3[0], PageId::new(2)); // score 50
        assert_eq!(top3[1], PageId::new(5)); // score 40
        assert_eq!(top3[2], PageId::new(3)); // score 30

        // Top 10 should return all 5 pages
        let top10 = state.top_hot_pages(10);
        assert_eq!(top10.len(), 5);

        // Top 0 should return empty
        let top0 = state.top_hot_pages(0);
        assert!(top0.is_empty());
    }

    #[test]
    fn test_policy_state_top_hot_pages_empty() {
        let state = PolicyState::new("test");
        let top = state.top_hot_pages(5);
        assert!(top.is_empty());
    }
}
