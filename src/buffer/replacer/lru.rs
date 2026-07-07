//! LRU (Least Recently Used) page replacement policy.
//!
//! Classic LRU evicts the page that hasn't been accessed for the longest time.
//! This policy works well when there's temporal locality in access patterns.
//!
//! Implementation uses the `lru` crate for O(1) access and eviction.
//!
//! ## Scoring Convention (consistent across all policies)
//! - Higher score = hotter = more recently used = evict later
//! - Lower score = colder = least recently used = evict first

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;

use lru::LruCache;

use crate::common::{FrameId, PageId};

use super::{EvictionPolicy, PolicyState};

/// LRU (Least Recently Used) replacement policy.
///
/// Evicts the frame that hasn't been accessed for the longest time.
/// Uses the `lru` crate for efficient O(1) operations.
pub struct LruReplacer {
    /// LRU cache tracking access order. Value is PageId for export_state.
    cache: LruCache<FrameId, PageId>,

    /// Evictable frames ordered by `(score, frame)` — `evict()` pops the
    /// first entry, O(log n). Replaced the old evictable HashSet + full
    /// min-scan per eviction (quadratic at trace-replay cache sizes,
    /// found by the Clock2Q+ reproduction runs).
    /// NOTE: ties between arbitrary imported scores were previously
    /// broken by HashSet iteration order (nondeterministic); the frame
    /// id in the key makes them deterministic.
    victim_order: BTreeSet<(u64, usize)>,

    /// Imported page scores waiting for frame assignment (for warm swap).
    pending_page_scores: HashMap<PageId, u64>,

    /// Counter for assigning scores during export (higher = more recent).
    access_counter: u64,

    /// Maps frame to its access order (for consistent export scores).
    frame_access_order: HashMap<FrameId, u64>,
}

impl LruReplacer {
    /// Create a new LRU replacer with the given capacity.
    ///
    /// `capacity` should match the buffer pool size.
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            cache: LruCache::new(capacity),
            victim_order: BTreeSet::new(),
            pending_page_scores: HashMap::new(),
            access_counter: 0,
            frame_access_order: HashMap::new(),
        }
    }
}

impl Default for LruReplacer {
    fn default() -> Self {
        Self::new(10000)
    }
}

impl EvictionPolicy for LruReplacer {
    fn name(&self) -> &'static str {
        "lru"
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
        // Check if this page has an imported score
        let imported_score = self.pending_page_scores.remove(&page_id);

        // The frame's score is about to change: pull it out of the
        // eviction order (if evictable) and re-file it after.
        let was_evictable = match self.frame_access_order.get(&frame_id) {
            Some(&old_score) => self.victim_order.remove(&(old_score, frame_id.0)),
            None => false,
        };

        // Update or insert into LRU cache (this promotes to most-recently-used)
        self.cache.put(frame_id, page_id);

        // Track access order for export_state scoring
        self.access_counter += 1;
        let score = if let Some(s) = imported_score {
            // Use imported score to maintain relative ordering
            s
        } else {
            self.access_counter
        };
        self.frame_access_order.insert(frame_id, score);
        if was_evictable {
            self.victim_order.insert((score, frame_id.0));
        }
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        // Q-35 contract: `evictable ⊆ tracked` — see FifoReplacer. Doubly
        // vital here: an untracked frame has no score, so a stale insert
        // would need a made-up key and become a phantom victim.
        let Some(&score) = self.frame_access_order.get(&frame_id) else {
            return;
        };
        if evictable {
            self.victim_order.insert((score, frame_id.0)); // idempotent
        } else {
            self.victim_order.remove(&(score, frame_id.0));
        }
    }

    fn evict(&mut self) -> Option<FrameId> {
        // Victim = minimum (score, frame): the least recently used
        // evictable frame; imported warm-swap scores order consistently.
        let &(score, frame_index) = self.victim_order.iter().next()?;
        let removed = self.victim_order.remove(&(score, frame_index));
        assert!(removed, "victim key must be present");

        let frame_id = FrameId::new(frame_index);
        self.cache.pop(&frame_id);
        let had_score = self.frame_access_order.remove(&frame_id);
        assert!(had_score.is_some(), "victim frame must have a score");
        Some(frame_id)
    }

    fn remove(&mut self, frame_id: FrameId) {
        self.cache.pop(&frame_id);
        if let Some(score) = self.frame_access_order.remove(&frame_id) {
            self.victim_order.remove(&(score, frame_id.0));
        }
    }

    fn size(&self) -> usize {
        self.victim_order.len()
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());

        // Export pages with scores based on access order
        // Higher score = more recently used = hotter
        for (&frame_id, &page_id) in self.cache.iter() {
            let score = self.frame_access_order.get(&frame_id).copied().unwrap_or(0);
            state.hot_pages.push((page_id, score));
        }

        state
    }

    fn import_state(&mut self, state: &PolicyState) {
        // Store imported scores for use when record_access is called
        self.pending_page_scores.clear();
        for &(page_id, score) in &state.hot_pages {
            self.pending_page_scores.insert(page_id, score);
        }

        // Update access_counter to be higher than any imported score
        // so new pages are considered "more recent"
        if let Some(&max_score) = state.hot_pages.iter().map(|(_, s)| s).max() {
            self.access_counter = max_score;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_name() {
        let replacer = LruReplacer::new(100);
        assert_eq!(replacer.name(), "lru");
    }

    #[test]
    fn test_lru_basic_eviction_order() {
        let mut replacer = LruReplacer::new(100);

        // Access frames in order: 0, 1, 2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        assert_eq!(replacer.size(), 3);

        // LRU order: 0 is least recent, should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_lru_reaccess_promotes_to_mru() {
        let mut replacer = LruReplacer::new(100);

        // Access frames: 0, 1, 2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        // Re-access frame 0 (promotes to most recently used)
        replacer.record_access(FrameId::new(0), PageId::new(100));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        // Now LRU order: 1, 2, 0 (0 was re-accessed, so it's most recent)
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
    }

    #[test]
    fn test_lru_skips_pinned_frames() {
        let mut replacer = LruReplacer::new(100);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        // Only frame 2 is evictable
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);
        replacer.set_evictable(FrameId::new(2), true);

        // Should skip 0 and 1, evict 2
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_lru_all_pinned_returns_none() {
        let mut replacer = LruReplacer::new(100);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        // All pinned
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);

        assert_eq!(replacer.evict(), None);
        assert_eq!(replacer.size(), 0);
    }

    #[test]
    fn test_lru_export_state_scores() {
        let mut replacer = LruReplacer::new(100);

        // Access in order: 0, 1, 2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        let state = replacer.export_state();

        assert_eq!(state.source_policy, "lru");
        assert_eq!(state.hot_pages.len(), 3);

        // Find scores for each page
        let score_100 = state
            .hot_pages
            .iter()
            .find(|(p, _)| *p == PageId::new(100))
            .map(|(_, s)| *s)
            .unwrap();
        let score_101 = state
            .hot_pages
            .iter()
            .find(|(p, _)| *p == PageId::new(101))
            .map(|(_, s)| *s)
            .unwrap();
        let score_102 = state
            .hot_pages
            .iter()
            .find(|(p, _)| *p == PageId::new(102))
            .map(|(_, s)| *s)
            .unwrap();

        // More recent = higher score
        assert!(score_100 < score_101);
        assert!(score_101 < score_102);
    }

    #[test]
    fn test_lru_import_preserves_order() {
        let mut replacer = LruReplacer::new(100);

        // Import state: page 100 is cold (score 10), page 101 is hot (score 100)
        let mut state = PolicyState::new("fifo");
        state.hot_pages.push((PageId::new(100), 10)); // cold
        state.hot_pages.push((PageId::new(101), 100)); // hot

        replacer.import_state(&state);

        // Fetch pages (order doesn't matter, scores determine LRU position)
        replacer.record_access(FrameId::new(5), PageId::new(101)); // hot
        replacer.record_access(FrameId::new(3), PageId::new(100)); // cold

        replacer.set_evictable(FrameId::new(5), true);
        replacer.set_evictable(FrameId::new(3), true);

        // Cold page (lower score) should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(3))); // page 100
        assert_eq!(replacer.evict(), Some(FrameId::new(5))); // page 101
    }

    #[test]
    fn test_lru_remove() {
        let mut replacer = LruReplacer::new(100);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        // Remove frame 0
        replacer.remove(FrameId::new(0));

        assert_eq!(replacer.size(), 2);

        // Should evict 1, then 2 (0 was removed)
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
    }
}
