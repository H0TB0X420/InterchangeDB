//! FIFO (First-In-First-Out) page replacement policy.
//!
//! This is a simple baseline eviction policy where pages are evicted
//! in the order they were first accessed.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::common::{FrameId, PageId};

use super::{EvictionPolicy, PolicyState};

/// FIFO replacement policy.
///
/// Evicts pages in the order they were first brought into the buffer pool.
/// Once a frame is in the queue, re-accessing it does NOT move it to the back.
///
/// On warm swap, FIFO preserves the relative "hotness" order from the previous
/// policy: cold pages go to front (evict first), hot pages to back (evict last).
pub struct FifoReplacer {
    /// Queue of frame IDs in eviction order (front = evict first).
    queue: VecDeque<FrameId>,

    /// Set of frame IDs currently in the queue (for O(1) membership check).
    in_queue: HashSet<FrameId>,

    /// Set of frame IDs that are evictable (pin_count == 0).
    evictable: HashSet<FrameId>,

    /// Mapping from frame to the page it currently holds.
    /// Needed for export_state() to return PageIds.
    frame_to_page: HashMap<FrameId, PageId>,

    /// Scores for each frame (derived from page scores during import).
    /// Used to maintain relative ordering from the previous policy.
    /// Higher score = hotter = later in queue.
    frame_scores: HashMap<FrameId, u64>,

    /// Imported page scores from warm swap, waiting to be assigned to frames.
    /// When record_access() sees a page with a score here, it transfers to frame_scores.
    pending_page_scores: HashMap<PageId, u64>,

    /// Next score for a fresh frame. Invariant: strictly greater than
    /// every score in `frame_scores`. Replaced a `values().max()` scan
    /// on every insertion — O(n) per miss, quadratic at trace-replay
    /// cache sizes (found by the Clock2Q+ reproduction runs).
    next_score: u64,
}

impl FifoReplacer {
    /// Create a new FIFO replacer.
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            in_queue: HashSet::new(),
            evictable: HashSet::new(),
            frame_to_page: HashMap::new(),
            frame_scores: HashMap::new(),
            pending_page_scores: HashMap::new(),
            next_score: 1,
        }
    }

    /// Find the insertion position for a frame with the given score.
    /// Maintains ascending score order: lower scores at front, higher at back.
    fn find_insert_position(&self, score: u64) -> usize {
        // Binary search would be more efficient, but queue is typically small
        // and this is only used during warm swap
        self.queue
            .iter()
            .position(|&fid| self.frame_scores.get(&fid).copied().unwrap_or(0) > score)
            .unwrap_or(self.queue.len())
    }
}

impl EvictionPolicy for FifoReplacer {
    fn name(&self) -> &'static str {
        "fifo"
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
        // Always update the frame→page mapping (page might change after eviction)
        self.frame_to_page.insert(frame_id, page_id);

        // For FIFO, only add to queue on first access
        if !self.in_queue.contains(&frame_id) {
            // Check if this page has an imported score from warm swap
            if let Some(score) = self.pending_page_scores.remove(&page_id) {
                // Insert at position based on score (preserves relative hotness)
                let pos = self.find_insert_position(score);
                self.queue.insert(pos, frame_id);
                self.frame_scores.insert(frame_id, score);
                self.next_score = self.next_score.max(score + 1);
            } else {
                // No imported score: append to back (standard FIFO behavior)
                // with a score higher than any existing, so it's evicted last
                self.queue.push_back(frame_id);
                self.frame_scores.insert(frame_id, self.next_score);
                self.next_score += 1;
            }
            self.in_queue.insert(frame_id);
        }
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        // Q-35 contract: `evictable ⊆ tracked`. A stale deferred
        // set_evictable(true) can target a frame this policy no longer
        // tracks (evicted and mid-handoff to a new owner); inserting it
        // anyway resurrects it as an eviction candidate it must not be.
        if evictable && !self.in_queue.contains(&frame_id) {
            return;
        }
        if evictable {
            self.evictable.insert(frame_id);
        } else {
            self.evictable.remove(&frame_id);
        }
    }

    fn evict(&mut self) -> Option<FrameId> {
        // Find the position of the first evictable frame
        let position = self
            .queue
            .iter()
            .position(|&fid| self.evictable.contains(&fid))?;

        // Remove only that frame
        let frame_id = self.queue.remove(position)?;
        self.in_queue.remove(&frame_id);
        self.evictable.remove(&frame_id);
        self.frame_to_page.remove(&frame_id);
        self.frame_scores.remove(&frame_id);

        Some(frame_id)
    }

    fn remove(&mut self, frame_id: FrameId) {
        self.evictable.remove(&frame_id);
        self.frame_to_page.remove(&frame_id);
        self.frame_scores.remove(&frame_id);

        if self.in_queue.remove(&frame_id) {
            self.queue.retain(|&fid| fid != frame_id);
        }
    }

    fn size(&self) -> usize {
        self.evictable.len()
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());

        // Export pages with their scores (lower score = evict first = colder)
        for &frame_id in &self.queue {
            if let Some(&page_id) = self.frame_to_page.get(&frame_id) {
                let score = self.frame_scores.get(&frame_id).copied().unwrap_or(0);
                state.hot_pages.push((page_id, score));
            }
        }

        state
    }

    fn import_state(&mut self, state: &PolicyState) {
        // Store imported scores. When record_access() is called for these pages,
        // we'll insert them at positions based on their scores.
        // Lower score = colder = front of queue = evicted first.
        // Higher score = hotter = back of queue = evicted last.
        self.pending_page_scores.clear();
        for &(page_id, score) in &state.hot_pages {
            self.pending_page_scores.insert(page_id, score);
        }
    }
}

impl Default for FifoReplacer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fifo_basic() {
        let mut replacer = FifoReplacer::new();

        // Add frames 0, 1, 2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        // Mark all evictable
        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        assert_eq!(replacer.size(), 3);

        // Should evict in FIFO order
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_fifo_skips_pinned() {
        let mut replacer = FifoReplacer::new();

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        // Only frame 1 is evictable
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), false);

        // Should skip 0, evict 1
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_fifo_remove() {
        let mut replacer = FifoReplacer::new();

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // Remove frame 0
        replacer.remove(FrameId::new(0));

        // Should evict frame 1 (frame 0 was removed)
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    }

    #[test]
    fn test_fifo_reaccess_no_reorder() {
        let mut replacer = FifoReplacer::new();

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(0), PageId::new(100)); // Access again - should NOT reorder

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // FIFO: frame 0 was first, should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    }

    /// Test that failed eviction attempts don't corrupt the queue.
    ///
    /// This was the bug: when all frames are pinned, evict() would empty
    /// the queue, making future evictions impossible even after unpinning.
    #[test]
    fn test_failed_evict_preserves_queue() {
        let mut replacer = FifoReplacer::new();

        // Add frames
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        // All pinned (not evictable)
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);

        // Evict fails - but queue should remain intact
        assert_eq!(replacer.evict(), None);
        assert_eq!(replacer.evict(), None);
        assert_eq!(replacer.evict(), None);

        // Now unpin frame 1
        replacer.set_evictable(FrameId::new(1), true);

        // Frame 0 is first in queue but not evictable, frame 1 should be evicted
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));

        // Now unpin frame 0
        replacer.set_evictable(FrameId::new(0), true);
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
    }

    #[test]
    fn test_evict_only_removes_evicted_frame() {
        let mut replacer = FifoReplacer::new();

        // Add frames in order: 0, 1, 2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        // 0 pinned, 1 evictable, 2 pinned
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), false);

        // Evict should skip 0, evict 1, leave 2 in queue
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));

        // Unpin 0 and 2
        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(2), true);

        // Should evict in original order: 0 then 2
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
    }

    #[test]
    fn test_fifo_name() {
        let replacer = FifoReplacer::new();
        assert_eq!(replacer.name(), "fifo");
    }

    #[test]
    fn test_fifo_export_state() {
        let mut replacer = FifoReplacer::new();

        // Add frames in order: 0, 1, 2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        let state = replacer.export_state();

        assert_eq!(state.source_policy, "fifo");
        assert_eq!(state.hot_pages.len(), 3);

        // Scores increase with arrival order: first = lowest score (evicted first)
        let page_100_score = state
            .hot_pages
            .iter()
            .find(|(pid, _)| *pid == PageId::new(100))
            .map(|(_, score)| *score)
            .unwrap();
        let page_101_score = state
            .hot_pages
            .iter()
            .find(|(pid, _)| *pid == PageId::new(101))
            .map(|(_, score)| *score)
            .unwrap();
        let page_102_score = state
            .hot_pages
            .iter()
            .find(|(pid, _)| *pid == PageId::new(102))
            .map(|(_, score)| *score)
            .unwrap();

        // Older pages have lower scores (evicted first)
        assert!(page_100_score < page_101_score);
        assert!(page_101_score < page_102_score);
    }

    #[test]
    fn test_fifo_export_state_after_eviction() {
        let mut replacer = FifoReplacer::new();

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // Evict frame 0
        replacer.evict();

        let state = replacer.export_state();

        // Only page 101 should remain
        assert_eq!(state.hot_pages.len(), 1);
        assert_eq!(state.hot_pages[0].0, PageId::new(101));
    }

    #[test]
    fn test_fifo_import_state_preserves_order() {
        let mut replacer = FifoReplacer::new();

        // Import state from another policy (e.g., LRU)
        // Page 101 is "hot" (score 100), Page 100 is "cold" (score 10)
        let mut state = PolicyState::new("lru");
        state.hot_pages.push((PageId::new(100), 10)); // cold
        state.hot_pages.push((PageId::new(101), 100)); // hot

        replacer.import_state(&state);

        // Queue is empty until record_access is called
        assert_eq!(replacer.size(), 0);

        // Now simulate BPM fetching these pages into frames
        // The ORDER we call record_access doesn't matter - scores determine position
        replacer.record_access(FrameId::new(5), PageId::new(101)); // hot page
        replacer.record_access(FrameId::new(3), PageId::new(100)); // cold page

        replacer.set_evictable(FrameId::new(5), true);
        replacer.set_evictable(FrameId::new(3), true);

        // Cold page (100, score=10) should be evicted first, even though
        // we called record_access for hot page (101) first
        assert_eq!(replacer.evict(), Some(FrameId::new(3))); // cold page in frame 3
        assert_eq!(replacer.evict(), Some(FrameId::new(5))); // hot page in frame 5
    }

    #[test]
    fn test_fifo_import_mixed_with_new_pages() {
        let mut replacer = FifoReplacer::new();

        // Import state: page 100 is cold (score 10), page 101 is hot (score 100)
        let mut state = PolicyState::new("lru");
        state.hot_pages.push((PageId::new(100), 10));
        state.hot_pages.push((PageId::new(101), 100));

        replacer.import_state(&state);

        // Fetch imported pages
        replacer.record_access(FrameId::new(0), PageId::new(100)); // cold
        replacer.record_access(FrameId::new(1), PageId::new(101)); // hot

        // Fetch a NEW page (not in imported state) - should go to back
        replacer.record_access(FrameId::new(2), PageId::new(999));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        // Eviction order: cold imported (100) -> hot imported (101) -> new page (999)
        assert_eq!(replacer.evict(), Some(FrameId::new(0))); // page 100
        assert_eq!(replacer.evict(), Some(FrameId::new(1))); // page 101
        assert_eq!(replacer.evict(), Some(FrameId::new(2))); // page 999 (new)
    }
}
