//! 2Q (Two-Queue) page replacement policy.
//!
//! 2Q uses three structures to achieve scan resistance:
//! - `A1in`: FIFO queue for first-access pages
//! - `A1out`: Ghost buffer tracking recently evicted pages from A1in
//! - `Am`: LRU queue for "hot" pages (those that hit A1out)
//!
//! ## Algorithm
//! - First access: page enters A1in (FIFO queue)
//! - If page hits A1out (ghost buffer): promote directly to Am (it's hot)
//! - If page is already in Am: move to MRU position
//! - Eviction: prefer A1in (FIFO), else Am (LRU)
//! - When evicting from A1in: add PageId to A1out ghost buffer
//!
//! ## Scoring Convention (consistent across all policies)
//! - Higher score = hotter = evict later
//! - Lower score = colder = evict first
//! - Am pages get higher scores than A1in pages
//!
//! ## Reference
//! Johnson & Shasha, "2Q: A Low Overhead High Performance Buffer Management
//! Replacement Algorithm" (1994)

use std::collections::{HashMap, HashSet, VecDeque};

use crate::common::{FrameId, PageId};

use super::{EvictionPolicy, PolicyState};

/// 2Q (Two-Queue) replacement policy.
///
/// Provides scan resistance by separating first-access pages from
/// repeatedly-accessed "hot" pages.
pub struct TwoQReplacer {
    /// FIFO queue for first-access pages.
    /// Front = oldest (evict first), Back = newest.
    a1_in: VecDeque<FrameId>,

    /// Ghost buffer: tracks PageIds recently evicted from A1in (FIFO order).
    /// Front = oldest (trimmed first), Back = newest.
    a1_out: VecDeque<PageId>,

    /// O(1) membership lookup companion for a1_out.
    a1_out_set: HashSet<PageId>,

    /// LRU queue for hot pages (promoted from A1in via A1out hit).
    /// Front = LRU (evict first), Back = MRU.
    am: VecDeque<FrameId>,

    /// Maps FrameId to its current queue location.
    frame_location: HashMap<FrameId, QueueLocation>,

    /// Maps FrameId to its PageId.
    frame_to_page: HashMap<FrameId, PageId>,

    /// Set of evictable frames.
    evictable: HashSet<FrameId>,

    /// Maximum size of ghost buffer (A1out).
    /// Typically 50% of pool size.
    ghost_buffer_max_size: usize,

    /// Imported page scores waiting for frame assignment.
    pending_page_scores: HashMap<PageId, u64>,

    /// Counter for scoring during export (higher = hotter).
    score_counter: u64,
}

/// Tracks which queue a frame belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueLocation {
    /// In the A1in FIFO queue.
    A1In,
    /// In the Am LRU queue.
    Am,
}

impl TwoQReplacer {
    /// Create a new 2Q replacer.
    ///
    /// `ghost_buffer_max_size` controls how many evicted PageIds to remember.
    /// Typically set to 50% of the buffer pool size.
    pub fn new(ghost_buffer_max_size: usize) -> Self {
        Self {
            a1_in: VecDeque::new(),
            a1_out: VecDeque::new(),
            a1_out_set: HashSet::new(),
            am: VecDeque::new(),
            frame_location: HashMap::new(),
            frame_to_page: HashMap::new(),
            evictable: HashSet::new(),
            ghost_buffer_max_size,
            pending_page_scores: HashMap::new(),
            score_counter: 0,
        }
    }

    /// Create with default ghost buffer size (will be set later based on pool size).
    pub fn with_default_ghost_size() -> Self {
        Self::new(50) // Default, can be adjusted
    }

    /// Trim ghost buffer to max size (FIFO: evict oldest entries from front).
    fn trim_ghost_buffer(&mut self) {
        while self.a1_out.len() > self.ghost_buffer_max_size {
            if let Some(page_id) = self.a1_out.pop_front() {
                self.a1_out_set.remove(&page_id);
            }
        }
    }

    /// Remove a frame from whichever queue it's in.
    fn remove_from_queues(&mut self, frame_id: FrameId) {
        if let Some(location) = self.frame_location.remove(&frame_id) {
            match location {
                QueueLocation::A1In => {
                    self.a1_in.retain(|&f| f != frame_id);
                }
                QueueLocation::Am => {
                    self.am.retain(|&f| f != frame_id);
                }
            }
        }
    }
}

impl Default for TwoQReplacer {
    fn default() -> Self {
        Self::with_default_ghost_size()
    }
}

impl EvictionPolicy for TwoQReplacer {
    fn name(&self) -> &'static str {
        "2q"
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
        self.score_counter += 1;

        // Check if frame already tracked
        if let Some(&location) = self.frame_location.get(&frame_id) {
            // Frame exists, update page mapping
            self.frame_to_page.insert(frame_id, page_id);

            match location {
                QueueLocation::A1In => {
                    // Re-access in A1in: no promotion yet (stays in A1in)
                    // The 2Q paper doesn't promote within A1in
                }
                QueueLocation::Am => {
                    // Re-access in Am: move to MRU position (back)
                    self.am.retain(|&f| f != frame_id);
                    self.am.push_back(frame_id);
                }
            }
            return;
        }

        // New frame - check for imported score
        let imported_score = self.pending_page_scores.remove(&page_id);

        // Check ghost buffer hit (O(1) via HashSet)
        let ghost_hit = self.a1_out_set.remove(&page_id);
        if ghost_hit {
            self.a1_out.retain(|&p| p != page_id);
        }

        if ghost_hit || imported_score.map(|s| s > 0).unwrap_or(false) {
            // Ghost hit or imported as hot: page is hot, goes directly to Am
            self.am.push_back(frame_id);
            self.frame_location.insert(frame_id, QueueLocation::Am);
        } else {
            // First access: goes to A1in
            self.a1_in.push_back(frame_id);
            self.frame_location.insert(frame_id, QueueLocation::A1In);
        }

        self.frame_to_page.insert(frame_id, page_id);
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if evictable {
            self.evictable.insert(frame_id);
        } else {
            self.evictable.remove(&frame_id);
        }
    }

    fn evict(&mut self) -> Option<FrameId> {
        if self.evictable.is_empty() {
            return None;
        }

        // Try to evict from A1in first (FIFO: front is oldest)
        for &frame_id in &self.a1_in {
            if self.evictable.contains(&frame_id) {
                // Found evictable frame in A1in
                let page_id = self.frame_to_page.remove(&frame_id);
                self.frame_location.remove(&frame_id);
                self.a1_in.retain(|&f| f != frame_id);
                self.evictable.remove(&frame_id);

                // Add to ghost buffer (FIFO: newest at back)
                if let Some(pid) = page_id {
                    self.a1_out.push_back(pid);
                    self.a1_out_set.insert(pid);
                    self.trim_ghost_buffer();
                }

                return Some(frame_id);
            }
        }

        // No evictable frame in A1in, try Am (LRU: front is oldest)
        for &frame_id in &self.am {
            if self.evictable.contains(&frame_id) {
                // Found evictable frame in Am
                self.frame_to_page.remove(&frame_id);
                self.frame_location.remove(&frame_id);
                self.am.retain(|&f| f != frame_id);
                self.evictable.remove(&frame_id);
                // Am evictions don't go to ghost buffer (already proven hot)
                return Some(frame_id);
            }
        }

        None
    }

    fn remove(&mut self, frame_id: FrameId) {
        self.remove_from_queues(frame_id);
        self.frame_to_page.remove(&frame_id);
        self.evictable.remove(&frame_id);
    }

    fn size(&self) -> usize {
        self.evictable.len()
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());

        // Am pages are hotter than A1in pages
        // Within each queue, later = hotter (higher score)

        let mut score = 1u64;

        // A1in pages: lower scores (colder)
        for &frame_id in &self.a1_in {
            if let Some(&page_id) = self.frame_to_page.get(&frame_id) {
                state.hot_pages.push((page_id, score));
                score += 1;
            }
        }

        // Am pages: higher scores (hotter)
        // Add a gap to clearly separate Am from A1in
        score += 1000;
        for &frame_id in &self.am {
            if let Some(&page_id) = self.frame_to_page.get(&frame_id) {
                state.hot_pages.push((page_id, score));
                score += 1;
            }
        }

        state
    }

    fn import_state(&mut self, state: &PolicyState) {
        self.pending_page_scores.clear();
        for &(page_id, score) in &state.hot_pages {
            self.pending_page_scores.insert(page_id, score);
        }

        // Update score counter to be above any imported score
        if let Some(&max_score) = state.hot_pages.iter().map(|(_, s)| s).max() {
            self.score_counter = max_score;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_two_q_name() {
        let replacer = TwoQReplacer::new(10);
        assert_eq!(replacer.name(), "2q");
    }

    #[test]
    fn test_two_q_first_access_goes_to_a1in() {
        let mut replacer = TwoQReplacer::new(10);

        // First access should go to A1in
        replacer.record_access(FrameId::new(0), PageId::new(100));

        assert_eq!(replacer.frame_location.get(&FrameId::new(0)), Some(&QueueLocation::A1In));
        assert_eq!(replacer.a1_in.len(), 1);
        assert_eq!(replacer.am.len(), 0);
    }

    #[test]
    fn test_two_q_ghost_hit_promotes_to_am() {
        let mut replacer = TwoQReplacer::new(10);

        // First access: page 100 goes to A1in
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.set_evictable(FrameId::new(0), true);

        // Evict: page 100 goes to ghost buffer
        let evicted = replacer.evict();
        assert_eq!(evicted, Some(FrameId::new(0)));
        assert!(replacer.a1_out_set.contains(&PageId::new(100)));

        // Second access: page 100 hits ghost buffer, should go to Am
        replacer.record_access(FrameId::new(1), PageId::new(100));

        assert_eq!(replacer.frame_location.get(&FrameId::new(1)), Some(&QueueLocation::Am));
        assert_eq!(replacer.am.len(), 1);
        assert!(!replacer.a1_out_set.contains(&PageId::new(100))); // Removed from ghost
    }

    #[test]
    fn test_two_q_am_reaccess_moves_to_mru() {
        let mut replacer = TwoQReplacer::new(10);

        // Simulate ghost hits to get frames into Am
        replacer.a1_out.push_back(PageId::new(100));
        replacer.a1_out_set.insert(PageId::new(100));
        replacer.a1_out.push_back(PageId::new(101));
        replacer.a1_out_set.insert(PageId::new(101));
        replacer.a1_out.push_back(PageId::new(102));
        replacer.a1_out_set.insert(PageId::new(102));

        replacer.record_access(FrameId::new(0), PageId::new(100)); // -> Am
        replacer.record_access(FrameId::new(1), PageId::new(101)); // -> Am
        replacer.record_access(FrameId::new(2), PageId::new(102)); // -> Am

        // Am order: [0, 1, 2] (0 is LRU)
        assert_eq!(replacer.am.front(), Some(&FrameId::new(0)));
        assert_eq!(replacer.am.back(), Some(&FrameId::new(2)));

        // Re-access frame 0: should move to MRU
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // Am order should now be: [1, 2, 0] (1 is LRU, 0 is MRU)
        assert_eq!(replacer.am.front(), Some(&FrameId::new(1)));
        assert_eq!(replacer.am.back(), Some(&FrameId::new(0)));
    }

    #[test]
    fn test_two_q_eviction_prefers_a1in() {
        let mut replacer = TwoQReplacer::new(10);

        // Put frame 0 in Am (via ghost hit)
        replacer.a1_out.push_back(PageId::new(100));
        replacer.a1_out_set.insert(PageId::new(100));
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // Put frames 1, 2 in A1in
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        // Should evict from A1in first (FIFO order: 1, then 2)
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
        // Now Am: evict frame 0
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
    }

    #[test]
    fn test_two_q_eviction_adds_to_ghost() {
        let mut replacer = TwoQReplacer::new(10);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.set_evictable(FrameId::new(0), true);

        // Before eviction: ghost buffer empty
        assert!(replacer.a1_out_set.is_empty());

        replacer.evict();

        // After eviction: page 100 in ghost buffer
        assert!(replacer.a1_out_set.contains(&PageId::new(100)));
    }

    #[test]
    fn test_two_q_ghost_buffer_limit() {
        let mut replacer = TwoQReplacer::new(2); // Small ghost buffer

        // Add 3 pages to A1in and evict them
        for i in 0..3 {
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
            replacer.set_evictable(FrameId::new(i), true);
        }

        replacer.evict(); // page 100 -> ghost
        replacer.evict(); // page 101 -> ghost
        replacer.evict(); // page 102 -> ghost (should trim oldest)

        // Ghost buffer should be capped at 2
        assert_eq!(replacer.a1_out.len(), 2);
        assert_eq!(replacer.a1_out_set.len(), 2);
        // Oldest (page 100) should have been trimmed, newest two remain
        assert!(!replacer.a1_out_set.contains(&PageId::new(100)));
        assert!(replacer.a1_out_set.contains(&PageId::new(101)));
        assert!(replacer.a1_out_set.contains(&PageId::new(102)));
    }

    #[test]
    fn test_two_q_skips_pinned_frames() {
        let mut replacer = TwoQReplacer::new(10);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        // Only frame 2 is evictable
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);
        replacer.set_evictable(FrameId::new(2), true);

        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_two_q_all_pinned_returns_none() {
        let mut replacer = TwoQReplacer::new(10);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);

        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_two_q_export_state_scores() {
        let mut replacer = TwoQReplacer::new(10);

        // Frame 0 in A1in
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // Frame 1 in Am (via ghost hit)
        replacer.a1_out.push_back(PageId::new(101));
        replacer.a1_out_set.insert(PageId::new(101));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        let state = replacer.export_state();

        assert_eq!(state.source_policy, "2q");
        assert_eq!(state.hot_pages.len(), 2);

        let score_100 = state.hot_pages.iter()
            .find(|(p, _)| *p == PageId::new(100))
            .map(|(_, s)| *s)
            .unwrap();
        let score_101 = state.hot_pages.iter()
            .find(|(p, _)| *p == PageId::new(101))
            .map(|(_, s)| *s)
            .unwrap();

        // Am page (101) should have higher score than A1in page (100)
        assert!(score_101 > score_100);
    }

    #[test]
    fn test_two_q_import_preserves_hotness() {
        let mut replacer = TwoQReplacer::new(10);

        // Import: page 100 is cold (score 0), page 101 is hot (score 1000)
        let mut state = PolicyState::new("lru");
        state.hot_pages.push((PageId::new(100), 0));
        state.hot_pages.push((PageId::new(101), 1000));

        replacer.import_state(&state);

        // Fetch pages
        replacer.record_access(FrameId::new(0), PageId::new(100)); // cold -> A1in
        replacer.record_access(FrameId::new(1), PageId::new(101)); // hot -> Am

        // Verify placement
        assert_eq!(replacer.frame_location.get(&FrameId::new(0)), Some(&QueueLocation::A1In));
        assert_eq!(replacer.frame_location.get(&FrameId::new(1)), Some(&QueueLocation::Am));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // Cold page in A1in should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    }

    #[test]
    fn test_two_q_remove() {
        let mut replacer = TwoQReplacer::new(10);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        // Remove frame 1
        replacer.remove(FrameId::new(1));

        assert_eq!(replacer.size(), 2);
        assert_eq!(replacer.a1_in.len(), 2);

        // Evict remaining
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
    }
}
