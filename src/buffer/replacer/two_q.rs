//! 2Q (Two-Queue) page replacement policy.
//!
//! 2Q uses three structures to achieve scan resistance:
//! - `A1in`: FIFO queue for first-access pages
//! - `A1out`: Ghost buffer tracking recently evicted pages from A1in
//! - `Am`: LRU queue for "hot" pages (those that hit A1out)
//!
//! ## Algorithm (full 2Q, paper §2)
//! - First access: page enters A1in (FIFO queue)
//! - If page hits A1out (ghost buffer): promote directly to Am (it's hot)
//! - If page is already in Am: move to MRU position
//! - If page is already in A1in: nothing (no promotion within A1in)
//! - Eviction (`reclaimfor`): from A1in ONLY while |A1in| > Kin
//!   (25% of capacity), else from Am (LRU). The Kin threshold is what
//!   gives first-access pages a residency window; evicting A1in-first
//!   unconditionally lets Am swallow the cache and starves new pages.
//! - When evicting from A1in: add PageId to A1out ghost buffer
//!   (Kout = 50% of capacity); Am evictions are not remembered
//!
//! ## Scoring Convention (consistent across all policies)
//! - Higher score = hotter = evict later
//! - Lower score = colder = evict first
//! - Am pages get higher scores than A1in pages
//!
//! ## Reference
//! Johnson & Shasha, "2Q: A Low Overhead High Performance Buffer Management
//! Replacement Algorithm" (1994)

use std::collections::{HashMap, HashSet};

use crate::common::{FrameId, PageId};

use super::ordered_list::OrderedList;
use super::{EvictionPolicy, PolicyState};

/// 2Q (Two-Queue) replacement policy.
///
/// Provides scan resistance by separating first-access pages from
/// repeatedly-accessed "hot" pages.
///
/// Queues are `OrderedList`s (seq-ordered BTreeMap + membership map):
/// O(log n) mid-queue removal replaced the original `VecDeque::retain`
/// walks, which were O(n) on every Am hit (found by the Clock2Q+
/// reproduction runs at 20K+ frames).
pub struct TwoQReplacer {
    /// FIFO queue for first-access pages. Front = oldest (evict first).
    a1_in: OrderedList<FrameId>,

    /// Ghost buffer: PageIds recently evicted from A1in.
    /// Front = oldest (trimmed first).
    a1_out: OrderedList<PageId>,

    /// LRU queue for hot pages (promoted from A1in via A1out hit).
    /// Front = LRU (evict first), Back = MRU.
    am: OrderedList<FrameId>,

    /// Maps FrameId to its current queue location.
    frame_location: HashMap<FrameId, QueueLocation>,

    /// Maps FrameId to its PageId.
    frame_to_page: HashMap<FrameId, PageId>,

    /// Set of evictable frames.
    evictable: HashSet<FrameId>,

    /// Kin (paper §3.2): evict from A1in only while `a1_in.len()`
    /// exceeds this; 25% of capacity. This is A1in's residency window —
    /// without it Am swallows the cache and first-access pages starve.
    a1_in_target: usize,

    /// Kout (paper §3.2): ghost capacity; 50% of pool capacity.
    ghost_max: usize,

    /// Imported page scores waiting for frame assignment.
    pending_page_scores: HashMap<PageId, u64>,
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
    /// Create a new 2Q replacer for a pool of `capacity` frames.
    ///
    /// Classic full-2Q tuning (Johnson & Shasha §3.2): Kin = 25% of
    /// capacity, Kout = 50%. Every call site already passed the pool
    /// capacity here; the old implementation misused it as the ghost
    /// size (= 100% of capacity, double the paper's Kout) and had no
    /// Kin at all.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity >= 1, "capacity must be >= 1");
        Self {
            a1_in: OrderedList::new(),
            a1_out: OrderedList::new(),
            am: OrderedList::new(),
            frame_location: HashMap::new(),
            frame_to_page: HashMap::new(),
            evictable: HashSet::new(),
            a1_in_target: (capacity / 4).max(1),
            ghost_max: (capacity / 2).max(1),
            pending_page_scores: HashMap::new(),
        }
    }

    /// Evict the oldest evictable A1in frame; its page goes to the
    /// ghost buffer. `None` if A1in has no evictable frame.
    fn evict_from_a1_in(&mut self) -> Option<FrameId> {
        let victim = self
            .a1_in
            .iter()
            .find(|frame_id| self.evictable.contains(frame_id))
            .copied()?;
        let removed = self.a1_in.remove(&victim);
        assert!(removed, "victim must be in A1in");
        self.frame_location.remove(&victim);
        self.evictable.remove(&victim);
        let page_id = self
            .frame_to_page
            .remove(&victim)
            .expect("A1in frame must have a page");

        // Remember the demoted page (FIFO trim to Kout).
        self.a1_out.push_back(page_id);
        while self.a1_out.len() > self.ghost_max {
            let trimmed = self.a1_out.pop_front();
            assert!(trimmed.is_some(), "over-cap ghost cannot be empty");
        }
        Some(victim)
    }

    /// Evict the LRU evictable Am frame. Am evictions are not ghosted
    /// (the page already proved itself hot once). `None` if Am has no
    /// evictable frame.
    fn evict_from_am(&mut self) -> Option<FrameId> {
        let victim = self
            .am
            .iter()
            .find(|frame_id| self.evictable.contains(frame_id))
            .copied()?;
        let removed = self.am.remove(&victim);
        assert!(removed, "victim must be in Am");
        self.frame_location.remove(&victim);
        self.evictable.remove(&victim);
        let page = self.frame_to_page.remove(&victim);
        assert!(page.is_some(), "Am frame must have a page");
        Some(victim)
    }
}

impl Default for TwoQReplacer {
    fn default() -> Self {
        Self::new(64)
    }
}

impl EvictionPolicy for TwoQReplacer {
    fn name(&self) -> &'static str {
        "2q"
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
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
                    let removed = self.am.remove(&frame_id);
                    assert!(removed, "Am-located frame must be in Am");
                    self.am.push_back(frame_id);
                }
            }
            return;
        }

        // New frame - check for imported score
        let imported_score = self.pending_page_scores.remove(&page_id);

        // Check ghost buffer hit (removes the entry either way).
        let ghost_hit = self.a1_out.remove(&page_id);

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
        // Q-35 contract: `evictable ⊆ tracked` — see FifoReplacer.
        if evictable && !self.frame_location.contains_key(&frame_id) {
            return;
        }
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

        // Classic reclaim (paper §2): A1in only while over Kin, else Am.
        // The other queue is the fallback when the preferred one is all
        // pinned (BPM contract). One pass over each queue suffices —
        // unlike S3-FIFO, 2Q's evict never promotes between queues, so
        // a candidate cannot move into an already-searched queue.
        if self.a1_in.len() > self.a1_in_target {
            self.evict_from_a1_in().or_else(|| self.evict_from_am())
        } else {
            self.evict_from_am().or_else(|| self.evict_from_a1_in())
        }
    }

    fn remove(&mut self, frame_id: FrameId) {
        if let Some(location) = self.frame_location.remove(&frame_id) {
            let removed = match location {
                QueueLocation::A1In => self.a1_in.remove(&frame_id),
                QueueLocation::Am => self.am.remove(&frame_id),
            };
            assert!(removed, "located frame must be in its queue");
        }
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
        for &frame_id in self.a1_in.iter() {
            if let Some(&page_id) = self.frame_to_page.get(&frame_id) {
                state.hot_pages.push((page_id, score));
                score += 1;
            }
        }

        // Am pages: higher scores (hotter)
        // Add a gap to clearly separate Am from A1in
        score += 1000;
        for &frame_id in self.am.iter() {
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

        assert_eq!(
            replacer.frame_location.get(&FrameId::new(0)),
            Some(&QueueLocation::A1In)
        );
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
        assert!(replacer.a1_out.contains(&PageId::new(100)));

        // Second access: page 100 hits ghost buffer, should go to Am
        replacer.record_access(FrameId::new(1), PageId::new(100));

        assert_eq!(
            replacer.frame_location.get(&FrameId::new(1)),
            Some(&QueueLocation::Am)
        );
        assert_eq!(replacer.am.len(), 1);
        assert!(!replacer.a1_out.contains(&PageId::new(100))); // Removed from ghost
    }

    #[test]
    fn test_two_q_am_reaccess_moves_to_mru() {
        let mut replacer = TwoQReplacer::new(10);

        // Simulate ghost hits to get frames into Am
        replacer.a1_out.push_back(PageId::new(100));
        replacer.a1_out.push_back(PageId::new(101));
        replacer.a1_out.push_back(PageId::new(102));

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

    // The classic reclaim rule (paper §2): A1in is only tapped while it
    // exceeds Kin; at or under target, Am's LRU pays instead. This is
    // the rule whose absence let Am swallow the whole cache.
    #[test]
    fn test_two_q_eviction_respects_kin_threshold() {
        let mut replacer = TwoQReplacer::new(8); // Kin = 2, Kout = 4

        // Put frame 0 in Am (via ghost hit)
        replacer.a1_out.push_back(PageId::new(100));
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // Put frames 1, 2, 3 in A1in (len 3 > Kin 2)
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));
        replacer.record_access(FrameId::new(3), PageId::new(103));

        for i in 0..4 {
            replacer.set_evictable(FrameId::new(i), true);
        }

        // |A1in| = 3 > Kin: evict A1in FIFO head (frame 1)
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        // |A1in| = 2 <= Kin: A1in is protected; Am LRU (frame 0) pays
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        // Am empty: fall back to A1in (FIFO order: 2 then 3)
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
        assert_eq!(replacer.evict(), Some(FrameId::new(3)));
    }

    // BPM pin contract: A1in over target but fully pinned must not
    // block eviction — Am is the fallback.
    #[test]
    fn test_two_q_falls_back_to_am_when_a1in_pinned() {
        let mut replacer = TwoQReplacer::new(4); // Kin = 1

        // Frame 0 in Am (via ghost hit), evictable
        replacer.a1_out.push_back(PageId::new(100));
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.set_evictable(FrameId::new(0), true);

        // Frames 1, 2 in A1in (len 2 > Kin 1), both pinned
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), None); // only pinned frames remain
    }

    #[test]
    fn test_two_q_eviction_adds_to_ghost() {
        let mut replacer = TwoQReplacer::new(10);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.set_evictable(FrameId::new(0), true);

        // Before eviction: ghost buffer empty
        assert!(replacer.a1_out.is_empty());

        replacer.evict();

        // After eviction: page 100 in ghost buffer
        assert!(replacer.a1_out.contains(&PageId::new(100)));
    }

    #[test]
    fn test_two_q_ghost_buffer_limit() {
        let mut replacer = TwoQReplacer::new(4); // Kout = 2

        // Add 3 pages to A1in and evict them
        for i in 0..3 {
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
            replacer.set_evictable(FrameId::new(i), true);
        }

        replacer.evict(); // page 100 -> ghost
        replacer.evict(); // page 101 -> ghost
        replacer.evict(); // page 102 -> ghost (should trim oldest)

        // Ghost buffer should be capped at Kout = 2
        assert_eq!(replacer.a1_out.len(), 2);
        // Oldest (page 100) should have been trimmed, newest two remain
        assert!(!replacer.a1_out.contains(&PageId::new(100)));
        assert!(replacer.a1_out.contains(&PageId::new(101)));
        assert!(replacer.a1_out.contains(&PageId::new(102)));
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
        replacer.record_access(FrameId::new(1), PageId::new(101));

        let state = replacer.export_state();

        assert_eq!(state.source_policy, "2q");
        assert_eq!(state.hot_pages.len(), 2);

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

        // Am page (101) should have higher score than A1in page (100)
        assert!(score_101 > score_100);
    }

    #[test]
    fn test_two_q_import_preserves_hotness() {
        let mut replacer = TwoQReplacer::new(4); // Kin = 1

        // Import: pages 100/102 are cold (score 0), page 101 is hot (1000)
        let mut state = PolicyState::new("lru");
        state.hot_pages.push((PageId::new(100), 0));
        state.hot_pages.push((PageId::new(101), 1000));
        state.hot_pages.push((PageId::new(102), 0));

        replacer.import_state(&state);

        // Fetch pages
        replacer.record_access(FrameId::new(0), PageId::new(100)); // cold -> A1in
        replacer.record_access(FrameId::new(1), PageId::new(101)); // hot -> Am
        replacer.record_access(FrameId::new(2), PageId::new(102)); // cold -> A1in

        // Verify placement
        assert_eq!(
            replacer.frame_location.get(&FrameId::new(0)),
            Some(&QueueLocation::A1In)
        );
        assert_eq!(
            replacer.frame_location.get(&FrameId::new(1)),
            Some(&QueueLocation::Am)
        );

        for i in 0..3 {
            replacer.set_evictable(FrameId::new(i), true);
        }

        // |A1in| = 2 > Kin 1: the oldest cold page pays before the hot
        // Am page (which outlives BOTH cold pages only if A1in stays
        // over target — here the second evict drops A1in to Kin, so Am
        // pays next per the classic reclaim rule).
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
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
