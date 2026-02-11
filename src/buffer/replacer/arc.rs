//! ARC (Adaptive Replacement Cache) page replacement policy.
//!
//! ARC maintains four lists and dynamically adapts between recency and frequency:
//! - `T1`: Pages seen once recently (recency)
//! - `T2`: Pages seen at least twice recently (frequency)
//! - `B1`: Ghost list for T1 (recently evicted from T1)
//! - `B2`: Ghost list for T2 (recently evicted from T2)
//!
//! The parameter `p` controls the target size of T1:
//! - B1 hit → increase p (favor recency)
//! - B2 hit → decrease p (favor frequency)
//!
//! ## Algorithm (simplified)
//! - New page: enters T1
//! - Hit in T1: move to T2 (promoted to frequent)
//! - Hit in T2: move to MRU of T2
//! - Hit in B1: increase p, fetch page to T2
//! - Hit in B2: decrease p, fetch page to T2
//! - Eviction: if |T1| > p, evict from T1; else evict from T2
//!
//! ## Scoring Convention (consistent across all policies)
//! - Higher score = hotter = evict later
//! - Lower score = colder = evict first
//! - T2 pages get higher scores than T1 pages
//!
//! ## Reference
//! Megiddo & Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache" (2003)

use std::collections::{HashMap, HashSet, VecDeque};

use crate::common::{FrameId, PageId};

use super::{EvictionPolicy, PolicyState};

/// ARC (Adaptive Replacement Cache) replacement policy.
///
/// Self-tuning cache that balances between recency and frequency
/// based on observed workload patterns.
pub struct ArcReplacer {
    /// T1: LRU list for pages seen once recently.
    /// Front = LRU (evict first), Back = MRU.
    t1: VecDeque<FrameId>,

    /// T2: LRU list for pages seen at least twice.
    /// Front = LRU (evict first), Back = MRU.
    t2: VecDeque<FrameId>,

    /// B1: Ghost list for T1 (tracks recently evicted PageIds from T1).
    b1: VecDeque<PageId>,

    /// O(1) membership lookup companion for b1.
    b1_set: HashSet<PageId>,

    /// B2: Ghost list for T2 (tracks recently evicted PageIds from T2).
    b2: VecDeque<PageId>,

    /// O(1) membership lookup companion for b2.
    b2_set: HashSet<PageId>,

    /// Target size for T1. Adapts based on ghost hits.
    /// Invariant: 0 <= p <= c
    p: usize,

    /// Cache capacity (number of frames in buffer pool).
    c: usize,

    /// Maps FrameId to which list it belongs to.
    frame_location: HashMap<FrameId, ListLocation>,

    /// Maps FrameId to its PageId.
    frame_to_page: HashMap<FrameId, PageId>,

    /// Set of evictable frames.
    evictable: HashSet<FrameId>,

    /// Imported page scores waiting for frame assignment.
    pending_page_scores: HashMap<PageId, u64>,
}

/// Tracks which list a frame belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListLocation {
    T1,
    T2,
}

impl ArcReplacer {
    /// Create a new ARC replacer with the given cache capacity.
    ///
    /// `capacity` is the number of frames in the buffer pool.
    pub fn new(capacity: usize) -> Self {
        Self {
            t1: VecDeque::new(),
            t2: VecDeque::new(),
            b1: VecDeque::new(),
            b1_set: HashSet::new(),
            b2: VecDeque::new(),
            b2_set: HashSet::new(),
            p: 0,
            c: capacity,
            frame_location: HashMap::new(),
            frame_to_page: HashMap::new(),
            evictable: HashSet::new(),
            pending_page_scores: HashMap::new(),
        }
    }

    /// Get the current target size for T1.
    pub fn p(&self) -> usize {
        self.p
    }

    /// Get the cache capacity.
    pub fn capacity(&self) -> usize {
        self.c
    }

    /// Check if a PageId is in B1 ghost list (O(1)).
    fn in_b1(&self, page_id: PageId) -> bool {
        self.b1_set.contains(&page_id)
    }

    /// Check if a PageId is in B2 ghost list (O(1)).
    fn in_b2(&self, page_id: PageId) -> bool {
        self.b2_set.contains(&page_id)
    }

    /// Remove a PageId from B1.
    fn remove_from_b1(&mut self, page_id: PageId) {
        if self.b1_set.remove(&page_id) {
            self.b1.retain(|&p| p != page_id);
        }
    }

    /// Remove a PageId from B2.
    fn remove_from_b2(&mut self, page_id: PageId) {
        if self.b2_set.remove(&page_id) {
            self.b2.retain(|&p| p != page_id);
        }
    }

    /// Remove a frame from T1 or T2.
    fn remove_from_lists(&mut self, frame_id: FrameId) {
        if let Some(location) = self.frame_location.remove(&frame_id) {
            match location {
                ListLocation::T1 => {
                    self.t1.retain(|&f| f != frame_id);
                }
                ListLocation::T2 => {
                    self.t2.retain(|&f| f != frame_id);
                }
            }
        }
    }

    /// REPLACE subroutine: evict a page from T1 or T2.
    /// Returns the evicted FrameId if successful.
    fn replace(&mut self, in_b2: bool) -> Option<FrameId> {
        // Decide whether to evict from T1 or T2
        let evict_from_t1 = if self.t1.is_empty() {
            false
        } else if self.t2.is_empty() {
            true
        } else {
            // |T1| > p, or (in_b2 and |T1| == p)
            let t1_len = self.t1.len();
            t1_len > self.p || (in_b2 && t1_len == self.p)
        };

        if evict_from_t1 {
            // Evict LRU from T1, add to B1
            self.evict_from_t1()
        } else {
            // Evict LRU from T2, add to B2
            self.evict_from_t2()
        }
    }

    /// Evict the LRU evictable frame from T1.
    fn evict_from_t1(&mut self) -> Option<FrameId> {
        // Find first evictable frame in T1 (from front = LRU)
        let victim_idx = self.t1.iter()
            .position(|&fid| self.evictable.contains(&fid))?;

        let frame_id = self.t1.remove(victim_idx)?;
        let page_id = self.frame_to_page.remove(&frame_id);
        self.frame_location.remove(&frame_id);
        self.evictable.remove(&frame_id);

        // Add to B1 ghost list
        if let Some(pid) = page_id {
            self.b1.push_back(pid);
            self.b1_set.insert(pid);
        }

        Some(frame_id)
    }

    /// Evict the LRU evictable frame from T2.
    fn evict_from_t2(&mut self) -> Option<FrameId> {
        // Find first evictable frame in T2 (from front = LRU)
        let victim_idx = self.t2.iter()
            .position(|&fid| self.evictable.contains(&fid))?;

        let frame_id = self.t2.remove(victim_idx)?;
        let page_id = self.frame_to_page.remove(&frame_id);
        self.frame_location.remove(&frame_id);
        self.evictable.remove(&frame_id);

        // Add to B2 ghost list
        if let Some(pid) = page_id {
            self.b2.push_back(pid);
            self.b2_set.insert(pid);
        }

        Some(frame_id)
    }

    /// Trim ghost lists before inserting a new page (Case IV from the paper).
    ///
    /// The paper's constraints:
    /// - |L1| = |T1| + |B1| ≤ c
    /// - |L1| + |L2| = |T1| + |B1| + |T2| + |B2| ≤ 2c
    ///
    /// Trimming happens once (one entry) before insertion, matching the paper's
    /// Case IV logic. This is much less aggressive than trimming after every
    /// eviction, allowing ghost lists to grow large enough for adaptation.
    fn trim_ghost_before_insert(&mut self) {
        let l1_len = self.t1.len() + self.b1.len();
        let l2_len = self.t2.len() + self.b2.len();

        if l1_len >= self.c {
            // L1 is full — trim B1 to make room (Case IV A)
            if let Some(pid) = self.b1.pop_front() {
                self.b1_set.remove(&pid);
            }
        } else if l1_len + l2_len >= 2 * self.c {
            // Total directory is full — trim B2 (Case IV B)
            if let Some(pid) = self.b2.pop_front() {
                self.b2_set.remove(&pid);
            }
        }
    }
}

impl Default for ArcReplacer {
    fn default() -> Self {
        Self::new(100) // Default capacity, should be set properly
    }
}

impl EvictionPolicy for ArcReplacer {
    fn name(&self) -> &'static str {
        "arc"
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
        // Check if frame already exists in T1 or T2
        if let Some(&location) = self.frame_location.get(&frame_id) {
            self.frame_to_page.insert(frame_id, page_id);

            match location {
                ListLocation::T1 => {
                    // Case I: Hit in T1 → promote to T2 (MRU)
                    self.t1.retain(|&f| f != frame_id);
                    self.t2.push_back(frame_id);
                    self.frame_location.insert(frame_id, ListLocation::T2);
                }
                ListLocation::T2 => {
                    // Case I: Hit in T2 → move to MRU of T2
                    self.t2.retain(|&f| f != frame_id);
                    self.t2.push_back(frame_id);
                }
            }
            return;
        }

        // New frame - check imported scores first
        let imported_score = self.pending_page_scores.remove(&page_id);

        // Check ghost lists
        let b1_hit = self.in_b1(page_id);
        let b2_hit = self.in_b2(page_id);

        if b1_hit {
            // Case II: Page in B1 → adapt p upward, move to T2
            self.remove_from_b1(page_id);

            // Increase p: delta = max(1, |B2| / |B1|)
            let delta = if self.b1.is_empty() {
                1
            } else {
                std::cmp::max(1, self.b2.len() / self.b1.len().max(1))
            };
            self.p = std::cmp::min(self.c, self.p.saturating_add(delta));

            // Add to T2 (MRU)
            self.t2.push_back(frame_id);
            self.frame_location.insert(frame_id, ListLocation::T2);
        } else if b2_hit {
            // Case III: Page in B2 → adapt p downward, move to T2
            self.remove_from_b2(page_id);

            // Decrease p: delta = max(1, |B1| / |B2|)
            let delta = if self.b2.is_empty() {
                1
            } else {
                std::cmp::max(1, self.b1.len() / self.b2.len().max(1))
            };
            self.p = self.p.saturating_sub(delta);

            // Add to T2 (MRU)
            self.t2.push_back(frame_id);
            self.frame_location.insert(frame_id, ListLocation::T2);
        } else if imported_score.map(|s| s > 0).unwrap_or(false) {
            // Imported as hot: goes to T2
            self.t2.push_back(frame_id);
            self.frame_location.insert(frame_id, ListLocation::T2);
        } else {
            // Case IV: New page, not in any list → add to T1
            // Trim ghost lists per paper's Case IV rules before insertion
            self.trim_ghost_before_insert();
            self.t1.push_back(frame_id);
            self.frame_location.insert(frame_id, ListLocation::T1);
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

        // Use REPLACE subroutine
        // We don't have a B2 hit context here, so pass false
        self.replace(false)
    }

    fn remove(&mut self, frame_id: FrameId) {
        self.remove_from_lists(frame_id);
        self.frame_to_page.remove(&frame_id);
        self.evictable.remove(&frame_id);
    }

    fn size(&self) -> usize {
        self.evictable.len()
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());

        let mut score = 1u64;

        // T1 pages: lower scores (recency-based, colder)
        for &frame_id in &self.t1 {
            if let Some(&page_id) = self.frame_to_page.get(&frame_id) {
                state.hot_pages.push((page_id, score));
                score += 1;
            }
        }

        // T2 pages: higher scores (frequency-based, hotter)
        score += 1000; // Gap to separate T1 and T2
        for &frame_id in &self.t2 {
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
    fn test_arc_name() {
        let replacer = ArcReplacer::new(10);
        assert_eq!(replacer.name(), "arc");
    }

    #[test]
    fn test_arc_new_page_goes_to_t1() {
        let mut replacer = ArcReplacer::new(10);

        replacer.record_access(FrameId::new(0), PageId::new(100));

        assert_eq!(replacer.frame_location.get(&FrameId::new(0)), Some(&ListLocation::T1));
        assert_eq!(replacer.t1.len(), 1);
        assert_eq!(replacer.t2.len(), 0);
    }

    #[test]
    fn test_arc_reaccess_promotes_to_t2() {
        let mut replacer = ArcReplacer::new(10);

        // First access: goes to T1
        replacer.record_access(FrameId::new(0), PageId::new(100));
        assert_eq!(replacer.frame_location.get(&FrameId::new(0)), Some(&ListLocation::T1));

        // Second access: promotes to T2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        assert_eq!(replacer.frame_location.get(&FrameId::new(0)), Some(&ListLocation::T2));
        assert_eq!(replacer.t1.len(), 0);
        assert_eq!(replacer.t2.len(), 1);
    }

    #[test]
    fn test_arc_t2_reaccess_moves_to_mru() {
        let mut replacer = ArcReplacer::new(10);

        // Get frames 0, 1, 2 into T2
        for i in 0..3 {
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
        }

        // T2 order: [0, 1, 2]
        assert_eq!(replacer.t2.front(), Some(&FrameId::new(0)));
        assert_eq!(replacer.t2.back(), Some(&FrameId::new(2)));

        // Re-access frame 0: moves to MRU
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // T2 order: [1, 2, 0]
        assert_eq!(replacer.t2.front(), Some(&FrameId::new(1)));
        assert_eq!(replacer.t2.back(), Some(&FrameId::new(0)));
    }

    #[test]
    fn test_arc_b1_hit_increases_p() {
        let mut replacer = ArcReplacer::new(10);

        // Manually populate B1 with a page
        replacer.b1.push_back(PageId::new(100));
        replacer.b1_set.insert(PageId::new(100));

        let initial_p = replacer.p;

        // Access page 100 (B1 hit): should increase p
        replacer.record_access(FrameId::new(0), PageId::new(100));

        assert!(replacer.p > initial_p);
        // Page should go to T2, not T1
        assert_eq!(replacer.frame_location.get(&FrameId::new(0)), Some(&ListLocation::T2));
        // Page should be removed from B1
        assert!(!replacer.b1.contains(&PageId::new(100)));
    }

    #[test]
    fn test_arc_b2_hit_decreases_p() {
        let mut replacer = ArcReplacer::new(10);

        // Set p to a non-zero value first
        replacer.p = 5;

        // Manually populate B2 with a page
        replacer.b2.push_back(PageId::new(100));
        replacer.b2_set.insert(PageId::new(100));

        let initial_p = replacer.p;

        // Access page 100 (B2 hit): should decrease p
        replacer.record_access(FrameId::new(0), PageId::new(100));

        assert!(replacer.p < initial_p);
        // Page should go to T2
        assert_eq!(replacer.frame_location.get(&FrameId::new(0)), Some(&ListLocation::T2));
        // Page should be removed from B2
        assert!(!replacer.b2.contains(&PageId::new(100)));
    }

    #[test]
    fn test_arc_eviction_respects_p() {
        let mut replacer = ArcReplacer::new(10);

        // Add pages to T1
        for i in 0..3 {
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
            replacer.set_evictable(FrameId::new(i), true);
        }

        // Add pages to T2 (via re-access)
        for i in 3..6 {
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
            replacer.set_evictable(FrameId::new(i), true);
        }

        // T1 has 3 frames, T2 has 3 frames
        assert_eq!(replacer.t1.len(), 3);
        assert_eq!(replacer.t2.len(), 3);

        // p = 0 by default, so |T1| > p, should evict from T1
        let evicted = replacer.evict().unwrap();
        assert!(evicted.0 < 3); // Should be from T1 (frames 0, 1, 2)
    }

    #[test]
    fn test_arc_eviction_adds_to_ghost() {
        let mut replacer = ArcReplacer::new(10);

        // Add a page to T1
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.set_evictable(FrameId::new(0), true);

        assert!(replacer.b1.is_empty());

        // Evict from T1
        replacer.evict();

        // Page should be in B1
        assert!(replacer.b1.contains(&PageId::new(100)));
    }

    #[test]
    fn test_arc_eviction_from_t2_adds_to_b2() {
        let mut replacer = ArcReplacer::new(10);

        // Add a page directly to T2 (via double access)
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.set_evictable(FrameId::new(0), true);

        // Set p high so we evict from T2
        replacer.p = 10;

        assert!(replacer.b2.is_empty());

        // Evict
        replacer.evict();

        // Page should be in B2
        assert!(replacer.b2.contains(&PageId::new(100)));
    }

    #[test]
    fn test_arc_skips_pinned_frames() {
        let mut replacer = ArcReplacer::new(10);

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
    fn test_arc_all_pinned_returns_none() {
        let mut replacer = ArcReplacer::new(10);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);

        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_arc_export_state_scores() {
        let mut replacer = ArcReplacer::new(10);

        // Frame 0 in T1
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // Frame 1 in T2 (via double access)
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        let state = replacer.export_state();

        assert_eq!(state.source_policy, "arc");
        assert_eq!(state.hot_pages.len(), 2);

        let score_100 = state.hot_pages.iter()
            .find(|(p, _)| *p == PageId::new(100))
            .map(|(_, s)| *s)
            .unwrap();
        let score_101 = state.hot_pages.iter()
            .find(|(p, _)| *p == PageId::new(101))
            .map(|(_, s)| *s)
            .unwrap();

        // T2 page (101) should have higher score than T1 page (100)
        assert!(score_101 > score_100);
    }

    #[test]
    fn test_arc_import_preserves_hotness() {
        let mut replacer = ArcReplacer::new(10);

        // Import: page 100 is cold (score 0), page 101 is hot (score 1000)
        let mut state = PolicyState::new("lru");
        state.hot_pages.push((PageId::new(100), 0));
        state.hot_pages.push((PageId::new(101), 1000));

        replacer.import_state(&state);

        // Fetch pages
        replacer.record_access(FrameId::new(0), PageId::new(100)); // cold -> T1
        replacer.record_access(FrameId::new(1), PageId::new(101)); // hot -> T2

        // Verify placement
        assert_eq!(replacer.frame_location.get(&FrameId::new(0)), Some(&ListLocation::T1));
        assert_eq!(replacer.frame_location.get(&FrameId::new(1)), Some(&ListLocation::T2));
    }

    #[test]
    fn test_arc_remove() {
        let mut replacer = ArcReplacer::new(10);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        replacer.remove(FrameId::new(1));

        assert_eq!(replacer.size(), 2);
        assert_eq!(replacer.t1.len(), 2);

        // Evict remaining
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
    }

    #[test]
    fn test_arc_adaptation_scenario() {
        let mut replacer = ArcReplacer::new(10);

        // Simulate a workload that should adapt p
        // 1. Add some pages to T1 and evict them to B1
        for i in 0..3 {
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
            replacer.set_evictable(FrameId::new(i), true);
        }

        // Evict all to B1
        while replacer.evict().is_some() {}

        // B1 now has pages 100, 101, 102
        assert_eq!(replacer.b1.len(), 3);
        let initial_p = replacer.p;

        // 2. Re-access a B1 page: should increase p
        replacer.record_access(FrameId::new(5), PageId::new(100));
        assert!(replacer.p > initial_p);

        // 3. Add pages and evict to B2
        replacer.p = 5; // Reset p for the test
        for i in 10..13 {
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32));
            replacer.record_access(FrameId::new(i), PageId::new(100 + i as u32)); // -> T2
            replacer.set_evictable(FrameId::new(i), true);
        }

        // Set p high to force eviction from T2
        replacer.p = 100;
        while replacer.evict().is_some() {}

        // B2 now has pages
        assert!(!replacer.b2.is_empty());
        let p_before_b2_hit = replacer.p;

        // 4. Re-access a B2 page: should decrease p
        let b2_page = *replacer.b2.front().unwrap();
        replacer.record_access(FrameId::new(20), b2_page);
        assert!(replacer.p < p_before_b2_hit);
    }
}
