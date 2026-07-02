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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;

use crate::common::{FrameId, PageId};

use super::{EvictionPolicy, PolicyState};

/// LRU-ordered list with O(log n) mid-list removal — sequence-numbered
/// `BTreeMap` (order) + `HashMap` (membership).
///
/// T16.5 tuning fix: the previous `VecDeque` representation made ARC's
/// cache-HIT path (`record_access` move-to-MRU) an O(pool_size)
/// `retain` scan inside the global replacer mutex — the top user-code
/// symbol in the TPC-C profile, with the other terminals parked on the
/// mutex behind it. Iteration order (front = LRU, back = MRU) and every
/// ARC semantic are unchanged; only the complexity class is.
/// NOTE (perf): frames are dense, so T1/T2 could go O(1) via a
/// slab-backed intrusive list; ghosts are sparse PageIds and could not.
/// One uniform O(log n) structure is the KISS choice until a profile
/// says the BTreeMap constants matter.
struct OrderedList<K> {
    /// Insertion order: ascending seq = LRU → MRU.
    by_seq: BTreeMap<u64, K>,
    /// Membership + reverse index for O(log n) removal.
    seq_of: HashMap<K, u64>,
    /// Monotonic per-list counter; u64 cannot realistically wrap.
    next_seq: u64,
}

impl<K: Copy + Eq + Hash> OrderedList<K> {
    fn new() -> Self {
        Self {
            by_seq: BTreeMap::new(),
            seq_of: HashMap::new(),
            next_seq: 0,
        }
    }

    /// Append at the MRU end. A key already present moves to MRU.
    fn push_back(&mut self, key: K) {
        if let Some(seq) = self.seq_of.remove(&key) {
            self.by_seq.remove(&seq);
        }
        self.by_seq.insert(self.next_seq, key);
        self.seq_of.insert(key, self.next_seq);
        self.next_seq += 1;
    }

    /// Remove `key` if present; true when it was.
    fn remove(&mut self, key: &K) -> bool {
        match self.seq_of.remove(key) {
            Some(seq) => {
                self.by_seq.remove(&seq);
                true
            }
            None => false,
        }
    }

    /// Remove and return the LRU entry.
    fn pop_front(&mut self) -> Option<K> {
        let (_, key) = self.by_seq.pop_first()?;
        self.seq_of.remove(&key);
        Some(key)
    }

    fn contains(&self, key: &K) -> bool {
        self.seq_of.contains_key(key)
    }

    fn len(&self) -> usize {
        self.seq_of.len()
    }

    fn is_empty(&self) -> bool {
        self.seq_of.is_empty()
    }

    /// LRU → MRU iteration.
    fn iter(&self) -> impl Iterator<Item = &K> {
        self.by_seq.values()
    }

    /// The LRU entry, if any. Test-only observer.
    #[cfg(test)]
    fn front(&self) -> Option<&K> {
        self.by_seq.first_key_value().map(|(_, k)| k)
    }

    /// The MRU entry, if any. Test-only observer.
    #[cfg(test)]
    fn back(&self) -> Option<&K> {
        self.by_seq.last_key_value().map(|(_, k)| k)
    }
}

/// ARC (Adaptive Replacement Cache) replacement policy.
///
/// Self-tuning cache that balances between recency and frequency
/// based on observed workload patterns.
pub struct ArcReplacer {
    /// T1: LRU list for pages seen once recently.
    /// Front = LRU (evict first), Back = MRU.
    t1: OrderedList<FrameId>,

    /// T2: LRU list for pages seen at least twice.
    /// Front = LRU (evict first), Back = MRU.
    t2: OrderedList<FrameId>,

    /// B1: Ghost list for T1 (recently evicted PageIds from T1).
    /// Membership is intrinsic to `OrderedList` — no companion set.
    b1: OrderedList<PageId>,

    /// B2: Ghost list for T2 (recently evicted PageIds from T2).
    b2: OrderedList<PageId>,

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

    /// Tracks whether evict_for_page already adapted p for a ghost hit.
    /// Consumed by record_access to avoid double-adaptation.
    pending_adaptation: PendingAdaptation,
}

/// Tracks which list a frame belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListLocation {
    T1,
    T2,
}

/// Tracks whether evict_for_page already adapted p for a ghost hit.
///
/// When evict_for_page detects that the incoming page is in B1 or B2,
/// it adapts p immediately (matching the paper's order: adapt → REPLACE).
/// record_access then checks this to avoid double-adaptation.
/// The PageId ensures stale state from a different page is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingAdaptation {
    /// No pending adaptation.
    None,
    /// p was already adapted for this page in evict_for_page.
    Done(PageId),
}

impl ArcReplacer {
    /// Create a new ARC replacer with the given cache capacity.
    ///
    /// `capacity` is the number of frames in the buffer pool.
    pub fn new(capacity: usize) -> Self {
        Self {
            t1: OrderedList::new(),
            t2: OrderedList::new(),
            b1: OrderedList::new(),
            b2: OrderedList::new(),
            p: 0,
            c: capacity,
            frame_location: HashMap::new(),
            frame_to_page: HashMap::new(),
            evictable: HashSet::new(),
            pending_page_scores: HashMap::new(),
            pending_adaptation: PendingAdaptation::None,
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
        self.b1.contains(&page_id)
    }

    /// Check if a PageId is in B2 ghost list (O(1)).
    fn in_b2(&self, page_id: PageId) -> bool {
        self.b2.contains(&page_id)
    }

    /// Remove a frame from T1 or T2 — O(log n), not a list scan.
    fn remove_from_lists(&mut self, frame_id: FrameId) {
        if let Some(location) = self.frame_location.remove(&frame_id) {
            match location {
                ListLocation::T1 => {
                    self.t1.remove(&frame_id);
                }
                ListLocation::T2 => {
                    self.t2.remove(&frame_id);
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
            // Evict LRU from T1, add to B1.
            // Fall back to T2 if every T1 frame is currently pinned: the BPM
            // contract is "find an evictable frame if one exists" — refusing
            // to evict because of policy preference would raise NoFreeFrames
            // even though T2 has unpinned frames available.
            self.evict_from_t1().or_else(|| self.evict_from_t2())
        } else {
            // Evict LRU from T2, add to B2. Symmetric fallback to T1.
            self.evict_from_t2().or_else(|| self.evict_from_t1())
        }
    }

    /// Evict the LRU evictable frame from T1.
    fn evict_from_t1(&mut self) -> Option<FrameId> {
        // First evictable frame from the LRU end. The scan is O(pinned
        // prefix), as before; the removal is O(log n) instead of an O(n)
        // element shift.
        let frame_id = *self.t1.iter().find(|fid| self.evictable.contains(fid))?;

        self.t1.remove(&frame_id);
        let page_id = self.frame_to_page.remove(&frame_id);
        self.frame_location.remove(&frame_id);
        self.evictable.remove(&frame_id);

        // Add to B1 ghost list
        if let Some(pid) = page_id {
            self.b1.push_back(pid);
        }

        Some(frame_id)
    }

    /// Evict the LRU evictable frame from T2.
    fn evict_from_t2(&mut self) -> Option<FrameId> {
        let frame_id = *self.t2.iter().find(|fid| self.evictable.contains(fid))?;

        self.t2.remove(&frame_id);
        let page_id = self.frame_to_page.remove(&frame_id);
        self.frame_location.remove(&frame_id);
        self.evictable.remove(&frame_id);

        // Add to B2 ghost list
        if let Some(pid) = page_id {
            self.b2.push_back(pid);
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
            self.b1.pop_front();
        } else if l1_len + l2_len >= 2 * self.c {
            // Total directory is full — trim B2 (Case IV B)
            self.b2.pop_front();
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
                    // Case I: Hit in T1 → promote to T2 (MRU). O(log n) —
                    // this and the T2 arm are THE hot path (every cache
                    // hit lands here under the replacer mutex).
                    self.t1.remove(&frame_id);
                    self.t2.push_back(frame_id);
                    self.frame_location.insert(frame_id, ListLocation::T2);
                }
                ListLocation::T2 => {
                    // Case I: Hit in T2 → move to MRU of T2 (push_back on
                    // a present key re-sequences it to MRU).
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
            // Case II: Page in B1 → adapt p upward, move to T2.
            let already_adapted = matches!(
                self.pending_adaptation,
                PendingAdaptation::Done(pid) if pid == page_id
            );
            if already_adapted {
                // evict_for_page already adapted p with correct ghost sizes.
                self.pending_adaptation = PendingAdaptation::None;
            } else {
                // Fallback (evict() was used). Compute delta BEFORE removal
                // so |B1| reflects the original size (paper's order).
                let delta = std::cmp::max(1, self.b2.len() / self.b1.len().max(1));
                self.p = std::cmp::min(self.c, self.p.saturating_add(delta));
            }

            // Remove from B1 AFTER delta computation (paper: "move x from B1 to T2").
            self.b1.remove(&page_id);

            // Add to T2 (MRU)
            self.t2.push_back(frame_id);
            self.frame_location.insert(frame_id, ListLocation::T2);
        } else if b2_hit {
            // Case III: Page in B2 → adapt p downward, move to T2.
            let already_adapted = matches!(
                self.pending_adaptation,
                PendingAdaptation::Done(pid) if pid == page_id
            );
            if already_adapted {
                self.pending_adaptation = PendingAdaptation::None;
            } else {
                // Fallback: compute delta BEFORE removal (paper's order).
                let delta = std::cmp::max(1, self.b1.len() / self.b2.len().max(1));
                self.p = self.p.saturating_sub(delta);
            }

            // Remove from B2 AFTER delta computation (paper: "move x from B2 to T2").
            self.b2.remove(&page_id);

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

        // Fallback: no incoming page context, so in_b2=false and no p adaptation.
        // Callers that know the incoming page should use evict_for_page instead.
        self.pending_adaptation = PendingAdaptation::None;
        self.replace(false)
    }

    fn evict_for_page(&mut self, incoming_page: PageId) -> Option<FrameId> {
        if self.evictable.is_empty() {
            return None;
        }

        // Paper's order: check ghost lists → adapt p → REPLACE(x, p).
        // Delta uses original ghost sizes (x still in B1/B2).
        let in_b1 = self.in_b1(incoming_page);
        let in_b2 = self.in_b2(incoming_page);

        if in_b1 {
            // Case II adaptation: increase p (favor recency).
            let delta = std::cmp::max(1, self.b2.len() / self.b1.len().max(1));
            self.p = std::cmp::min(self.c, self.p.saturating_add(delta));
            self.pending_adaptation = PendingAdaptation::Done(incoming_page);
        } else if in_b2 {
            // Case III adaptation: decrease p (favor frequency).
            let delta = std::cmp::max(1, self.b1.len() / self.b2.len().max(1));
            self.p = self.p.saturating_sub(delta);
            self.pending_adaptation = PendingAdaptation::Done(incoming_page);
        } else {
            self.pending_adaptation = PendingAdaptation::None;
        }

        // REPLACE with updated p and correct in_b2 flag.
        self.replace(in_b2)
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
        for &frame_id in self.t1.iter() {
            if let Some(&page_id) = self.frame_to_page.get(&frame_id) {
                state.hot_pages.push((page_id, score));
                score += 1;
            }
        }

        // T2 pages: higher scores (frequency-based, hotter)
        score += 1000; // Gap to separate T1 and T2
        for &frame_id in self.t2.iter() {
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

        assert_eq!(
            replacer.frame_location.get(&FrameId::new(0)),
            Some(&ListLocation::T1)
        );
        assert_eq!(replacer.t1.len(), 1);
        assert_eq!(replacer.t2.len(), 0);
    }

    #[test]
    fn test_arc_reaccess_promotes_to_t2() {
        let mut replacer = ArcReplacer::new(10);

        // First access: goes to T1
        replacer.record_access(FrameId::new(0), PageId::new(100));
        assert_eq!(
            replacer.frame_location.get(&FrameId::new(0)),
            Some(&ListLocation::T1)
        );

        // Second access: promotes to T2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        assert_eq!(
            replacer.frame_location.get(&FrameId::new(0)),
            Some(&ListLocation::T2)
        );
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

        let initial_p = replacer.p;

        // Access page 100 (B1 hit): should increase p
        replacer.record_access(FrameId::new(0), PageId::new(100));

        assert!(replacer.p > initial_p);
        // Page should go to T2, not T1
        assert_eq!(
            replacer.frame_location.get(&FrameId::new(0)),
            Some(&ListLocation::T2)
        );
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

        let initial_p = replacer.p;

        // Access page 100 (B2 hit): should decrease p
        replacer.record_access(FrameId::new(0), PageId::new(100));

        assert!(replacer.p < initial_p);
        // Page should go to T2
        assert_eq!(
            replacer.frame_location.get(&FrameId::new(0)),
            Some(&ListLocation::T2)
        );
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
        assert_eq!(
            replacer.frame_location.get(&FrameId::new(0)),
            Some(&ListLocation::T1)
        );
        assert_eq!(
            replacer.frame_location.get(&FrameId::new(1)),
            Some(&ListLocation::T2)
        );
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

    /// Regression test for the "preferred list fully pinned" bug.
    ///
    /// The bug: `replace()` picks T1 or T2 by length-vs-p, then bails if the
    /// chosen list has no evictable frame — even when the *other* list does.
    /// FIFO/LRU/LRU-K don't have this shape (single search structure); 2Q
    /// already falls back. ARC alone needed the fallback.
    ///
    /// Surfaced under range_scan benchmarking with cache=64 frames. T1 holds
    /// freshly-fetched scan leaves; T2 holds hot internal nodes. p stays at 0
    /// (no B1 ghost hits in a forward scan), so `replace` always picks T1.
    /// When the iterator's descent transiently pins enough T1 frames to leave
    /// no evictable T1 candidate, the BPM raised `NoFreeFrames` even though
    /// T2 had unpinned internal-node frames available.
    #[test]
    fn test_arc_falls_back_when_chosen_list_pinned() {
        let mut replacer = ArcReplacer::new(4);

        // T1: two pinned frames (frames 0, 1 holding fresh pages).
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);

        // T2: two evictable frames (double-access promotes T1 → T2).
        replacer.record_access(FrameId::new(2), PageId::new(102));
        replacer.record_access(FrameId::new(2), PageId::new(102));
        replacer.record_access(FrameId::new(3), PageId::new(103));
        replacer.record_access(FrameId::new(3), PageId::new(103));
        replacer.set_evictable(FrameId::new(2), true);
        replacer.set_evictable(FrameId::new(3), true);

        // Confirm the test setup: T1 has 2 entries (both pinned), T2 has 2
        // (both evictable), p = 0.
        assert_eq!(replacer.t1.len(), 2);
        assert_eq!(replacer.t2.len(), 2);
        assert_eq!(replacer.p, 0);
        assert_eq!(replacer.size(), 2); // only T2 frames are evictable

        // With p=0 and t1_len(2) > p(0), `replace` picks T1. T1 has no
        // evictable frame. Without the fallback, this returns None and the
        // BPM panics with NoFreeFrames. With the fallback, it must return
        // an evictable frame from T2.
        let victim = replacer.evict();
        assert!(
            victim.is_some(),
            "ARC must fall back to T2 when its preferred list (T1) has no \
             evictable frame, even though T2 has evictable candidates"
        );
        let victim_id = victim.unwrap();
        assert!(
            victim_id == FrameId::new(2) || victim_id == FrameId::new(3),
            "fallback victim must come from T2 (frames 2 or 3), got {victim_id:?}"
        );
    }

    /// Symmetric case: when `replace` picks T2 (e.g., t1_len <= p) but every
    /// T2 frame is pinned, it must fall back to T1.
    #[test]
    fn test_arc_falls_back_t2_to_t1_when_t2_pinned() {
        let mut replacer = ArcReplacer::new(4);

        // T1: two evictable frames.
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // T2: two pinned frames.
        replacer.record_access(FrameId::new(2), PageId::new(102));
        replacer.record_access(FrameId::new(2), PageId::new(102));
        replacer.record_access(FrameId::new(3), PageId::new(103));
        replacer.record_access(FrameId::new(3), PageId::new(103));
        replacer.set_evictable(FrameId::new(2), false);
        replacer.set_evictable(FrameId::new(3), false);

        // Force `replace` to pick T2 by setting p large enough that
        // t1_len(2) <= p.
        replacer.p = 4;

        let victim = replacer.evict();
        assert!(
            victim.is_some(),
            "ARC must fall back to T1 when its preferred list (T2) has no \
             evictable frame"
        );
        let victim_id = victim.unwrap();
        assert!(
            victim_id == FrameId::new(0) || victim_id == FrameId::new(1),
            "fallback victim must come from T1 (frames 0 or 1), got {victim_id:?}"
        );
    }
}
