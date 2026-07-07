//! LRU-K page replacement policy.
//!
//! LRU-K evicts the page with the largest "backward K-distance" — the time
//! since the Kth most recent access. This provides scan resistance: a page
//! accessed only once (during a sequential scan) has infinite K-distance
//! and will be evicted before frequently-accessed pages.
//!
//! ## Algorithm (K=2 example)
//! - Page accessed once: K-distance = +∞ (evict first)
//! - Page accessed twice: K-distance = now - 2nd_most_recent_access
//! - Evict the page with the LARGEST K-distance
//!
//! ## Scoring Convention (consistent across all policies)
//! - Higher score = hotter = evict later
//! - Lower score = colder = evict first
//! - For LRU-K: score = inverse of K-distance
//!   - +∞ distance → score = 0 (coldest)
//!   - Small distance → high score (hottest)
//!
//! ## Reference
//! O'Neil et al., "The LRU-K Page Replacement Algorithm" (1993)

use std::collections::{BTreeSet, HashMap, VecDeque};

use crate::common::{FrameId, PageId};

use super::{EvictionPolicy, PolicyState};

/// Tracks the last K access times for a frame.
///
/// Times are ticks of the replacer's logical `access_clock`, not wall
/// clock: K-distance ORDERING depends only on the order of kth-recent
/// accesses (elapsed time grows uniformly for every frame), so a logical
/// counter preserves the algorithm exactly while removing the
/// `Instant::now` syscall per access and the nondeterministic eviction
/// order that same-nanosecond timestamp ties produced.
#[derive(Debug, Clone)]
struct AccessHistory {
    /// Logical times of recent accesses, newest at back. Max length K.
    timestamps: VecDeque<u64>,

    /// The page currently in this frame.
    page_id: PageId,

    /// Logical time of first access (FIFO tie-break among +∞ frames).
    first_access_seq: u64,
}

impl AccessHistory {
    fn new(page_id: PageId, k: usize, seq: u64) -> Self {
        let mut timestamps = VecDeque::with_capacity(k);
        timestamps.push_back(seq);
        Self {
            timestamps,
            page_id,
            first_access_seq: seq,
        }
    }

    /// Record a new access, keeping only the last K times.
    fn record_access(&mut self, k: usize, seq: u64) {
        self.timestamps.push_back(seq);
        while self.timestamps.len() > k {
            self.timestamps.pop_front();
        }
    }

    /// Logical time of the Kth most recent access.
    /// Returns None if < K accesses (backward K-distance = +∞).
    fn kth_recent_seq(&self, k: usize) -> Option<u64> {
        if self.timestamps.len() < k {
            None // +∞ distance
        } else {
            // Kth most recent is at index len - k
            Some(self.timestamps[self.timestamps.len() - k])
        }
    }

    /// Eviction sort key: the victim is the MINIMUM key.
    ///
    /// Class 0 = +∞ K-distance (evict first), FIFO by first access;
    /// class 1 = finite, oldest kth-recent access first (= largest
    /// backward K-distance). `frame.0` last makes keys unique so a
    /// `BTreeSet` can hold one entry per evictable frame. Keys change
    /// only on `record_access`, so the set stays sorted between updates.
    fn victim_key(&self, k: usize, frame_id: FrameId) -> (u8, u64, usize) {
        match self.kth_recent_seq(k) {
            None => (0, self.first_access_seq, frame_id.0),
            Some(kth_seq) => (1, kth_seq, frame_id.0),
        }
    }

    /// Score for warm swap export.
    /// Higher score = hotter = more recent kth access = evict later.
    fn hotness_score(&self, k: usize) -> u64 {
        match self.kth_recent_seq(k) {
            None => 0, // +∞ distance = coldest
            // +1 keeps every finite-distance frame strictly hotter than
            // the +∞ class (importers treat score > 0 as "hot").
            Some(kth_seq) => kth_seq + 1,
        }
    }
}

/// LRU-K replacement policy.
///
/// Evicts the frame with the largest backward K-distance.
/// Frames with fewer than K accesses have +∞ distance and are evicted first (FIFO).
pub struct LruKReplacer {
    /// The K parameter (number of accesses to track).
    k: usize,

    /// Access history for each frame.
    history: HashMap<FrameId, AccessHistory>,

    /// Evictable frames ordered by `victim_key` — `evict()` pops the
    /// first entry, O(log n). Replaced the old evictable HashSet + full
    /// linear max-scan per eviction (quadratic at trace-replay cache
    /// sizes, found by the Clock2Q+ reproduction runs).
    victim_order: BTreeSet<(u8, u64, usize)>,

    /// Logical access clock; incremented on every `record_access`.
    access_clock: u64,

    /// Imported page scores waiting for frame assignment.
    pending_page_scores: HashMap<PageId, u64>,
}

impl LruKReplacer {
    /// Create a new LRU-K replacer with the given K parameter.
    ///
    /// K=2 is the most common choice (LRU-2), providing good scan resistance
    /// while being simple to understand.
    pub fn new(k: usize) -> Self {
        assert!(k >= 1, "K must be at least 1");
        Self {
            k,
            history: HashMap::new(),
            victim_order: BTreeSet::new(),
            access_clock: 0,
            pending_page_scores: HashMap::new(),
        }
    }

    /// Create a new LRU-2 replacer (the most common configuration).
    pub fn new_lru2() -> Self {
        Self::new(2)
    }

    /// Get the K parameter.
    pub fn k(&self) -> usize {
        self.k
    }
}

impl Default for LruKReplacer {
    fn default() -> Self {
        Self::new_lru2()
    }
}

impl EvictionPolicy for LruKReplacer {
    fn name(&self) -> &'static str {
        "lru-k"
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
        self.access_clock += 1;
        let seq = self.access_clock;

        if let Some(hist) = self.history.get_mut(&frame_id) {
            // Existing frame: its victim key changes, so pull it out of
            // the eviction order (if evictable), update, and re-file.
            let was_evictable = self.victim_order.remove(&hist.victim_key(self.k, frame_id));
            hist.record_access(self.k, seq);
            hist.page_id = page_id;
            if was_evictable {
                let inserted = self.victim_order.insert(hist.victim_key(self.k, frame_id));
                assert!(inserted, "victim keys are unique per frame");
            }
        } else {
            // New frame: check for imported score
            let mut hist = AccessHistory::new(page_id, self.k, seq);

            if let Some(score) = self.pending_page_scores.remove(&page_id) {
                // Seed history based on imported score
                // Higher score = hotter = simulate more recent/frequent access
                if score > 0 {
                    // Add K accesses at the current time to give a finite,
                    // maximally recent K-distance
                    hist.timestamps.clear();
                    for _ in 0..self.k {
                        hist.timestamps.push_back(seq);
                    }
                }
                // If score == 0, leave with single access (+∞ distance)
            }

            self.history.insert(frame_id, hist);
        }
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        // Q-35 contract: `evictable ⊆ tracked` — see FifoReplacer.
        let Some(hist) = self.history.get(&frame_id) else {
            return;
        };
        let key = hist.victim_key(self.k, frame_id);
        if evictable {
            self.victim_order.insert(key); // idempotent re-insert
        } else {
            self.victim_order.remove(&key);
        }
    }

    fn evict(&mut self) -> Option<FrameId> {
        // Victim = minimum key: +∞-distance frames (class 0, FIFO) before
        // finite ones (class 1, oldest kth-recent access first).
        let &(class, seq, frame_index) = self.victim_order.iter().next()?;
        let removed = self.victim_order.remove(&(class, seq, frame_index));
        assert!(removed, "victim key must be present");

        let frame_id = FrameId::new(frame_index);
        let evicted = self.history.remove(&frame_id);
        assert!(evicted.is_some(), "victim frame must have history");
        Some(frame_id)
    }

    fn remove(&mut self, frame_id: FrameId) {
        if let Some(hist) = self.history.remove(&frame_id) {
            self.victim_order.remove(&hist.victim_key(self.k, frame_id));
        }
    }

    fn size(&self) -> usize {
        self.victim_order.len()
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());

        for hist in self.history.values() {
            let score = hist.hotness_score(self.k);
            state.hot_pages.push((hist.page_id, score));
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
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_lru_k_name() {
        let replacer = LruKReplacer::new(2);
        assert_eq!(replacer.name(), "lru-k");
    }

    #[test]
    fn test_lru_k_parameter() {
        let replacer = LruKReplacer::new(3);
        assert_eq!(replacer.k(), 3);

        let replacer2 = LruKReplacer::new_lru2();
        assert_eq!(replacer2.k(), 2);
    }

    #[test]
    fn test_lru_k_infinite_distance_evicted_first() {
        let mut replacer = LruKReplacer::new(2);

        // Frame 0: access twice (finite K-distance)
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // Frame 1: access once (+∞ K-distance)
        replacer.record_access(FrameId::new(1), PageId::new(101));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // Frame 1 has +∞ distance, should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
    }

    #[test]
    fn test_lru_k_fifo_tiebreak_for_infinite() {
        let mut replacer = LruKReplacer::new(2);

        // Both frames have only 1 access (+∞ K-distance)
        // Frame 0 accessed first
        replacer.record_access(FrameId::new(0), PageId::new(100));
        thread::sleep(Duration::from_millis(5));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // Both have +∞ distance, FIFO: frame 0 (older) evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    }

    #[test]
    fn test_lru_k_scan_resistance() {
        let mut replacer = LruKReplacer::new(2);

        // "Hot" page: accessed multiple times
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // "Scan" pages: accessed only once each (simulating sequential scan)
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));
        replacer.record_access(FrameId::new(3), PageId::new(103));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);
        replacer.set_evictable(FrameId::new(3), true);

        // Scan pages should be evicted first (all have +∞ K-distance)
        // Hot page (frame 0) should be evicted last
        let evict1 = replacer.evict().unwrap();
        let evict2 = replacer.evict().unwrap();
        let evict3 = replacer.evict().unwrap();
        let evict4 = replacer.evict().unwrap();

        // Frame 0 should be last (it's the only one with finite K-distance)
        assert_eq!(evict4, FrameId::new(0));

        // Frames 1, 2, 3 should be evicted first (any order due to +∞, but FIFO expected)
        assert!(evict1 != FrameId::new(0));
        assert!(evict2 != FrameId::new(0));
        assert!(evict3 != FrameId::new(0));
    }

    #[test]
    fn test_lru_k_larger_distance_evicted_first() {
        let mut replacer = LruKReplacer::new(2);

        // Frame 0: accessed twice, then wait
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(0), PageId::new(100));

        thread::sleep(Duration::from_millis(10));

        // Frame 1: accessed twice more recently
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // Frame 0 has larger K-distance (older), should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    }

    #[test]
    fn test_lru_k_all_pinned_returns_none() {
        let mut replacer = LruKReplacer::new(2);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        // All pinned
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);

        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_lru_k_export_state_scores() {
        let mut replacer = LruKReplacer::new(2);

        // Frame 0: only 1 access (+∞ distance = score 0)
        replacer.record_access(FrameId::new(0), PageId::new(100));

        // Frame 1: 2 accesses (finite distance = positive score)
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(1), PageId::new(101));

        let state = replacer.export_state();

        assert_eq!(state.source_policy, "lru-k");
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

        // Page 100 has +∞ distance (score = 0)
        // Page 101 has finite distance (score > 0)
        assert_eq!(score_100, 0);
        assert!(score_101 > 0);
    }

    #[test]
    fn test_lru_k_import_preserves_hotness() {
        let mut replacer = LruKReplacer::new(2);

        // Import: page 100 is cold (score 0), page 101 is hot (score 1000)
        let mut state = PolicyState::new("lru");
        state.hot_pages.push((PageId::new(100), 0));
        state.hot_pages.push((PageId::new(101), 1000));

        replacer.import_state(&state);

        // Simulate BPM fetching pages
        replacer.record_access(FrameId::new(0), PageId::new(100)); // cold
        replacer.record_access(FrameId::new(1), PageId::new(101)); // hot

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // Cold page (score 0 = +∞ distance) should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    }

    #[test]
    fn test_lru_k_remove() {
        let mut replacer = LruKReplacer::new(2);

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        replacer.remove(FrameId::new(1));

        assert_eq!(replacer.size(), 2);

        // Evict remaining frames
        let evict1 = replacer.evict().unwrap();
        let evict2 = replacer.evict().unwrap();

        assert!(evict1 == FrameId::new(0) || evict1 == FrameId::new(2));
        assert!(evict2 == FrameId::new(0) || evict2 == FrameId::new(2));
        assert_ne!(evict1, evict2);
    }
}
