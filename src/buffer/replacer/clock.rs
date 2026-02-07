//! Clock (Second-Chance) page replacement policy.
//!
//! The Clock algorithm is an approximation of LRU that uses a single
//! "reference bit" per frame instead of tracking full access history.
//!
//! How it works:
//! 1. Frames are arranged in a circular buffer with a "clock hand"
//! 2. When a page is accessed, its reference bit is set to 1
//! 3. When evicting, the hand sweeps around:
//!    - If ref_bit=1: clear it (second chance), move to next
//!    - If ref_bit=0 AND evictable: evict this frame
//!
//! This gives recently-accessed pages a "second chance" before eviction.

use std::collections::HashMap;

use crate::common::{FrameId, PageId};

use super::{EvictionPolicy, PolicyState};

/// Entry in the clock buffer tracking a frame's state.
#[derive(Debug, Clone)]
struct ClockEntry {
    frame_id: FrameId,
    page_id: PageId,
    ref_bit: bool,
    evictable: bool,
}

/// Clock (second-chance) replacement policy.
///
/// Approximates LRU using reference bits. When a page is accessed,
/// its ref bit is set. During eviction, the clock hand sweeps around,
/// clearing ref bits until it finds an evictable frame with ref_bit=false.
pub struct ClockReplacer {
    /// Circular buffer of frame entries.
    entries: Vec<ClockEntry>,

    /// Maps FrameId to index in entries vector.
    frame_to_index: HashMap<FrameId, usize>,

    /// Current clock hand position.
    hand: usize,

    /// Number of evictable frames.
    num_evictable: usize,

    /// Imported page scores waiting for frame assignment (for warm swap).
    pending_page_scores: HashMap<PageId, u64>,
}

impl ClockReplacer {
    /// Create a new Clock replacer.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            frame_to_index: HashMap::new(),
            hand: 0,
            num_evictable: 0,
            pending_page_scores: HashMap::new(),
        }
    }

    /// Advance the clock hand, wrapping around if needed.
    fn advance_hand(&mut self) {
        if !self.entries.is_empty() {
            self.hand = (self.hand + 1) % self.entries.len();
        }
    }
}

impl Default for ClockReplacer {
    fn default() -> Self {
        Self::new()
    }
}

impl EvictionPolicy for ClockReplacer {
    fn name(&self) -> &'static str {
        "clock"
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
        if let Some(&index) = self.frame_to_index.get(&frame_id) {
            // Frame exists: set reference bit (second chance on next sweep)
            self.entries[index].ref_bit = true;
            self.entries[index].page_id = page_id;
        } else {
            // New frame: add to clock buffer
            let ref_bit = if let Some(score) = self.pending_page_scores.remove(&page_id) {
                // Imported page: set ref_bit based on score (hot pages get ref_bit=true)
                // Use threshold: score > 0 means it was considered "hot" by previous policy
                score > 0
            } else {
                // New page: start with ref_bit=true (just accessed)
                true
            };

            let entry = ClockEntry {
                frame_id,
                page_id,
                ref_bit,
                evictable: false, // Starts pinned; BPM will call set_evictable
            };

            let index = self.entries.len();
            self.entries.push(entry);
            self.frame_to_index.insert(frame_id, index);
        }
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if let Some(&index) = self.frame_to_index.get(&frame_id) {
            let was_evictable = self.entries[index].evictable;
            self.entries[index].evictable = evictable;

            // Update evictable count
            if evictable && !was_evictable {
                self.num_evictable += 1;
            } else if !evictable && was_evictable {
                self.num_evictable -= 1;
            }
        }
    }

    fn evict(&mut self) -> Option<FrameId> {
        if self.num_evictable == 0 {
            return None;
        }

        // Sweep until we find an evictable frame with ref_bit=false
        // Maximum iterations: 2 * entries.len() (clear all ref bits, then find victim)
        let max_iterations = self.entries.len() * 2;

        for _ in 0..max_iterations {
            if self.entries.is_empty() {
                return None;
            }

            let entry = &mut self.entries[self.hand];

            if entry.evictable {
                if entry.ref_bit {
                    // Second chance: clear ref bit and move on
                    entry.ref_bit = false;
                } else {
                    // Found victim: evictable and ref_bit=false
                    let frame_id = entry.frame_id;

                    // Remove from clock buffer
                    let index = self.hand;
                    self.entries.remove(index);
                    self.frame_to_index.remove(&frame_id);

                    // Update indices for entries after the removed one
                    for entry in &self.entries[index..] {
                        if let Some(idx) = self.frame_to_index.get_mut(&entry.frame_id) {
                            *idx -= 1;
                        }
                    }

                    // Adjust hand if needed
                    if !self.entries.is_empty() {
                        self.hand %= self.entries.len();
                    } else {
                        self.hand = 0;
                    }

                    self.num_evictable -= 1;
                    return Some(frame_id);
                }
            }

            self.advance_hand();
        }

        None // Should not reach here if num_evictable > 0
    }

    fn remove(&mut self, frame_id: FrameId) {
        if let Some(index) = self.frame_to_index.remove(&frame_id) {
            let was_evictable = self.entries[index].evictable;
            self.entries.remove(index);

            if was_evictable {
                self.num_evictable -= 1;
            }

            // Update indices for entries after the removed one
            for entry in &self.entries[index..] {
                if let Some(idx) = self.frame_to_index.get_mut(&entry.frame_id) {
                    *idx -= 1;
                }
            }

            // Adjust hand if needed
            if !self.entries.is_empty() && self.hand >= self.entries.len() {
                self.hand %= self.entries.len();
            } else if self.entries.is_empty() {
                self.hand = 0;
            }
        }
    }

    fn size(&self) -> usize {
        self.num_evictable
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());

        // Export pages with scores based on ref_bit
        // ref_bit=true (hot) gets score 1, ref_bit=false (cold) gets score 0
        for entry in &self.entries {
            let score = if entry.ref_bit { 1 } else { 0 };
            state.hot_pages.push((entry.page_id, score));
        }

        state
    }

    fn import_state(&mut self, state: &PolicyState) {
        // Store imported scores for use when record_access is called
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
    fn test_clock_name() {
        let replacer = ClockReplacer::new();
        assert_eq!(replacer.name(), "clock");
    }

    #[test]
    fn test_clock_basic_eviction_and_second_chance() {
        let mut replacer = ClockReplacer::new();

        // Add frames 0, 1, 2
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        // Mark all evictable
        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        assert_eq!(replacer.size(), 3);

        // All frames have ref_bit=true (just accessed)
        // First eviction: sweep clears all ref bits, then evicts frame 0
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));

        // Re-access frame 1 (sets ref_bit=true again)
        replacer.record_access(FrameId::new(1), PageId::new(101));

        // Next eviction: frame 1 gets second chance, frame 2 is evicted
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));

        // Now only frame 1 left
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_clock_wrap_around_and_all_pinned() {
        let mut replacer = ClockReplacer::new();

        // Add frames
        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        // All pinned (not evictable)
        replacer.set_evictable(FrameId::new(0), false);
        replacer.set_evictable(FrameId::new(1), false);
        replacer.set_evictable(FrameId::new(2), false);

        // Evict should return None
        assert_eq!(replacer.evict(), None);
        assert_eq!(replacer.size(), 0);

        // Unpin frame 2 only
        replacer.set_evictable(FrameId::new(2), true);

        // Should wrap around and find frame 2
        assert_eq!(replacer.evict(), Some(FrameId::new(2)));
    }

    #[test]
    fn test_clock_export_import_roundtrip() {
        let mut replacer1 = ClockReplacer::new();

        // Add frames with different ref_bit states
        replacer1.record_access(FrameId::new(0), PageId::new(100));
        replacer1.record_access(FrameId::new(1), PageId::new(101));
        replacer1.set_evictable(FrameId::new(0), true);
        replacer1.set_evictable(FrameId::new(1), true);

        // Evict once to clear frame 0's ref_bit, then re-add it
        // This creates: frame 0 has ref_bit cleared during sweep
        // Actually let's just export and check the state
        let state = replacer1.export_state();

        assert_eq!(state.source_policy, "clock");
        assert_eq!(state.hot_pages.len(), 2);

        // Both should have ref_bit=true (score=1) since just accessed
        for (_, score) in &state.hot_pages {
            assert_eq!(*score, 1);
        }

        // Import into a new replacer
        let mut replacer2 = ClockReplacer::new();
        replacer2.import_state(&state);

        // Simulate BPM fetching the pages
        replacer2.record_access(FrameId::new(5), PageId::new(100));
        replacer2.record_access(FrameId::new(6), PageId::new(101));
        replacer2.set_evictable(FrameId::new(5), true);
        replacer2.set_evictable(FrameId::new(6), true);

        // Both should have ref_bit=true from import (score > 0)
        // So first evict clears both, second evict gets frame 5
        let first = replacer2.evict();
        assert!(first.is_some());
    }

    #[test]
    fn test_clock_remove() {
        let mut replacer = ClockReplacer::new();

        replacer.record_access(FrameId::new(0), PageId::new(100));
        replacer.record_access(FrameId::new(1), PageId::new(101));
        replacer.record_access(FrameId::new(2), PageId::new(102));

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);
        replacer.set_evictable(FrameId::new(2), true);

        // Remove frame 1
        replacer.remove(FrameId::new(1));

        assert_eq!(replacer.size(), 2);

        // Evict should skip removed frame
        let evicted1 = replacer.evict();
        let evicted2 = replacer.evict();

        // Should get frames 0 and 2 (not 1)
        let evicted_set: std::collections::HashSet<_> =
            [evicted1, evicted2].into_iter().flatten().collect();
        assert!(evicted_set.contains(&FrameId::new(0)));
        assert!(evicted_set.contains(&FrameId::new(2)));
        assert!(!evicted_set.contains(&FrameId::new(1)));
    }
}
