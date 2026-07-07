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

/// Entry in the clock ring tracking a frame's state.
///
/// `prev`/`next` are intrusive circular-ring links (a sole entry links
/// to itself). The ring replaced the original Vec + index-map layout,
/// whose middle removal re-indexed every later entry — O(n) per
/// eviction, quadratic at trace-replay cache sizes (found by the
/// Clock2Q+ reproduction runs at 14K+ frames).
#[derive(Debug, Clone)]
struct ClockEntry {
    page_id: PageId,
    ref_bit: bool,
    evictable: bool,
    prev: FrameId,
    next: FrameId,
}

/// Clock (second-chance) replacement policy.
///
/// Approximates LRU using reference bits. When a page is accessed,
/// its ref bit is set. During eviction, the clock hand sweeps around,
/// clearing ref bits until it finds an evictable frame with ref_bit=false.
pub struct ClockReplacer {
    /// Frame state + ring links, keyed by frame. All ops O(1).
    entries: HashMap<FrameId, ClockEntry>,

    /// The frame the sweep examines next. `None` iff the ring is empty.
    hand: Option<FrameId>,

    /// Most recently inserted frame. New frames join between `tail` and
    /// the oldest frame (`tail.next`), preserving insertion order around
    /// the ring — the position `Vec::push` gave them in the old layout.
    tail: Option<FrameId>,

    /// Number of evictable frames.
    num_evictable: usize,

    /// Imported page scores waiting for frame assignment (for warm swap).
    pending_page_scores: HashMap<PageId, u64>,
}

impl ClockReplacer {
    /// Create a new Clock replacer.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            hand: None,
            tail: None,
            num_evictable: 0,
            pending_page_scores: HashMap::new(),
        }
    }

    /// Unlink `frame_id` from the ring and remove it from the map,
    /// returning its entry. `hand`/`tail` move to the successor /
    /// predecessor respectively if they pointed at the removed frame.
    fn unlink(&mut self, frame_id: FrameId) -> ClockEntry {
        let entry = self
            .entries
            .remove(&frame_id)
            .expect("unlink: frame not tracked");
        if entry.next == frame_id {
            // Sole entry: it must self-link, and the ring becomes empty.
            assert_eq!(entry.prev, frame_id, "sole ring entry must self-link");
            self.hand = None;
            self.tail = None;
        } else {
            self.entries
                .get_mut(&entry.prev)
                .expect("ring: prev of unlinked frame missing")
                .next = entry.next;
            self.entries
                .get_mut(&entry.next)
                .expect("ring: next of unlinked frame missing")
                .prev = entry.prev;
            if self.hand == Some(frame_id) {
                self.hand = Some(entry.next);
            }
            if self.tail == Some(frame_id) {
                self.tail = Some(entry.prev);
            }
        }
        entry
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
        if let Some(entry) = self.entries.get_mut(&frame_id) {
            // Frame exists: set reference bit (second chance on next sweep)
            entry.ref_bit = true;
            entry.page_id = page_id;
        } else {
            // New frame: join the ring between `tail` and the oldest frame
            let ref_bit = if let Some(score) = self.pending_page_scores.remove(&page_id) {
                // Imported page: set ref_bit based on score (hot pages get ref_bit=true)
                // Use threshold: score > 0 means it was considered "hot" by previous policy
                score > 0
            } else {
                // New page: start with ref_bit=true (just accessed)
                true
            };

            let (prev, next) = match self.tail {
                // Splice between the newest and oldest frames.
                Some(tail_id) => (tail_id, self.entries[&tail_id].next),
                // First frame: self-linked ring.
                None => (frame_id, frame_id),
            };
            self.entries.insert(
                frame_id,
                ClockEntry {
                    page_id,
                    ref_bit,
                    evictable: false, // Starts pinned; BPM will call set_evictable
                    prev,
                    next,
                },
            );
            if prev == frame_id {
                // First frame: the hand starts at the oldest frame.
                assert!(self.hand.is_none(), "empty ring must have no hand");
                self.hand = Some(frame_id);
            } else {
                self.entries
                    .get_mut(&prev)
                    .expect("ring: tail missing")
                    .next = frame_id;
                self.entries
                    .get_mut(&next)
                    .expect("ring: head missing")
                    .prev = frame_id;
            }
            self.tail = Some(frame_id);
        }
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if let Some(entry) = self.entries.get_mut(&frame_id) {
            let was_evictable = entry.evictable;
            entry.evictable = evictable;

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

        // Sweep until we find an evictable frame with ref_bit=false.
        // Maximum iterations: 2 * entries.len() — one lap clears every
        // ref bit, the next lap must reach an evictable clear frame.
        let max_iterations = self.entries.len() * 2;

        for _ in 0..max_iterations {
            let hand_id = self
                .hand
                .expect("evict: evictable frames imply non-empty ring");
            let entry = self
                .entries
                .get_mut(&hand_id)
                .expect("evict: hand frame must be tracked");
            let next = entry.next;

            if entry.evictable {
                if entry.ref_bit {
                    // Second chance: clear ref bit and move on
                    entry.ref_bit = false;
                } else {
                    // Found victim: evictable and ref_bit=false.
                    // unlink() leaves the hand on the successor.
                    self.unlink(hand_id);
                    self.num_evictable -= 1;
                    return Some(hand_id);
                }
            }

            self.hand = Some(next);
        }

        None // Unreachable while num_evictable > 0; kept as a bounded-loop backstop
    }

    fn remove(&mut self, frame_id: FrameId) {
        if self.entries.contains_key(&frame_id) {
            // NOTE: the old Vec layout shifted indices on removal, which
            // silently advanced the hand past one surviving frame when
            // the removed entry sat below it. The ring keeps the hand on
            // its current frame unless the removed frame IS the hand —
            // strictly truer clock behavior; no test observes the
            // difference (victim-order tests around remove() are
            // set-based, equivalence suites compare database results).
            let entry = self.unlink(frame_id);
            if entry.evictable {
                self.num_evictable -= 1;
            }
        }
    }

    fn size(&self) -> usize {
        self.num_evictable
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());

        // Export pages with scores based on ref_bit, walking the ring
        // once from the oldest frame (tail.next) in insertion order —
        // the same order the old Vec layout exported.
        // ref_bit=true (hot) gets score 1, ref_bit=false (cold) gets score 0
        if let Some(tail_id) = self.tail {
            let head_id = self.entries[&tail_id].next;
            let mut cursor = head_id;
            for _ in 0..self.entries.len() {
                let entry = &self.entries[&cursor];
                let score = if entry.ref_bit { 1 } else { 0 };
                state.hot_pages.push((entry.page_id, score));
                cursor = entry.next;
            }
            assert_eq!(cursor, head_id, "ring walk must return to the oldest frame");
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
