//! FIFO (First-In-First-Out) replacement policy.
//!
//! Simple placeholder policy for initial BufferPoolManager testing.
//! Will be replaced by LRU/CLOCK/LRU-K/2Q implementations.

use std::collections::{HashSet, VecDeque};

use crate::common::FrameId;

/// A simple FIFO eviction policy.
///
/// Evicts pages in the order they were added to the pool.
/// Pinned pages are skipped during eviction.
pub struct FifoReplacer {
    /// Queue of frame IDs in insertion order (front = oldest).
    queue: VecDeque<FrameId>,

    /// Set for O(1) membership check.
    in_queue: HashSet<FrameId>,

    /// Frames that are currently evictable (pin_count == 0).
    evictable: HashSet<FrameId>,
}

impl FifoReplacer {
    /// Create a new FIFO replacer.
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            in_queue: HashSet::new(),
            evictable: HashSet::new(),
        }
    }

    /// Record that a frame was accessed.
    ///
    /// For FIFO, only adds to queue if not already present.
    pub fn record_access(&mut self, frame_id: FrameId) {
        if !self.in_queue.contains(&frame_id) {
            self.queue.push_back(frame_id);
            self.in_queue.insert(frame_id);
        }
    }

    /// Mark a frame as evictable (pin_count dropped to 0).
    pub fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if evictable {
            self.evictable.insert(frame_id);
        } else {
            self.evictable.remove(&frame_id);
        }
    }

    /// Select a victim frame for eviction.
    ///
    /// Returns the oldest evictable frame, or None if all frames are pinned.
    pub fn evict(&mut self) -> Option<FrameId> {
        while let Some(frame_id) = self.queue.pop_front() {
            self.in_queue.remove(&frame_id);

            if self.evictable.remove(&frame_id) {
                return Some(frame_id);
            }
            // Frame is pinned or removed, skip it
        }
        None
    }

    /// Remove a frame from the replacer entirely.
    ///
    /// Called when a page is deleted from the buffer pool.
    pub fn remove(&mut self, frame_id: FrameId) {
        self.in_queue.remove(&frame_id);
        self.evictable.remove(&frame_id);
        // Note: We don't remove from queue (expensive O(n)).
        // The evict() loop will skip it since it's not in in_queue.
    }

    /// Number of evictable frames.
    pub fn size(&self) -> usize {
        self.evictable.len()
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
        replacer.record_access(FrameId::new(0));
        replacer.record_access(FrameId::new(1));
        replacer.record_access(FrameId::new(2));

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

        replacer.record_access(FrameId::new(0));
        replacer.record_access(FrameId::new(1));
        replacer.record_access(FrameId::new(2));

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

        replacer.record_access(FrameId::new(0));
        replacer.record_access(FrameId::new(1));
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

        replacer.record_access(FrameId::new(0));
        replacer.record_access(FrameId::new(1));
        replacer.record_access(FrameId::new(0)); // Access again - should NOT reorder

        replacer.set_evictable(FrameId::new(0), true);
        replacer.set_evictable(FrameId::new(1), true);

        // FIFO: frame 0 was first, should be evicted first
        assert_eq!(replacer.evict(), Some(FrameId::new(0)));
        assert_eq!(replacer.evict(), Some(FrameId::new(1)));
    }
}