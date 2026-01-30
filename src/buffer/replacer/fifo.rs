//! FIFO (First-In-First-Out) page replacement policy.
//!
//! This is a simple baseline eviction policy where pages are evicted
//! in the order they were first accessed.

use std::collections::{HashSet, VecDeque};

use crate::common::{FrameId, PageId};

/// FIFO replacement policy.
///
/// Evicts pages in the order they were first brought into the buffer pool.
/// Once a frame is in the queue, re-accessing it does NOT move it to the back.
pub struct FifoReplacer {
    /// Queue of frame IDs in access order (front = oldest).
    queue: VecDeque<FrameId>,

    /// Set of frame IDs currently in the queue (for O(1) membership check).
    in_queue: HashSet<FrameId>,

    /// Set of frame IDs that are evictable (pin_count == 0).
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
    /// For FIFO, we only add the frame to the queue on first access.
    /// Subsequent accesses do NOT reorder the frame.
    ///
    /// # Arguments
    /// * `frame_id` - The frame that was accessed
    /// * `_page_id` - The page ID (unused for FIFO, but needed for LRU-K)
    pub fn record_access(&mut self, frame_id: FrameId, _page_id: PageId) {
        if !self.in_queue.contains(&frame_id) {
            self.queue.push_back(frame_id);
            self.in_queue.insert(frame_id);
        }
    }

    /// Set whether a frame is evictable.
    ///
    /// A frame is evictable when its pin_count reaches 0.
    pub fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if evictable {
            self.evictable.insert(frame_id);
        } else {
            self.evictable.remove(&frame_id);
        }
    }

    /// Evict the oldest evictable frame.
    ///
    /// Returns the frame ID of the evicted frame, or None if no frames are evictable.
    ///
    /// **Important**: This implementation does NOT remove non-evictable frames from
    /// the queue. It finds the first evictable frame and only removes that one.
    pub fn evict(&mut self) -> Option<FrameId> {
        // Find the position of the first evictable frame
        let position = self.queue.iter()
            .position(|&fid| self.evictable.contains(&fid))?;

        // Remove only that frame
        let frame_id = self.queue.remove(position)?;
        self.in_queue.remove(&frame_id);
        self.evictable.remove(&frame_id);

        Some(frame_id)
    }

    /// Remove a frame from the replacer entirely.
    ///
    /// Called when a page is deleted from the buffer pool.
    pub fn remove(&mut self, frame_id: FrameId) {
        // Remove from evictable set
        self.evictable.remove(&frame_id);

        // Remove from queue tracking
        if self.in_queue.remove(&frame_id) {
            // Also remove from actual queue
            self.queue.retain(|&fid| fid != frame_id);
        }
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
}
