//! Frame - a slot in the buffer pool.
//!
//! A [`Frame`] holds a [`Page`] plus metadata needed for buffer management:
//! - Which page is loaded (if any)
//! - Pin count for reference counting
//! - Dirty flag for write-back tracking

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::common::PageId;
use crate::storage::page::Page;

/// A frame in the buffer pool.
///
/// Frames are the "slots" in the buffer pool. Each frame can hold one page.
/// The buffer pool has a fixed number of frames allocated at startup.
///
/// # Thread Safety
/// All fields use interior mutability for safe concurrent access:
/// - `page`: `RwLock` for read/write synchronization
/// - `page_id`: `Mutex` for safe updates
/// - `pin_count`: `AtomicU32` for lock-free reference counting
/// - `is_dirty`: `AtomicBool` for lock-free dirty tracking
pub struct Frame {
    /// The page data, protected by RwLock.
    page: RwLock<Page>,

    /// Which page is currently loaded, or None if frame is empty.
    page_id: Mutex<Option<PageId>>,

    /// Number of active references to this frame.
    pin_count: AtomicU32,

    /// Whether the page has been modified since loading.
    is_dirty: AtomicBool,
}

impl Frame {
    /// Create a new empty frame.
    pub fn new() -> Self {
        Self {
            page: RwLock::new(Page::new()),
            page_id: Mutex::new(None),
            pin_count: AtomicU32::new(0),
            is_dirty: AtomicBool::new(false),
        }
    }

    // ========================================================================
    // Page access (RwLock)
    // ========================================================================

    /// Acquire read lock on the page.
    #[inline]
    pub fn page(&self) -> RwLockReadGuard<'_, Page> {
        self.page.read()
    }

    /// Acquire write lock on the page.
    #[inline]
    pub fn page_mut(&self) -> RwLockWriteGuard<'_, Page> {
        self.page.write()
    }

    // ========================================================================
    // Page ID management (Mutex for interior mutability)
    // ========================================================================

    /// Get the page ID of the loaded page.
    #[inline]
    pub fn page_id(&self) -> Option<PageId> {
        *self.page_id.lock()
    }

    /// Set the page ID.
    #[inline]
    pub fn set_page_id(&self, page_id: Option<PageId>) {
        *self.page_id.lock() = page_id;
    }

    // ========================================================================
    // Pin count operations (Atomic)
    // ========================================================================

    /// Increment the pin count. Returns the new pin count.
    #[inline]
    pub fn pin(&self) -> u32 {
        self.pin_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Decrement the pin count. Returns the new pin count.
    ///
    /// # Panics
    /// Panics if pin count is already 0.
    #[inline]
    pub fn unpin(&self) -> u32 {
        let old = self.pin_count.fetch_sub(1, Ordering::Relaxed);
        assert!(old > 0, "pin count underflow");
        old - 1
    }

    /// Get the current pin count.
    #[inline]
    pub fn pin_count(&self) -> u32 {
        self.pin_count.load(Ordering::Relaxed)
    }

    /// Check if the frame is currently pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.pin_count() > 0
    }

    // ========================================================================
    // Dirty flag operations (Atomic)
    // ========================================================================

    /// Mark the frame as dirty (modified).
    #[inline]
    pub fn mark_dirty(&self) {
        self.is_dirty.store(true, Ordering::Relaxed);
    }

    /// Clear the dirty flag.
    #[inline]
    pub fn clear_dirty(&self) {
        self.is_dirty.store(false, Ordering::Relaxed);
    }

    /// Check if the frame is dirty.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Relaxed)
    }

    // ========================================================================
    // Frame state queries
    // ========================================================================

    /// Check if the frame is empty (no page loaded).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.page_id().is_none()
    }

    /// Check if the frame can be evicted.
    #[inline]
    pub fn is_evictable(&self) -> bool {
        self.page_id().is_some() && !self.is_pinned()
    }

    /// Reset the frame to empty state.
    ///
    /// Called after eviction to prepare for reuse.
    pub fn reset(&self) {
        self.page_mut().reset();
        self.set_page_id(None);
        self.pin_count.store(0, Ordering::Relaxed);
        self.is_dirty.store(false, Ordering::Relaxed);
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_new() {
        let frame = Frame::new();
        assert!(frame.is_empty());
        assert!(!frame.is_pinned());
        assert!(!frame.is_dirty());
        assert_eq!(frame.pin_count(), 0);
        assert_eq!(frame.page_id(), None);
    }

    #[test]
    fn test_frame_pin_unpin() {
        let frame = Frame::new();

        assert_eq!(frame.pin(), 1);
        assert!(frame.is_pinned());

        assert_eq!(frame.pin(), 2);
        assert_eq!(frame.pin_count(), 2);

        assert_eq!(frame.unpin(), 1);
        assert!(frame.is_pinned());

        assert_eq!(frame.unpin(), 0);
        assert!(!frame.is_pinned());
    }

    #[test]
    #[should_panic(expected = "pin count underflow")]
    fn test_frame_unpin_underflow() {
        let frame = Frame::new();
        frame.unpin();
    }

    #[test]
    fn test_frame_dirty_flag() {
        let frame = Frame::new();
        assert!(!frame.is_dirty());

        frame.mark_dirty();
        assert!(frame.is_dirty());

        frame.clear_dirty();
        assert!(!frame.is_dirty());
    }

    #[test]
    fn test_frame_page_access() {
        let frame = Frame::new();

        // Write through write lock
        frame.page_mut().as_mut_slice()[0] = 0xAB;

        // Read through read lock
        assert_eq!(frame.page().as_slice()[0], 0xAB);
    }

    #[test]
    fn test_frame_page_id() {
        let frame = Frame::new();
        assert_eq!(frame.page_id(), None);
        assert!(frame.is_empty());

        frame.set_page_id(Some(PageId::new(42)));
        assert_eq!(frame.page_id(), Some(PageId::new(42)));
        assert!(!frame.is_empty());
    }

    #[test]
    fn test_frame_evictable() {
        let frame = Frame::new();

        // Empty frame is not evictable
        assert!(!frame.is_evictable());

        // Load a page
        frame.set_page_id(Some(PageId::new(1)));
        assert!(frame.is_evictable());

        // Pin it
        frame.pin();
        assert!(!frame.is_evictable());

        // Unpin it
        frame.unpin();
        assert!(frame.is_evictable());
    }

    #[test]
    fn test_frame_reset() {
        let frame = Frame::new();

        frame.set_page_id(Some(PageId::new(99)));
        frame.pin();
        frame.mark_dirty();
        frame.page_mut().as_mut_slice()[100] = 0xFF;

        frame.reset();

        assert!(frame.is_empty());
        assert!(!frame.is_pinned());
        assert!(!frame.is_dirty());
        assert_eq!(frame.page().as_slice()[100], 0);
    }

    #[test]
    fn test_frame_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        let frame = Arc::new(Frame::new());
        frame.page_mut().as_mut_slice()[0] = 0x42;

        let mut handles = vec![];

        for _ in 0..10 {
            let frame_clone = Arc::clone(&frame);
            handles.push(thread::spawn(move || {
                let page = frame_clone.page();
                assert_eq!(page.as_slice()[0], 0x42);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_frame_concurrent_pin() {
        use std::sync::Arc;
        use std::thread;

        let frame = Arc::new(Frame::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let frame_clone = Arc::clone(&frame);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    frame_clone.pin();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(frame.pin_count(), 1000);
    }
}