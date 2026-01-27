//! RAII guards for page access.
//!
//! These guards provide safe access to pages in the buffer pool:
//! - [`PageReadGuard`] - Shared read access (multiple allowed)
//! - [`PageWriteGuard`] - Exclusive write access (auto-marks dirty)
//!
//! Both guards auto-unpin the page when dropped. The `drop_guard()` method
//! allows explicit early release and is safe to call multiple times.

use std::ops::{Deref, DerefMut};

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use crate::common::{FrameId, PageId};
use crate::storage::page::Page;

use super::buffer_pool_manager::BufferPoolManager;

/// Guard for read-only page access.
///
/// Multiple `PageReadGuard`s can exist for the same page simultaneously.
/// The page is automatically unpinned when the guard is dropped.
///
/// # Example
/// ```ignore
/// let guard = bpm.fetch_page_read(page_id)?;
/// let data = guard.as_slice();
///
/// // Explicit early release (optional, matches BusTub's Drop())
/// guard.drop_guard();
/// ```
pub struct PageReadGuard<'a> {
    /// Reference back to BPM for unpin on drop.
    bpm: &'a BufferPoolManager,
    /// Frame holding this page.
    frame_id: FrameId,
    /// Page ID for convenience.
    page_id: PageId,
    /// Lock guard providing access to page data.
    /// Option allows take() for explicit drop.
    lock: Option<RwLockReadGuard<'a, Page>>,
    /// Whether this guard has been dropped.
    dropped: bool,
}

impl<'a> PageReadGuard<'a> {
    /// Create a new read guard.
    pub(crate) fn new(
        bpm: &'a BufferPoolManager,
        frame_id: FrameId,
        page_id: PageId,
        lock: RwLockReadGuard<'a, Page>,
    ) -> Self {
        Self {
            bpm,
            frame_id,
            page_id,
            lock: Some(lock),
            dropped: false,
        }
    }

    /// Get the page ID.
    #[inline]
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get the frame ID.
    #[inline]
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Check if this guard has been dropped.
    #[inline]
    pub fn is_dropped(&self) -> bool {
        self.dropped
    }

    /// Explicitly drop the guard, releasing the lock and unpinning the page.
    ///
    /// Safe to call multiple times - subsequent calls are no-ops.
    /// Matches BusTub's `Drop()` method on page guards.
    pub fn drop_guard(&mut self) {
        if !self.dropped {
            self.dropped = true;
            self.lock.take(); // Release the lock first
            self.bpm.unpin_page_internal(self.frame_id, false);
        }
    }
}

impl Deref for PageReadGuard<'_> {
    type Target = Page;

    #[inline]
    fn deref(&self) -> &Page {
        self.lock
            .as_ref()
            .expect("PageReadGuard used after drop_guard()")
    }
}

impl Drop for PageReadGuard<'_> {
    fn drop(&mut self) {
        self.drop_guard();
    }
}

/// Guard for exclusive write access to a page.
///
/// Only one `PageWriteGuard` can exist for a page at a time.
/// The page is automatically marked dirty and unpinned when the guard is dropped.
///
/// # Example
/// ```ignore
/// let mut guard = bpm.fetch_page_write(page_id)?;
/// guard.as_mut_slice()[0] = 0xFF;
///
/// // Explicit early release (optional)
/// guard.drop_guard();
/// ```
pub struct PageWriteGuard<'a> {
    /// Reference back to BPM for unpin on drop.
    bpm: &'a BufferPoolManager,
    /// Frame holding this page.
    frame_id: FrameId,
    /// Page ID for convenience.
    page_id: PageId,
    /// Lock guard providing access to page data.
    /// Option allows take() for explicit drop.
    lock: Option<RwLockWriteGuard<'a, Page>>,
    /// Whether this guard has been dropped.
    dropped: bool,
}

impl<'a> PageWriteGuard<'a> {
    /// Create a new write guard.
    pub(crate) fn new(
        bpm: &'a BufferPoolManager,
        frame_id: FrameId,
        page_id: PageId,
        lock: RwLockWriteGuard<'a, Page>,
    ) -> Self {
        Self {
            bpm,
            frame_id,
            page_id,
            lock: Some(lock),
            dropped: false,
        }
    }

    /// Get the page ID.
    #[inline]
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get the frame ID.
    #[inline]
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Check if this guard has been dropped.
    #[inline]
    pub fn is_dropped(&self) -> bool {
        self.dropped
    }

    /// Explicitly drop the guard, releasing the lock and unpinning the page.
    ///
    /// Safe to call multiple times - subsequent calls are no-ops.
    /// The page is marked dirty on first drop.
    pub fn drop_guard(&mut self) {
        if !self.dropped {
            self.dropped = true;
            self.lock.take(); // Release the lock first
            self.bpm.unpin_page_internal(self.frame_id, true); // Always dirty
        }
    }
}

impl Deref for PageWriteGuard<'_> {
    type Target = Page;

    #[inline]
    fn deref(&self) -> &Page {
        self.lock
            .as_ref()
            .expect("PageWriteGuard used after drop_guard()")
    }
}

impl DerefMut for PageWriteGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Page {
        self.lock
            .as_mut()
            .expect("PageWriteGuard used after drop_guard()")
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        self.drop_guard();
    }
}