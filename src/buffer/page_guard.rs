//! RAII guards for page access.
//!
//! These guards provide safe access to pages in the buffer pool:
//! - [`PageReadGuard`] - Shared read access (multiple allowed)
//! - [`PageWriteGuard`] - Exclusive write access (auto-marks dirty)
//!
//! Both guards auto-unpin the page when dropped.

use std::ops::{Deref, DerefMut};

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use crate::common::{FrameId, PageId};
use crate::storage::page::Page;

// Forward declaration - BufferPoolManager will be in the same module
use super::buffer_pool_manager::BufferPoolManager;

/// Guard for read-only page access.
///
/// Multiple `PageReadGuard`s can exist for the same page simultaneously.
/// The page is automatically unpinned when the guard is dropped.
///
/// # Example
/// ```ignore
/// let guard = bpm.fetch_page_read(page_id)?;
/// let data = guard.as_slice();  // Deref to &Page
/// // guard drops here, page unpinned
/// ```
pub struct PageReadGuard<'a> {
    /// Reference back to BPM for unpin on drop.
    bpm: &'a BufferPoolManager,
    /// Frame holding this page.
    frame_id: FrameId,
    /// Page ID for convenience.
    page_id: PageId,
    /// Lock guard providing access to page data.
    lock: RwLockReadGuard<'a, Page>,
}

impl<'a> PageReadGuard<'a> {
    /// Create a new read guard.
    ///
    /// Called by `BufferPoolManager::fetch_page_read()`.
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
            lock,
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
}

impl Deref for PageReadGuard<'_> {
    type Target = Page;

    #[inline]
    fn deref(&self) -> &Page {
        &self.lock
    }
}

impl Drop for PageReadGuard<'_> {
    fn drop(&mut self) {
        // Read guard: not dirty
        self.bpm.unpin_page_internal(self.frame_id, false);
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
/// guard.as_mut_slice()[0] = 0xFF;  // DerefMut to &mut Page
/// // guard drops here, page marked dirty and unpinned
/// ```
pub struct PageWriteGuard<'a> {
    /// Reference back to BPM for unpin on drop.
    bpm: &'a BufferPoolManager,
    /// Frame holding this page.
    frame_id: FrameId,
    /// Page ID for convenience.
    page_id: PageId,
    /// Lock guard providing access to page data.
    lock: RwLockWriteGuard<'a, Page>,
}

impl<'a> PageWriteGuard<'a> {
    /// Create a new write guard.
    ///
    /// Called by `BufferPoolManager::fetch_page_write()`.
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
            lock,
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
}

impl Deref for PageWriteGuard<'_> {
    type Target = Page;

    #[inline]
    fn deref(&self) -> &Page {
        &self.lock
    }
}

impl DerefMut for PageWriteGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Page {
        &mut self.lock
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        // Write guard: always dirty
        self.bpm.unpin_page_internal(self.frame_id, true);
    }
}