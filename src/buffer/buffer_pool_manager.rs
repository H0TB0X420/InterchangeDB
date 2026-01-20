//! Buffer Pool Manager - the core page caching layer.
//!
//! The [`BufferPoolManager`] provides:
//! - Page caching between disk and memory
//! - Pin-based reference counting
//! - Automatic dirty page write-back
//! - Pluggable eviction policies

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use parking_lot::{Mutex, RwLock};

use crate::buffer::replacer::FifoReplacer;
use crate::buffer::{BufferPoolStats, Frame, PageReadGuard, PageWriteGuard};
use crate::common::{Error, FrameId, PageId, Result};
use crate::storage::DiskManager;

/// Manages a pool of buffer frames for caching disk pages.
///
/// # Architecture
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │                    BufferPoolManager                        │
/// │  ┌──────────────┐  ┌───────────────────────────────────┐   │
/// │  │ page_table   │  │        frames: Vec<Frame>         │   │
/// │  │PageId → Fid  │─▶│  [Frame0] [Frame1] [Frame2] ...   │   │
/// │  └──────────────┘  └───────────────────────────────────┘   │
/// │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
/// │  │  free_list   │  │   replacer   │  │disk_manager  │      │
/// │  │ Vec<FrameId> │  │ FifoReplacer │  │   Mutex      │      │
/// │  └──────────────┘  └──────────────┘  └──────────────┘      │
/// └─────────────────────────────────────────────────────────────┘
/// ```
///
/// # Thread Safety
/// - `page_table`: `RwLock` — many readers, few writers
/// - `free_list`: `Mutex` — always modified
/// - `replacer`: `Mutex` — internal state changes on access
/// - `disk_manager`: `Mutex` — single-threaded I/O
/// - `frames`: No lock — fixed size, each Frame has internal locks
/// - `stats`: No lock — all atomic counters
///
/// # Usage
/// ```ignore
/// let dm = DiskManager::create("test.db")?;
/// let bpm = BufferPoolManager::new(10, dm);
///
/// // Allocate a new page
/// let mut guard = bpm.new_page()?;
/// guard.as_mut_slice()[0] = 0xAB;
/// // guard drops: page marked dirty, unpinned
///
/// // Fetch existing page for reading
/// let guard = bpm.fetch_page_read(PageId::new(0))?;
/// let data = guard.as_slice();
/// ```
pub struct BufferPoolManager {
    /// Fixed pool of frames allocated at startup.
    frames: Vec<Frame>,

    /// Maps page IDs to frame IDs.
    page_table: RwLock<HashMap<PageId, FrameId>>,

    /// Stack of free frame IDs (LIFO for cache locality).
    free_list: Mutex<Vec<FrameId>>,

    /// Eviction policy for selecting victim frames.
    replacer: Mutex<FifoReplacer>,

    /// Handles all disk I/O.
    disk_manager: Mutex<DiskManager>,

    /// Performance statistics.
    stats: BufferPoolStats,

    /// Number of frames in the pool (immutable after construction).
    pool_size: usize,
}

impl BufferPoolManager {
    /// Create a new buffer pool manager.
    ///
    /// # Arguments
    /// * `pool_size` - Number of frames in the pool
    /// * `disk_manager` - Handles disk I/O
    ///
    /// # Panics
    /// Panics if `pool_size` is 0.
    pub fn new(pool_size: usize, disk_manager: DiskManager) -> Self {
        assert!(pool_size > 0, "pool_size must be > 0");

        // Allocate all frames upfront
        let frames: Vec<Frame> = (0..pool_size).map(|_| Frame::new()).collect();

        // All frames start on the free list (LIFO order)
        let free_list: Vec<FrameId> = (0..pool_size).map(FrameId::new).collect();

        Self {
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: Mutex::new(free_list),
            replacer: Mutex::new(FifoReplacer::new()),
            disk_manager: Mutex::new(disk_manager),
            stats: BufferPoolStats::new(),
            pool_size,
        }
    }

    // ========================================================================
    // Public API: Fetch pages
    // ========================================================================

    /// Fetch a page for reading (shared access).
    ///
    /// If the page is already in the buffer pool, returns immediately.
    /// Otherwise, loads the page from disk (possibly evicting another page).
    ///
    /// # Errors
    /// - `Error::PageNotFound` if the page doesn't exist on disk
    /// - `Error::NoFreeFrames` if all frames are pinned
    pub fn fetch_page_read(&self, page_id: PageId) -> Result<PageReadGuard<'_>> {
        let frame_id = self.fetch_page_internal(page_id)?;
        let lock = self.frames[frame_id.0].page();

        Ok(PageReadGuard::new(self, frame_id, page_id, lock))
    }

    /// Fetch a page for writing (exclusive access).
    ///
    /// Same as `fetch_page_read`, but returns an exclusive guard.
    /// The page is automatically marked dirty when the guard drops.
    ///
    /// # Errors
    /// - `Error::PageNotFound` if the page doesn't exist on disk
    /// - `Error::NoFreeFrames` if all frames are pinned
    pub fn fetch_page_write(&self, page_id: PageId) -> Result<PageWriteGuard<'_>> {
        let frame_id = self.fetch_page_internal(page_id)?;
        let lock = self.frames[frame_id.0].page_mut();

        Ok(PageWriteGuard::new(self, frame_id, page_id, lock))
    }

    // ========================================================================
    // Public API: Create and delete pages
    // ========================================================================

    /// Allocate a new page on disk and load it into the buffer pool.
    ///
    /// Returns a write guard for the new page.
    ///
    /// # Errors
    /// - `Error::NoFreeFrames` if all frames are pinned
    /// - I/O errors from disk allocation
    pub fn new_page(&self) -> Result<PageWriteGuard<'_>> {
        // Get a free frame (or evict one)
        let frame_id = self.get_free_frame()?;

        // Allocate page on disk
        let page_id = {
            let mut dm = self.disk_manager.lock();
            dm.allocate_page()?
        };

        // Set up the frame
        let frame = &self.frames[frame_id.0];

        // Reset the page data
        frame.page_mut().reset();

        // Set frame metadata
        frame.set_page_id(Some(page_id));

        // Pin the frame (new page starts with pin_count = 1)
        frame.pin();

        // Add to page table
        {
            let mut pt = self.page_table.write();
            pt.insert(page_id, frame_id);
        }

        // Record access with replacer (not evictable since pinned)
        {
            let mut replacer = self.replacer.lock();
            replacer.record_access(frame_id);
            replacer.set_evictable(frame_id, false);
        }

        // Get write lock and return guard
        let lock = frame.page_mut();

        Ok(PageWriteGuard::new(self, frame_id, page_id, lock))
    }

    /// Delete a page from the buffer pool.
    ///
    /// The page must not be pinned. This removes the page from the buffer pool
    /// but does NOT deallocate it on disk.
    ///
    /// # Errors
    /// - Returns error if page is still pinned
    pub fn delete_page(&self, page_id: PageId) -> Result<()> {
        let mut pt = self.page_table.write();

        let frame_id = match pt.get(&page_id) {
            Some(&fid) => fid,
            None => return Ok(()), // Page not in pool, nothing to do
        };

        let frame = &self.frames[frame_id.0];

        // Can't delete a pinned page
        if frame.is_pinned() {
            return Err(Error::PageNotPinned(page_id.0));
        }

        // Remove from page table
        pt.remove(&page_id);
        drop(pt); // Release write lock

        // Clear frame state
        frame.set_page_id(None);
        frame.clear_dirty();

        // Remove from replacer
        {
            let mut replacer = self.replacer.lock();
            replacer.remove(frame_id);
        }

        // Add back to free list
        {
            let mut fl = self.free_list.lock();
            fl.push(frame_id);
        }

        Ok(())
    }

    // ========================================================================
    // Public API: Flush pages
    // ========================================================================

    /// Flush a specific page to disk if it's dirty.
    ///
    /// # Errors
    /// - I/O errors from disk write
    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let frame_id = {
            let pt = self.page_table.read();
            match pt.get(&page_id) {
                Some(&fid) => fid,
                None => return Ok(()), // Page not in pool
            }
        };

        self.flush_frame(frame_id, page_id)
    }

    /// Flush all dirty pages to disk.
    ///
    /// # Errors
    /// - I/O errors from disk writes
    pub fn flush_all_pages(&self) -> Result<()> {
        // Collect all (page_id, frame_id) pairs
        let pages: Vec<(PageId, FrameId)> = {
            let pt = self.page_table.read();
            pt.iter().map(|(&pid, &fid)| (pid, fid)).collect()
        };

        for (page_id, frame_id) in pages {
            self.flush_frame(frame_id, page_id)?;
        }

        Ok(())
    }

    // ========================================================================
    // Public API: Stats and info
    // ========================================================================

    /// Get buffer pool statistics.
    pub fn stats(&self) -> &BufferPoolStats {
        &self.stats
    }

    /// Get the pool size.
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Get the number of free frames.
    pub fn free_frame_count(&self) -> usize {
        self.free_list.lock().len()
    }

    /// Get the number of pages in the buffer pool.
    pub fn page_count(&self) -> usize {
        self.page_table.read().len()
    }

    // ========================================================================
    // Internal: Called by PageGuard on drop
    // ========================================================================

    /// Unpin a page. Called by PageReadGuard/PageWriteGuard on drop.
    ///
    /// This is `pub(crate)` so guards in the same module can call it.
    pub(crate) fn unpin_page_internal(&self, frame_id: FrameId, is_dirty: bool) {
        let frame = &self.frames[frame_id.0];

        // Mark dirty if requested
        if is_dirty {
            frame.mark_dirty();
        }

        // Decrement pin count
        let new_pin_count = frame.unpin();

        // If pin count dropped to 0, page is now evictable
        if new_pin_count == 0 {
            let mut replacer = self.replacer.lock();
            replacer.set_evictable(frame_id, true);
        }
    }

    // ========================================================================
    // Internal: Core fetch logic
    // ========================================================================

    /// Fetch a page into the buffer pool, returning its frame ID.
    fn fetch_page_internal(&self, page_id: PageId) -> Result<FrameId> {
        // Fast path: check if page is already in pool (read lock only)
        {
            let pt = self.page_table.read();
            if let Some(&frame_id) = pt.get(&page_id) {
                // Cache hit!
                self.handle_cache_hit(frame_id);
                return Ok(frame_id);
            }
        }

        // Cache miss: need to load from disk
        self.handle_cache_miss(page_id)
    }

    /// Handle a cache hit: pin the frame and update replacer.
    fn handle_cache_hit(&self, frame_id: FrameId) {
        let frame = &self.frames[frame_id.0];
        frame.pin();

        // Update replacer
        {
            let mut replacer = self.replacer.lock();
            replacer.record_access(frame_id);
            replacer.set_evictable(frame_id, false);
        }

        self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Handle a cache miss: get a frame, load from disk, update mappings.
    fn handle_cache_miss(&self, page_id: PageId) -> Result<FrameId> {
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Get a free frame
        let frame_id = self.get_free_frame()?;

        // Read page from disk
        let page_data = {
            let mut dm = self.disk_manager.lock();
            dm.read_page(page_id)?
        };

        self.stats.pages_read.fetch_add(1, Ordering::Relaxed);

        // Set up the frame
        let frame = &self.frames[frame_id.0];

        // Copy page data into frame
        {
            let mut page = frame.page_mut();
            page.as_mut_slice().copy_from_slice(page_data.as_slice());
        }

        // Set frame metadata
        frame.set_page_id(Some(page_id));
        frame.pin();

        // Add to page table
        {
            let mut pt = self.page_table.write();
            pt.insert(page_id, frame_id);
        }

        // Update replacer
        {
            let mut replacer = self.replacer.lock();
            replacer.record_access(frame_id);
            replacer.set_evictable(frame_id, false);
        }

        Ok(frame_id)
    }

    // ========================================================================
    // Internal: Frame allocation and eviction
    // ========================================================================

    /// Get a free frame, evicting if necessary.
    fn get_free_frame(&self) -> Result<FrameId> {
        // Try the free list first
        {
            let mut fl = self.free_list.lock();
            if let Some(frame_id) = fl.pop() {
                return Ok(frame_id);
            }
        }

        // No free frames, need to evict
        self.evict_page()
    }

    /// Evict a page and return its frame.
    fn evict_page(&self) -> Result<FrameId> {
        // Ask replacer for a victim
        let frame_id = {
            let mut replacer = self.replacer.lock();
            replacer.evict().ok_or(Error::NoFreeFrames)?
        };

        self.stats.evictions.fetch_add(1, Ordering::Relaxed);

        let frame = &self.frames[frame_id.0];

        // Get the page ID before we clear it
        let old_page_id = frame.page_id();

        // If dirty, flush to disk
        if frame.is_dirty() {
            if let Some(pid) = old_page_id {
                self.flush_frame(frame_id, pid)?;
            }
        }

        // Remove from page table
        if let Some(pid) = old_page_id {
            let mut pt = self.page_table.write();
            pt.remove(&pid);
        }

        // Reset frame state
        frame.clear_dirty();
        frame.set_page_id(None);

        Ok(frame_id)
    }

    /// Flush a frame to disk if dirty.
    fn flush_frame(&self, frame_id: FrameId, page_id: PageId) -> Result<()> {
        let frame = &self.frames[frame_id.0];

        if frame.is_dirty() {
            // Hold page read lock while writing to disk
            let page = frame.page();
            {
                let mut dm = self.disk_manager.lock();
                dm.write_page(page_id, &page)?;
            }
            drop(page);

            frame.clear_dirty();
            self.stats.pages_written.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Helper to create a BPM with a temporary database file.
    fn create_test_bpm(pool_size: usize) -> (BufferPoolManager, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::create(&path).unwrap();
        (BufferPoolManager::new(pool_size, dm), dir)
    }

    #[test]
    fn test_new_page() {
        let (bpm, _dir) = create_test_bpm(10);

        let guard = bpm.new_page().unwrap();
        assert_eq!(guard.page_id(), PageId::new(0));
        drop(guard);

        let guard = bpm.new_page().unwrap();
        assert_eq!(guard.page_id(), PageId::new(1));
    }

    #[test]
    fn test_fetch_page_read() {
        let (bpm, _dir) = create_test_bpm(10);

        // Create a page and write data
        {
            let mut guard = bpm.new_page().unwrap();
            guard.as_mut_slice()[0] = 0xAB;
        }

        // Fetch and verify
        {
            let guard = bpm.fetch_page_read(PageId::new(0)).unwrap();
            assert_eq!(guard.as_slice()[0], 0xAB);
        }
    }

    #[test]
    fn test_fetch_page_write() {
        let (bpm, _dir) = create_test_bpm(10);

        // Create a page
        {
            let _guard = bpm.new_page().unwrap();
        }

        // Fetch for write and modify
        {
            let mut guard = bpm.fetch_page_write(PageId::new(0)).unwrap();
            guard.as_mut_slice()[0] = 0xCD;
        }

        // Verify modification
        {
            let guard = bpm.fetch_page_read(PageId::new(0)).unwrap();
            assert_eq!(guard.as_slice()[0], 0xCD);
        }
    }

    #[test]
    fn test_cache_hit() {
        let (bpm, _dir) = create_test_bpm(10);

        // Create a page
        {
            let _guard = bpm.new_page().unwrap();
        }

        // Fetch multiple times - should be cache hits
        {
            let _guard = bpm.fetch_page_read(PageId::new(0)).unwrap();
        }
        {
            let _guard = bpm.fetch_page_read(PageId::new(0)).unwrap();
        }

        let snapshot = bpm.stats().snapshot();
        assert!(snapshot.cache_hits >= 2);
    }

    #[test]
    fn test_eviction() {
        let (bpm, _dir) = create_test_bpm(3); // Small pool

        // Fill the pool
        for _ in 0..3 {
            let _guard = bpm.new_page().unwrap();
        }

        // All frames used, free list empty
        assert_eq!(bpm.free_frame_count(), 0);

        // Create one more page (forces eviction)
        let guard = bpm.new_page().unwrap();
        assert_eq!(guard.page_id(), PageId::new(3));

        let snapshot = bpm.stats().snapshot();
        assert_eq!(snapshot.evictions, 1);
    }

    #[test]
    fn test_dirty_page_flushed_on_eviction() {
        let (bpm, _dir) = create_test_bpm(1); // Only 1 frame!

        // Create page 0 and write data
        {
            let mut guard = bpm.new_page().unwrap();
            guard.as_mut_slice()[0] = 0x42;
        } // Drops, marks dirty

        // Create page 1 (evicts page 0, should flush first)
        {
            let _guard = bpm.new_page().unwrap();
        }

        // Fetch page 0 again (should load from disk with our data)
        {
            let guard = bpm.fetch_page_read(PageId::new(0)).unwrap();
            assert_eq!(guard.as_slice()[0], 0x42);
        }
    }

    #[test]
    fn test_delete_page() {
        let (bpm, _dir) = create_test_bpm(10);

        // Create a page
        {
            let _guard = bpm.new_page().unwrap();
        }

        assert_eq!(bpm.page_count(), 1);

        // Delete it
        bpm.delete_page(PageId::new(0)).unwrap();

        // Frame should be back on free list
        assert_eq!(bpm.free_frame_count(), 10);
        assert_eq!(bpm.page_count(), 0);
    }

    #[test]
    fn test_delete_pinned_page_fails() {
        let (bpm, _dir) = create_test_bpm(10);

        // Create and hold a page
        let _guard = bpm.new_page().unwrap();

        // Try to delete while pinned - should fail
        let result = bpm.delete_page(PageId::new(0));
        assert!(result.is_err());
    }

    #[test]
    fn test_flush_page() {
        let (bpm, _dir) = create_test_bpm(10);

        // Create and modify a page
        {
            let mut guard = bpm.new_page().unwrap();
            guard.as_mut_slice()[0] = 0xFF;
        }

        // Explicitly flush
        bpm.flush_page(PageId::new(0)).unwrap();

        let snapshot = bpm.stats().snapshot();
        assert!(snapshot.pages_written >= 1);
    }

    #[test]
    fn test_flush_all_pages() {
        let (bpm, _dir) = create_test_bpm(10);

        // Create multiple dirty pages
        for i in 0..5 {
            let mut guard = bpm.new_page().unwrap();
            guard.as_mut_slice()[0] = i;
        }

        // Flush all
        bpm.flush_all_pages().unwrap();

        let snapshot = bpm.stats().snapshot();
        assert!(snapshot.pages_written >= 5);
    }

    #[test]
    fn test_multiple_read_guards() {
        let (bpm, _dir) = create_test_bpm(10);

        // Create a page
        {
            let _guard = bpm.new_page().unwrap();
        }

        // Multiple simultaneous read guards should work
        let guard1 = bpm.fetch_page_read(PageId::new(0)).unwrap();
        let guard2 = bpm.fetch_page_read(PageId::new(0)).unwrap();

        assert_eq!(guard1.page_id(), guard2.page_id());

        drop(guard1);
        drop(guard2);
    }

    #[test]
    fn test_page_not_found() {
        let (bpm, _dir) = create_test_bpm(10);

        // Try to fetch a page that doesn't exist
        let result = bpm.fetch_page_read(PageId::new(999));
        assert!(result.is_err());
    }

    #[test]
    fn test_no_free_frames() {
        let (bpm, _dir) = create_test_bpm(2);

        // Pin both frames (hold the guards)
        let _guard1 = bpm.new_page().unwrap();
        let _guard2 = bpm.new_page().unwrap();

        // All frames pinned, can't allocate
        let result = bpm.new_page();
        assert!(result.is_err());
    }

    #[test]
    fn test_pin_count_tracking() {
        let (bpm, _dir) = create_test_bpm(10);

        
        let _guard = bpm.new_page().unwrap();
    
        // Frame should be evictable now (pin_count = 0)
        let frame = &bpm.frames[0];
        assert_eq!(frame.pin_count(), 0);
        assert!(frame.page_id().is_some());
        assert!(frame.is_evictable());

        // Fetch again - pins it
        let _guard = bpm.fetch_page_read(PageId::new(0)).unwrap();
        assert_eq!(frame.pin_count(), 1);
        assert!(!frame.is_evictable());

        // Guard dropped - unpinned
        assert_eq!(frame.pin_count(), 0);
        assert!(frame.is_evictable());
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        let (bpm, _dir) = create_test_bpm(10);
        let bpm = Arc::new(bpm);

        // Create a page
        {
            let mut guard = bpm.new_page().unwrap();
            guard.as_mut_slice()[0] = 0x42;
        }

        let mut handles = vec![];

        // Multiple threads reading the same page
        for _ in 0..10 {
            let bpm_clone = Arc::clone(&bpm);
            handles.push(thread::spawn(move || {
                let guard = bpm_clone.fetch_page_read(PageId::new(0)).unwrap();
                assert_eq!(guard.as_slice()[0], 0x42);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}