//! Buffer Pool Manager - the core page caching layer.
//!
//! The [`BufferPoolManager`] provides:
//! - Page caching between disk and memory
//! - Pin-based reference counting
//! - Automatic dirty page write-back
//! - Pluggable eviction policies

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use crate::buffer::replacer::{EvictionPolicy, FifoReplacer};
use crate::buffer::swap::{SwapMode, SwapResult};
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
/// │  │ Vec<FrameId> │  │ dyn Policy   │  │   Mutex      │      │
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
pub struct BufferPoolManager {
    /// Fixed pool of frames allocated at startup.
    frames: Vec<Frame>,

    /// Maps page IDs to frame IDs.
    page_table: RwLock<HashMap<PageId, FrameId>>,

    /// Stack of free frame IDs (LIFO for cache locality).
    free_list: Mutex<Vec<FrameId>>,

    /// Eviction policy for selecting victim frames.
    /// Uses trait object for runtime policy swapping.
    replacer: Mutex<Box<dyn EvictionPolicy>>,

    /// Handles all disk I/O. `Box<dyn DiskManager>` allows runtime
    /// substitution of file-backed, in-memory, or fault-injection backends.
    /// Vtable dispatch cost is negligible next to the I/O it gates.
    disk_manager: Mutex<Box<dyn DiskManager>>,

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
    pub fn new<D: DiskManager + 'static>(pool_size: usize, disk_manager: D) -> Self {
        assert!(pool_size > 0, "pool_size must be > 0");

        let frames: Vec<Frame> = (0..pool_size).map(|_| Frame::new()).collect();
        let free_list: Vec<FrameId> = (0..pool_size).map(FrameId::new).collect();

        Self {
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: Mutex::new(free_list),
            replacer: Mutex::new(Box::new(FifoReplacer::new())),
            disk_manager: Mutex::new(Box::new(disk_manager)),
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

    /// Fetch a page for reading, returning None if not possible.
    ///
    /// Matches BusTub's `CheckedReadPage()`.
    pub fn checked_read_page(&self, page_id: PageId) -> Option<PageReadGuard<'_>> {
        self.fetch_page_read(page_id).ok()
    }

    /// Fetch a page for writing, returning None if not possible.
    ///
    /// Matches BusTub's `CheckedWritePage()`.
    pub fn checked_write_page(&self, page_id: PageId) -> Option<PageWriteGuard<'_>> {
        self.fetch_page_write(page_id).ok()
    }

    // ========================================================================
    // Public API: Create and delete pages
    // ========================================================================

    /// Allocate a new page ID on disk.
    ///
    /// This just allocates the page ID without bringing it into the buffer pool.
    /// Use `fetch_page_write()` to actually load the page.
    ///
    /// Matches BusTub's `NewPage()` which only allocates the ID.
    ///
    /// # Errors
    /// - I/O errors from disk allocation
    pub fn allocate_page_id(&self) -> Result<PageId> {
        let mut dm = self.disk_manager.lock();
        dm.allocate_page()
    }

    /// Allocate a new page on disk and load it into the buffer pool.
    ///
    /// This is a convenience method that combines `allocate_page_id()` and
    /// `fetch_page_write()`. For BusTub-style usage, call them separately.
    ///
    /// Returns a write guard for the new page.
    ///
    /// # Errors
    /// - `Error::NoFreeFrames` if all frames are pinned
    /// - I/O errors from disk allocation
    pub fn new_page(&self) -> Result<PageWriteGuard<'_>> {
        // Allocate page ID first (this always succeeds unless I/O error)
        let page_id = self.allocate_page_id()?;

        // Now try to bring it into the buffer pool
        // If this fails with NoFreeFrames, the page ID is "leaked" on disk
        // but that's acceptable - BusTub has the same behavior
        self.fetch_page_write_new(page_id)
    }

    /// Fetch a newly allocated page for writing.
    ///
    /// Unlike `fetch_page_write`, this initializes the page to zeros
    /// instead of reading from disk (since it's a new page).
    fn fetch_page_write_new(&self, page_id: PageId) -> Result<PageWriteGuard<'_>> {
        let frame_id = self.get_free_frame(page_id)?;

        let frame = &self.frames[frame_id.0];

        // Initialize to zeros (new page)
        frame.page_mut().reset();
        frame.set_page_id(Some(page_id));
        frame.pin();

        {
            let mut pt = self.page_table.write();
            pt.insert(page_id, frame_id);
        }

        {
            let mut replacer = self.replacer.lock();
            replacer.record_access(frame_id, page_id);
            replacer.set_evictable(frame_id, false);
        }

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
            None => return Ok(()), // Page not in pool
        };

        let frame = &self.frames[frame_id.0];

        if frame.is_pinned() {
            return Err(Error::PageNotPinned(page_id.0));
        }

        pt.remove(&page_id);
        drop(pt);

        frame.set_page_id(None);
        frame.clear_dirty();

        {
            let mut replacer = self.replacer.lock();
            replacer.remove(frame_id);
        }

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
    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        let frame_id = {
            let pt = self.page_table.read();
            match pt.get(&page_id) {
                Some(&fid) => fid,
                None => return Ok(()),
            }
        };
        self.flush_frame(frame_id, page_id)
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all_pages(&self) -> Result<()> {
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

    /// Get the total number of pages allocated on disk.
    pub fn disk_page_count(&self) -> u32 {
        self.disk_manager.lock().page_count()
    }

    /// Get pin count for a page. Returns None if page not in pool.
    ///
    /// Matches BusTub's `GetPinCount()`.
    pub fn get_pin_count(&self, page_id: PageId) -> Option<u32> {
        let pt = self.page_table.read();
        let &frame_id = pt.get(&page_id)?;
        Some(self.frames[frame_id.0].pin_count())
    }

    /// Check if a page is in the buffer pool.
    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.page_table.read().contains_key(&page_id)
    }

    /// Get the name of the current eviction policy.
    pub fn get_policy_name(&self) -> &'static str {
        self.replacer.lock().name()
    }

    // ========================================================================
    // Public API: Policy swapping
    // ========================================================================

    /// Swap the eviction policy at runtime.
    ///
    /// # Arguments
    /// * `new_policy` - The new eviction policy to use
    /// * `mode` - `Cold` (fresh start) or `Warm` (transfer hot page knowledge)
    ///
    /// # How it works
    /// 1. Snapshots the current page table (frame → page mappings)
    /// 2. Acquires the replacer lock (brief blocking period)
    /// 3. For warm swap: exports state from old policy, imports into new
    /// 4. Re-registers all in-pool frames with the new policy
    /// 5. Sets correct evictability based on current pin counts
    ///
    /// # Warm vs Cold
    /// - **Cold**: New policy starts with no access pattern knowledge. Good when
    ///   changing workload characteristics entirely.
    /// - **Warm**: Transfers "hot page" scores so the new policy knows which pages
    ///   are frequently accessed. Avoids a burst of poor eviction decisions.
    pub fn swap_policy(&self, new_policy: Box<dyn EvictionPolicy>, mode: SwapMode) -> SwapResult {
        // Q-27: hold pt.write across the entire swap (snapshot + replace +
        // re-register). evict_page also holds pt.write across its
        // evict_for_page → pt.remove sequence. With both paths gated on
        // pt.write, swap_policy cannot run while a victim is mid-eviction
        // (between evict_for_page and pt.remove) — preventing the new
        // replacer from re-marking that victim evictable and letting a
        // second concurrent eviction pick it.
        let start = Instant::now();
        let pt = self.page_table.write();
        let frame_mappings: Vec<(FrameId, PageId)> =
            pt.iter().map(|(&pid, &fid)| (fid, pid)).collect();

        let mut replacer = self.replacer.lock();

        let old_name = replacer.name();
        let pages_transferred;

        match mode {
            SwapMode::Cold => {
                pages_transferred = 0;
                *replacer = new_policy;
            }
            SwapMode::Warm => {
                let state = replacer.export_state();
                pages_transferred = state.hot_pages.len();
                *replacer = new_policy;
                replacer.import_state(&state);
            }
        }

        let new_name = replacer.name();

        // Re-register all in-pool frames with the new policy.
        for &(frame_id, page_id) in &frame_mappings {
            replacer.record_access(frame_id, page_id);

            let evictable = !self.frames[frame_id.0].is_pinned();
            replacer.set_evictable(frame_id, evictable);
        }

        let swap_duration = start.elapsed();
        drop(replacer);
        drop(pt);

        self.stats.policy_swaps.fetch_add(1, Ordering::Relaxed);

        SwapResult {
            old_policy: old_name,
            new_policy: new_name,
            mode,
            pages_transferred,
            frames_registered: frame_mappings.len(),
            swap_duration,
        }
    }

    // ========================================================================
    // Internal: Called by PageGuard on drop
    // ========================================================================

    /// Unpin a page. Called by PageReadGuard/PageWriteGuard on drop.
    pub(crate) fn unpin_page_internal(&self, frame_id: FrameId, is_dirty: bool) {
        let frame = &self.frames[frame_id.0];

        if is_dirty {
            frame.mark_dirty();
        }

        let new_pin_count = frame.unpin();

        if new_pin_count == 0 {
            let mut replacer = self.replacer.lock();
            replacer.set_evictable(frame_id, true);
        }
    }

    // ========================================================================
    // Internal: Core fetch logic
    // ========================================================================

    fn fetch_page_internal(&self, page_id: PageId) -> Result<FrameId> {
        // Fast path: cache hit
        {
            let pt = self.page_table.read();
            if let Some(&frame_id) = pt.get(&page_id) {
                self.handle_cache_hit(frame_id, page_id);
                return Ok(frame_id);
            }
        }
        // Cache miss
        self.handle_cache_miss(page_id)
    }

    fn handle_cache_hit(&self, frame_id: FrameId, page_id: PageId) {
        let frame = &self.frames[frame_id.0];
        frame.pin();

        // NOTE (perf): this per-hit `replacer` mutex is the dominant
        // concurrency bottleneck for the write-heavy TPC-C mix (profiled —
        // every page hit serializes here). Removing it is a known lever but
        // conflicts with the push-based, pluggable `EvictionPolicy`. See
        // docs/scalability-investigation.md for the full analysis and the
        // refactor options before touching this.
        {
            let mut replacer = self.replacer.lock();
            replacer.record_access(frame_id, page_id);
            replacer.set_evictable(frame_id, false);
        }

        self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn handle_cache_miss(&self, page_id: PageId) -> Result<FrameId> {
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

        let frame_id = self.get_free_frame(page_id)?;

        let page_data = {
            let mut dm = self.disk_manager.lock();
            dm.read_page(page_id)?
        };

        self.stats.pages_read.fetch_add(1, Ordering::Relaxed);

        let frame = &self.frames[frame_id.0];

        {
            let mut page = frame.page_mut();
            page.as_mut_slice().copy_from_slice(page_data.as_slice());
        }

        frame.set_page_id(Some(page_id));
        frame.pin();

        {
            let mut pt = self.page_table.write();
            pt.insert(page_id, frame_id);
        }

        {
            let mut replacer = self.replacer.lock();
            replacer.record_access(frame_id, page_id);
            replacer.set_evictable(frame_id, false);
        }

        Ok(frame_id)
    }

    // ========================================================================
    // Internal: Frame allocation and eviction
    // ========================================================================

    fn get_free_frame(&self, incoming_page: PageId) -> Result<FrameId> {
        {
            let mut fl = self.free_list.lock();
            if let Some(frame_id) = fl.pop() {
                return Ok(frame_id);
            }
        }
        self.evict_page(incoming_page)
    }

    fn evict_page(&self, incoming_page: PageId) -> Result<FrameId> {
        // Q-27: `evict_for_page` only consults the replacer's evictable
        // set, which is updated by `set_evictable` calls *after* a
        // cache-hit thread pins a frame. There is a window where a frame
        // is still in the evictable set but a concurrent cache-hit has
        // selected it and is about to pin. Without the pin-count check
        // below, we'd flush + overwrite a frame that another thread
        // holds — surfacing as "byte 0 of page X is page Y's data" reads.
        //
        // The check has to happen *under* `pt.write`: cache-hit threads
        // hold `pt.read` for the entirety of `handle_cache_hit`, which is
        // exclusive with `pt.write`. So while we hold `pt.write`, no
        // cache-hit can complete its pin, and `pin_count` is the final
        // value, not a moving target.
        //
        // If the victim is pinned at check time, we abandon it (don't
        // re-add to the evictable set; the pinning thread's eventual
        // `unpin_page_internal` will set_evictable(true)) and try again.
        // Bounded by frame count to avoid pathological retry loops.
        // Q-27 also addresses a swap_policy race: swap_policy iterates the
        // page table and re-registers each frame's evictability in the new
        // replacer. If swap_policy runs after our `evict_for_page` (which
        // removed our victim from the old policy's evictable set) but
        // before our `pt.remove`, the victim is still in pt and the new
        // replacer marks it evictable again — a second concurrent
        // eviction can then select the same frame, and we end up with
        // two threads "owning" it. Holding pt.write across evict_for_page
        // prevents the window: swap_policy must also acquire pt.write,
        // so it serializes with our entire selection-and-removal.
        const MAX_EVICT_RETRIES: usize = 64;
        for _ in 0..MAX_EVICT_RETRIES {
            let frame_id;
            let old_page_id;
            let was_dirty;
            {
                let mut pt = self.page_table.write();
                frame_id = {
                    let mut replacer = self.replacer.lock();
                    match replacer.evict_for_page(incoming_page) {
                        Some(fid) => fid,
                        None => return Err(Error::NoFreeFrames),
                    }
                };
                let frame = &self.frames[frame_id.0];
                if frame.pin_count() > 0 {
                    // Cache-hit raced ahead. Abandon this victim; the
                    // unpinning thread will set_evictable(true) and add
                    // the frame back to the evictable set.
                    drop(pt);
                    continue;
                }
                old_page_id = frame.page_id();
                was_dirty = frame.is_dirty();
                if let Some(pid) = old_page_id {
                    pt.remove(&pid);
                }
                frame.set_page_id(None);
            }

            self.stats.evictions.fetch_add(1, Ordering::Relaxed);

            if was_dirty {
                if let Some(pid) = old_page_id {
                    self.flush_frame(frame_id, pid)?;
                }
            }
            let frame = &self.frames[frame_id.0];
            frame.clear_dirty();
            return Ok(frame_id);
        }
        Err(Error::NoFreeFrames)
    }

    fn flush_frame(&self, frame_id: FrameId, page_id: PageId) -> Result<()> {
        let frame = &self.frames[frame_id.0];

        if frame.is_dirty() {
            // Checksum is computed at encode time (encode_leaf_node, encode_internal_node,
            // BTreeHeaderPage::encode) when the page data is finalized. This avoids
            // corrupting raw pages that don't have a PageHeader prefix.
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

    fn create_test_bpm(pool_size: usize) -> (BufferPoolManager, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = crate::storage::FileDiskManager::create(&path).unwrap();
        (BufferPoolManager::new(pool_size, dm), dir)
    }

    // ========================================================================
    // Core functionality tests
    // ========================================================================

    #[test]
    fn test_new_page_and_fetch() {
        let (bpm, _dir) = create_test_bpm(10);
        let data = b"Hello, world!";

        // Create and write
        let pid = {
            let mut guard = bpm.new_page().unwrap();
            assert_eq!(guard.page_id(), PageId::new(0));
            guard.as_mut_slice()[..data.len()].copy_from_slice(data);
            guard.page_id()
        };

        // Read back
        {
            let guard = bpm.fetch_page_read(pid).unwrap();
            assert_eq!(&guard.as_slice()[..data.len()], data);
        }

        // Delete
        bpm.delete_page(pid).unwrap();
        assert!(!bpm.contains_page(pid));
    }

    #[test]
    fn test_eviction_persists_data() {
        let (bpm, _dir) = create_test_bpm(1); // Only 1 frame!

        // Create page 0, write data
        let pid0 = {
            let mut guard = bpm.new_page().unwrap();
            guard.as_mut_slice()[0] = 0x42;
            guard.page_id()
        };

        // Create page 1 (evicts page 0)
        let _pid1 = bpm.new_page().unwrap().page_id();

        assert_eq!(bpm.stats().snapshot().evictions, 1);

        // Fetch page 0 - should load from disk with data intact
        {
            let guard = bpm.fetch_page_read(pid0).unwrap();
            assert_eq!(guard.as_slice()[0], 0x42);
        }
    }

    #[test]
    fn test_no_free_frames_when_all_pinned() {
        let (bpm, _dir) = create_test_bpm(2);

        let _guard1 = bpm.new_page().unwrap();
        let _guard2 = bpm.new_page().unwrap();

        // All frames pinned
        assert!(bpm.new_page().is_err());
    }

    // ========================================================================
    // BusTub compatibility: drop_guard and pin counting
    // ========================================================================

    #[test]
    fn test_drop_guard_idempotent() {
        let (bpm, _dir) = create_test_bpm(10);

        let pid = bpm.new_page().unwrap().page_id();

        let mut guard = bpm.fetch_page_write(pid).unwrap();
        assert_eq!(bpm.get_pin_count(pid), Some(1));

        // First drop
        guard.drop_guard();
        assert!(guard.is_dropped());
        assert_eq!(bpm.get_pin_count(pid), Some(0));

        // Second drop - no effect
        guard.drop_guard();
        assert_eq!(bpm.get_pin_count(pid), Some(0));

        // Can acquire again after drop
        let _guard2 = bpm.fetch_page_write(pid).unwrap();
    }

    #[test]
    fn test_pin_count_with_checked_methods() {
        let (bpm, _dir) = create_test_bpm(2);

        let pid0 = bpm.new_page().unwrap().page_id();
        let pid1 = bpm.new_page().unwrap().page_id();

        // Hold both pages
        {
            let mut g0 = bpm.checked_write_page(pid0).expect("should get page0");
            let mut g1 = bpm.checked_write_page(pid1).expect("should get page1");

            g0.as_mut_slice()[0] = 0xAA;
            g1.as_mut_slice()[0] = 0xBB;

            assert_eq!(bpm.get_pin_count(pid0), Some(1));
            assert_eq!(bpm.get_pin_count(pid1), Some(1));

            // All frames pinned - can't create new page
            assert!(bpm.new_page().is_err());

            // Drop one
            g0.drop_guard();
            assert_eq!(bpm.get_pin_count(pid0), Some(0));

            // Still can't create - need to check if evictable
            // (g1 still pinned, so new_page would evict pid0)
        }

        // After both dropped, verify data persisted
        let g0 = bpm.checked_read_page(pid0).unwrap();
        assert_eq!(g0.as_slice()[0], 0xAA);
    }

    // ========================================================================
    // Concurrent access
    // ========================================================================

    #[test]
    fn test_concurrent_readers() {
        use std::sync::Arc;
        use std::thread;

        let (bpm, _dir) = create_test_bpm(10);
        let bpm = Arc::new(bpm);

        let pid = {
            let mut guard = bpm.new_page().unwrap();
            guard.as_mut_slice()[0] = 0x42;
            guard.page_id()
        };

        let mut handles = vec![];
        for _ in 0..10 {
            let bpm_clone = Arc::clone(&bpm);
            handles.push(thread::spawn(move || {
                let guard = bpm_clone.fetch_page_read(pid).unwrap();
                assert_eq!(guard.as_slice()[0], 0x42);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    /// BusTub: EvictableTest - pinned pages cannot be evicted
    #[test]
    fn test_evictable_under_contention() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let (bpm, _dir) = create_test_bpm(1);
        let bpm = Arc::new(bpm);

        let winner_pid = bpm.new_page().unwrap().page_id();
        let loser_pid = bpm.new_page().unwrap().page_id(); // Evicts winner

        // KEY: Main thread fetches winner and HOLDS IT before spawning threads.
        // This ensures winner is in the pool and pinned.
        let _winner_guard = bpm.fetch_page_read(winner_pid).unwrap();

        let barrier = Arc::new(Barrier::new(5));
        let mut handles = vec![];

        for _ in 0..4 {
            let bpm_clone = Arc::clone(&bpm);
            let barrier_clone = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                barrier_clone.wait();
                // Cache HIT - winner already in pool, just increments pin count
                let _guard = bpm_clone.fetch_page_read(winner_pid).unwrap();
                // Can't evict winner (pinned by main + this thread) - loser fetch fails
                assert!(bpm_clone.checked_read_page(loser_pid).is_none());
            }));
        }

        barrier.wait();

        for h in handles {
            h.join().unwrap();
        }
        // _winner_guard drops here after all threads complete
    }

    #[test]
    fn test_get_policy_name() {
        let (bpm, _dir) = create_test_bpm(10);
        assert_eq!(bpm.get_policy_name(), "fifo");
    }

    // ========================================================================
    // Policy swap tests
    // ========================================================================

    #[test]
    fn test_cold_swap_idle() {
        use crate::buffer::replacer::LruReplacer;

        let (bpm, _dir) = create_test_bpm(10);
        assert_eq!(bpm.get_policy_name(), "fifo");

        let result = bpm.swap_policy(Box::new(LruReplacer::new(10)), SwapMode::Cold);

        assert_eq!(result.old_policy, "fifo");
        assert_eq!(result.new_policy, "lru");
        assert_eq!(result.mode, SwapMode::Cold);
        assert_eq!(result.pages_transferred, 0);
        assert_eq!(result.frames_registered, 0);
        assert_eq!(bpm.get_policy_name(), "lru");
        assert_eq!(bpm.stats().snapshot().policy_swaps, 1);
    }

    #[test]
    fn test_cold_swap_with_pages_in_pool() {
        use crate::buffer::replacer::LruReplacer;

        let (bpm, _dir) = create_test_bpm(5);

        // Create some pages (all unpinned after guard drops)
        let pid0 = bpm.new_page().unwrap().page_id();
        let pid1 = bpm.new_page().unwrap().page_id();
        let pid2 = bpm.new_page().unwrap().page_id();

        assert_eq!(bpm.page_count(), 3);

        let result = bpm.swap_policy(Box::new(LruReplacer::new(5)), SwapMode::Cold);

        assert_eq!(result.frames_registered, 3);
        assert_eq!(result.pages_transferred, 0);
        assert_eq!(bpm.get_policy_name(), "lru");

        // All pages should still be accessible after swap
        let g0 = bpm.fetch_page_read(pid0).unwrap();
        let g1 = bpm.fetch_page_read(pid1).unwrap();
        let g2 = bpm.fetch_page_read(pid2).unwrap();
        drop((g0, g1, g2));
    }

    #[test]
    fn test_warm_swap_preserves_hot_pages() {
        use crate::buffer::replacer::LruReplacer;

        let (bpm, _dir) = create_test_bpm(3);

        // Create 3 pages with written data
        let pid0 = {
            let mut g = bpm.new_page().unwrap();
            g.as_mut_slice()[0] = 0xAA;
            g.page_id()
        };
        let pid1 = {
            let mut g = bpm.new_page().unwrap();
            g.as_mut_slice()[0] = 0xBB;
            g.page_id()
        };
        let pid2 = {
            let mut g = bpm.new_page().unwrap();
            g.as_mut_slice()[0] = 0xCC;
            g.page_id()
        };

        // Re-access pid2 to make it "hot" under FIFO → doesn't matter for FIFO,
        // but the export scores will reflect insertion order
        let _ = bpm.fetch_page_read(pid2).unwrap();

        let result = bpm.swap_policy(Box::new(LruReplacer::new(3)), SwapMode::Warm);

        assert_eq!(result.old_policy, "fifo");
        assert_eq!(result.new_policy, "lru");
        assert_eq!(result.mode, SwapMode::Warm);
        assert_eq!(result.frames_registered, 3);
        assert!(result.pages_transferred > 0);

        // All data intact after warm swap
        {
            let g = bpm.fetch_page_read(pid0).unwrap();
            assert_eq!(g.as_slice()[0], 0xAA);
        }
        {
            let g = bpm.fetch_page_read(pid1).unwrap();
            assert_eq!(g.as_slice()[0], 0xBB);
        }
        {
            let g = bpm.fetch_page_read(pid2).unwrap();
            assert_eq!(g.as_slice()[0], 0xCC);
        }
    }

    #[test]
    fn test_warm_swap_eviction_order() {
        use crate::buffer::replacer::LruReplacer;

        let (bpm, _dir) = create_test_bpm(3);

        // Create 3 pages. Under FIFO, insertion order: pid0, pid1, pid2
        let pid0 = bpm.new_page().unwrap().page_id();
        let pid1 = bpm.new_page().unwrap().page_id();
        let pid2 = bpm.new_page().unwrap().page_id();

        // Warm swap to LRU — imported scores should preserve FIFO ordering:
        // pid0 coldest (lowest score), pid2 hottest (highest score)
        bpm.swap_policy(Box::new(LruReplacer::new(3)), SwapMode::Warm);

        // Pool is full (3/3). Creating a new page forces eviction.
        // With imported scores, pid0 should be evicted first (coldest).
        let _pid3 = bpm.new_page().unwrap().page_id();

        assert!(
            !bpm.contains_page(pid0),
            "pid0 should have been evicted (coldest)"
        );
        assert!(bpm.contains_page(pid1));
        assert!(bpm.contains_page(pid2));
    }

    #[test]
    fn test_swap_with_pinned_pages() {
        use crate::buffer::replacer::LruReplacer;

        let (bpm, _dir) = create_test_bpm(3);

        let pid0 = bpm.new_page().unwrap().page_id();
        let _pid1 = bpm.new_page().unwrap().page_id();

        // Hold pid0 pinned during swap
        let _guard = bpm.fetch_page_read(pid0).unwrap();

        bpm.swap_policy(Box::new(LruReplacer::new(3)), SwapMode::Cold);

        // pid0 is pinned → should NOT be evictable after swap
        // pid1 is unpinned → should be evictable
        // Create a 3rd page (uses free frame, no eviction needed)
        let _pid2 = bpm.new_page().unwrap().page_id();

        // Now pool is full. Create 4th → must evict. pid0 is pinned, so pid1 is evicted.
        let _pid3 = bpm.new_page().unwrap().page_id();
        assert!(bpm.contains_page(pid0), "pinned page should not be evicted");
    }

    #[test]
    fn test_multiple_consecutive_swaps() {
        use crate::buffer::replacer::{ClockReplacer, LruKReplacer, LruReplacer};

        let (bpm, _dir) = create_test_bpm(5);

        // Create some pages
        let pid0 = {
            let mut g = bpm.new_page().unwrap();
            g.as_mut_slice()[0] = 0x11;
            g.page_id()
        };
        let pid1 = {
            let mut g = bpm.new_page().unwrap();
            g.as_mut_slice()[0] = 0x22;
            g.page_id()
        };

        // FIFO → LRU (warm)
        bpm.swap_policy(Box::new(LruReplacer::new(5)), SwapMode::Warm);
        assert_eq!(bpm.get_policy_name(), "lru");

        // LRU → Clock (cold)
        bpm.swap_policy(Box::new(ClockReplacer::new()), SwapMode::Cold);
        assert_eq!(bpm.get_policy_name(), "clock");

        // Clock → LRU-K (warm)
        bpm.swap_policy(Box::new(LruKReplacer::new(2)), SwapMode::Warm);
        assert_eq!(bpm.get_policy_name(), "lru-k");

        assert_eq!(bpm.stats().snapshot().policy_swaps, 3);

        // Data integrity preserved through all swaps
        {
            let g = bpm.fetch_page_read(pid0).unwrap();
            assert_eq!(g.as_slice()[0], 0x11);
        }
        {
            let g = bpm.fetch_page_read(pid1).unwrap();
            assert_eq!(g.as_slice()[0], 0x22);
        }
    }

    #[test]
    fn test_swap_during_active_workload() {
        use crate::buffer::replacer::LruReplacer;
        use std::sync::Arc;
        use std::thread;

        let (bpm, _dir) = create_test_bpm(10);
        let bpm = Arc::new(bpm);

        // Create pages
        let mut pids = vec![];
        for i in 0..5u8 {
            let mut g = bpm.new_page().unwrap();
            g.as_mut_slice()[0] = i;
            pids.push(g.page_id());
        }

        // Spawn readers in background
        let mut handles = vec![];
        for _ in 0..4 {
            let bpm_clone = Arc::clone(&bpm);
            let pids_clone = pids.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..20 {
                    for &pid in &pids_clone {
                        if let Ok(g) = bpm_clone.fetch_page_read(pid) {
                            let _ = g.as_slice()[0];
                        }
                    }
                }
            }));
        }

        // Swap while readers are active
        bpm.swap_policy(Box::new(LruReplacer::new(10)), SwapMode::Warm);

        for h in handles {
            h.join().unwrap();
        }

        // Verify data integrity after concurrent swap
        for (i, &pid) in pids.iter().enumerate() {
            let g = bpm.fetch_page_read(pid).unwrap();
            assert_eq!(g.as_slice()[0], i as u8);
        }
    }
}
