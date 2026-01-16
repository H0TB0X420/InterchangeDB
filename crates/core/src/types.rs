//! Core type definitions for InterchangeDB.

use crate::config::PAGE_SIZE;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// PAGE IDENTIFIERS
// ============================================================================

/// Identifies a page on disk.
///
/// Using `u32` allows for 4 billion pages:
/// - 4,294,967,296 pages × 4KB = 16TB maximum database size
///
/// This matches BusTub's `page_id_t` type.
///
/// # Example
/// ```
/// use interchangedb_core::PageId;
///
/// let page_id = PageId::new(42);
/// assert!(page_id.is_valid());
/// assert_eq!(page_id.0, 42);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u32);

impl PageId {
    /// Invalid/sentinel page ID.
    ///
    /// Used to represent "no page" or uninitialized state.
    pub const INVALID: PageId = PageId(u32::MAX);

    /// Create a new PageId.
    #[inline]
    pub fn new(id: u32) -> Self {
        PageId(id)
    }

    /// Check if this page ID is valid (not the sentinel value).
    #[inline]
    pub fn is_valid(&self) -> bool {
        *self != Self::INVALID
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "Page(INVALID)")
        } else {
            write!(f, "Page({})", self.0)
        }
    }
}

// ============================================================================
// FRAME IDENTIFIERS
// ============================================================================

/// Identifies a frame in the buffer pool.
///
/// Using `usize` because:
/// 1. Frames are stored in `Vec<Frame>`
/// 2. Direct indexing without casting: `frames[frame_id.0]`
/// 3. Matches Rust idioms for array/vector indexing
///
/// # Example
/// ```
/// use interchangedb_core::FrameId;
///
/// let frame_id = FrameId::new(5);
/// // Can use directly as index: frames[frame_id.0]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId(pub usize);

impl FrameId {
    /// Create a new FrameId.
    #[inline]
    pub fn new(id: usize) -> Self {
        FrameId(id)
    }
}

impl fmt::Display for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Frame({})", self.0)
    }
}

// ============================================================================
// PAGE DATA
// ============================================================================

/// A page of data (4KB, 4KB-aligned).
///
/// This is the fundamental unit of I/O between disk and memory.
/// The buffer pool manages these in frames.
///
/// # Memory Layout
/// - Size: 4096 bytes (4KB)
/// - Alignment: 4096 bytes (for efficient Direct I/O with O_DIRECT)
///
/// # Why 4KB Alignment?
/// Operating systems and storage devices work most efficiently with aligned I/O:
/// - SSDs have 4KB pages internally
/// - OS page cache uses 4KB pages
/// - O_DIRECT I/O requires alignment to sector size
///
/// # Clone Implementation
/// `Page` does NOT implement `Clone` in production code to match production
/// database behavior (copying 4KB is expensive and should be explicit).
/// A `#[cfg(test)]` Clone is provided for tests.
///
/// # Example
/// ```
/// use interchangedb_core::Page;
///
/// let mut page = Page::new();
/// page.as_mut_slice()[0] = 0xFF;
/// assert_eq!(page.as_slice()[0], 0xFF);
/// ```
#[repr(align(4096))]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a new zeroed page.
    #[inline]
    pub fn new() -> Self {
        Self {
            data: [0u8; PAGE_SIZE],
        }
    }

    /// Get immutable slice of page data.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable slice of page data.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Zero out the entire page.
    pub fn reset(&mut self) {
        self.data.fill(0);
    }

    /// Get the size of a page.
    #[inline]
    pub const fn size() -> usize {
        PAGE_SIZE
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

// Clone only available in tests - forces explicit copying in production
#[cfg(test)]
impl Clone for Page {
    fn clone(&self) -> Self {
        let mut new_page = Page::new();
        new_page.data.copy_from_slice(&self.data);
        new_page
    }
}

// ============================================================================
// BUFFER POOL STATISTICS
// ============================================================================

/// Statistics tracked by the buffer pool.
///
/// All fields are atomic for lock-free, thread-safe updates.
/// Multiple threads can increment counters without locks.
///
/// # Memory Ordering
/// We use `Ordering::Relaxed` for all operations because:
/// - We only need atomicity (no partial updates)
/// - We don't need synchronization between different counters
/// - Statistics are "eventually consistent" - exact ordering doesn't matter
///
/// # Example
/// ```
/// use interchangedb_core::BufferPoolStats;
/// use std::sync::atomic::Ordering;
///
/// let stats = BufferPoolStats::new();
/// stats.cache_hits.fetch_add(1, Ordering::Relaxed);
/// assert_eq!(stats.cache_hits.load(Ordering::Relaxed), 1);
/// ```
#[derive(Debug)]
pub struct BufferPoolStats {
    /// Number of times a page was found in the buffer pool.
    pub cache_hits: AtomicU64,

    /// Number of times a page had to be read from disk.
    pub cache_misses: AtomicU64,

    /// Number of times a page was evicted from the buffer pool.
    pub evictions: AtomicU64,

    /// Number of pages read from disk.
    pub pages_read: AtomicU64,

    /// Number of pages written to disk.
    pub pages_written: AtomicU64,
}

impl BufferPoolStats {
    /// Create a new stats tracker with all counters at zero.
    pub fn new() -> Self {
        Self {
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            pages_read: AtomicU64::new(0),
            pages_written: AtomicU64::new(0),
        }
    }

    /// Calculate cache hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get a snapshot of current statistics.
    ///
    /// This returns a non-atomic copy for display/logging.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            pages_read: self.pages_read.load(Ordering::Relaxed),
            pages_written: self.pages_written.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.pages_read.store(0, Ordering::Relaxed);
        self.pages_written.store(0, Ordering::Relaxed);
    }
}

impl Default for BufferPoolStats {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time snapshot of buffer pool statistics.
///
/// Unlike `BufferPoolStats`, this is not atomic and can be safely
/// printed, serialized, compared, etc.
///
/// # Example
/// ```
/// use interchangedb_core::BufferPoolStats;
///
/// let stats = BufferPoolStats::new();
/// // ... update stats ...
/// let snapshot = stats.snapshot();
/// println!("{}", snapshot);  // Can print safely
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub pages_read: u64,
    pub pages_written: u64,
}

impl StatsSnapshot {
    /// Calculate cache hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }
}

impl fmt::Display for StatsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Stats {{ hits: {}, misses: {}, evictions: {}, hit_rate: {:.2}% }}",
            self.cache_hits,
            self.cache_misses,
            self.evictions,
            self.hit_rate() * 100.0
        )
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_size_and_alignment() {
        // Verify page is exactly 4KB
        assert_eq!(std::mem::size_of::<Page>(), PAGE_SIZE);
        assert_eq!(std::mem::size_of::<Page>(), 4096);
        
        // Verify page is 4KB-aligned
        assert_eq!(std::mem::align_of::<Page>(), 4096);
    }

    #[test]
    fn test_page_id() {
        let pid = PageId::new(42);
        assert_eq!(pid.0, 42);
        assert!(pid.is_valid());

        // Test invalid page
        assert!(!PageId::INVALID.is_valid());
        assert_eq!(PageId::INVALID.0, u32::MAX);

        // Test ordering
        assert!(PageId::new(1) < PageId::new(2));
        assert!(PageId::new(5) > PageId::new(3));
    }

    #[test]
    fn test_frame_id() {
        let fid = FrameId::new(10);
        assert_eq!(fid.0, 10);
        
        // Test equality
        assert_eq!(FrameId::new(5), FrameId::new(5));
        assert_ne!(FrameId::new(5), FrameId::new(6));
    }

    #[test]
    fn test_page_operations() {
        let mut page = Page::new();

        // Initially all zeros
        assert_eq!(page.as_slice()[0], 0);
        assert_eq!(page.as_slice()[4095], 0);

        // Modify through mutable slice
        page.as_mut_slice()[0] = 0xFF;
        page.as_mut_slice()[100] = 0xAB;
        page.as_mut_slice()[4095] = 0xCD;

        // Read back
        assert_eq!(page.as_slice()[0], 0xFF);
        assert_eq!(page.as_slice()[100], 0xAB);
        assert_eq!(page.as_slice()[4095], 0xCD);

        // Reset
        page.reset();
        assert_eq!(page.as_slice()[0], 0);
        assert_eq!(page.as_slice()[100], 0);
        assert_eq!(page.as_slice()[4095], 0);
    }

    #[test]
    fn test_page_clone_in_tests() {
        // Clone is only available in test code
        let mut page = Page::new();
        page.as_mut_slice()[0] = 0xAB;

        let cloned = page.clone();
        assert_eq!(cloned.as_slice()[0], 0xAB);
        
        // Cloned page is independent
        assert_eq!(page.as_slice()[0], 0xAB);
    }

    #[test]
    fn test_stats() {
        let stats = BufferPoolStats::new();

        // Initial state - all zeros
        assert_eq!(stats.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(stats.hit_rate(), 0.0);

        // Increment counters
        stats.cache_hits.fetch_add(7, Ordering::Relaxed);
        stats.cache_misses.fetch_add(3, Ordering::Relaxed);

        // Check hit rate: 7/(7+3) = 0.7
        assert_eq!(stats.hit_rate(), 0.7);

        // Take snapshot
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.cache_hits, 7);
        assert_eq!(snapshot.cache_misses, 3);
        assert_eq!(snapshot.hit_rate(), 0.7);

        // Reset
        stats.reset();
        assert_eq!(stats.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_stats_display() {
        let stats = BufferPoolStats::new();
        stats.cache_hits.fetch_add(80, Ordering::Relaxed);
        stats.cache_misses.fetch_add(20, Ordering::Relaxed);
        stats.evictions.fetch_add(5, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        let display = format!("{}", snapshot);
        
        // Should contain key information
        assert!(display.contains("hits: 80"));
        assert!(display.contains("misses: 20"));
        assert!(display.contains("80.00%"));  // Hit rate
    }
}