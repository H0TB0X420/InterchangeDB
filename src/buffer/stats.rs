//! Buffer pool statistics tracking.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

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
/// use interchangedb::BufferPoolStats;
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
/// use interchangedb::BufferPoolStats;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_new() {
        let stats = BufferPoolStats::new();
        assert_eq!(stats.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(stats.cache_misses.load(Ordering::Relaxed), 0);
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_stats_increment() {
        let stats = BufferPoolStats::new();

        stats.cache_hits.fetch_add(7, Ordering::Relaxed);
        stats.cache_misses.fetch_add(3, Ordering::Relaxed);

        assert_eq!(stats.hit_rate(), 0.7);
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = BufferPoolStats::new();
        stats.cache_hits.fetch_add(7, Ordering::Relaxed);
        stats.cache_misses.fetch_add(3, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.cache_hits, 7);
        assert_eq!(snapshot.cache_misses, 3);
        assert_eq!(snapshot.hit_rate(), 0.7);
    }

    #[test]
    fn test_stats_reset() {
        let stats = BufferPoolStats::new();
        stats.cache_hits.fetch_add(100, Ordering::Relaxed);

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

        assert!(display.contains("hits: 80"));
        assert!(display.contains("misses: 20"));
        assert!(display.contains("80.00%"));
    }
}