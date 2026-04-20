//! Storage engine trait and utilities.
//!
//! This module defines the `StorageEngine` trait that all storage backends implement.
//! Currently supports compile-time selection via generics, with the trait designed
//! to also support runtime swapping via `Box<dyn StorageEngine>` if needed.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                  StorageEngine                       │
//! │  ┌─────────────────┐    ┌─────────────────────────┐ │
//! │  │     B-tree      │ OR │       LSM-tree          │ │
//! │  │  (point lookup) │    │  (write-optimized)      │ │
//! │  └─────────────────┘    └─────────────────────────┘ │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage Modes
//!
//! **Compile-time selection (zero-cost):**
//! ```ignore
//! struct Database<E: StorageEngine> {
//!     engine: E,
//! }
//! type BTreeDatabase = Database<BTreeEngine>;
//! ```
//!
//! **Runtime selection (flexible):**
//! ```ignore
//! struct Database {
//!     engine: Box<dyn StorageEngine>,
//! }
//! ```
//!
//! ## Implementations
//!
//! - `BTreeEngine` - B+tree index (Task 3.2)
//! - `LsmEngine` - LSM-tree with compaction (Task 3.4)

use std::ops::RangeBounds;

use crate::common::Result;

/// Iterator for scanning key-value pairs.
///
/// This trait follows the ToyDB pattern: a double-ended iterator that yields
/// `Result<(key, value)>` pairs. The `DoubleEndedIterator` bound enables
/// both forward and reverse scans.
///
/// ## Blanket Implementation
///
/// Any iterator satisfying the bounds automatically implements `ScanIterator`,
/// so storage engines can return their native iterator types.
///
/// ## Example
///
/// ```ignore
/// let mut iter = engine.scan(b"a".to_vec()..b"z".to_vec());
///
/// // Forward scan
/// while let Some(Ok((key, value))) = iter.next() {
///     println!("{:?} = {:?}", key, value);
/// }
///
/// // Reverse scan
/// while let Some(Ok((key, value))) = iter.next_back() {
///     println!("{:?} = {:?}", key, value);
/// }
/// ```
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

// Blanket implementation: any qualifying iterator is a ScanIterator
impl<T> ScanIterator for T where T: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

/// Core storage engine interface.
///
/// All storage backends (B-tree, LSM-tree, etc.) implement this trait.
/// The trait is designed for both compile-time generics and runtime
/// trait objects (`Box<dyn StorageEngine>`).
///
/// ## Thread Safety
///
/// The `Send` bound allows engines to be transferred between threads.
/// Internal synchronization (if needed) is the engine's responsibility.
///
/// ## Key-Value Model
///
/// - Keys and values are arbitrary byte slices
/// - Keys are ordered lexicographically
/// - Duplicate keys are not allowed (put overwrites)
/// - Delete uses tombstones (actual removal deferred to compaction)
///
/// ## Error Handling
///
/// All operations return `Result<T>` to handle I/O errors, corruption, etc.
pub trait StorageEngine: Send + Sync {
    /// Get the engine's name (e.g., "btree", "lsm").
    fn name(&self) -> &'static str;

    /// Retrieve a value by key.
    ///
    /// Returns `Ok(Some(value))` if found, `Ok(None)` if not found,
    /// or `Err` on I/O or corruption errors.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Insert or update a key-value pair.
    ///
    /// If the key exists, its value is overwritten.
    /// Takes `&self` — engines use interior mutability for thread safety.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key.
    ///
    /// Typically writes a tombstone marker; actual removal happens
    /// during compaction. Deleting a non-existent key is a no-op.
    /// Takes `&self` — engines use interior mutability for thread safety.
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Scan a range of keys.
    ///
    /// Returns an iterator over `(key, value)` pairs in sorted order.
    /// The range bounds follow Rust's `RangeBounds` trait:
    /// - `..` — all keys
    /// - `start..end` — keys in [start, end)
    /// - `start..=end` — keys in [start, end]
    /// - `start..` — keys >= start
    /// - `..end` — keys < end
    ///
    /// ## Lifetime
    ///
    /// The returned iterator borrows from the engine. For `Box<dyn StorageEngine>`,
    /// this means the iterator cannot outlive the boxed engine.
    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Box<dyn ScanIterator + '_>;

    /// Get engine statistics.
    fn status(&self) -> StorageStatus;

    /// Flush any buffered writes to durable storage.
    ///
    /// For LSM: flushes memtable to SSTable.
    /// For B-tree: flushes dirty pages via buffer pool.
    fn flush(&self) -> Result<()>;

    /// Export engine state for warm transfer to another engine.
    ///
    /// Returns an iterator over all key-value pairs. Used when
    /// swapping engines at runtime to preserve data.
    ///
    /// Default implementation scans all keys.
    fn export_data(&self) -> Box<dyn ScanIterator + '_> {
        self.scan(..)
    }

    /// Import data from another engine's export.
    ///
    /// Clears existing data and imports all key-value pairs from the iterator.
    /// Used for runtime engine swapping.
    fn import_data(&self, data: &mut dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>) -> Result<()> {
        for item in data {
            let (key, value) = item?;
            self.put(&key, &value)?;
        }
        Ok(())
    }
}

/// Storage engine statistics.
///
/// Provides insight into engine state for monitoring and tuning.
#[derive(Debug, Clone)]
pub struct StorageStatus {
    /// Engine name (e.g., "btree", "lsm").
    pub name: &'static str,

    /// Number of live keys (excluding tombstones).
    pub keys: u64,

    /// Logical data size in bytes (sum of key + value lengths).
    pub size: u64,

    /// Total bytes on disk (includes metadata, indexes, garbage).
    pub disk_size: u64,

    /// Bytes on disk for live data only (excludes garbage).
    pub live_disk_size: u64,

    /// Number of tombstones (deleted but not yet compacted).
    pub tombstones: u64,
}

impl StorageStatus {
    /// Create a new status with the given engine name.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            keys: 0,
            size: 0,
            disk_size: 0,
            live_disk_size: 0,
            tombstones: 0,
        }
    }

    /// Bytes of garbage (dead data not yet reclaimed).
    ///
    /// Garbage = disk_size - live_disk_size
    pub fn garbage_bytes(&self) -> u64 {
        self.disk_size.saturating_sub(self.live_disk_size)
    }

    /// Percentage of disk space that is garbage.
    ///
    /// Returns 0.0 if disk_size is 0.
    pub fn garbage_percent(&self) -> f64 {
        if self.disk_size == 0 {
            0.0
        } else {
            (self.garbage_bytes() as f64 / self.disk_size as f64) * 100.0
        }
    }

    /// Space amplification factor.
    ///
    /// Ratio of disk_size to logical size. Lower is better.
    /// Returns 1.0 if size is 0.
    pub fn space_amplification(&self) -> f64 {
        if self.size == 0 {
            1.0
        } else {
            self.disk_size as f64 / self.size as f64
        }
    }
}

impl std::fmt::Display for StorageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageStatus {{ engine: {}, keys: {}, size: {}, disk: {}, garbage: {:.1}% }}",
            self.name,
            self.keys,
            self.size,
            self.disk_size,
            self.garbage_percent()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_status_new() {
        let status = StorageStatus::new("test");
        assert_eq!(status.name, "test");
        assert_eq!(status.keys, 0);
        assert_eq!(status.size, 0);
        assert_eq!(status.disk_size, 0);
        assert_eq!(status.live_disk_size, 0);
        assert_eq!(status.tombstones, 0);
    }

    #[test]
    fn test_garbage_bytes() {
        let status = StorageStatus {
            name: "test",
            keys: 100,
            size: 1000,
            disk_size: 2000,
            live_disk_size: 1500,
            tombstones: 10,
        };
        assert_eq!(status.garbage_bytes(), 500);
    }

    #[test]
    fn test_garbage_bytes_no_garbage() {
        let status = StorageStatus {
            name: "test",
            keys: 100,
            size: 1000,
            disk_size: 1500,
            live_disk_size: 1500,
            tombstones: 0,
        };
        assert_eq!(status.garbage_bytes(), 0);
    }

    #[test]
    fn test_garbage_bytes_underflow_protection() {
        let status = StorageStatus {
            name: "test",
            keys: 100,
            size: 1000,
            disk_size: 1000,
            live_disk_size: 1500, // live > disk (shouldn't happen, but be safe)
            tombstones: 0,
        };
        assert_eq!(status.garbage_bytes(), 0); // saturating_sub prevents underflow
    }

    #[test]
    fn test_garbage_percent() {
        let status = StorageStatus {
            name: "test",
            keys: 100,
            size: 1000,
            disk_size: 2000,
            live_disk_size: 1000,
            tombstones: 10,
        };
        assert!((status.garbage_percent() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_garbage_percent_zero_disk() {
        let status = StorageStatus::new("test");
        assert_eq!(status.garbage_percent(), 0.0);
    }

    #[test]
    fn test_space_amplification() {
        let status = StorageStatus {
            name: "test",
            keys: 100,
            size: 1000,
            disk_size: 3000,
            live_disk_size: 2000,
            tombstones: 10,
        };
        assert!((status.space_amplification() - 3.0).abs() < 0.01);
    }

    #[test]
    fn test_space_amplification_zero_size() {
        let status = StorageStatus::new("test");
        assert_eq!(status.space_amplification(), 1.0);
    }

    #[test]
    fn test_storage_status_display() {
        let status = StorageStatus {
            name: "btree",
            keys: 1000,
            size: 50000,
            disk_size: 60000,
            live_disk_size: 55000,
            tombstones: 50,
        };
        let display = format!("{}", status);
        assert!(display.contains("btree"));
        assert!(display.contains("1000"));
        assert!(display.contains("50000"));
        assert!(display.contains("60000"));
    }
}
