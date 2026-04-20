//! B-Tree storage engine — wraps the B+Tree in the `StorageEngine` trait.
//!
//! `BTreeEngine` owns a `BufferPoolManager` and creates `BTree` handles on
//! demand (cheap — just a reference + config). This avoids self-referential
//! struct problems since `BTree` borrows `&BPM`.

use std::ops::RangeBounds;

use crate::buffer::BufferPoolManager;
use crate::common::config::PAGE_SIZE;
use crate::common::{PageId, Result};
use crate::storage::engine::{ScanIterator, StorageEngine, StorageStatus};

use super::page_layout::{calculate_internal_max_size, calculate_leaf_max_size};
use super::tree::BTree;
use super::BTreeHeaderPage;

/// Default average key size for max_size calculation.
const DEFAULT_AVG_KEY_SIZE: usize = 32;

/// Default average value size for max_size calculation.
const DEFAULT_AVG_VALUE_SIZE: usize = 64;

/// B-Tree storage engine.
///
/// Provides key-value storage backed by a B+Tree index. Owns the buffer pool
/// and creates lightweight `BTree` handles for each operation.
pub struct BTreeEngine {
    /// Owns the buffer pool (and therefore the disk manager).
    bpm: BufferPoolManager,
    /// Page ID of the B-tree header page.
    header_page_id: PageId,
    /// Leaf node capacity.
    leaf_max_size: u16,
    /// Internal node capacity.
    internal_max_size: u16,
    /// Maximum tombstones per leaf node.
    max_tombstones: usize,
    /// Number of live keys (maintained on put/delete).
    key_count: u64,
    /// Logical data size in bytes (sum of key + value lengths).
    data_size: u64,
}

impl BTreeEngine {
    /// Create or open a B-tree engine with default node sizes.
    ///
    /// If the underlying disk already has pages (from a previous session),
    /// reads the existing header page (page 0) to recover the tree structure.
    /// Otherwise, allocates a fresh header page for a new tree.
    pub fn new(bpm: BufferPoolManager) -> Result<Self> {
        let leaf_max_size = calculate_leaf_max_size(DEFAULT_AVG_KEY_SIZE, DEFAULT_AVG_VALUE_SIZE);
        let internal_max_size = calculate_internal_max_size(DEFAULT_AVG_KEY_SIZE);

        if bpm.disk_page_count() > 0 {
            // Existing tree on disk — reopen by reading header from page 0.
            Self::open_existing(bpm, leaf_max_size, internal_max_size)
        } else {
            // Empty disk — create fresh tree.
            Self::build(bpm, leaf_max_size, internal_max_size, 0)
        }
    }

    /// Reopen an existing B-tree by reading its header page from disk.
    /// Falls back to creating fresh if page 0 doesn't look like a valid header.
    fn open_existing(
        bpm: BufferPoolManager,
        leaf_max_size: u16,
        internal_max_size: u16,
    ) -> Result<Self> {
        let header_page_id = PageId::new(0);

        // Check if page 0 has been initialized (non-zero content).
        let has_content = {
            match bpm.fetch_page_read(header_page_id) {
                Ok(guard) => {
                    let result = guard.as_slice().iter().take(16).any(|&b| b != 0);
                    drop(guard);
                    result
                }
                Err(_) => false,
            }
        };

        if has_content {
            Ok(Self {
                bpm,
                header_page_id,
                leaf_max_size,
                internal_max_size,
                max_tombstones: 0,
                key_count: 0,
                data_size: 0,
            })
        } else {
            Self::build(bpm, leaf_max_size, internal_max_size, 0)
        }
    }

    /// Create a new B-tree engine with explicit node sizes.
    ///
    /// Used for testing with small node sizes to force splits and merges.
    pub fn with_sizes(
        bpm: BufferPoolManager,
        leaf_max_size: u16,
        internal_max_size: u16,
        max_tombstones: usize,
    ) -> Result<Self> {
        Self::build(bpm, leaf_max_size, internal_max_size, max_tombstones)
    }

    /// Internal constructor — allocates header page and initializes state.
    fn build(
        bpm: BufferPoolManager,
        leaf_max_size: u16,
        internal_max_size: u16,
        max_tombstones: usize,
    ) -> Result<Self> {
        // Allocate the header page for the B-tree root pointer.
        let mut header_guard = bpm.new_page()?;
        let header_page_id = header_guard.page_id();
        let header = BTreeHeaderPage::new();
        header.encode(header_guard.as_mut_slice());
        drop(header_guard);

        Ok(Self {
            bpm,
            header_page_id,
            leaf_max_size,
            internal_max_size,
            max_tombstones,
            key_count: 0,
            data_size: 0,
        })
    }

    /// Create a `BTree` handle borrowing the buffer pool.
    fn tree(&self) -> BTree<'_> {
        if self.max_tombstones > 0 {
            BTree::with_tombstones(
                &self.bpm,
                self.header_page_id,
                self.leaf_max_size,
                self.internal_max_size,
                self.max_tombstones,
            )
        } else {
            BTree::with_sizes(
                &self.bpm,
                self.header_page_id,
                self.leaf_max_size,
                self.internal_max_size,
            )
        }
    }
}

impl StorageEngine for BTreeEngine {
    fn name(&self) -> &'static str {
        "btree"
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.tree().get(key)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let tree = self.tree();
        let inserted = tree.insert(key, value)?;

        if inserted {
            // New key (or revived tombstone).
            self.key_count += 1;
            self.data_size += (key.len() + value.len()) as u64;
        } else {
            // Key already exists — update by delete + re-insert.
            // Retrieve old value size for data_size tracking.
            let old_value_size = tree.get(key)?
                .map(|v| v.len())
                .unwrap_or(0);

            tree.delete(key)?;
            let reinserted = tree.insert(key, value)?;
            debug_assert!(reinserted, "re-insert after delete must succeed");

            // key_count unchanged (delete -1, insert +1 cancel out).
            // Adjust data_size for value size change.
            self.data_size = self.data_size
                .saturating_sub(old_value_size as u64)
                + value.len() as u64;
        }

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let tree = self.tree();

        // Get value size before delete for data_size tracking.
        let entry_size = tree.get(key)?
            .map(|v| (key.len() + v.len()) as u64)
            .unwrap_or(0);

        let deleted = tree.delete(key)?;

        if deleted {
            self.key_count -= 1;
            self.data_size = self.data_size.saturating_sub(entry_size);
        }

        Ok(())
    }

    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Box<dyn ScanIterator + '_> {
        // Collect into Vec to satisfy DoubleEndedIterator requirement.
        // A true reverse iterator would need prev_page_id traversal (deferred).
        let results: Vec<Result<(Vec<u8>, Vec<u8>)>> = match self.tree().scan(range) {
            Ok(iter) => iter.collect(),
            Err(e) => vec![Err(e)],
        };
        Box::new(results.into_iter())
    }

    fn status(&self) -> StorageStatus {
        let disk_size = self.bpm.disk_page_count() as u64 * PAGE_SIZE as u64;

        StorageStatus {
            name: "btree",
            keys: self.key_count,
            size: self.data_size,
            disk_size,
            // No garbage tracking yet — approximate as full disk usage.
            live_disk_size: disk_size,
            tombstones: 0,
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.bpm.flush_all_pages()
    }
}
