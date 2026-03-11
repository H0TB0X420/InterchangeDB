//! LSM-tree storage engine — wraps `LsmTree` in the `StorageEngine` trait.
//!
//! `LsmEngine` tracks key counts, data size, and tombstone counts via
//! the same get-before-mutate pattern as `BTreeEngine`.

use std::ops::RangeBounds;
use std::path::Path;

use crate::common::Result;
use crate::storage::engine::{ScanIterator, StorageEngine, StorageStatus};

use super::LsmTree;

/// LSM-tree storage engine.
///
/// Wraps an `LsmTree` with counter tracking for the `StorageEngine` trait.
pub struct LsmEngine {
    tree: LsmTree,
    key_count: u64,
    data_size: u64,
    tombstone_count: u64,
}

impl LsmEngine {
    /// Create a new LSM engine at the given directory.
    ///
    /// Scans existing SSTables to initialize key/data counters so that
    /// `status()` is accurate even after recovery or reopen.
    pub fn new(data_dir: &Path) -> Result<Self> {
        let tree = LsmTree::open(data_dir)?;
        let (key_count, data_size) = Self::count_existing_data(&tree)?;
        Ok(Self {
            tree,
            key_count,
            data_size,
            tombstone_count: 0,
        })
    }

    /// Create a new LSM engine with a custom memtable size limit.
    pub fn with_memtable_size(data_dir: &Path, memtable_size_limit: usize) -> Result<Self> {
        let tree = LsmTree::open_with_memtable_size(data_dir, memtable_size_limit)?;
        let (key_count, data_size) = Self::count_existing_data(&tree)?;
        Ok(Self {
            tree,
            key_count,
            data_size,
            tombstone_count: 0,
        })
    }

    /// Scan all live entries to compute initial key_count and data_size.
    fn count_existing_data(tree: &LsmTree) -> Result<(u64, u64)> {
        let entries = tree.scan(..)?;
        let mut key_count: u64 = 0;
        let mut data_size: u64 = 0;
        for (key, value) in &entries {
            key_count += 1;
            data_size += (key.len() + value.len()) as u64;
        }
        Ok((key_count, data_size))
    }
}

impl StorageEngine for LsmEngine {
    fn name(&self) -> &'static str {
        "lsm"
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.tree.get(key)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Check if key already exists for counter tracking.
        let old_value = self.tree.get(key)?;

        match old_value {
            Some(old_val) => {
                // Overwrite: adjust data_size for value change.
                self.data_size = self.data_size
                    .saturating_sub(old_val.len() as u64)
                    + value.len() as u64;
            }
            None => {
                // New key.
                self.key_count += 1;
                self.data_size += (key.len() + value.len()) as u64;
            }
        }

        self.tree.put(key.to_vec(), value.to_vec())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let old_value = self.tree.get(key)?;

        if let Some(old_val) = old_value {
            let entry_size = (key.len() + old_val.len()) as u64;
            // saturating_sub: on recovery, the engine counters start at 0 but the
            // tree may already contain keys from persisted SSTables. Deleting one
            // of those "uncounted" keys would underflow without saturation.
            self.key_count = self.key_count.saturating_sub(1);
            self.data_size = self.data_size.saturating_sub(entry_size);
            self.tombstone_count += 1;
        }

        self.tree.delete(key.to_vec())
    }

    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Box<dyn ScanIterator + '_> {
        match self.tree.scan(range) {
            Ok(entries) => {
                let results: Vec<Result<(Vec<u8>, Vec<u8>)>> =
                    entries.into_iter().map(Ok).collect();
                Box::new(results.into_iter())
            }
            Err(e) => Box::new(vec![Err(e)].into_iter()),
        }
    }

    fn status(&self) -> StorageStatus {
        let disk_size = self.tree.level_state().total_disk_size();

        StorageStatus {
            name: "lsm",
            keys: self.key_count,
            size: self.data_size,
            disk_size,
            live_disk_size: disk_size,
            tombstones: self.tombstone_count,
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.tree.flush_memtable()
    }
}
