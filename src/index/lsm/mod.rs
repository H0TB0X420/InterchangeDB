//! LSM-tree storage engine.
//!
//! An append-oriented storage engine that writes to an in-memory memtable
//! and periodically flushes sorted runs (SSTables) to disk. Uses leveled
//! compaction to bound read amplification.
//!
//! Read path: memtable → immutable memtables → L0 SSTables → L1+ SSTables.
//! Write path: insert into memtable; flush when size threshold is reached.

pub mod bloom;
pub mod compaction;
pub mod config;
pub mod engine;
pub mod manifest;
pub mod memtable;
pub mod merge_iterator;
pub mod sstable;

pub use engine::LsmEngine;

/// A key-value entry where `None` value indicates a tombstone.
pub(crate) type Entry = (Vec<u8>, Option<Vec<u8>>);

/// A memtable frozen by a flush — the sorted map a memtable freezes to
/// (`None` value = tombstone). Shared via `Arc` between the reader-visible
/// immutable list and the off-lock SSTable writer (see `LsmTree::flush`).
type FrozenMemtable = std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>;

use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common::error::Result;
use config::DEFAULT_MEMTABLE_SIZE_BYTES;
use manifest::{LevelState, Manifest};
use memtable::Memtable;
use merge_iterator::MergeIterator;
use sstable::{write_sstable, SSTableReader};

/// Mutable state grouped behind a single Mutex for interior mutability.
///
/// `immutable_memtables` holds memtables that have been frozen by a flush but
/// not yet written to disk. The read path consults them between the active
/// memtable and L0, so a flush can drop the `inner` lock during its disk I/O
/// without the frozen keys ever becoming invisible. Frozen maps are `Arc`'d so
/// the off-lock writer can iterate the same map the readers see, with no clone
/// under the lock.
struct LsmInner {
    memtable: Memtable,
    immutable_memtables: Vec<Arc<FrozenMemtable>>,
    level_state: LevelState,
}

/// LSM-tree data structure.
///
/// Uses interior mutability via `Mutex<LsmInner>` for write state and
/// `Mutex<HashMap>` for SSTable readers. All public methods take `&self`.
pub struct LsmTree {
    inner: Mutex<LsmInner>,
    readers: Mutex<HashMap<u64, SSTableReader>>,
    /// The manifest lives outside `inner` so a flush can append + fsync it
    /// without holding the lock that every read takes.
    manifest: Mutex<Manifest>,
    /// Serializes flushes so SSTable ids, manifest records, and L0 order all
    /// agree, and bounds `immutable_memtables` to a single in-flight entry.
    /// Reads never take this lock.
    flush_lock: Mutex<()>,
    #[allow(dead_code)]
    data_dir: PathBuf,
    sst_dir: PathBuf,
    memtable_size_limit: usize,
}

impl LsmTree {
    /// Open or create an LSM-tree at the given directory.
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_with_memtable_size(data_dir, DEFAULT_MEMTABLE_SIZE_BYTES)
    }

    /// Open or create an LSM-tree with a custom memtable size limit.
    pub fn open_with_memtable_size(data_dir: &Path, memtable_size_limit: usize) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let sst_dir = data_dir.join("sst");
        std::fs::create_dir_all(&sst_dir)?;

        let manifest_path = data_dir.join("MANIFEST");
        let (manifest, level_state) = Manifest::open(&manifest_path, &sst_dir)?;

        // Open readers for all SSTables referenced in the manifest.
        let mut readers = HashMap::new();
        for level in &level_state.levels {
            for meta in level {
                if meta.path.exists() {
                    let reader = SSTableReader::open(&meta.path, meta.id)?;
                    readers.insert(meta.id, reader);
                }
            }
        }

        let inner = LsmInner {
            memtable: Memtable::new(),
            immutable_memtables: Vec::new(),
            level_state,
        };

        Ok(Self {
            inner: Mutex::new(inner),
            readers: Mutex::new(readers),
            manifest: Mutex::new(manifest),
            flush_lock: Mutex::new(()),
            data_dir: data_dir.to_path_buf(),
            sst_dir,
            memtable_size_limit,
        })
    }

    /// Insert a key-value pair. Flushes the memtable if it exceeds the size limit.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let over_limit = {
            let mut inner = self.inner.lock();
            inner.memtable.put(key, value);
            inner.memtable.size_bytes() >= self.memtable_size_limit
        };
        if over_limit {
            self.flush(false)?;
        }
        Ok(())
    }

    /// Delete a key by inserting a tombstone.
    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        let over_limit = {
            let mut inner = self.inner.lock();
            inner.memtable.delete(key);
            inner.memtable.size_bytes() >= self.memtable_size_limit
        };
        if over_limit {
            self.flush(false)?;
        }
        Ok(())
    }

    /// Look up a key. Returns the value, or `None` if not found (or deleted).
    ///
    /// Read path: memtable → immutable memtables → L0 → L1+.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Lock inner to read memtable and level_state snapshot.
        let inner = self.inner.lock();

        // 1. Check active memtable.
        if let Some(entry) = inner.memtable.get(key) {
            return match entry {
                Some(value) => Ok(Some(value.clone())),
                None => Ok(None), // Tombstone.
            };
        }

        // 2. Check immutable memtables (newest first).
        for imm in inner.immutable_memtables.iter().rev() {
            if let Some(entry) = imm.get(key) {
                return match entry {
                    Some(value) => Ok(Some(value.clone())),
                    None => Ok(None), // Tombstone.
                };
            }
        }

        // Clone level_state to release inner lock before acquiring readers lock.
        let level_state = inner.level_state.clone();
        drop(inner);

        // 3. Check L0 SSTables (newest first, may overlap).
        let mut readers = self.readers.lock();
        for meta in level_state.levels[0].iter().rev() {
            if let Some(reader) = readers.get_mut(&meta.id) {
                if !reader.may_contain(key) {
                    continue;
                }
                if let Some(result) = reader.get(key)? {
                    return match result {
                        Some(value) => Ok(Some(value)),
                        None => Ok(None), // Tombstone.
                    };
                }
            }
        }

        // 4. Check L1+ SSTables (non-overlapping within each level).
        for level_idx in 1..level_state.levels.len() {
            let level = &level_state.levels[level_idx];
            if level.is_empty() {
                continue;
            }

            // Binary search for the SSTable whose key range contains the key.
            let sst_idx = level.partition_point(|m| m.last_key.as_slice() < key);
            if sst_idx >= level.len() {
                continue;
            }
            let meta = &level[sst_idx];
            if key < meta.first_key.as_slice() {
                continue;
            }

            if let Some(reader) = readers.get_mut(&meta.id) {
                if !reader.may_contain(key) {
                    continue;
                }
                if let Some(result) = reader.get(key)? {
                    return match result {
                        Some(value) => Ok(Some(value)),
                        None => Ok(None), // Tombstone.
                    };
                }
            }
        }

        Ok(None)
    }

    /// Scan a range of keys. Returns entries in sorted order, excluding tombstones.
    pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Lock inner to snapshot memtable state.
        let inner = self.inner.lock();

        // Collect all sources for the merge iterator.
        let mut sources: Vec<Vec<Entry>> = Vec::new();

        // 1. Active memtable.
        let memtable_entries: Vec<Entry> = inner
            .memtable
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        sources.push(memtable_entries);

        // 2. Immutable memtables (newest first).
        for imm in inner.immutable_memtables.iter().rev() {
            let entries: Vec<Entry> = imm.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            sources.push(entries);
        }

        // Clone level_state, drop inner before acquiring readers.
        let level_state = inner.level_state.clone();
        drop(inner);

        // 3. L0 SSTables (newest first).
        let mut readers = self.readers.lock();
        for meta in level_state.levels[0].iter().rev() {
            if let Some(reader) = readers.get_mut(&meta.id) {
                let entries = reader.iter()?;
                sources.push(entries);
            }
        }

        // 4. L1+ SSTables (in order within each level).
        for level_idx in 1..level_state.levels.len() {
            for meta in &level_state.levels[level_idx] {
                if let Some(reader) = readers.get_mut(&meta.id) {
                    let entries = reader.iter()?;
                    sources.push(entries);
                }
            }
        }

        // Merge all sources.
        let merged = MergeIterator::new(sources).collect_all()?;

        // Filter by range and remove tombstones.
        let result: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .filter(|(key, _)| range.contains(key))
            .filter_map(|(key, value)| value.map(|v| (key, v)))
            .collect();

        Ok(result)
    }

    /// Force-flush the active memtable to an L0 SSTable.
    pub fn flush_memtable(&self) -> Result<()> {
        self.flush(true)
    }

    /// Flush the active memtable to an L0 SSTable without holding `inner` across
    /// the disk I/O.
    ///
    /// Reads (`get`/`scan`) only ever hold `inner` briefly; the slow part of a
    /// flush — writing the SSTable and fsyncing the manifest — must NOT run
    /// under `inner`, or every concurrent reader stalls behind it (the cause of
    /// LSM's negative read scaling under a write-mixed load). The flush runs in
    /// three phases:
    ///
    ///   1. Under `inner`: swap the active memtable into the immutable list and
    ///      install a fresh one. Reads now serve the frozen data from the
    ///      immutable list. O(1) — no disk I/O.
    ///   2. No locks held (serialized by `flush_lock`): write the SSTable and
    ///      append + fsync the manifest. Reads run concurrently.
    ///   3. Under `inner`: publish the SSTable into L0 and drop the now-durable
    ///      frozen memtable from the immutable list, atomically.
    ///
    /// `force` flushes a memtable that is below the size limit (explicit
    /// `flush_memtable`); a put-triggered flush passes `false` so a flush that
    /// lost the race to drain the memtable becomes a no-op.
    ///
    /// NOTE: `maybe_compact` still runs under `inner` in phase 3. Compaction is
    /// ~4x rarer than flush (L0 trigger) but rewrites whole levels; moving it
    /// off-lock is a separate, larger change tracked as a follow-up.
    fn flush(&self, force: bool) -> Result<()> {
        // Serialize flushes: keeps SSTable id / manifest / L0 order consistent
        // and bounds the immutable list to a single in-flight entry.
        let _flush = self.flush_lock.lock();

        // Phase 1 — swap under `inner` (O(1), no disk I/O).
        let (frozen, sst_id) = {
            let mut inner = self.inner.lock();
            if inner.memtable.is_empty() {
                return Ok(());
            }
            if !force && inner.memtable.size_bytes() < self.memtable_size_limit {
                // A racing flush already drained it below the limit.
                return Ok(());
            }
            let frozen = Arc::new(std::mem::take(&mut inner.memtable).freeze());
            inner.immutable_memtables.push(Arc::clone(&frozen));
            let sst_id = inner.level_state.next_id();
            (frozen, sst_id)
        };

        // Phase 2 — write the SSTable and fsync the manifest, no `inner` held.
        // The entries are cloned out of the shared frozen map (it must stay
        // readable in the immutable list until phase 3); the clone is off the
        // critical section and dwarfed by the disk write it feeds.
        let sst_path = self.sst_dir.join(format!("{sst_id:06}.sst"));
        let entries = frozen.iter().map(|(k, v)| (k.clone(), v.clone()));
        let meta = write_sstable(&sst_path, sst_id, entries)?;

        let reader = match &meta {
            Some(meta) => {
                self.manifest.lock().log_add(0, meta)?;
                Some(SSTableReader::open(&meta.path, meta.id)?)
            }
            None => None,
        };

        // Phase 3 — publish into L0 and retire the frozen memtable, under `inner`.
        {
            let mut inner = self.inner.lock();
            if let Some(meta) = meta {
                // The reader must be visible before the L0 entry that names it.
                self.readers.lock().insert(meta.id, reader.unwrap());
                inner.level_state.levels[0].push(meta);
                compaction::maybe_compact(
                    &mut inner.level_state,
                    &mut self.manifest.lock(),
                    &self.readers,
                    &self.sst_dir,
                    self.memtable_size_limit,
                )?;
            }
            inner
                .immutable_memtables
                .retain(|m| !Arc::ptr_eq(m, &frozen));
        }

        Ok(())
    }

    /// Access the level state (for status reporting).
    /// Returns a clone since the original is behind a Mutex.
    pub fn level_state(&self) -> LevelState {
        self.inner.lock().level_state.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_tree() -> (LsmTree, TempDir) {
        let dir = TempDir::new().unwrap();
        let tree = LsmTree::open(dir.path()).unwrap();
        (tree, dir)
    }

    #[test]
    fn put_get_memtable_only() {
        let (tree, _dir) = test_tree();
        tree.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
        assert_eq!(tree.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(tree.get(b"missing").unwrap(), None);
    }

    #[test]
    fn tombstone() {
        let (tree, _dir) = test_tree();
        tree.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        tree.delete(b"key".to_vec()).unwrap();
        assert_eq!(tree.get(b"key").unwrap(), None);
    }

    #[test]
    fn overwrite() {
        let (tree, _dir) = test_tree();
        tree.put(b"key".to_vec(), b"old".to_vec()).unwrap();
        tree.put(b"key".to_vec(), b"new".to_vec()).unwrap();
        assert_eq!(tree.get(b"key").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn scan_all() {
        let (tree, _dir) = test_tree();
        tree.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        tree.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        tree.put(b"b".to_vec(), b"2".to_vec()).unwrap();

        let results = tree.scan::<std::ops::RangeFull>(..).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(results[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn scan_range() {
        let (tree, _dir) = test_tree();
        for i in 0u8..10 {
            tree.put(vec![i], vec![i * 10]).unwrap();
        }

        let results = tree.scan(vec![3u8]..vec![7u8]).unwrap();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, vec![3]);
        assert_eq!(results[3].0, vec![6]);
    }

    #[test]
    fn scan_empty() {
        let (tree, _dir) = test_tree();
        let results = tree.scan::<std::ops::RangeFull>(..).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn flush_and_read() {
        let (tree, _dir) = test_tree();
        tree.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        tree.put(b"key2".to_vec(), b"val2".to_vec()).unwrap();

        // Force flush.
        tree.flush_memtable().unwrap();

        // Memtable should be empty now — verify via get returning from SSTable.
        // (Can't access inner.memtable directly from test anymore.)

        // Reads should still work via SSTable.
        assert_eq!(tree.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(tree.get(b"key2").unwrap(), Some(b"val2".to_vec()));
    }

    #[test]
    fn read_across_memtable_and_sstable() {
        let (tree, _dir) = test_tree();

        // Put some entries and flush.
        tree.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        tree.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        tree.flush_memtable().unwrap();

        // Put more entries in active memtable.
        tree.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        tree.put(b"b".to_vec(), b"2-new".to_vec()).unwrap();

        // Active memtable overwrite should win.
        assert_eq!(tree.get(b"b").unwrap(), Some(b"2-new".to_vec()));
        // SSTable entry still readable.
        assert_eq!(tree.get(b"a").unwrap(), Some(b"1".to_vec()));
        // New memtable entry.
        assert_eq!(tree.get(b"c").unwrap(), Some(b"3".to_vec()));

        // Scan should merge correctly.
        let results = tree.scan::<std::ops::RangeFull>(..).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[1].1, b"2-new"); // "b" should have new value.
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = TempDir::new().unwrap();

        // Write and flush.
        {
            let tree = LsmTree::open(dir.path()).unwrap();
            tree.put(b"persist".to_vec(), b"me".to_vec()).unwrap();
            tree.flush_memtable().unwrap();
        }

        // Reopen.
        {
            let tree = LsmTree::open(dir.path()).unwrap();
            assert_eq!(tree.get(b"persist").unwrap(), Some(b"me".to_vec()));
        }
    }
}
