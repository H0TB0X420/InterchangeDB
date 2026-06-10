//! Leveled compaction for the LSM-tree.
//!
//! Compaction merges SSTables from one level into the next to reduce
//! read amplification and reclaim space from overwrites/deletions.
//!
//! Strategy:
//! - L0 → L1: Merge ALL L0 SSTables with overlapping L1 SSTables.
//! - Ln → Ln+1: Pick one SSTable from Ln, merge with overlapping Ln+1 SSTables.
//! - Cascading: After compaction, if the target level exceeds its size limit,
//!   compact it into the next level.
//! - Tombstones are dropped only at the bottom (highest populated) level.
//! - Output SSTables are split at ~TARGET_SSTABLE_SIZE_BYTES.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::Path;

use crate::common::error::Result;
use crate::index::lsm::config::{
    LEVEL_SIZE_RATIO, MAX_L0_SSTABLE_COUNT, MAX_LEVEL_COUNT, TARGET_SSTABLE_SIZE_BYTES,
};
use crate::index::lsm::manifest::{LevelState, Manifest};
use crate::index::lsm::merge_iterator::MergeIterator;
use crate::index::lsm::sstable::{write_sstable, SSTableMeta, SSTableReader};
use crate::index::lsm::Entry;

/// Check if compaction is needed and run it.
///
/// Called after each flush. May cascade through multiple levels.
pub fn maybe_compact(
    level_state: &mut LevelState,
    manifest: &mut Manifest,
    readers: &Mutex<HashMap<u64, SSTableReader>>,
    sst_dir: &Path,
    memtable_size_limit: usize,
) -> Result<()> {
    // Check L0 → L1.
    if level_state.levels[0].len() >= MAX_L0_SSTABLE_COUNT {
        compact_l0(level_state, manifest, readers, sst_dir)?;
    }

    // Check L1+ for cascading.
    for level in 1..MAX_LEVEL_COUNT - 1 {
        let limit = level_size_limit(level, memtable_size_limit);
        if level_state.level_size(level) > limit {
            compact_level(level, level_state, manifest, readers, sst_dir)?;
        }
    }

    Ok(())
}

/// L0 → L1 compaction: merge ALL L0 SSTables with overlapping L1 SSTables.
fn compact_l0(
    level_state: &mut LevelState,
    manifest: &mut Manifest,
    readers: &Mutex<HashMap<u64, SSTableReader>>,
    sst_dir: &Path,
) -> Result<()> {
    if level_state.levels[0].is_empty() {
        return Ok(());
    }

    // Determine the key range of all L0 SSTables.
    let mut min_key = level_state.levels[0][0].first_key.clone();
    let mut max_key = level_state.levels[0][0].last_key.clone();
    for meta in &level_state.levels[0] {
        if meta.first_key < min_key {
            min_key = meta.first_key.clone();
        }
        if meta.last_key > max_key {
            max_key = meta.last_key.clone();
        }
    }

    // Find overlapping L1 SSTables.
    let (l1_overlapping_ids, l1_remaining): (Vec<_>, Vec<_>) = level_state.levels[1]
        .iter()
        .cloned()
        .partition(|m| m.last_key >= min_key && m.first_key <= max_key);

    // Collect all entries from L0 (newest first) and overlapping L1.
    let mut sources = Vec::new();
    let mut input_ids: Vec<u64> = Vec::new();

    // L0 SSTables newest first.
    {
        let mut readers_mut = readers.lock();
        for meta in level_state.levels[0].iter().rev() {
            if let Some(reader) = readers_mut.get_mut(&meta.id) {
                sources.push(reader.iter()?);
                input_ids.push(meta.id);
            }
        }

        // Overlapping L1 SSTables (lower priority than L0).
        for meta in &l1_overlapping_ids {
            if let Some(reader) = readers_mut.get_mut(&meta.id) {
                sources.push(reader.iter()?);
                input_ids.push(meta.id);
            }
        }
    }

    // Merge.
    let is_bottom_level = is_bottom_level(1, level_state);
    let merged = MergeIterator::new(sources).collect_all()?;

    // Write output SSTables, splitting at TARGET_SSTABLE_SIZE_BYTES.
    let new_metas = write_split_sstables(merged, level_state, sst_dir, is_bottom_level)?;

    // Update manifest: delete old, add new.
    for id in &input_ids {
        manifest.log_del(*id)?;
    }
    for meta in &new_metas {
        manifest.log_add(1, meta)?;
    }

    // Close old readers, open new ones.
    {
        let mut readers_mut = readers.lock();
        for id in &input_ids {
            readers_mut.remove(id);
        }
        for meta in &new_metas {
            let reader = SSTableReader::open(&meta.path, meta.id)?;
            readers_mut.insert(meta.id, reader);
        }
    }

    // Delete old SSTable files.
    for id in &input_ids {
        let old_path = sst_dir.join(format!("{id:06}.sst"));
        let _ = std::fs::remove_file(old_path);
    }

    // Update level state.
    level_state.levels[0].clear();
    level_state.levels[1] = l1_remaining;
    // Insert new SSTables into L1 in sorted order.
    for meta in new_metas {
        let pos = level_state.levels[1].partition_point(|m| m.first_key < meta.first_key);
        level_state.levels[1].insert(pos, meta);
    }

    Ok(())
}

/// Ln → Ln+1 compaction: pick one SSTable from level, merge with overlapping
/// SSTables in the next level.
fn compact_level(
    level: usize,
    level_state: &mut LevelState,
    manifest: &mut Manifest,
    readers: &Mutex<HashMap<u64, SSTableReader>>,
    sst_dir: &Path,
) -> Result<()> {
    assert!((1..MAX_LEVEL_COUNT - 1).contains(&level));

    if level_state.levels[level].is_empty() {
        return Ok(());
    }

    let target_level = level + 1;

    // Pick the first SSTable from this level (simple strategy).
    let picked = level_state.levels[level][0].clone();
    let min_key = &picked.first_key;
    let max_key = &picked.last_key;

    // Find overlapping SSTables in target level.
    let (target_overlapping, target_remaining): (Vec<_>, Vec<_>) = level_state.levels[target_level]
        .iter()
        .cloned()
        .partition(|m| m.last_key >= *min_key && m.first_key <= *max_key);

    // Collect entries. Picked SSTable has higher priority.
    let mut sources = Vec::new();
    let mut input_ids = Vec::new();

    {
        let mut readers_mut = readers.lock();

        if let Some(reader) = readers_mut.get_mut(&picked.id) {
            sources.push(reader.iter()?);
            input_ids.push(picked.id);
        }

        for meta in &target_overlapping {
            if let Some(reader) = readers_mut.get_mut(&meta.id) {
                sources.push(reader.iter()?);
                input_ids.push(meta.id);
            }
        }
    }

    let is_bottom = is_bottom_level(target_level, level_state);
    let merged = MergeIterator::new(sources).collect_all()?;

    let new_metas = write_split_sstables(merged, level_state, sst_dir, is_bottom)?;

    // Update manifest.
    for id in &input_ids {
        manifest.log_del(*id)?;
    }
    for meta in &new_metas {
        manifest.log_add(target_level, meta)?;
    }

    // Close old readers, open new ones.
    {
        let mut readers_mut = readers.lock();
        for id in &input_ids {
            readers_mut.remove(id);
        }
        for meta in &new_metas {
            let reader = SSTableReader::open(&meta.path, meta.id)?;
            readers_mut.insert(meta.id, reader);
        }
    }

    // Delete old files.
    for id in &input_ids {
        let old_path = sst_dir.join(format!("{id:06}.sst"));
        let _ = std::fs::remove_file(old_path);
    }

    // Update level state.
    // Remove picked from source level.
    level_state.levels[level].retain(|m| m.id != picked.id);
    // Replace target level.
    level_state.levels[target_level] = target_remaining;
    for meta in new_metas {
        let pos =
            level_state.levels[target_level].partition_point(|m| m.first_key < meta.first_key);
        level_state.levels[target_level].insert(pos, meta);
    }

    Ok(())
}

/// Write merged entries into one or more SSTables, splitting at TARGET_SSTABLE_SIZE_BYTES.
///
/// If `is_bottom_level` is true, tombstones are dropped.
fn write_split_sstables(
    entries: Vec<Entry>,
    level_state: &mut LevelState,
    sst_dir: &Path,
    is_bottom_level: bool,
) -> Result<Vec<SSTableMeta>> {
    let mut output_metas = Vec::new();
    let mut current_batch: Vec<Entry> = Vec::new();
    let mut current_size: usize = 0;

    for (key, value) in entries {
        // Drop tombstones at bottom level.
        if is_bottom_level && value.is_none() {
            continue;
        }

        let entry_size = key.len() + value.as_ref().map_or(0, |v| v.len()) + 4; // +4 for length headers.
        current_batch.push((key, value));
        current_size += entry_size;

        if current_size >= TARGET_SSTABLE_SIZE_BYTES {
            let meta = flush_batch(&mut current_batch, level_state, sst_dir)?;
            if let Some(m) = meta {
                output_metas.push(m);
            }
            current_size = 0;
        }
    }

    // Flush remaining.
    if !current_batch.is_empty() {
        let meta = flush_batch(&mut current_batch, level_state, sst_dir)?;
        if let Some(m) = meta {
            output_metas.push(m);
        }
    }

    Ok(output_metas)
}

/// Write a batch of entries to a new SSTable file.
fn flush_batch(
    batch: &mut Vec<Entry>,
    level_state: &mut LevelState,
    sst_dir: &Path,
) -> Result<Option<SSTableMeta>> {
    let sst_id = level_state.next_id();
    let path = sst_dir.join(format!("{sst_id:06}.sst"));
    let entries = batch.drain(..);
    write_sstable(&path, sst_id, entries)
}

/// Compute the size limit for a given level.
///
/// L1 limit = MAX_L0_SSTABLE_COUNT * memtable_size_limit (e.g., 4 * 4MB = 16MB).
/// Each subsequent level = previous * LEVEL_SIZE_RATIO.
fn level_size_limit(level: usize, memtable_size_limit: usize) -> u64 {
    assert!(level >= 1, "L0 has a count limit, not a size limit");
    let l1_limit = MAX_L0_SSTABLE_COUNT as u64 * memtable_size_limit as u64;
    let mut limit = l1_limit;
    for _ in 1..level {
        limit *= LEVEL_SIZE_RATIO as u64;
    }
    limit
}

/// Check if a level is the bottom (highest populated) level.
///
/// Tombstones should only be dropped at the bottom level.
fn is_bottom_level(level: usize, level_state: &LevelState) -> bool {
    for l in (level + 1)..MAX_LEVEL_COUNT {
        if !level_state.levels[l].is_empty() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::lsm::LsmTree;
    use tempfile::TempDir;

    #[test]
    fn level_size_limits() {
        let memtable_size = 4 * 1024 * 1024; // 4MB.
        assert_eq!(level_size_limit(1, memtable_size), 16 * 1024 * 1024); // 16MB.
        assert_eq!(level_size_limit(2, memtable_size), 160 * 1024 * 1024); // 160MB.
        assert_eq!(level_size_limit(3, memtable_size), 1600 * 1024 * 1024); // 1.6GB.
    }

    #[test]
    fn is_bottom_level_check() {
        let mut state = LevelState::new();
        // No SSTables anywhere — level 0 is bottom.
        assert!(is_bottom_level(0, &state));

        // Add something to L1.
        state.levels[1].push(SSTableMeta {
            id: 1,
            path: "dummy.sst".into(),
            first_key: vec![],
            last_key: vec![],
            file_size: 100,
            entry_count: 1,
        });
        assert!(!is_bottom_level(0, &state));
        assert!(is_bottom_level(1, &state));
    }

    #[test]
    fn flush_creates_l0() {
        let dir = TempDir::new().unwrap();
        let tree = LsmTree::open(dir.path()).unwrap();
        tree.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        tree.flush_memtable().unwrap();

        assert_eq!(tree.level_state().levels[0].len(), 1);
        assert_eq!(tree.level_state().levels[0][0].id, 1);
    }

    #[test]
    fn empty_flush_no_op() {
        let dir = TempDir::new().unwrap();
        let tree = LsmTree::open(dir.path()).unwrap();
        tree.flush_memtable().unwrap();
        assert!(tree.level_state().levels[0].is_empty());
    }

    #[test]
    fn l0_compaction_triggered() {
        let dir = TempDir::new().unwrap();
        // Use a small memtable (64 bytes) so we can trigger multiple flushes quickly.
        let tree = LsmTree::open_with_memtable_size(dir.path(), 64).unwrap();

        // Insert enough data to trigger MAX_L0_SSTABLE_COUNT flushes.
        // With 64-byte limit, a single key-value pair of ~40 bytes should flush.
        for i in 0u32..20 {
            let key = format!("key_{i:04}").into_bytes();
            let value = vec![b'v'; 30];
            tree.put(key, value).unwrap();
        }

        // Force-flush any remaining in memtable.
        tree.flush_memtable().unwrap();

        // After compaction, data should have moved below L0.
        // With cascading, it may be in L1, L2, or deeper.
        let l0_count = tree.level_state().levels[0].len();
        let below_l0_count: usize = tree.level_state().levels[1..].iter().map(|l| l.len()).sum();

        assert!(
            l0_count < MAX_L0_SSTABLE_COUNT,
            "L0 should be compacted: {l0_count} SSTables still in L0"
        );
        assert!(
            below_l0_count > 0,
            "levels below L0 should have SSTables after compaction"
        );

        // All keys should still be readable.
        for i in 0u32..20 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(
                tree.get(&key).unwrap().is_some(),
                "key_{i:04} should be readable after compaction"
            );
        }
    }

    #[test]
    fn compaction_deduplication() {
        let dir = TempDir::new().unwrap();
        let tree = LsmTree::open_with_memtable_size(dir.path(), 64).unwrap();

        // Write the same key multiple times across flushes.
        for i in 0..10 {
            let value = format!("v{i}").into_bytes();
            tree.put(b"same_key".to_vec(), value).unwrap();
            tree.flush_memtable().unwrap();
        }

        // The latest value should be readable.
        assert_eq!(tree.get(b"same_key").unwrap(), Some(b"v9".to_vec()));

        // Scan should return exactly one entry.
        let results = tree.scan::<std::ops::RangeFull>(..).unwrap();
        let same_key_entries: Vec<_> = results.iter().filter(|(k, _)| k == b"same_key").collect();
        assert_eq!(same_key_entries.len(), 1);
        assert_eq!(same_key_entries[0].1, b"v9");
    }

    #[test]
    fn tombstone_preservation() {
        let dir = TempDir::new().unwrap();
        let tree = LsmTree::open_with_memtable_size(dir.path(), 64).unwrap();

        // Insert a key, flush, delete it, flush again.
        tree.put(b"victim".to_vec(), b"alive".to_vec()).unwrap();
        tree.flush_memtable().unwrap();
        tree.delete(b"victim".to_vec()).unwrap();
        tree.flush_memtable().unwrap();

        // Key should be gone.
        assert_eq!(tree.get(b"victim").unwrap(), None);
    }
}
