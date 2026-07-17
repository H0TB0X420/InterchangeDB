//! LSM-tree engine-specific integration tests.
//!
//! Tests behavior unique to `LsmEngine` that cannot be covered by the
//! shared `database_test.rs` suite. Common StorageEngine behavior is tested
//! there via the `database_tests!` macro.

use interchangedb::engines::lsm::LsmEngine;
use interchangedb::storage::engine::StorageEngine;
use tempfile::tempdir;

// =============================================================================
// Helpers
// =============================================================================

fn setup_engine_small_memtable() -> (LsmEngine, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("lsm_data");
    let engine = LsmEngine::with_memtable_size(&data_dir, 256).unwrap();
    (engine, dir)
}

// =============================================================================
// LSM-specific tests
// =============================================================================

#[test]
fn test_engine_name() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("lsm_data");
    let engine = LsmEngine::new(&data_dir).unwrap();
    assert_eq!(engine.name(), "lsm");
}

#[test]
fn test_engine_tombstone_tracking() {
    // LSM tracks tombstones in status because they persist in SSTables until
    // compaction. BTree doesn't — its tombstones are internal leaf markers.
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("lsm_data");
    let engine = LsmEngine::new(&data_dir).unwrap();

    engine.put(b"a", b"1").unwrap();
    engine.put(b"b", b"2").unwrap();
    engine.put(b"c", b"3").unwrap();

    assert_eq!(engine.status().tombstones, 0);

    engine.delete(b"a").unwrap();
    assert_eq!(engine.status().tombstones, 1);

    engine.delete(b"b").unwrap();
    assert_eq!(engine.status().tombstones, 2);
}

#[test]
fn test_engine_compaction() {
    // Use small memtable to trigger flushes and compaction with fewer keys —
    // exercises LSM-specific SSTable creation, merge, and level management.
    let (engine, _dir) = setup_engine_small_memtable();

    for i in 0u32..100 {
        let key = format!("key_{i:04}").into_bytes();
        let value = vec![b'v'; 30];
        engine.put(&key, &value).unwrap();
    }

    engine.flush().unwrap();

    // All keys should be readable after compaction.
    for i in 0u32..100 {
        let key = format!("key_{i:04}").into_bytes();
        assert!(
            engine.get(&key).unwrap().is_some(),
            "key_{i:04} not found after compaction"
        );
    }

    assert_eq!(engine.status().keys, 100);
}
