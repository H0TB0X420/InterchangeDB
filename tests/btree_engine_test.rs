//! B-Tree engine-specific integration tests.
//!
//! Tests behavior unique to `BTreeEngine` that cannot be covered by the
//! shared `database_test.rs` suite. Common StorageEngine behavior is tested
//! there via the `database_tests!` macro.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::engine::StorageEngine;
use interchangedb::storage::FileDiskManager;
use tempfile::tempdir;

// =============================================================================
// Helpers
// =============================================================================

fn setup_engine() -> (BTreeEngine, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&path).unwrap();
    let bpm = BufferPoolManager::new(128, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    (engine, dir)
}

fn setup_engine_with_sizes(
    leaf_max: u16,
    internal_max: u16,
    max_tombstones: usize,
) -> (BTreeEngine, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&path).unwrap();
    let bpm = BufferPoolManager::new(256, dm);
    let engine = BTreeEngine::with_sizes(bpm, leaf_max, internal_max, max_tombstones).unwrap();
    (engine, dir)
}

// =============================================================================
// BTree-specific tests
// =============================================================================

#[test]
fn test_engine_name() {
    let (engine, _dir) = setup_engine();
    assert_eq!(engine.name(), "btree");
}

#[test]
fn test_engine_initial_disk_size() {
    // BTree allocates a header page on creation, so disk_size > 0 from the start.
    // LSM starts at 0 — this is a BTree-specific invariant.
    let (engine, _dir) = setup_engine();
    let status = engine.status();
    assert!(
        status.disk_size > 0,
        "btree should have header page on disk"
    );
}

#[test]
fn test_engine_many_keys_with_splits() {
    // Use small node sizes to force many splits — exercises BTree-specific
    // split/merge/redistribute logic that LSM doesn't have.
    let (engine, _dir) = setup_engine_with_sizes(3, 3, 0);

    // Insert 100 keys.
    for i in 0u16..100 {
        let key = i.to_be_bytes();
        let value = (i * 10).to_be_bytes();
        engine.put(&key, &value).unwrap();
    }

    let status = engine.status();
    assert_eq!(status.keys, 100);

    // All keys retrievable.
    for i in 0u16..100 {
        let key = i.to_be_bytes();
        let result = engine.get(&key).unwrap();
        assert!(result.is_some(), "key {} not found", i);
    }

    // Full scan returns all 100 in order.
    let results: Vec<_> = engine.scan(..).map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 100);

    // Verify sorted order.
    for i in 0..99 {
        assert!(results[i].0 < results[i + 1].0);
    }
}
