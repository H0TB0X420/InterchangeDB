//! B-Tree engine-specific integration tests.
//!
//! Tests behavior unique to `BTreeEngine` that cannot be covered by the
//! shared `database_test.rs` suite. Common StorageEngine behavior is tested
//! there via the `database_tests!` macro.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::engine::StorageEngine;
use interchangedb::storage::MemoryDiskManager;

// =============================================================================
// Helpers
// =============================================================================

fn setup_engine() -> BTreeEngine {
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(128, dm);
    BTreeEngine::new(bpm).unwrap()
}

fn setup_engine_with_sizes(leaf_max: u16, internal_max: u16, max_tombstones: usize) -> BTreeEngine {
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(256, dm);
    BTreeEngine::with_sizes(bpm, leaf_max, internal_max, max_tombstones).unwrap()
}

// =============================================================================
// BTree-specific tests
// =============================================================================

#[test]
fn test_engine_name() {
    let engine = setup_engine();
    assert_eq!(engine.name(), "btree");
}

#[test]
fn test_engine_initial_disk_size() {
    // BTree allocates a header page on creation, so disk_size > 0 from the start.
    // LSM starts at 0 — this is a BTree-specific invariant.
    let engine = setup_engine();
    let status = engine.status();
    assert!(
        status.disk_size > 0,
        "btree should have header page on disk"
    );
}

#[test]
fn test_engine_status_size_tracks_entry_bytes_exactly() {
    // HOW: `status().size` is maintained by three separate accounting arms
    // in `put`/`delete` (insert adds key+value, overwrite swaps old value
    // for new, delete subtracts key+value). Walk the whole lifecycle with
    // DISTINCT key and value lengths so any arithmetic slip in any arm
    // (wrong operator, wrong operand) lands on a different number.
    let engine = setup_engine();

    engine.put(b"kk", &[0x11; 10]).unwrap(); // insert: 2 + 10
    let status = engine.status();
    assert_eq!(status.keys, 1);
    assert_eq!(status.size, 12);

    engine.put(b"kk", &[0x22; 30]).unwrap(); // overwrite: 12 − 10 + 30
    let status = engine.status();
    assert_eq!(status.keys, 1, "overwrite must not change key count");
    assert_eq!(status.size, 32);

    engine.put(b"j", &[0x33; 5]).unwrap(); // second insert: 32 + 1 + 5
    let status = engine.status();
    assert_eq!(status.keys, 2);
    assert_eq!(status.size, 38);

    engine.delete(b"kk").unwrap(); // delete: 38 − 2 − 30
    let status = engine.status();
    assert_eq!(status.keys, 1);
    assert_eq!(status.size, 6);

    engine.delete(b"j").unwrap(); // back to empty: everything cancels
    let status = engine.status();
    assert_eq!(status.keys, 0);
    assert_eq!(status.size, 0, "full lifecycle must return size to zero");
}

#[test]
fn test_engine_many_keys_with_splits() {
    // Use small node sizes to force many splits — exercises BTree-specific
    // split/merge/redistribute logic that LSM doesn't have.
    let engine = setup_engine_with_sizes(3, 3, 0);

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
