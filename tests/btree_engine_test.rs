//! B-Tree StorageEngine integration tests.
//!
//! Tests exercise the `StorageEngine` trait interface only — no direct
//! `BTree` API calls. Verifies that `BTreeEngine` correctly wraps the
//! B+Tree with proper key counting, data size tracking, scan support,
//! and flush behavior.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::DiskManager;
use interchangedb::storage::engine::StorageEngine;
use tempfile::tempdir;

// =============================================================================
// Helpers
// =============================================================================

fn setup_engine() -> (BTreeEngine, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = DiskManager::create(&path).unwrap();
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
    let dm = DiskManager::create(&path).unwrap();
    let bpm = BufferPoolManager::new(256, dm);
    let engine = BTreeEngine::with_sizes(bpm, leaf_max, internal_max, max_tombstones).unwrap();
    (engine, dir)
}

// =============================================================================
// Tests
// =============================================================================

#[test]
fn test_engine_name() {
    let (engine, _dir) = setup_engine();
    assert_eq!(engine.name(), "btree");
}

#[test]
fn test_engine_put_get() {
    let (mut engine, _dir) = setup_engine();

    engine.put(b"hello", b"world").unwrap();
    engine.put(b"foo", b"bar").unwrap();
    engine.put(b"baz", b"qux").unwrap();

    assert_eq!(engine.get(b"hello").unwrap(), Some(b"world".to_vec()));
    assert_eq!(engine.get(b"foo").unwrap(), Some(b"bar".to_vec()));
    assert_eq!(engine.get(b"baz").unwrap(), Some(b"qux".to_vec()));
    assert_eq!(engine.get(b"missing").unwrap(), None);

    let status = engine.status();
    assert_eq!(status.keys, 3);
}

#[test]
fn test_engine_put_overwrite() {
    let (mut engine, _dir) = setup_engine();

    engine.put(b"key", b"value1").unwrap();
    assert_eq!(engine.get(b"key").unwrap(), Some(b"value1".to_vec()));

    // Overwrite with different value.
    engine.put(b"key", b"value2").unwrap();
    assert_eq!(engine.get(b"key").unwrap(), Some(b"value2".to_vec()));

    // Key count should still be 1.
    let status = engine.status();
    assert_eq!(status.keys, 1);

    // Data size should reflect the current key+value, not the old one.
    // "key" (3 bytes) + "value2" (6 bytes) = 9.
    assert_eq!(status.size, 9);
}

#[test]
fn test_engine_delete() {
    let (mut engine, _dir) = setup_engine();

    engine.put(b"a", b"1").unwrap();
    engine.put(b"b", b"2").unwrap();
    engine.put(b"c", b"3").unwrap();

    engine.delete(b"b").unwrap();
    assert_eq!(engine.get(b"b").unwrap(), None);

    // Remaining keys intact.
    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"c").unwrap(), Some(b"3".to_vec()));

    let status = engine.status();
    assert_eq!(status.keys, 2);
}

#[test]
fn test_engine_delete_nonexistent() {
    let (mut engine, _dir) = setup_engine();

    // Deleting a key that doesn't exist is a no-op, not an error.
    engine.delete(b"ghost").unwrap();

    let status = engine.status();
    assert_eq!(status.keys, 0);
}

#[test]
fn test_engine_scan_all() {
    let (mut engine, _dir) = setup_engine();

    engine.put(b"cherry", b"3").unwrap();
    engine.put(b"apple", b"1").unwrap();
    engine.put(b"banana", b"2").unwrap();

    // Full scan should return all entries in sorted order.
    let results: Vec<_> = engine.scan(..)
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (b"apple".to_vec(), b"1".to_vec()));
    assert_eq!(results[1], (b"banana".to_vec(), b"2".to_vec()));
    assert_eq!(results[2], (b"cherry".to_vec(), b"3".to_vec()));
}

#[test]
fn test_engine_scan_range() {
    let (mut engine, _dir) = setup_engine();

    for i in 0u8..10 {
        engine.put(&[i], &[i * 10]).unwrap();
    }

    // Scan [3, 7) — should return keys 3, 4, 5, 6.
    let results: Vec<_> = engine.scan(vec![3u8]..vec![7u8])
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].0, vec![3]);
    assert_eq!(results[3].0, vec![6]);

    // Scan [5, 8] inclusive — should return keys 5, 6, 7, 8.
    let results: Vec<_> = engine.scan(vec![5u8]..=vec![8u8])
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].0, vec![5]);
    assert_eq!(results[3].0, vec![8]);
}

#[test]
fn test_engine_scan_empty() {
    let (engine, _dir) = setup_engine();

    let results: Vec<_> = engine.scan(..)
        .map(|r| r.unwrap())
        .collect();

    assert!(results.is_empty());
}

#[test]
fn test_engine_status() {
    let (mut engine, _dir) = setup_engine();

    let status = engine.status();
    assert_eq!(status.name, "btree");
    assert_eq!(status.keys, 0);
    assert_eq!(status.size, 0);
    assert_eq!(status.tombstones, 0);
    // At least the header page is allocated.
    assert!(status.disk_size > 0);

    engine.put(b"key1", b"val1").unwrap();
    engine.put(b"key2", b"val2").unwrap();

    let status = engine.status();
    assert_eq!(status.keys, 2);
    // "key1" + "val1" + "key2" + "val2" = 4+4+4+4 = 16.
    assert_eq!(status.size, 16);

    engine.delete(b"key1").unwrap();

    let status = engine.status();
    assert_eq!(status.keys, 1);
    // "key2" + "val2" = 8.
    assert_eq!(status.size, 8);
}

#[test]
fn test_engine_flush() {
    let (mut engine, _dir) = setup_engine();

    engine.put(b"persist", b"me").unwrap();
    // Flush should not error.
    engine.flush().unwrap();
}

#[test]
fn test_engine_export_import() {
    // Create first engine and populate it.
    let dir = tempdir().unwrap();
    let path1 = dir.path().join("source.db");
    let dm1 = DiskManager::create(&path1).unwrap();
    let bpm1 = BufferPoolManager::new(128, dm1);
    let mut engine1 = BTreeEngine::new(bpm1).unwrap();

    engine1.put(b"alpha", b"1").unwrap();
    engine1.put(b"beta", b"2").unwrap();
    engine1.put(b"gamma", b"3").unwrap();

    // Export data from engine1.
    let exported: Vec<_> = engine1.export_data()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(exported.len(), 3);

    // Create second engine and import.
    let path2 = dir.path().join("target.db");
    let dm2 = DiskManager::create(&path2).unwrap();
    let bpm2 = BufferPoolManager::new(128, dm2);
    let mut engine2 = BTreeEngine::new(bpm2).unwrap();

    let mut iter = exported.into_iter().map(Ok);
    engine2.import_data(&mut iter).unwrap();

    // Verify identical contents.
    assert_eq!(engine2.get(b"alpha").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine2.get(b"beta").unwrap(), Some(b"2".to_vec()));
    assert_eq!(engine2.get(b"gamma").unwrap(), Some(b"3".to_vec()));

    let status = engine2.status();
    assert_eq!(status.keys, 3);
}

#[test]
fn test_engine_many_keys_with_splits() {
    // Use small node sizes to force many splits.
    let (mut engine, _dir) = setup_engine_with_sizes(3, 3, 0);

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
    let results: Vec<_> = engine.scan(..)
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 100);

    // Verify sorted order.
    for i in 0..99 {
        assert!(results[i].0 < results[i + 1].0);
    }
}
