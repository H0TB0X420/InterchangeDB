//! Shared database integration tests.
//!
//! Tests the `Database<E>` API for both B-tree and LSM engines. Each test
//! is generated twice — once per engine — via the `database_tests!` macro.
//! This verifies identical observable behavior regardless of engine choice.
//!
//! Engine-specific tests (small node sizes, compaction, etc.) remain in
//! their dedicated test files (`btree_engine_test.rs`, `lsm_engine_test.rs`).

use interchangedb::buffer::BufferPoolManager;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::engines::lsm::LsmEngine;
use interchangedb::storage::FileDiskManager;
use tempfile::{tempdir, TempDir};

// =============================================================================
// Macro: generate identical tests for any engine
// =============================================================================

/// Generates a module of tests for a given engine setup function.
///
/// The setup function must return `(Database<impl StorageEngine>, TempDir)`.
/// Tests exercise only the `Database` public API — no engine internals.
macro_rules! database_tests {
    ($mod_name:ident, $setup:expr) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn put_get() {
                let (db, _dir) = $setup();

                db.put(b"hello", b"world").unwrap();
                db.put(b"foo", b"bar").unwrap();
                db.put(b"baz", b"qux").unwrap();

                assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
                assert_eq!(db.get(b"foo").unwrap(), Some(b"bar".to_vec()));
                assert_eq!(db.get(b"baz").unwrap(), Some(b"qux".to_vec()));
                assert_eq!(db.get(b"missing").unwrap(), None);

                assert_eq!(db.status().keys, 3);
            }

            #[test]
            fn overwrite() {
                let (db, _dir) = $setup();

                db.put(b"key", b"value1").unwrap();
                assert_eq!(db.get(b"key").unwrap(), Some(b"value1".to_vec()));

                db.put(b"key", b"value2").unwrap();
                assert_eq!(db.get(b"key").unwrap(), Some(b"value2".to_vec()));

                // Key count should still be 1.
                let status = db.status();
                assert_eq!(status.keys, 1);
                // "key" (3) + "value2" (6) = 9.
                assert_eq!(status.size, 9);
            }

            #[test]
            fn delete() {
                let (db, _dir) = $setup();

                db.put(b"a", b"1").unwrap();
                db.put(b"b", b"2").unwrap();
                db.put(b"c", b"3").unwrap();

                db.delete(b"b").unwrap();
                assert_eq!(db.get(b"b").unwrap(), None);

                // Remaining keys intact.
                assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
                assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));

                assert_eq!(db.status().keys, 2);
            }

            #[test]
            fn delete_nonexistent() {
                let (db, _dir) = $setup();

                // Deleting a key that doesn't exist is a no-op.
                db.delete(b"ghost").unwrap();
                assert_eq!(db.status().keys, 0);
            }

            #[test]
            fn scan_all() {
                let (db, _dir) = $setup();

                db.put(b"cherry", b"3").unwrap();
                db.put(b"apple", b"1").unwrap();
                db.put(b"banana", b"2").unwrap();

                let results: Vec<_> = db.scan(..).map(|r| r.unwrap()).collect();

                assert_eq!(results.len(), 3);
                assert_eq!(results[0], (b"apple".to_vec(), b"1".to_vec()));
                assert_eq!(results[1], (b"banana".to_vec(), b"2".to_vec()));
                assert_eq!(results[2], (b"cherry".to_vec(), b"3".to_vec()));
            }

            #[test]
            fn scan_range() {
                let (db, _dir) = $setup();

                for i in 0u8..10 {
                    db.put(&[i], &[i * 10]).unwrap();
                }

                // Half-open range [3, 7).
                let results: Vec<_> = db.scan(vec![3u8]..vec![7u8]).map(|r| r.unwrap()).collect();
                assert_eq!(results.len(), 4);
                assert_eq!(results[0].0, vec![3]);
                assert_eq!(results[3].0, vec![6]);

                // Inclusive range [5, 8].
                let results: Vec<_> = db.scan(vec![5u8]..=vec![8u8]).map(|r| r.unwrap()).collect();
                assert_eq!(results.len(), 4);
                assert_eq!(results[0].0, vec![5]);
                assert_eq!(results[3].0, vec![8]);
            }

            #[test]
            fn scan_empty() {
                let (db, _dir) = $setup();

                let results: Vec<_> = db.scan(..).map(|r| r.unwrap()).collect();
                assert!(results.is_empty());
            }

            #[test]
            fn status_tracking() {
                let (db, _dir) = $setup();

                // Initial state.
                let status = db.status();
                assert_eq!(status.keys, 0);
                assert_eq!(status.size, 0);
                assert_eq!(status.tombstones, 0);

                // After inserts.
                db.put(b"key1", b"val1").unwrap();
                db.put(b"key2", b"val2").unwrap();

                let status = db.status();
                assert_eq!(status.keys, 2);
                // "key1"(4) + "val1"(4) + "key2"(4) + "val2"(4) = 16.
                assert_eq!(status.size, 16);

                // After delete.
                db.delete(b"key1").unwrap();

                let status = db.status();
                assert_eq!(status.keys, 1);
                // "key2"(4) + "val2"(4) = 8.
                assert_eq!(status.size, 8);
            }

            #[test]
            fn flush() {
                let (db, _dir) = $setup();

                db.put(b"persist", b"me").unwrap();
                db.flush().unwrap();

                // Data readable after flush.
                assert_eq!(db.get(b"persist").unwrap(), Some(b"me".to_vec()));
            }

            #[test]
            fn export_import() {
                let (db, _dir) = $setup();

                db.put(b"alpha", b"1").unwrap();
                db.put(b"beta", b"2").unwrap();
                db.put(b"gamma", b"3").unwrap();

                // Export all data.
                let exported: Vec<_> = db.export_data().map(|r| r.unwrap()).collect();
                assert_eq!(exported.len(), 3);

                // Import into a fresh database of the same type.
                let (db2, _dir2) = $setup();
                let mut iter = exported.into_iter().map(Ok);
                db2.import_data(&mut iter).unwrap();

                assert_eq!(db2.get(b"alpha").unwrap(), Some(b"1".to_vec()));
                assert_eq!(db2.get(b"beta").unwrap(), Some(b"2".to_vec()));
                assert_eq!(db2.get(b"gamma").unwrap(), Some(b"3".to_vec()));
                assert_eq!(db2.status().keys, 3);
            }

            #[test]
            fn many_keys() {
                let (db, _dir) = $setup();

                // Insert 200 keys.
                for i in 0u16..200 {
                    let key = i.to_be_bytes();
                    let value = (i * 10).to_be_bytes();
                    db.put(&key, &value).unwrap();
                }

                assert_eq!(db.status().keys, 200);

                // All keys retrievable.
                for i in 0u16..200 {
                    let key = i.to_be_bytes();
                    let result = db.get(&key).unwrap();
                    assert!(result.is_some(), "key {} not found", i);
                }

                // Scan returns all in sorted order.
                let results: Vec<_> = db.scan(..).map(|r| r.unwrap()).collect();
                assert_eq!(results.len(), 200);
                for i in 0..199 {
                    assert!(results[i].0 < results[i + 1].0);
                }
            }

            #[test]
            fn flush_and_read() {
                let (db, _dir) = $setup();

                db.put(b"key1", b"val1").unwrap();
                db.put(b"key2", b"val2").unwrap();
                db.flush().unwrap();

                // Data readable after flush.
                assert_eq!(db.get(b"key1").unwrap(), Some(b"val1".to_vec()));
                assert_eq!(db.get(b"key2").unwrap(), Some(b"val2".to_vec()));

                // New data after flush still works.
                db.put(b"key3", b"val3").unwrap();
                assert_eq!(db.get(b"key3").unwrap(), Some(b"val3".to_vec()));
            }

            #[test]
            fn mixed_operations() {
                let (db, _dir) = $setup();

                // Insert, overwrite, delete, then verify.
                db.put(b"a", b"1").unwrap();
                db.put(b"b", b"2").unwrap();
                db.put(b"c", b"3").unwrap();

                db.put(b"b", b"22").unwrap(); // Overwrite.
                db.delete(b"a").unwrap(); // Delete.

                assert_eq!(db.get(b"a").unwrap(), None);
                assert_eq!(db.get(b"b").unwrap(), Some(b"22".to_vec()));
                assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));

                let results: Vec<_> = db.scan(..).map(|r| r.unwrap()).collect();
                assert_eq!(results.len(), 2);
                assert_eq!(results[0], (b"b".to_vec(), b"22".to_vec()));
                assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));

                assert_eq!(db.status().keys, 2);
            }
        }
    };
}

// =============================================================================
// Engine setup functions
// =============================================================================

fn setup_btree() -> (Database<BTreeEngine>, TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&path).unwrap();
    let bpm = BufferPoolManager::new(128, dm);
    let db = Database::btree(bpm).unwrap();
    (db, dir)
}

fn setup_lsm() -> (Database<LsmEngine>, TempDir) {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("lsm_data");
    let db = Database::lsm(&data_dir).unwrap();
    (db, dir)
}

// =============================================================================
// Generate test suites for both engines
// =============================================================================

database_tests!(btree, setup_btree);
database_tests!(lsm, setup_lsm);

// =============================================================================
// Cross-engine transfer tests
// =============================================================================

#[test]
fn cross_engine_btree_to_lsm() {
    // Populate a B-tree database.
    let (btree_db, _dir1) = setup_btree();
    btree_db.put(b"alpha", b"1").unwrap();
    btree_db.put(b"beta", b"2").unwrap();
    btree_db.put(b"gamma", b"3").unwrap();

    // Export from B-tree.
    let exported: Vec<_> = btree_db.export_data().map(|r| r.unwrap()).collect();

    // Import into LSM.
    let (lsm_db, _dir2) = setup_lsm();
    let mut iter = exported.into_iter().map(Ok);
    lsm_db.import_data(&mut iter).unwrap();

    // Verify identical data.
    assert_eq!(lsm_db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
    assert_eq!(lsm_db.get(b"beta").unwrap(), Some(b"2".to_vec()));
    assert_eq!(lsm_db.get(b"gamma").unwrap(), Some(b"3".to_vec()));
    assert_eq!(lsm_db.status().keys, 3);
}

#[test]
fn cross_engine_lsm_to_btree() {
    // Populate an LSM database.
    let (lsm_db, _dir1) = setup_lsm();
    lsm_db.put(b"x", b"10").unwrap();
    lsm_db.put(b"y", b"20").unwrap();
    lsm_db.put(b"z", b"30").unwrap();

    // Export from LSM.
    let exported: Vec<_> = lsm_db.export_data().map(|r| r.unwrap()).collect();

    // Import into B-tree.
    let (btree_db, _dir2) = setup_btree();
    let mut iter = exported.into_iter().map(Ok);
    btree_db.import_data(&mut iter).unwrap();

    // Verify identical data.
    assert_eq!(btree_db.get(b"x").unwrap(), Some(b"10".to_vec()));
    assert_eq!(btree_db.get(b"y").unwrap(), Some(b"20".to_vec()));
    assert_eq!(btree_db.get(b"z").unwrap(), Some(b"30".to_vec()));
    assert_eq!(btree_db.status().keys, 3);
}
