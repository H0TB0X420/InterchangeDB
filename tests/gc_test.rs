//! Garbage collection tests for MVCC version reclamation.
//!
//! Verifies that GC correctly identifies and removes old versions while
//! preserving the newest committed version per key and respecting active
//! snapshot boundaries.

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::DiskManager;
use interchangedb::txn::TxnMode;

fn setup() -> (Database<BTreeEngine>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = DiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (db, dir)
}

#[test]
fn gc_single_version_kept() {
    // One key, one committed version. GC keeps it (it's the newest below WM).
    let (db, _dir) = setup();
    db.put(b"key", b"value").unwrap();

    let stats = db.gc().unwrap();
    assert_eq!(stats.versions_removed, 0);

    // Key still readable.
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn gc_removes_old_versions() {
    // Write same key 5 times. GC should keep only the newest version.
    let (db, _dir) = setup();

    for i in 0..5 {
        let val = format!("v{}", i);
        db.put(b"key", val.as_bytes()).unwrap();
    }

    let stats = db.gc().unwrap();
    // 4 old versions removed, 1 kept (newest).
    assert_eq!(stats.versions_removed, 4, "Should remove 4 old versions");

    // Newest version still readable.
    assert_eq!(db.get(b"key").unwrap(), Some(b"v4".to_vec()));

    // Second GC run — nothing left to remove.
    let stats2 = db.gc().unwrap();
    assert_eq!(stats2.versions_removed, 0, "Second GC should be a no-op");
}

#[test]
fn gc_preserves_versions_above_watermark() {
    // Start a long-running read txn, then write new versions.
    // GC should not remove versions above the long-running txn's read_ts.
    let (db, _dir) = setup();

    db.put(b"key", b"v1").unwrap();

    // Long-running reader pins watermark.
    let reader = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let _val = db.txn_get(reader, b"key").unwrap();

    // Write new versions after reader's snapshot.
    db.put(b"key", b"v2").unwrap();
    db.put(b"key", b"v3").unwrap();

    // GC — watermark is pinned at reader's read_ts.
    // v2 and v3 are above watermark, v1 is below but it's the keeper.
    let stats = db.gc().unwrap();
    assert_eq!(stats.versions_removed, 0, "All versions should be kept while reader is active");

    // Reader still sees v1.
    let val = db.txn_get(reader, b"key").unwrap();
    assert_eq!(val, Some(b"v1".to_vec()));

    db.commit_txn(reader).unwrap();

    // Now GC can clean up — no active snapshots.
    let stats2 = db.gc().unwrap();
    assert!(stats2.versions_removed >= 2, "Should remove old versions after reader commits");

    // Latest version still visible.
    assert_eq!(db.get(b"key").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn gc_removes_aborted_versions() {
    // Write via txn and abort. GC should remove the aborted version.
    let (db, _dir) = setup();

    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"ghost", b"aborted_data").unwrap();
    db.txn_abort(txn).unwrap();

    // Version exists in engine but invisible.
    assert_eq!(db.get(b"ghost").unwrap(), None);

    let stats = db.gc().unwrap();
    assert_eq!(stats.versions_removed, 1, "Aborted version should be removed");

    // Still not visible (was never visible).
    assert_eq!(db.get(b"ghost").unwrap(), None);
}

#[test]
fn gc_tombstone_cleanup() {
    // Write key, commit. Delete key, commit. GC — tombstone is the keeper.
    // Second GC with no older versions... tombstone stays (conservative).
    // This documents current behavior: full tombstone cleanup needs two passes
    // or lookahead.
    let (db, _dir) = setup();

    db.put(b"key", b"value").unwrap();
    db.delete(b"key").unwrap();

    // Key is deleted.
    assert_eq!(db.get(b"key").unwrap(), None);

    // First GC: removes the old value version, tombstone is the keeper.
    let stats = db.gc().unwrap();
    assert!(stats.versions_removed >= 1 || stats.tombstones_removed > 0);

    // Key still deleted.
    assert_eq!(db.get(b"key").unwrap(), None);
}

#[test]
fn gc_multiple_keys() {
    // Multiple keys, each with multiple versions. GC keeps newest per key.
    let (db, _dir) = setup();

    for i in 0..5 {
        for v in 0..3 {
            let key = format!("k{}", i);
            let val = format!("v{}_{}", i, v);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
    }

    let stats = db.gc().unwrap();
    // 5 keys × 2 old versions each = 10 removed.
    assert_eq!(stats.versions_removed, 10);
    assert_eq!(stats.keys_processed, 5);

    // All keys still have their latest values.
    for i in 0..5 {
        let key = format!("k{}", i);
        let expected = format!("v{}_2", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(expected.into_bytes()));
    }
}

#[test]
fn gc_idempotent() {
    // Run GC multiple times — second and subsequent runs are no-ops.
    let (db, _dir) = setup();

    for i in 0..10 {
        let val = format!("v{}", i);
        db.put(b"key", val.as_bytes()).unwrap();
    }

    let stats1 = db.gc().unwrap();
    assert_eq!(stats1.versions_removed, 9);

    let stats2 = db.gc().unwrap();
    assert_eq!(stats2.versions_removed, 0, "Second GC should remove nothing");

    let stats3 = db.gc().unwrap();
    assert_eq!(stats3.versions_removed, 0, "Third GC should remove nothing");
}

#[test]
fn gc_does_not_corrupt_active_transaction() {
    // Begin a write txn, run GC, commit — data should be intact.
    let (db, _dir) = setup();

    db.put(b"existing", b"before").unwrap();

    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"new_key", b"new_val").unwrap();

    // GC while txn is active — should not touch txn's versions.
    let _stats = db.gc().unwrap();

    db.commit_txn(txn).unwrap();

    assert_eq!(db.get(b"new_key").unwrap(), Some(b"new_val".to_vec()));
    assert_eq!(db.get(b"existing").unwrap(), Some(b"before".to_vec()));
}

#[test]
fn gc_stats_accurate() {
    let (db, _dir) = setup();

    // 3 keys, 4 versions each = 12 total versions.
    for k in 0..3 {
        for v in 0..4 {
            let key = format!("key{}", k);
            let val = format!("val{}_{}", k, v);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
    }

    let stats = db.gc().unwrap();
    assert_eq!(stats.keys_processed, 3);
    assert_eq!(stats.versions_removed, 9); // 3 keys × 3 old versions
    assert_eq!(stats.versions_scanned, 12); // 3 keys × 4 versions total
}
