//! Integration tests for the transactional Database API.
//!
//! Tests the full stack: Database<E> → TransactionManager → LockManager → WAL → Engine.
//! Covers begin/commit/abort lifecycle, undo correctness, locking, recovery,
//! and backward compatibility with auto-commit operations.

use std::path::Path;

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::Error;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::index::lsm::LsmEngine;
use interchangedb::storage::FileDiskManager;
use interchangedb::txn::TxnMode;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup_btree_wal() -> (Database<BTreeEngine>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (db, dir)
}

fn setup_lsm_wal() -> (Database<LsmEngine>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("lsm_data");
    std::fs::create_dir_all(&lsm_dir).unwrap();
    let engine = LsmEngine::new(&lsm_dir).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (db, dir)
}

fn setup_btree_no_wal() -> (Database<BTreeEngine>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    (Database::new(engine), dir)
}

/// Reopen a WAL-enabled BTree database from the same directory.
fn reopen_btree(dir: &Path) -> Database<BTreeEngine> {
    let db_path = dir.join("test.db");
    let dm = FileDiskManager::open(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    Database::open(dir, engine).unwrap()
}

// ---------------------------------------------------------------------------
// Backward compatibility — auto-commit still works
// ---------------------------------------------------------------------------

#[test]
fn auto_commit_backward_compat() {
    // Database::new() with no WAL — put/get/delete work as before.
    let (db, _dir) = setup_btree_no_wal();
    db.put(b"key", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    db.delete(b"key").unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);
}

#[test]
fn auto_commit_with_wal() {
    // Database::open() — auto-commit put/get/delete still work.
    let (db, _dir) = setup_btree_wal();
    db.put(b"key", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    db.delete(b"key").unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);
}

#[test]
fn txn_not_supported_without_wal() {
    // Database::new() cannot begin transactions.
    let (db, _dir) = setup_btree_no_wal();
    let result = db.begin_txn(TxnMode::ReadWrite);
    assert!(matches!(result, Err(Error::TxnNotSupported)));
}

// ---------------------------------------------------------------------------
// Basic transaction lifecycle
// ---------------------------------------------------------------------------

#[test]
fn begin_returns_valid_txn_id() {
    let (db, _dir) = setup_btree_wal();
    let txn_id = db.begin_txn(TxnMode::ReadWrite).unwrap();
    assert!(txn_id.is_valid());
    assert!(txn_id.0 > 0); // 0 is reserved for AUTO_COMMIT.
    db.commit_txn(txn_id).unwrap();
}

#[test]
fn explicit_txn_lifecycle_btree() {
    // Full cycle: begin → put → get → commit → verify.
    let (db, _dir) = setup_btree_wal();
    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"name", b"alice").unwrap();
    let val = db.txn_get(txn, b"name").unwrap();
    assert_eq!(val, Some(b"alice".to_vec()));
    db.commit_txn(txn).unwrap();

    // Visible after commit via auto-commit get.
    assert_eq!(db.get(b"name").unwrap(), Some(b"alice".to_vec()));
}

#[test]
fn explicit_txn_lifecycle_lsm() {
    // Same lifecycle on LSM engine — both engines work.
    let (db, _dir) = setup_lsm_wal();
    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"name", b"bob").unwrap();
    let val = db.txn_get(txn, b"name").unwrap();
    assert_eq!(val, Some(b"bob".to_vec()));
    db.commit_txn(txn).unwrap();

    assert_eq!(db.get(b"name").unwrap(), Some(b"bob".to_vec()));
}

#[test]
fn commit_makes_visible() {
    let (db, _dir) = setup_btree_wal();
    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"key", b"val").unwrap();
    db.commit_txn(txn).unwrap();

    // Auto-commit get sees the value.
    assert_eq!(db.get(b"key").unwrap(), Some(b"val".to_vec()));
}

// ---------------------------------------------------------------------------
// Abort undo
// ---------------------------------------------------------------------------

#[test]
fn abort_makes_invisible() {
    // Put via txn, abort → key should not exist.
    let (db, _dir) = setup_btree_wal();
    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"key", b"val").unwrap();
    db.txn_abort(txn).unwrap();

    assert_eq!(db.get(b"key").unwrap(), None);
}

#[test]
fn abort_undoes_all_writes() {
    // Txn writes 10 keys, aborts — none visible.
    let (db, _dir) = setup_btree_wal();
    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    for i in 0..10u32 {
        let key = format!("key_{}", i);
        let val = format!("val_{}", i);
        db.txn_put(txn, key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.txn_abort(txn).unwrap();

    for i in 0..10u32 {
        let key = format!("key_{}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            None,
            "key_{} should be absent after abort",
            i
        );
    }
}

#[test]
fn abort_restores_overwritten_value() {
    // Auto-commit put key=v1, txn puts key=v2, abort → key=v1 restored.
    let (db, _dir) = setup_btree_wal();
    db.put(b"key", b"v1").unwrap();

    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"key", b"v2").unwrap();
    assert_eq!(db.txn_get(txn, b"key").unwrap(), Some(b"v2".to_vec()));
    db.txn_abort(txn).unwrap();

    assert_eq!(db.get(b"key").unwrap(), Some(b"v1".to_vec()));
}

#[test]
fn abort_restores_deleted_value() {
    // Auto-commit put key=v1, txn deletes key, abort → key=v1 restored.
    let (db, _dir) = setup_btree_wal();
    db.put(b"key", b"original").unwrap();

    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_delete(txn, b"key").unwrap();
    db.txn_abort(txn).unwrap();

    assert_eq!(db.get(b"key").unwrap(), Some(b"original".to_vec()));
}

// ---------------------------------------------------------------------------
// Multi-operation transactions
// ---------------------------------------------------------------------------

#[test]
fn multi_op_commit() {
    let (db, _dir) = setup_btree_wal();
    // Pre-existing key to delete.
    db.put(b"c", b"to_delete").unwrap();

    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"a", b"1").unwrap();
    db.txn_put(txn, b"b", b"2").unwrap();
    db.txn_delete(txn, b"c").unwrap();
    db.commit_txn(txn).unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), None);
}

#[test]
fn multi_op_abort() {
    let (db, _dir) = setup_btree_wal();
    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"a", b"1").unwrap();
    db.txn_put(txn, b"b", b"2").unwrap();
    db.txn_abort(txn).unwrap();

    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap(), None);
}

// ---------------------------------------------------------------------------
// Read-only transactions
// ---------------------------------------------------------------------------

#[test]
fn read_only_txn_allows_get() {
    let (db, _dir) = setup_btree_wal();
    db.put(b"key", b"value").unwrap();

    let txn = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let val = db.txn_get(txn, b"key").unwrap();
    assert_eq!(val, Some(b"value".to_vec()));
    db.commit_txn(txn).unwrap();
}

#[test]
fn read_only_txn_rejects_put() {
    let (db, _dir) = setup_btree_wal();
    let txn = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let result = db.txn_put(txn, b"key", b"val");
    assert!(matches!(result, Err(Error::TxnReadOnly(_))));
    db.txn_abort(txn).unwrap();
}

#[test]
fn read_only_txn_rejects_delete() {
    let (db, _dir) = setup_btree_wal();
    let txn = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let result = db.txn_delete(txn, b"key");
    assert!(matches!(result, Err(Error::TxnReadOnly(_))));
    db.txn_abort(txn).unwrap();
}

// ---------------------------------------------------------------------------
// Auto-commit mixed with explicit transactions
// ---------------------------------------------------------------------------

#[test]
fn auto_commit_mixed_with_explicit() {
    let (db, _dir) = setup_btree_wal();

    // Auto-commit write.
    db.put(b"auto_key", b"auto_val").unwrap();

    // Explicit txn write.
    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(txn, b"txn_key", b"txn_val").unwrap();
    db.commit_txn(txn).unwrap();

    // Another auto-commit.
    db.put(b"auto_key2", b"auto_val2").unwrap();

    // All three visible.
    assert_eq!(db.get(b"auto_key").unwrap(), Some(b"auto_val".to_vec()));
    assert_eq!(db.get(b"txn_key").unwrap(), Some(b"txn_val".to_vec()));
    assert_eq!(db.get(b"auto_key2").unwrap(), Some(b"auto_val2".to_vec()));
}

// ---------------------------------------------------------------------------
// Recovery tests
// ---------------------------------------------------------------------------

#[test]
fn recovery_committed_txn() {
    // Commit a txn, "crash" (drop without clean shutdown), reopen → data visible.
    let dir = tempdir().unwrap();
    {
        let db_path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&db_path).unwrap();
        let bpm = BufferPoolManager::new(1000, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let db = Database::open(dir.path(), engine).unwrap();

        let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
        for i in 0..5u32 {
            let key = format!("rkey_{}", i);
            let val = format!("rval_{}", i);
            db.txn_put(txn, key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.commit_txn(txn).unwrap();
        // Drop without flush — simulates crash.
    }

    // Reopen — recovery replays the committed txn.
    let db = reopen_btree(dir.path());
    for i in 0..5u32 {
        let key = format!("rkey_{}", i);
        let val = format!("rval_{}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "rkey_{} should be present after recovery",
            i
        );
    }
}

#[test]
fn recovery_uncommitted_txn() {
    // Begin a txn, write keys, DON'T commit, "crash", reopen → data absent.
    let dir = tempdir().unwrap();
    {
        let db_path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&db_path).unwrap();
        let bpm = BufferPoolManager::new(1000, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let db = Database::open(dir.path(), engine).unwrap();

        let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
        for i in 0..5u32 {
            let key = format!("ukey_{}", i);
            let val = format!("uval_{}", i);
            db.txn_put(txn, key.as_bytes(), val.as_bytes()).unwrap();
        }
        // No commit — crash.
    }

    let db = reopen_btree(dir.path());
    for i in 0..5u32 {
        let key = format!("ukey_{}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            None,
            "ukey_{} should be absent after recovery (uncommitted)",
            i
        );
    }
}

#[test]
fn recovery_mixed_txns() {
    // T1 commits, T2 doesn't. After recovery: T1 data visible, T2 data not.
    let dir = tempdir().unwrap();
    {
        let db_path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&db_path).unwrap();
        let bpm = BufferPoolManager::new(1000, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let db = Database::open(dir.path(), engine).unwrap();

        // T1: commit.
        let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        db.txn_put(t1, b"committed_key", b"committed_val").unwrap();
        db.commit_txn(t1).unwrap();

        // T2: no commit.
        let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        db.txn_put(t2, b"uncommitted_key", b"uncommitted_val")
            .unwrap();
        // Crash without committing T2.
    }

    let db = reopen_btree(dir.path());
    assert_eq!(
        db.get(b"committed_key").unwrap(),
        Some(b"committed_val".to_vec())
    );
    assert_eq!(db.get(b"uncommitted_key").unwrap(), None);
}

#[test]
fn recovery_abort_before_crash() {
    // Put pre-existing data, begin txn, overwrite, abort, crash, reopen.
    // Pre-existing data should be intact.
    let dir = tempdir().unwrap();
    {
        let db_path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&db_path).unwrap();
        let bpm = BufferPoolManager::new(1000, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let db = Database::open(dir.path(), engine).unwrap();

        db.put(b"survive", b"original").unwrap();

        let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
        db.txn_put(txn, b"survive", b"overwritten").unwrap();
        db.txn_put(txn, b"new_key", b"new_val").unwrap();
        db.txn_abort(txn).unwrap();
        // Crash.
    }

    let db = reopen_btree(dir.path());
    assert_eq!(
        db.get(b"survive").unwrap(),
        Some(b"original".to_vec()),
        "Original value should survive abort + crash"
    );
    assert_eq!(
        db.get(b"new_key").unwrap(),
        None,
        "Aborted new key should not exist after recovery"
    );
}
