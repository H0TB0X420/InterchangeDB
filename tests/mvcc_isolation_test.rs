//! MVCC Snapshot Isolation verification tests.
//!
//! Proves that the MVCC implementation provides correct snapshot isolation:
//! - No dirty reads (uncommitted writes invisible to other txns)
//! - No non-repeatable reads (snapshot is frozen at begin time)
//! - No phantom reads (scans return consistent results)
//! - Own writes visible within the same transaction
//! - Aborted writes invisible to everyone
//! - Read-only transactions never block writers (no S locks)
//! - Write skew is possible (expected under SI, not a bug)

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::Error;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::FileDiskManager;
use interchangedb::txn::TxnMode;

fn setup() -> (Database<BTreeEngine>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (db, dir)
}

// ---------------------------------------------------------------------------
// No dirty reads
// ---------------------------------------------------------------------------

#[test]
fn no_dirty_reads() {
    // T1 writes a key. T2 starts after T1 but before T1 commits.
    // T2 should NOT see T1's uncommitted write.
    let (db, _dir) = setup();

    // Pre-existing value.
    db.put(b"key", b"original").unwrap();

    // T1 writes but does not commit.
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"key", b"dirty").unwrap();

    // T2 starts — its snapshot was taken after T1 began but T1 hasn't committed.
    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let val = db.txn_get(t2, b"key").unwrap();

    // T2 sees the original value, NOT T1's dirty write.
    assert_eq!(
        val,
        Some(b"original".to_vec()),
        "T2 should not see T1's uncommitted write"
    );

    db.commit_txn(t2).unwrap();
    db.txn_abort(t1).unwrap();
}

#[test]
fn no_dirty_reads_new_key() {
    // T1 inserts a brand-new key. T2 should not see it until T1 commits.
    let (db, _dir) = setup();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"new_key", b"new_val").unwrap();

    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let val = db.txn_get(t2, b"new_key").unwrap();
    assert_eq!(val, None, "T2 should not see T1's uncommitted insert");

    db.commit_txn(t2).unwrap();
    db.txn_abort(t1).unwrap();
}

// ---------------------------------------------------------------------------
// No non-repeatable reads
// ---------------------------------------------------------------------------

#[test]
fn no_non_repeatable_reads() {
    // T1 reads a key. T2 writes + commits. T1 reads again — same value.
    let (db, _dir) = setup();

    db.put(b"key", b"v1").unwrap();

    // T1 takes a snapshot and reads.
    let t1 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let first_read = db.txn_get(t1, b"key").unwrap();
    assert_eq!(first_read, Some(b"v1".to_vec()));

    // T2 writes and commits (after T1's snapshot).
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t2, b"key", b"v2").unwrap();
    db.commit_txn(t2).unwrap();

    // T1 reads again — still sees v1 (snapshot frozen).
    let second_read = db.txn_get(t1, b"key").unwrap();
    assert_eq!(
        second_read,
        Some(b"v1".to_vec()),
        "Snapshot should be frozen — no repeatable read anomaly"
    );

    db.commit_txn(t1).unwrap();
}

// ---------------------------------------------------------------------------
// No phantom reads
// ---------------------------------------------------------------------------

#[test]
fn no_phantom_reads() {
    // T1 scans a range. T2 inserts into that range + commits. T1 scans again — same results.
    let (db, _dir) = setup();

    db.put(b"a", b"1").unwrap();
    db.put(b"c", b"3").unwrap();

    // T1 takes snapshot and scans.
    let t1 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    // Use txn_get to verify keys individually (scan through txn is not directly exposed yet).
    let a = db.txn_get(t1, b"a").unwrap();
    let b_before = db.txn_get(t1, b"b").unwrap();
    let c = db.txn_get(t1, b"c").unwrap();
    assert_eq!(a, Some(b"1".to_vec()));
    assert_eq!(b_before, None); // "b" doesn't exist yet.
    assert_eq!(c, Some(b"3".to_vec()));

    // T2 inserts "b" and commits.
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t2, b"b", b"2").unwrap();
    db.commit_txn(t2).unwrap();

    // T1 reads again — "b" still doesn't exist in T1's snapshot.
    let b_after = db.txn_get(t1, b"b").unwrap();
    assert_eq!(
        b_after, None,
        "Phantom should not appear in frozen snapshot"
    );

    db.commit_txn(t1).unwrap();
}

// ---------------------------------------------------------------------------
// Own writes visible
// ---------------------------------------------------------------------------

#[test]
fn own_writes_visible() {
    // A transaction can see its own uncommitted writes.
    let (db, _dir) = setup();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"key", b"my_value").unwrap();

    let val = db.txn_get(t1, b"key").unwrap();
    assert_eq!(
        val,
        Some(b"my_value".to_vec()),
        "Transaction should see its own write"
    );

    db.commit_txn(t1).unwrap();
}

#[test]
fn own_writes_overwrite_visible() {
    // Write twice to the same key within one txn — sees the latest write.
    let (db, _dir) = setup();

    db.put(b"key", b"original").unwrap();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"key", b"first").unwrap();
    db.txn_put(t1, b"key", b"second").unwrap();

    let val = db.txn_get(t1, b"key").unwrap();
    assert_eq!(val, Some(b"second".to_vec()), "Should see latest own write");

    db.commit_txn(t1).unwrap();
}

// ---------------------------------------------------------------------------
// Aborted writes invisible
// ---------------------------------------------------------------------------

#[test]
fn aborted_writes_invisible() {
    // T1 writes and aborts. T2 cannot see the write.
    let (db, _dir) = setup();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"secret", b"aborted_data").unwrap();
    db.txn_abort(t1).unwrap();

    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let val = db.txn_get(t2, b"secret").unwrap();
    assert_eq!(val, None, "Aborted writes must be invisible");
    db.commit_txn(t2).unwrap();
}

#[test]
fn aborted_overwrite_preserves_original() {
    // Pre-existing value. T1 overwrites and aborts. Original still visible.
    let (db, _dir) = setup();

    db.put(b"key", b"original").unwrap();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"key", b"overwritten").unwrap();
    db.txn_abort(t1).unwrap();

    let val = db.get(b"key").unwrap();
    assert_eq!(
        val,
        Some(b"original".to_vec()),
        "Original should survive abort"
    );
}

// ---------------------------------------------------------------------------
// Tombstone correctness
// ---------------------------------------------------------------------------

#[test]
fn tombstone_makes_key_invisible() {
    // T1 writes key, commits. T2 deletes key, commits. T3 reads — gets None.
    let (db, _dir) = setup();

    db.put(b"key", b"value").unwrap();

    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_delete(t2, b"key").unwrap();
    db.commit_txn(t2).unwrap();

    let val = db.get(b"key").unwrap();
    assert_eq!(val, None, "Tombstoned key should not be visible");
}

#[test]
fn tombstone_not_visible_to_earlier_snapshot() {
    // T1 takes snapshot. T2 deletes key + commits. T1 still sees the key.
    let (db, _dir) = setup();

    db.put(b"key", b"value").unwrap();

    let t1 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let val_before = db.txn_get(t1, b"key").unwrap();
    assert_eq!(val_before, Some(b"value".to_vec()));

    // T2 deletes after T1's snapshot.
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_delete(t2, b"key").unwrap();
    db.commit_txn(t2).unwrap();

    // T1 still sees the key (snapshot frozen before delete).
    let val_after = db.txn_get(t1, b"key").unwrap();
    assert_eq!(
        val_after,
        Some(b"value".to_vec()),
        "Earlier snapshot should not see later delete"
    );

    db.commit_txn(t1).unwrap();
}

// ---------------------------------------------------------------------------
// Readers never block writers
// ---------------------------------------------------------------------------

#[test]
fn readers_dont_block_writers() {
    // A read-only transaction does NOT acquire locks. A writer can proceed.
    let (db, _dir) = setup();

    db.put(b"key", b"v1").unwrap();

    // Reader takes snapshot.
    let reader = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let _val = db.txn_get(reader, b"key").unwrap();

    // Writer can write the same key — no blocking (no S lock from reader).
    let writer = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(writer, b"key", b"v2").unwrap();
    db.commit_txn(writer).unwrap();

    // Reader still sees v1.
    let val = db.txn_get(reader, b"key").unwrap();
    assert_eq!(val, Some(b"v1".to_vec()));

    db.commit_txn(reader).unwrap();

    // After reader commits, new snapshot sees v2.
    assert_eq!(db.get(b"key").unwrap(), Some(b"v2".to_vec()));
}

// ---------------------------------------------------------------------------
// Write-write conflict (X locks still active)
// ---------------------------------------------------------------------------

#[test]
fn write_write_conflict_detected() {
    // Two write transactions on the same key — second gets deadlock or timeout.
    let (db, _dir) = setup();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"contested", b"t1_value").unwrap();

    // T2 tries to write same key — should get deadlock (T1 holds X lock).
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let result = db.txn_put(t2, b"contested", b"t2_value");

    assert!(
        matches!(result, Err(Error::Deadlock(_)) | Err(Error::LockTimeout)),
        "Expected write-write conflict, got: {:?}",
        result
    );

    db.txn_abort(t2).unwrap();
    db.commit_txn(t1).unwrap();

    assert_eq!(db.get(b"contested").unwrap(), Some(b"t1_value".to_vec()));
}

// ---------------------------------------------------------------------------
// Write skew (expected under SI — documents the limitation)
// ---------------------------------------------------------------------------

#[test]
fn write_skew_possible_under_snapshot_isolation() {
    // Classic write skew: T1 reads A+B, T2 reads A+B.
    // T1 writes A based on B's value. T2 writes B based on A's value.
    // Both commit. Under serializability, one should abort. Under SI, both succeed.
    //
    // This test documents that write skew IS possible — it's expected behavior,
    // not a bug. Full serializability would require SSI (Phase 8+).
    let (db, _dir) = setup();

    db.put(b"A", b"1").unwrap();
    db.put(b"B", b"1").unwrap();

    // T1 reads both, decides to set A = B + 1.
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let b_val = db.txn_get(t1, b"B").unwrap().unwrap();
    assert_eq!(b_val, b"1");

    // T2 reads both, decides to set B = A + 1.
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let a_val = db.txn_get(t2, b"A").unwrap().unwrap();
    assert_eq!(a_val, b"1");

    // T1 writes A = "2" (B was "1", so A = 1+1).
    db.txn_put(t1, b"A", b"2").unwrap();
    db.commit_txn(t1).unwrap();

    // T2 writes B = "2" (A was "1", so B = 1+1). No conflict — different keys.
    db.txn_put(t2, b"B", b"2").unwrap();
    db.commit_txn(t2).unwrap();

    // Final state: A=2, B=2. Under serializability, one should have seen
    // the other's write and computed 3. Under SI, both used stale reads.
    assert_eq!(db.get(b"A").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"B").unwrap(), Some(b"2".to_vec()));
    // This IS write skew. Documenting it as expected behavior.
}

// ---------------------------------------------------------------------------
// Version ordering correctness
// ---------------------------------------------------------------------------

#[test]
fn multiple_versions_newest_wins() {
    // Write a key 5 times via separate transactions. Get returns the latest.
    let (db, _dir) = setup();

    for i in 0..5 {
        let val = format!("v{}", i);
        db.put(b"key", val.as_bytes()).unwrap();
    }

    assert_eq!(db.get(b"key").unwrap(), Some(b"v4".to_vec()));
}

#[test]
fn older_snapshot_sees_older_version() {
    // T1 takes snapshot after v1. Write v2. T1 still sees v1.
    let (db, _dir) = setup();

    db.put(b"key", b"v1").unwrap();

    let t1 = db.begin_txn(TxnMode::ReadOnly).unwrap();

    db.put(b"key", b"v2").unwrap();
    db.put(b"key", b"v3").unwrap();

    // T1's snapshot predates v2 and v3.
    let val = db.txn_get(t1, b"key").unwrap();
    assert_eq!(val, Some(b"v1".to_vec()), "Older snapshot should see v1");

    db.commit_txn(t1).unwrap();

    // Current view sees v3.
    assert_eq!(db.get(b"key").unwrap(), Some(b"v3".to_vec()));
}
