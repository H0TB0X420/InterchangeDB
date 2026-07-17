//! Deterministic concurrency tests.
//!
//! Since shuttle/loom don't integrate with parking_lot, we use manual
//! barrier-based tests that force specific interleavings to verify
//! correctness at each critical point.
//!
//! Each test exercises one specific ordering that could go wrong.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::Error;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::MemoryDiskManager;
use interchangedb::txn::lock_manager::{LockManager, LockMode};
use interchangedb::txn::{TxnId, TxnMode};
use interchangedb::wal::SyncMode;

// ---------------------------------------------------------------------------
// Lock Manager: forced orderings
// ---------------------------------------------------------------------------

#[test]
fn lock_ordering_s_then_x_blocked() {
    // Force ordering: T1 acquires S, then T2 tries X — must block.
    // After T1 releases, T2 gets X.
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let t1 = TxnId::new(1);
    let t2 = TxnId::new(2);

    // Step 1: T1 acquires S.
    lm.acquire(t1, b"key", LockMode::Shared).unwrap();

    // Step 2: T2 tries X — spawned thread blocks.
    let lm2 = lm.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b2 = barrier.clone();
    let handle = thread::spawn(move || {
        b2.wait(); // Signal we're about to try.
        lm2.acquire(t2, b"key", LockMode::Exclusive)
    });

    barrier.wait(); // Wait for T2 to be ready.
    thread::sleep(Duration::from_millis(50)); // Let T2 actually block.

    // T2 should NOT hold the lock yet.
    assert!(lm.held_by(t2).is_empty(), "T2 should be blocked");

    // Step 3: T1 releases S → T2 gets X.
    lm.release(t1, b"key");
    let result = handle.join().unwrap();
    assert!(result.is_ok(), "T2 should get X after T1 releases S");
    assert_eq!(lm.held_by(t2), vec![b"key".to_vec()]);
}

#[test]
fn lock_ordering_x_then_s_blocked() {
    // Force ordering: T1 acquires X, then T2 tries S — must block.
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let t1 = TxnId::new(1);
    let t2 = TxnId::new(2);

    lm.acquire(t1, b"key", LockMode::Exclusive).unwrap();

    let lm2 = lm.clone();
    let handle = thread::spawn(move || lm2.acquire(t2, b"key", LockMode::Shared));

    thread::sleep(Duration::from_millis(50));
    assert!(lm.held_by(t2).is_empty(), "T2 should be blocked by X");

    lm.release(t1, b"key");
    handle.join().unwrap().unwrap();
    assert_eq!(lm.held_by(t2), vec![b"key".to_vec()]);
}

#[test]
fn lock_ordering_deadlock_detected_immediately() {
    // Force ordering: T1 holds A, T2 holds B.
    // T1 requests B (blocks). T2 requests A (deadlock detected immediately).
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let t1 = TxnId::new(1);
    let t2 = TxnId::new(2);

    lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
    lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();

    // T1 requests B — blocks (T2 holds B).
    let lm2 = lm.clone();
    let handle = thread::spawn(move || lm2.acquire(t1, b"B", LockMode::Exclusive));
    thread::sleep(Duration::from_millis(50));

    // T2 requests A — should detect cycle immediately (not timeout after 5s).
    let start = std::time::Instant::now();
    let result = lm.acquire(t2, b"A", LockMode::Exclusive);
    let elapsed = start.elapsed();

    assert!(
        matches!(result, Err(Error::Deadlock(_))),
        "Should get Deadlock, got: {:?}",
        result
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "Deadlock should be detected immediately, not after timeout ({:?})",
        elapsed
    );

    // Cleanup: release T2's B so T1 can proceed.
    lm.release(t2, b"B");
    handle.join().unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// MVCC: forced visibility orderings
// ---------------------------------------------------------------------------

#[test]
fn mvcc_write_then_read_sees_nothing() {
    // T1 writes. T2 (started after T1 but before T1 commits) reads.
    // T2 must NOT see T1's write.
    let dir = tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap();

    db.put(b"key", b"original").unwrap();

    // T1 writes (uncommitted).
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"key", b"t1_write").unwrap();

    // T2 reads — forced to start AFTER T1's write but BEFORE T1's commit.
    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let val = db.txn_get(t2, b"key").unwrap();
    assert_eq!(
        val,
        Some(b"original".to_vec()),
        "T2 must see original, not T1's uncommitted write"
    );

    // T1 commits.
    db.commit_txn(t1).unwrap();

    // T2 STILL sees original (snapshot frozen).
    let val_after = db.txn_get(t2, b"key").unwrap();
    assert_eq!(
        val_after,
        Some(b"original".to_vec()),
        "T2's snapshot is frozen — must not see T1's commit"
    );

    db.commit_txn(t2).unwrap();

    // New reader sees T1's committed value.
    assert_eq!(db.get(b"key").unwrap(), Some(b"t1_write".to_vec()));
}

#[test]
fn mvcc_concurrent_writes_one_wins() {
    // T1 and T2 both try to write the same key.
    // One succeeds (gets X lock first), the other gets blocked/deadlock/timeout.
    let dir = tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();

    // T1 writes first (gets X lock).
    db.txn_put(t1, b"contested", b"t1_wins").unwrap();

    // T2 tries same key — blocked by T1's X lock.
    let result = db.txn_put(t2, b"contested", b"t2_tries");
    assert!(
        matches!(result, Err(Error::Deadlock(_)) | Err(Error::LockTimeout)),
        "T2 should fail with conflict, got: {:?}",
        result
    );

    // T1 commits — its value persists.
    db.txn_abort(t2).unwrap();
    db.commit_txn(t1).unwrap();
    assert_eq!(db.get(b"contested").unwrap(), Some(b"t1_wins".to_vec()));
}

#[test]
fn mvcc_abort_then_read_sees_original() {
    // T1 writes + aborts. T2 reads — must see original (not T1's aborted write).
    let dir = tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap();

    db.put(b"key", b"original").unwrap();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"key", b"aborted_value").unwrap();
    db.txn_abort(t1).unwrap();

    // T2 reads — must see original.
    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let val = db.txn_get(t2, b"key").unwrap();
    assert_eq!(
        val,
        Some(b"original".to_vec()),
        "Aborted write must be invisible"
    );
    db.commit_txn(t2).unwrap();
}

#[test]
fn mvcc_own_write_visible_before_commit() {
    // T1 writes a key, then reads it — must see own write even before commit.
    let dir = tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"new_key", b"my_value").unwrap();

    let val = db.txn_get(t1, b"new_key").unwrap();
    assert_eq!(
        val,
        Some(b"my_value".to_vec()),
        "Transaction must see its own uncommitted write"
    );

    db.commit_txn(t1).unwrap();
}

// ---------------------------------------------------------------------------
// Write-write conflict: all orderings
// ---------------------------------------------------------------------------

#[test]
fn write_write_both_orderings() {
    // Test both possible orderings: T1 first vs T2 first.
    let dir = tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap();

    // Ordering 1: T1 writes first, T2 blocked.
    {
        let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();

        db.txn_put(t1, b"order1", b"t1").unwrap();
        let r2 = db.txn_put(t2, b"order1", b"t2");
        assert!(r2.is_err(), "Second writer must fail");

        db.txn_abort(t2).unwrap();
        db.commit_txn(t1).unwrap();
        assert_eq!(db.get(b"order1").unwrap(), Some(b"t1".to_vec()));
    }

    // Ordering 2: T2 writes first, T1 blocked.
    {
        let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();

        db.txn_put(t2, b"order2", b"t2").unwrap();
        let r1 = db.txn_put(t1, b"order2", b"t1");
        assert!(r1.is_err(), "Second writer must fail");

        db.txn_abort(t1).unwrap();
        db.commit_txn(t2).unwrap();
        assert_eq!(db.get(b"order2").unwrap(), Some(b"t2".to_vec()));
    }
}
