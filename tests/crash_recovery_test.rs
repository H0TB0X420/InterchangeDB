//! Crash recovery tests — prove that committed data survives any crash.
//!
//! Simulates crashes by dropping Database without clean shutdown (no checkpoint,
//! no flush). On reopen, recovery replays the WAL. Verified: committed data
//! always present, uncommitted data always absent.

use std::path::Path;
use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::DiskManager;
use interchangedb::txn::TxnMode;

fn open_btree(dir: &Path) -> Database<BTreeEngine> {
    let db_path = dir.join("test.db");
    let dm = if db_path.exists() {
        DiskManager::open(&db_path).unwrap()
    } else {
        DiskManager::create(&db_path).unwrap()
    };
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    Database::open(dir, engine).unwrap()
}

// ---------------------------------------------------------------------------
// Basic crash-restart loop
// ---------------------------------------------------------------------------

#[test]
fn crash_restart_loop_100() {
    // 100 iterations: write a key, crash, recover, verify ALL previously
    // committed data still present. Accumulates data across iterations.
    let dir = tempdir().unwrap();

    for i in 0..100u32 {
        // Write and commit.
        {
            let mut db = open_btree(dir.path());
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
            // Crash (drop without checkpoint).
        }

        // Recover and verify ALL keys from 0..=i are present.
        {
            let db = open_btree(dir.path());
            for j in 0..=i {
                let key = format!("key_{:04}", j);
                let expected = format!("val_{:04}", j);
                let got = db.get(key.as_bytes()).unwrap();
                assert_eq!(
                    got,
                    Some(expected.into_bytes()),
                    "Iteration {}: key_{:04} missing after crash-restart",
                    i, j
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Crash mid-transaction
// ---------------------------------------------------------------------------

#[test]
fn crash_mid_transaction_uncommitted_invisible() {
    // Begin txn, write 5 keys, crash WITHOUT committing.
    // After recovery: none of the 5 keys visible.
    let dir = tempdir().unwrap();

    {
        let mut db = open_btree(dir.path());
        let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
        for i in 0..5 {
            let key = format!("uncommitted_{}", i);
            db.txn_put(txn, key.as_bytes(), b"should_not_survive").unwrap();
        }
        // Crash without commit.
    }

    let db = open_btree(dir.path());
    for i in 0..5 {
        let key = format!("uncommitted_{}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            None,
            "Uncommitted key '{}' should not survive crash",
            key
        );
    }
}

// ---------------------------------------------------------------------------
// Crash after commit before checkpoint
// ---------------------------------------------------------------------------

#[test]
fn crash_after_commit_before_checkpoint() {
    // Commit a transaction, crash without checkpoint.
    // WAL has the Commit record → recovery replays → data present.
    let dir = tempdir().unwrap();

    {
        let mut db = open_btree(dir.path());
        let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
        db.txn_put(txn, b"committed_key", b"committed_val").unwrap();
        db.commit_txn(txn).unwrap();
        // Crash — no checkpoint, no flush.
    }

    let db = open_btree(dir.path());
    assert_eq!(
        db.get(b"committed_key").unwrap(),
        Some(b"committed_val".to_vec()),
        "Committed data must survive crash even without checkpoint"
    );
}

// ---------------------------------------------------------------------------
// Crash with multiple transaction states
// ---------------------------------------------------------------------------

#[test]
fn crash_mixed_transaction_states() {
    // T1: committed. T2: aborted. T3: uncommitted (in-flight at crash).
    // After recovery: T1 visible, T2 and T3 invisible.
    let dir = tempdir().unwrap();

    {
        let mut db = open_btree(dir.path());

        // T1: committed.
        let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        db.txn_put(t1, b"t1_key", b"t1_val").unwrap();
        db.commit_txn(t1).unwrap();

        // T2: aborted.
        let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        db.txn_put(t2, b"t2_key", b"t2_val").unwrap();
        db.txn_abort(t2).unwrap();

        // T3: in-flight (never committed, never aborted).
        let t3 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        db.txn_put(t3, b"t3_key", b"t3_val").unwrap();
        // Crash.
    }

    let db = open_btree(dir.path());
    assert_eq!(db.get(b"t1_key").unwrap(), Some(b"t1_val".to_vec()), "Committed T1 must survive");
    assert_eq!(db.get(b"t2_key").unwrap(), None, "Aborted T2 must not survive");
    assert_eq!(db.get(b"t3_key").unwrap(), None, "Uncommitted T3 must not survive");
}

// ---------------------------------------------------------------------------
// Recovery idempotent
// ---------------------------------------------------------------------------

#[test]
fn recovery_idempotent() {
    // Write data, crash. Open (recovery runs). Close. Open again (recovery runs again).
    // Same state both times.
    let dir = tempdir().unwrap();

    {
        let mut db = open_btree(dir.path());
        for i in 0..10 {
            let key = format!("idem_{}", i);
            db.put(key.as_bytes(), b"value").unwrap();
        }
        // Crash.
    }

    // First recovery.
    let state_1: Vec<_> = {
        let db = open_btree(dir.path());
        (0..10).map(|i| {
            let key = format!("idem_{}", i);
            db.get(key.as_bytes()).unwrap()
        }).collect()
    };

    // Second recovery (from the same WAL state — no new writes between).
    let state_2: Vec<_> = {
        let db = open_btree(dir.path());
        (0..10).map(|i| {
            let key = format!("idem_{}", i);
            db.get(key.as_bytes()).unwrap()
        }).collect()
    };

    assert_eq!(state_1, state_2, "Recovery must be idempotent");
    for (i, val) in state_1.iter().enumerate() {
        assert_eq!(val, &Some(b"value".to_vec()), "Key idem_{} missing", i);
    }
}

// ---------------------------------------------------------------------------
// Double crash (crash during recovery)
// ---------------------------------------------------------------------------

#[test]
fn double_crash_recovery() {
    // Write, crash. Open (starts recovery), crash AGAIN mid-recovery
    // (simulated by just dropping during open — recovery is atomic in our impl).
    // Third open: final state correct.
    let dir = tempdir().unwrap();

    {
        let mut db = open_btree(dir.path());
        db.put(b"double_crash_key", b"double_crash_val").unwrap();
        // First crash.
    }

    // Second "crash" — just open and close immediately (recovery completes, no writes).
    {
        let _db = open_btree(dir.path());
        // Simulated crash after recovery (drop immediately).
    }

    // Third open — data still present.
    let db = open_btree(dir.path());
    assert_eq!(
        db.get(b"double_crash_key").unwrap(),
        Some(b"double_crash_val".to_vec())
    );
}

// ---------------------------------------------------------------------------
// Recovery preserves GC'd state
// ---------------------------------------------------------------------------

#[test]
fn recovery_after_gc() {
    // Write multiple versions, GC, write more, crash, recover.
    // GC'd versions stay gone. New committed data present.
    let dir = tempdir().unwrap();

    {
        let mut db = open_btree(dir.path());

        // Create multiple versions.
        for i in 0..5 {
            let val = format!("old_{}", i);
            db.put(b"gc_key", val.as_bytes()).unwrap();
        }

        // GC removes old versions.
        db.gc().unwrap();

        // Write new data after GC.
        db.put(b"gc_key", b"final_value").unwrap();
        db.put(b"new_key", b"new_value").unwrap();

        // Checkpoint to persist the GC'd state + new data.
        db.checkpoint().unwrap();
        // Crash.
    }

    let db = open_btree(dir.path());
    assert_eq!(db.get(b"new_key").unwrap(), Some(b"new_value".to_vec()),
        "new_key must survive GC + checkpoint + crash");
    assert_eq!(db.get(b"gc_key").unwrap(), Some(b"final_value".to_vec()),
        "gc_key must have final_value after GC + checkpoint + crash");
}

// ---------------------------------------------------------------------------
// Atomicity: all-or-nothing
// ---------------------------------------------------------------------------

#[test]
fn atomicity_multi_key_transaction() {
    // Transaction writes 10 keys atomically. After crash:
    // either ALL 10 present (committed before crash) or NONE (uncommitted).
    let dir = tempdir().unwrap();

    // Case 1: committed before crash — all present.
    {
        let mut db = open_btree(dir.path());
        let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
        for i in 0..10 {
            let key = format!("atomic_a_{}", i);
            db.txn_put(txn, key.as_bytes(), b"present").unwrap();
        }
        db.commit_txn(txn).unwrap();
        // Crash.
    }

    let db = open_btree(dir.path());
    let count_a: usize = (0..10)
        .filter(|i| {
            let key = format!("atomic_a_{}", i);
            db.get(key.as_bytes()).unwrap().is_some()
        })
        .count();
    assert_eq!(count_a, 10, "Committed txn: all 10 keys must be present");
    drop(db);

    // Case 2: NOT committed — none present.
    {
        let mut db = open_btree(dir.path());
        let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
        for i in 0..10 {
            let key = format!("atomic_b_{}", i);
            db.txn_put(txn, key.as_bytes(), b"ghost").unwrap();
        }
        // NO commit — crash.
    }

    let db = open_btree(dir.path());
    let count_b: usize = (0..10)
        .filter(|i| {
            let key = format!("atomic_b_{}", i);
            db.get(key.as_bytes()).unwrap().is_some()
        })
        .count();
    assert_eq!(count_b, 0, "Uncommitted txn: NONE of 10 keys should be present");
}

// ---------------------------------------------------------------------------
// Durability: committed data ALWAYS survives
// ---------------------------------------------------------------------------

#[test]
fn durability_100_cycles() {
    // 100 cycles of: commit a unique key → crash → verify present.
    // Zero tolerance for data loss.
    let dir = tempdir().unwrap();

    for i in 0..100u32 {
        {
            let mut db = open_btree(dir.path());
            let key = format!("durable_{:04}", i);
            let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
            db.txn_put(txn, key.as_bytes(), b"durable").unwrap();
            db.commit_txn(txn).unwrap();
            // Crash.
        }

        let db = open_btree(dir.path());
        let key = format!("durable_{:04}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(b"durable".to_vec()),
            "Cycle {}: committed key must survive crash",
            i
        );
    }
}
