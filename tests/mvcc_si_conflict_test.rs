//! Snapshot-isolation correctness regressions.
//!
//! Issue 1: first-committer-wins on `txn_put` / `txn_delete`.
//! Issue 2: aborted MVCC versions are invisible to all snapshots.
//!
//! Each test pins down one concrete failure mode that the fixes resolve.
//! If `find_conflicting_committed_version` is removed or the aborted_txns
//! set is dropped from `known_not_committed`, these tests fail.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::{Error, Result};
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::DiskManager;
use interchangedb::txn::TxnMode;

fn setup_db() -> (Arc<Database<BTreeEngine>>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = DiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (Arc::new(db), dir)
}

// ---------------------------------------------------------------------------
// Issue 1 — first-committer-wins on txn_put / txn_delete
// ---------------------------------------------------------------------------

#[test]
fn write_write_conflict_aborts_second_writer() {
    // Minimal regression: T2 (older snapshot) writes K after T1 commits K.
    // Before fix: X-lock released at commit, T2 silently overwrites T1.
    // After fix: T2 sees T1's commit_ts > T2.begin_ts and aborts.
    let (db, _dir) = setup_db();

    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"k", b"v1").unwrap();
    db.commit_txn(t1).unwrap();

    let result = db.txn_put(t2, b"k", b"v2");
    assert!(
        matches!(result, Err(Error::WriteConflict { .. })),
        "Expected WriteConflict, got: {:?}",
        result
    );
    db.txn_abort(t2).unwrap();
}

#[test]
fn read_then_write_si_scenario() {
    // Exact scenario from the review.
    // T2's snapshot precedes T1: T2 cannot see T1's write on read, but its
    // own write must still abort because T1's commit landed in T2's interval.
    let (db, _dir) = setup_db();

    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"k", b"v_from_t1").unwrap();
    db.commit_txn(t1).unwrap();

    let read_result = db.txn_get(t2, b"k").unwrap();
    assert_eq!(
        read_result, None,
        "T2's snapshot precedes T1; T1's write must be invisible to reads"
    );

    let write_result = db.txn_put(t2, b"k", b"v_from_t2");
    assert!(
        matches!(write_result, Err(Error::WriteConflict { .. })),
        "T2's write must conflict (first-committer-wins): {:?}",
        write_result
    );
    db.txn_abort(t2).unwrap();
}

#[test]
fn delete_write_conflict_symmetric() {
    // Deletes are writes under SI: same first-committer-wins rule applies.
    let (db, _dir) = setup_db();
    db.put(b"k", b"initial").unwrap();

    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"k", b"updated").unwrap();
    db.commit_txn(t1).unwrap();

    let result = db.txn_delete(t2, b"k");
    assert!(
        matches!(result, Err(Error::WriteConflict { .. })),
        "T2's delete must conflict with T1's commit: {:?}",
        result
    );
    db.txn_abort(t2).unwrap();
}

#[test]
fn lock_released_after_abort_following_conflict() {
    // The X-lock acquired before the conflict check is held until txn_abort.
    // Verify abort actually releases it — otherwise the lock leaks and the
    // next writer would block forever.
    let (db, _dir) = setup_db();

    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"k", b"v1").unwrap();
    db.commit_txn(t1).unwrap();

    let result = db.txn_put(t2, b"k", b"v2");
    assert!(matches!(result, Err(Error::WriteConflict { .. })));
    db.txn_abort(t2).unwrap();

    db.put(b"k", b"v3").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn concurrent_counter_increments_no_lost_updates() {
    // 8 threads × 100 increments × 10 shared counters, retry on conflict.
    // Invariant: sum of all counters == 800. Without first-committer-wins,
    // lost updates would silently drop increments and the sum falls below.
    let (db, _dir) = setup_db();

    for i in 0..10u8 {
        db.put(&[i], &0u64.to_le_bytes()).unwrap();
    }

    let barrier = Arc::new(Barrier::new(8));
    let aborts = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..8u8)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            let aborts = aborts.clone();
            thread::spawn(move || {
                barrier.wait();
                for iter in 0..100u16 {
                    let counter_idx =
                        ((thread_idx as u16 * 7 + iter * 3) % 10) as u8;
                    loop {
                        match increment_counter(&db, &[counter_idx]) {
                            Ok(()) => break,
                            Err(Error::WriteConflict { .. })
                            | Err(Error::Deadlock(_))
                            | Err(Error::LockTimeout) => {
                                aborts.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => panic!("unexpected error: {}", e),
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total: u64 = (0..10u8)
        .map(|i| {
            let bytes = db.get(&[i]).unwrap().unwrap();
            u64::from_le_bytes(bytes.try_into().unwrap())
        })
        .sum();

    assert_eq!(
        total,
        800,
        "Sum of 8×100 increments must be 800 (got {}, {} retried aborts)",
        total,
        aborts.load(Ordering::Relaxed)
    );
}

fn increment_counter(db: &Database<BTreeEngine>, key: &[u8]) -> Result<()> {
    let txn = db.begin_txn(TxnMode::ReadWrite)?;
    let current = match db.txn_get(txn, key)? {
        Some(bytes) => u64::from_le_bytes(bytes.try_into().unwrap()),
        None => 0,
    };
    let next = current + 1;
    match db.txn_put(txn, key, &next.to_le_bytes()) {
        Ok(()) => db.commit_txn(txn),
        Err(e) => {
            let _ = db.txn_abort(txn);
            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Issue 2 — aborted MVCC versions invisible
// ---------------------------------------------------------------------------

#[test]
fn aborted_version_invisible_basic() {
    // T1 writes K then aborts. A subsequent read must return None.
    // Without the aborted_txns set, this is correct in this scenario only
    // because version_ts > checkpoint_ts; aborted_version_invisible_after_checkpoint
    // covers the harder case.
    let (db, _dir) = setup_db();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"k", b"never_committed").unwrap();
    db.txn_abort(t1).unwrap();

    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn aborted_version_invisible_after_checkpoint() {
    // Ghost-commit scenario.
    // T1 writes K, checkpoint advances checkpoint_ts past T1.begin_ts,
    // then T1 aborts. Without tracking aborted txns, visibility falls into
    // the "assumed committed pre-checkpoint" branch and ghost-commits K.
    let (db, _dir) = setup_db();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"k_ghost", b"ghost_value").unwrap();

    db.checkpoint().unwrap();

    db.txn_abort(t1).unwrap();

    assert_eq!(
        db.get(b"k_ghost").unwrap(),
        None,
        "Aborted pre-checkpoint version must not ghost-commit"
    );
}

#[test]
fn aborted_version_skipped_returns_older_committed() {
    // K has a committed version AND a newer aborted version. The aborted
    // version sorts first (higher version_ts) but must be skipped, returning
    // the committed older value. Catches "skip vs. return None" off-by-ones
    // in the visibility loop.
    let (db, _dir) = setup_db();
    db.put(b"k", b"committed").unwrap();

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"k", b"never_committed").unwrap();
    db.txn_abort(t1).unwrap();

    assert_eq!(
        db.get(b"k").unwrap(),
        Some(b"committed".to_vec()),
        "Read must skip aborted newer version and return committed older one"
    );
}
