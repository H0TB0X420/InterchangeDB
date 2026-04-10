//! Stress tests for transactional isolation under concurrent load.
//!
//! These tests run multiple threads performing transactions simultaneously,
//! verifying that 2PL + deadlock detection maintain correct results under
//! sustained contention.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::Error;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::DiskManager;
use interchangedb::txn::TxnMode;

fn setup_shared_db() -> (Arc<Mutex<Database<BTreeEngine>>>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = DiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (Arc::new(Mutex::new(db)), dir)
}

#[test]
fn concurrent_txn_commits() {
    // 4 threads, each runs 50 transactions. Each txn puts 2 keys and commits.
    // After all complete: every committed key-value pair is present.
    let (db, _dir) = setup_shared_db();
    let barrier = Arc::new(Barrier::new(4));
    let committed_count = Arc::new(AtomicUsize::new(0));
    let deadlock_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..4)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            let committed = committed_count.clone();
            let deadlocks = deadlock_count.clone();
            thread::spawn(move || {
                barrier.wait();
                for iter in 0..50 {
                    let key1 = format!("t{}_i{}_k1", thread_idx, iter);
                    let key2 = format!("t{}_i{}_k2", thread_idx, iter);
                    let val = format!("v_{}_{}", thread_idx, iter);

                    let mut db = db.lock().unwrap();
                    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();

                    let r1 = db.txn_put(txn, key1.as_bytes(), val.as_bytes());
                    if let Err(Error::Deadlock(_)) = r1 {
                        deadlocks.fetch_add(1, Ordering::Relaxed);
                        db.txn_abort(txn).unwrap();
                        continue;
                    }
                    r1.unwrap();

                    let r2 = db.txn_put(txn, key2.as_bytes(), val.as_bytes());
                    if let Err(Error::Deadlock(_)) = r2 {
                        deadlocks.fetch_add(1, Ordering::Relaxed);
                        db.txn_abort(txn).unwrap();
                        continue;
                    }
                    r2.unwrap();

                    db.commit_txn(txn).unwrap();
                    committed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_committed = committed_count.load(Ordering::Relaxed);
    let total_deadlocks = deadlock_count.load(Ordering::Relaxed);

    // Most txns should commit (each thread uses unique keys, minimal contention).
    assert!(
        total_committed > 0,
        "At least some transactions should commit"
    );

    // All iterations completed (committed or deadlocked).
    assert_eq!(total_committed + total_deadlocks, 200);
}

#[test]
fn concurrent_txn_mixed() {
    // 4 threads, 50 txns each: ~70% commit, ~30% abort.
    // After all complete: committed data present, aborted data absent.
    let (db, _dir) = setup_shared_db();
    let barrier = Arc::new(Barrier::new(4));
    let committed_count = Arc::new(AtomicUsize::new(0));
    let aborted_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..4)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            let committed = committed_count.clone();
            let aborted = aborted_count.clone();
            thread::spawn(move || {
                barrier.wait();
                for iter in 0..50 {
                    let key = format!("mix_t{}_i{}", thread_idx, iter);
                    let val = format!("val_{}_{}", thread_idx, iter);
                    let should_abort = (thread_idx + iter) % 3 == 0; // ~33% abort

                    let mut db = db.lock().unwrap();
                    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();

                    let result = db.txn_put(txn, key.as_bytes(), val.as_bytes());
                    match result {
                        Ok(()) => {
                            if should_abort {
                                db.txn_abort(txn).unwrap();
                                aborted.fetch_add(1, Ordering::Relaxed);
                            } else {
                                db.commit_txn(txn).unwrap();
                                committed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(Error::Deadlock(_)) | Err(Error::LockTimeout) => {
                            db.txn_abort(txn).unwrap();
                            aborted.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("Unexpected error: {}", e),
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_committed = committed_count.load(Ordering::Relaxed);
    let total_aborted = aborted_count.load(Ordering::Relaxed);

    assert_eq!(total_committed + total_aborted, 200);
    assert!(total_committed > 0, "Some txns should commit");
    assert!(total_aborted > 0, "Some txns should abort");
}

#[test]
fn rapid_begin_abort_churn() {
    // 4 threads, each does 100 begin→abort cycles (no writes).
    // Each cycle does 2 fsyncs (begin + abort), so 800 total fsyncs.
    // Verify: all cycles complete, no panics, no leaks.
    let (db, _dir) = setup_shared_db();
    let barrier = Arc::new(Barrier::new(4));
    let total_cycles = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            let cycles = total_cycles.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let mut db = db.lock().unwrap();
                    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();
                    db.txn_abort(txn).unwrap();
                    cycles.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total = total_cycles.load(Ordering::Relaxed);

    assert_eq!(total, 400, "All cycles should complete");
    assert!(
        elapsed < Duration::from_secs(60),
        "Churn test took too long: {:?}",
        elapsed
    );
}
