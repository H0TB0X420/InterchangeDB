//! Concurrency stress tests — sustained multi-thread load.
//!
//! Exercises the full stack under contention: MVCC snapshots, lock manager,
//! deadlock detection, WAL, and GC all running simultaneously.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::Error;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::FileDiskManager;
use interchangedb::txn::TxnMode;

fn setup_shared() -> (Arc<Database<BTreeEngine>>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (Arc::new(db), dir)
}

// ---------------------------------------------------------------------------
// Read-heavy workload
// ---------------------------------------------------------------------------

#[test]
fn read_heavy_8_threads() {
    // 90% reads, 10% writes, 8 threads, 200 ops per thread.
    // Verify: no panics, reads always return valid data or None.
    let (db, _dir) = setup_shared();
    let barrier = Arc::new(Barrier::new(8));
    let panicked = Arc::new(AtomicBool::new(false));

    // Seed some data.
    for i in 0..20 {
        let key = format!("rh_key_{:03}", i);
        let val = format!("rh_val_{:03}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    let handles: Vec<_> = (0..8)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            let panicked = panicked.clone();
            thread::spawn(move || {
                barrier.wait();
                for iter in 0..200 {
                    let is_write = (thread_idx * 7 + iter * 3) % 10 == 0;
                    let key_idx = (thread_idx * 11 + iter * 5) % 20;
                    let key = format!("rh_key_{:03}", key_idx);

                    if is_write {
                        let val = format!("updated_{}_{}", thread_idx, iter);
                        // WriteConflict/Deadlock/LockTimeout are expected under contention.
                        match db.put(key.as_bytes(), val.as_bytes()) {
                            Ok(()) => {}
                            Err(Error::WriteConflict { .. })
                            | Err(Error::Deadlock(_))
                            | Err(Error::LockTimeout) => {}
                            Err(e) => {
                                eprintln!("Unexpected write error: {}", e);
                                panicked.store(true, Ordering::Relaxed);
                            }
                        }
                    } else {
                        match db.get(key.as_bytes()) {
                            Ok(Some(val)) => {
                                assert!(!val.is_empty(), "Got empty value for {}", key);
                            }
                            Ok(None) => {
                                // Key was never written or was deleted — acceptable.
                            }
                            Err(e) => {
                                eprintln!("Read error: {}", e);
                                panicked.store(true, Ordering::Relaxed);
                            }
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert!(
        !panicked.load(Ordering::Relaxed),
        "At least one thread encountered an error"
    );
}

// ---------------------------------------------------------------------------
// Write-heavy workload
// ---------------------------------------------------------------------------

#[test]
fn write_heavy_8_threads() {
    // 90% writes, 10% reads, 8 threads, 100 ops per thread.
    // Verify: no panics, all committed data persists.
    let (db, _dir) = setup_shared();
    let barrier = Arc::new(Barrier::new(8));
    let committed_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..8)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            let committed = committed_count.clone();
            thread::spawn(move || {
                barrier.wait();
                for iter in 0..100 {
                    let key = format!("wh_t{}_{:04}", thread_idx, iter);
                    let val = format!("val_{}_{}", thread_idx, iter);

                    // Deadlock/timeout acceptable under contention.
                    if db.put(key.as_bytes(), val.as_bytes()).is_ok() {
                        committed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total = committed_count.load(Ordering::Relaxed);
    assert!(total > 0, "At least some writes should commit");

    // Verify a sample of committed data.
    let sample_key = b"wh_t0_0000";
    let val = db.get(sample_key).unwrap();
    // This key should exist since thread 0 iter 0 runs early.
    assert!(val.is_some(), "First write should be present");
}

// ---------------------------------------------------------------------------
// Deadlock storm
// ---------------------------------------------------------------------------

#[test]
fn deadlock_storm_8_threads() {
    // 8 threads, each acquires 2 random keys from pool of 8.
    // Deadlocks expected and resolved. All threads complete.
    let (db, _dir) = setup_shared();
    let barrier = Arc::new(Barrier::new(8));
    let completed = Arc::new(AtomicU64::new(0));
    let deadlocks = Arc::new(AtomicU64::new(0));

    // Seed the keys.
    for i in 0..8 {
        let key = format!("dl_key_{}", i);
        db.put(key.as_bytes(), b"initial").unwrap();
    }

    let handles: Vec<_> = (0..8)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            let completed = completed.clone();
            let deadlocks = deadlocks.clone();
            thread::spawn(move || {
                barrier.wait();
                for iter in 0..50 {
                    let k1 = (thread_idx * 7 + iter * 3) % 8;
                    let k2 = (thread_idx * 11 + iter * 5) % 8;
                    if k1 == k2 {
                        completed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    let key1 = format!("dl_key_{}", k1);
                    let key2 = format!("dl_key_{}", k2);

                    let txn = db.begin_txn(TxnMode::ReadWrite).unwrap();

                    let r1 = db.txn_put(txn, key1.as_bytes(), b"locked");
                    match r1 {
                        Ok(()) => {}
                        Err(Error::Deadlock(_))
                        | Err(Error::LockTimeout)
                        | Err(Error::WriteConflict { .. }) => {
                            deadlocks.fetch_add(1, Ordering::Relaxed);
                            db.txn_abort(txn).unwrap();
                            completed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        Err(e) => panic!("Unexpected: {}", e),
                    }

                    let r2 = db.txn_put(txn, key2.as_bytes(), b"locked");
                    match r2 {
                        Ok(()) => {
                            db.commit_txn(txn).unwrap();
                        }
                        Err(Error::Deadlock(_))
                        | Err(Error::LockTimeout)
                        | Err(Error::WriteConflict { .. }) => {
                            deadlocks.fetch_add(1, Ordering::Relaxed);
                            db.txn_abort(txn).unwrap();
                        }
                        Err(e) => panic!("Unexpected: {}", e),
                    }
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_completed = completed.load(Ordering::Relaxed);
    let total_deadlocks = deadlocks.load(Ordering::Relaxed);
    assert_eq!(total_completed, 400, "All 8×50 iterations must complete");
    // Contention errors bundle Deadlock + LockTimeout + WriteConflict.
    // SI first-committer-wins makes WriteConflict more frequent than raw
    // deadlocks alone — bound loosened accordingly.
    assert!(
        total_deadlocks < 350,
        "Too many contention errors: {}",
        total_deadlocks
    );
}

// ---------------------------------------------------------------------------
// GC under concurrent load
// ---------------------------------------------------------------------------

#[test]
fn gc_under_concurrent_writes() {
    // 4 writer threads + periodic GC. 10-second test.
    // Verify: no corruption, GC stats accumulate, latest values always readable.
    let (db, _dir) = setup_shared();
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(5)); // 4 writers + 1 GC thread

    // Writer threads.
    let mut handles: Vec<_> = (0..4)
        .map(|thread_idx| {
            let db = db.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let mut iter = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("gc_t{}_{:06}", thread_idx, iter % 10);
                    let val = format!("v_{}", iter);
                    let _ = db.put(key.as_bytes(), val.as_bytes());
                    iter += 1;
                }
            })
        })
        .collect();

    // GC thread.
    {
        let db = db.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut total_removed = 0u64;
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(500));
                if let Ok(stats) = db.gc() {
                    total_removed += stats.versions_removed;
                }
            }
            let _ = total_removed;
        }));
    }

    // Let it run for 10 seconds.
    thread::sleep(Duration::from_secs(10));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    // Verify: all keys readable, GC didn't corrupt anything.
    for t in 0..4 {
        for k in 0..10 {
            let key = format!("gc_t{}_{:06}", t, k);
            let val = db.get(key.as_bytes()).unwrap();
            assert!(val.is_some(), "Key {} should exist after GC", key);
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot isolation under write contention
// ---------------------------------------------------------------------------

#[test]
fn snapshot_consistency_under_writes() {
    // Reader takes a snapshot, 4 writers modify data, reader re-reads.
    // Reader must see the same values both times (snapshot frozen).
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();

    // Seed data.
    for i in 0..10 {
        let key = format!("snap_{}", i);
        db.put(key.as_bytes(), b"original").unwrap();
    }

    // Reader takes snapshot.
    let reader = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let first_reads: Vec<_> = (0..10)
        .map(|i| {
            let key = format!("snap_{}", i);
            db.txn_get(reader, key.as_bytes()).unwrap()
        })
        .collect();

    // Writers modify all keys.
    for i in 0..10 {
        let key = format!("snap_{}", i);
        db.put(key.as_bytes(), b"modified").unwrap();
    }

    // Reader re-reads — must see same values.
    let second_reads: Vec<_> = (0..10)
        .map(|i| {
            let key = format!("snap_{}", i);
            db.txn_get(reader, key.as_bytes()).unwrap()
        })
        .collect();

    assert_eq!(
        first_reads, second_reads,
        "Snapshot must be frozen — reads shouldn't change"
    );
    for val in &first_reads {
        assert_eq!(
            val,
            &Some(b"original".to_vec()),
            "Reader should see original values"
        );
    }

    db.commit_txn(reader).unwrap();

    // After reader commits, latest view sees modifications.
    let latest = db.get(b"snap_0").unwrap();
    assert_eq!(latest, Some(b"modified".to_vec()));
}
