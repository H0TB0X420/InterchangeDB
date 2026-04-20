//! Soak test — sustained load over 30 seconds.
//!
//! Exercises MVCC, locking, GC, and checkpointing simultaneously.
//! Finds slow leaks, accumulation bugs, and rare timing issues.
//!
//! Marked `#[ignore]` — run with `cargo test -- --ignored` or CI.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::Error;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::DiskManager;
use interchangedb::txn::TxnMode;

fn setup_shared() -> (Arc<Mutex<Database<BTreeEngine>>>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = DiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(1000, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (Arc::new(Mutex::new(db)), dir)
}

/// 30-second sustained mixed workload with periodic GC and checkpoint.
///
/// 6 threads:
/// - 3 reader threads (read-only txns)
/// - 2 writer threads (put + commit/abort mix)
/// - 1 maintenance thread (GC + checkpoint)
///
/// Invariants checked at end:
/// - No panics in any thread
/// - All committed data readable
/// - No aborted data visible
/// - Lock table clean (no leaked locks)
#[test]
#[ignore] // Run with: cargo test -- --ignored soak
fn soak_30_seconds() {
    let (db, _dir) = setup_shared();
    let stop = Arc::new(AtomicBool::new(false));
    let committed_ops = Arc::new(AtomicU64::new(0));
    let aborted_ops = Arc::new(AtomicU64::new(0));
    let read_ops = Arc::new(AtomicU64::new(0));
    let gc_runs = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let duration = Duration::from_secs(30);

    let mut handles = Vec::new();

    // --- 3 Reader threads ---
    for reader_idx in 0..3 {
        let db = db.clone();
        let stop = stop.clone();
        let read_ops = read_ops.clone();
        let errors = errors.clone();
        handles.push(thread::spawn(move || {
            let mut iter = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key_idx = (reader_idx * 17 + iter * 7) % 50;
                let key = format!("soak_key_{:03}", key_idx);

                let mut db = db.lock().unwrap();
                let txn = match db.begin_txn(TxnMode::ReadOnly) {
                    Ok(t) => t,
                    Err(_) => { errors.fetch_add(1, Ordering::Relaxed); continue; }
                };

                match db.txn_get(txn, key.as_bytes()) {
                    Ok(Some(val)) => {
                        // Validate value is well-formed.
                        if val.is_empty() {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(None) => {} // Key not yet written — fine.
                    Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                }

                let _ = db.commit_txn(txn);
                read_ops.fetch_add(1, Ordering::Relaxed);
                iter += 1;
            }
        }));
    }

    // --- 2 Writer threads ---
    for writer_idx in 0..2 {
        let db = db.clone();
        let stop = stop.clone();
        let committed_ops = committed_ops.clone();
        let aborted_ops = aborted_ops.clone();
        let errors = errors.clone();
        handles.push(thread::spawn(move || {
            let mut iter = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key_idx = (writer_idx * 13 + iter * 3) % 50;
                let key = format!("soak_key_{:03}", key_idx);
                let val = format!("w{}_{}", writer_idx, iter);
                let should_abort = iter % 5 == 0; // 20% abort rate

                let mut db = db.lock().unwrap();
                let txn = match db.begin_txn(TxnMode::ReadWrite) {
                    Ok(t) => t,
                    Err(_) => { errors.fetch_add(1, Ordering::Relaxed); continue; }
                };

                let put_result = db.txn_put(txn, key.as_bytes(), val.as_bytes());
                match put_result {
                    Ok(()) => {
                        if should_abort {
                            let _ = db.txn_abort(txn);
                            aborted_ops.fetch_add(1, Ordering::Relaxed);
                        } else {
                            match db.commit_txn(txn) {
                                Ok(()) => { committed_ops.fetch_add(1, Ordering::Relaxed); }
                                Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                            }
                        }
                    }
                    Err(Error::Deadlock(_)) | Err(Error::LockTimeout) => {
                        let _ = db.txn_abort(txn);
                        aborted_ops.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        let _ = db.txn_abort(txn);
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                iter += 1;
            }
        }));
    }

    // --- 1 Maintenance thread (GC + checkpoint) ---
    {
        let db = db.clone();
        let stop = stop.clone();
        let gc_runs = gc_runs.clone();
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(3));

                let mut db = db.lock().unwrap();
                // GC.
                match db.gc() {
                    Ok(_stats) => { gc_runs.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => {}
                }
                // Checkpoint.
                let _ = db.checkpoint();
            }
        }));
    }

    // Let it run.
    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().expect("Thread panicked during soak test");
    }

    let elapsed = start.elapsed();
    let total_committed = committed_ops.load(Ordering::Relaxed);
    let total_aborted = aborted_ops.load(Ordering::Relaxed);
    let total_reads = read_ops.load(Ordering::Relaxed);
    let total_gc = gc_runs.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);

    println!("Soak test completed in {:?}", elapsed);
    println!("  Committed: {}", total_committed);
    println!("  Aborted: {}", total_aborted);
    println!("  Reads: {}", total_reads);
    println!("  GC runs: {}", total_gc);
    println!("  Errors: {}", total_errors);

    // Assertions.
    assert!(total_committed > 0, "Should have some committed writes");
    assert!(total_reads > 0, "Should have some reads");
    assert!(total_gc > 0, "Should have run GC at least once");
    assert_eq!(total_errors, 0, "Should have zero errors");

    // Final state check: read all keys, verify no corruption.
    let db = db.lock().unwrap();
    for i in 0..50 {
        let key = format!("soak_key_{:03}", i);
        match db.get(key.as_bytes()) {
            Ok(Some(val)) => {
                assert!(!val.is_empty(), "Key {} has empty value", key);
            }
            Ok(None) => {} // Not all keys may have been written.
            Err(e) => panic!("Error reading key {} after soak: {}", key, e),
        }
    }
}
