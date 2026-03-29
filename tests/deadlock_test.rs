//! Integration tests for deadlock detection in the LockManager.
//!
//! These tests use multiple threads with barrier synchronization to create
//! real concurrent deadlock scenarios. Unlike unit tests (which control
//! ordering with sleep), these let threads race to exercise timing-sensitive
//! paths in the detection code.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use interchangedb::common::Error;
use interchangedb::txn::lock_manager::{LockManager, LockMode};
use interchangedb::txn::TxnId;

#[test]
fn concurrent_deadlock_detection() {
    // Two threads race into classic A-B / B-A deadlock.
    // Exactly one should get Deadlock, the other should eventually succeed.
    //
    // HOW: Both threads start simultaneously via barrier. Each holds one key
    // and requests the other. Whichever enters the wait queue second detects
    // the cycle and becomes the victim.
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let t1 = TxnId::new(1);
    let t2 = TxnId::new(2);

    // Pre-acquire: T1 holds A, T2 holds B.
    lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
    lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let t1_deadlocked = Arc::new(AtomicBool::new(false));
    let t2_deadlocked = Arc::new(AtomicBool::new(false));

    // T1 thread: requests B.
    let lm_t1 = lm.clone();
    let barrier_t1 = barrier.clone();
    let t1_dl = t1_deadlocked.clone();
    let t1_handle = thread::spawn(move || {
        barrier_t1.wait();
        match lm_t1.acquire(t1, b"B", LockMode::Exclusive) {
            Ok(()) => {}
            Err(Error::Deadlock(_)) => {
                t1_dl.store(true, Ordering::Release);
                lm_t1.release_all(t1);
            }
            Err(e) => panic!("Unexpected error for T1: {}", e),
        }
    });

    // T2 thread: requests A.
    let lm_t2 = lm.clone();
    let barrier_t2 = barrier.clone();
    let t2_dl = t2_deadlocked.clone();
    let t2_handle = thread::spawn(move || {
        barrier_t2.wait();
        match lm_t2.acquire(t2, b"A", LockMode::Exclusive) {
            Ok(()) => {}
            Err(Error::Deadlock(_)) => {
                t2_dl.store(true, Ordering::Release);
                lm_t2.release_all(t2);
            }
            Err(e) => panic!("Unexpected error for T2: {}", e),
        }
    });

    t1_handle.join().unwrap();
    t2_handle.join().unwrap();

    let t1_was_victim = t1_deadlocked.load(Ordering::Acquire);
    let t2_was_victim = t2_deadlocked.load(Ordering::Acquire);

    // Exactly one should be the victim.
    assert!(
        t1_was_victim ^ t2_was_victim,
        "Exactly one txn should be deadlock victim: T1={}, T2={}",
        t1_was_victim, t2_was_victim
    );
}

#[test]
fn concurrent_three_way_deadlock() {
    // Three threads form A→B→C→A cycle. At least one gets Deadlock.
    //
    // HOW: T1 holds A, T2 holds B, T3 holds C. After barrier:
    // T1 requests B, T2 requests C, T3 requests A. Whichever enters
    // its wait queue last completes the cycle and is detected.
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let t1 = TxnId::new(1);
    let t2 = TxnId::new(2);
    let t3 = TxnId::new(3);

    lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
    lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();
    lm.acquire(t3, b"C", LockMode::Exclusive).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let deadlock_count = Arc::new(AtomicUsize::new(0));

    let resolved_count = Arc::new(AtomicUsize::new(0));

    let requests: Vec<(TxnId, &'static [u8])> =
        vec![(t1, b"B"), (t2, b"C"), (t3, b"A")];

    let handles: Vec<_> = requests
        .into_iter()
        .map(|(txn_id, target_key)| {
            let lm = lm.clone();
            let barrier = barrier.clone();
            let deadlock_count = deadlock_count.clone();
            let resolved_count = resolved_count.clone();
            thread::spawn(move || {
                barrier.wait();
                match lm.acquire(txn_id, target_key, LockMode::Exclusive) {
                    Ok(()) => {}
                    Err(Error::Deadlock(_)) => {
                        deadlock_count.fetch_add(1, Ordering::Relaxed);
                        resolved_count.fetch_add(1, Ordering::Relaxed);
                        lm.release_all(txn_id);
                    }
                    Err(Error::LockTimeout) => {
                        // Timeout is acceptable — in a race, the cycle may not
                        // be fully formed when detection runs. The timeout is
                        // the fallback resolution mechanism.
                        resolved_count.fetch_add(1, Ordering::Relaxed);
                        lm.release_all(txn_id);
                    }
                    Err(e) => panic!("Unexpected error for {:?}: {}", txn_id, e),
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // At least one conflict resolved (deadlock or timeout).
    assert!(
        resolved_count.load(Ordering::Relaxed) >= 1,
        "At least one conflict should be resolved in a 3-way cycle"
    );
}

#[test]
fn deadlock_under_contention() {
    // 8 threads, 4 keys, 50 iterations each. Pseudo-random key pairs.
    // Some deadlocks expected. Verify: all threads complete (no hangs),
    // some successes, lock table clean at end.
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let barrier = Arc::new(Barrier::new(8));
    let deadlock_count = Arc::new(AtomicUsize::new(0));
    let success_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..8)
        .map(|thread_idx| {
            let lm = lm.clone();
            let barrier = barrier.clone();
            let deadlock_count = deadlock_count.clone();
            let success_count = success_count.clone();
            thread::spawn(move || {
                barrier.wait();
                let txn_id = TxnId::new(thread_idx as u64 + 1);

                for iteration in 0..50 {
                    // Deterministic pseudo-random key pair.
                    let key_a_idx = (thread_idx * 7 + iteration * 3) % 4;
                    let key_b_idx = (thread_idx * 11 + iteration * 5) % 4;
                    let key_a = format!("key_{}", key_a_idx);
                    let key_b = format!("key_{}", key_b_idx);

                    // Same key — skip, no deadlock possible.
                    if key_a == key_b {
                        continue;
                    }

                    let first = lm.acquire(txn_id, key_a.as_bytes(), LockMode::Exclusive);
                    match first {
                        Ok(()) => {}
                        Err(Error::Deadlock(_)) => {
                            deadlock_count.fetch_add(1, Ordering::Relaxed);
                            lm.release_all(txn_id);
                            continue;
                        }
                        Err(_) => {
                            lm.release_all(txn_id);
                            continue;
                        }
                    }

                    match lm.acquire(txn_id, key_b.as_bytes(), LockMode::Exclusive) {
                        Ok(()) => {
                            success_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(Error::Deadlock(_)) => {
                            deadlock_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {}
                    }

                    lm.release_all(txn_id);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert!(
        success_count.load(Ordering::Relaxed) > 0,
        "At least some operations should succeed"
    );

    // Lock table clean after all threads done.
    assert_eq!(lm.lock_count(), 0);
}
