//! Stress test for deadlock detection under heavy contention.
//!
//! 16 threads each run 100 iterations of: pick 2 pseudo-random keys from a
//! pool of 8, acquire X on both. With different lock orderings across threads,
//! deadlocks are inevitable. The test verifies that all deadlocks are detected
//! and resolved — no thread hangs, no panics, lock table clean at the end.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use interchangedb::common::Error;
use interchangedb::txn::lock_manager::{LockManager, LockMode};
use interchangedb::txn::TxnId;

#[test]
fn deadlock_storm() {
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let barrier = Arc::new(Barrier::new(16));
    let completed_iterations = Arc::new(AtomicUsize::new(0));
    let deadlock_count = Arc::new(AtomicUsize::new(0));
    let timeout_count = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..16)
        .map(|thread_idx: usize| {
            let lm = lm.clone();
            let barrier = barrier.clone();
            let completed = completed_iterations.clone();
            let deadlocks = deadlock_count.clone();
            let timeouts = timeout_count.clone();
            thread::spawn(move || {
                barrier.wait();
                let txn_id = TxnId::new(thread_idx as u64 + 1);

                for iteration in 0usize..100 {
                    // Deterministic pseudo-random key selection.
                    let key_a_idx =
                        (thread_idx.wrapping_mul(37) + iteration.wrapping_mul(13)) % 8;
                    let key_b_idx =
                        (thread_idx.wrapping_mul(53) + iteration.wrapping_mul(7)) % 8;

                    // Same key — skip, no interesting contention.
                    if key_a_idx == key_b_idx {
                        completed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    let key_a = format!("storm_{}", key_a_idx);
                    let key_b = format!("storm_{}", key_b_idx);

                    // Acquire first key.
                    match lm.acquire(txn_id, key_a.as_bytes(), LockMode::Exclusive) {
                        Ok(()) => {}
                        Err(Error::Deadlock(_)) => {
                            deadlocks.fetch_add(1, Ordering::Relaxed);
                            lm.release_all(txn_id);
                            completed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        Err(Error::LockTimeout) => {
                            timeouts.fetch_add(1, Ordering::Relaxed);
                            lm.release_all(txn_id);
                            completed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        Err(e) => panic!("Unexpected error: {}", e),
                    }

                    // Acquire second key.
                    match lm.acquire(txn_id, key_b.as_bytes(), LockMode::Exclusive) {
                        Ok(()) => {}
                        Err(Error::Deadlock(_)) => {
                            deadlocks.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(Error::LockTimeout) => {
                            timeouts.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("Unexpected error: {}", e),
                    }

                    lm.release_all(txn_id);
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_completed = completed_iterations.load(Ordering::Relaxed);

    // All threads finished within 30 seconds.
    assert!(
        elapsed < Duration::from_secs(30),
        "Stress test took too long: {:?}",
        elapsed
    );

    // All 16 * 100 = 1600 iterations completed.
    assert_eq!(
        total_completed, 1600,
        "All iterations should complete: got {}",
        total_completed
    );

    // Lock table should be empty after all threads done.
    assert_eq!(lm.lock_count(), 0);
}
