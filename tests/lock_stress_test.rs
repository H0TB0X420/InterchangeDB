//! Stress test for LockManager under high contention.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use interchangedb::txn::lock_manager::{LockManager, LockMode};
use interchangedb::txn::TxnId;

#[test]
fn high_contention_stress() {
    // 16 threads, 5 hotspot keys, 1000 lock/release cycles each.
    // Random S/X mode. Verify: no deadlocks within lock manager, no panics,
    // all threads complete within 30 seconds, lock table empty after.
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let barrier = Arc::new(Barrier::new(16));
    let completed = Arc::new(AtomicUsize::new(0));
    let timeouts = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..16)
        .map(|thread_idx: usize| {
            let lm = lm.clone();
            let barrier = barrier.clone();
            let completed = completed.clone();
            let timeouts = timeouts.clone();
            thread::spawn(move || {
                barrier.wait();
                let txn_id = TxnId::new(thread_idx as u64 + 1);

                for iteration in 0usize..1000 {
                    // Deterministic pseudo-random key (5 hotspot keys).
                    let key_idx =
                        (thread_idx.wrapping_mul(31) + iteration.wrapping_mul(17)) % 5;
                    let key = format!("hot_{}", key_idx);

                    // Deterministic pseudo-random mode.
                    let mode = if (thread_idx + iteration) % 4 == 0 {
                        LockMode::Exclusive
                    } else {
                        LockMode::Shared
                    };

                    match lm.acquire(txn_id, key.as_bytes(), mode) {
                        Ok(()) => {
                            // Simulate work: yield to increase contention overlap.
                            thread::yield_now();
                            lm.release(txn_id, key.as_bytes());
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            timeouts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_completed = completed.load(Ordering::Relaxed);
    let total_timeouts = timeouts.load(Ordering::Relaxed);

    // All threads finished within 30 seconds.
    assert!(
        elapsed < Duration::from_secs(30),
        "Stress test took too long: {:?}",
        elapsed
    );

    // Most operations should succeed (some timeouts acceptable under contention).
    let total_ops = 16 * 1000;
    assert_eq!(total_completed + total_timeouts, total_ops);
    assert!(
        total_completed > total_ops / 2,
        "Too many timeouts: {}/{} succeeded",
        total_completed,
        total_ops
    );

    // Lock table should be empty after all threads done.
    assert_eq!(lm.lock_count(), 0);
}
