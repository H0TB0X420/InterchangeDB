//! Integration tests for LockManager — multi-threaded lock scenarios.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use interchangedb::txn::lock_manager::{LockManager, LockMode};
use interchangedb::txn::TxnId;

/// Create a lock manager with a generous timeout for integration tests.
fn lm() -> Arc<LockManager> {
    Arc::new(LockManager::with_timeout(Duration::from_secs(10)))
}

#[test]
fn concurrent_shared_readers() {
    // 8 threads acquire S locks on the same key simultaneously.
    // All should succeed without blocking.
    let lm = lm();
    let barrier = Arc::new(Barrier::new(8));
    let success_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (1..=8)
        .map(|i| {
            let lm = lm.clone();
            let barrier = barrier.clone();
            let success_count = success_count.clone();
            thread::spawn(move || {
                barrier.wait();
                lm.acquire(TxnId::new(i), b"shared_key", LockMode::Shared)
                    .unwrap();
                success_count.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(success_count.load(Ordering::Relaxed), 8);

    // All 8 hold the lock.
    for i in 1..=8 {
        assert_eq!(lm.held_by(TxnId::new(i)), vec![b"shared_key".to_vec()]);
    }
}

#[test]
fn exclusive_serialization() {
    // 4 threads acquire X lock on same key.
    // They must execute serially — only one holds at a time.
    let lm = lm();
    let barrier = Arc::new(Barrier::new(4));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let current_holders = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (1..=4)
        .map(|i| {
            let lm = lm.clone();
            let barrier = barrier.clone();
            let max_concurrent = max_concurrent.clone();
            let current_holders = current_holders.clone();
            thread::spawn(move || {
                barrier.wait();
                lm.acquire(TxnId::new(i), b"excl_key", LockMode::Exclusive)
                    .unwrap();

                // Record that we hold the lock.
                let prev = current_holders.fetch_add(1, Ordering::SeqCst);
                // Update max concurrent holders seen.
                let current = prev + 1;
                max_concurrent.fetch_max(current, Ordering::SeqCst);

                // Hold briefly.
                thread::sleep(Duration::from_millis(10));

                current_holders.fetch_sub(1, Ordering::SeqCst);
                lm.release(TxnId::new(i), b"excl_key");
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // At most 1 thread held the X lock at any time.
    assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
    assert_eq!(lm.lock_count(), 0);
}

#[test]
fn mixed_readers_writer() {
    // 3 readers hold S, writer waits for X.
    // Readers release one at a time, writer gets X after all release.
    let lm = lm();
    let t_writer = TxnId::new(100);

    // 3 readers acquire S.
    for i in 1..=3 {
        lm.acquire(TxnId::new(i), b"key", LockMode::Shared).unwrap();
    }

    // Writer thread requests X — will block.
    let lm2 = lm.clone();
    let writer_handle = thread::spawn(move || lm2.acquire(t_writer, b"key", LockMode::Exclusive));

    thread::sleep(Duration::from_millis(50));

    // Writer should be waiting.
    assert!(lm.held_by(t_writer).is_empty());

    // Release readers one by one.
    lm.release(TxnId::new(1), b"key");
    thread::sleep(Duration::from_millis(20));
    assert!(lm.held_by(t_writer).is_empty()); // Still 2 S holders.

    lm.release(TxnId::new(2), b"key");
    thread::sleep(Duration::from_millis(20));
    assert!(lm.held_by(t_writer).is_empty()); // Still 1 S holder.

    lm.release(TxnId::new(3), b"key");

    // Writer should now get X.
    writer_handle.join().unwrap().unwrap();
    assert_eq!(lm.held_by(t_writer), vec![b"key".to_vec()]);
}

#[test]
fn lock_release_under_contention() {
    // 8 threads, 10 keys, random S/X acquire/release cycles.
    // Verify: no hangs, no panics, all threads complete.
    let lm = Arc::new(LockManager::with_timeout(Duration::from_secs(5)));
    let barrier = Arc::new(Barrier::new(8));

    let handles: Vec<_> = (0..8)
        .map(|thread_idx| {
            let lm = lm.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let txn_id = TxnId::new(thread_idx as u64 + 1);

                for iteration in 0..50 {
                    // Deterministic "random" key and mode based on thread + iteration.
                    let key_idx = (thread_idx * 7 + iteration * 3) % 10;
                    let key = format!("key_{}", key_idx);
                    let mode = if (thread_idx + iteration) % 3 == 0 {
                        LockMode::Exclusive
                    } else {
                        LockMode::Shared
                    };

                    match lm.acquire(txn_id, key.as_bytes(), mode) {
                        Ok(()) => {
                            // Hold briefly, then release.
                            thread::yield_now();
                            lm.release(txn_id, key.as_bytes());
                        }
                        Err(_) => {
                            // Timeout is acceptable under contention.
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // All locks should be released.
    assert_eq!(lm.lock_count(), 0);
}
