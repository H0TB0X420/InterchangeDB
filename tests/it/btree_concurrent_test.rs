//! B+Tree concurrent access tests.
//!
//! Tests latch crabbing correctness under concurrent insert, delete, and read
//! operations. Uses `thread::scope` so all threads share the same BPM via
//! reference. Each thread creates its own `BTree` handle (cheap — just borrows
//! the BPM).
//!
//! Pool sizes are large enough to avoid NoFreeFrames errors under contention.
//! Node sizes are kept small to force frequent splits/merges and exercise
//! the latch protocol thoroughly.

use std::collections::HashSet;
use std::sync::Barrier;
use std::thread;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::PageId;
use interchangedb::engines::btree::{BTree, BTreeHeaderPage};
use interchangedb::storage::MemoryDiskManager;

// =============================================================================
// Helpers
// =============================================================================

fn setup_bpm(pool_size: usize) -> BufferPoolManager {
    let dm = MemoryDiskManager::new();
    BufferPoolManager::new(pool_size, dm)
}

fn create_empty_tree(bpm: &BufferPoolManager) -> PageId {
    let header_page = bpm.new_page().unwrap();
    let header_page_id = header_page.page_id();
    {
        let mut guard = header_page;
        let header = BTreeHeaderPage::new();
        header.encode(guard.as_mut_slice());
    }
    header_page_id
}

/// Encode i64 as 8-byte big-endian with sign bit flipped for correct
/// lexicographic ordering of signed integers.
fn encode_key(val: i64) -> [u8; 8] {
    let unsigned = (val as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes()
}

fn decode_key(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    let unsigned = u64::from_be_bytes(buf);
    (unsigned ^ (1u64 << 63)) as i64
}

fn encode_value(val: i64) -> [u8; 8] {
    val.to_le_bytes()
}

fn decode_value(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    i64::from_le_bytes(buf)
}

// =============================================================================
// Tests
// =============================================================================

/// Multiple threads read the same pre-inserted keys simultaneously.
/// Verifies no panics or data corruption under concurrent read access.
#[test]
fn test_concurrent_readers() {
    let bpm = setup_bpm(200);
    let header_id = create_empty_tree(&bpm);

    // Pre-insert 100 keys single-threaded.
    let tree = BTree::with_sizes(&bpm, header_id, 4, 5);
    let key_count = 100;
    for i in 0..key_count {
        let key = encode_key(i);
        let val = encode_value(i * 10);
        assert!(tree.insert(&key, &val).unwrap());
    }

    let thread_count = 4;
    let barrier = Barrier::new(thread_count);

    // Spawn N reader threads that all verify every key.
    thread::scope(|s| {
        for _ in 0..thread_count {
            let barrier = &barrier;
            let bpm = &bpm;
            s.spawn(move || {
                let tree = BTree::with_sizes(bpm, header_id, 4, 5);
                barrier.wait();

                for i in 0..key_count {
                    let key = encode_key(i);
                    let val = tree.get(&key).unwrap();
                    assert!(val.is_some(), "Key {} not found", i);
                    assert_eq!(decode_value(&val.unwrap()), i * 10);
                }
            });
        }
    });
}

/// N threads insert non-overlapping key ranges into the same tree.
/// After all threads complete, every key from every range must be present.
#[test]
fn test_concurrent_insert_disjoint() {
    let bpm = setup_bpm(500);
    let header_id = create_empty_tree(&bpm);

    let thread_count = 4;
    let keys_per_thread = 50;
    let barrier = Barrier::new(thread_count);

    thread::scope(|s| {
        for t in 0..thread_count {
            let barrier = &barrier;
            let bpm = &bpm;
            s.spawn(move || {
                let tree = BTree::with_sizes(bpm, header_id, 4, 5);
                barrier.wait();

                let start = (t * keys_per_thread) as i64;
                let end = start + keys_per_thread as i64;
                for i in start..end {
                    let key = encode_key(i);
                    let val = encode_value(i * 100);
                    tree.insert(&key, &val).unwrap();
                }
            });
        }
    });

    // Verify all keys present.
    let tree = BTree::with_sizes(&bpm, header_id, 4, 5);
    let total = (thread_count * keys_per_thread) as i64;
    for i in 0..total {
        let key = encode_key(i);
        let val = tree.get(&key).unwrap();
        assert!(val.is_some(), "Key {} not found after concurrent insert", i);
        assert_eq!(decode_value(&val.unwrap()), i * 100);
    }
}

/// N threads all insert the same key range (duplicates). Only one insert
/// per key should succeed. Verify the final count matches the range size.
#[test]
fn test_concurrent_insert_overlapping() {
    let bpm = setup_bpm(500);
    let header_id = create_empty_tree(&bpm);

    let thread_count = 4;
    let key_count: i64 = 50;
    let barrier = Barrier::new(thread_count);

    thread::scope(|s| {
        for _ in 0..thread_count {
            let barrier = &barrier;
            let bpm = &bpm;
            s.spawn(move || {
                let tree = BTree::with_sizes(bpm, header_id, 4, 5);
                barrier.wait();

                for i in 0..key_count {
                    let key = encode_key(i);
                    let val = encode_value(i);
                    // Ignore return value — only one thread "wins" each key.
                    let _ = tree.insert(&key, &val);
                }
            });
        }
    });

    // Verify exactly key_count unique keys exist.
    let tree = BTree::with_sizes(&bpm, header_id, 4, 5);
    let all = tree.scan_all().unwrap();
    assert_eq!(all.len(), key_count as usize);
    for (i, (k, _v)) in all.iter().enumerate() {
        assert_eq!(decode_key(k), i as i64);
    }
}

/// Pre-insert keys, then N threads delete disjoint ranges. After completion,
/// the tree should be empty.
#[test]
fn test_concurrent_delete() {
    let bpm = setup_bpm(500);
    let header_id = create_empty_tree(&bpm);

    let thread_count = 4;
    let keys_per_thread = 50;
    let total = (thread_count * keys_per_thread) as i64;

    // Pre-insert all keys.
    let tree = BTree::with_sizes(&bpm, header_id, 4, 5);
    for i in 0..total {
        let key = encode_key(i);
        let val = encode_value(i);
        assert!(tree.insert(&key, &val).unwrap());
    }

    let barrier = Barrier::new(thread_count);

    // Each thread deletes its disjoint range.
    thread::scope(|s| {
        for t in 0..thread_count {
            let barrier = &barrier;
            let bpm = &bpm;
            s.spawn(move || {
                let tree = BTree::with_sizes(bpm, header_id, 4, 5);
                barrier.wait();

                let start = (t * keys_per_thread) as i64;
                let end = start + keys_per_thread as i64;
                for i in start..end {
                    let key = encode_key(i);
                    assert!(
                        tree.delete(&key).unwrap(),
                        "Key {} should exist for delete",
                        i
                    );
                }
            });
        }
    });

    // Tree should be empty.
    let tree = BTree::with_sizes(&bpm, header_id, 4, 5);
    assert!(tree.is_empty().unwrap());
}

/// Concurrent readers and writers. Inserters add keys while readers
/// continuously scan. No panics or corruption should occur.
#[test]
fn test_concurrent_reader_writer() {
    let bpm = setup_bpm(500);
    let header_id = create_empty_tree(&bpm);

    let writer_count = 2;
    let reader_count = 2;
    let keys_per_writer = 50;
    let barrier = Barrier::new(writer_count + reader_count);

    thread::scope(|s| {
        // Writer threads.
        for t in 0..writer_count {
            let barrier = &barrier;
            let bpm = &bpm;
            s.spawn(move || {
                let tree = BTree::with_sizes(bpm, header_id, 4, 5);
                barrier.wait();

                let start = (t * keys_per_writer) as i64;
                let end = start + keys_per_writer as i64;
                for i in start..end {
                    let key = encode_key(i);
                    let val = encode_value(i);
                    let _ = tree.insert(&key, &val);
                }
            });
        }

        // Reader threads.
        for _ in 0..reader_count {
            let barrier = &barrier;
            let bpm = &bpm;
            s.spawn(move || {
                let tree = BTree::with_sizes(bpm, header_id, 4, 5);
                barrier.wait();

                // Continuously read for a while. Keys may or may not be present
                // yet — we just verify no panics.
                for round in 0..5 {
                    let total = (writer_count * keys_per_writer) as i64;
                    for i in 0..total {
                        let key = encode_key(i);
                        let _ = tree.get(&key);
                        let _ = tree.contains(&key);
                    }
                    // Also try scanning.
                    if round == 0 {
                        let _ = tree.scan_all();
                    }
                }
            });
        }
    });

    // After all threads complete, verify all inserted keys are present.
    let tree = BTree::with_sizes(&bpm, header_id, 4, 5);
    let total = (writer_count * keys_per_writer) as i64;
    for i in 0..total {
        let key = encode_key(i);
        assert!(tree.contains(&key).unwrap(), "Key {} should exist", i);
    }
}

/// Concurrent inserters and deleters. Inserters add keys, deleters remove
/// some of them. Final state should be consistent — every key is either
/// present or absent, no corruption.
#[test]
fn test_concurrent_insert_delete_mix() {
    let bpm = setup_bpm(500);
    let header_id = create_empty_tree(&bpm);

    let key_count: i64 = 100;

    // Pre-insert even keys [0, 2, 4, ..., 98] so deleters have something to delete.
    let tree = BTree::with_sizes(&bpm, header_id, 4, 5);
    for i in (0..key_count).step_by(2) {
        let key = encode_key(i);
        let val = encode_value(i);
        assert!(tree.insert(&key, &val).unwrap());
    }

    let barrier = Barrier::new(4);
    let barrier_ref = &barrier;
    let bpm_ref = &bpm;

    thread::scope(|s| {
        // Thread 1: Insert odd keys [1, 3, 5, ..., 99].
        s.spawn(move || {
            let tree = BTree::with_sizes(bpm_ref, header_id, 4, 5);
            barrier_ref.wait();

            for i in (1..key_count).step_by(2) {
                let key = encode_key(i);
                let val = encode_value(i);
                let _ = tree.insert(&key, &val);
            }
        });

        // Thread 2: Insert more odd keys (overlapping with thread 1).
        s.spawn(move || {
            let tree = BTree::with_sizes(bpm_ref, header_id, 4, 5);
            barrier_ref.wait();

            for i in (1..key_count).step_by(2) {
                let key = encode_key(i);
                let val = encode_value(i * 10);
                let _ = tree.insert(&key, &val);
            }
        });

        // Thread 3: Delete even keys [0, 2, 4, ..., 48] (first half).
        s.spawn(move || {
            let tree = BTree::with_sizes(bpm_ref, header_id, 4, 5);
            barrier_ref.wait();

            for i in (0..key_count / 2).step_by(2) {
                let key = encode_key(i);
                let _ = tree.delete(&key);
            }
        });

        // Thread 4: Reader.
        s.spawn(move || {
            let tree = BTree::with_sizes(bpm_ref, header_id, 4, 5);
            barrier_ref.wait();

            for _ in 0..3 {
                for i in 0..key_count {
                    let key = encode_key(i);
                    let _ = tree.get(&key);
                }
            }
        });
    });

    // Verify consistency: every key is either present or absent, no partial state.
    let tree = BTree::with_sizes(&bpm, header_id, 4, 5);
    let all = tree.scan_all().unwrap();

    // All returned entries have valid key encoding and are in sorted order.
    let mut prev: Option<i64> = None;
    let mut present_keys = HashSet::new();
    for (k, _v) in &all {
        let key_val = decode_key(k);
        if let Some(p) = prev {
            assert!(
                key_val > p,
                "Keys not in sorted order: {} after {}",
                key_val,
                p
            );
        }
        prev = Some(key_val);
        present_keys.insert(key_val);
    }

    // Odd keys [1..99] should all be present (inserted, not deleted).
    for i in (1..key_count).step_by(2) {
        assert!(present_keys.contains(&i), "Odd key {} should be present", i);
    }

    // Even keys [50, 52, ..., 98] should still be present (not deleted).
    for i in (key_count / 2..key_count).step_by(2) {
        assert!(
            present_keys.contains(&i),
            "Even key {} (not deleted) should be present",
            i
        );
    }

    // Even keys [0, 2, ..., 48] should be deleted.
    for i in (0..key_count / 2).step_by(2) {
        assert!(
            !present_keys.contains(&i),
            "Even key {} should have been deleted",
            i
        );
    }
}
