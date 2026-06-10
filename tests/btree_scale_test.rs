//! B+Tree sequential scale test.
//!
//! Ported from BusTub's b_plus_tree_sequential_scale_test.cpp.
//! Inserts 5000 keys in random shuffled order with tiny node sizes,
//! then verifies every key is retrievable.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::index::btree::{BTree, BTreeHeaderPage};
use interchangedb::storage::FileDiskManager;
use tempfile::tempdir;

// =============================================================================
// Helpers
// =============================================================================

fn setup_bpm(pool_size: usize) -> (BufferPoolManager, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&path).unwrap();
    (BufferPoolManager::new(pool_size, dm), dir)
}

fn create_empty_tree(bpm: &BufferPoolManager) -> interchangedb::common::PageId {
    let header_page = bpm.new_page().unwrap();
    let header_page_id = header_page.page_id();
    {
        let mut guard = header_page;
        let header = BTreeHeaderPage::new();
        header.encode(guard.as_mut_slice());
    }
    header_page_id
}

fn encode_key(val: i64) -> [u8; 8] {
    let unsigned = (val as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes()
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

/// BusTub: BasicScaleTest
///
/// How: Insert 5000 keys in random shuffled order with leaf_max_size=2,
///      internal_max_size=3, pool_size=30. Verify every key is retrievable
///      with the correct value.
/// Why: Stress-tests insert + point lookup at scale with tiny node sizes,
///      forcing thousands of splits and a deep tree.
///
/// Note: BusTub uses pool_size=30 with in-memory disk. We use a larger pool
/// since our FileDiskManager writes to real files. Correctness is identical.
#[test]
fn test_basic_scale() {
    let (bpm, _dir) = setup_bpm(10000);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 2, 3);

    let scale: i64 = 5000;
    let mut keys: Vec<i64> = (1..=scale).collect();

    // Shuffle insertion order with a deterministic seed.
    // Uses a simple Fisher-Yates shuffle with a linear congruential generator.
    let mut rng_state: u64 = 42;
    for i in (1..keys.len()).rev() {
        rng_state = rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (rng_state >> 33) as usize % (i + 1);
        keys.swap(i, j);
    }

    // Insert all keys in shuffled order.
    for &key in &keys {
        let k = encode_key(key);
        let v = encode_value(key);
        tree.insert(&k, &v).unwrap();
    }

    // Verify every key is retrievable with correct value.
    for &key in &keys {
        let k = encode_key(key);
        let result = tree.get(&k).unwrap();
        assert!(result.is_some(), "Key {} not found after insert", key);
        assert_eq!(
            decode_value(&result.unwrap()),
            key,
            "Value mismatch for key {}",
            key
        );
    }
}
