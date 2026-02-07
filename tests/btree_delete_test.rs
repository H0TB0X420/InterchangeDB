//! B+Tree delete tests.
//!
//! Ported from BusTub's b_plus_tree_delete_test.cpp.
//! Tests use small node sizes (leaf_max_size=2-5, internal_max_size=3)
//! to force splits, merges, and redistributions with very few keys.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::PageId;
use interchangedb::index::btree::{BTree, BTreeHeaderPage};
use interchangedb::storage::DiskManager;
use tempfile::tempdir;

// =============================================================================
// Helpers
// =============================================================================

fn setup_bpm(pool_size: usize) -> (BufferPoolManager, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = DiskManager::create(&path).unwrap();
    (BufferPoolManager::new(pool_size, dm), dir)
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

/// Encode i64 as 8-byte big-endian with sign bit flipped.
///
/// This gives correct lexicographic ordering for signed integers:
/// -2 < -1 < 0 < 1 < 2 maps to ascending byte sequences.
fn encode_key(val: i64) -> [u8; 8] {
    let unsigned = (val as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes()
}

/// Decode key bytes back to i64.
fn decode_key(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    let unsigned = u64::from_be_bytes(buf);
    (unsigned ^ (1u64 << 63)) as i64
}

/// Encode i64 value as 8-byte little-endian (just for storage).
fn encode_value(val: i64) -> [u8; 8] {
    val.to_le_bytes()
}

/// Decode value bytes back to i64.
fn decode_value(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    i64::from_le_bytes(buf)
}

/// Verify that the tree contains exactly the expected keys and none of the
/// deleted keys. Panics on mismatch.
fn assert_tree_values_match(tree: &BTree, inserted: &[i64], deleted: &[i64]) {
    // Every inserted key should be findable with correct value.
    for &key in inserted {
        let k = encode_key(key);
        let result = tree.get(&k).unwrap();
        assert!(
            result.is_some(),
            "Key {} should be present but was not found",
            key,
        );
        let val = decode_value(&result.unwrap());
        assert_eq!(val, key, "Value mismatch for key {}", key);
    }

    // Every deleted key should NOT be findable.
    for &key in deleted {
        let k = encode_key(key);
        let result = tree.get(&k).unwrap();
        assert!(
            result.is_none(),
            "Key {} should be deleted but was found",
            key,
        );
    }
}

// =============================================================================
// Tests
// =============================================================================

/// BusTub: DeleteTestNoIterator
///
/// How: Insert [1,2,3,4,5] with leaf_max_size=2, internal_max_size=3.
///      Remove [1,5,3,4], verify only key 2 remains.
///      Remove key 2, verify tree is empty.
/// Why: Tests delete with rebalancing (merges) using tiny node sizes.
#[test]
fn test_delete_no_iterator() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 2, 3);

    let keys: Vec<i64> = vec![1, 2, 3, 4, 5];
    for &key in &keys {
        let k = encode_key(key);
        let v = encode_value(key);
        tree.insert(&k, &v).unwrap();
    }

    // Verify all keys retrievable.
    for &key in &keys {
        let k = encode_key(key);
        let result = tree.get(&k).unwrap();
        assert!(result.is_some(), "Key {} not found after insert", key);
        assert_eq!(decode_value(&result.unwrap()), key);
    }

    // Remove [1, 5, 3, 4].
    let remove_keys: Vec<i64> = vec![1, 5, 3, 4];
    for &key in &remove_keys {
        let k = encode_key(key);
        tree.delete(&k).unwrap();
    }

    // Verify: removed keys absent, remaining keys present.
    let mut remaining_count = 0;
    for &key in &keys {
        let k = encode_key(key);
        let result = tree.get(&k).unwrap();
        if result.is_none() {
            assert!(
                remove_keys.contains(&key),
                "Key {} missing but was not in remove list",
                key,
            );
        } else {
            assert_eq!(decode_value(&result.unwrap()), key);
            remaining_count += 1;
        }
    }
    assert_eq!(remaining_count, 1, "Only key 2 should remain");

    // Remove the last key — tree should become empty.
    let k = encode_key(2);
    tree.delete(&k).unwrap();
    assert!(tree.is_empty().unwrap(), "Tree should be empty after deleting all keys");
}

/// BusTub: OptimisticDeleteTest (adapted)
///
/// How: Insert 25 keys with leaf_max_size=4, internal_max_size=3.
///      Find a leaf that won't underflow on delete. Delete one key from it.
///      Verify the key is gone and all others remain.
/// Why: Tests the simple case where delete doesn't trigger rebalancing.
///
/// Note: BusTub's original test checks I/O counts (reads/writes).
/// We skip I/O counting since our optimistic path is deferred to 3.2.6.
#[test]
fn test_optimistic_delete() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 4, 3);

    let num_keys: i64 = 25;
    for i in 0..num_keys {
        let k = encode_key(i);
        let v = encode_value(i);
        tree.insert(&k, &v).unwrap();
    }

    // Walk leaves to find one with size > min_size (safe for delete).
    let mut to_delete: Option<i64> = None;
    let first_leaf = tree.get_first_leaf().unwrap().unwrap();
    let mut current = first_leaf;
    while current != PageId::INVALID {
        let leaf = tree.read_leaf(current).unwrap();
        if leaf.is_safe_for_delete() {
            // Pick the first key of this leaf.
            to_delete = Some(decode_key(leaf.key_at(0)));
        }
        current = leaf.next_page_id;
    }

    let to_delete = to_delete.expect("Should find a leaf safe for delete");

    // Delete the key — no rebalancing needed.
    let k = encode_key(to_delete);
    tree.delete(&k).unwrap();

    // Verify deleted key is gone.
    assert!(tree.get(&k).unwrap().is_none(), "Deleted key should be gone");

    // Verify all other keys still present.
    for i in 0..num_keys {
        if i == to_delete {
            continue;
        }
        let k = encode_key(i);
        assert!(
            tree.get(&k).unwrap().is_some(),
            "Key {} should still exist",
            i,
        );
    }
}

/// BusTub: SequentialEdgeMixTest
///
/// How: For each leaf_max_size in [2, 3, 4, 5] (internal_max_size=3):
///      Insert keys in mixed order, verifying after each insert.
///      Delete key 1, insert key 3, then delete all remaining — verifying
///      after every single mutation.
/// Why: Tests rebalancing correctness across all small node sizes
///      with interleaved inserts and deletes.
#[test]
fn test_sequential_edge_mix() {
    let (bpm, _dir) = setup_bpm(50);

    for leaf_max_size in 2..=5u16 {
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::with_sizes(&bpm, header_id, leaf_max_size, 3);

        let insert_keys: Vec<i64> = vec![1, 5, 15, 20, 25, 2, -1, -2, 6, 14, 4];
        let mut inserted: Vec<i64> = Vec::new();
        let mut deleted: Vec<i64> = Vec::new();

        // Insert all keys, verifying after each.
        for &key in &insert_keys {
            let k = encode_key(key);
            let v = encode_value(key);
            tree.insert(&k, &v).unwrap();
            inserted.push(key);
            assert_tree_values_match(&tree, &inserted, &deleted);
        }

        // Remove key 1.
        let k = encode_key(1);
        tree.delete(&k).unwrap();
        deleted.push(1);
        inserted.retain(|&x| x != 1);
        assert_tree_values_match(&tree, &inserted, &deleted);

        // Insert key 3.
        let k = encode_key(3);
        let v = encode_value(3);
        tree.insert(&k, &v).unwrap();
        inserted.push(3);
        assert_tree_values_match(&tree, &inserted, &deleted);

        // Remove all remaining keys in specific order.
        let remove_keys: Vec<i64> = vec![4, 14, 6, 2, 15, -2, -1, 3, 5, 25, 20];
        for &key in &remove_keys {
            let k = encode_key(key);
            tree.delete(&k).unwrap();
            deleted.push(key);
            inserted.retain(|&x| x != key);
            assert_tree_values_match(&tree, &inserted, &deleted);
        }

        // Tree should be empty.
        assert!(tree.is_empty().unwrap(), "Tree should be empty for leaf_max_size={}", leaf_max_size);
    }
}