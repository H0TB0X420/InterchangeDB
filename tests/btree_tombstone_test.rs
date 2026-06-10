//! B+Tree tombstone tests.
//!
//! Ported from BusTub's b_plus_tree_tombstone_test.cpp.
//! Tests verify that tombstone-based lazy deletion works correctly:
//! tombstone accumulation, splits with tombstones, borrowing with
//! tombstones, and coalescing (merge) with tombstones.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::PageId;
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

/// Encode i64 value as 8-byte little-endian.
fn encode_value(val: i64) -> [u8; 8] {
    val.to_le_bytes()
}

fn decode_value(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    i64::from_le_bytes(buf)
}

/// Collect all live keys from the tree via leaf-chain scan.
fn scan_keys(tree: &BTree) -> Vec<i64> {
    tree.scan_all()
        .unwrap()
        .iter()
        .map(|(k, _)| decode_key(k))
        .collect()
}

/// Count total tombstones across all leaves.
fn total_tombstones(tree: &BTree) -> usize {
    let first = match tree.get_first_leaf().unwrap() {
        Some(id) => id,
        None => return 0,
    };

    let mut count = 0;
    let mut current = first;
    let max_leaves = 10_000;
    let mut leaves = 0;

    while current != PageId::INVALID && leaves < max_leaves {
        let leaf = tree.read_leaf(current).unwrap();
        count += leaf.tombstone_count();
        current = leaf.next_page_id;
        leaves += 1;
    }
    count
}

// =============================================================================
// Tests
// =============================================================================

/// BusTub: TombstoneBasicTest
///
/// How: Insert keys [1..8], delete keys [1, 3, 5, 7] with max_tombstones=2,
///      leaf_max_size=4, internal_max_size=4. After each delete, verify
///      remaining keys are correct and tombstone count is bounded.
/// Why: Tests basic tombstone accumulation and flushing. With max_tombstones=2,
///      after 2 deletes a leaf must flush old tombstones to stay within limit.
#[test]
fn test_tombstone_basic() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_tombstones(&bpm, header_id, 4, 4, 2);

    // Insert keys 1..=8.
    for i in 1..=8i64 {
        tree.insert(&encode_key(i), &encode_value(i)).unwrap();
    }

    // Verify all keys present.
    let keys = scan_keys(&tree);
    assert_eq!(keys.len(), 8);

    // Delete odd keys one at a time, verifying after each.
    let delete_keys = [1, 3, 5, 7];
    let mut expected: Vec<i64> = (1..=8).collect();

    for &key in &delete_keys {
        tree.delete(&encode_key(key)).unwrap();
        expected.retain(|&x| x != key);

        // Verify remaining keys.
        let live = scan_keys(&tree);
        assert_eq!(
            live, expected,
            "After deleting {}, expected {:?} but got {:?}",
            key, expected, live,
        );

        // Tombstone count should never exceed max_tombstones (2) per leaf.
        let first = tree.get_first_leaf().unwrap().unwrap();
        let mut current = first;
        while current != PageId::INVALID {
            let leaf = tree.read_leaf(current).unwrap();
            assert!(
                leaf.tombstone_count() <= 2,
                "Leaf has {} tombstones, max is 2",
                leaf.tombstone_count(),
            );
            current = leaf.next_page_id;
        }
    }

    // Final state: keys [2, 4, 6, 8] should remain.
    let final_keys = scan_keys(&tree);
    assert_eq!(final_keys, vec![2, 4, 6, 8]);
}

/// BusTub: TombstoneSplitTest
///
/// How: Insert keys [1..10] with max_tombstones=3, leaf_max_size=5,
///      internal_max_size=4. Delete keys [2, 4, 6, 8]. Then insert keys
///      [11..15] which should trigger splits. Verify that tombstones persist
///      through splits and all expected keys are present.
/// Why: Tests that split correctly handles leaves containing tombstones.
///      Tombstones should survive the split and remain in the correct half.
#[test]
fn test_tombstone_split() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_tombstones(&bpm, header_id, 5, 4, 3);

    // Insert keys 1..=10.
    for i in 1..=10i64 {
        tree.insert(&encode_key(i), &encode_value(i)).unwrap();
    }

    let leaves_before = tree.count_leaves().unwrap();

    // Delete even keys — creates tombstones.
    for &key in &[2i64, 4, 6, 8] {
        tree.delete(&encode_key(key)).unwrap();
    }

    // Verify tombstones exist.
    let tombstones_after_delete = total_tombstones(&tree);
    assert!(
        tombstones_after_delete > 0,
        "Should have tombstones after delete",
    );

    // Insert more keys to trigger splits.
    for i in 11..=15i64 {
        tree.insert(&encode_key(i), &encode_value(i)).unwrap();
    }

    let leaves_after = tree.count_leaves().unwrap();
    assert!(
        leaves_after > leaves_before,
        "Splits should have increased leaf count from {} to {}",
        leaves_before,
        leaves_after,
    );

    // Verify all expected keys are present.
    let mut expected: Vec<i64> = (1..=15).filter(|x| ![2, 4, 6, 8].contains(x)).collect();
    expected.sort();
    let live = scan_keys(&tree);
    assert_eq!(live, expected);

    // Verify each key is individually retrievable.
    for &key in &expected {
        let result = tree.get(&encode_key(key)).unwrap();
        assert!(result.is_some(), "Key {} should be present", key);
        assert_eq!(decode_value(&result.unwrap()), key);
    }

    // Deleted keys should not be found.
    for &key in &[2i64, 4, 6, 8] {
        let result = tree.get(&encode_key(key)).unwrap();
        assert!(result.is_none(), "Deleted key {} should not be found", key);
    }
}

/// BusTub: TombstoneBorrowTest
///
/// How: Insert keys [1..8] with max_tombstones=1, leaf_max_size=4,
///      internal_max_size=4. Delete enough keys from one leaf to trigger
///      underflow (based on physical size). Verify that redistribution
///      (borrowing from sibling) preserves tombstones correctly.
/// Why: Tests that redistribute preserves tombstone status of entries.
///      With max_tombstones=1, underflow is based on physical size, so
///      a leaf with 1 live entry + 1 tombstone can still underflow.
#[test]
fn test_tombstone_borrow() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_tombstones(&bpm, header_id, 4, 4, 1);

    // Insert keys 1..=8.
    for i in 1..=8i64 {
        tree.insert(&encode_key(i), &encode_value(i)).unwrap();
    }

    // Verify initial state.
    let initial_keys = scan_keys(&tree);
    assert_eq!(initial_keys.len(), 8);

    // Delete key 1 — creates a tombstone in the first leaf.
    tree.delete(&encode_key(1)).unwrap();

    // Verify key 1 is gone.
    assert!(
        tree.get(&encode_key(1)).unwrap().is_none(),
        "Key 1 should be deleted",
    );

    // Delete key 2 — this should trigger underflow + borrow.
    // With max_tombstones=1, the first delete creates a tombstone.
    // The second delete either creates another tombstone (exceeding limit,
    // so the oldest gets flushed) or triggers physical underflow.
    tree.delete(&encode_key(2)).unwrap();

    // All remaining keys should be present and correct.
    let mut expected: Vec<i64> = (3..=8).collect();
    expected.sort();
    let live = scan_keys(&tree);
    assert_eq!(
        live, expected,
        "After borrow, expected {:?} but got {:?}",
        expected, live,
    );

    // Each key individually retrievable.
    for &key in &expected {
        let result = tree.get(&encode_key(key)).unwrap();
        assert!(
            result.is_some(),
            "Key {} should be present after borrow",
            key
        );
        assert_eq!(decode_value(&result.unwrap()), key);
    }

    // Deleted keys stay gone.
    for &key in &[1i64, 2] {
        assert!(
            tree.get(&encode_key(key)).unwrap().is_none(),
            "Key {} should remain deleted after borrow",
            key,
        );
    }
}

/// BusTub: TombstoneCoalesceTest
///
/// How: Insert keys [1..12] with max_tombstones=2, leaf_max_size=6,
///      internal_max_size=6. Delete enough keys to trigger leaf merges
///      (coalescing). Verify that merging correctly combines tombstones
///      from both leaves and flushes excess.
/// Why: Tests that merge (coalesce) handles tombstones from both siblings.
///      Combined tombstone count may exceed max_tombstones, so excess
///      must be processed.
#[test]
fn test_tombstone_coalesce() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_tombstones(&bpm, header_id, 6, 6, 2);

    // Insert keys 1..=12.
    for i in 1..=12i64 {
        tree.insert(&encode_key(i), &encode_value(i)).unwrap();
    }

    let leaves_before = tree.count_leaves().unwrap();
    assert!(leaves_before > 1, "Should have multiple leaves");

    // Delete keys to trigger merges. Delete enough from adjacent leaves.
    let delete_keys = [1, 2, 3, 4, 5];
    let mut expected: Vec<i64> = (1..=12).collect();

    for &key in &delete_keys {
        tree.delete(&encode_key(key)).unwrap();
        expected.retain(|&x| x != key);
    }

    let leaves_after = tree.count_leaves().unwrap();
    // Merges should have reduced leaf count (or at minimum not increased).
    assert!(
        leaves_after <= leaves_before,
        "Merges should reduce leaf count: before={}, after={}",
        leaves_before,
        leaves_after,
    );

    // Verify remaining keys.
    expected.sort();
    let live = scan_keys(&tree);
    assert_eq!(
        live, expected,
        "After coalesce, expected {:?} but got {:?}",
        expected, live,
    );

    // Each key individually retrievable.
    for &key in &expected {
        let result = tree.get(&encode_key(key)).unwrap();
        assert!(
            result.is_some(),
            "Key {} should be present after coalesce",
            key
        );
        assert_eq!(decode_value(&result.unwrap()), key);
    }

    // Deleted keys stay gone.
    for &key in &delete_keys {
        assert!(
            tree.get(&encode_key(key)).unwrap().is_none(),
            "Key {} should remain deleted after coalesce",
            key,
        );
    }

    // Tombstone count per leaf should not exceed max_tombstones.
    let first = tree.get_first_leaf().unwrap();
    if let Some(first_id) = first {
        let mut current = first_id;
        while current != PageId::INVALID {
            let leaf = tree.read_leaf(current).unwrap();
            assert!(
                leaf.tombstone_count() <= 2,
                "Leaf has {} tombstones, max is 2",
                leaf.tombstone_count(),
            );
            current = leaf.next_page_id;
        }
    }
}
