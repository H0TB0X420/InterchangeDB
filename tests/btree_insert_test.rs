//! B+Tree insert tests.
//!
//! Ported from BusTub's b_plus_tree_insert_test.cpp.
//! Tests use small node sizes (leaf_max_size=2-4, internal_max_size=3)
//! to force splits with very few keys.

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

// =============================================================================
// Tests
// =============================================================================

/// BusTub: BasicInsertTest
///
/// How: Insert a single key (42) into a tree with leaf_max_size=2,
///      internal_max_size=3. Read the root page and verify it is a leaf
///      with exactly 1 entry containing the correct key.
/// Why: Tests the simplest insert case — single key into empty tree, no split.
#[test]
fn test_basic_insert() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 2, 3);

    let key: i64 = 42;
    let k = encode_key(key);
    let v = encode_value(key);
    tree.insert(&k, &v).unwrap();

    // Verify tree has exactly 1 leaf with 1 entry.
    assert_eq!(tree.count_leaves().unwrap(), 1);

    let first_leaf_id = tree.get_first_leaf().unwrap().unwrap();
    let leaf = tree.read_leaf(first_leaf_id).unwrap();
    assert_eq!(leaf.physical_size(), 1);
    assert_eq!(decode_key(leaf.key_at(0)), 42);

    // Also verify via get.
    let result = tree.get(&k).unwrap();
    assert!(result.is_some());
    assert_eq!(decode_value(&result.unwrap()), 42);
}

/// BusTub: OptimisticInsertTest (adapted)
///
/// How: Insert 25 keys (0, 2, 4, ..., 48) with leaf_max_size=4,
///      internal_max_size=3. Walk leaves to find one with room for an
///      insert without triggering a split. Insert a key into that leaf.
///      Verify the key is present and all others remain.
/// Why: Tests the optimistic insert path where no split is needed.
///
/// Note: BusTub's original test checks I/O counts (reads/writes).
/// We skip I/O counting since our optimistic path is deferred to 3.2.6.
#[test]
fn test_optimistic_insert() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 4, 3);

    let num_keys: i64 = 25;
    for i in 0..num_keys {
        let k = encode_key(2 * i);
        let v = encode_value(i);
        tree.insert(&k, &v).unwrap();
    }

    // Walk leaves to find one with room (physical_size + 1 < max_size).
    let mut to_insert: Option<i64> = None;
    let first_leaf = tree.get_first_leaf().unwrap().unwrap();
    let mut current = first_leaf;
    while current != PageId::INVALID {
        let leaf = tree.read_leaf(current).unwrap();
        if (leaf.physical_size() as u16 + 1) < leaf.header.max_size {
            // Pick the first key of this leaf + 1 (odd number, not yet inserted).
            to_insert = Some(decode_key(leaf.key_at(0)) + 1);
        }
        current = leaf.next_page_id;
    }

    let to_insert = to_insert.expect("Should find a leaf with room for insert");

    // Insert the key — no split needed.
    let k = encode_key(to_insert);
    let v = encode_value(to_insert);
    tree.insert(&k, &v).unwrap();

    // Verify inserted key is present.
    let result = tree.get(&k).unwrap();
    assert!(
        result.is_some(),
        "Inserted key {} should be present",
        to_insert
    );

    // Verify all original keys still present.
    for i in 0..num_keys {
        let k = encode_key(2 * i);
        assert!(
            tree.get(&k).unwrap().is_some(),
            "Original key {} should still exist",
            2 * i,
        );
    }
}

/// BusTub: InsertTest1NoIterator
///
/// How: Insert keys [1, 2, 3, 4, 5] with leaf_max_size=2,
///      internal_max_size=3. Verify each key is retrievable with the
///      correct value.
/// Why: Tests insert with splits using tiny node sizes, verified by
///      point lookups (no iterator).
#[test]
fn test_insert_no_iterator() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 2, 3);

    let keys: Vec<i64> = vec![1, 2, 3, 4, 5];
    for &key in &keys {
        let k = encode_key(key);
        let v = encode_value(key);
        tree.insert(&k, &v).unwrap();
    }

    // Verify each key is retrievable with correct value.
    for &key in &keys {
        let k = encode_key(key);
        let result = tree.get(&k).unwrap();
        assert!(result.is_some(), "Key {} not found", key);
        assert_eq!(decode_value(&result.unwrap()), key);
    }
}

/// BusTub: InsertTest2
///
/// How: Insert keys [5, 4, 3, 2, 1] (reverse order) with leaf_max_size=2,
///      internal_max_size=3. Verify each key retrievable. Then scan all
///      keys verifying sorted ascending order. Then scan from key 3
///      verifying keys [3, 4, 5] in order.
/// Why: Tests insert in reverse order (triggers different split patterns),
///      full iteration, and partial iteration from a given start key.
#[test]
fn test_insert_with_scan() {
    let (bpm, _dir) = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 2, 3);

    let keys: Vec<i64> = vec![5, 4, 3, 2, 1];
    for &key in &keys {
        let k = encode_key(key);
        let v = encode_value(key);
        tree.insert(&k, &v).unwrap();
    }

    // Verify each key retrievable with correct value.
    for &key in &keys {
        let k = encode_key(key);
        let result = tree.get(&k).unwrap();
        assert!(result.is_some(), "Key {} not found", key);
        assert_eq!(decode_value(&result.unwrap()), key);
    }

    // Full scan: verify sorted ascending order 1, 2, 3, 4, 5.
    let all = tree.scan_all().unwrap();
    assert_eq!(all.len(), 5);
    for (i, (key_bytes, val_bytes)) in all.iter().enumerate() {
        let expected = (i + 1) as i64;
        assert_eq!(decode_key(key_bytes), expected);
        assert_eq!(decode_value(val_bytes), expected);
    }

    // Scan from key 3: verify keys [3, 4, 5] in order.
    let from_3 = tree.scan_from(&encode_key(3)).unwrap();
    assert_eq!(from_3.len(), 3);
    for (i, (key_bytes, val_bytes)) in from_3.iter().enumerate() {
        let expected = (i + 3) as i64;
        assert_eq!(decode_key(key_bytes), expected);
        assert_eq!(decode_value(val_bytes), expected);
    }
}
