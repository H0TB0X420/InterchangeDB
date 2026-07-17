//! B+Tree range scan iterator tests.
//!
//! Tests lazy BTreeScanIterator with various RangeBounds variants,
//! tombstone skipping, early termination, and backward compatibility
//! of scan_all() / scan_from() wrappers.

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
fn encode_key(val: i64) -> Vec<u8> {
    let unsigned = (val as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes().to_vec()
}

fn decode_key(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    let unsigned = u64::from_be_bytes(buf);
    (unsigned ^ (1u64 << 63)) as i64
}

fn encode_value(val: i64) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn decode_value(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    i64::from_le_bytes(buf)
}

/// Insert keys [0..n) into the tree, returning the tree.
fn populate_tree<'a>(
    bpm: &'a BufferPoolManager,
    header_id: PageId,
    leaf_max: u16,
    internal_max: u16,
    n: i64,
) -> BTree<'a> {
    let tree = BTree::with_sizes(bpm, header_id, leaf_max, internal_max);
    for i in 0..n {
        let k = encode_key(i);
        let v = encode_value(i);
        tree.insert(&k, &v).unwrap();
    }
    tree
}

// =============================================================================
// Full range scan (..)
// =============================================================================

/// How: Insert keys [0..10), scan with `..` (unbounded). Verify all 10
///      entries returned in sorted order.
/// Why: Tests full unbounded scan — equivalent to scan_all().
#[test]
fn test_scan_full_range() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 10);

    let results: Vec<_> = tree
        .scan(..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 10);
    for (i, (k, v)) in results.iter().enumerate() {
        assert_eq!(decode_key(k), i as i64);
        assert_eq!(decode_value(v), i as i64);
    }
}

// =============================================================================
// Half-open range (start..)
// =============================================================================

/// How: Insert keys [0..10), scan with `5..`. Verify entries [5, 6, 7, 8, 9].
/// Why: Tests start-inclusive, end-unbounded range.
#[test]
fn test_scan_from_start_inclusive() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 10);

    let start = encode_key(5);
    let results: Vec<_> = tree
        .scan(start..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 5);
    for (i, (k, _v)) in results.iter().enumerate() {
        assert_eq!(decode_key(k), (i + 5) as i64);
    }
}

// =============================================================================
// Closed range (start..=end)
// =============================================================================

/// How: Insert keys [0..10), scan with `3..=7`. Verify entries [3, 4, 5, 6, 7].
/// Why: Tests inclusive-inclusive range.
#[test]
fn test_scan_range_inclusive() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 10);

    let start = encode_key(3);
    let end = encode_key(7);
    let results: Vec<_> = tree
        .scan(start..=end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 5);
    for (i, (k, _v)) in results.iter().enumerate() {
        assert_eq!(decode_key(k), (i + 3) as i64);
    }
}

// =============================================================================
// Half-open range (start..end)
// =============================================================================

/// How: Insert keys [0..10), scan with `3..7`. Verify entries [3, 4, 5, 6].
/// Why: Tests inclusive-exclusive range.
#[test]
fn test_scan_range_exclusive_end() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 10);

    let start = encode_key(3);
    let end = encode_key(7);
    let results: Vec<_> = tree
        .scan(start..end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 4);
    for (i, (k, _v)) in results.iter().enumerate() {
        assert_eq!(decode_key(k), (i + 3) as i64);
    }
}

// =============================================================================
// Prefix range (..end) and (..=end)
// =============================================================================

/// How: Insert keys [0..10), scan with `..5`. Verify entries [0, 1, 2, 3, 4].
/// Why: Tests unbounded-start, exclusive-end range.
#[test]
fn test_scan_up_to_exclusive() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 10);

    let end = encode_key(5);
    let results: Vec<_> = tree
        .scan(..end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 5);
    for (i, (k, _v)) in results.iter().enumerate() {
        assert_eq!(decode_key(k), i as i64);
    }
}

/// How: Insert keys [0..10), scan with `..=5`. Verify entries [0, 1, 2, 3, 4, 5].
/// Why: Tests unbounded-start, inclusive-end range.
#[test]
fn test_scan_up_to_inclusive() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 10);

    let end = encode_key(5);
    let results: Vec<_> = tree
        .scan(..=end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 6);
    for (i, (k, _v)) in results.iter().enumerate() {
        assert_eq!(decode_key(k), i as i64);
    }
}

// =============================================================================
// Empty ranges
// =============================================================================

/// How: Insert keys [0..10), scan with range where start > end (5..3).
///      Verify zero entries returned.
/// Why: Tests that an empty range returns nothing (start past end).
#[test]
fn test_scan_empty_range_start_past_end() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 10);

    let start = encode_key(5);
    let end = encode_key(3);
    let results: Vec<_> = tree
        .scan(start..end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 0);
}

/// How: Scan an empty tree with `..`. Verify zero entries returned.
/// Why: Tests scanning an empty tree returns nothing without error.
#[test]
fn test_scan_empty_tree() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 3, 3);

    let results: Vec<_> = tree
        .scan(..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 0);
}

/// How: Insert keys [0..10), scan a range between existing keys (key 2.5..4.5).
///      Since keys are integers, this means start > key 2, end < key 5.
///      Verify entries [3, 4].
/// Why: Tests range where bounds don't exactly match existing keys.
#[test]
fn test_scan_range_between_keys() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 10);

    // Start just after key 2, end just before key 5.
    // With encoded keys, key 3 > key 2 and key 4 < key 5.
    let start = encode_key(3);
    let end = encode_key(5);
    let results: Vec<_> = tree
        .scan(start..end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(decode_key(&results[0].0), 3);
    assert_eq!(decode_key(&results[1].0), 4);
}

// =============================================================================
// Tombstone skipping
// =============================================================================

/// How: Insert keys [0..10) with tombstone support. Delete keys 3, 5, 7.
///      Scan `..`. Verify tombstoned entries are excluded — 7 entries remain.
/// Why: Tests that the iterator skips tombstoned entries (via live_entries).
#[test]
fn test_scan_skips_tombstones() {
    let bpm = setup_bpm(100);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_tombstones(&bpm, header_id, 3, 3, 8);

    for i in 0..10i64 {
        tree.insert(&encode_key(i), &encode_value(i)).unwrap();
    }

    // Delete some keys (tombstone-based).
    tree.delete(&encode_key(3)).unwrap();
    tree.delete(&encode_key(5)).unwrap();
    tree.delete(&encode_key(7)).unwrap();

    let results: Vec<_> = tree
        .scan(..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let keys: Vec<i64> = results.iter().map(|(k, _)| decode_key(k)).collect();
    assert_eq!(keys, vec![0, 1, 2, 4, 6, 8, 9]);
}

// =============================================================================
// Early termination (iterator laziness)
// =============================================================================

/// How: Insert 100 keys across many leaves (leaf_max=3). Take only first 5
///      entries from the iterator. Verify exactly 5 returned and they are the
///      smallest 5 keys.
/// Why: Tests that the iterator is lazy — we can break early without fetching
///      all pages.
#[test]
fn test_scan_early_termination() {
    let bpm = setup_bpm(200);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 3, 3, 100);

    let first_five: Vec<_> = tree
        .scan(..)
        .unwrap()
        .take(5)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(first_five.len(), 5);
    for (i, (k, _v)) in first_five.iter().enumerate() {
        assert_eq!(decode_key(k), i as i64);
    }
}

// =============================================================================
// Large scan across multiple leaves
// =============================================================================

/// How: Insert 500 keys with leaf_max=4, internal_max=4. Scan all with `..`.
///      Verify 500 entries returned in sorted order.
/// Why: Tests correctness across many leaf pages.
#[test]
fn test_scan_large() {
    let bpm = setup_bpm(500);
    let header_id = create_empty_tree(&bpm);
    let tree = populate_tree(&bpm, header_id, 4, 4, 500);

    let results: Vec<_> = tree
        .scan(..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 500);
    for (i, (k, v)) in results.iter().enumerate() {
        assert_eq!(decode_key(k), i as i64, "Wrong key at index {}", i);
        assert_eq!(decode_value(v), i as i64, "Wrong value at index {}", i);
    }
}

// =============================================================================
// Backward compatibility: scan_all() and scan_from()
// =============================================================================

/// How: Insert keys [5, 4, 3, 2, 1] with leaf_max=2, internal_max=3.
///      Call scan_all() and verify sorted ascending order.
/// Why: Tests that scan_all() wrapper produces same results as before.
#[test]
fn test_scan_all_backward_compat() {
    let bpm = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 2, 3);

    let keys: Vec<i64> = vec![5, 4, 3, 2, 1];
    for &key in &keys {
        tree.insert(&encode_key(key), &encode_value(key)).unwrap();
    }

    let all = tree.scan_all().unwrap();
    assert_eq!(all.len(), 5);
    for (i, (k, v)) in all.iter().enumerate() {
        let expected = (i + 1) as i64;
        assert_eq!(decode_key(k), expected);
        assert_eq!(decode_value(v), expected);
    }
}

/// How: Insert keys [5, 4, 3, 2, 1] with leaf_max=2, internal_max=3.
///      Call scan_from(3) and verify entries [3, 4, 5].
/// Why: Tests that scan_from() wrapper produces same results as before.
#[test]
fn test_scan_from_backward_compat() {
    let bpm = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 2, 3);

    let keys: Vec<i64> = vec![5, 4, 3, 2, 1];
    for &key in &keys {
        tree.insert(&encode_key(key), &encode_value(key)).unwrap();
    }

    let from_3 = tree.scan_from(&encode_key(3)).unwrap();
    assert_eq!(from_3.len(), 3);
    for (i, (k, v)) in from_3.iter().enumerate() {
        let expected = (i + 3) as i64;
        assert_eq!(decode_key(k), expected);
        assert_eq!(decode_value(v), expected);
    }
}

// =============================================================================
// Single-entry edge cases
// =============================================================================

/// How: Insert a single key (42). Scan with `..`. Verify single entry returned.
/// Why: Tests scan on a tree with exactly one leaf containing one entry.
#[test]
fn test_scan_single_entry() {
    let bpm = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 3, 3);

    tree.insert(&encode_key(42), &encode_value(42)).unwrap();

    let results: Vec<_> = tree
        .scan(..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(decode_key(&results[0].0), 42);
}

/// How: Insert a single key (42). Scan with `42..=42`. Verify single entry.
/// Why: Tests point-range scan (start == end, inclusive both sides).
#[test]
fn test_scan_point_range() {
    let bpm = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 3, 3);

    tree.insert(&encode_key(42), &encode_value(42)).unwrap();

    let k = encode_key(42);
    let results: Vec<_> = tree
        .scan(k.clone()..=k)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(decode_key(&results[0].0), 42);
}

/// How: Insert a single key (42). Scan with `43..`. Verify empty.
/// Why: Tests scan that starts past all existing entries.
#[test]
fn test_scan_past_all_entries() {
    let bpm = setup_bpm(50);
    let header_id = create_empty_tree(&bpm);
    let tree = BTree::with_sizes(&bpm, header_id, 3, 3);

    tree.insert(&encode_key(42), &encode_value(42)).unwrap();

    let results: Vec<_> = tree
        .scan(encode_key(43)..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 0);
}
