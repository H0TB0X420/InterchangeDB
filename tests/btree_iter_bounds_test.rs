//! Q-05 regression tests: `BTreeScanIterator` must reject corrupt leaf
//! pages instead of following bogus `next_page_id` pointers off the end of
//! the buffer pool or yielding mismatched key/value arrays.
//!
//! The bug was a silent follow: a corrupt page with `next_page_id =
//! u32::MAX` would cause the iterator to request a non-existent page, and
//! whatever the BPM did (panic, return junk, depending on the path) would
//! be the user-visible failure mode. After the fix, the iterator
//! validates the decoded leaf and the sibling pointer, returning
//! `Error::StorageCorrupted` with a precise diagnostic.

use std::ops::Bound;
use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::{Error, PageId};
use interchangedb::engines::btree::{encode_leaf_node, BTreeScanIterator, LeafNode};
use interchangedb::storage::FileDiskManager;
use tempfile::TempDir;

/// Build a fresh BPM with a small pool and an empty disk file.
fn fresh_bpm() -> (Arc<BufferPoolManager>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&path).unwrap();
    let bpm = Arc::new(BufferPoolManager::new(16, dm));
    (bpm, dir)
}

/// Allocate a new page, encode `leaf` into it, drop the write guard.
/// Returns the page id.
fn write_leaf(bpm: &BufferPoolManager, leaf: &LeafNode) -> PageId {
    let mut guard = bpm.new_page().unwrap();
    let pid = guard.page_id();
    encode_leaf_node(leaf, guard.as_mut_slice());
    drop(guard);
    pid
}

#[test]
fn out_of_range_next_page_id_returns_storage_corrupted() {
    let (bpm, _dir) = fresh_bpm();

    // Build a valid one-entry leaf, then set its sibling pointer to a
    // page id beyond the BPM's disk_page_count (which is small here).
    let mut leaf = LeafNode::new(10);
    leaf.keys.push(b"k1".to_vec());
    leaf.values.push(b"v1".to_vec());
    leaf.next_page_id = PageId(999_999); // far beyond any allocated page

    let pid = write_leaf(&bpm, &leaf);

    let mut iter = BTreeScanIterator::new(&bpm, pid, Bound::Unbounded, Bound::Unbounded);

    // The validation fires inside `load_next_page` before any entries are
    // yielded — if `next_page_id` is corrupt, the rest of the leaf may be
    // too, so the iterator bails before serving anything from this page.
    match iter.next().expect("expected an error item") {
        Err(Error::StorageCorrupted(msg)) => {
            assert!(
                msg.contains("next_page_id=999999"),
                "diagnostic missing offending value: {}",
                msg
            );
        }
        other => panic!("expected StorageCorrupted, got: {:?}", other),
    }
}

#[test]
fn valid_invalid_terminator_iterates_cleanly() {
    // Regression guard: the bounds check must not reject `PageId::INVALID`
    // (the normal terminator for the leaf chain). A single-leaf tree with
    // next_page_id == INVALID is the most common scan shape.
    let (bpm, _dir) = fresh_bpm();

    let mut leaf = LeafNode::new(10);
    leaf.keys.push(b"a".to_vec());
    leaf.values.push(b"1".to_vec());
    leaf.keys.push(b"b".to_vec());
    leaf.values.push(b"2".to_vec());
    leaf.next_page_id = PageId::INVALID;

    let pid = write_leaf(&bpm, &leaf);

    let mut iter = BTreeScanIterator::new(&bpm, pid, Bound::Unbounded, Bound::Unbounded);
    let collected: Vec<_> = (&mut iter).collect::<Result<_, _>>().unwrap();
    assert_eq!(
        collected,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ]
    );
    assert!(iter.next().is_none());
}

#[test]
fn next_page_id_at_disk_page_count_is_out_of_range() {
    // Edge case: next_page_id exactly equal to disk_page_count is invalid
    // (page ids are 0..disk_page_count, exclusive). The check uses >=, not >.
    let (bpm, _dir) = fresh_bpm();

    let mut leaf = LeafNode::new(10);
    leaf.keys.push(b"k".to_vec());
    leaf.values.push(b"v".to_vec());

    let pid = write_leaf(&bpm, &leaf);
    let boundary = PageId(bpm.disk_page_count()); // == count, NOT count - 1

    // Patch next_page_id in place.
    let mut guard = bpm.fetch_page_write(pid).unwrap();
    let mut patched = leaf.clone();
    patched.next_page_id = boundary;
    encode_leaf_node(&patched, guard.as_mut_slice());
    drop(guard);

    let mut iter = BTreeScanIterator::new(&bpm, pid, Bound::Unbounded, Bound::Unbounded);
    match iter.next() {
        Some(Err(Error::StorageCorrupted(msg))) => {
            assert!(msg.contains("out of range"), "got: {}", msg);
        }
        other => panic!("expected StorageCorrupted, got: {:?}", other),
    }
}
