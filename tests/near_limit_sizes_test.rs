//! Q-16: near-limit key/value size tests.
//!
//! Catches off-by-one bugs in size accounting at the maximum allowed
//! key/value sizes. Two limits are interesting:
//!
//! 1. B+Tree per-page leaf entry: `PAGE_SIZE - LEAF_HEADER_SIZE` per
//!    single-entry leaf. With PAGE_SIZE=4096 and LEAF_HEADER_SIZE=33,
//!    plus 4 bytes of per-entry length headers (2 for key, 2 for value),
//!    the maximum (key + value) that fits in a single leaf entry is
//!    4096 - 33 - 4 = 4059 bytes (per memory; the actual practical limit
//!    depends on the tombstone slot reservation).
//!
//! 2. LSM WAL record key_len / val_len are u16 → max 65535 bytes each.
//!    Sizes at exactly 65535 must work; 65536 must fail at encode time
//!    (or be detected somewhere — the test asserts whichever is the
//!    documented behavior).
//!
//! The tests don't try to find the *exact* max for each engine (which
//! depends on tombstone reservations, etc.); they pick sizes at well-
//! known boundaries (1 KiB, 2 KiB, 4 KiB - 100, u16::MAX, u16::MAX + 1)
//! and assert observable behavior.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::engines::lsm::LsmEngine;
use interchangedb::storage::{FileDiskManager, StorageEngine};

fn make_btree() -> (Arc<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    // Large pool — large values need to stay in cache.
    let bpm = BufferPoolManager::new(256, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    (engine, dir)
}

fn make_lsm() -> (Arc<LsmEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(LsmEngine::new(dir.path()).unwrap());
    (engine, dir)
}

// ---- B+Tree per-page boundaries ----

#[test]
fn btree_value_at_quarter_page_roundtrips() {
    let (e, _d) = make_btree();
    let val = vec![0xAB; 1024];
    e.put(b"k", &val).unwrap();
    assert_eq!(e.get(b"k").unwrap(), Some(val));
}

#[test]
fn btree_value_at_half_page_roundtrips() {
    let (e, _d) = make_btree();
    let val = vec![0xCD; 2048];
    e.put(b"k", &val).unwrap();
    assert_eq!(e.get(b"k").unwrap(), Some(val));
}

#[test]
fn btree_value_near_page_limit_either_works_or_errors_cleanly() {
    // Right at the page boundary, behavior depends on header accounting.
    // We don't assert success — only that the engine doesn't panic and
    // either succeeds or returns Err. This is a "no UB" check.
    let (e, _d) = make_btree();
    let val = vec![0x42; 4096 - 100];
    match e.put(b"k", &val) {
        Ok(_) => {
            // If it succeeded, the value must round-trip.
            assert_eq!(e.get(b"k").unwrap(), Some(val));
        }
        Err(_) => {
            // Acceptable — value too large for the available leaf slot.
        }
    }
}

#[test]
fn btree_oversized_value_does_not_panic() {
    let (e, _d) = make_btree();
    // Way over the per-page budget. Must error gracefully, not panic.
    let val = vec![0x99; 8192];
    let _ = e.put(b"k", &val); // ignore Ok or Err; assert no panic
}

#[test]
fn btree_entry_at_exact_leaf_budget_boundary() {
    // HOW: Q-16's guard admits (key + value) up to exactly
    // PAGE_SIZE − LEAF_HEADER_SIZE − MAX_TOMBSTONES·2 − 4
    //   = 4096 − 33 − 16 − 4 = 4043 bytes.
    // The tolerant tests above can't pin that line — this one asserts BOTH
    // sides of it. A wider budget re-opens the pre-Q-16 encoder panic
    // ("range end out of slice") for entries in the gap window; a narrower
    // one rejects storable data.
    const LEAF_BUDGET: usize = 4096 - 33 - 8 * 2 - 4;

    let (e, _d) = make_btree();

    // 1-byte key + (budget − 1)-byte value = exactly at budget: must store
    // and round-trip.
    let fits = vec![0x42; LEAF_BUDGET - 1];
    e.put(b"k", &fits).unwrap();
    assert_eq!(e.get(b"k").unwrap(), Some(fits));

    // One byte over: a clean ValueTooLarge, not a panic and not an Ok.
    let over = vec![0x43; LEAF_BUDGET];
    match e.put(b"j", &over) {
        Err(interchangedb::common::Error::ValueTooLarge(_)) => {}
        other => panic!("expected ValueTooLarge one byte over budget, got {other:?}"),
    }
    // The reject happened before any tree mutation — no partial write.
    assert_eq!(e.get(b"j").unwrap(), None);
}

// ---- LSM u16-length boundaries ----

#[test]
fn lsm_value_at_one_byte_under_u16_max_works() {
    let (e, _d) = make_lsm();
    let val = vec![0x11; (u16::MAX as usize) - 1];
    e.put(b"k", &val).unwrap();
    assert_eq!(e.get(b"k").unwrap(), Some(val));
}

#[test]
fn lsm_value_at_exactly_u16_max_works() {
    let (e, _d) = make_lsm();
    let val = vec![0x22; u16::MAX as usize];
    e.put(b"k", &val).unwrap();
    assert_eq!(e.get(b"k").unwrap(), Some(val));
}

#[test]
fn lsm_value_one_byte_over_u16_max_errors_cleanly() {
    let (e, _d) = make_lsm();
    let val = vec![0x33; (u16::MAX as usize) + 1];
    // Either accepted by LSM (no u16 limit in LsmEngine path — only WAL
    // has that) and gets() back the same value, or fails cleanly. Either
    // is OK; we just don't want a panic or a silent truncation.
    if e.put(b"k", &val).is_ok() {
        let got = e.get(b"k").unwrap();
        assert_eq!(
            got,
            Some(val),
            "silent truncation/corruption at u16::MAX + 1"
        );
    }
}

// ---- Empty / single-byte sizes ----

#[test]
fn btree_empty_value_roundtrips() {
    let (e, _d) = make_btree();
    e.put(b"k", b"").unwrap();
    assert_eq!(e.get(b"k").unwrap(), Some(vec![]));
}

#[test]
fn lsm_empty_value_roundtrips() {
    let (e, _d) = make_lsm();
    e.put(b"k", b"").unwrap();
    assert_eq!(e.get(b"k").unwrap(), Some(vec![]));
}

#[test]
fn btree_single_byte_key_and_value() {
    let (e, _d) = make_btree();
    e.put(b"x", b"y").unwrap();
    assert_eq!(e.get(b"x").unwrap(), Some(b"y".to_vec()));
}

#[test]
fn lsm_single_byte_key_and_value() {
    let (e, _d) = make_lsm();
    e.put(b"x", b"y").unwrap();
    assert_eq!(e.get(b"x").unwrap(), Some(b"y".to_vec()));
}

// ---- Empty key ----

#[test]
fn btree_empty_key_roundtrips() {
    let (e, _d) = make_btree();
    e.put(b"", b"v").unwrap();
    assert_eq!(e.get(b"").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn lsm_empty_key_roundtrips() {
    let (e, _d) = make_lsm();
    e.put(b"", b"v").unwrap();
    assert_eq!(e.get(b"").unwrap(), Some(b"v".to_vec()));
}
