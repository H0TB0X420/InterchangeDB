//! P12.1: per-index engine map on `Catalog<E>`.
//!
//! Verifies the in-memory plumbing for Phase 12's per-index storage
//! backend choice. Subsequent phases (P12.2 persistence, P12.3 factory,
//! P12.4 index maintenance, P12.5 IndexScan) will build on this.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, IndexId};
use interchangedb::index::btree::BTreeEngine;
use interchangedb::index::lsm::LsmEngine;
use interchangedb::storage::{FileDiskManager, StorageEngine};

fn fresh_btree_catalog() -> (Arc<Catalog<BTreeEngine>>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("cat.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let cat = Arc::new(Catalog::open(engine).unwrap());
    (cat, dir)
}

fn fresh_btree_engine() -> Arc<BTreeEngine> {
    // Each index engine needs its own backing store; use a fresh tempfile.
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let dm = FileDiskManager::create(dir.path().join("idx.db")).unwrap();
    let bpm = BufferPoolManager::new(32, dm);
    Arc::new(BTreeEngine::new(bpm).unwrap())
}

fn fresh_lsm_engine() -> Arc<LsmEngine> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    Arc::new(LsmEngine::new(dir.path()).unwrap())
}

#[test]
fn unregistered_index_returns_none() {
    let (cat, _d) = fresh_btree_catalog();
    assert!(cat.index_engine(IndexId(42)).is_none());
}

#[test]
fn register_then_lookup_returns_same_handle() {
    let (cat, _d) = fresh_btree_catalog();
    let idx_engine = fresh_btree_engine();

    cat.register_index_engine(IndexId(1), idx_engine.clone());

    let looked_up = cat.index_engine(IndexId(1)).expect("just registered");
    // Round-trip a key through the looked-up `Arc<dyn StorageEngine>` and
    // observe it via the original concrete handle — proves they alias.
    looked_up.put(b"k", b"v").unwrap();
    assert_eq!(idx_engine.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn two_indexes_can_use_different_backends() {
    // The whole point of per-index storage: a BTree index and an LSM
    // index can live side by side under the same catalog.
    let (cat, _d) = fresh_btree_catalog();
    let btree_idx = fresh_btree_engine();
    let lsm_idx = fresh_lsm_engine();

    cat.register_index_engine(IndexId(1), btree_idx.clone());
    cat.register_index_engine(IndexId(2), lsm_idx.clone());

    let e1 = cat.index_engine(IndexId(1)).unwrap();
    let e2 = cat.index_engine(IndexId(2)).unwrap();
    assert_eq!(e1.name(), "btree");
    assert_eq!(e2.name(), "lsm");

    // Writes routed independently.
    e1.put(b"k", b"from_btree").unwrap();
    e2.put(b"k", b"from_lsm").unwrap();
    assert_eq!(btree_idx.get(b"k").unwrap(), Some(b"from_btree".to_vec()));
    assert_eq!(lsm_idx.get(b"k").unwrap(), Some(b"from_lsm".to_vec()));
}

#[test]
fn re_register_overwrites_previous_handle() {
    // Idempotency: the map is keyed by IndexId. Registering twice with
    // the same id replaces. Useful for P12.3's reopen path.
    let (cat, _d) = fresh_btree_catalog();
    let first = fresh_btree_engine();
    let second = fresh_btree_engine();
    cat.register_index_engine(IndexId(7), first.clone());
    cat.register_index_engine(IndexId(7), second.clone());

    let looked_up = cat.index_engine(IndexId(7)).unwrap();
    looked_up.put(b"k", b"v").unwrap();
    assert_eq!(second.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(first.get(b"k").unwrap(), None, "first handle should be detached");
}

#[test]
fn dyn_scan_range_works_through_handle() {
    // The trait's `scan` method is `where Self: Sized` and unavailable
    // through `dyn`. `scan_range` is the dyn-compatible primitive that
    // the IndexScan operator (P12.5) will use.
    let (cat, _d) = fresh_btree_catalog();
    let idx = fresh_btree_engine();
    cat.register_index_engine(IndexId(1), idx);

    let handle = cat.index_engine(IndexId(1)).unwrap();
    handle.put(b"a", b"1").unwrap();
    handle.put(b"b", b"2").unwrap();
    handle.put(b"c", b"3").unwrap();

    let collected: Vec<_> = handle
        .scan_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        collected,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );
}
