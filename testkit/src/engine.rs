//! `StorageEngine` axis: the engine constructors (the registry's leaves) and
//! [`assert_contract`] — the key-value contract both engines must satisfy
//! identically. Driven from [`crate::for_each_engine`].
//!
//! Engines are returned as concrete types inside [`Built`] (LSM keeps its temp
//! dir alive). The contract is generic over `E: StorageEngine`, instantiated
//! per engine by the registry — so it can use the ergonomic `scan` (which is
//! `Self: Sized`, not on `dyn`).

use interchangedb::buffer::BufferPoolManager;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::index::lsm::LsmEngine;
use interchangedb::storage::{MemoryDiskManager, StorageEngine};

use crate::handles::Built;

/// Buffer-pool size for the in-memory B-tree config — comfortably larger than
/// any conformance workload so eviction doesn't enter the picture here.
pub const POOL: usize = 256;

pub fn btree() -> Built<BTreeEngine> {
    let bpm = BufferPoolManager::new(POOL, MemoryDiskManager::new());
    Built::new(BTreeEngine::new(bpm).unwrap())
}

pub fn lsm() -> Built<LsmEngine> {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmEngine::new(dir.path()).unwrap();
    Built::with_dir(engine, dir)
}

/// Assert the universal `StorageEngine` contract on a fresh engine.
pub fn assert_contract<E: StorageEngine>(name: &str, e: &E) {
    // put / get round-trip.
    e.put(b"k1", b"v1").unwrap();
    assert_eq!(
        e.get(b"k1").unwrap(),
        Some(b"v1".to_vec()),
        "{name}: get must return the value just put"
    );

    // Overwrite — last write wins.
    e.put(b"k1", b"v2").unwrap();
    assert_eq!(
        e.get(b"k1").unwrap(),
        Some(b"v2".to_vec()),
        "{name}: overwrite must win"
    );

    // Missing key.
    assert_eq!(
        e.get(b"absent").unwrap(),
        None,
        "{name}: a missing key must be None"
    );

    // delete hides the key.
    e.put(b"k2", b"v2").unwrap();
    e.delete(b"k2").unwrap();
    assert_eq!(
        e.get(b"k2").unwrap(),
        None,
        "{name}: get after delete must be None"
    );

    // scan returns live keys in sorted order.
    for i in 0..20u32 {
        e.put(format!("s{i:02}").as_bytes(), b"x").unwrap();
    }
    let scanned: Vec<Vec<u8>> = e.scan(..).map(|r| r.unwrap().0).collect();
    let mut sorted = scanned.clone();
    sorted.sort();
    assert_eq!(
        scanned, sorted,
        "{name}: scan must yield keys in sorted order"
    );
    assert!(
        !scanned.iter().any(|k| k == b"k2"),
        "{name}: a deleted key must not appear in a scan"
    );

    // Range scan respects [start, end) bounds.
    let range: Vec<Vec<u8>> = e
        .scan(b"s05".to_vec()..b"s10".to_vec())
        .map(|r| r.unwrap().0)
        .collect();
    let expected: Vec<Vec<u8>> = (5..10).map(|i| format!("s{i:02}").into_bytes()).collect();
    assert_eq!(
        range, expected,
        "{name}: range scan must honor [start, end)"
    );
}
