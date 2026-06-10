//! Q-09: cross-engine differential test. The interchange thesis says
//! `BTreeEngine` and `LsmEngine` are observably equivalent via the
//! `StorageEngine` trait: for any sequence of put/delete/get/scan, both
//! engines return identical results. This test runs random op sequences
//! against both engines in lockstep and asserts equivalence after each
//! observation.
//!
//! What it catches: divergences in semantic behavior — e.g. an engine
//! that silently drops a delete on a non-existent key, or scans that
//! return different ordering, or get-after-delete returning the
//! pre-delete value. The interchange thesis (Phase 16+) loses meaning
//! if engines diverge.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::index::lsm::LsmEngine;
use interchangedb::storage::{FileDiskManager, StorageEngine};
use proptest::prelude::*;
use tempfile::{tempdir, TempDir};

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
    /// Scan a range; we materialize the iterator into a `Vec` for
    /// comparison. Bounds modeled as `Option<key>`; `None` is unbounded.
    ScanRange(Option<Vec<u8>>, Option<Vec<u8>>),
}

fn op_strategy(pool: usize) -> impl Strategy<Value = Op> {
    let key = move || (0usize..pool).prop_map(|i| format!("k{:03}", i).into_bytes());
    let value = || prop::collection::vec(any::<u8>(), 0..32);
    let opt_key = move || proptest::option::of(key());
    prop_oneof![
        4 => (key(), value()).prop_map(|(k, v)| Op::Put(k, v)),
        2 => key().prop_map(Op::Delete),
        4 => key().prop_map(Op::Get),
        2 => (opt_key(), opt_key()).prop_map(|(s, e)| Op::ScanRange(s, e)),
    ]
}

fn make_btree() -> (Arc<BTreeEngine>, TempDir) {
    let dir = tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("btree.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    (engine, dir)
}

fn make_lsm() -> (Arc<LsmEngine>, TempDir) {
    let dir = tempdir().unwrap();
    let engine = Arc::new(LsmEngine::new(dir.path()).unwrap());
    (engine, dir)
}

/// A materialized key/value run, sorted by key.
type KvRun = Vec<(Vec<u8>, Vec<u8>)>;

/// Materialize a scan over `range` into a `KvRun`, sorted by key (which is
/// what `StorageEngine::scan` guarantees).
fn collect_scan<E: StorageEngine>(
    engine: &E,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
) -> Result<KvRun, interchangedb::Error> {
    engine.scan((start, end)).collect::<Result<Vec<_>, _>>()
}

fn bound_from_opt(opt: &Option<Vec<u8>>) -> Bound<Vec<u8>> {
    match opt {
        None => Bound::Unbounded,
        Some(k) => Bound::Included(k.clone()),
    }
}

proptest! {
    /// For any random sequence of mutations + observations against
    /// freshly-constructed BTree and LSM engines, every observation
    /// (get / scan) returns identical results.
    #[test]
    fn btree_and_lsm_observationally_equivalent(
        ops in prop::collection::vec(op_strategy(8), 1..40)
    ) {
        let (btree, _bdir) = make_btree();
        let (lsm, _ldir) = make_lsm();
        // A BTreeMap oracle catches divergences from BOTH engines at once
        // (e.g. a corruption that flips a value in both). Not strictly
        // required for the differential property, but cheap insurance.
        let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for (i, op) in ops.iter().enumerate() {
            match op {
                Op::Put(k, v) => {
                    btree.put(k, v).unwrap();
                    lsm.put(k, v).unwrap();
                    oracle.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    btree.delete(k).unwrap();
                    lsm.delete(k).unwrap();
                    oracle.remove(k);
                }
                Op::Get(k) => {
                    let b = btree.get(k).unwrap();
                    let l = lsm.get(k).unwrap();
                    let o = oracle.get(k).cloned();
                    prop_assert_eq!(
                        b.clone(), l.clone(),
                        "engines diverged on get({:?}) at op #{}: btree={:?} lsm={:?}",
                        k, i, b, l
                    );
                    prop_assert_eq!(
                        b.as_ref(), o.as_ref(),
                        "engines agree but oracle disagrees on get({:?}) at op #{}",
                        k, i
                    );
                }
                Op::ScanRange(start, end) => {
                    // Skip ranges where start > end — undefined behavior at
                    // the API level and not what this test is about.
                    if let (Some(s), Some(e)) = (start, end) {
                        if s > e {
                            continue;
                        }
                    }
                    let sb = bound_from_opt(start);
                    let eb = bound_from_opt(end);
                    let bs = collect_scan(&*btree, sb.clone(), eb.clone()).unwrap();
                    let ls = collect_scan(&*lsm, sb.clone(), eb.clone()).unwrap();
                    prop_assert_eq!(
                        &bs, &ls,
                        "scans diverged at op #{}: btree={:?} lsm={:?}",
                        i, bs, ls
                    );
                    let oracle_range: Vec<(Vec<u8>, Vec<u8>)> = oracle
                        .range((sb, eb))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    prop_assert_eq!(&bs, &oracle_range,
                        "engines agree but oracle disagrees on scan at op #{}", i);
                }
            }
        }
    }
}

/// Smoke test: at least one concrete cross-engine equivalence case runs
/// outside of proptest, so the integration is exercised even if a
/// proptest shrink hides issues.
#[test]
fn smoke_get_after_delete_returns_none_in_both() {
    let (btree, _b) = make_btree();
    let (lsm, _l) = make_lsm();
    btree.put(b"a", b"1").unwrap();
    lsm.put(b"a", b"1").unwrap();
    btree.delete(b"a").unwrap();
    lsm.delete(b"a").unwrap();
    assert_eq!(btree.get(b"a").unwrap(), None);
    assert_eq!(lsm.get(b"a").unwrap(), None);
}

#[test]
fn smoke_scan_orders_keys_identically() {
    let (btree, _b) = make_btree();
    let (lsm, _l) = make_lsm();
    let pairs: &[(&[u8], &[u8])] = &[(b"c", b"3"), (b"a", b"1"), (b"b", b"2")];
    for (k, v) in pairs {
        btree.put(k, v).unwrap();
        lsm.put(k, v).unwrap();
    }
    let bs: Vec<_> = btree.scan(..).collect::<Result<Vec<_>, _>>().unwrap();
    let ls: Vec<_> = lsm.scan(..).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(bs, ls);
    // Confirm sorted-by-key.
    assert_eq!(
        bs,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );
}
