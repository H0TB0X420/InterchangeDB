//! Property-based tests using proptest.
//!
//! Generates thousands of random inputs to verify invariants hold for all
//! possible values, not just hand-picked examples. Finds edge cases that
//! manual tests miss: empty keys, max-length keys, special byte patterns,
//! timestamp boundaries, etc.

use proptest::prelude::*;
use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::FileDiskManager;
use interchangedb::txn::mvcc::{
    decode_mvcc_key, decode_mvcc_value, encode_mvcc_key, encode_mvcc_value, MvccValue,
};
use interchangedb::txn::{Timestamp, TxnId, TxnMode};

// ---------------------------------------------------------------------------
// MVCC Key Encoding Properties
// ---------------------------------------------------------------------------

proptest! {
    /// Encode then decode any key+timestamp produces the original values.
    #[test]
    fn mvcc_key_roundtrip(
        key in prop::collection::vec(any::<u8>(), 0..256),
        ts in 0u64..=(u64::MAX - 1), // Avoid u64::MAX which is INVALID
    ) {
        let encoded = encode_mvcc_key(&key, Timestamp(ts));
        let (decoded_key, decoded_ts) = decode_mvcc_key(&encoded).unwrap();
        prop_assert_eq!(&decoded_key, &key);
        prop_assert_eq!(decoded_ts.0, ts);
    }

    /// For the same user key, higher timestamp encodes to lexicographically
    /// SMALLER bytes (newest version sorts first).
    #[test]
    fn mvcc_key_ordering_same_key(
        key in prop::collection::vec(any::<u8>(), 1..64),
        ts_low in 0u64..1_000_000,
        ts_delta in 1u64..1_000_000,
    ) {
        let ts_high = ts_low + ts_delta;
        let encoded_high = encode_mvcc_key(&key, Timestamp(ts_high));
        let encoded_low = encode_mvcc_key(&key, Timestamp(ts_low));
        // Higher timestamp → smaller encoded bytes (inverted).
        prop_assert!(encoded_high < encoded_low,
            "Higher ts should encode smaller: ts_high={}, ts_low={}", ts_high, ts_low);
    }

    /// Different user keys of the SAME length sort by key content, not timestamp.
    #[test]
    fn mvcc_key_ordering_different_keys_same_length(
        key_a in prop::collection::vec(any::<u8>(), 4..4+1), // fixed length 4
        key_b in prop::collection::vec(any::<u8>(), 4..4+1),
        ts in 0u64..1_000_000,
    ) {
        prop_assume!(key_a != key_b);
        let encoded_a = encode_mvcc_key(&key_a, Timestamp(ts));
        let encoded_b = encode_mvcc_key(&key_b, Timestamp(ts));
        // With same-length keys and same timestamp, encoded order matches key order.
        if key_a < key_b {
            prop_assert!(encoded_a < encoded_b);
        } else {
            prop_assert!(encoded_a > encoded_b);
        }
    }

    /// Encoded key length is always exactly 4 + key.len() + 8.
    #[test]
    fn mvcc_key_length_correct(
        key in prop::collection::vec(any::<u8>(), 0..512),
        ts in any::<u64>(),
    ) {
        let encoded = encode_mvcc_key(&key, Timestamp(ts));
        prop_assert_eq!(encoded.len(), 4 + key.len() + 8);
    }
}

// ---------------------------------------------------------------------------
// MVCC Value Encoding Properties
// ---------------------------------------------------------------------------

proptest! {
    /// Encode then decode a Value produces the same data and txn_id.
    #[test]
    fn mvcc_value_roundtrip(
        txn_id in 0u64..1_000_000,
        data in prop::collection::vec(any::<u8>(), 0..1024),
    ) {
        let val = MvccValue::Value { txn_id: TxnId(txn_id), data: data.clone() };
        let encoded = encode_mvcc_value(&val);
        let decoded = decode_mvcc_value(&encoded).unwrap();
        prop_assert_eq!(decoded, val);
    }

    /// Encode then decode a Tombstone produces the same txn_id.
    #[test]
    fn mvcc_tombstone_roundtrip(txn_id in 0u64..1_000_000) {
        let val = MvccValue::Tombstone { txn_id: TxnId(txn_id) };
        let encoded = encode_mvcc_value(&val);
        let decoded = decode_mvcc_value(&encoded).unwrap();
        prop_assert_eq!(decoded, val);
    }

    /// Value encoding first byte distinguishes Value (1) from Tombstone (0).
    #[test]
    fn mvcc_value_tag_correct(
        txn_id in 0u64..1_000_000,
        data in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let value_enc = encode_mvcc_value(&MvccValue::Value { txn_id: TxnId(txn_id), data });
        let tomb_enc = encode_mvcc_value(&MvccValue::Tombstone { txn_id: TxnId(txn_id) });
        prop_assert_eq!(value_enc[0], 1u8);
        prop_assert_eq!(tomb_enc[0], 0u8);
    }
}

// ---------------------------------------------------------------------------
// Storage Engine Put/Get Consistency
// ---------------------------------------------------------------------------

proptest! {
    /// For any sequence of put operations on distinct keys, get always returns
    /// the last value written for each key.
    #[test]
    fn put_get_consistency(
        ops in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 1..32), prop::collection::vec(any::<u8>(), 1..64)),
            1..50
        )
    ) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&db_path).unwrap();
        let bpm = BufferPoolManager::new(1000, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let db = Database::open(dir.path(), engine).unwrap();

        // Track expected state.
        let mut expected: std::collections::HashMap<Vec<u8>, Vec<u8>> = std::collections::HashMap::new();

        for (key, value) in &ops {
            db.put(key, value).unwrap();
            expected.insert(key.clone(), value.clone());
        }

        // Verify every key returns its last written value.
        for (key, expected_value) in &expected {
            let got = db.get(key).unwrap();
            prop_assert_eq!(got.as_ref(), Some(expected_value),
                "Key {:?} expected {:?}, got {:?}", key, expected_value, got);
        }
    }
}

// ---------------------------------------------------------------------------
// GC Safety Invariant
// ---------------------------------------------------------------------------

proptest! {
    /// After GC, every key that was visible before GC is still visible
    /// with the same value. GC never removes the "current" version.
    #[test]
    fn gc_preserves_visible_data(
        num_keys in 1usize..20,
        num_versions in 2usize..5,
    ) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&db_path).unwrap();
        let bpm = BufferPoolManager::new(1000, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let db = Database::open(dir.path(), engine).unwrap();

        // Write multiple versions of each key.
        for k in 0..num_keys {
            for v in 0..num_versions {
                let key = format!("k{:04}", k);
                let val = format!("v{:04}_{:04}", k, v);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
        }

        // Snapshot the expected state BEFORE GC.
        let mut expected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for k in 0..num_keys {
            let key = format!("k{:04}", k);
            let val = db.get(key.as_bytes()).unwrap();
            if let Some(v) = val {
                expected.push((key.into_bytes(), v));
            }
        }

        // Run GC.
        let stats = db.gc().unwrap();
        prop_assert!(stats.versions_removed > 0 || num_versions == 1,
            "GC should remove old versions when num_versions > 1");

        // Verify every visible key+value is preserved.
        for (key, expected_val) in &expected {
            let got = db.get(key).unwrap();
            prop_assert_eq!(got.as_ref(), Some(expected_val),
                "Key {:?} lost after GC", String::from_utf8_lossy(key));
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction Lifecycle Chaos
// ---------------------------------------------------------------------------

proptest! {
    /// Random sequence of txn operations. Committed data visible, aborted invisible.
    #[test]
    fn txn_chaos(
        commit_keys in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 1..16), 1..10
        ),
        abort_keys in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 1..16), 1..10
        ),
    ) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&db_path).unwrap();
        let bpm = BufferPoolManager::new(1000, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let db = Database::open(dir.path(), engine).unwrap();

        // Committed transaction.
        let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        for key in &commit_keys {
            db.txn_put(t1, key, b"committed").unwrap();
        }
        db.commit_txn(t1).unwrap();

        // Aborted transaction.
        let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
        for key in &abort_keys {
            db.txn_put(t2, key, b"aborted").unwrap();
        }
        db.txn_abort(t2).unwrap();

        // Verify: committed keys visible (unless overwritten by abort then restored).
        for key in &commit_keys {
            let val = db.get(key).unwrap();
            // If same key was in both sets, abort should have left committed value.
            if abort_keys.contains(key) {
                // The abort wrote "aborted" over "committed" but then aborted.
                // Under MVCC, the aborted version is invisible, committed is visible.
                prop_assert_eq!(val, Some(b"committed".to_vec()),
                    "Key {:?} should show committed value after abort", key);
            } else {
                prop_assert_eq!(val, Some(b"committed".to_vec()));
            }
        }

        // Verify: abort-only keys invisible.
        for key in &abort_keys {
            if !commit_keys.contains(key) {
                let val = db.get(key).unwrap();
                prop_assert_eq!(val, None,
                    "Aborted-only key {:?} should be invisible", key);
            }
        }
    }
}

// ---- Q-08: LSM invariants vs. HashMap oracle ----

use interchangedb::engines::lsm::LsmTree;
use std::collections::HashMap;

#[derive(Debug, Clone)]
enum LsmOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
    Flush,
}

fn lsm_op_strategy(key_pool: usize) -> impl Strategy<Value = LsmOp> {
    let key = (0usize..key_pool).prop_map(|i| format!("k{:03}", i).into_bytes());
    let value = prop::collection::vec(any::<u8>(), 0..64);
    prop_oneof![
        4 => (key.clone(), value).prop_map(|(k, v)| LsmOp::Put(k, v)),
        2 => key.clone().prop_map(LsmOp::Delete),
        4 => key.prop_map(LsmOp::Get),
        1 => Just(LsmOp::Flush),
    ]
}

proptest! {
    /// LsmTree behaves identically to a `HashMap` for any sequence of
    /// put/delete/get/flush operations. Catches: stale reads across
    /// memtable/SSTable boundaries, tombstone visibility, lookup
    /// ordering between L0 / Ln, flush-related bugs.
    #[test]
    fn lsm_matches_hashmap_oracle(
        ops in prop::collection::vec(lsm_op_strategy(8), 1..50)
    ) {
        let dir = tempdir().unwrap();
        let lsm = LsmTree::open(dir.path()).unwrap();
        let mut oracle: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        for op in &ops {
            match op {
                LsmOp::Put(k, v) => {
                    lsm.put(k.clone(), v.clone()).unwrap();
                    oracle.insert(k.clone(), v.clone());
                }
                LsmOp::Delete(k) => {
                    lsm.delete(k.clone()).unwrap();
                    oracle.remove(k);
                }
                LsmOp::Get(k) => {
                    let actual = lsm.get(k).unwrap();
                    let expected = oracle.get(k).cloned();
                    prop_assert_eq!(
                        actual.as_deref(),
                        expected.as_deref(),
                        "LSM/oracle divergence on key {:?}",
                        k
                    );
                }
                LsmOp::Flush => {
                    lsm.flush_memtable().unwrap();
                }
            }
        }

        for (k, v) in &oracle {
            let got = lsm.get(k).unwrap();
            prop_assert_eq!(got.as_deref(), Some(v.as_slice()),
                "final state: key {:?} missing/wrong in LSM", k);
        }
    }

    /// LSM state survives a drop+reopen cycle. Catches: SSTable
    /// persistence bugs, manifest replay errors. Always flushes before
    /// drop so the SSTable path is exercised.
    #[test]
    fn lsm_state_survives_reopen(
        ops in prop::collection::vec(lsm_op_strategy(6), 1..30)
    ) {
        let dir = tempdir().unwrap();
        let mut oracle: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        {
            let lsm = LsmTree::open(dir.path()).unwrap();
            for op in &ops {
                match op {
                    LsmOp::Put(k, v) => {
                        lsm.put(k.clone(), v.clone()).unwrap();
                        oracle.insert(k.clone(), v.clone());
                    }
                    LsmOp::Delete(k) => {
                        lsm.delete(k.clone()).unwrap();
                        oracle.remove(k);
                    }
                    LsmOp::Get(_) => {}
                    LsmOp::Flush => {
                        lsm.flush_memtable().unwrap();
                    }
                }
            }
            lsm.flush_memtable().unwrap();
        }

        let lsm2 = LsmTree::open(dir.path()).unwrap();
        for (k, v) in &oracle {
            let got = lsm2.get(k).unwrap();
            prop_assert_eq!(got.as_deref(), Some(v.as_slice()),
                "post-reopen: key {:?} missing/wrong", k);
        }
    }
}

// ---- Q-08: LockManager invariants ----

use interchangedb::txn::lock_manager::LockManager;
use interchangedb::txn::LockMode;

proptest! {
    /// Any sequence of uncontested acquires followed by `release_all`
    /// per txn must leave the lock table empty. Catches: leaked entries,
    /// orphaned wait queues, unreleased state in any policy path.
    #[test]
    fn lock_manager_table_empty_at_quiescence(
        actions in prop::collection::vec(
            (1u64..16, 0u8..8, prop_oneof![Just(LockMode::Shared), Just(LockMode::Exclusive)]),
            0..30,
        )
    ) {
        let lm = LockManager::new();
        let mut acquired: std::collections::HashSet<(u64, u8)> = std::collections::HashSet::new();
        let mut txns_used: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (txn_raw, key, mode) in &actions {
            let txn = interchangedb::TxnId(*txn_raw);
            let key_bytes = [*key];
            if !acquired.contains(&(*txn_raw, *key)) {
                // Skip contended acquires — those would block on default
                // timeout. Single-threaded property covers cleanup, not
                // contention. (Contention is exercised in lock_stress_test.)
                let contended = acquired.iter().any(|(t, k)| *k == *key && *t != *txn_raw);
                if !contended {
                    let res = lm.acquire(txn, &key_bytes, *mode);
                    prop_assert!(res.is_ok(), "uncontested acquire failed: {:?}", res);
                    acquired.insert((*txn_raw, *key));
                    txns_used.insert(*txn_raw);
                }
            }
        }

        for txn_raw in &txns_used {
            lm.release_all(interchangedb::TxnId(*txn_raw));
        }

        prop_assert_eq!(
            lm.lock_count(), 0,
            "lock_count should be 0 after release_all on every txn"
        );
    }

    /// S locks on the same key from multiple txns are always granted —
    /// never block, never spuriously return Deadlock. Catches false-
    /// positive conflict detection in the S+S compatibility check.
    #[test]
    fn lock_manager_no_false_positive_on_shared(
        n_shared in 2usize..8,
        key in 0u8..4,
    ) {
        let lm = LockManager::new();
        let key_bytes = [key];
        for i in 0..n_shared {
            let res = lm.acquire(
                interchangedb::TxnId((i + 1) as u64),
                &key_bytes,
                LockMode::Shared,
            );
            prop_assert!(res.is_ok(), "S+S grant {}/{}: {:?}", i, n_shared, res);
        }
        for i in 0..n_shared {
            lm.release_all(interchangedb::TxnId((i + 1) as u64));
        }
        prop_assert_eq!(lm.lock_count(), 0);
    }
}
