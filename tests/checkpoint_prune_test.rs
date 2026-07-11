//! T16.1 — `committed_txns` checkpoint-time pruning.
//!
//! The committed-txns map was insert-only until recovery, leaking memory
//! under sustained load. `Database::checkpoint` now prunes entries with
//! `commit_ts <= min(checkpoint_ts, oldest_active_read_ts)` — versions from
//! pruned writers are answered by the assumed-committed heuristic
//! identically (see `TransactionManager::prune_committed_below` for the
//! proof). These tests pin the three ways that can go wrong:
//!
//! 1. The bound: a long-running reader's snapshot must pin the prune (its
//!    read_ts caps the bound), or a version committed after the reader began
//!    would flip from invisible to visible mid-transaction.
//! 2. Post-prune visibility: new readers must see pre-checkpoint data via
//!    the heuristic (map entry gone, data still visible).
//! 3. Crash equivalence: pruning must match what recovery rebuilds after
//!    the checkpoint truncated old segments — reopen and verify.
//!
//! Plus the reason the feature exists: the map stays bounded across
//! sustained checkpointed load.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::FileDiskManager;
use interchangedb::txn::TxnMode;

fn open_db(dir: &std::path::Path) -> Database<BTreeEngine> {
    let dm = FileDiskManager::create(dir.join("test.db")).unwrap();
    let engine = BTreeEngine::new(BufferPoolManager::new(256, dm)).unwrap();
    Database::open(dir, engine).unwrap()
}

fn reopen_db(dir: &std::path::Path) -> Database<BTreeEngine> {
    let dm = FileDiskManager::open(dir.join("test.db")).unwrap();
    let engine = BTreeEngine::new(BufferPoolManager::new(256, dm)).unwrap();
    Database::open(dir, engine).unwrap()
}

/// One committed write txn; returns nothing. Keys are distinct per call.
fn commit_one(db: &Database<BTreeEngine>, key: &[u8], val: &[u8]) {
    let t = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t, key, val).unwrap();
    db.commit_txn(t).unwrap();
}

// The feature: sustained load with periodic checkpoints keeps the map
// bounded instead of growing linearly with committed txns.
#[test]
fn committed_map_stays_bounded_across_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    for round in 0..5u32 {
        for i in 0..20u32 {
            let key = format!("k{}_{}", round, i);
            commit_one(&db, key.as_bytes(), b"v");
        }
        db.checkpoint().unwrap();
        // No active txns at checkpoint time → bound = checkpoint_ts →
        // every entry committed before the checkpoint is prunable.
        let tracked = db.gc_status().unwrap().committed_txns_tracked;
        assert_eq!(
            tracked, 0,
            "round {}: map should be fully pruned at an idle checkpoint, \
             still tracking {}",
            round, tracked
        );
    }
}

// The bound: an active reader's snapshot caps the prune. A version
// committed AFTER the reader began must stay invisible to it across a
// checkpoint — if the prune dropped that entry, the assumed-committed
// heuristic would flip it visible (the exact divergence the
// min(checkpoint, oldest-active) bound exists to prevent).
#[test]
fn long_running_reader_visibility_survives_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    commit_one(&db, b"key", b"v1");

    // Reader pins its snapshot before the second write.
    let reader = db.begin_txn(TxnMode::ReadOnly).unwrap();
    assert_eq!(db.txn_get(reader, b"key").unwrap(), Some(b"v1".to_vec()));

    // v2 commits after the reader's snapshot.
    commit_one(&db, b"key", b"v2");

    db.checkpoint().unwrap();

    // v2's entry must have survived the prune (commit_ts > reader's
    // read_ts ≥ bound): the reader still sees v1.
    assert_eq!(
        db.txn_get(reader, b"key").unwrap(),
        Some(b"v1".to_vec()),
        "reader's snapshot changed across a checkpoint — prune bound broken"
    );
    db.commit_txn(reader).unwrap();

    // With the reader gone, a fresh read sees v2.
    let after = db.begin_txn(TxnMode::ReadOnly).unwrap();
    assert_eq!(db.txn_get(after, b"key").unwrap(), Some(b"v2".to_vec()));
    db.commit_txn(after).unwrap();
}

// Post-prune reads: a NEW reader must see pre-checkpoint data even though
// its writers' map entries are gone — the assumed-committed heuristic
// (version_ts <= checkpoint_ts) is now the answering path.
#[test]
fn pruned_writers_stay_visible_to_new_readers() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path());

    for i in 0..10u32 {
        commit_one(&db, format!("k{}", i).as_bytes(), b"v");
    }
    db.checkpoint().unwrap();
    assert_eq!(db.gc_status().unwrap().committed_txns_tracked, 0);

    let reader = db.begin_txn(TxnMode::ReadOnly).unwrap();
    for i in 0..10u32 {
        assert_eq!(
            db.txn_get(reader, format!("k{}", i).as_bytes()).unwrap(),
            Some(b"v".to_vec()),
            "pre-checkpoint key k{} invisible after prune",
            i
        );
    }
    db.commit_txn(reader).unwrap();
}

// Crash equivalence: prune + checkpoint truncation, more commits, crash
// (drop without clean shutdown), reopen. Recovery rebuilds only
// post-checkpoint commits — which is exactly the pruned shape — and ALL
// data (pre- and post-checkpoint) must read back.
#[test]
fn recovery_after_prune_and_crash_reads_everything() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_db(dir.path());
        commit_one(&db, b"before", b"old");
        db.checkpoint().unwrap();
        commit_one(&db, b"after", b"new");
        // Crash: drop with no clean shutdown / final checkpoint.
    }

    let db = reopen_db(dir.path());
    let reader = db.begin_txn(TxnMode::ReadOnly).unwrap();
    assert_eq!(
        db.txn_get(reader, b"before").unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(db.txn_get(reader, b"after").unwrap(), Some(b"new".to_vec()));
    db.commit_txn(reader).unwrap();
}
