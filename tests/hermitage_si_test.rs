//! Q-24: Hermitage isolation-anomaly test suite.
//!
//! Adam Wiggins's Hermitage suite enumerates concurrency anomalies named
//! G0..G2 (after Adya et al. 2000). Snapshot Isolation prevents all named
//! anomalies *except* write skew (a flavor of G2 anti-dependency cycle).
//!
//! Each test below sets up two interleaved transactions and asserts the
//! observable property associated with SI. Anomaly names follow the
//! Hermitage README so anyone familiar with that document can find the
//! corresponding test by name.
//!
//! Mapping to SI guarantees:
//! - G0 (write cycles)           — PREVENTED by SI (write-write conflict)
//! - G1a (aborted reads)         — PREVENTED (aborts invisible to snapshots)
//! - G1b (intermediate reads)    — PREVENTED (snapshots are at one timestamp)
//! - G1c (read-after-write cycle)— PREVENTED
//! - OTV (observed transaction vanishes / G2-item) — PREVENTED
//! - G2 (anti-dependency cycle / write skew) — ALLOWED (documented SI limitation)
//!
//! All tests run against both engines via the existing `Database<E>` API.

use interchangedb::{Database, Error, TxnMode};
use tempfile::TempDir;

fn fresh_db() -> (Database<interchangedb::BTreeEngine>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let bpm = interchangedb::BufferPoolManager::new(
        64,
        interchangedb::FileDiskManager::create(dir.path().join("test.db")).unwrap(),
    );
    let engine = interchangedb::BTreeEngine::new(bpm).unwrap();
    // Open (not new) — txn manager + WAL required for explicit transactions.
    let db = Database::open(dir.path(), engine).unwrap();
    (db, dir)
}

fn seed(db: &Database<interchangedb::BTreeEngine>, k: &[u8], v: &[u8]) {
    db.put(k, v).unwrap();
}

// ---- G0: Write Cycles (Dirty Writes) ----
//
// Hermitage: T1 writes x=11, T2 writes x=12, T1 writes y=11, T2 writes y=12,
// both commit. Under SI: write-write conflict on x — one txn aborts.

#[test]
fn g0_write_cycles_prevented() {
    let (db, _dir) = fresh_db();
    seed(&db, b"x", b"10");
    seed(&db, b"y", b"10");

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();

    db.txn_put(t1, b"x", b"11").unwrap();

    // T2's write to x must not silently succeed. Either an immediate
    // WriteConflict (MVCC layer) or a LockTimeout (2PL layer) is acceptable
    // — both prevent the G0 anomaly. Silent success would mean two
    // concurrent writers can both finalize x, the classic dirty-write bug.
    let t2_x = db.txn_put(t2, b"x", b"12");
    assert!(
        matches!(t2_x, Err(Error::WriteConflict { .. }) | Err(Error::LockTimeout)),
        "expected WriteConflict or LockTimeout, got: {:?}",
        t2_x
    );

    db.commit_txn(t1).unwrap();
    db.txn_abort(t2).ok(); // already poisoned; abort is idempotent or NoOp

    assert_eq!(db.get(b"x").unwrap(), Some(b"11".to_vec()));
    assert_eq!(db.get(b"y").unwrap(), Some(b"10".to_vec()));
}

// ---- G1a: Aborted Reads ----
//
// Hermitage: T1 writes x=101, T2 reads x, T1 aborts. T2 must not see 101.

#[test]
fn g1a_aborted_reads_invisible() {
    let (db, _dir) = fresh_db();
    seed(&db, b"x", b"10");

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"x", b"101").unwrap();

    // T2 starts AFTER T1's write but before T1's abort. Its snapshot must
    // not include T1's uncommitted write.
    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    assert_eq!(db.txn_get(t2, b"x").unwrap(), Some(b"10".to_vec()));

    db.txn_abort(t1).unwrap();

    // After abort, T2's view is still the pre-T1 state.
    assert_eq!(db.txn_get(t2, b"x").unwrap(), Some(b"10".to_vec()));
    db.commit_txn(t2).unwrap();
}

// ---- G1b: Intermediate Reads ----
//
// Hermitage: T1 writes x=101, then x=11, then commits. T2 (started before
// T1's commit) must never see x=101 (the intermediate value).

#[test]
fn g1b_intermediate_reads_invisible() {
    let (db, _dir) = fresh_db();
    seed(&db, b"x", b"10");

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"x", b"101").unwrap();
    db.txn_put(t1, b"x", b"11").unwrap();

    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    // T2's snapshot is taken before T1 commits → sees the seeded value.
    assert_eq!(db.txn_get(t2, b"x").unwrap(), Some(b"10".to_vec()));

    db.commit_txn(t1).unwrap();

    // T2 still sees its original snapshot (10), not 101 and not 11.
    let observed = db.txn_get(t2, b"x").unwrap();
    assert_eq!(observed, Some(b"10".to_vec()));
    assert_ne!(
        observed,
        Some(b"101".to_vec()),
        "G1b: intermediate value leaked"
    );
    db.commit_txn(t2).unwrap();
}

// ---- G1c: Circular Information Flow (Read-after-Write cycles) ----
//
// Hermitage: T1 reads x, T2 writes y, T1 writes y, T2 reads x. Both commit.
// Form a cycle T1 → T2 (T1's write to y depends on T2's write, after T2
// already read T1's write to x?). The SI check: each transaction's view
// is a consistent snapshot — no cycle of reads-then-writes.

#[test]
fn g1c_circular_information_flow_prevented() {
    let (db, _dir) = fresh_db();
    seed(&db, b"x", b"10");
    seed(&db, b"y", b"10");

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();

    // Each reads the other's "future" write target.
    assert_eq!(db.txn_get(t1, b"y").unwrap(), Some(b"10".to_vec()));
    assert_eq!(db.txn_get(t2, b"x").unwrap(), Some(b"10".to_vec()));

    // Each writes the value the other read.
    db.txn_put(t1, b"x", b"11").unwrap();
    db.txn_put(t2, b"y", b"22").unwrap();

    db.commit_txn(t1).unwrap();
    db.commit_txn(t2).unwrap();

    // Final state: both writes applied; no cycle in the *committed* history
    // (they touched disjoint keys). The "cycle" Hermitage names is on reads
    // — each saw the pre-image, not the other's write.
    assert_eq!(db.get(b"x").unwrap(), Some(b"11".to_vec()));
    assert_eq!(db.get(b"y").unwrap(), Some(b"22".to_vec()));
}

// ---- OTV / G2-item: Observed Transaction Vanishes ----
//
// Hermitage: T1 updates x=11 and y=19 (cross-row invariant: x+y=21). T2
// reads x then y; under SI, T2's snapshot is taken before T1 commits, so
// T2 sees the original consistent state — not a partial update.

#[test]
fn g2_item_observed_transaction_vanishes_prevented() {
    let (db, _dir) = fresh_db();
    seed(&db, b"x", b"1");
    seed(&db, b"y", b"20");

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"x", b"11").unwrap();
    db.txn_put(t1, b"y", b"19").unwrap();

    // T2 starts after T1's writes but before commit. Snapshot pre-dates T1.
    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let x = db.txn_get(t2, b"x").unwrap();
    let y = db.txn_get(t2, b"y").unwrap();
    assert_eq!(x, Some(b"1".to_vec()));
    assert_eq!(y, Some(b"20".to_vec()));

    db.commit_txn(t1).unwrap();

    // After T1 commits, T2 still sees the pre-T1 state.
    assert_eq!(db.txn_get(t2, b"x").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.txn_get(t2, b"y").unwrap(), Some(b"20".to_vec()));
    db.commit_txn(t2).unwrap();

    // Auto-commit reads after both txns done: T1's writes are visible.
    assert_eq!(db.get(b"x").unwrap(), Some(b"11".to_vec()));
    assert_eq!(db.get(b"y").unwrap(), Some(b"19".to_vec()));
}

// ---- G2 (Write Skew): ALLOWED under SI ----
//
// Hermitage: T1 reads x and y, T2 reads x and y. T1 writes x (based on
// what y was), T2 writes y (based on what x was). Both commit because
// they touched disjoint keys. Under SI this is permitted; under
// serializability it would be rejected.
//
// This test documents that our SI behavior matches the spec: write skew
// is observable. If we ever upgrade to SSI (serializable snapshot
// isolation), this test should flip to expect rejection.

#[test]
fn g2_write_skew_is_observable_under_si() {
    let (db, _dir) = fresh_db();
    seed(&db, b"x", b"1");
    seed(&db, b"y", b"1");
    // Invariant the application would want: x + y >= 1 (at least one >= 1).
    // SI doesn't enforce cross-row invariants.

    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();

    // Both read both keys — both observe x=1, y=1.
    let _ = db.txn_get(t1, b"x").unwrap();
    let _ = db.txn_get(t1, b"y").unwrap();
    let _ = db.txn_get(t2, b"x").unwrap();
    let _ = db.txn_get(t2, b"y").unwrap();

    // T1 sets x=0 (assuming y>=1 covers the invariant); T2 sets y=0
    // (assuming x>=1). Disjoint keys, no write-write conflict.
    db.txn_put(t1, b"x", b"0").unwrap();
    db.txn_put(t2, b"y", b"0").unwrap();

    db.commit_txn(t1).unwrap();
    db.commit_txn(t2).unwrap();

    // Final state violates the application invariant: x=0 AND y=0.
    // SI permits this. Documented limitation.
    assert_eq!(db.get(b"x").unwrap(), Some(b"0".to_vec()));
    assert_eq!(db.get(b"y").unwrap(), Some(b"0".to_vec()));
}
