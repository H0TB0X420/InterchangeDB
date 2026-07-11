//! `IsolationPolicy` axis (Q-34): isolation-level configurations + the
//! conformance contract.
//!
//! Unlike the other axes this is **not** an equivalence matrix — isolation
//! levels differ by design. Each level is checked against the Hermitage-style
//! anomaly spectrum it is required to block vs. allow. The contrast cells
//! (`non_repeatable_read`, `lost_update`: SI blocks, RC allows) are what give
//! the matrix teeth.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::FileDiskManager;
use interchangedb::txn::isolation::{IsolationPolicy, ReadCommitted, SnapshotIsolation};
use interchangedb::txn::TxnMode;
use interchangedb::Error;
use tempfile::TempDir;

/// A maker for the isolation registry.
pub type IsolationMaker = fn() -> Arc<dyn IsolationPolicy>;

pub fn si() -> Arc<dyn IsolationPolicy> {
    Arc::new(SnapshotIsolation)
}

pub fn read_committed() -> Arc<dyn IsolationPolicy> {
    Arc::new(ReadCommitted)
}

/// Whether an anomaly is blocked (`Prevented`) or observable (`Allowed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Prevented,
    Allowed,
}

fn fresh_db(policy: Arc<dyn IsolationPolicy>) -> (Database<BTreeEngine>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let bpm = BufferPoolManager::new(
        64,
        FileDiskManager::create(dir.path().join("iso.db")).unwrap(),
    );
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open_with_isolation(dir.path(), engine, policy).unwrap();
    (db, dir)
}

// ---- anomaly scenarios: each returns whether the anomaly is observable ----

/// G0 dirty write: two txns write the same key concurrently. Both levels prevent
/// it via the commit-duration X-lock.
fn g0_dirty_write(policy: Arc<dyn IsolationPolicy>) -> Outcome {
    let (db, _dir) = fresh_db(policy);
    db.put(b"x", b"0").unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"x", b"1").unwrap(); // t1 holds the X-lock
    let r = db.txn_put(t2, b"x", b"2"); // must not dirty-write
    db.commit_txn(t1).ok();
    db.txn_abort(t2).ok();
    match r {
        Err(Error::WriteConflict { .. }) | Err(Error::LockTimeout) => Outcome::Prevented,
        _ => Outcome::Allowed,
    }
}

/// Non-repeatable (fuzzy) read: a txn reads x, another commits a new x, the txn
/// re-reads. SI is repeatable (snapshot-at-begin); RC sees the new value.
fn non_repeatable_read(policy: Arc<dyn IsolationPolicy>) -> Outcome {
    let (db, _dir) = fresh_db(policy);
    db.put(b"x", b"10").unwrap();
    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let first = db.txn_get(t2, b"x").unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"x", b"20").unwrap();
    db.commit_txn(t1).unwrap();
    let second = db.txn_get(t2, b"x").unwrap();
    db.commit_txn(t2).ok();
    if first == second {
        Outcome::Prevented
    } else {
        Outcome::Allowed
    }
}

/// Lost update (P4): two txns read x; one commits x+1; the other then writes x+1
/// from its stale read. SI rejects the second write (first-committer-wins); RC
/// lets it overwrite, losing the first update.
fn lost_update(policy: Arc<dyn IsolationPolicy>) -> Outcome {
    let (db, _dir) = fresh_db(policy);
    db.put(b"x", b"10").unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let _ = db.txn_get(t1, b"x").unwrap();
    let _ = db.txn_get(t2, b"x").unwrap();
    db.txn_put(t1, b"x", b"11").unwrap();
    db.commit_txn(t1).unwrap(); // releases t1's X-lock
    match db.txn_put(t2, b"x", b"11") {
        Err(Error::WriteConflict { .. }) | Err(Error::LockTimeout) => {
            db.txn_abort(t2).ok();
            Outcome::Prevented
        }
        Ok(()) => {
            db.commit_txn(t2).ok();
            Outcome::Allowed
        }
        Err(_) => {
            db.txn_abort(t2).ok();
            Outcome::Prevented
        }
    }
}

/// Write skew (G2-item): two txns read x,y then write *disjoint* keys based on
/// the other. Both SI and RC allow it (no write-write conflict) — SI's documented
/// limitation; only serializability would block it.
fn write_skew(policy: Arc<dyn IsolationPolicy>) -> Outcome {
    let (db, _dir) = fresh_db(policy);
    db.put(b"x", b"1").unwrap();
    db.put(b"y", b"1").unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let t2 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    let _ = db.txn_get(t1, b"y").unwrap();
    let _ = db.txn_get(t2, b"x").unwrap();
    db.txn_put(t1, b"x", b"0").unwrap();
    db.txn_put(t2, b"y", b"0").unwrap();
    let c1 = db.commit_txn(t1);
    let c2 = db.commit_txn(t2);
    let both_skewed = c1.is_ok()
        && c2.is_ok()
        && db.get(b"x").unwrap() == Some(b"0".to_vec())
        && db.get(b"y").unwrap() == Some(b"0".to_vec());
    if both_skewed {
        Outcome::Allowed
    } else {
        Outcome::Prevented
    }
}

/// Dirty read (G1a-ish): a txn reads a key while another holds an *uncommitted*
/// write to it, then that writer aborts. Both levels must hide the uncommitted
/// value — "Read Committed" literally promises this. This is the scenario the
/// visibility predicate exists for; without it a `visible -> true` bug slips
/// through (caught by mutation testing).
fn dirty_read(policy: Arc<dyn IsolationPolicy>) -> Outcome {
    let (db, _dir) = fresh_db(policy);
    db.put(b"x", b"10").unwrap();
    let t1 = db.begin_txn(TxnMode::ReadWrite).unwrap();
    db.txn_put(t1, b"x", b"99").unwrap(); // uncommitted
    let t2 = db.begin_txn(TxnMode::ReadOnly).unwrap();
    let seen = db.txn_get(t2, b"x").unwrap();
    db.txn_abort(t1).ok();
    db.commit_txn(t2).ok();
    if seen == Some(b"10".to_vec()) {
        Outcome::Prevented // never saw the uncommitted write
    } else {
        Outcome::Allowed // saw the dirty 99
    }
}

/// The anomaly spectrum each isolation level is required to exhibit.
fn expected(level: &str) -> [(&'static str, Outcome); 5] {
    use Outcome::{Allowed, Prevented};
    match level {
        "si" => [
            ("g0_dirty_write", Prevented),
            ("non_repeatable_read", Prevented),
            ("lost_update", Prevented),
            ("write_skew", Allowed),
            ("dirty_read", Prevented),
        ],
        "read-committed" => [
            ("g0_dirty_write", Prevented),
            ("non_repeatable_read", Allowed),
            ("lost_update", Allowed),
            ("write_skew", Allowed),
            ("dirty_read", Prevented),
        ],
        other => panic!("no anomaly spec for isolation level `{other}`"),
    }
}

/// Run the anomaly scenarios at the given level and assert each outcome matches
/// the level's required spectrum.
pub fn assert_isolation_contract(make: IsolationMaker) {
    let level = make().name();
    let actual = [
        ("g0_dirty_write", g0_dirty_write(make())),
        ("non_repeatable_read", non_repeatable_read(make())),
        ("lost_update", lost_update(make())),
        ("write_skew", write_skew(make())),
        ("dirty_read", dirty_read(make())),
    ];
    for ((name, got), (ename, want)) in actual.iter().zip(expected(level).iter()) {
        assert_eq!(name, ename, "scenario order mismatch");
        assert_eq!(
            got, want,
            "level `{level}`: anomaly `{name}` — expected {want:?}, got {got:?}"
        );
    }
}
