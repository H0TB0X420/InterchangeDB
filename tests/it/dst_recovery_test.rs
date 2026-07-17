//! Q-32 / stability.md pillar D: deterministic crash-at-every-LSN recovery
//! torture test.
//!
//! ## The idea
//!
//! A seeded workload of explicit transactions is run once against a database to
//! produce a **golden WAL**. Then, for *every* record boundary in that log, we
//! reconstruct a "crashed at LSN k" state by truncating the WAL to k, recover
//! into a **fresh empty engine**, and assert the recovered state equals an
//! **oracle** computed from the same truncated record list. Sweeping every
//! crash point exercises the WAL + recovery path under exactly the partial-tail
//! failures it exists to handle.
//!
//! ## Why this is deterministic (true DST)
//!
//! Recovery is WAL-only — the engine starts empty and `redo` rebuilds all state
//! from LSN 0 (no checkpoints in the workload), so we never depend on the
//! engine's data file being a consistent crash image. LSN assignment is a
//! deterministic counter, so a seed fully reproduces any failure.
//!
//! ## Why explicit transactions only
//!
//! Under WAL/MVCC every write is a transaction (`db.put` itself is
//! begin→txn_put→commit). Using explicit txns lets us capture each assigned
//! `TxnId` and the user keys/values we wrote, so the oracle never has to parse
//! MVCC-encoded WAL payloads — it only reads record *headers* (txn_id, type)
//! and record *boundaries* from the golden log.
//!
//! ## Invariants asserted at every crash point
//!
//! 1. Every committed write present; no aborted/uncommitted write visible
//!    (`db.get` == oracle, over the full key universe).
//! 2. Recovery is idempotent (recover twice from the same truncated WAL →
//!    identical state).
//!
//! The "no torn page passes checksum undetected" invariant needs torn-write
//! fault injection in the *engine* data file (not WAL truncation) and is the
//! next pillar-D milestone — out of scope here. See the Q-32 row in ISSUES.md.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::engines::lsm::LsmEngine;
use interchangedb::storage::{MemoryDiskManager, StorageEngine};
use interchangedb::txn::TxnMode;
use interchangedb::wal::{LogPayload, LogRecord};

// ---------------------------------------------------------------------------
// Deterministic RNG (SplitMix64-style: an LCG step plus an output mix). Pure
// function of the seed — the whole test reproduces from the seed alone.
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let mut x = self.0;
        x ^= x >> 33;
        x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^= x >> 33;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// Planned workload — explicit transactions over a small key space.
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum Write {
    Put(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
}

struct PlannedTxn {
    writes: Vec<Write>,
    commit: bool,
}

fn gen_workload(seed: u64, n_txns: usize, key_space: usize, max_writes: usize) -> Vec<PlannedTxn> {
    let mut rng = Rng::new(seed);
    let mut txns = Vec::with_capacity(n_txns);
    for _ in 0..n_txns {
        let n_writes = 1 + rng.below(max_writes);
        let mut writes = Vec::with_capacity(n_writes);
        for _ in 0..n_writes {
            let key = format!("k{:02}", rng.below(key_space)).into_bytes();
            // ~1 in 4 writes is a delete.
            if rng.below(4) == 0 {
                writes.push(Write::Del(key));
            } else {
                let value = format!("v{}", rng.next_u64() % 1000).into_bytes();
                writes.push(Write::Put(key, value));
            }
        }
        // ~4 in 5 transactions commit; the rest abort.
        let commit = rng.below(5) != 0;
        txns.push(PlannedTxn { writes, commit });
    }
    txns
}

/// Every distinct user key the workload touches — the comparison universe.
fn key_universe(txns: &[PlannedTxn]) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for txn in txns {
        for write in &txn.writes {
            let key = match write {
                Write::Put(k, _) => k,
                Write::Del(k) => k,
            };
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
    }
    keys
}

// ---------------------------------------------------------------------------
// Engine + database constructors, one per engine. Recovery is WAL-only — the
// engine starts fresh on every open and `redo` rebuilds all state from the
// (file-backed) WAL, so the sweep is engine-agnostic. The B-tree is in-memory;
// the LSM is file-backed in the same dir as its WAL (separate subdirs, no
// clash). Both are `Database<E>` so the generic sweep drives them identically.
// ---------------------------------------------------------------------------

type DbMaker<E> = fn(&Path) -> Database<E>;

fn btree_db(dir: &Path) -> Database<BTreeEngine> {
    let bpm = BufferPoolManager::new(256, MemoryDiskManager::new());
    Database::open(dir, BTreeEngine::new(bpm).unwrap()).unwrap()
}

fn lsm_db(dir: &Path) -> Database<LsmEngine> {
    Database::open(dir, LsmEngine::new(dir).unwrap()).unwrap()
}

/// Run the planned workload against a fresh db in `dir`. Returns the assigned
/// transaction id for each planned txn, in order. The db is dropped here, so
/// the WAL files in `dir/wal` are fully flushed (every txn commits or aborts,
/// and both `sync_to`).
fn run_workload<E: StorageEngine>(
    dir: &Path,
    txns: &[PlannedTxn],
    make_db: DbMaker<E>,
) -> Vec<u64> {
    let db = make_db(dir);
    let mut ids = Vec::with_capacity(txns.len());
    for txn in txns {
        let txn_id = db.begin_txn(TxnMode::ReadWrite).unwrap();
        for write in &txn.writes {
            match write {
                Write::Put(key, value) => db.txn_put(txn_id, key, value).unwrap(),
                Write::Del(key) => db.txn_delete(txn_id, key).unwrap(),
            }
        }
        if txn.commit {
            db.commit_txn(txn_id).unwrap();
        } else {
            db.txn_abort(txn_id).unwrap();
        }
        ids.push(txn_id.0);
    }
    ids
}

/// db.get for every key in the universe, aligned with `universe`.
fn snapshot<E: StorageEngine>(db: &Database<E>, universe: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
    universe.iter().map(|k| db.get(k).unwrap()).collect()
}

// ---------------------------------------------------------------------------
// Golden WAL scan. We only read headers (txn_id, is_commit) and record
// boundaries — never the MVCC-encoded payload keys.
// ---------------------------------------------------------------------------

struct Rec {
    txn_id: u64,
    is_commit: bool,
    /// Byte offset just past this record in the segment (the truncation point
    /// for "crash right after this record").
    end: usize,
}

/// Read the single golden WAL segment, returning its raw bytes and the ordered
/// record metadata. Asserts the workload fit one segment (multi-segment
/// truncation is the documented next step, not needed at this scale).
fn read_golden_wal(dir: &Path) -> (Vec<u8>, Vec<Rec>) {
    let wal_dir = dir.join("wal");
    let mut segments: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .collect();
    segments.sort();
    assert_eq!(
        segments.len(),
        1,
        "workload must fit one WAL segment (multi-segment truncation is future work)"
    );

    let bytes = fs::read(&segments[0]).unwrap();
    let mut recs = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let (record, consumed) =
            LogRecord::decode(&bytes[offset..]).expect("golden WAL must decode cleanly");
        offset += consumed;
        recs.push(Rec {
            txn_id: record.txn_id,
            is_commit: matches!(record.payload, LogPayload::Commit { .. }),
            end: offset,
        });
    }
    assert_eq!(offset, bytes.len(), "golden WAL must have no torn tail");
    (bytes, recs)
}

/// Map each committed txn id to the index of its Commit record.
fn commit_indices(recs: &[Rec]) -> HashMap<u64, usize> {
    let mut map = HashMap::new();
    for (i, rec) in recs.iter().enumerate() {
        if rec.is_commit {
            map.insert(rec.txn_id, i);
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Oracle. With `cut` records kept (indices [0, cut)), a txn is durably
// committed iff its Commit record index < cut. Apply committed txns' writes in
// commit order (= generation order, single-threaded) — last writer wins, which
// matches MVCC visibility (highest commit_ts).
// ---------------------------------------------------------------------------

fn oracle(
    txns: &[PlannedTxn],
    ids: &[u64],
    commit_idx: &HashMap<u64, usize>,
    cut: usize,
) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut committed: Vec<(usize, usize)> = Vec::new(); // (commit_record_index, txn_pos)
    for (pos, id) in ids.iter().enumerate() {
        if let Some(&ci) = commit_idx.get(id) {
            if ci < cut {
                committed.push((ci, pos));
            }
        }
    }
    committed.sort_unstable();

    let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for (_, pos) in committed {
        for write in &txns[pos].writes {
            match write {
                Write::Put(key, value) => {
                    state.insert(key.clone(), value.clone());
                }
                Write::Del(key) => {
                    state.remove(key);
                }
            }
        }
    }
    state
}

// ---------------------------------------------------------------------------
// The sweep.
// ---------------------------------------------------------------------------

fn run_sweep<E: StorageEngine>(
    label: &str,
    make_db: DbMaker<E>,
    seed: u64,
    n_txns: usize,
    key_space: usize,
    max_writes: usize,
) {
    let txns = gen_workload(seed, n_txns, key_space, max_writes);
    let universe = key_universe(&txns);

    let golden_dir = tempdir().unwrap();
    let ids = run_workload(golden_dir.path(), &txns, make_db);
    let (golden, recs) = read_golden_wal(golden_dir.path());
    let commit_idx = commit_indices(&recs);

    // Self-check: the golden log's Commit records match the planned commits.
    let planned_commits = txns.iter().filter(|t| t.commit).count();
    let logged_commits = recs.iter().filter(|r| r.is_commit).count();
    assert_eq!(
        logged_commits, planned_commits,
        "[{label}] seed {seed}: golden WAL has {logged_commits} commits, planned {planned_commits}"
    );

    // Crash at every record boundary, including the empty prefix (cut 0) and the
    // full log (cut == N).
    for cut in 0..=recs.len() {
        let cut_bytes = if cut == 0 { 0 } else { recs[cut - 1].end };
        let want = oracle(&txns, &ids, &commit_idx, cut);

        // First recovery from the truncated WAL.
        let recover_dir = tempdir().unwrap();
        let wal_dir = recover_dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        fs::write(wal_dir.join("00000000.wal"), &golden[..cut_bytes]).unwrap();

        let snap1 = {
            let db = make_db(recover_dir.path());
            snapshot(&db, &universe)
        };

        // Invariant 1: recovered state == oracle over the whole key universe.
        for (i, key) in universe.iter().enumerate() {
            let got = &snap1[i];
            let expected = want.get(key).cloned();
            assert_eq!(
                got, &expected,
                "[{label}] seed {seed}, cut {cut}: key {key:?} = {got:?}, oracle {expected:?}"
            );
        }

        // Invariant 2: recovery is idempotent — recover again from the same
        // truncated WAL into a fresh engine, expect identical state.
        let snap2 = {
            let db = make_db(recover_dir.path());
            snapshot(&db, &universe)
        };
        assert_eq!(
            snap1, snap2,
            "[{label}] seed {seed}, cut {cut}: recovery is not idempotent"
        );
    }
}

// ---------------------------------------------------------------------------
// Test entry points.
// ---------------------------------------------------------------------------

/// CI-friendly: one seed, modest workload, full LSN sweep — per engine. The
/// same crash sweep proves durability for *both* engines through the shared
/// Database/WAL/recovery stack, not just the B-tree.
#[test]
fn dst_crash_at_every_lsn_btree() {
    run_sweep("btree", btree_db, 0x00C0_FFEE, 10, 8, 3);
}

#[test]
fn dst_crash_at_every_lsn_lsm() {
    run_sweep("lsm", lsm_db, 0x00C0_FFEE, 10, 8, 3);
}

/// Soak: many seeds × larger workloads × full LSN sweep, both engines. Run with
/// `cargo test --release -- --ignored`.
#[test]
#[ignore = "soak: many seeds, full per-LSN recovery sweep; run with --ignored --release"]
fn dst_crash_at_every_lsn_soak() {
    for seed in 0..16u64 {
        let s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
        run_sweep("btree", btree_db, s, 30, 16, 5);
        run_sweep("lsm", lsm_db, s, 30, 16, 5);
    }
}
