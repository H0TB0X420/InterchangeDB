//! Group-commit behaviour: concurrent commits batch into far fewer
//! fsyncs, without losing durability.
//!
//! The WAL leader flushes the buffer then releases the writer lock before
//! fsyncing, so while one commit's fsync is in flight, other threads
//! append and ride the next batch. These tests pin both halves of that
//! contract: **batching** (fsync count ≪ commit count under concurrency)
//! and **durability** (every committed value is present afterward).

use std::sync::{Arc, Barrier};
use std::thread;

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::FileDiskManager;

fn setup() -> (Arc<Database<BTreeEngine>>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let bpm = BufferPoolManager::new(1024, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let db = Database::open(dir.path(), engine).unwrap();
    (Arc::new(db), dir)
}

#[test]
fn concurrent_commits_batch_into_fewer_fsyncs() {
    // Each `db.put` is one autocommit = one WAL commit. With 8 threads
    // hammering commits, the leader/follower group commit should fold many
    // of them into single fsyncs. Without batching, fsyncs == commits.
    const THREADS: usize = 8;
    const PER_THREAD: usize = 200;
    let total = THREADS * PER_THREAD;

    let (db, _dir) = setup();
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait(); // maximize overlap so commits actually contend
                for i in 0..PER_THREAD {
                    let key = format!("k_{:02}_{:04}", t, i);
                    db.put(key.as_bytes(), b"v").unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    // Durability: every committed key is present.
    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            let key = format!("k_{:02}_{:04}", t, i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap().as_deref(),
                Some(&b"v"[..]),
                "missing committed key {}",
                key
            );
        }
    }

    // Batching: fsyncs must be meaningfully below the commit count. The
    // exact ratio is timing-dependent; we only require that batching
    // clearly happened (well under 1 fsync per commit).
    let fsyncs = db.wal_fsync_count();
    assert!(
        fsyncs < total as u64,
        "expected fewer fsyncs than {} commits, got {}",
        total,
        fsyncs
    );
    assert!(
        fsyncs <= (total as u64) * 3 / 4,
        "expected meaningful batching (<=75% of {} commits), got {} fsyncs",
        total,
        fsyncs
    );
    eprintln!(
        "group commit: {} commits → {} fsyncs ({:.1}x batch)",
        total,
        fsyncs,
        total as f64 / fsyncs as f64
    );
}

#[test]
fn single_threaded_commits_are_each_durable() {
    // No concurrency → no batching opportunity, but every commit must
    // still be durable. Guards against the leader path mishandling the
    // uncontended case.
    let (db, _dir) = setup();
    for i in 0..50 {
        let key = format!("solo_{:03}", i);
        db.put(key.as_bytes(), key.as_bytes()).unwrap();
    }
    for i in 0..50 {
        let key = format!("solo_{:03}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap().as_deref(),
            Some(key.as_bytes())
        );
    }
}
