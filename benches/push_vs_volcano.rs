//! Phase 15 / Increment 2b.5: Volcano (pull) vs. Push streaming win.
//!
//! `SELECT * FROM t LIMIT 10` over a large table. Pull's `SeqScan` eagerly
//! builds a `Vec` of every row at construction, then `Limit` discards all but
//! 10. Push's `LimitSink` returns `Stop` after 10, which halts `ScanSource`'s
//! loop — and now that the storage read is lazy end-to-end (lazy
//! `MvccScan` + lazy `BTreeScanIterator`, after the `ScanIterator:
//! DoubleEndedIterator` bound was relaxed), that `Stop` propagates all the way
//! to the engine: push reads ~10 rows, not 20k.
//!
//! Measured (~20k rows, this machine): **Push ≈ 73 µs** (early `Stop` reaches
//! the engine, ~10 rows read) vs **Volcano ≈ 17 ms** (`SeqScan` still eagerly
//! collects all 20k — making it lazy is a separate lever; the iterator would
//! be self-referential with the table handle, see `seq_scan.rs`). So push is
//! ~235× faster *because* it early-terminates the scan. Before the lazy-scan
//! lever this was Volcano ≈ 24 ms / Push ≈ 20 ms (~20%): the MVCC layer
//! buffered the whole range up front, so `Stop` saved only executor-level
//! tuple building, never the engine read. The lever unlocked the rest.
//!
//! Run: `cargo bench --bench push_vs_volcano`

use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::ExecModel;
use interchangedb::session::Session;
use interchangedb::storage::FileDiskManager;
use interchangedb::Database;
use tempfile::TempDir;

const ROWS: i32 = 20_000;

/// A session over a `t(id, v)` table seeded with `ROWS` rows, loaded inside one
/// transaction so the load isn't fsync-bound.
fn seeded_session() -> (Session<BTreeEngine>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("t.db")).unwrap();
    let bpm = BufferPoolManager::new(4096, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database = Arc::new(Database::open(dir.path(), engine).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    let mut session = Session::new(database, catalog);

    session
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)")
        .unwrap();
    session.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        session
            .execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    session.execute("COMMIT").unwrap();
    (session, dir)
}

fn scan_limit(c: &mut Criterion) {
    let (mut session, _dir) = seeded_session();
    let sql = "SELECT * FROM t LIMIT 10";

    let mut group = c.benchmark_group("scan_limit_10");
    // The scan dominates; a few samples are enough to separate the two models.
    group.measurement_time(Duration::from_secs(5));
    for model in [ExecModel::Volcano, ExecModel::Push] {
        let name = model.name();
        session.set_execution_model(model);
        group.bench_function(name, |b| {
            b.iter(|| black_box(session.execute(black_box(sql)).unwrap()));
        });
    }
    group.finish();
}

criterion_group!(benches, scan_limit);
criterion_main!(benches);
