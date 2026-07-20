//! TPC-H harness smoke test (Phase H5).
//!
//! A FAST sanity gate for the committed `queries/tpch/*.sql` assets: build a
//! tiny TPC-H-shaped fixture, load it, and run three representative queries
//! (Q1 — grouped aggregation over lineitem; Q13 — LEFT OUTER JOIN inside a
//! derived table; Q22 — SUBSTRING + scalar + correlated NOT EXISTS inside a
//! derived table) across all six planner × execution-model configurations,
//! asserting each returns a non-empty result of the right arity.
//!
//! This is deliberately NOT an oracle diff — validating the query *answers*
//! against DuckDB at SF 0.01 across the whole matrix is the `tpch` binary's
//! job (`cargo run --bin tpch -- --validate`). Keeping the fixture tiny keeps
//! the `it` suite fast. The queries here are the exact committed files, so a
//! regression that makes any of them stop planning/executing fails here.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::ExecModel;
use interchangedb::session::{QueryResult, Session};
use interchangedb::sql::{Planner, RuleBasedPlanner, SelingerPlanner, VolcanoPlanner};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::wal::SyncMode;

fn session() -> (Session<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(1024, dm);
    // Small leaf fan-out keeps the wide lineitem row inside a 4 KB page (the
    // `tpch` binary derives this from row width; a fixed 16 is safe here).
    let engine = BTreeEngine::with_sizes(bpm, 16, 64, 0).unwrap();
    let database =
        Arc::new(Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    (Session::new(database, catalog), dir)
}

/// Full-schema subset of the three tables Q1/Q13/Q22 touch (columns match the
/// binary's `SCHEMA` so the committed query files bind unchanged).
const CREATE: [&str; 3] = [
    "CREATE TABLE customer (\
        c_custkey INT PRIMARY KEY, c_name VARCHAR(25), c_address VARCHAR(40), \
        c_nationkey INT, c_phone CHAR(15), c_acctbal DECIMAL(12,2), \
        c_mktsegment CHAR(10), c_comment VARCHAR(117))",
    "CREATE TABLE orders (\
        o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus CHAR(1), \
        o_totalprice DECIMAL(12,2), o_orderdate DATE, o_orderpriority CHAR(15), \
        o_clerk CHAR(15), o_shippriority INT, o_comment VARCHAR(79))",
    "CREATE TABLE lineitem (\
        l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, \
        l_quantity DECIMAL(12,2), l_extendedprice DECIMAL(12,2), \
        l_discount DECIMAL(3,2), l_tax DECIMAL(3,2), l_returnflag CHAR(1), \
        l_linestatus CHAR(1), l_shipdate DATE, l_commitdate DATE, \
        l_receiptdate DATE, l_shipinstruct CHAR(25), l_shipmode CHAR(10), \
        l_comment VARCHAR(44), PRIMARY KEY (l_orderkey, l_linenumber))",
];

/// A hand-built micro-fixture chosen so Q1/Q13/Q22 each return rows:
/// - customers span the Q22 country codes ('13'/'17'), some above the AVG
///   threshold and order-free (survive Q22), one with an order (excluded);
/// - orders carry a "special ... requests" comment (excluded by Q13's ON) and
///   a plain one (kept);
/// - lineitem covers three returnflag/linestatus pairs (three Q1 groups).
fn seed(s: &mut Session<BTreeEngine>) {
    for sql in CREATE {
        s.execute(sql).unwrap();
    }
    for sql in [
        "INSERT INTO customer VALUES \
            (1,'Customer#1','a',3,'13-111-111-1111',900.00,'BUILDING','x'), \
            (2,'Customer#2','b',3,'13-222-222-2222',600.00,'BUILDING','x'), \
            (3,'Customer#3','c',7,'17-333-333-3333',800.00,'AUTOMOBILE','x'), \
            (4,'Customer#4','d',3,'13-444-444-4444',300.00,'FURNITURE','x'), \
            (6,'Customer#6','e',3,'13-666-666-6666',850.00,'HOUSEHOLD','x'), \
            (7,'Customer#7','f',9,'99-777-777-7777',9000.00,'MACHINERY','x'), \
            (8,'Customer#8','g',3,'13-888-888-8888',-100.00,'BUILDING','x')",
        "INSERT INTO orders VALUES \
            (10,1,'O',100.00,DATE '1995-01-01','1-URGENT','Clerk#1',0,'regular comment'), \
            (11,2,'O',100.00,DATE '1995-02-01','2-HIGH','Clerk#2',0,'has special big requests here'), \
            (12,6,'F',100.00,DATE '1995-03-01','3-MEDIUM','Clerk#3',0,'normal order')",
        "INSERT INTO lineitem VALUES \
            (10,1,1,1,17.00,1000.00,0.05,0.02,'N','O',DATE '1996-01-01',DATE '1996-02-01',DATE '1996-01-15','DELIVER IN PERSON','AIR','c'), \
            (10,2,1,2,20.00,2000.00,0.06,0.03,'R','F',DATE '1994-01-01',DATE '1994-02-01',DATE '1994-01-15','NONE','MAIL','c'), \
            (11,1,1,1,10.00,500.00,0.00,0.01,'A','F',DATE '1993-06-01',DATE '1993-07-01',DATE '1993-06-15','NONE','SHIP','c')",
    ] {
        s.execute(sql).unwrap();
    }
}

/// Read a committed query file, strip `--` comments and the trailing `;`.
fn load_query(id: u8) -> String {
    let path = format!("{}/queries/tpch/q{:02}.sql", env!("CARGO_MANIFEST_DIR"), id);
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut sql = String::new();
    for line in raw.lines() {
        let code = line.find("--").map(|p| &line[..p]).unwrap_or(line);
        sql.push_str(code);
        sql.push('\n');
    }
    sql.trim().trim_end_matches(';').trim().to_string()
}

fn rows(s: &mut Session<BTreeEngine>, sql: &str) -> Vec<Vec<interchangedb::types::Value>> {
    match s.execute(sql).unwrap() {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// The six planner × execution-model configurations.
fn all_configs() -> Vec<(Planner, ExecModel)> {
    let mut out = Vec::new();
    for exec in [ExecModel::Volcano, ExecModel::Push] {
        out.push((Planner::RuleBased(RuleBasedPlanner), exec));
        out.push((Planner::Selinger(SelingerPlanner::default()), exec));
        out.push((Planner::VolcanoMemo(VolcanoPlanner::default()), exec));
    }
    out
}

/// Q1/Q13/Q22 each return a non-empty result of the expected arity, under every
/// planner and execution model.
#[test]
fn tpch_representative_queries_run_across_all_configs() {
    let (mut s, _dir) = session();
    seed(&mut s);

    // (query id, expected column count).
    let checks = [(1u8, 10usize), (13u8, 2usize), (22u8, 3usize)];
    for (planner, exec) in all_configs() {
        s.set_planner(planner);
        s.set_execution_model(exec);
        for (id, cols) in checks {
            let sql = load_query(id);
            let out = rows(&mut s, &sql);
            assert!(
                !out.is_empty(),
                "Q{id:02} returned no rows under {} / {}",
                s.planner_name(),
                s.execution_model_name()
            );
            assert_eq!(
                out[0].len(),
                cols,
                "Q{id:02} arity mismatch under {} / {}",
                s.planner_name(),
                s.execution_model_name()
            );
        }
    }
}
