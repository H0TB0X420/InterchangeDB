//! H4a — derived tables (FROM-subqueries, uncorrelated) end to end.
//!
//! The acceptance gate is TPC-H Q13 run FULL VERBATIM over a hand-built
//! customer/orders fixture, plus a Q7-shaped query whose derived table joins
//! two tables and groups by an aliased EXTRACT(YEAR …) column. Both exercise
//! the whole path: bind the inner query, materialize it as a `DerivedScan`
//! leaf, and resolve outer WHERE / GROUP BY / aggregates over the derived
//! columns.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::sql::{parse, Binder, LogicalPlan};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::{ColumnType, Value};
use interchangedb::wal::SyncMode;

use crate::common::mock_catalog::MockCatalog;

fn session() -> (Session<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database =
        Arc::new(Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    (Session::new(database, catalog), dir)
}

fn rows(session: &mut Session<BTreeEngine>, sql: &str) -> Vec<Vec<Value>> {
    match session.execute(sql).unwrap() {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows for {sql}, got {other:?}"),
    }
}

fn explain(session: &mut Session<BTreeEngine>, sql: &str) -> String {
    match session.execute(&format!("EXPLAIN {sql}")).unwrap() {
        QueryResult::Explain(text) => text,
        other => panic!("expected Explain for {sql}, got {other:?}"),
    }
}

/// Build the Q13 customer/orders fixture (see the module-level result table).
fn q13_fixture(s: &mut Session<BTreeEngine>) {
    s.execute("CREATE TABLE customer (c_custkey INT PRIMARY KEY)")
        .unwrap();
    s.execute(
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT, o_comment VARCHAR(40))",
    )
    .unwrap();
    for k in 1..=5 {
        s.execute(&format!("INSERT INTO customer VALUES ({k})"))
            .unwrap();
    }
    for sql in [
        // cust 1: two normal orders.
        "INSERT INTO orders VALUES (10, 1, 'regular order')",
        "INSERT INTO orders VALUES (11, 1, 'quick delivery')",
        // cust 2: one normal + one special-requests (the latter excluded by ON).
        "INSERT INTO orders VALUES (12, 2, 'normal')",
        "INSERT INTO orders VALUES (13, 2, 'has special requests inside')",
        // cust 3: only a special-requests order → excluded → padded → count 0.
        "INSERT INTO orders VALUES (14, 3, 'special requests here')",
        // cust 4: no orders at all → padded → count 0.
        // cust 5: three normal orders.
        "INSERT INTO orders VALUES (15, 5, 'order a')",
        "INSERT INTO orders VALUES (16, 5, 'order b')",
        "INSERT INTO orders VALUES (17, 5, 'order c')",
    ] {
        s.execute(sql).unwrap();
    }
}

const Q13: &str = "SELECT c_count, COUNT(*) AS custdist \
     FROM ( \
       SELECT c_custkey, COUNT(o_orderkey) \
       FROM customer LEFT OUTER JOIN orders \
         ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' \
       GROUP BY c_custkey \
     ) AS c_orders (c_custkey, c_count) \
     GROUP BY c_count \
     ORDER BY COUNT(*) DESC, c_count DESC";

// NOTE (Q13 verbatim deviation): the spec's ORDER BY references the output
// alias `custdist`; ORDER BY output-aliases are not supported by the binder,
// so we use the equivalent `ORDER BY COUNT(*) DESC` (custdist IS COUNT(*)).
// Every other clause — the derived table, its column-list alias
// `(c_custkey, c_count)`, the LEFT OUTER JOIN with the compound NOT LIKE ON,
// and the outer GROUP BY — is verbatim.
#[test]
fn q13_full_verbatim_acceptance() {
    let (mut s, _dir) = session();
    q13_fixture(&mut s);

    // Hand-computed inner (c_custkey, c_count): 1→2, 2→1, 3→0, 4→0, 5→3.
    // Outer GROUP BY c_count → custdist: 0→2, 1→1, 2→1, 3→1.
    // ORDER BY custdist DESC, c_count DESC.
    let expected = vec![
        vec![Value::Int64(0), Value::Int64(2)],
        vec![Value::Int64(3), Value::Int64(1)],
        vec![Value::Int64(2), Value::Int64(1)],
        vec![Value::Int64(1), Value::Int64(1)],
    ];
    assert_eq!(rows(&mut s, Q13), expected);
}

// PROBE: print the EXPLAIN tree so the slt goldens capture the exact
// DerivedScan rendering. Kept as a real assertion (the header + the inner
// subtree's presence) rather than a bare print.
#[test]
fn q13_explain_shape() {
    let (mut s, _dir) = session();
    q13_fixture(&mut s);
    let plan = explain(&mut s, Q13);
    eprintln!("=== Q13 EXPLAIN ===\n{plan}=== end ===");
    assert!(plan.contains("DerivedScan(c_orders)"), "{plan}");
}

// A Q7-shaped query: the derived table JOINS two tables and computes an
// aliased EXTRACT(YEAR …) column; the outer query groups by that year and
// SUMs. Proves an inner join under the derived boundary + a computed alias
// (`yr`) becoming a plain outer group key.
#[test]
fn q7_shaped_derived_join_group_by_year() {
    let (mut s, _dir) = session();
    s.execute("CREATE TABLE lineitem (l_orderkey INT PRIMARY KEY, l_shipdate DATE, l_amount INT)")
        .unwrap();
    s.execute("CREATE TABLE orders (o_orderkey INT PRIMARY KEY)")
        .unwrap();
    for sql in [
        "INSERT INTO lineitem VALUES (1, DATE '1995-03-15', 100)",
        "INSERT INTO lineitem VALUES (2, DATE '1995-07-20', 200)",
        "INSERT INTO lineitem VALUES (3, DATE '1996-01-10', 300)",
        "INSERT INTO lineitem VALUES (4, DATE '1996-11-05', 400)",
        // order 5 has no matching orders row → dropped by the inner join.
        "INSERT INTO lineitem VALUES (5, DATE '1997-02-02', 500)",
        "INSERT INTO orders VALUES (1)",
        "INSERT INTO orders VALUES (2)",
        "INSERT INTO orders VALUES (3)",
        "INSERT INTO orders VALUES (4)",
    ] {
        s.execute(sql).unwrap();
    }

    let q7 = "SELECT yr, SUM(amount) \
        FROM ( \
          SELECT EXTRACT(YEAR FROM l_shipdate) AS yr, l_amount AS amount \
          FROM lineitem JOIN orders ON l_orderkey = o_orderkey \
        ) AS shipping \
        GROUP BY yr";

    // Inner (yr, amount): 1995→{100,200}, 1996→{300,400}; order 5 dropped.
    // Outer GROUP BY yr, SUM(amount): 1995→300, 1996→700 (canonical key order).
    let expected = vec![
        vec![Value::Int32(1995), Value::Int64(300)],
        vec![Value::Int32(1996), Value::Int64(700)],
    ];
    assert_eq!(rows(&mut s, q7), expected);
}

// H4a nullability: a derived table over a LEFT OUTER JOIN must report its
// outer-join-padded RIGHT columns as nullable. The executor pads unmatched
// right rows with NULL (`concat_schemas` ORs a `right_nullable` flag into each
// right column), so the bound derived schema has to agree — else a consumer
// reads `nullable: false` for a column the runtime can hand back NULL in.
//
// Method: bind `SELECT *` over `l LEFT OUTER JOIN r` (a `*` derived subquery
// returns the join-tuple input schema verbatim, so this isolates the binder's
// join-kind-aware input concat) and inspect the bound `DerivedTable.schema`
// directly — the most direct surface for the nullability the fix sets.
#[test]
fn derived_schema_over_left_outer_join_marks_right_columns_nullable() {
    let mc = MockCatalog::new()
        .with_table("l", &[("l_id", ColumnType::Int32, false)], &["l_id"])
        .with_table(
            "r",
            &[
                ("r_id", ColumnType::Int32, false),
                ("r_val", ColumnType::Int32, false),
            ],
            &["r_id"],
        );
    let binder = Binder::new(mc.catalog.clone());

    let sql = "SELECT * FROM (SELECT * FROM l LEFT OUTER JOIN r ON l_id = r_id) AS d";
    let stmt = parse(sql).unwrap().into_iter().next().unwrap();
    let plan = binder.bind(stmt).unwrap();

    let derived = match &plan {
        LogicalPlan::Select { derived, .. } => derived,
        other => panic!("expected Select carrying a derived table, got {other:?}"),
    };
    assert_eq!(derived.len(), 1, "one derived table in the plan");
    let cols = &derived[0].schema;

    let nullable = |name: &str| -> bool {
        cols.iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("column '{name}' in derived schema"))
            .nullable
    };
    // Left column (l_id) is never padded → stays NOT NULL. Both right columns
    // (r_id, r_val), declared NOT NULL, become nullable through the LOJ.
    assert!(!nullable("l_id"), "left column must stay non-nullable");
    assert!(nullable("r_id"), "LOJ-padded right key must be nullable");
    assert!(
        nullable("r_val"),
        "LOJ-padded right column must be nullable"
    );
}
