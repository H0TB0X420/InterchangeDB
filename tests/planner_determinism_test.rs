//! Q-20: planner determinism — the same logical plan must produce the
//! same physical plan, byte-for-byte, on every run.
//!
//! Why this matters: the workload-log fingerprinter (Phase 11) and plan
//! store (Phase 19) both hash the physical plan. If plan rendering is
//! non-deterministic (HashMap iteration order, allocator addresses,
//! timestamps, anything) those hashes collide across runs and the cache
//! is wasted. Catching non-determinism here is much cheaper than
//! debugging cache misses in Phase 19.
//!
//! Mechanism: parse + bind + plan the same SQL N times, compare the
//! deterministic `pretty_plan` output across all runs. Run on identical
//! catalog setups; do NOT share a catalog (a fresh setup per iteration
//! checks that table_id assignment / column_id assignment / any other
//! sequential allocation doesn't leak into the rendered plan).

mod common;

use common::golden_plan::pretty_plan;
use common::mock_catalog::MockCatalog;
use interchangedb::sql::{parse, plan, Binder, PhysicalPlan};
use interchangedb::types::ColumnType;

fn render_once(sql: &str) -> String {
    let mc = MockCatalog::new().with_table(
        "t",
        &[
            ("id", ColumnType::Int32, false),
            ("name", ColumnType::Varchar(50), true),
            ("age", ColumnType::Int32, true),
        ],
        &["id"],
    );
    let stmts = parse(sql).unwrap();
    let binder = Binder::new(mc.catalog.clone());
    let logical = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
    let p: PhysicalPlan = plan(logical, &mc.catalog).unwrap();
    pretty_plan(&p)
}

fn assert_deterministic(sql: &str, runs: usize) {
    let first = render_once(sql);
    for i in 1..runs {
        let next = render_once(sql);
        assert_eq!(
            first, next,
            "non-deterministic plan rendering for SQL: {}\n--- run 0 ---\n{}\n--- run {} ---\n{}",
            sql, first, i, next
        );
    }
}

#[test]
fn select_star_is_deterministic() {
    assert_deterministic("SELECT * FROM t", 10);
}

#[test]
fn select_with_projection_is_deterministic() {
    assert_deterministic("SELECT id, name FROM t", 10);
}

#[test]
fn select_with_filter_and_limit_is_deterministic() {
    assert_deterministic("SELECT id FROM t WHERE id = 1 LIMIT 5", 10);
}

#[test]
fn insert_multi_row_is_deterministic() {
    assert_deterministic(
        "INSERT INTO t VALUES (1, 'a', 20), (2, 'b', 30), (3, 'c', 40)",
        10,
    );
}

#[test]
fn update_with_filter_is_deterministic() {
    assert_deterministic("UPDATE t SET name = 'x' WHERE id = 1", 10);
}

#[test]
fn delete_with_filter_is_deterministic() {
    assert_deterministic("DELETE FROM t WHERE id = 1", 10);
}

#[test]
fn explain_is_deterministic() {
    assert_deterministic("EXPLAIN SELECT id FROM t WHERE id = 1", 10);
}

#[test]
fn ddl_is_deterministic() {
    assert_deterministic("CREATE TABLE u (id INT PRIMARY KEY, val VARCHAR(20))", 10);
}

#[test]
fn transaction_control_is_deterministic() {
    assert_deterministic("BEGIN", 5);
    assert_deterministic("COMMIT", 5);
    assert_deterministic("ROLLBACK", 5);
}

// Cross-instance determinism: two MockCatalogs created independently must
// still produce identical plans for the same SQL. Catches any reliance on
// pointer identity or per-instance counters that leak into rendering.
#[test]
fn deterministic_across_independent_catalog_instances() {
    let sql = "SELECT id, name FROM t WHERE id = 1 LIMIT 3";

    let plans: Vec<String> = (0..5)
        .map(|_| render_once(sql)) // each call builds a fresh catalog
        .collect();

    let first = &plans[0];
    for (i, p) in plans.iter().enumerate().skip(1) {
        assert_eq!(
            first, p,
            "instance {} produced a different plan:\n--- first ---\n{}\n--- this ---\n{}",
            i, first, p
        );
    }
}
