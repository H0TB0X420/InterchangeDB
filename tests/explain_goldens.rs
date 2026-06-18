//! Goldens for every supported `PhysicalPlan` shape (Q-11). Locks down plan
//! structure before Phase 14 (Selinger) starts rewriting it. When any of
//! these goldens fails, the diff in `pretty_plan` output is exactly the
//! shape change to review — intentional changes update the goldens, drift
//! changes get rolled back.
//!
//! Consumers: this file. Adding a new operator or rewrite rule in Phase 12+
//! means adding a golden here too.
//!
//! Scaffolding from Q-02: `tests/common/mock_catalog.rs`,
//! `tests/common/golden_plan.rs`.

mod common;

use common::golden_plan::assert_plan_matches;
use common::mock_catalog::MockCatalog;
use interchangedb::sql::{parse, plan, Binder, PhysicalPlan};
use interchangedb::types::ColumnType;

/// Parse + bind + plan a single SQL statement against `mc`. Panics on any
/// error — goldens shouldn't be exercising error paths.
fn plan_sql(mc: &MockCatalog, sql: &str) -> PhysicalPlan {
    let stmts = parse(sql).expect("parse failed");
    let binder = Binder::new(mc.catalog.clone());
    let logical = binder
        .bind(stmts.into_iter().next().expect("empty parse result"))
        .expect("bind failed");
    plan(logical, &mc.catalog).expect("plan failed")
}

fn fresh_catalog() -> MockCatalog {
    MockCatalog::new().with_table(
        "t",
        &[
            ("id", ColumnType::Int32, false),
            ("name", ColumnType::Varchar(50), true),
            ("age", ColumnType::Int32, true),
        ],
        &["id"],
    )
}

// ---- DDL ----

#[test]
fn golden_create_table() {
    let mc = MockCatalog::new();
    let p = plan_sql(
        &mc,
        "CREATE TABLE t2 (id INT PRIMARY KEY, name VARCHAR(50))",
    );
    assert_plan_matches(&p, "CreateTable(t2)");
}

// ---- Transaction control ----

#[test]
fn golden_begin_txn() {
    let mc = MockCatalog::new();
    let p = plan_sql(&mc, "BEGIN");
    assert_plan_matches(&p, "BeginTxn");
}

#[test]
fn golden_commit_txn() {
    let mc = MockCatalog::new();
    let p = plan_sql(&mc, "COMMIT");
    assert_plan_matches(&p, "CommitTxn");
}

#[test]
fn golden_rollback_txn() {
    let mc = MockCatalog::new();
    let p = plan_sql(&mc, "ROLLBACK");
    assert_plan_matches(&p, "AbortTxn");
}

// ---- SELECT shapes ----

#[test]
fn golden_select_star_is_bare_seqscan() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "SELECT * FROM t");
    assert_plan_matches(&p, "SeqScan(t)");
}

#[test]
fn golden_select_projection_wraps_seqscan() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "SELECT id FROM t");
    assert_plan_matches(
        &p,
        r#"
            Projection([0])
              SeqScan(t)
        "#,
    );
}

#[test]
fn golden_select_filter_then_projection() {
    let mc = fresh_catalog();
    // Non-PK predicate (age) keeps the scan + filter shape.
    let p = plan_sql(&mc, "SELECT id FROM t WHERE age = 1");
    assert_plan_matches(
        &p,
        r#"
            Projection([0])
              Filter
                SeqScan(t)
        "#,
    );
}

#[test]
fn golden_select_pk_equality_is_pk_lookup() {
    let mc = fresh_catalog();
    // id is the PK → a point lookup replaces scan + filter.
    let p = plan_sql(&mc, "SELECT id FROM t WHERE id = 1");
    assert_plan_matches(
        &p,
        r#"
            Projection([0])
              PkLookup(t)
        "#,
    );
}

#[test]
fn golden_select_full_chain_limit_projection_filter_scan() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "SELECT id FROM t WHERE age = 1 LIMIT 3");
    assert_plan_matches(
        &p,
        r#"
            Limit(3)
              Projection([0])
                Filter
                  SeqScan(t)
        "#,
    );
}

#[test]
fn golden_select_multi_column_projection() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "SELECT id, name FROM t");
    assert_plan_matches(
        &p,
        r#"
            Projection([0, 1])
              SeqScan(t)
        "#,
    );
}

// ---- INSERT ----

#[test]
fn golden_insert_single_row() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "INSERT INTO t VALUES (1, 'a', 20)");
    assert_plan_matches(&p, "Insert(1 rows → t)");
}

#[test]
fn golden_insert_multi_row() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "INSERT INTO t VALUES (1, 'a', 20), (2, 'b', 30)");
    assert_plan_matches(&p, "Insert(2 rows → t)");
}

// ---- UPDATE ----

#[test]
fn golden_update_with_filter() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "UPDATE t SET name = 'x' WHERE age = 1");
    assert_plan_matches(
        &p,
        r#"
            Update(t, set_cols=[1])
              Filter
                SeqScan(t)
        "#,
    );
}

#[test]
fn golden_update_without_filter() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "UPDATE t SET name = 'x'");
    assert_plan_matches(
        &p,
        r#"
            Update(t, set_cols=[1])
              SeqScan(t)
        "#,
    );
}

// ---- DELETE ----

#[test]
fn golden_delete_with_filter() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "DELETE FROM t WHERE age = 1");
    assert_plan_matches(
        &p,
        r#"
            Delete(t)
              Filter
                SeqScan(t)
        "#,
    );
}

#[test]
fn golden_delete_without_filter() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "DELETE FROM t");
    assert_plan_matches(
        &p,
        r#"
            Delete(t)
              SeqScan(t)
        "#,
    );
}

// ---- EXPLAIN ----
//
// EXPLAIN wraps the rendered inner plan in `PhysicalPlan::Explain(String)`.
// `pretty_plan` passes through that string verbatim, so the golden is the
// inner plan's rendered form.

#[test]
fn golden_explain_wraps_select_chain() {
    let mc = fresh_catalog();
    let p = plan_sql(&mc, "EXPLAIN SELECT id FROM t WHERE age = 1");
    assert_plan_matches(
        &p,
        r#"
            Projection([0])
              Filter
                SeqScan(t)
        "#,
    );
}

#[test]
fn golden_explain_wraps_create_table() {
    let mc = MockCatalog::new();
    let p = plan_sql(&mc, "EXPLAIN CREATE TABLE t2 (id INT PRIMARY KEY)");
    assert_plan_matches(&p, "CreateTable(t2)");
}
