//! P14.8: Planner-comparison tests.
//!
//! Locks the V1 contract: every SQL shape we support produces the
//! same EXPLAIN tree under `RuleBasedPlanner` and `SelingerPlanner`.
//! When real Selinger DP enumeration ships, these assertions will
//! break for any query where a cheaper plan exists — surfacing the
//! divergence at the operator-tree level.
//!
//! What's covered here: SELECT *, SELECT with WHERE (filter), SELECT
//! with WHERE matching a PK (index-scan lowering), INSERT, UPDATE,
//! DELETE, ORDER BY, LIMIT, aggregates, two-table JOIN with ON, and
//! the DDL/TC descriptor variants. Every test runs the same parsed
//! statement through both strategies and compares the result.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::sql::binder::Binder;
use interchangedb::sql::frontend::parse;
use interchangedb::sql::{PhysicalPlan, PlannerStrategy, RuleBasedPlanner, SelingerPlanner};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::ColumnType;

struct Env {
    catalog: Arc<Catalog<BTreeEngine>>,
    binder: Binder<BTreeEngine>,
    _dir: tempfile::TempDir,
}

fn setup() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());
    let binder = Binder::new(catalog.clone());

    // warehouse(w_id PK, w_ytd)
    let warehouse = Schema {
        name: "warehouse".into(),
        table_id: TableId(0),
        columns: vec![
            ColumnDef {
                name: "w_id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_ytd".into(),
                ty: ColumnType::Int64,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    };
    catalog.create_table("warehouse".into(), warehouse).unwrap();

    // district(d_id PK, d_w_id, d_name)
    let district = Schema {
        name: "district".into(),
        table_id: TableId(0),
        columns: vec![
            ColumnDef {
                name: "d_id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "d_w_id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "d_name".into(),
                ty: ColumnType::Varchar(20),
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    };
    catalog.create_table("district".into(), district).unwrap();

    Env {
        catalog,
        binder,
        _dir: dir,
    }
}

/// Plan the same SQL through both strategies. For executor plans,
/// returns the rendered EXPLAIN trees. For descriptor plans (DDL/TC),
/// returns rendered placeholders that must match exactly.
fn plans_match(env: &Env, sql: &str) -> (String, String) {
    let stmts = parse(sql).unwrap();
    let a = env
        .binder
        .bind(stmts.clone().into_iter().next().unwrap())
        .unwrap();
    let b = env.binder.bind(stmts.into_iter().next().unwrap()).unwrap();

    let p_rule = RuleBasedPlanner.plan(a, &env.catalog).unwrap();
    let p_sel = SelingerPlanner::default().plan(b, &env.catalog).unwrap();

    (render(&p_rule), render(&p_sel))
}

fn render(p: &PhysicalPlan) -> String {
    match p {
        PhysicalPlan::Query(physop) => physop.explain(0),
        PhysicalPlan::CreateTable { name, .. } => format!("CreateTable({})", name),
        PhysicalPlan::Analyze { table } => format!("Analyze({})", table),
        PhysicalPlan::BeginTxn => "BeginTxn".into(),
        PhysicalPlan::CommitTxn => "CommitTxn".into(),
        PhysicalPlan::AbortTxn => "AbortTxn".into(),
        PhysicalPlan::Explain(s) => format!("Explain:{}", s),
    }
}

fn assert_match(env: &Env, sql: &str) {
    let (a, b) = plans_match(env, sql);
    assert_eq!(a, b, "planner divergence for SQL: {}", sql);
}

#[test]
fn select_star_matches() {
    let env = setup();
    assert_match(&env, "SELECT * FROM warehouse");
}

#[test]
fn select_with_where_matches() {
    let env = setup();
    assert_match(&env, "SELECT w_id FROM warehouse WHERE w_ytd = 100");
}

#[test]
fn select_with_pk_predicate_matches() {
    let env = setup();
    // w_id = 1 lowers to IndexScan on PK in RuleBased.
    assert_match(&env, "SELECT w_ytd FROM warehouse WHERE w_id = 1");
}

#[test]
fn select_with_limit_matches() {
    let env = setup();
    assert_match(&env, "SELECT w_id FROM warehouse WHERE w_id = 1 LIMIT 3");
}

#[test]
fn select_with_order_by_matches() {
    let env = setup();
    assert_match(&env, "SELECT w_id FROM warehouse ORDER BY w_ytd DESC");
}

#[test]
fn select_with_aggregate_matches() {
    let env = setup();
    assert_match(&env, "SELECT COUNT(*) FROM warehouse");
}

#[test]
fn both_planners_pick_hashjoin_for_unindexed_equijoin() {
    // The join is on `d_w_id`, which has no index. Selinger costs the
    // alternatives and picks HashJoin (linear vs quadratic). As of Phase D
    // (predicate-pushdown plan) the rule-based planner ALSO emits HashJoin for
    // an unindexed equi-key — it used to fall back to NestedLoopJoin (this test
    // formerly asserted that divergence). So the two planners now *converge*
    // here: same algorithm, same textual order, same Projection layout. They
    // can still diverge on cost-sensitive cases (a tiny inner where Selinger
    // prefers NestedLoop); this query is no longer one of them.
    let env = setup();
    let (rule_tree, selinger_tree) = plans_match(
        &env,
        "SELECT w_id, d_id FROM warehouse JOIN district ON w_id = d_w_id",
    );
    assert!(
        rule_tree.contains("HashJoin"),
        "rule-based should now use HashJoin (Phase D), got:\n{}",
        rule_tree
    );
    assert!(
        selinger_tree.contains("HashJoin"),
        "Selinger should use HashJoin, got:\n{}",
        selinger_tree
    );
    // Layout is unchanged: both wrap the join in the same Projection.
    assert!(rule_tree.starts_with("Projection([0, 2])"));
    assert!(selinger_tree.starts_with("Projection([0, 2])"));
}

#[test]
fn insert_matches() {
    let env = setup();
    assert_match(&env, "INSERT INTO warehouse VALUES (1, 1000)");
}

#[test]
fn update_with_where_matches() {
    let env = setup();
    assert_match(&env, "UPDATE warehouse SET w_ytd = 9999 WHERE w_id = 1");
}

#[test]
fn delete_with_where_matches() {
    let env = setup();
    assert_match(&env, "DELETE FROM warehouse WHERE w_id = 1");
}

#[test]
fn create_table_descriptor_matches() {
    let env = setup();
    assert_match(&env, "CREATE TABLE x (id INT PRIMARY KEY)");
}

#[test]
fn transaction_control_matches() {
    let env = setup();
    assert_match(&env, "BEGIN");
    assert_match(&env, "COMMIT");
    assert_match(&env, "ROLLBACK");
}

#[test]
fn explain_matches() {
    let env = setup();
    assert_match(&env, "EXPLAIN SELECT w_id FROM warehouse WHERE w_id = 1");
}

#[test]
fn planner_names_are_distinct() {
    // The two strategies must report distinct names so logs / EXPLAIN
    // headers can attribute plans to the right planner.
    assert_eq!(RuleBasedPlanner.name(), "rule-based");
    assert_eq!(SelingerPlanner::default().name(), "selinger");
}
