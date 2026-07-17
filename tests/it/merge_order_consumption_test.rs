//! T17-B.3 — ORDER BY consumption through merge-join order delivery.
//!
//! When the memo's root winner is searched under the query's ORDER BY
//! requirement and delivers it (a merge join passes its left input's
//! order through), emission skips the spine Sort — the §7.5
//! interesting-orders payoff.
//!
//! Under the default cost model Sort+Merge never beats Hash (it is Hash
//! plus two n·log n sorts), so the test drives `VolcanoPlanner` with a
//! hash/NLJ-hostile cost model that makes Merge the winner, then checks:
//!   1. EXPLAIN has NO Sort above the MergeJoin — the Sorts below it are
//!      the children's enforcers, not the spine;
//!   2. rows come back in ORDER BY order and, as a multiset, equal the
//!      rule-based planner's output (tie order within equal keys is not
//!      pinned — both plans satisfy the same ORDER BY).

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::ExecModel;
use interchangedb::layout::RowLayout;
use interchangedb::sql::optimizer::cost::{Cost, CostModel, DefaultCostModel};
use interchangedb::sql::{
    parse, Binder, PhysicalPlan, PlannerStrategy, RuleBasedPlanner, VolcanoPlanner,
};
use interchangedb::storage::FileDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Value};

/// Delegates everywhere except hash and nested-loop joins, which it
/// makes ruinous — forcing the memo to a Merge plan without touching
/// the search machinery.
struct MergeFriendlyModel(DefaultCostModel);

impl CostModel for MergeFriendlyModel {
    fn cost_seq_scan(&self, p: f64, r: f64) -> Cost {
        self.0.cost_seq_scan(p, r)
    }
    fn cost_index_scan(&self, m: f64) -> Cost {
        self.0.cost_index_scan(m)
    }
    fn cost_filter(&self, r: f64) -> Cost {
        self.0.cost_filter(r)
    }
    fn cost_projection(&self, r: f64) -> Cost {
        self.0.cost_projection(r)
    }
    fn cost_limit(&self, r: f64) -> Cost {
        self.0.cost_limit(r)
    }
    fn cost_sort(&self, r: f64) -> Cost {
        self.0.cost_sort(r)
    }
    fn cost_nested_loop_join(&self, _o: f64, _i: f64) -> Cost {
        Cost {
            io_units: 1e9,
            cpu_units: 0.0,
        }
    }
    fn cost_index_nested_loop_join(&self, o: f64, m: f64) -> Cost {
        self.0.cost_index_nested_loop_join(o, m)
    }
    fn cost_hash_join(&self, _b: f64, _p: f64) -> Cost {
        Cost {
            io_units: 1e9,
            cpu_units: 0.0,
        }
    }
    fn cost_merge_join(&self, l: f64, r: f64) -> Cost {
        self.0.cost_merge_join(l, r)
    }
    fn scalar(&self, c: Cost) -> f64 {
        self.0.scalar(c)
    }
}

struct Env {
    engine: Arc<BTreeEngine>,
    catalog: Arc<Catalog<BTreeEngine>>,
    _dir: tempfile::TempDir,
}

fn int32_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty: ColumnType::Int32,
        nullable: false,
        default: None,
    }
}

/// t1(a_id PK, a_key) ⋈ t2(b_id PK, b_key) on the keys; duplicate keys
/// on both sides so equal-key runs exercise the merge, plus one
/// unpartnered key per side.
fn setup() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let engine = Arc::new(BTreeEngine::new(BufferPoolManager::new(256, dm)).unwrap());
    let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());

    for (name, id_col, key_col) in [("t1", "a_id", "a_key"), ("t2", "b_id", "b_key")] {
        catalog
            .create_table(
                name.into(),
                Schema {
                    name: name.into(),
                    table_id: TableId(0),
                    columns: vec![int32_col(id_col), int32_col(key_col)],
                    primary_key: vec![0],
                },
            )
            .unwrap();
    }
    let t1_schema = catalog.get_table("t1").unwrap();
    let t1 = Table::with_indexes(engine.clone(), t1_schema, RowLayout, Vec::new());
    for (id, key) in [(1, 5), (2, 3), (3, 5), (4, 1)] {
        t1.insert(&[Value::Int32(id), Value::Int32(key)]).unwrap();
    }
    let t2_schema = catalog.get_table("t2").unwrap();
    let t2 = Table::with_indexes(engine.clone(), t2_schema, RowLayout, Vec::new());
    for (id, key) in [(10, 5), (11, 3), (12, 7), (13, 5)] {
        t2.insert(&[Value::Int32(id), Value::Int32(key)]).unwrap();
    }

    Env {
        engine,
        catalog,
        _dir: dir,
    }
}

const QUERY: &str = "SELECT a_key, b_id FROM t1 JOIN t2 ON b_key = a_key ORDER BY a_key";

fn plan_with(env: &Env, planner: &impl PlannerStrategy) -> interchangedb::sql::PhysOp {
    let stmts = parse(QUERY).unwrap();
    let logical = Binder::new(env.catalog.clone())
        .bind(stmts.into_iter().next().unwrap())
        .unwrap();
    match planner.plan(logical, &env.catalog).unwrap() {
        PhysicalPlan::Query(op) => op,
        other => panic!("expected Query, got {:?}", std::mem::discriminant(&other)),
    }
}

fn run(env: &Env, op: &interchangedb::sql::PhysOp) -> Vec<Vec<Value>> {
    let (_schema, rows) = ExecModel::Volcano
        .execute(op, &env.engine, &env.catalog)
        .unwrap();
    rows
}

#[test]
fn order_by_consumed_by_merge_delivery() {
    let env = setup();

    // Rule-based keeps the spine Sort — the contrast baseline.
    let rule_op = plan_with(&env, &RuleBasedPlanner);
    assert!(
        rule_op.explain(0).contains("Sort[1 ASC]"),
        "rule-based should sort in the spine:\n{}",
        rule_op.explain(0)
    );

    // Merge-friendly memo: the root delivers ORDER BY a_key through the
    // merge, so no Sort sits above the join — the two Sorts below it
    // are the children's enforcers.
    let memo_planner = VolcanoPlanner {
        cost_model: MergeFriendlyModel(DefaultCostModel::new()),
    };
    let memo_op = plan_with(&env, &memo_planner);
    let expected = "\
Projection([1, 2])
  MergeJoin(left_key=1, right_key=1)
    Sort[1 ASC]
      SeqScan(t1)
    Sort[1 ASC]
      SeqScan(t2)
";
    assert_eq!(memo_op.explain(0), expected);

    // Correctness: rows arrive in ORDER BY order…
    let memo_rows = run(&env, &memo_op);
    assert!(!memo_rows.is_empty());
    for pair in memo_rows.windows(2) {
        assert!(
            pair[0][0].compare_sql(&pair[1][0]) != Some(std::cmp::Ordering::Greater),
            "a_key out of order: {pair:?}"
        );
    }
    // …and as a multiset equal the rule-based result (tie order within
    // equal keys is not part of the ORDER BY contract).
    let sorted = |mut rows: Vec<Vec<Value>>| {
        rows.sort_by_key(|r| format!("{r:?}"));
        rows
    };
    assert_eq!(sorted(memo_rows), sorted(run(&env, &rule_op)));
}
