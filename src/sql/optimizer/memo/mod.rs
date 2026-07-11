//! Phase 17 — Volcano-style memo planner (17-A: parity core).
//!
//! Pipeline (module per stage, per `docs/phase_17_plan.md` §2):
//!
//! ```text
//!   LogicalPlan::Select
//!     │ normalize   — optimizable join/filter core split from the spine
//!     ▼
//!   NormalizedQuery
//!     │ memo        — groups keyed by relation-set bitmask (D2)
//!     │ search      — top-down branch-and-bound optimize() (§4/§5)
//!     ▼
//!   winner tree
//!     │ emit        — PhysOp + ColumnRemap + shared spine (§6)
//!     ▼
//!   PhysOp
//! ```
//!
//! [`VolcanoPlanner`] drives the pipeline as the third `PlannerStrategy`;
//! any shape the memo does not handle falls back to `plan_inner` with
//! cost-based join selection — byte-identical to `SelingerPlanner`'s
//! bail path (D8).

pub(crate) mod emit;
#[cfg(test)]
pub(crate) mod fixtures;
// CLIPPY-ALLOW(module_inception): `memo::memo` follows the plan §2 file
// layout; the inner module holds the Memo structure itself.
#[allow(clippy::module_inception)]
pub(crate) mod memo;
pub(crate) mod normalize;
pub(crate) mod props;
pub(crate) mod search;

use crate::catalog::Catalog;
use crate::common::Result;
use crate::sql::ir::logical::LogicalPlan;
use crate::sql::optimizer::cost::{CostModel, DefaultCostModel};
use crate::sql::optimizer::selinger::gather_query_stats;
use crate::sql::planner::{
    plan_inner, render_explain, JoinSelection, PhysicalPlan, PlannerStrategy,
};
use crate::storage::StorageEngine;

/// Phase 17 planner: memo + top-down branch-and-bound over the
/// join/filter/access-path core, bushy join space (D4), leaf lowering
/// shared with the rule-based planner (D6).
pub struct VolcanoPlanner<C: CostModel = DefaultCostModel> {
    pub cost_model: C,
}

impl Default for VolcanoPlanner<DefaultCostModel> {
    fn default() -> Self {
        VolcanoPlanner {
            cost_model: DefaultCostModel::default(),
        }
    }
}

impl<C: CostModel> PlannerStrategy for VolcanoPlanner<C> {
    fn plan<CatE>(&self, logical: LogicalPlan, catalog: &Catalog<CatE>) -> Result<PhysicalPlan>
    where
        CatE: StorageEngine,
    {
        // EXPLAIN renders the inner statement's memo plan. The parser
        // forbids nested EXPLAIN, so recursion depth is 1.
        if let LogicalPlan::Explain(inner) = logical {
            let inner_physical = self.plan(*inner, catalog)?;
            return Ok(PhysicalPlan::Explain(render_explain(&inner_physical)));
        }

        let stats = gather_query_stats(&logical, catalog)?;
        // Two bail points share the D8 fallback: normalize rejects the
        // statement shape, or the join graph is dense enough that the
        // memo's group cap would overflow (review fix #1 — a bail, never
        // a panic, on valid SQL).
        let core = normalize::normalize(&logical, catalog, &stats)?
            .and_then(|query| memo::Memo::try_new(&query).map(|memo| (query, memo)));
        match core {
            Some((query, mut memo)) => {
                let unordered = search::optimize_root(&mut memo, &query, &self.cost_model);

                // §7.5 ORDER BY consumption: when the ORDER BY maps to a
                // single relation, also search the root under that
                // requirement. NOTE (deviation from the plan's wording):
                // the ordered plan is emitted only when STRICTLY cheaper
                // than sorting the unordered winner — on a tie the
                // enforcer-at-root plan is the same plan in a different
                // coat, so the spine Sort stays (plan-shape stability).
                let mut root_required = None;
                if let Some(key) = props::order_by_requirement(&query) {
                    let ordered =
                        search::optimize_root_ordered(&mut memo, &query, &self.cost_model, &key);
                    let root_group_id = memo.group_id(memo::full_relset(query.relations.len()));
                    // Add the Costs, then scalarize ONCE — the trait does
                    // not promise `scalar` is linear, and every other
                    // comparison in the pipeline sums before ranking
                    // (review fix #3).
                    let sorted_unordered = self.cost_model.scalar(
                        unordered.add(
                            self.cost_model
                                .cost_sort(memo.group(root_group_id).cardinality),
                        ),
                    );
                    if self.cost_model.scalar(ordered) < sorted_unordered {
                        root_required = Some(key);
                    }
                }
                Ok(PhysicalPlan::Query(emit::emit(
                    &memo,
                    &query,
                    &root_required,
                )))
            }
            // D8 fallback — byte-identical to SelingerPlanner's bail
            // path: the untouched logical plan lowers through the shared
            // textual path with cost-based join selection.
            None => plan_inner(
                logical,
                catalog,
                &JoinSelection::CostBased {
                    cost_model: &self.cost_model,
                    stats: &stats,
                },
            ),
        }
    }

    fn name(&self) -> &'static str {
        "volcano-memo"
    }
}

/// D10 — marker for planners whose plans are reproducible baselines
/// (Phase 19's plan-store prerequisite). The actual property is
/// determinism — same input ⇒ identical EXPLAIN — pinned by
/// `tests/planner_three_way_test.rs`; the marker is the gate.
pub trait BaselineCapable {}

impl<C: CostModel> BaselineCapable for VolcanoPlanner<C> {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::Catalog;
    use crate::database::Database;
    use crate::engines::btree::BTreeEngine;
    use crate::session::Session;
    use crate::sql::binder::Binder;
    use crate::sql::frontend::parse;
    use crate::sql::optimizer::join_order::{cost_of_order, enumerate_join_orders, RelId};
    use crate::sql::optimizer::selinger::build_join_graph;
    use crate::storage::FileDiskManager;

    /// The selinger_reorder_test chain (a: 2 rows — b: 4 — c: 8), built
    /// through a real session so stats come from ANALYZE — the same
    /// inputs the Selinger planner sees.
    fn chain_env() -> (Arc<Catalog<BTreeEngine>>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
        let bpm = BufferPoolManager::new(512, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let database = Arc::new(Database::open(dir.path(), engine).unwrap());
        let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
        let mut session = Session::new(database.clone(), catalog.clone());

        session
            .execute("CREATE TABLE a (a_id INT PRIMARY KEY, a_val INT)")
            .unwrap();
        session
            .execute("CREATE TABLE b (b_id INT PRIMARY KEY, b_a INT, b_val INT)")
            .unwrap();
        session
            .execute("CREATE TABLE c (c_id INT PRIMARY KEY, c_b INT, c_val INT)")
            .unwrap();
        for a_id in 1..=2 {
            session
                .execute(&format!("INSERT INTO a VALUES ({}, {})", a_id, a_id * 100))
                .unwrap();
        }
        for b_id in 1..=4 {
            let b_a = (b_id - 1) % 2 + 1;
            session
                .execute(&format!(
                    "INSERT INTO b VALUES ({b_id}, {b_a}, {})",
                    b_id * 10
                ))
                .unwrap();
        }
        for c_id in 1..=8 {
            let c_b = (c_id - 1) % 4 + 1;
            session
                .execute(&format!("INSERT INTO c VALUES ({c_id}, {c_b}, {c_id})"))
                .unwrap();
        }
        for table in ["a", "b", "c"] {
            session.execute(&format!("ANALYZE TABLE {table}")).unwrap();
        }
        (catalog, dir)
    }

    /// Gate G3 (cost dominance): the memo's bushy space is a superset of
    /// Selinger's left-deep space and its cost formulas keep argument
    /// parity, so the memo winner's scalar cost can never exceed the
    /// cost of Selinger's chosen order (DP best if strictly cheaper than
    /// textual, else textual — `maybe_reorder`'s rule).
    #[test]
    fn g3_memo_winner_never_costs_more_than_selinger_order() {
        let (catalog, _dir) = chain_env();
        let binder = Binder::new(catalog.clone());
        let cost_model = DefaultCostModel::new();
        let queries = [
            "SELECT c_val, b_val, a_val FROM c JOIN b ON c_b = b_id JOIN a ON b_a = a_id",
            "SELECT c_val, b_val, a_val FROM a JOIN b ON b_a = a_id \
             JOIN c ON c_b = b_id WHERE c_val = 1",
        ];
        for sql in queries {
            let stmts = parse(sql).unwrap();
            let logical = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
            let stats = gather_query_stats(&logical, &catalog).unwrap();

            let query = normalize::normalize(&logical, &catalog, &stats)
                .unwrap()
                .unwrap();
            let mut m = memo::Memo::try_new(&query).expect("under the group cap");
            let memo_scalar = cost_model.scalar(search::optimize_root(&mut m, &query, &cost_model));

            let LogicalPlan::Select {
                table,
                joins,
                filter,
                ..
            } = &logical
            else {
                panic!("expected SELECT")
            };
            let graph = build_join_graph(table, joins, filter.as_ref(), &catalog, &stats)
                .unwrap()
                .unwrap();
            let best =
                enumerate_join_orders(&graph.relations, &graph.edges, &stats, &cost_model, None);
            let textual: Vec<RelId> = (0..graph.relations.len()).collect();
            let selinger_scalar = match cost_of_order(
                &textual,
                &graph.relations,
                &graph.edges,
                &stats,
                &cost_model,
            ) {
                Some(textual_cost) => cost_model
                    .scalar(best.cost)
                    .min(cost_model.scalar(textual_cost)),
                None => cost_model.scalar(best.cost),
            };

            assert!(
                memo_scalar <= selinger_scalar + 1e-9,
                "G3 violated on `{sql}`: memo {memo_scalar} > selinger {selinger_scalar}"
            );
        }
    }

    /// Review fix #1 regression: a 12-spoke star join is valid SQL whose
    /// connected-subset count (2^12 + 12) exceeds GROUP_COUNT_MAX. The
    /// planner must produce a plan via the D8 fallback — the original
    /// lazy-creation memo panicked mid-search on exactly this shape.
    #[test]
    fn dense_join_graph_falls_back_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
        let bpm = BufferPoolManager::new(512, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let database = Arc::new(Database::open(dir.path(), engine).unwrap());
        let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
        let mut session = Session::new(database.clone(), catalog.clone());

        let hub_columns: String = (1..=12).map(|k| format!(", h{k} INT")).collect();
        session
            .execute(&format!(
                "CREATE TABLE hub (h_id INT PRIMARY KEY{hub_columns})"
            ))
            .unwrap();
        let mut sql = String::from("SELECT h_id FROM hub");
        for k in 1..=12 {
            session
                .execute(&format!(
                    "CREATE TABLE s{k} (s{k}_id INT PRIMARY KEY, s{k}_h INT)"
                ))
                .unwrap();
            sql += &format!(" JOIN s{k} ON s{k}_h = h{k}");
        }

        let binder = Binder::new(catalog.clone());
        let stmts = parse(&sql).unwrap();
        let logical = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
        let plan = VolcanoPlanner::default().plan(logical, &catalog).unwrap();
        assert!(
            matches!(plan, PhysicalPlan::Query(_)),
            "dense star must plan through the fallback"
        );
    }
}
