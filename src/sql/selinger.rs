//! P14.7: Selinger-style cost-based planner (scaffolding).
//!
//! This file establishes the `SelingerPlanner` struct as a second
//! `PlannerStrategy` impl. The strategy carries the two knobs a real
//! Selinger optimizer needs — a `CostModel` and a `time_budget_ms` —
//! so callers and tests can wire interchangeability today.
//!
//! ## What this version does NOT do
//!
//! Real Selinger DP enumerates subsets of base tables and considers
//! every join order. We can't safely reorder joins yet: predicates
//! carry tuple-global column indices (assigned by the binder against
//! the left-to-right scan order), so a permuted join would invalidate
//! every index in every predicate. The follow-up that introduces
//! table-relative predicates will unlock real enumeration; until
//! then, this planner produces the same tree the rule-based planner
//! does, and the `PlannerStrategy` boundary is what's exercised.
//!
//! ## Why ship the scaffold now
//!
//! It locks the *interchangeability promise* end-to-end: the session
//! can choose `RuleBasedPlanner` or `SelingerPlanner` at compile
//! time, and the planner-comparison tests (P14.8) pin both impls to
//! the same plan output. When the real DP work lands, divergences
//! show up as test diffs at the operator-tree level — exactly the
//! signal we want.

use std::sync::Arc;

use crate::catalog::Catalog;
use crate::common::Result;
use crate::sql::cost::{CostModel, DefaultCostModel};
use crate::sql::logical::LogicalPlan;
use crate::sql::planner::{plan as plan_rule_based, PhysicalPlan, PlannerStrategy};
use crate::storage::StorageEngine;

/// Cost-based planner. Carries the `CostModel` and a `time_budget_ms`
/// budget; in scaffolding mode the search space has no branching, so
/// the budget is never consumed. The knob exists so the contract
/// future DP enumeration must respect is visible today.
pub struct SelingerPlanner<C: CostModel = DefaultCostModel> {
    pub cost_model: C,
    pub time_budget_ms: u64,
}

impl Default for SelingerPlanner<DefaultCostModel> {
    fn default() -> Self {
        Self {
            cost_model: DefaultCostModel::default(),
            // 1s is a generous upper bound — TPC-C OLTP plans should
            // finish in microseconds. Larger analytical queries
            // (Phase 17+) will tune this per-session.
            time_budget_ms: 1_000,
        }
    }
}

impl<C: CostModel> PlannerStrategy for SelingerPlanner<C> {
    fn plan<TblE, CatE>(
        &self,
        logical: LogicalPlan,
        engine: Arc<TblE>,
        catalog: &Catalog<CatE>,
    ) -> Result<PhysicalPlan>
    where
        TblE: StorageEngine + 'static,
        CatE: StorageEngine,
    {
        // Scaffolding: delegate. The dispatch site for cost-driven
        // leaf-scan and join-algorithm choices lives here once
        // table-relative predicates land. Until then, the rule-based
        // planner's tree is also the cost-optimal tree under the
        // identity ordering.
        let _ = &self.cost_model;
        let _ = self.time_budget_ms;
        plan_rule_based(logical, engine, catalog)
    }

    fn name(&self) -> &'static str {
        "selinger"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{Catalog, ColumnDef, Schema, TableId};
    use crate::index::btree::BTreeEngine;
    use crate::sql::binder::Binder;
    use crate::sql::frontend::parse;
    use crate::sql::planner::RuleBasedPlanner;
    use crate::storage::FileDiskManager;
    use crate::types::ColumnType;
    use tempfile::TempDir;

    struct Env {
        engine: Arc<BTreeEngine>,
        catalog: Arc<Catalog<BTreeEngine>>,
        binder: Binder<BTreeEngine>,
        _dir: TempDir,
    }

    fn setup() -> Env {
        let dir = tempfile::tempdir().unwrap();
        let dm = FileDiskManager::create(dir.path().join("t.db")).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());
        let binder = Binder::new(catalog.clone());
        let schema = Schema {
            name: "t".into(),
            table_id: TableId(0),
            columns: vec![
                ColumnDef { name: "id".into(), ty: ColumnType::Int32, nullable: false, default: None },
                ColumnDef { name: "v".into(), ty: ColumnType::Int64, nullable: false, default: None },
            ],
            primary_key: vec![0],
        };
        catalog.create_table("t".into(), schema).unwrap();
        Env { engine, catalog, binder, _dir: dir }
    }

    #[test]
    fn name_is_selinger() {
        let p = SelingerPlanner::<DefaultCostModel>::default();
        assert_eq!(p.name(), "selinger");
    }

    #[test]
    fn carries_cost_model_and_budget() {
        let p = SelingerPlanner::<DefaultCostModel>::default();
        assert_eq!(p.time_budget_ms, 1_000);
        // The cost model must be addressable on the struct; the
        // smoke check below just proves the trait method is in scope.
        let _ = p.cost_model.cost_seq_scan(1.0, 1.0);
    }

    #[test]
    fn select_plan_matches_rule_based() {
        // Scaffolding contract: same SQL → same EXPLAIN tree under
        // both strategies. When the real DP lands, this test breaks
        // for any query where a cheaper plan exists — which is the
        // signal we want.
        let env = setup();
        let stmts = parse("SELECT v FROM t WHERE id = 1 LIMIT 2").unwrap();
        let a = env.binder.bind(stmts.clone().into_iter().next().unwrap()).unwrap();
        let b = env.binder.bind(stmts.into_iter().next().unwrap()).unwrap();

        let p_rule = RuleBasedPlanner
            .plan(a, env.engine.clone(), &env.catalog)
            .unwrap();
        let p_sel = SelingerPlanner::<DefaultCostModel>::default()
            .plan(b, env.engine.clone(), &env.catalog)
            .unwrap();

        let (t_rule, t_sel) = match (p_rule, p_sel) {
            (PhysicalPlan::Executor(a), PhysicalPlan::Executor(b)) => (a.explain(0), b.explain(0)),
            _ => panic!("expected executor trees"),
        };
        assert_eq!(t_rule, t_sel);
    }
}
