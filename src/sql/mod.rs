//! SQL surface — parser → logical plan → physical plan (executor tree).
//!
//! Pipeline stages (in dependency order):
//!   - `frontend`     SQL string → sqlparser AST.
//!   - `binder`       AST → catalog-resolved `LogicalPlan`.
//!   - `ir`           the three plan representations: `expr`
//!     (Expression/Predicate), `logical` (LogicalPlan), `physical`
//!     (PhysOp — the model-neutral executor IR).
//!   - `optimizer`    cost-based planning machinery: cost model, stats,
//!     selectivity, join-order DP (Selinger), and the Phase 17 memo
//!     planner.
//!   - `planner`      the `PlannerStrategy` socket, the `Planner` enum,
//!     and the rule-based lowering every planner shares.
//!   - `workload_log` append-only journal of executed statements.

pub mod binder;
pub mod frontend;
pub mod ir;
pub mod optimizer;
pub mod planner;
pub mod workload_log;

pub use binder::Binder;
pub use frontend::parse;
pub use ir::expr::{BinaryOp, CompareOp, Expression, Predicate};
pub use ir::logical::LogicalPlan;
pub use ir::physical::PhysOp;
pub use optimizer::column_map::ColumnRemap;
pub use optimizer::cost::{Cost, CostModel, CostWeights, DefaultCostModel};
pub use optimizer::join_order::{
    cost_of_order, enumerate_join_orders, JoinAlgorithm, JoinEdge, JoinOrder, JoinPlan,
    JoinRelation, RelId,
};
pub use optimizer::memo::{BaselineCapable, VolcanoPlanner};
pub use optimizer::selectivity::{
    estimate_predicate_selectivity, join_selectivity, EQ_FALLBACK, JOIN_FALLBACK, MIN_SELECTIVITY,
    RANGE_FALLBACK,
};
pub use optimizer::selinger::SelingerPlanner;
pub use optimizer::stats::{
    CatalogStatsProvider, MockStatsProvider, QueryStats, StatsProvider, DEFAULT_ROW_COUNT,
};
pub use planner::{plan, PhysicalPlan, Planner, PlannerStrategy, RuleBasedPlanner};
pub use workload_log::WorkloadLog;
