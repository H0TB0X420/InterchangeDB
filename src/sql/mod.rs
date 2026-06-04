//! SQL surface — parser → logical plan → physical plan (executor tree).
//!
//! Phase 11 components (in dependency order):
//!   - `frontend`     SQL string → sqlparser AST.
//!   - `logical`      AST → our `LogicalPlan` IR (catalog-resolved).
//!   - `expr`         Expression / Predicate IR + closure compilation.
//!   - `planner`      `LogicalPlan` → `Box<dyn Executor>`.
//!   - `workload_log` Append-only journal of executed statements.
//!
//! Rule-based for V1. Optimizer arrives in Phase 14 (Selinger) and Phase
//! 17/18 (Cascades, as an interchangeable alternative).

pub mod binder;
pub mod cost;
pub mod expr;
pub mod frontend;
pub mod join_order;
pub mod logical;
pub mod planner;
pub mod selectivity;
pub mod selinger;
pub mod stats;
pub mod workload_log;

pub use binder::Binder;
pub use cost::{Cost, CostModel, CostWeights, DefaultCostModel};
pub use join_order::{
    enumerate_join_orders, JoinAlgorithm, JoinEdge, JoinOrder, JoinPlan, JoinRelation, RelId,
};
pub use selectivity::{
    estimate_predicate_selectivity, join_selectivity, EQ_FALLBACK, JOIN_FALLBACK, MIN_SELECTIVITY,
    RANGE_FALLBACK,
};
pub use expr::{BinaryOp, CompareOp, Expression, Predicate};
pub use frontend::parse;
pub use logical::LogicalPlan;
pub use planner::{plan, PhysicalPlan, Planner, PlannerStrategy, RuleBasedPlanner};
pub use selinger::SelingerPlanner;
pub use stats::{
    CatalogStatsProvider, MockStatsProvider, QueryStats, StatsProvider, DEFAULT_ROW_COUNT,
};
pub use workload_log::WorkloadLog;
