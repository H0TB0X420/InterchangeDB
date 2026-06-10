//! Physical planner — `LogicalPlan` → `PhysicalPlan`.
//!
//! Rule-based: each LogicalPlan variant maps deterministically to one
//! shape of executor tree (or to a non-executable descriptor for DDL /
//! transaction control). No cost model, no statistics, no choice
//! between alternative plans. Phase 14 (Selinger) and Phase 17/18
//! (Cascades) will replace this with cost-based selection.
//!
//! Tree shapes produced:
//!
//!   SELECT → SeqScan → [Filter] → [Projection] → [Limit]
//!   INSERT → Insert
//!   UPDATE → Update wrapping (SeqScan → [Filter])
//!   DELETE → Delete wrapping (SeqScan → [Filter])
//!
//! `[ … ]` brackets indicate optional wrappers — emitted only when the
//! corresponding clause is present in the logical plan.

use std::sync::Arc;

use crate::catalog::{Catalog, ColumnDef};
use crate::common::Result;
use crate::execution::{
    Delete, Executor, Filter, HashJoin, IndexNestedLoopJoin, IndexScan, Insert, Limit,
    NestedLoopJoin, Projection, SeqScan, SetExpr, Update,
};
use crate::layout::RowLayout;
use crate::sql::cost::{CostModel, DefaultCostModel};
use crate::sql::expr::Predicate;
use crate::sql::join_order::JoinAlgorithm;
use crate::sql::logical::LogicalPlan;
use crate::sql::selectivity::join_selectivity;
use crate::sql::selinger::SelingerPlanner;
use crate::sql::stats::QueryStats;
use crate::storage::StorageEngine;
use crate::table::Table;

/// How a `SELECT`'s joins pick their algorithm. The difference between
/// the rule-based and Selinger planners is *only* this choice — both
/// build the same textual-order, same-layout tree (P14.13a).
pub(crate) enum JoinSelection<'a> {
    /// Phase 11 rule-based: `IndexNestedLoopJoin` when the inner side is
    /// indexed on the join column, else `NestedLoopJoin`. Never `HashJoin`.
    Heuristic,
    /// P14.13a cost-based: the cheapest of NLJ / Hash / INLJ under the
    /// model, with join order left textual (no layout change). `stats` is
    /// the query's `QueryStats` snapshot.
    CostBased {
        cost_model: &'a dyn CostModel,
        stats: &'a QueryStats,
    },
}

/// Output of the planner: either an executable operator tree, a
/// descriptor for a side-effect-only plan, or an EXPLAIN string.
pub enum PhysicalPlan {
    Executor(Box<dyn Executor>),
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Vec<usize>,
    },
    /// `ANALYZE TABLE t` (P14.2). Side-effect-only — session handler
    /// scans the table and persists stats; nothing runs through the
    /// operator tree.
    Analyze {
        table: String,
    },
    BeginTxn,
    CommitTxn,
    AbortTxn,
    Explain(String),
}

/// A strategy for turning a `LogicalPlan` into a `PhysicalPlan`.
///
/// V1 has one impl (`RuleBasedPlanner`); Selinger (P14.7) will add a
/// second. The trait exists so the session — and tests — can swap
/// planners without touching call sites. It is intentionally NOT
/// dyn-compatible: each impl is picked at compile time at the session
/// level, and the generic engine params would force erasure that buys
/// nothing.
pub trait PlannerStrategy {
    /// Plan a single logical statement.
    fn plan<TblE, CatE>(
        &self,
        logical: LogicalPlan,
        engine: Arc<TblE>,
        catalog: &Catalog<CatE>,
    ) -> Result<PhysicalPlan>
    where
        TblE: StorageEngine + 'static,
        CatE: StorageEngine;

    /// Short identifier for logging / EXPLAIN headers.
    fn name(&self) -> &'static str;
}

/// V1 planner: each LogicalPlan variant maps to one shape of executor
/// tree. No cost model, no statistics. See module docs for tree shapes.
pub struct RuleBasedPlanner;

impl PlannerStrategy for RuleBasedPlanner {
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
        plan(logical, engine, catalog)
    }

    fn name(&self) -> &'static str {
        "rule-based"
    }
}

/// Runtime-selectable planner held by a `Session` (P14.14).
///
/// Enum dispatch rather than `Box<dyn PlannerStrategy>` because
/// `PlannerStrategy::plan` is generic over the engine types, so the trait
/// isn't object-safe. The planner set is closed and known (rule-based,
/// Selinger now; Volcano, Cascades later), so an enum is the right
/// pragmatic swap. See plan.md P14.14 for the Option-C refactor (rework
/// the trait to be object-safe) if an *open* set is ever needed.
pub enum Planner {
    RuleBased(RuleBasedPlanner),
    Selinger(SelingerPlanner<DefaultCostModel>),
}

impl Default for Planner {
    fn default() -> Self {
        // Rule-based stays the default — Selinger is opt-in until the
        // Phase 16 harness validates it (see the P14.13a watch item).
        Planner::RuleBased(RuleBasedPlanner)
    }
}

impl Planner {
    /// Plan one statement with the selected strategy.
    pub fn plan<TblE, CatE>(
        &self,
        logical: LogicalPlan,
        engine: Arc<TblE>,
        catalog: &Catalog<CatE>,
    ) -> Result<PhysicalPlan>
    where
        TblE: StorageEngine + 'static,
        CatE: StorageEngine,
    {
        match self {
            Planner::RuleBased(p) => p.plan(logical, engine, catalog),
            Planner::Selinger(p) => p.plan(logical, engine, catalog),
        }
    }

    /// Identifier of the active strategy, for logging / introspection.
    pub fn name(&self) -> &'static str {
        match self {
            Planner::RuleBased(p) => p.name(),
            Planner::Selinger(p) => p.name(),
        }
    }
}

/// Plan a single logical statement.
///
/// `engine` is the storage handle DML operators read/write through —
/// in production this is `Arc<TxnEngine<E>>` so MVCC + locking apply.
/// `catalog` is the schema lookup source. They're independent generic
/// params: the catalog uses raw storage (DDL is non-transactional in
/// V2), while DML wraps that same raw storage in a TxnEngine handle.
pub fn plan<TblE, CatE>(
    logical: LogicalPlan,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<PhysicalPlan>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    plan_inner(logical, engine, catalog, &JoinSelection::Heuristic)
}

/// Plan with an explicit join-selection strategy. `plan()` is the
/// rule-based entry (`Heuristic`); `SelingerPlanner` (P14.13a) calls this
/// with `CostBased`. Only `SELECT` join lowering reads `selection`; every
/// other arm is identical across planners.
pub(crate) fn plan_inner<TblE, CatE>(
    logical: LogicalPlan,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
    selection: &JoinSelection,
) -> Result<PhysicalPlan>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    match logical {
        LogicalPlan::CreateTable {
            name,
            columns,
            primary_key,
        } => Ok(PhysicalPlan::CreateTable {
            name,
            columns,
            primary_key,
        }),
        LogicalPlan::Analyze { table } => Ok(PhysicalPlan::Analyze { table }),
        LogicalPlan::BeginTxn => Ok(PhysicalPlan::BeginTxn),
        LogicalPlan::CommitTxn => Ok(PhysicalPlan::CommitTxn),
        LogicalPlan::AbortTxn => Ok(PhysicalPlan::AbortTxn),
        LogicalPlan::Explain(inner) => {
            let inner_phys = plan_inner(*inner, engine, catalog, selection)?;
            Ok(PhysicalPlan::Explain(render_explain(&inner_phys)))
        }
        LogicalPlan::Select {
            table,
            joins,
            projection,
            aggregates,
            filter,
            order_by,
            limit,
        } => {
            let exec = plan_select(
                table, joins, projection, aggregates, filter, order_by, limit, engine, catalog,
                selection,
            )?;
            Ok(PhysicalPlan::Executor(exec))
        }
        LogicalPlan::Insert { table, rows } => {
            let exec = plan_insert(table, rows, engine, catalog)?;
            Ok(PhysicalPlan::Executor(exec))
        }
        LogicalPlan::Update {
            table,
            set_clauses,
            filter,
        } => {
            let exec = plan_update(table, set_clauses, filter, engine, catalog)?;
            Ok(PhysicalPlan::Executor(exec))
        }
        LogicalPlan::Delete { table, filter } => {
            let exec = plan_delete(table, filter, engine, catalog)?;
            Ok(PhysicalPlan::Executor(exec))
        }
    }
}

// CLIPPY-ALLOW(too_many_arguments): a SELECT carries table/joins/projection/
// aggregates/filter/order/limit plus engine + catalog + join selection; a
// params struct would just rename the same fields without simplifying callers.
#[allow(clippy::too_many_arguments)]
fn plan_select<TblE, CatE>(
    table_name: String,
    joins: Vec<crate::sql::logical::JoinClause>,
    projection: Vec<usize>,
    aggregates: Vec<crate::sql::logical::AggregateSpec>,
    filter: Option<Predicate>,
    order_by: Vec<(usize, crate::sql::logical::OrderDir)>,
    limit: Option<usize>,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
    selection: &JoinSelection,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let left_schema = catalog.get_table(&table_name)?;
    let left_table_id = left_schema.table_id;
    let left_indexes = catalog.indexes_for_table(left_schema.table_id, &left_schema)?;
    let left_table = Arc::new(Table::with_indexes(
        engine.clone(),
        left_schema,
        RowLayout,
        left_indexes.clone(),
    ));

    // Build the leaf scan for the left side. We only attempt IndexScan
    // lowering on the LEFT side when there are NO joins — once joins
    // enter, predicate scoping spans multiple tables and column indices
    // shift, so the lowering rule (column index == table column index)
    // no longer holds. Index-driven joins use IndexNestedLoopJoin below.
    let mut current: Box<dyn Executor>;
    let mut residual_filter: Option<Predicate> = filter;
    if joins.is_empty() {
        if let Some(pred) = residual_filter {
            match try_lower_index_predicate(pred, &left_indexes) {
                IndexLowering::Matched { handle, prefix } => {
                    current = Box::new(IndexScan::new(&*left_table, &handle, &prefix)?);
                    residual_filter = None;
                }
                IndexLowering::Unmatched(pred) => {
                    current = Box::new(SeqScan::new(&*left_table)?);
                    residual_filter = Some(pred);
                }
            }
        } else {
            current = Box::new(SeqScan::new(&*left_table)?);
        }
    } else {
        current = Box::new(SeqScan::new(&*left_table)?);
    }

    // P13.1: chain joins onto the left side. For each join, pick
    // IndexNestedLoopJoin when the ON predicate is `left.col = right.idx_col`
    // and the right side has a matching single-column index; fall back to
    // NestedLoopJoin with the compiled ON predicate (or trivially-true for
    // cross joins).
    let mut outer_offset = current.schema().columns.len();
    // Running output-cardinality estimate — used only in cost mode to pick
    // join algorithms. Seeded from the left table's base row count. NOTE
    // (P14.13a): this ignores any narrowing from the left's index scan /
    // local filter; an over-estimate of the outer only biases toward Hash,
    // which is the safe direction. The exact figure arrives with the
    // P14.12 table-relative refactor.
    let mut outer_card: f64 = match selection {
        JoinSelection::CostBased { stats, .. } => stats.row_count(left_table_id),
        JoinSelection::Heuristic => 0.0,
    };
    for join in joins {
        let right_schema = catalog.get_table(&join.right_table)?;
        let right_indexes = catalog.indexes_for_table(right_schema.table_id, &right_schema)?;
        let right_table = Arc::new(Table::with_indexes(
            engine.clone(),
            right_schema.clone(),
            RowLayout,
            right_indexes.clone(),
        ));
        let right_cols = right_schema.columns.len();

        let next: Box<dyn Executor> = match selection {
            // Phase 11 heuristic: INLJ when the inner is indexed, else NLJ.
            JoinSelection::Heuristic => {
                let inlj_choice = join
                    .on
                    .as_ref()
                    .and_then(|pred| try_match_inlj(pred, outer_offset, &right_indexes));
                if let Some((outer_col, handle)) = inlj_choice {
                    Box::new(IndexNestedLoopJoin::new(
                        current,
                        right_table,
                        handle,
                        vec![outer_col],
                    )?)
                } else {
                    build_nested_loop_join(current, right_table, join.on)?
                }
            }
            // P14.13a cost-based: cheapest of NLJ / Hash / INLJ, textual
            // order preserved (so column layout is unchanged).
            JoinSelection::CostBased { cost_model, stats } => {
                let keys = join
                    .on
                    .as_ref()
                    .and_then(|p| extract_equi_join_keys(p, outer_offset));
                let inlj = join
                    .on
                    .as_ref()
                    .and_then(|p| try_match_inlj(p, outer_offset, &right_indexes));
                let inner_card = stats.row_count(right_schema.table_id);
                // NOTE (P14.13a): the outer join key can't be mapped back to
                // a (table, col) for its NDV until P14.12, so selectivity
                // uses the inner key's NDV only. Adequate for algorithm
                // choice (Hash-vs-NLJ is cardinality-driven).
                let inner_ndv = keys
                    .map(|(_, ic)| stats.ndv(right_schema.table_id, ic as u32))
                    .unwrap_or(0);
                let edge_sel = if keys.is_some() {
                    join_selectivity(0, inner_ndv)
                } else {
                    1.0
                };
                let algorithm = choose_join_algorithm(
                    *cost_model,
                    outer_card,
                    inner_card,
                    edge_sel,
                    keys.is_some(),
                    inlj.is_some(),
                );
                outer_card = (outer_card * inner_card * edge_sel).max(1.0);
                match algorithm {
                    JoinAlgorithm::IndexNestedLoop => {
                        let (outer_col, handle) = inlj.expect("INLJ chosen only when available");
                        Box::new(IndexNestedLoopJoin::new(
                            current,
                            right_table,
                            handle,
                            vec![outer_col],
                        )?)
                    }
                    JoinAlgorithm::Hash => {
                        let (outer_col, inner_col) =
                            keys.expect("Hash chosen only when equi-keys exist");
                        let right_seq = Box::new(SeqScan::new(&*right_table)?);
                        Box::new(HashJoin::new(current, right_seq, outer_col, inner_col)?)
                    }
                    JoinAlgorithm::NestedLoop => {
                        build_nested_loop_join(current, right_table, join.on)?
                    }
                }
            }
        };
        outer_offset += right_cols;
        current = next;
    }

    if let Some(pred) = residual_filter {
        current = Box::new(Filter::from_boxed(current, pred.compile()));
    }
    if !aggregates.is_empty() {
        // P13.4: HashAggregate before Projection (aggregates ARE the
        // output; binder enforces projection empty alongside aggregates).
        let agg_fns: Vec<crate::execution::AggregateFn> = aggregates
            .into_iter()
            .map(translate_aggregate_spec)
            .collect();
        current = Box::new(crate::execution::HashAggregate::new(current, agg_fns)?);
    }
    // P13.6: Sort BEFORE Projection so sort keys can reference columns
    // even if they're not in the projection (TPC-C Payment does this).
    // Binder resolves ORDER BY indices against the pre-projection scope.
    if !order_by.is_empty() {
        let keys: Vec<(usize, crate::execution::SortDir)> = order_by
            .into_iter()
            .map(|(c, d)| {
                let dir = match d {
                    crate::sql::logical::OrderDir::Asc => crate::execution::SortDir::Asc,
                    crate::sql::logical::OrderDir::Desc => crate::execution::SortDir::Desc,
                };
                (c, dir)
            })
            .collect();
        current = Box::new(crate::execution::Sort::new(current, keys)?);
    }
    if aggregates_was_empty_marker(&projection) && !projection.is_empty() {
        current = Box::new(Projection::new(current, projection)?);
    }
    if let Some(n) = limit {
        current = Box::new(Limit::new(current, n));
    }
    Ok(current)
}

/// Always true unless we want to suppress Projection for aggregate plans.
/// The aggregate path replaces the input schema entirely, so a trailing
/// Projection would re-index into the aggregate's output tuple, not the
/// pre-aggregate scope. Binder enforces projection is empty when
/// aggregates are non-empty, so this guard is conservative: only run
/// Projection if there's something to project.
fn aggregates_was_empty_marker(projection: &[usize]) -> bool {
    let _ = projection;
    true
}

fn translate_aggregate_spec(
    spec: crate::sql::logical::AggregateSpec,
) -> crate::execution::AggregateFn {
    use crate::execution::AggregateFn;
    use crate::sql::logical::AggregateSpec;
    match spec {
        AggregateSpec::CountStar => AggregateFn::CountStar,
        AggregateSpec::Count {
            col,
            distinct: false,
        } => AggregateFn::Count(col),
        AggregateSpec::Count {
            col,
            distinct: true,
        } => AggregateFn::CountDistinct(col),
        AggregateSpec::Sum(col) => AggregateFn::Sum(col),
        AggregateSpec::Min(col) => AggregateFn::Min(col),
        AggregateSpec::Max(col) => AggregateFn::Max(col),
        AggregateSpec::Avg(col) => AggregateFn::Avg(col),
    }
}

/// Build a `NestedLoopJoin` of `outer` with a fresh scan of `right_table`,
/// compiling the ON predicate (or constant-true for a cross product). The
/// predicate's column indices are tuple-global over `outer || inner`,
/// which holds because joins are lowered in textual order.
fn build_nested_loop_join<TblE>(
    outer: Box<dyn Executor>,
    right_table: Arc<Table<TblE, RowLayout>>,
    on: Option<Predicate>,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
{
    let pred_fn: crate::execution::JoinPredicate = match on {
        Some(p) => {
            let f = p.compile();
            Box::new(move |outer, inner| {
                let mut combined = outer.clone();
                combined.extend_from_slice(inner);
                f(&combined)
            })
        }
        None => Box::new(|_, _| true),
    };
    let right_seq = Box::new(SeqScan::new(&*right_table)?);
    Ok(Box::new(NestedLoopJoin::new(outer, right_seq, pred_fn)?))
}

/// Extract `(outer_col_global, inner_col_local)` from an equi-join ON
/// predicate `outer.col = inner.col`. Mirrors `try_match_inlj` but needs
/// no index — it drives `HashJoin`. `None` for non-equi or composite
/// predicates (those stay on `NestedLoopJoin`).
fn extract_equi_join_keys(pred: &Predicate, outer_offset: usize) -> Option<(usize, usize)> {
    let (l, r) = match pred {
        Predicate::Compare {
            op: crate::sql::expr::CompareOp::Eq,
            left: crate::sql::expr::Expression::Column(l),
            right: crate::sql::expr::Expression::Column(r),
        } => (*l, *r),
        _ => return None,
    };
    let (outer_col, inner_col_global) = if l < outer_offset && r >= outer_offset {
        (l, r)
    } else if r < outer_offset && l >= outer_offset {
        (r, l)
    } else {
        return None;
    };
    Some((outer_col, inner_col_global - outer_offset))
}

/// Pick the cheapest join algorithm under `cost_model` for one
/// textual-order join. `Hash` is considered only when equi-join keys
/// exist; `IndexNestedLoop` only when the inner side is indexed on the
/// join column. The caller builds the executor for the returned choice.
fn choose_join_algorithm(
    cost_model: &dyn CostModel,
    outer_card: f64,
    inner_card: f64,
    edge_sel: f64,
    hash_available: bool,
    inlj_available: bool,
) -> JoinAlgorithm {
    let mut best_algorithm = JoinAlgorithm::NestedLoop;
    let mut best_cost = cost_model.scalar(cost_model.cost_nested_loop_join(outer_card, inner_card));

    if hash_available {
        let (build, probe) = if outer_card <= inner_card {
            (outer_card, inner_card)
        } else {
            (inner_card, outer_card)
        };
        let hash_cost = cost_model.scalar(cost_model.cost_hash_join(build, probe));
        if hash_cost < best_cost {
            best_cost = hash_cost;
            best_algorithm = JoinAlgorithm::Hash;
        }
    }
    if inlj_available {
        let avg_matches = (inner_card * edge_sel).max(0.0);
        let inlj_cost =
            cost_model.scalar(cost_model.cost_index_nested_loop_join(outer_card, avg_matches));
        if inlj_cost < best_cost {
            best_algorithm = JoinAlgorithm::IndexNestedLoop;
        }
    }
    best_algorithm
}

/// Try to lower a join ON predicate into IndexNestedLoopJoin. Recognized
/// shape: `Column(outer) = Column(inner_col)` where `inner_col` is local
/// to the right table (i.e. inner_col_tuple - outer_offset == 0 for a
/// single-column index on the right's first indexed column).
///
/// Returns `(outer_col_idx_in_outer_tuple, IndexHandle)` on match.
fn try_match_inlj(
    pred: &Predicate,
    outer_offset: usize,
    right_indexes: &[crate::table::IndexHandle],
) -> Option<(usize, crate::table::IndexHandle)> {
    let (l, r) = match pred {
        Predicate::Compare {
            op: crate::sql::expr::CompareOp::Eq,
            left: crate::sql::expr::Expression::Column(l),
            right: crate::sql::expr::Expression::Column(r),
        } => (*l, *r),
        _ => return None,
    };
    // Identify which side is outer (< outer_offset) and which is inner.
    let (outer_col, inner_col_global) = if l < outer_offset && r >= outer_offset {
        (l, r)
    } else if r < outer_offset && l >= outer_offset {
        (r, l)
    } else {
        return None;
    };
    let inner_col_local = inner_col_global - outer_offset;
    for ix in right_indexes {
        if ix.def.columns == [inner_col_local] {
            return Some((outer_col, ix.clone()));
        }
    }
    None
}

/// Result of attempting to lower a `WHERE` clause into an IndexScan.
enum IndexLowering {
    /// A single-column equality predicate matched a single-column index.
    /// `handle` is the index; `prefix` is the literal value to scan for.
    /// The original predicate is consumed and dropped — IndexScan
    /// guarantees no false positives for this shape.
    Matched {
        handle: crate::table::IndexHandle,
        prefix: Vec<crate::types::Value>,
    },
    /// No index matched. Original predicate returned unchanged so the
    /// caller can drop it onto a `Filter` on top of `SeqScan`.
    Unmatched(Predicate),
}

/// Try to lower `pred` into an IndexScan against any of `indexes`. First
/// cut handles only the simplest indexable shape: a single equality
/// between one indexed column and a literal. Composite indexes, range
/// predicates, and AND-decomposition land in later phases.
fn try_lower_index_predicate(
    pred: Predicate,
    indexes: &[crate::table::IndexHandle],
) -> IndexLowering {
    // Extract `(col_idx, value)` from `Compare(Eq, Column(i), Literal(v))`
    // and the mirror `Compare(Eq, Literal(v), Column(i))`.
    let extracted = match &pred {
        Predicate::Compare {
            op: crate::sql::expr::CompareOp::Eq,
            left: crate::sql::expr::Expression::Column(i),
            right: crate::sql::expr::Expression::Literal(v),
        } => Some((*i, v.clone())),
        Predicate::Compare {
            op: crate::sql::expr::CompareOp::Eq,
            left: crate::sql::expr::Expression::Literal(v),
            right: crate::sql::expr::Expression::Column(i),
        } => Some((*i, v.clone())),
        _ => None,
    };

    if let Some((col_idx, lit)) = extracted {
        for ix in indexes {
            // Only single-column indexes match this simple shape.
            if ix.def.columns == [col_idx] {
                return IndexLowering::Matched {
                    handle: ix.clone(),
                    prefix: vec![lit],
                };
            }
        }
    }
    IndexLowering::Unmatched(pred)
}

fn plan_insert<TblE, CatE>(
    table_name: String,
    rows: Vec<Vec<crate::sql::expr::Expression>>,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    // Evaluate each expression against an empty tuple — INSERT VALUES
    // exprs are literals or pre-substituted parameters, never column
    // refs. (Parameter substitution happens upstream in
    // PreparedStatement::execute; any unsubstituted parameters here are
    // a bug — `Expression::compile` debug-asserts.)
    let rows: Vec<Vec<crate::types::Value>> = rows
        .into_iter()
        .map(|row| row.into_iter().map(|e| e.compile()(&Vec::new())).collect())
        .collect();
    let schema = catalog.get_table(&table_name)?;
    let indexes = catalog.indexes_for_table(schema.table_id, &schema)?;
    let table = Arc::new(Table::with_indexes(engine, schema, RowLayout, indexes));
    Ok(Box::new(Insert::new(table, rows)))
}

fn plan_update<TblE, CatE>(
    table_name: String,
    set_clauses: Vec<(usize, crate::sql::expr::Expression)>,
    filter: Option<Predicate>,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let schema = catalog.get_table(&table_name)?;
    let indexes = catalog.indexes_for_table(schema.table_id, &schema)?;
    let table = Arc::new(Table::with_indexes(
        engine,
        schema,
        RowLayout,
        indexes.clone(),
    ));

    // Same IndexScan lowering as plan_select. Index-driven updates are
    // common in OLTP: `UPDATE customer SET … WHERE c_id = ?`.
    let mut child: Box<dyn Executor>;
    let mut residual_filter: Option<Predicate> = filter;
    if let Some(pred) = residual_filter {
        match try_lower_index_predicate(pred, &indexes) {
            IndexLowering::Matched { handle, prefix } => {
                child = Box::new(IndexScan::new(&*table, &handle, &prefix)?);
                residual_filter = None;
            }
            IndexLowering::Unmatched(pred) => {
                child = Box::new(SeqScan::new(&*table)?);
                residual_filter = Some(pred);
            }
        }
    } else {
        child = Box::new(SeqScan::new(&*table)?);
    }
    if let Some(pred) = residual_filter {
        child = Box::new(Filter::from_boxed(child, pred.compile()));
    }

    let set_exprs: Vec<SetExpr> = set_clauses
        .into_iter()
        .map(|(idx, expr)| (idx, expr.compile()))
        .collect();

    Ok(Box::new(Update::new(table, child, set_exprs)))
}

fn plan_delete<TblE, CatE>(
    table_name: String,
    filter: Option<Predicate>,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let schema = catalog.get_table(&table_name)?;
    let indexes = catalog.indexes_for_table(schema.table_id, &schema)?;
    let table = Arc::new(Table::with_indexes(
        engine,
        schema,
        RowLayout,
        indexes.clone(),
    ));

    let mut child: Box<dyn Executor>;
    let mut residual_filter: Option<Predicate> = filter;
    if let Some(pred) = residual_filter {
        match try_lower_index_predicate(pred, &indexes) {
            IndexLowering::Matched { handle, prefix } => {
                child = Box::new(IndexScan::new(&*table, &handle, &prefix)?);
                residual_filter = None;
            }
            IndexLowering::Unmatched(pred) => {
                child = Box::new(SeqScan::new(&*table)?);
                residual_filter = Some(pred);
            }
        }
    } else {
        child = Box::new(SeqScan::new(&*table)?);
    }
    if let Some(pred) = residual_filter {
        child = Box::new(Filter::from_boxed(child, pred.compile()));
    }
    Ok(Box::new(Delete::new(table, child)))
}

/// Render an EXPLAIN string for a `PhysicalPlan`. For executor trees this
/// delegates to the operator's own `explain`; for descriptors it emits a
/// one-line summary.
fn render_explain(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Executor(exec) => exec.explain(0),
        PhysicalPlan::CreateTable { name, .. } => format!("CreateTable({})\n", name),
        PhysicalPlan::Analyze { table } => format!("Analyze({})\n", table),
        PhysicalPlan::BeginTxn => "BeginTxn\n".to_string(),
        PhysicalPlan::CommitTxn => "CommitTxn\n".to_string(),
        PhysicalPlan::AbortTxn => "AbortTxn\n".to_string(),
        PhysicalPlan::Explain(text) => {
            // Inner Explain already carries a rendered, newline-terminated string.
            // Indent each line so the nesting is visible in the output.
            let indented: String = text.lines().map(|l| format!("  {}\n", l)).collect();
            format!("Explain\n{}", indented)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{Schema, TableId};
    use crate::common::Error;
    use crate::index::btree::BTreeEngine;
    use crate::sql::binder::Binder;
    use crate::sql::frontend::parse;
    use crate::storage::FileDiskManager;
    use crate::types::{ColumnType, Value};
    use tempfile::TempDir;

    struct TestEnv {
        engine: Arc<BTreeEngine>,
        catalog: Arc<Catalog<BTreeEngine>>,
        binder: Binder<BTreeEngine>,
        _dir: TempDir,
    }

    fn setup() -> TestEnv {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(512, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());
        let binder = Binder::new(catalog.clone());
        TestEnv {
            engine,
            catalog,
            binder,
            _dir: dir,
        }
    }

    fn create_warehouse(env: &TestEnv) {
        let schema = Schema {
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
        env.catalog
            .create_table("warehouse".into(), schema)
            .unwrap();
    }

    fn plan_sql(env: &TestEnv, sql: &str) -> PhysicalPlan {
        let stmts = parse(sql).unwrap();
        let logical = env.binder.bind(stmts.into_iter().next().unwrap()).unwrap();
        plan(logical, env.engine.clone(), &env.catalog).unwrap()
    }

    // ---- DDL / TC variants ----

    #[test]
    fn plans_create_table_to_descriptor() {
        let env = setup();
        let p = plan_sql(&env, "CREATE TABLE t (id INT PRIMARY KEY)");
        assert!(matches!(p, PhysicalPlan::CreateTable { .. }));
    }

    #[test]
    fn plans_transaction_control_to_descriptors() {
        let env = setup();
        assert!(matches!(plan_sql(&env, "BEGIN"), PhysicalPlan::BeginTxn));
        assert!(matches!(plan_sql(&env, "COMMIT"), PhysicalPlan::CommitTxn));
        assert!(matches!(plan_sql(&env, "ROLLBACK"), PhysicalPlan::AbortTxn));
    }

    // ---- SELECT shapes ----

    #[test]
    fn plans_select_star_to_seq_scan_only() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "SELECT * FROM warehouse");
        match p {
            PhysicalPlan::Executor(exec) => {
                let tree = exec.explain(0);
                assert_eq!(tree, "SeqScan(warehouse)\n");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_select_with_where_to_filter_chain() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "SELECT w_id FROM warehouse WHERE w_id = 1");
        match p {
            PhysicalPlan::Executor(exec) => {
                let tree = exec.explain(0);
                let expected = "\
Projection([0])
  Filter
    SeqScan(warehouse)
";
                assert_eq!(tree, expected);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_select_with_full_chain() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "SELECT w_id FROM warehouse WHERE w_id = 1 LIMIT 3");
        match p {
            PhysicalPlan::Executor(exec) => {
                let tree = exec.explain(0);
                // Order is: SeqScan → Filter → Projection → Limit (outermost wraps inward)
                let expected = "\
Limit(3)
  Projection([0])
    Filter
      SeqScan(warehouse)
";
                assert_eq!(tree, expected);
            }
            _ => panic!(),
        }
    }

    // ---- INSERT / UPDATE / DELETE ----

    #[test]
    fn plans_insert_to_insert_op_and_drives() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "INSERT INTO warehouse VALUES (1, 1000), (2, 2000)");
        match p {
            PhysicalPlan::Executor(mut exec) => {
                assert_eq!(exec.next().unwrap(), Some(vec![Value::Int64(2)]));
                assert_eq!(exec.next().unwrap(), None);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_update_with_where_and_drives() {
        let env = setup();
        create_warehouse(&env);
        // Seed: 1, 1000; 2, 2000.
        let seed = plan_sql(&env, "INSERT INTO warehouse VALUES (1, 1000), (2, 2000)");
        if let PhysicalPlan::Executor(mut e) = seed {
            e.next().unwrap();
        }
        // UPDATE w_ytd to 9999 WHERE w_id = 1.
        let p = plan_sql(&env, "UPDATE warehouse SET w_ytd = 9999 WHERE w_id = 1");
        match p {
            PhysicalPlan::Executor(mut exec) => {
                assert_eq!(exec.next().unwrap(), Some(vec![Value::Int64(1)]));
                // Verify the explain has the right shape.
                let tree = exec.explain(0);
                assert!(tree.starts_with("Update(warehouse, set_cols=[1])"));
                assert!(tree.contains("Filter"));
                assert!(tree.contains("SeqScan(warehouse)"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_delete_with_where() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "DELETE FROM warehouse WHERE w_id = 99");
        match p {
            PhysicalPlan::Executor(exec) => {
                let tree = exec.explain(0);
                assert!(tree.starts_with("Delete(warehouse)"));
                assert!(tree.contains("Filter"));
                assert!(tree.contains("SeqScan(warehouse)"));
            }
            _ => panic!(),
        }
    }

    // ---- EXPLAIN ----

    #[test]
    fn plans_explain_returns_tree_string() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "EXPLAIN SELECT w_id FROM warehouse WHERE w_id = 1");
        match p {
            PhysicalPlan::Explain(text) => {
                assert!(text.contains("Projection"));
                assert!(text.contains("Filter"));
                assert!(text.contains("SeqScan(warehouse)"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_explain_for_ddl_emits_descriptor_line() {
        let env = setup();
        let p = plan_sql(&env, "EXPLAIN CREATE TABLE t (id INT PRIMARY KEY)");
        match p {
            PhysicalPlan::Explain(text) => {
                assert!(text.starts_with("CreateTable(t)"));
            }
            _ => panic!(),
        }
    }

    // Regression: `render_explain` used to emit the stub "Explain[nested]\n"
    // when the inner plan was itself a `PhysicalPlan::Explain`, dropping the
    // inner text. The SQL surface forbids nested EXPLAIN at the parser layer
    // ("Explain must be root of the plan"), so we hand-build the nested
    // LogicalPlan to exercise the planner branch directly. The fix remains
    // load-bearing for any internal caller that constructs nested plans (e.g.
    // future optimizer rewrites or hand-built test plans).
    #[test]
    fn plans_nested_explain_preserves_inner_render() {
        let env = setup();
        create_warehouse(&env);
        let stmts = parse("EXPLAIN SELECT w_id FROM warehouse WHERE w_id = 1").unwrap();
        let inner_logical = env.binder.bind(stmts.into_iter().next().unwrap()).unwrap();
        let nested = LogicalPlan::Explain(Box::new(inner_logical));
        let p = plan(nested, env.engine.clone(), &env.catalog).unwrap();
        match p {
            PhysicalPlan::Explain(text) => {
                assert!(text.starts_with("Explain\n"), "got: {}", text);
                assert!(
                    !text.contains("Explain[nested]"),
                    "old stub leaked: {}",
                    text
                );
                assert!(text.contains("  Projection"), "got: {}", text);
                assert!(text.contains("SeqScan(warehouse)"), "got: {}", text);
                for line in text.lines().skip(1).filter(|l| !l.is_empty()) {
                    assert!(line.starts_with("  "), "unindented inner line: {:?}", line);
                }
            }
            _ => panic!(),
        }
    }

    // ---- Error paths ----

    // ---- PlannerStrategy ----

    #[test]
    fn rule_based_planner_strategy_matches_free_function() {
        let env = setup();
        create_warehouse(&env);
        let stmts = parse("SELECT w_id FROM warehouse WHERE w_id = 1 LIMIT 3").unwrap();
        let logical_a = env
            .binder
            .bind(stmts.clone().into_iter().next().unwrap())
            .unwrap();
        let logical_b = env.binder.bind(stmts.into_iter().next().unwrap()).unwrap();

        let free = plan(logical_a, env.engine.clone(), &env.catalog).unwrap();
        let via_strategy = RuleBasedPlanner
            .plan(logical_b, env.engine.clone(), &env.catalog)
            .unwrap();

        let (free_tree, strat_tree) = match (free, via_strategy) {
            (PhysicalPlan::Executor(a), PhysicalPlan::Executor(b)) => (a.explain(0), b.explain(0)),
            _ => panic!("expected two executor trees"),
        };
        assert_eq!(free_tree, strat_tree);
        assert_eq!(RuleBasedPlanner.name(), "rule-based");
    }

    #[test]
    fn plan_on_unknown_table_errors() {
        // The binder normally catches unknown tables. Bypass it by
        // constructing a `LogicalPlan` directly to exercise the planner's
        // own catalog lookup path.
        let env = setup();
        let logical = LogicalPlan::Select {
            table: "nonexistent".to_string(),
            joins: vec![],
            projection: vec![],
            aggregates: vec![],
            filter: None,
            order_by: vec![],
            limit: None,
        };
        match plan(logical, env.engine.clone(), &env.catalog) {
            Err(Error::TableNotFound { name }) => assert_eq!(name, "nonexistent"),
            other => panic!("expected TableNotFound, got {:?}", other.err()),
        }
    }
}
