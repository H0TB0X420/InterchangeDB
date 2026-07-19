//! Physical planner — `LogicalPlan` → `PhysicalPlan`.
//!
//! Rule-based: each LogicalPlan variant maps deterministically to one
//! shape of `PhysOp` plan (or to a non-executable descriptor for DDL /
//! transaction control); an `ExecutionModel` later builds the `PhysOp`
//! into a runnable operator tree. No cost model, no statistics, no choice
//! between alternative plans. Phase 14 (Selinger) and Phase 17/18
//! (Cascades) will replace this with cost-based selection.
//!
//! Tree shapes produced:
//!
//! ```text
//!   SELECT → SeqScan → [Filter] → [Projection] → [Limit]
//!   INSERT → Insert
//!   UPDATE → Update wrapping (SeqScan → [Filter])
//!   DELETE → Delete wrapping (SeqScan → [Filter])
//! ```
//!
//! `[ … ]` brackets indicate optional wrappers — emitted only when the
//! corresponding clause is present in the logical plan.

use crate::catalog::{Catalog, ColumnDef, Schema};
use crate::common::Result;
use crate::sql::ir::expr::{Expression, Predicate};
use crate::sql::ir::logical::{JoinKind, LogicalPlan};
use crate::sql::ir::physical::PhysOp;
use crate::sql::optimizer::cost::{CostModel, DefaultCostModel};
use crate::sql::optimizer::join_order::JoinAlgorithm;
use crate::sql::optimizer::memo::VolcanoPlanner;
use crate::sql::optimizer::selectivity::join_selectivity;
use crate::sql::optimizer::selinger::SelingerPlanner;
use crate::sql::optimizer::stats::QueryStats;
use crate::storage::StorageEngine;

/// How a `SELECT`'s joins pick their algorithm. The difference between
/// the rule-based and Selinger planners is *only* this choice — both
/// build the same textual-order, same-layout tree (P14.13a).
pub(crate) enum JoinSelection<'a> {
    /// Rule-based: `IndexNestedLoopJoin` when the inner side is indexed on the
    /// join column, `HashJoin` for an equi-key with no usable index (Phase D),
    /// else `NestedLoopJoin`. No cost model — purely shape-driven.
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
    Query(PhysOp),
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Vec<usize>,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<usize>,
        unique: bool,
        backend: crate::catalog::IndexBackend,
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
/// dyn-compatible: `plan` is generic over the catalog's storage engine, so a
/// trait object would force erasure that buys nothing. The planner set is
/// closed (rule-based, Selinger), so the `Planner` enum dispatches instead.
pub trait PlannerStrategy {
    /// Plan a single logical statement.
    fn plan<CatE>(&self, logical: LogicalPlan, catalog: &Catalog<CatE>) -> Result<PhysicalPlan>
    where
        CatE: StorageEngine;

    /// Short identifier for logging / EXPLAIN headers.
    fn name(&self) -> &'static str;
}

/// V1 planner: each LogicalPlan variant maps to one shape of `PhysOp` plan.
/// No cost model, no statistics. See module docs for tree shapes.
pub struct RuleBasedPlanner;

impl PlannerStrategy for RuleBasedPlanner {
    fn plan<CatE>(&self, logical: LogicalPlan, catalog: &Catalog<CatE>) -> Result<PhysicalPlan>
    where
        CatE: StorageEngine,
    {
        plan(logical, catalog)
    }

    fn name(&self) -> &'static str {
        "rule-based"
    }
}

/// Runtime-selectable planner held by a `Session` (P14.14).
///
/// Enum dispatch rather than `Box<dyn PlannerStrategy>` because
/// `PlannerStrategy::plan` is generic over the catalog's storage engine, so
/// the trait isn't object-safe. The planner set is closed and known (rule-based,
/// Selinger now; Volcano, Cascades later), so an enum is the right
/// pragmatic swap. See plan.md P14.14 for the Option-C refactor (rework
/// the trait to be object-safe) if an *open* set is ever needed.
pub enum Planner {
    RuleBased(RuleBasedPlanner),
    Selinger(SelingerPlanner<DefaultCostModel>),
    VolcanoMemo(VolcanoPlanner<DefaultCostModel>),
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
    pub fn plan<CatE>(&self, logical: LogicalPlan, catalog: &Catalog<CatE>) -> Result<PhysicalPlan>
    where
        CatE: StorageEngine,
    {
        match self {
            Planner::RuleBased(p) => p.plan(logical, catalog),
            Planner::Selinger(p) => p.plan(logical, catalog),
            Planner::VolcanoMemo(p) => p.plan(logical, catalog),
        }
    }

    /// Identifier of the active strategy, for logging / introspection.
    pub fn name(&self) -> &'static str {
        match self {
            Planner::RuleBased(p) => p.name(),
            Planner::Selinger(p) => p.name(),
            Planner::VolcanoMemo(p) => p.name(),
        }
    }
}

/// Plan a single logical statement.
///
/// `catalog` is the schema-lookup + index/stats source. Planning is
/// engine-free: it emits a `PhysOp` IR that an `ExecutionModel` later builds
/// against a storage engine.
pub fn plan<CatE>(logical: LogicalPlan, catalog: &Catalog<CatE>) -> Result<PhysicalPlan>
where
    CatE: StorageEngine,
{
    plan_inner(logical, catalog, &JoinSelection::Heuristic)
}

/// Plan with an explicit join-selection strategy. `plan()` is the
/// rule-based entry (`Heuristic`); `SelingerPlanner` (P14.13a) calls this
/// with `CostBased`. Only `SELECT` join lowering reads `selection`; every
/// other arm is identical across planners.
///
/// Planning is engine-free — it produces a `PhysOp` IR; building it into a
/// runnable tree is the `ExecutionModel`'s job.
pub(crate) fn plan_inner<CatE>(
    logical: LogicalPlan,
    catalog: &Catalog<CatE>,
    selection: &JoinSelection,
) -> Result<PhysicalPlan>
where
    CatE: StorageEngine,
{
    match logical {
        LogicalPlan::CreateIndex {
            name,
            table,
            columns,
            unique,
            backend,
        } => Ok(PhysicalPlan::CreateIndex {
            name,
            table,
            columns,
            unique,
            backend,
        }),
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
            let inner_phys = plan_inner(*inner, catalog, selection)?;
            Ok(PhysicalPlan::Explain(render_explain(&inner_phys)))
        }
        LogicalPlan::Select {
            table,
            joins,
            projection,
            aggregates,
            select_list,
            filter,
            order_by,
            having,
            limit,
        } => {
            let physop = plan_select(
                table,
                joins,
                projection,
                aggregates,
                select_list,
                filter,
                order_by,
                having,
                limit,
                catalog,
                selection,
            )?;
            Ok(PhysicalPlan::Query(physop))
        }
        LogicalPlan::Insert { table, rows } => {
            let physop = plan_insert(table, rows, catalog)?;
            Ok(PhysicalPlan::Query(physop))
        }
        LogicalPlan::Update {
            table,
            set_clauses,
            filter,
        } => {
            let physop = plan_update(table, set_clauses, filter, catalog)?;
            Ok(PhysicalPlan::Query(physop))
        }
        LogicalPlan::Delete { table, filter } => {
            let physop = plan_delete(table, filter, catalog)?;
            Ok(PhysicalPlan::Query(physop))
        }
    }
}

// CLIPPY-ALLOW(too_many_arguments): a SELECT carries table/joins/projection/
// aggregates/filter/order/limit plus catalog + join selection; a params
// struct would just rename the same fields without simplifying callers.
#[allow(clippy::too_many_arguments)]
fn plan_select<CatE>(
    table_name: String,
    joins: Vec<crate::sql::ir::logical::JoinClause>,
    projection: Vec<usize>,
    aggregates: Vec<crate::sql::ir::logical::AggregateSpec>,
    select_list: Vec<Expression>,
    filter: Option<Predicate>,
    order_by: Vec<(usize, crate::sql::ir::logical::OrderDir)>,
    having: Option<Predicate>,
    limit: Option<usize>,
    catalog: &Catalog<CatE>,
    selection: &JoinSelection,
) -> Result<PhysOp>
where
    CatE: StorageEngine,
{
    let left_schema = catalog.get_table(&table_name)?;
    let left_table_id = left_schema.table_id;
    let left_indexes = catalog.indexes_for_table(left_schema.table_id, &left_schema)?;

    // Per-table column ranges (left = index 0, then each join's right table) —
    // used to route each WHERE conjunct to the table it constrains.
    let ranges = table_ranges(&left_schema, &joins, catalog)?;

    // Build the left leaf and collect the conjuncts we can't place on it into
    // `residual`. With no joins the whole WHERE scopes to the one table. With
    // joins (Phase B) we push the left-table-only conjuncts onto the left leaf
    // and leave the rest in `residual` — which the join loop then drains
    // (promoting an equi-key, pushing right-table predicates), and whatever
    // survives becomes a Filter on top.
    let mut current: PhysOp;
    let mut residual: Vec<Predicate> = Vec::new();
    if joins.is_empty() {
        if let Some(pred) = filter {
            if let Some(pk) = try_lower_pk_lookup(&pred, &left_schema) {
                current = PhysOp::PkLookup {
                    table: table_name.clone(),
                    pk,
                };
            } else {
                match try_lower_index_predicate(pred, &left_indexes) {
                    IndexLowering::Matched {
                        handle,
                        prefix,
                        recheck,
                    } => {
                        current = PhysOp::IndexScan {
                            table: table_name.clone(),
                            index: handle.def.name.clone(),
                            prefix,
                        };
                        // MVCC recheck (see IndexLowering::Matched). With no
                        // joins, `residual` becomes the Filter directly above
                        // this leaf.
                        residual.push(recheck);
                    }
                    IndexLowering::Unmatched(pred) => {
                        current = PhysOp::SeqScan {
                            table: table_name.clone(),
                        };
                        residual.push(pred);
                    }
                }
            }
        } else {
            current = PhysOp::SeqScan {
                table: table_name.clone(),
            };
        }
    } else {
        let mut left_preds = Vec::new();
        for conjunct in filter.map(flatten_conjuncts).unwrap_or_default() {
            let mut cols = Vec::new();
            referenced_columns(&conjunct, &mut cols);
            match bucket_of(&cols, &ranges) {
                Bucket::SingleTable(0) => left_preds.push(conjunct),
                _ => residual.push(conjunct),
            }
        }
        current = build_left_leaf(left_preds, &left_schema, &table_name, &left_indexes);
    }

    // P13.1 + Phase C: chain joins onto the left side. For each join we first
    // promote a residual equi-predicate into the join's ON (so a comma join
    // `FROM a, b WHERE … a.k = b.k …` gets a real join key) and push that
    // table's single-table predicates onto its inner leaf; then pick the
    // algorithm — INLJ when the inner is indexed on the key, HashJoin for an
    // equi-key with no usable index (Phase D), else NestedLoopJoin.
    let mut outer_offset = left_schema.columns.len();
    // Running output-cardinality estimate — used only in cost mode. Seeded from
    // the left table's base row count. NOTE (P14.13a): ignores narrowing from
    // the left's filter; an over-estimate only biases toward Hash (the safe
    // direction). Exact figure arrives with the P14.12 table-relative refactor.
    let mut outer_card: f64 = match selection {
        JoinSelection::CostBased { stats, .. } => stats.row_count(left_table_id),
        JoinSelection::Heuristic => 0.0,
    };
    for (i, join) in joins.into_iter().enumerate() {
        let right_range = &ranges[i + 1];
        let right_schema = catalog.get_table(&join.right_table)?;
        let right_indexes = catalog.indexes_for_table(right_schema.table_id, &right_schema)?;
        let right_cols = right_schema.columns.len();
        let right_table = join.right_table.clone();
        let join_kind = join.kind;

        // Phase C.1: promote a residual equi-predicate connecting the outer to
        // this right table into the join's ON — only when the join has no
        // explicit ON (a comma/cross join). A LEFT OUTER join always carries an
        // ON (binder invariant), so this never fires for one.
        let mut on = join.on;
        if on.is_none() {
            if let Some(pos) = residual.iter().position(|c| {
                extract_equi_join_keys(c, outer_offset).is_some()
                    && predicate_touches_range(c, right_range)
            }) {
                on = Some(residual.remove(pos));
            }
        }

        // Phase C.2: pull this right table's single-table predicates out of the
        // residual to push onto its inner leaf. They carry global join-tuple
        // indices; `build_right_leaf` rebases them to the table's local columns.
        //
        // R2 (H3b): this pushdown is INVALID below a LEFT OUTER join — the
        // right side is null-padded, and a WHERE conjunct on a right column
        // must run ABOVE the pad (it drops padded rows), never below it (which
        // would re-filter the right table before padding and change which left
        // rows pad). So for an outer join we push NOTHING onto the inner leaf;
        // right-referencing conjuncts stay in `residual` → a Filter above the
        // join. Left-side pushdown stays valid and already happened at the left
        // leaf. Gated on kind, not incidental.
        let mut right_preds = Vec::new();
        if join_kind == JoinKind::Inner {
            let mut keep = Vec::new();
            for c in residual.drain(..) {
                let mut cols = Vec::new();
                referenced_columns(&c, &mut cols);
                let right_only = !cols.is_empty()
                    && cols
                        .iter()
                        .all(|&col| col >= right_range.start && col < right_range.end);
                if right_only {
                    right_preds.push(c);
                } else {
                    keep.push(c);
                }
            }
            residual = keep;
        }

        let next: PhysOp = match join_kind {
            // LEFT OUTER (H3b): only two algorithms are candidates —
            // hash-outer (with a residual ON) when a single equi conjunct is
            // extractable, else NLJ-outer running the full ON per pair. INLJ
            // and MergeJoin are STRUCTURALLY excluded (never candidates), not
            // guarded at runtime: neither can null-pad. `right_preds` is empty
            // here (R2 gated the pushdown off), so the inner leaf is unfiltered.
            JoinKind::LeftOuter => {
                let on_pred = on.expect("binder guarantees a LEFT OUTER join carries an ON");
                let split = split_outer_hash_on(&on_pred, outer_offset);
                let use_hash = match selection {
                    JoinSelection::Heuristic => split.is_some(),
                    JoinSelection::CostBased { cost_model, stats } => {
                        let inner_card = stats.row_count(right_schema.table_id);
                        let edge_sel = match &split {
                            Some(((_, inner_col), _)) => join_selectivity(
                                0,
                                stats.ndv(right_schema.table_id, *inner_col as u32),
                            ),
                            None => 1.0,
                        };
                        // Choose with the pre-join outer cardinality; hash-outer
                        // only competes when an equi key exists.
                        let chosen = split.is_some()
                            && matches!(
                                choose_join_algorithm(
                                    *cost_model,
                                    outer_card,
                                    inner_card,
                                    edge_sel,
                                    true,
                                    false,
                                ),
                                JoinAlgorithm::Hash
                            );
                        // A LEFT OUTER emits at least every outer row — floor the
                        // running estimate at `outer_card`.
                        outer_card = (outer_card * inner_card * edge_sel)
                            .max(outer_card)
                            .max(1.0);
                        chosen
                    }
                };
                let inner_leaf = build_right_leaf(
                    right_preds,
                    &right_schema,
                    &right_table,
                    &right_indexes,
                    right_range.start,
                );
                if use_hash {
                    let ((outer_col, inner_col), residual_on) =
                        split.expect("hash-outer chosen only when an equi key exists");
                    PhysOp::HashJoin {
                        outer: Box::new(current),
                        inner: Box::new(inner_leaf),
                        outer_key_col: outer_col,
                        inner_key_col: inner_col,
                        kind: JoinKind::LeftOuter,
                        residual: residual_on,
                    }
                } else {
                    PhysOp::NestedLoopJoin {
                        outer: Box::new(current),
                        inner: Box::new(inner_leaf),
                        on: Some(on_pred),
                        kind: JoinKind::LeftOuter,
                    }
                }
            }
            // Phase 11 heuristic + Phase D: INLJ when the inner is indexed on
            // the key, HashJoin for an equi-key without a usable index, else NLJ.
            JoinKind::Inner => match selection {
                JoinSelection::Heuristic => {
                    let inlj = on
                        .as_ref()
                        .and_then(|p| try_match_inlj(p, outer_offset, &right_indexes));
                    if let Some((outer_col, handle)) = inlj {
                        // INLJ probes the index; its right predicates can't ride the
                        // probe, so they return to the residual (a Filter on top).
                        residual.extend(right_preds);
                        PhysOp::IndexNestedLoopJoin {
                            outer: Box::new(current),
                            inner_table: right_table,
                            inner_index: handle.def.name.clone(),
                            outer_key_cols: vec![outer_col],
                        }
                    } else if let Some((outer_col, inner_col)) = on
                        .as_ref()
                        .and_then(|p| extract_equi_join_keys(p, outer_offset))
                    {
                        PhysOp::HashJoin {
                            outer: Box::new(current),
                            inner: Box::new(build_right_leaf(
                                right_preds,
                                &right_schema,
                                &right_table,
                                &right_indexes,
                                right_range.start,
                            )),
                            outer_key_col: outer_col,
                            inner_key_col: inner_col,
                            kind: JoinKind::Inner,
                            residual: None,
                        }
                    } else {
                        PhysOp::NestedLoopJoin {
                            outer: Box::new(current),
                            inner: Box::new(build_right_leaf(
                                right_preds,
                                &right_schema,
                                &right_table,
                                &right_indexes,
                                right_range.start,
                            )),
                            on,
                            kind: JoinKind::Inner,
                        }
                    }
                }
                // P14.13a cost-based: cheapest of NLJ / Hash / INLJ, textual order
                // preserved. Now reads the promoted `on` and pushes right predicates.
                JoinSelection::CostBased { cost_model, stats } => {
                    let keys = on
                        .as_ref()
                        .and_then(|p| extract_equi_join_keys(p, outer_offset));
                    let inlj = on
                        .as_ref()
                        .and_then(|p| try_match_inlj(p, outer_offset, &right_indexes));
                    let inner_card = stats.row_count(right_schema.table_id);
                    // NOTE (P14.13a): the outer join key can't be mapped back to a
                    // (table, col) for its NDV until P14.12, so selectivity uses the
                    // inner key's NDV only — adequate for Hash-vs-NLJ (cardinality-
                    // driven).
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
                            let (outer_col, handle) =
                                inlj.expect("INLJ chosen only when available");
                            residual.extend(right_preds);
                            PhysOp::IndexNestedLoopJoin {
                                outer: Box::new(current),
                                inner_table: right_table,
                                inner_index: handle.def.name.clone(),
                                outer_key_cols: vec![outer_col],
                            }
                        }
                        JoinAlgorithm::Hash => {
                            let (outer_col, inner_col) =
                                keys.expect("Hash chosen only when equi-keys exist");
                            PhysOp::HashJoin {
                                outer: Box::new(current),
                                inner: Box::new(build_right_leaf(
                                    right_preds,
                                    &right_schema,
                                    &right_table,
                                    &right_indexes,
                                    right_range.start,
                                )),
                                outer_key_col: outer_col,
                                inner_key_col: inner_col,
                                kind: JoinKind::Inner,
                                residual: None,
                            }
                        }
                        JoinAlgorithm::NestedLoop => PhysOp::NestedLoopJoin {
                            outer: Box::new(current),
                            inner: Box::new(build_right_leaf(
                                right_preds,
                                &right_schema,
                                &right_table,
                                &right_indexes,
                                right_range.start,
                            )),
                            on,
                            kind: JoinKind::Inner,
                        },
                    }
                }
            },
        };
        outer_offset += right_cols;
        current = next;
    }

    Ok(apply_select_spine(
        current,
        residual,
        aggregates,
        order_by,
        having,
        projection,
        select_list,
        limit,
    ))
}

/// Apply the fixed statement spine above an optimized core (D3): residual
/// Filter → HashAggregate → HAVING Filter → Sort → Projection/Compute →
/// Limit. Factored from `plan_select`'s tail (T17-A.4) so the memo
/// planner's emission applies the identical spine; column indices in every
/// argument must already be in the core's output coordinates — except
/// `having` (and `select_list`/`order_by` when aggregated), whose indices
/// are in the aggregate OUTPUT row (the binder's coordinate rule), which no
/// core optimization can move.
// CLIPPY-ALLOW(too_many_arguments): the spine mirrors the Select display
// fields (residual/aggregates/order_by/having/projection/select_list/limit);
// a params struct would just rename them without simplifying the two callers.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_select_spine(
    mut current: PhysOp,
    residual: Vec<Predicate>,
    aggregates: Vec<crate::sql::ir::logical::AggregateSpec>,
    order_by: Vec<(usize, crate::sql::ir::logical::OrderDir)>,
    having: Option<Predicate>,
    projection: Vec<usize>,
    select_list: Vec<Expression>,
    limit: Option<usize>,
) -> PhysOp {
    // Binder invariant: HAVING only exists alongside aggregates.
    assert!(having.is_none() || !aggregates.is_empty());
    // Negative space (binder invariant): a non-empty `select_list` is a
    // display over either the aggregate output row (grouped keeps
    // `projection` as its keys) or, when unaggregated, a core whose columns
    // the Compute reads directly — in which case `projection` is empty (the
    // Compute subsumes the Projection).
    assert!(select_list.is_empty() || (!aggregates.is_empty() || projection.is_empty()));
    if let Some(pred) = and_all(residual) {
        current = PhysOp::Filter {
            input: Box::new(current),
            predicate: pred,
        };
    }
    let aggregated = !aggregates.is_empty();
    if aggregated {
        // P13.4/H1: with GROUP BY, the projected plain columns ARE the
        // group keys (binder contract), so `projection` doubles as
        // `group_by` — empty for whole-table aggregation. Either way the
        // aggregate's output is already in final column order (keys ++
        // aggregates), so no trailing Projection runs above it.
        current = PhysOp::HashAggregate {
            input: Box::new(current),
            group_by: projection.clone(),
            aggregates,
        };
        if let Some(pred) = having {
            // HAVING filters aggregate output rows, so it sits directly
            // above the aggregate and below Sort/Limit.
            current = PhysOp::Filter {
                input: Box::new(current),
                predicate: pred,
            };
        }
    }
    // P13.6: Sort BEFORE Projection so sort keys can reference columns
    // even if they're not in the projection (TPC-C Payment does this).
    // Binder resolves ORDER BY indices against the pre-projection scope —
    // or against the aggregate output row when aggregated (coordinate
    // rule), where the Sort correctly sits above the HashAggregate.
    if !order_by.is_empty() {
        current = PhysOp::Sort {
            input: Box::new(current),
            keys: order_by,
        };
    }
    if !aggregated && !projection.is_empty() {
        current = PhysOp::Projection {
            input: Box::new(current),
            cols: projection,
        };
    }
    // H2b: the computed display sits AFTER Sort (sort keys are pre-Compute
    // coordinates — same reasoning as Sort-before-Projection above) and
    // BEFORE Limit. A non-empty `select_list` subsumes the bare Projection,
    // so the two are mutually exclusive above (the assert guarantees it).
    if !select_list.is_empty() {
        current = PhysOp::Compute {
            input: Box::new(current),
            exprs: select_list,
        };
    }
    if let Some(n) = limit {
        current = PhysOp::Limit {
            input: Box::new(current),
            max_rows: n,
        };
    }
    current
}

/// Extract `(outer_col_global, inner_col_local)` from an equi-join ON
/// predicate `outer.col = inner.col`. Mirrors `try_match_inlj` but needs
/// no index — it drives `HashJoin`. `None` for non-equi or composite
/// predicates (those stay on `NestedLoopJoin`).
fn extract_equi_join_keys(pred: &Predicate, outer_offset: usize) -> Option<(usize, usize)> {
    let (l, r) = match pred {
        Predicate::Compare {
            op: crate::sql::ir::expr::CompareOp::Eq,
            left: crate::sql::ir::expr::Expression::Column(l),
            right: crate::sql::ir::expr::Expression::Column(r),
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

/// Split a LEFT OUTER join's ON into a single hashable equi conjunct and
/// the residual (the remaining conjuncts, ANDed) for a hash-outer lowering.
/// Returns `((outer_col, inner_col_local), residual)` when some conjunct is
/// a `Column = Column` equi across the outer/inner boundary; `None` when
/// none is — the caller then lowers NLJ-outer, running the full ON per
/// pair. The residual keeps global concatenated-tuple coordinates: the
/// operator evaluates it over `outer ++ inner`, exactly like the NLJ
/// predicate. R1: the equi conjunct + residual together are the WHOLE ON,
/// so nothing is dropped or hoisted — the compound ON stays at the join.
fn split_outer_hash_on(
    on: &Predicate,
    outer_offset: usize,
) -> Option<((usize, usize), Option<Predicate>)> {
    let conjuncts = flatten_conjuncts(on.clone());
    let pos = conjuncts
        .iter()
        .position(|c| extract_equi_join_keys(c, outer_offset).is_some())?;
    let keys =
        extract_equi_join_keys(&conjuncts[pos], outer_offset).expect("position just matched");
    let residual: Vec<Predicate> = conjuncts
        .into_iter()
        .enumerate()
        .filter_map(|(i, c)| (i != pos).then_some(c))
        .collect();
    Some((keys, and_all(residual)))
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
            op: crate::sql::ir::expr::CompareOp::Eq,
            left: crate::sql::ir::expr::Expression::Column(l),
            right: crate::sql::ir::expr::Expression::Column(r),
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

/// Try to lower a `WHERE` clause into a primary-key point lookup. Recognized
/// shape: a single-column PK pinned by `Column(pk) = Literal(v)` (or the
/// mirror). Returns the (PK-typed) key on match. Composite PKs and non-equality
/// predicates fall through to the index / scan paths. This is the strongest
/// access path for `WHERE pk = …`, so callers try it first.
/// `pub(crate)`: the memo planner's leaf candidates share this lowering
/// verbatim (D6) — re-implementing it is forbidden (recheck bug class).
pub(crate) fn try_lower_pk_lookup(
    pred: &Predicate,
    schema: &Schema,
) -> Option<Vec<crate::types::Value>> {
    // First cut: single-column primary keys only.
    if schema.primary_key.len() != 1 {
        return None;
    }
    let pk_col = schema.primary_key[0];
    let value = match pred {
        Predicate::Compare {
            op: crate::sql::ir::expr::CompareOp::Eq,
            left: crate::sql::ir::expr::Expression::Column(i),
            right: crate::sql::ir::expr::Expression::Literal(v),
        }
        | Predicate::Compare {
            op: crate::sql::ir::expr::CompareOp::Eq,
            left: crate::sql::ir::expr::Expression::Literal(v),
            right: crate::sql::ir::expr::Expression::Column(i),
        } if *i == pk_col => v,
        _ => return None,
    };
    // The key encoder demands the value's type match the PK column exactly
    // (e.g. an unconstrained `1` literal binds as Int64 but a w_id PK is Int32),
    // so coerce; bail to scan+filter if the value can't be represented.
    let coerced = value.coerce_exact(&schema.columns[pk_col].ty)?;
    Some(vec![coerced])
}

/// Result of attempting to lower a `WHERE` clause into an IndexScan.
/// `pub(crate)`: shared with the memo planner's leaf candidates (D6).
pub(crate) enum IndexLowering {
    /// A single-column equality predicate matched a single-column index.
    /// `handle` is the index; `prefix` is the literal value to scan for.
    /// `recheck` is the original predicate, which the caller MUST re-apply
    /// as a Filter above the IndexScan: secondary indexes are unversioned
    /// (raw engine, physical delete on update) while table reads are MVCC,
    /// so a stale index entry can dereference to a snapshot-visible row
    /// whose current indexed value no longer equals `prefix`. The recheck
    /// drops those false positives (E1/O12).
    Matched {
        handle: crate::table::IndexHandle,
        prefix: Vec<crate::types::Value>,
        recheck: Predicate,
    },
    /// No index matched. Original predicate returned unchanged so the
    /// caller can drop it onto a `Filter` on top of `SeqScan`.
    Unmatched(Predicate),
}

/// Try to lower `pred` into an IndexScan against any of `indexes`. First
/// cut handles only the simplest indexable shape: a single equality
/// between one indexed column and a literal. Composite indexes, range
/// predicates, and AND-decomposition land in later phases.
/// `pub(crate)`: shared with the memo planner's leaf candidates (D6).
pub(crate) fn try_lower_index_predicate(
    pred: Predicate,
    indexes: &[crate::table::IndexHandle],
) -> IndexLowering {
    // Extract `(col_idx, value)` from `Compare(Eq, Column(i), Literal(v))`
    // and the mirror `Compare(Eq, Literal(v), Column(i))`.
    let extracted = match &pred {
        Predicate::Compare {
            op: crate::sql::ir::expr::CompareOp::Eq,
            left: crate::sql::ir::expr::Expression::Column(i),
            right: crate::sql::ir::expr::Expression::Literal(v),
        } => Some((*i, v.clone())),
        Predicate::Compare {
            op: crate::sql::ir::expr::CompareOp::Eq,
            left: crate::sql::ir::expr::Expression::Literal(v),
            right: crate::sql::ir::expr::Expression::Column(i),
        } => Some((*i, v.clone())),
        _ => None,
    };

    if let Some((col_idx, lit)) = extracted {
        for ix in indexes {
            // Only single-column indexes match this simple shape.
            if ix.def.columns == [col_idx] {
                // The IndexScan prefix is key-encoded strictly against the
                // indexed column's type, so lower only when the literal is
                // exactly representable in it. This must live HERE, not
                // just in the binder: prepared statements substitute
                // parameters after binding, so an Int64-bound `$1` against
                // an Int32 index reaches this point un-narrowed. A
                // non-representable literal (`WHERE int32_col = 5e9`)
                // falls back to SeqScan + Filter → correctly zero rows.
                match lit.coerce_exact(&ix.key_types[0]) {
                    Some(coerced) => {
                        return IndexLowering::Matched {
                            handle: ix.clone(),
                            prefix: vec![coerced],
                            recheck: pred,
                        };
                    }
                    None => break,
                }
            }
        }
    }
    IndexLowering::Unmatched(pred)
}

// ---------------------------------------------------------------------------
// Predicate pushdown (Phase A helpers + Phase B left-leaf lowering).
//
// `plan_select` used to drop the whole `WHERE` as one `Filter` on top of the
// joins — for `FROM a, b WHERE a.x = $1 AND …` that means the join runs over
// the *unfiltered* left table. These helpers split the conjunction and route
// each conjunct to the table it constrains, so a single-table predicate
// shrinks its base scan before the join sees it. (Right-side pushdown and
// join-key promotion are Phase C; see `docs/plan-predicate-pushdown.md`.)
// ---------------------------------------------------------------------------

/// Flatten a predicate's top-level `AND` chain into its conjuncts. `Or`, `Not`,
/// and `Compare` are leaf conjuncts — an `OR`/`NOT` is one indivisible unit, we
/// never push half of it. Iterative (an explicit stack) so the recursion depth
/// is the parsed `WHERE`'s `AND` nesting, not the call stack.
/// `pub(crate)`: also used by `selinger.rs` to route WHERE conjuncts into
/// per-relation selectivities (O10).
pub(crate) fn flatten_conjuncts(pred: Predicate) -> Vec<Predicate> {
    let mut out = Vec::new();
    let mut stack = vec![pred];
    while let Some(p) = stack.pop() {
        match p {
            Predicate::And(a, b) => {
                // Push right then left so `pop` yields left-to-right order.
                stack.push(*b);
                stack.push(*a);
            }
            leaf => out.push(leaf),
        }
    }
    out
}

/// Collect every column index a predicate references (into the join's running
/// tuple). `Parameter`s and `Literal`s contribute nothing; by plan time
/// parameters are substituted to literals anyway. Duplicates are allowed — the
/// caller only checks each index against a range.
pub(crate) fn referenced_columns(pred: &Predicate, out: &mut Vec<usize>) {
    match pred {
        Predicate::Compare { left, right, .. } => {
            columns_in_expr(left, out);
            columns_in_expr(right, out);
        }
        Predicate::And(a, b) | Predicate::Or(a, b) => {
            referenced_columns(a, out);
            referenced_columns(b, out);
        }
        Predicate::Not(p) => referenced_columns(p, out),
        Predicate::Like { expr, .. } => columns_in_expr(expr, out),
        Predicate::IsNull(expr) => columns_in_expr(expr, out),
    }
}

/// Column indices referenced by an expression (recursing through arithmetic).
fn columns_in_expr(expr: &Expression, out: &mut Vec<usize>) {
    match expr {
        Expression::Column(i) => out.push(*i),
        Expression::Literal(_) | Expression::Parameter(_) => {}
        Expression::BinaryOp { left, right, .. } => {
            columns_in_expr(left, out);
            columns_in_expr(right, out);
        }
        Expression::ExtractYear(arg) => columns_in_expr(arg, out),
        // A CASE references columns in its branch conditions (via
        // `referenced_columns`) and its branch/else results.
        Expression::Case {
            branches,
            else_expr,
        } => {
            for (pred, result) in branches {
                referenced_columns(pred, out);
                columns_in_expr(result, out);
            }
            if let Some(e) = else_expr {
                columns_in_expr(e, out);
            }
        }
    }
}

/// A table's column range `[start, end)` in the join's running output tuple.
struct TableRange {
    start: usize,
    end: usize,
}

/// Column ranges for the left table (index 0) then each joined right table, in
/// textual order — mirroring how `plan_select` grows `outer_offset`.
fn table_ranges<CatE>(
    left: &Schema,
    joins: &[crate::sql::ir::logical::JoinClause],
    catalog: &Catalog<CatE>,
) -> Result<Vec<TableRange>>
where
    CatE: StorageEngine,
{
    let mut ranges = Vec::with_capacity(1 + joins.len());
    let mut end = left.columns.len();
    ranges.push(TableRange { start: 0, end });
    for join in joins {
        let right = catalog.get_table(&join.right_table)?;
        let start = end;
        end = start + right.columns.len();
        ranges.push(TableRange { start, end });
    }
    Ok(ranges)
}

/// Where a conjunct's columns fall. `SingleTable(k)` iff *every* referenced
/// column is within table `k`'s range; `Spans` if they cross tables (a join or
/// otherwise un-pushable predicate); `NoColumns` for a constant predicate.
enum Bucket {
    SingleTable(usize),
    Spans,
    NoColumns,
}

fn bucket_of(cols: &[usize], ranges: &[TableRange]) -> Bucket {
    let first = match cols.first() {
        Some(&c) => c,
        None => return Bucket::NoColumns,
    };
    let table = match ranges
        .iter()
        .position(|r| first >= r.start && first < r.end)
    {
        Some(t) => t,
        None => return Bucket::Spans,
    };
    let r = &ranges[table];
    if cols.iter().all(|&c| c >= r.start && c < r.end) {
        Bucket::SingleTable(table)
    } else {
        Bucket::Spans
    }
}

/// Re-combine conjuncts into one left-associative `AND`, or `None` if empty.
/// `pub(crate)`: also used by the memo planner's leaf candidates (D6).
pub(crate) fn and_all(preds: Vec<Predicate>) -> Option<Predicate> {
    preds
        .into_iter()
        .reduce(|acc, p| Predicate::And(Box::new(acc), Box::new(p)))
}

/// Build the left leaf scan from the predicates that reference only the left
/// table: lower one to a `PkLookup`/`IndexScan` access path when possible, and
/// drop the rest onto a leaf `Filter`. Left columns sit at offset 0, so the
/// global column indices are already local — no rebasing needed.
fn build_left_leaf(
    left_preds: Vec<Predicate>,
    schema: &Schema,
    table_name: &str,
    indexes: &[crate::table::IndexHandle],
) -> PhysOp {
    let mut access: Option<PhysOp> = None;
    let mut leftover: Vec<Predicate> = Vec::new();
    for pred in left_preds {
        if access.is_some() {
            leftover.push(pred);
            continue;
        }
        if let Some(pk) = try_lower_pk_lookup(&pred, schema) {
            access = Some(PhysOp::PkLookup {
                table: table_name.to_string(),
                pk,
            });
            continue;
        }
        match try_lower_index_predicate(pred, indexes) {
            IndexLowering::Matched {
                handle,
                prefix,
                recheck,
            } => {
                access = Some(PhysOp::IndexScan {
                    table: table_name.to_string(),
                    index: handle.def.name.clone(),
                    prefix,
                });
                // MVCC recheck (see IndexLowering::Matched) — ANDed into the
                // leaf Filter alongside the non-lowered conjuncts.
                leftover.push(recheck);
            }
            IndexLowering::Unmatched(pred) => leftover.push(pred),
        }
    }
    let mut leaf = access.unwrap_or(PhysOp::SeqScan {
        table: table_name.to_string(),
    });
    if let Some(pred) = and_all(leftover) {
        leaf = PhysOp::Filter {
            input: Box::new(leaf),
            predicate: pred,
        };
    }
    leaf
}

/// Whether any column the predicate references falls in `range`.
fn predicate_touches_range(pred: &Predicate, range: &TableRange) -> bool {
    let mut cols = Vec::new();
    referenced_columns(pred, &mut cols);
    cols.iter().any(|&c| c >= range.start && c < range.end)
}

/// Shift every column index in an expression by `delta` — negative to
/// rebase a join-tuple index down to a table-local one, positive to
/// raise a local index into a join tuple. One walk for both directions
/// (review fix #5): a new `Expression` variant is handled here exactly
/// once. Underflow is a coordinate-space bug and crashes.
fn shift_expr(expr: Expression, delta: isize) -> Expression {
    match expr {
        Expression::Column(i) => {
            let shifted = i as isize + delta;
            assert!(shifted >= 0, "column {i} shifted below zero by {delta}");
            Expression::Column(shifted as usize)
        }
        lit_or_param @ (Expression::Literal(_) | Expression::Parameter(_)) => lit_or_param,
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op,
            left: Box::new(shift_expr(*left, delta)),
            right: Box::new(shift_expr(*right, delta)),
        },
        Expression::ExtractYear(arg) => Expression::ExtractYear(Box::new(shift_expr(*arg, delta))),
        Expression::Case {
            branches,
            else_expr,
        } => Expression::Case {
            branches: branches
                .into_iter()
                .map(|(pred, result)| (shift_predicate(pred, delta), shift_expr(result, delta)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(shift_expr(*e, delta))),
        },
    }
}

/// Shift every column in a predicate by `delta` (see `shift_expr`).
pub(crate) fn shift_predicate(pred: Predicate, delta: isize) -> Predicate {
    match pred {
        Predicate::Compare { op, left, right } => Predicate::Compare {
            op,
            left: shift_expr(left, delta),
            right: shift_expr(right, delta),
        },
        Predicate::And(a, b) => Predicate::And(
            Box::new(shift_predicate(*a, delta)),
            Box::new(shift_predicate(*b, delta)),
        ),
        Predicate::Or(a, b) => Predicate::Or(
            Box::new(shift_predicate(*a, delta)),
            Box::new(shift_predicate(*b, delta)),
        ),
        Predicate::Not(p) => Predicate::Not(Box::new(shift_predicate(*p, delta))),
        Predicate::Like { expr, pattern } => Predicate::Like {
            expr: shift_expr(expr, delta),
            pattern,
        },
        Predicate::IsNull(expr) => Predicate::IsNull(shift_expr(expr, delta)),
    }
}

/// Build a join's right (inner) leaf from the predicates that reference only
/// that table. The predicates carry *global* join-tuple column indices, so we
/// rebase them to the table's local indices (`- offset`) before lowering — the
/// rebasing is what `build_left_leaf` doesn't need (the left table is at
/// offset 0).
fn build_right_leaf(
    global_preds: Vec<Predicate>,
    schema: &Schema,
    table_name: &str,
    indexes: &[crate::table::IndexHandle],
    offset: usize,
) -> PhysOp {
    let local_preds = global_preds
        .into_iter()
        .map(|p| shift_predicate(p, -(offset as isize)))
        .collect();
    build_left_leaf(local_preds, schema, table_name, indexes)
}

fn plan_insert<CatE>(
    table_name: String,
    rows: Vec<Vec<crate::sql::ir::expr::Expression>>,
    catalog: &Catalog<CatE>,
) -> Result<PhysOp>
where
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
    // Validate the table exists at plan time (the builder would also catch a
    // missing table, but this preserves the plan-time error the prior
    // table-construction had).
    catalog.get_table(&table_name)?;
    Ok(PhysOp::Insert {
        table: table_name,
        rows,
    })
}

fn plan_update<CatE>(
    table_name: String,
    set_clauses: Vec<(usize, crate::sql::ir::expr::Expression)>,
    filter: Option<Predicate>,
    catalog: &Catalog<CatE>,
) -> Result<PhysOp>
where
    CatE: StorageEngine,
{
    let schema = catalog.get_table(&table_name)?;
    let indexes = catalog.indexes_for_table(schema.table_id, &schema)?;

    // Same IndexScan lowering as plan_select. Index-driven updates are
    // common in OLTP: `UPDATE customer SET … WHERE c_id = ?`.
    let mut child: PhysOp;
    let mut residual_filter: Option<Predicate> = filter;
    if let Some(pred) = residual_filter {
        if let Some(pk) = try_lower_pk_lookup(&pred, &schema) {
            child = PhysOp::PkLookup {
                table: table_name.clone(),
                pk,
            };
            residual_filter = None;
        } else {
            match try_lower_index_predicate(pred, &indexes) {
                IndexLowering::Matched {
                    handle,
                    prefix,
                    recheck,
                } => {
                    child = PhysOp::IndexScan {
                        table: table_name.clone(),
                        index: handle.def.name.clone(),
                        prefix,
                    };
                    // MVCC recheck (see IndexLowering::Matched). Vital here:
                    // a false positive would UPDATE the wrong row.
                    residual_filter = Some(recheck);
                }
                IndexLowering::Unmatched(pred) => {
                    child = PhysOp::SeqScan {
                        table: table_name.clone(),
                    };
                    residual_filter = Some(pred);
                }
            }
        }
    } else {
        child = PhysOp::SeqScan {
            table: table_name.clone(),
        };
    }
    if let Some(pred) = residual_filter {
        child = PhysOp::Filter {
            input: Box::new(child),
            predicate: pred,
        };
    }

    Ok(PhysOp::Update {
        table: table_name,
        input: Box::new(child),
        set: set_clauses,
    })
}

fn plan_delete<CatE>(
    table_name: String,
    filter: Option<Predicate>,
    catalog: &Catalog<CatE>,
) -> Result<PhysOp>
where
    CatE: StorageEngine,
{
    let schema = catalog.get_table(&table_name)?;
    let indexes = catalog.indexes_for_table(schema.table_id, &schema)?;

    let mut child: PhysOp;
    let mut residual_filter: Option<Predicate> = filter;
    if let Some(pred) = residual_filter {
        if let Some(pk) = try_lower_pk_lookup(&pred, &schema) {
            child = PhysOp::PkLookup {
                table: table_name.clone(),
                pk,
            };
            residual_filter = None;
        } else {
            match try_lower_index_predicate(pred, &indexes) {
                IndexLowering::Matched {
                    handle,
                    prefix,
                    recheck,
                } => {
                    child = PhysOp::IndexScan {
                        table: table_name.clone(),
                        index: handle.def.name.clone(),
                        prefix,
                    };
                    // MVCC recheck (see IndexLowering::Matched). Vital here:
                    // a false positive would DELETE the wrong row.
                    residual_filter = Some(recheck);
                }
                IndexLowering::Unmatched(pred) => {
                    child = PhysOp::SeqScan {
                        table: table_name.clone(),
                    };
                    residual_filter = Some(pred);
                }
            }
        }
    } else {
        child = PhysOp::SeqScan {
            table: table_name.clone(),
        };
    }
    if let Some(pred) = residual_filter {
        child = PhysOp::Filter {
            input: Box::new(child),
            predicate: pred,
        };
    }
    Ok(PhysOp::Delete {
        table: table_name,
        input: Box::new(child),
    })
}

/// Render an EXPLAIN string for a `PhysicalPlan`. For query plans this walks
/// the `PhysOp` IR directly (engine-free — no build, no scan); for descriptors
/// it emits a one-line summary.
/// `pub(crate)`: the memo planner renders its own EXPLAIN arm (T17-A.6).
pub(crate) fn render_explain(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Query(physop) => physop.explain(0),
        PhysicalPlan::CreateTable { name, .. } => format!("CreateTable({})\n", name),
        PhysicalPlan::CreateIndex { name, table, .. } => {
            format!("CreateIndex({name} on {table})\n")
        }
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
    use std::sync::Arc;

    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{Schema, TableId};
    use crate::common::Error;
    use crate::engines::btree::BTreeEngine;
    use crate::execution::build::build_executor;
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
        plan(logical, &env.catalog).unwrap()
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
            PhysicalPlan::Query(physop) => {
                let tree = physop.explain(0);
                assert_eq!(tree, "SeqScan(warehouse)\n");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_select_with_where_to_filter_chain() {
        let env = setup();
        create_warehouse(&env);
        // Non-PK predicate (w_ytd is column 1, not the PK) → scan + filter.
        let p = plan_sql(&env, "SELECT w_id FROM warehouse WHERE w_ytd = 100");
        match p {
            PhysicalPlan::Query(physop) => {
                let tree = physop.explain(0);
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
    fn plans_select_with_pk_equality_lowers_to_pk_lookup() {
        let env = setup();
        create_warehouse(&env);
        // w_id is the PK → a point lookup, not scan + filter.
        let p = plan_sql(&env, "SELECT w_id FROM warehouse WHERE w_id = 1");
        match p {
            PhysicalPlan::Query(physop) => {
                let tree = physop.explain(0);
                let expected = "\
Projection([0])
  PkLookup(warehouse)
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
        // Non-PK predicate keeps the full scan + filter chain under the wrappers.
        let p = plan_sql(&env, "SELECT w_id FROM warehouse WHERE w_ytd = 100 LIMIT 3");
        match p {
            PhysicalPlan::Query(physop) => {
                let tree = physop.explain(0);
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
            PhysicalPlan::Query(physop) => {
                let mut exec = build_executor(&physop, &env.engine, &env.catalog).unwrap();
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
        if let PhysicalPlan::Query(physop) = seed {
            build_executor(&physop, &env.engine, &env.catalog)
                .unwrap()
                .next()
                .unwrap();
        }
        // UPDATE w_ytd to 9999 WHERE w_id = 1.
        let p = plan_sql(&env, "UPDATE warehouse SET w_ytd = 9999 WHERE w_id = 1");
        match p {
            PhysicalPlan::Query(physop) => {
                // Verify the explain shape (engine-free, off the IR).
                let tree = physop.explain(0);
                assert!(tree.starts_with("Update(warehouse, set_cols=[1])"));
                assert!(tree.contains("PkLookup(warehouse)"));
                let mut exec = build_executor(&physop, &env.engine, &env.catalog).unwrap();
                assert_eq!(exec.next().unwrap(), Some(vec![Value::Int64(1)]));
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
            PhysicalPlan::Query(physop) => {
                let tree = physop.explain(0);
                assert!(tree.starts_with("Delete(warehouse)"));
                assert!(tree.contains("PkLookup(warehouse)"));
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
                assert!(text.contains("PkLookup(warehouse)"));
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
        let p = plan(nested, &env.catalog).unwrap();
        match p {
            PhysicalPlan::Explain(text) => {
                assert!(text.starts_with("Explain\n"), "got: {}", text);
                assert!(
                    !text.contains("Explain[nested]"),
                    "old stub leaked: {}",
                    text
                );
                assert!(text.contains("  Projection"), "got: {}", text);
                assert!(text.contains("PkLookup(warehouse)"), "got: {}", text);
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

        let free = plan(logical_a, &env.catalog).unwrap();
        let via_strategy = RuleBasedPlanner.plan(logical_b, &env.catalog).unwrap();

        let (free_tree, strat_tree) = match (free, via_strategy) {
            (PhysicalPlan::Query(a), PhysicalPlan::Query(b)) => (a.explain(0), b.explain(0)),
            _ => panic!("expected two query plans"),
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
            select_list: vec![],
            filter: None,
            order_by: vec![],
            having: None,
            limit: None,
        };
        match plan(logical, &env.catalog) {
            Err(Error::TableNotFound { name }) => assert_eq!(name, "nonexistent"),
            other => panic!("expected TableNotFound, got {:?}", other.err()),
        }
    }

    // ---- Predicate-pushdown helpers (Phase A) ----

    fn col(i: usize) -> Expression {
        Expression::Column(i)
    }
    fn lit(n: i32) -> Expression {
        Expression::Literal(Value::Int32(n))
    }
    fn eq(l: Expression, r: Expression) -> Predicate {
        Predicate::Compare {
            op: crate::sql::ir::expr::CompareOp::Eq,
            left: l,
            right: r,
        }
    }
    fn and(a: Predicate, b: Predicate) -> Predicate {
        Predicate::And(Box::new(a), Box::new(b))
    }

    #[test]
    fn flatten_conjuncts_splits_only_top_level_and() {
        // `A AND B AND C` flattens to three conjuncts...
        let p = and(
            and(eq(col(0), lit(1)), eq(col(1), lit(2))),
            eq(col(2), lit(3)),
        );
        assert_eq!(flatten_conjuncts(p).len(), 3);
        // ...a bare compare is one conjunct...
        assert_eq!(flatten_conjuncts(eq(col(0), lit(1))).len(), 1);
        // ...and an `OR` is never split (one indivisible unit).
        let or = Predicate::Or(Box::new(eq(col(0), lit(1))), Box::new(eq(col(1), lit(2))));
        assert_eq!(flatten_conjuncts(and(eq(col(2), lit(3)), or)).len(), 2);
    }

    #[test]
    fn referenced_columns_walks_arithmetic_and_logic() {
        // Plain compare.
        let mut cols = Vec::new();
        referenced_columns(&eq(col(0), lit(5)), &mut cols);
        assert_eq!(cols, vec![0]);

        // Arithmetic on a side recurses: `col3 + col4 = col7`.
        let arith = Expression::BinaryOp {
            op: crate::sql::ir::expr::BinaryOp::Add,
            left: Box::new(col(3)),
            right: Box::new(col(4)),
        };
        let mut cols = Vec::new();
        referenced_columns(&eq(arith, col(7)), &mut cols);
        cols.sort();
        assert_eq!(cols, vec![3, 4, 7]);

        // `Not`/`And` recurse into their operands.
        let mut cols = Vec::new();
        let p = Predicate::Not(Box::new(and(eq(col(1), lit(0)), eq(col(2), lit(0)))));
        referenced_columns(&p, &mut cols);
        cols.sort();
        assert_eq!(cols, vec![1, 2]);
    }

    #[test]
    fn bucket_of_classifies_table_membership() {
        // Two tables: left = cols [0,2), right = cols [2,5).
        let ranges = vec![
            TableRange { start: 0, end: 2 },
            TableRange { start: 2, end: 5 },
        ];
        assert!(matches!(
            bucket_of(&[0, 1], &ranges),
            Bucket::SingleTable(0)
        ));
        assert!(matches!(
            bucket_of(&[2, 4], &ranges),
            Bucket::SingleTable(1)
        ));
        // Crosses both tables → a join/residual predicate.
        assert!(matches!(bucket_of(&[1, 3], &ranges), Bucket::Spans));
        // No columns → a constant predicate.
        assert!(matches!(bucket_of(&[], &ranges), Bucket::NoColumns));
        // Out of range → Spans (un-pushable, the safe default).
        assert!(matches!(bucket_of(&[9], &ranges), Bucket::Spans));
    }

    // ---- Phase B: left-table predicate pushdown (end-to-end) ----

    fn create_district(env: &TestEnv) {
        let schema = Schema {
            name: "district".into(),
            table_id: TableId(1),
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
                    name: "d_ytd".into(),
                    ty: ColumnType::Int64,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec![0],
        };
        env.catalog.create_table("district".into(), schema).unwrap();
    }

    /// Descend through `Projection`/`Filter` wrappers to the first other op.
    fn skip_to_join(op: &PhysOp) -> &PhysOp {
        match op {
            PhysOp::Projection { input, .. } | PhysOp::Filter { input, .. } => skip_to_join(input),
            other => other,
        }
    }

    #[test]
    fn join_promotes_equi_key_to_hashjoin_with_left_pushdown() {
        // `FROM warehouse, district WHERE w_ytd = 100 AND d_w_id = w_id`:
        //  - `w_ytd = 100` (warehouse-only) is pushed onto the left leaf (a
        //    `Filter`, since `w_ytd` is neither PK nor indexed) BELOW the join
        //    (Phase B).
        //  - `d_w_id = w_id` (an equi-predicate across the two tables) is
        //    promoted to the join key; with no index on `d_w_id` the join becomes
        //    a `HashJoin` (Phase C.1 + D). Nothing is left over → no top `Filter`.
        let env = setup();
        create_warehouse(&env);
        create_district(&env);
        let p = plan_sql(
            &env,
            "SELECT w_id FROM warehouse, district WHERE w_ytd = 100 AND d_w_id = w_id",
        );
        let physop = match p {
            PhysicalPlan::Query(op) => op,
            other => panic!("expected Query, got {:?}", std::mem::discriminant(&other)),
        };
        match skip_to_join(&physop) {
            PhysOp::HashJoin { outer, .. } => assert!(
                matches!(outer.as_ref(), PhysOp::Filter { .. }),
                "left predicate not pushed below the join; outer = {:?}",
                outer
            ),
            other => panic!("expected HashJoin (equi-key promoted), got {:?}", other),
        }
    }

    #[test]
    fn join_pushes_right_table_predicate_onto_the_inner() {
        // `d_ytd = 5` touches only `district` (the right table) → pushed onto the
        // HashJoin's inner leaf (rebased to district-local columns), so the inner
        // is a `Filter`, not a bare `SeqScan`. `d_w_id = w_id` is the join key.
        let env = setup();
        create_warehouse(&env);
        create_district(&env);
        let p = plan_sql(
            &env,
            "SELECT w_id FROM warehouse, district WHERE d_ytd = 5 AND d_w_id = w_id",
        );
        let physop = match p {
            PhysicalPlan::Query(op) => op,
            other => panic!("expected Query, got {:?}", std::mem::discriminant(&other)),
        };
        match skip_to_join(&physop) {
            PhysOp::HashJoin { inner, .. } => assert!(
                matches!(inner.as_ref(), PhysOp::Filter { .. }),
                "right predicate not pushed onto the inner; inner = {:?}",
                inner
            ),
            other => panic!("expected HashJoin, got {:?}", other),
        }
    }
}
