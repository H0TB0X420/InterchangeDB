//! Selinger-style cost-based planner.
//!
//! `SelingerPlanner` is the second `PlannerStrategy` impl. It carries a
//! `CostModel` and a `time_budget_ms`, and optimizes two axes:
//!
//!   - **Join order** (P14.13b): build the join graph from a `SELECT`,
//!     run the `enumerate_join_orders` DP, and if it finds a *strictly
//!     cheaper* left-deep order, rewrite the `LogicalPlan` into that
//!     order — reordering tables and remapping every column reference via
//!     `ColumnRemap`. The rewritten plan then flows through the *same*
//!     textual lowering path the rule-based planner uses, so there is no
//!     parallel executor-lowering to keep correct.
//!   - **Join algorithm** (P14.13a): the textual lowering then picks the
//!     cheapest of NLJ / Hash / INLJ per join under the cost model.
//!
//! ## Why reorder the logical plan, not the executor tree
//!
//! Rewriting `LogicalPlan → LogicalPlan` (reorder + `ColumnRemap`) lets
//! the existing, tested `plan_inner` lowering do all the executor
//! construction. The reorder is gated on *strictly cheaper than textual*
//! (`cost_of_order`), so tie-breaks — e.g. unanalyzed tables, all at the
//! default row count — never reorder, and the rule-based / TPC-C path is
//! untouched.
//!
//! ## When reordering is skipped (falls back to textual order)
//!
//! Non-`SELECT` statements; queries with no joins; any join whose `ON` is
//! not a single `col = col` equi-join (composite / non-equi / cross →
//! can't be a clean graph edge); and orders the DP doesn't find cheaper.

use std::time::Duration;

use crate::catalog::system_tables::ColumnStats;
use crate::catalog::{Catalog, TableId};
use crate::common::Result;
use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
use crate::sql::ir::logical::{AggregateSpec, JoinClause, JoinKind, LogicalPlan, OrderDir};
use crate::sql::optimizer::column_map::ColumnRemap;
use crate::sql::optimizer::cost::{CostModel, DefaultCostModel};
use crate::sql::optimizer::join_order::{
    cost_of_order, enumerate_join_orders, JoinEdge, JoinOrder, JoinRelation, RelId,
};
use crate::sql::optimizer::selectivity::estimate_predicate_selectivity;
use crate::sql::optimizer::stats::{CatalogStatsProvider, QueryStats};
use crate::sql::planner::{
    flatten_conjuncts, plan_inner, referenced_columns, shift_predicate, JoinSelection,
    PhysicalPlan, PlannerStrategy,
};
use crate::storage::StorageEngine;

/// Cost-based planner. `cost_model` ranks plans; `time_budget_ms` bounds
/// the join-order DP search (it bails to a greedy order when exceeded).
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
    fn plan<CatE>(&self, logical: LogicalPlan, catalog: &Catalog<CatE>) -> Result<PhysicalPlan>
    where
        CatE: StorageEngine,
    {
        // Snapshot stats once, then (P14.13b) optionally rewrite the plan
        // into a strictly-cheaper join order, then (P14.13a) lower it,
        // picking each join's algorithm by cost.
        let stats = gather_query_stats(&logical, catalog)?;
        let logical = maybe_reorder(
            logical,
            catalog,
            &stats,
            &self.cost_model,
            self.time_budget_ms,
        )?;
        let selection = JoinSelection::CostBased {
            cost_model: &self.cost_model,
            stats: &stats,
        };
        plan_inner(logical, catalog, &selection)
    }

    fn name(&self) -> &'static str {
        "selinger"
    }
}

/// Table names referenced by a query, for stats gathering: a `SELECT`'s
/// base table plus every join's right table (`EXPLAIN` unwraps to its
/// inner plan). Non-`SELECT` statements have no joins, so they need no
/// snapshot and return empty.
fn collect_select_tables(logical: &LogicalPlan) -> Vec<String> {
    match logical {
        LogicalPlan::Select { table, joins, .. } => {
            let mut names = vec![table.clone()];
            for j in joins {
                names.push(j.right_table.clone());
            }
            names
        }
        LogicalPlan::Explain(inner) => collect_select_tables(inner),
        _ => Vec::new(),
    }
}

/// Snapshot stats for every table a query touches (all columns of each),
/// through a `CatalogStatsProvider`. One pass; the cost-based join loop
/// then reads the owned `QueryStats`.
/// `pub(crate)`: the memo planner (T17-A.6) snapshots identically.
pub(crate) fn gather_query_stats<CatE: StorageEngine>(
    logical: &LogicalPlan,
    catalog: &Catalog<CatE>,
) -> Result<QueryStats> {
    let provider = CatalogStatsProvider::new(catalog);
    let mut requests_owned: Vec<(TableId, Vec<u32>)> = Vec::new();
    for name in collect_select_tables(logical) {
        let schema = catalog.get_table(&name)?;
        let column_count = schema.columns.len() as u32;
        requests_owned.push((schema.table_id, (0..column_count).collect()));
    }
    let requests: Vec<(TableId, &[u32])> = requests_owned
        .iter()
        .map(|(t, c)| (*t, c.as_slice()))
        .collect();
    QueryStats::gather(&provider, &requests)
}

// ===========================================================================
// P14.13b — join reordering as a LogicalPlan → LogicalPlan rewrite.
// ===========================================================================

/// The join graph extracted from a `SELECT`, plus the per-table metadata
/// the rewrite needs. Tables are indexed by `RelId` in textual order
/// (rel 0 = `FROM` table, rel i = `joins[i-1]`'s right table).
pub(crate) struct JoinGraph {
    pub(crate) relations: Vec<JoinRelation>,
    pub(crate) edges: Vec<JoinEdge>,
    /// Column count per table, textual order.
    widths: Vec<usize>,
    /// Tuple-global offset of each table's first column, textual order.
    textual_base: Vec<usize>,
    names: Vec<String>,
    aliases: Vec<Option<String>>,
}

/// If `logical` is a join `SELECT` (or `EXPLAIN` of one) whose DP-optimal
/// left-deep order is strictly cheaper than the textual order, return it
/// rewritten into that order (tables reordered, all column references
/// remapped). Otherwise return it unchanged — the textual lowering path
/// then handles algorithm selection.
fn maybe_reorder<CatE: StorageEngine>(
    logical: LogicalPlan,
    catalog: &Catalog<CatE>,
    stats: &QueryStats,
    cost_model: &dyn CostModel,
    time_budget_ms: u64,
) -> Result<LogicalPlan> {
    match logical {
        LogicalPlan::Explain(inner) => {
            let reordered = maybe_reorder(*inner, catalog, stats, cost_model, time_budget_ms)?;
            Ok(LogicalPlan::Explain(Box::new(reordered)))
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
            // No joins, or an `ON` we can't model as a clean equi-join
            // graph → leave textual order for P14.13a to lower.
            let graph = if joins.is_empty() {
                None
            } else {
                build_join_graph(&table, &joins, filter.as_ref(), catalog, stats)?
            };
            let Some(graph) = graph else {
                return Ok(LogicalPlan::Select {
                    table,
                    joins,
                    projection,
                    aggregates,
                    select_list,
                    filter,
                    order_by,
                    having,
                    limit,
                });
            };

            let budget = Some(Duration::from_millis(time_budget_ms));
            let best =
                enumerate_join_orders(&graph.relations, &graph.edges, stats, cost_model, budget);
            let textual: Vec<RelId> = (0..graph.relations.len()).collect();
            let textual_cost =
                cost_of_order(&textual, &graph.relations, &graph.edges, stats, cost_model);

            // Reorder only when the DP found a *strictly* cheaper order;
            // ties (e.g. unanalyzed, all-equal tables) keep textual order.
            let cheaper = match textual_cost {
                Some(tc) => cost_model.scalar(best.cost) < cost_model.scalar(tc),
                None => true,
            };
            if !cheaper {
                return Ok(LogicalPlan::Select {
                    table,
                    joins,
                    projection,
                    aggregates,
                    select_list,
                    filter,
                    order_by,
                    having,
                    limit,
                });
            }
            Ok(rewrite_select(
                &graph,
                &best.order,
                projection,
                aggregates,
                select_list,
                filter,
                order_by,
                having,
                limit,
            ))
        }
        other => Ok(other),
    }
}

/// Extract the equi-join graph from a `SELECT`'s tables and `ON` clauses.
/// Returns `None` (→ keep textual order) if any `ON` is missing or is not
/// a single `Column = Column` equi-join, or references columns out of an
/// expected (left-deep) ordering. `pub(crate)`: the memo normalizer
/// (T17-A.1) asserts edge-extraction parity against this function.
pub(crate) fn build_join_graph<CatE: StorageEngine>(
    table: &str,
    joins: &[JoinClause],
    filter: Option<&Predicate>,
    catalog: &Catalog<CatE>,
    stats: &QueryStats,
) -> Result<Option<JoinGraph>> {
    // No-reorder rule (H3b): outer joins are not commutative or associative
    // with the joins around them — reordering across one would change which
    // left rows get null-padded (a different query). The DP reorders Inner
    // joins only; ANY non-Inner kind bails the whole SELECT to textual
    // lowering, where the outer join keeps its written position. Explicit on
    // kind, not an incidental consequence of the equi-ON edge test below.
    if joins.iter().any(|j| j.kind != JoinKind::Inner) {
        return Ok(None);
    }
    let mut names = vec![table.to_string()];
    let mut aliases: Vec<Option<String>> = vec![None];
    for j in joins {
        names.push(j.right_table.clone());
        aliases.push(j.right_alias.clone());
    }
    let table_count = names.len();

    let mut widths = Vec::with_capacity(table_count);
    let mut relations = Vec::with_capacity(table_count);
    let mut indexes_per_rel = Vec::with_capacity(table_count);
    for name in &names {
        let schema = catalog.get_table(name)?;
        widths.push(schema.columns.len());
        let indexes = catalog.indexes_for_table(schema.table_id, &schema)?;
        relations.push(JoinRelation {
            table_id: schema.table_id,
            local_selectivity: 1.0,
        });
        indexes_per_rel.push(indexes);
    }
    let textual_base = crate::sql::optimizer::column_map::textual_base(&widths);

    // O10: feed single-table WHERE conjuncts into each relation's
    // `local_selectivity` — without this the DP orders joins blind to
    // filters (`FROM big JOIN small WHERE big.x = 5` costs `big` at full
    // size even when the filter makes it tiny).
    apply_local_selectivities(&mut relations, filter, stats, &textual_base, &widths);

    let mut edges = Vec::with_capacity(joins.len());
    for (i, join) in joins.iter().enumerate() {
        let added = i + 1; // rel index of this join's right table
        let (ga, gb) = match &join.on {
            Some(Predicate::Compare {
                op: CompareOp::Eq,
                left: Expression::Column(a),
                right: Expression::Column(b),
            }) => (*a, *b),
            _ => return Ok(None), // cross / non-equi / composite → bail
        };
        let (rel_a, loc_a) = match decompose(ga, &textual_base, &widths) {
            Some(x) => x,
            None => return Ok(None),
        };
        let (rel_b, loc_b) = match decompose(gb, &textual_base, &widths) {
            Some(x) => x,
            None => return Ok(None),
        };
        // One side must be the table this join adds; the other earlier.
        let (early_rel, early_loc, added_loc) = if rel_a == added && rel_b < added {
            (rel_b, loc_b, loc_a)
        } else if rel_b == added && rel_a < added {
            (rel_a, loc_a, loc_b)
        } else {
            return Ok(None);
        };
        edges.push(JoinEdge {
            left: early_rel,
            right: added,
            left_col: early_loc as u32,
            right_col: added_loc as u32,
            left_col_indexed: has_single_col_index(&indexes_per_rel[early_rel], early_loc),
            right_col_indexed: has_single_col_index(&indexes_per_rel[added], added_loc),
        });
    }

    Ok(Some(JoinGraph {
        relations,
        edges,
        widths,
        textual_base,
        names,
        aliases,
    }))
}

/// True when some index on the table covers exactly `[column]`.
/// `pub(crate)`: shared with the memo normalizer (T17-A.1).
pub(crate) fn has_single_col_index(indexes: &[crate::table::IndexHandle], column: usize) -> bool {
    indexes.iter().any(|ix| ix.def.columns == [column])
}

/// Route each single-table WHERE conjunct to the relation it constrains
/// and fold its estimated selectivity into that relation's
/// `local_selectivity` (multiplying across conjuncts — the independence
/// assumption, O-S1). Conjuncts spanning tables are skipped: join edges
/// are costed separately by `edge_selectivity`, so nothing double-counts.
/// Literal-only conjuncts carry no per-relation narrowing and are skipped.
fn apply_local_selectivities(
    relations: &mut [JoinRelation],
    filter: Option<&Predicate>,
    stats: &QueryStats,
    textual_base: &[usize],
    widths: &[usize],
) {
    let conjuncts = match filter {
        Some(f) => flatten_conjuncts(f.clone()),
        None => return,
    };
    for conjunct in conjuncts {
        let mut cols = Vec::new();
        referenced_columns(&conjunct, &mut cols);
        let Some(&first) = cols.first() else {
            continue;
        };
        let Some((rel, _)) = decompose(first, textual_base, widths) else {
            continue;
        };
        let single_table = cols
            .iter()
            .all(|&c| matches!(decompose(c, textual_base, widths), Some((r, _)) if r == rel));
        if !single_table {
            continue;
        }
        // Rebase to table-local column indices — that's how the estimator
        // indexes its per-column stats slice.
        let local_pred = shift_predicate(conjunct, -(textual_base[rel] as isize));
        let column_stats: Vec<Option<ColumnStats>> = (0..widths[rel])
            .map(|c| {
                stats
                    .column_stats(relations[rel].table_id, c as u32)
                    .cloned()
            })
            .collect();
        relations[rel].local_selectivity *=
            estimate_predicate_selectivity(&local_pred, &column_stats);
    }
}

/// Find the `(rel, local)` a tuple-global index belongs to in textual
/// layout. `None` if out of range (treated as "can't reorder").
/// `pub(crate)`: shared with the memo normalizer (T17-A.1).
pub(crate) fn decompose(
    global: usize,
    textual_base: &[usize],
    widths: &[usize],
) -> Option<(usize, usize)> {
    for r in 0..widths.len() {
        if global >= textual_base[r] && global < textual_base[r] + widths[r] {
            return Some((r, global - textual_base[r]));
        }
    }
    None
}

/// Flatten a left-deep `JoinOrder` into its first (leftmost) relation and
/// the sequence of `(added_rel, connecting_edge_indices)` steps. Iterative
/// — depth is bounded by table count (≤ `MAX_RELATIONS`).
fn flatten_join_order(order: &JoinOrder) -> (RelId, Vec<(RelId, Vec<usize>)>) {
    let mut steps: Vec<(RelId, Vec<usize>)> = Vec::new();
    let mut node = order;
    loop {
        match node {
            JoinOrder::Join {
                left, right, edges, ..
            } => {
                steps.push((*right, edges.clone()));
                node = left;
            }
            JoinOrder::Scan { rel } => {
                steps.reverse();
                return (*rel, steps);
            }
        }
    }
}

/// Rewrite a `SELECT` into the chosen physical order: reorder the tables,
/// rebuild each join's `ON` from the connecting edges, and remap every
/// column reference (predicates, projection, order-by, aggregates) to the
/// physical layout. The result lowers through the normal textual path; its
/// `Projection` selects columns in the original SELECT order, so output
/// column order is preserved.
#[allow(clippy::too_many_arguments)] // mirrors the Select fields, same as plan_select.
fn rewrite_select(
    graph: &JoinGraph,
    order: &JoinOrder,
    projection: Vec<usize>,
    aggregates: Vec<AggregateSpec>,
    select_list: Vec<Expression>,
    filter: Option<Predicate>,
    order_by: Vec<(usize, OrderDir)>,
    having: Option<Predicate>,
    limit: Option<usize>,
) -> LogicalPlan {
    let (first_rel, steps) = flatten_join_order(order);

    let mut physical_order = Vec::with_capacity(steps.len() + 1);
    physical_order.push(first_rel);
    for (rel, _) in &steps {
        physical_order.push(*rel);
    }
    let remap = ColumnRemap::new(&graph.widths, &physical_order);

    let mut new_joins = Vec::with_capacity(steps.len());
    for (rel, edge_idxs) in &steps {
        let on = build_on_predicate(edge_idxs, &graph.edges, &graph.textual_base, &remap);
        new_joins.push(JoinClause {
            right_table: graph.names[*rel].clone(),
            right_alias: graph.aliases[*rel].clone(),
            on: Some(on),
            // Reorder only runs on Inner-only queries (build_join_graph bails
            // on any outer join), so every rewritten join is Inner.
            kind: JoinKind::Inner,
        });
    }

    // Coordinate rule (see LogicalPlan::Select): with aggregates present,
    // `order_by`, `having`, and `select_list` index the aggregate OUTPUT
    // row — invariant under table reorder, so they must NOT be remapped.
    // Without aggregates, `order_by` and `select_list` are input-space like
    // everything else.
    let (order_by, select_list) = if aggregates.is_empty() {
        (
            order_by
                .into_iter()
                .map(|(c, d)| (remap.apply_index(c), d))
                .collect(),
            select_list
                .into_iter()
                .map(|e| remap.apply_expression(e))
                .collect(),
        )
    } else {
        (order_by, select_list)
    };
    LogicalPlan::Select {
        table: graph.names[first_rel].clone(),
        joins: new_joins,
        projection: projection
            .into_iter()
            .map(|c| remap.apply_index(c))
            .collect(),
        aggregates: aggregates
            .into_iter()
            .map(|a| remap_aggregate(a, &remap))
            .collect(),
        select_list,
        filter: filter.map(|f| remap.apply_predicate(f)),
        order_by,
        having,
        limit,
    }
}

/// Rebuild a join's `ON` predicate from its connecting edges, remapped to
/// the physical layout. Single edge → one `col = col`; multiple → their
/// conjunction.
fn build_on_predicate(
    edge_idxs: &[usize],
    edges: &[JoinEdge],
    textual_base: &[usize],
    remap: &ColumnRemap,
) -> Predicate {
    let mut compares = edge_idxs.iter().map(|&ei| {
        let e = &edges[ei];
        let ga = textual_base[e.left] + e.left_col as usize;
        let gb = textual_base[e.right] + e.right_col as usize;
        Predicate::Compare {
            op: CompareOp::Eq,
            left: Expression::Column(remap.apply_index(ga)),
            right: Expression::Column(remap.apply_index(gb)),
        }
    });
    let first = compares
        .next()
        .expect("a join step has at least one connecting edge");
    compares.fold(first, |acc, c| Predicate::And(Box::new(acc), Box::new(c)))
}

/// Remap the columns an aggregate's argument expression reads (no-op for
/// `COUNT(*)`). `pub(crate)`: the memo planner's emission (T17-A.4) remaps
/// aggregates identically. Routes through the shared `apply_expression`
/// walker so arithmetic arguments (H2a) remap under join reorder.
pub(crate) fn remap_aggregate(spec: AggregateSpec, remap: &ColumnRemap) -> AggregateSpec {
    match spec {
        AggregateSpec::CountStar => AggregateSpec::CountStar,
        AggregateSpec::Count { arg, distinct } => AggregateSpec::Count {
            arg: remap.apply_expression(arg),
            distinct,
        },
        AggregateSpec::Sum(e) => AggregateSpec::Sum(remap.apply_expression(e)),
        AggregateSpec::Min(e) => AggregateSpec::Min(remap.apply_expression(e)),
        AggregateSpec::Max(e) => AggregateSpec::Max(remap.apply_expression(e)),
        AggregateSpec::Avg(e) => AggregateSpec::Avg(remap.apply_expression(e)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{Catalog, ColumnDef, Schema, TableId};
    use crate::engines::btree::BTreeEngine;
    use crate::sql::binder::Binder;
    use crate::sql::frontend::parse;
    use crate::sql::planner::RuleBasedPlanner;
    use crate::storage::FileDiskManager;
    use crate::types::ColumnType;
    use tempfile::TempDir;

    struct Env {
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
                ColumnDef {
                    name: "id".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "v".into(),
                    ty: ColumnType::Int64,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec![0],
        };
        catalog.create_table("t".into(), schema).unwrap();
        Env {
            catalog,
            binder,
            _dir: dir,
        }
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
        let a = env
            .binder
            .bind(stmts.clone().into_iter().next().unwrap())
            .unwrap();
        let b = env.binder.bind(stmts.into_iter().next().unwrap()).unwrap();

        let p_rule = RuleBasedPlanner.plan(a, &env.catalog).unwrap();
        let p_sel = SelingerPlanner::<DefaultCostModel>::default()
            .plan(b, &env.catalog)
            .unwrap();

        let (t_rule, t_sel) = match (p_rule, p_sel) {
            (PhysicalPlan::Query(a), PhysicalPlan::Query(b)) => (a.explain(0), b.explain(0)),
            _ => panic!("expected query plans"),
        };
        assert_eq!(t_rule, t_sel);
    }
}
