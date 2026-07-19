//! T17-A.1 — normalization: `LogicalPlan::Select` → [`NormalizedQuery`].
//!
//! Splits a SELECT into the optimizable core (relations, equi-join edges,
//! per-relation local predicates) and the fixed statement spine (residual
//! filter, aggregates, ORDER BY, projection, LIMIT) per decision D3. All
//! catalog- and stats-touching work happens HERE, once: row counts and
//! selectivities are snapshotted into the result, so the memo and search
//! layers are pure over owned data (control plane / data plane
//! separation, mirroring how `QueryStats` keeps MVCC reads out of the
//! Selinger DP loop).
//!
//! Coordinates: relations are indexed `0..n` in textual order (FROM table
//! = 0, `joins[i].right_table` = i+1) — identical to `selinger.rs`. Local
//! predicates are rebased to relation-local columns; `residual` and the
//! spine keep tuple-global (textual) indices.
//!
//! Returns `Ok(None)` for any unsupported shape (D8) — non-SELECT, more
//! than `MAX_RELATIONS` relations, or any join without a single
//! `Column = Column` equi ON after promotion. The caller then falls back
//! to `plan_inner` with the untouched logical plan.

use crate::catalog::system_tables::ColumnStats;
use crate::catalog::{Catalog, Schema};
use crate::common::Result;
use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
use crate::sql::ir::logical::{AggregateSpec, JoinKind, LogicalPlan, OrderDir};
use crate::sql::optimizer::column_map::textual_base;
use crate::sql::optimizer::join_order::{RelId, MAX_RELATIONS};
use crate::sql::optimizer::selectivity::{estimate_predicate_selectivity, join_selectivity};
use crate::sql::optimizer::selinger::{decompose, has_single_col_index};
use crate::sql::optimizer::stats::QueryStats;
use crate::sql::planner::{flatten_conjuncts, referenced_columns, shift_predicate};
use crate::storage::StorageEngine;
use crate::table::IndexHandle;

/// One base relation of the optimizable core, in textual order.
///
/// NOTE (deviation from the plan §3 sketch): the sketch lists an `alias`
/// field; nothing in 17-A/B reads it (`PhysOp` references tables by name,
/// and the D8 fallback re-plans the original logical plan), so it is
/// omitted per the minimal-code rule.
pub(crate) struct RelInfo {
    pub(crate) name: String,
    pub(crate) schema: std::sync::Arc<Schema>,
    pub(crate) indexes: Vec<IndexHandle>,
    /// Single-table WHERE conjuncts, rebased to this relation's local
    /// columns — the leaf-lowering (D6) and Filter inputs.
    pub(crate) local_preds: Vec<LocalPred>,
    /// Base row count from the stats snapshot — a D7 cardinality input.
    pub(crate) row_count: f64,
}

impl RelInfo {
    /// Combined selectivity of all local conjuncts — a D7 cardinality
    /// input. Independence product (same rule as
    /// `selinger.rs::apply_local_selectivities`), `1.0` when unfiltered.
    pub(crate) fn local_selectivity(&self) -> f64 {
        self.local_preds.iter().map(|p| p.selectivity).product()
    }
}

/// One local conjunct with its own selectivity estimate. Per-conjunct
/// (not just the combined product) because leaf costing (§5) needs the
/// matched conjunct's selectivity alone — `cost_index_scan` charges for
/// the rows the *index* matches, before the leaf Filter narrows further —
/// and the search layer is stats-free by design.
pub(crate) struct LocalPred {
    pub(crate) pred: Predicate,
    pub(crate) selectivity: f64,
}

/// One equi-join edge `left.left_col = right.right_col` in textual
/// coordinates. `left_rel < right_rel` always (the earlier relation is
/// `left`, matching `build_join_graph`'s convention). Columns are
/// relation-local. The `*_indexed` flags gate `IndexNested` candidates.
pub(crate) struct Edge {
    pub(crate) left_rel: RelId,
    pub(crate) right_rel: RelId,
    pub(crate) left_col: usize,
    pub(crate) right_col: usize,
    pub(crate) left_indexed: bool,
    pub(crate) right_indexed: bool,
    /// Equi-join selectivity from the snapshot NDVs — a D7 cardinality
    /// input. NOTE (deviation from the plan §3 sketch): precomputed here,
    /// once per edge, so the memo never touches `QueryStats`.
    pub(crate) selectivity: f64,
}

/// The fixed statement spine (D3): applied above the memo's output in
/// original textual-global coordinates, untouched by optimization.
/// Exception: `having` (and `order_by` / `select_list` when `aggregates` is
/// non-empty) index the aggregate OUTPUT row — those are remap-invariant
/// and must pass through emission untouched.
pub(crate) struct SpineParts {
    pub(crate) projection: Vec<usize>,
    pub(crate) aggregates: Vec<AggregateSpec>,
    pub(crate) select_list: Vec<Expression>,
    pub(crate) order_by: Vec<(usize, OrderDir)>,
    pub(crate) having: Option<Predicate>,
    pub(crate) limit: Option<usize>,
}

/// Output of normalization: the optimizable core + the untouched spine.
pub(crate) struct NormalizedQuery {
    pub(crate) relations: Vec<RelInfo>,
    pub(crate) edges: Vec<Edge>,
    /// Cross-table WHERE conjuncts that are not equi-edges, in textual-
    /// global coordinates — emitted as a residual Filter in the spine.
    pub(crate) residual: Vec<Predicate>,
    pub(crate) spine: SpineParts,
}

impl NormalizedQuery {
    /// Column count per relation, textual order.
    pub(crate) fn widths(&self) -> Vec<usize> {
        self.relations
            .iter()
            .map(|r| r.schema.columns.len())
            .collect()
    }

    /// Tuple-global offset of each relation's first column, textual
    /// order (the shared prefix sum).
    pub(crate) fn textual_base(&self) -> Vec<usize> {
        textual_base(&self.widths())
    }
}

/// Normalize a SELECT into its optimizable core, or `Ok(None)` for any
/// shape the memo does not handle (D8 — the caller falls back to
/// `plan_inner` with the untouched `logical`).
pub(crate) fn normalize<CatE: StorageEngine>(
    logical: &LogicalPlan,
    catalog: &Catalog<CatE>,
    stats: &QueryStats,
) -> Result<Option<NormalizedQuery>> {
    // Only SELECT has an optimizable join/filter core; every other
    // statement shape is the caller's fallback (D8).
    let LogicalPlan::Select {
        table,
        joins,
        projection,
        aggregates,
        select_list,
        filter,
        order_by,
        having,
        limit,
    } = logical
    else {
        return Ok(None);
    };

    // No-reorder rule (H3b): the memo only searches Inner join cores. An
    // outer join is not commutative or associative with its neighbours —
    // reordering across one changes which left rows get null-padded — so ANY
    // non-Inner kind bails the whole query to the D8 textual fallback, where
    // the outer join keeps its written position. Explicit on kind, matching
    // `build_join_graph`; the equi-ON edge test below would only bail
    // incidentally (and not at all for a bare `col = col` outer ON).
    if joins.iter().any(|j| j.kind != JoinKind::Inner) {
        return Ok(None);
    }

    // The memo keys groups by a u32 bitmask (D2) — same hard bound as the
    // Selinger DP. Wider queries fall back (D8).
    let relation_count = 1 + joins.len();
    if relation_count > MAX_RELATIONS {
        return Ok(None);
    }

    // --- Per-relation catalog data + row counts, textual order.
    let mut relations = Vec::with_capacity(relation_count);
    for name in std::iter::once(table).chain(joins.iter().map(|j| &j.right_table)) {
        let schema = catalog.get_table(name)?;
        let indexes = catalog.indexes_for_table(schema.table_id, &schema)?;
        let row_count = stats.row_count(schema.table_id);
        relations.push(RelInfo {
            name: name.clone(),
            schema,
            indexes,
            local_preds: Vec::new(),
            row_count,
        });
    }

    // Tuple-global base offset of each relation — the key for
    // decomposing textual-global column indices.
    let widths: Vec<usize> = relations.iter().map(|r| r.schema.columns.len()).collect();
    let textual_base = textual_base(&widths);

    // --- Route WHERE conjuncts: single-table → that relation's local
    // predicates (rebased to local columns); everything else (cross-table,
    // constant) → residual, where edge promotion below may still claim
    // one per comma join. Same routing as `plan_select` / O10.
    let mut residual: Vec<Predicate> = Vec::new();
    for conjunct in filter.clone().map(flatten_conjuncts).unwrap_or_default() {
        let mut cols = Vec::new();
        referenced_columns(&conjunct, &mut cols);
        match single_relation_of(&cols, &textual_base, &widths) {
            Some(rel) => {
                let local = shift_predicate(conjunct, -(textual_base[rel] as isize));
                // Selectivity is filled by the per-relation pass below
                // (batching the column-stats snapshot per relation).
                relations[rel].local_preds.push(LocalPred {
                    pred: local,
                    selectivity: 1.0,
                });
            }
            None => residual.push(conjunct),
        }
    }

    // --- Edges: exactly one per join. Explicit ON, or — for comma joins —
    // the first residual equi-conjunct connecting this relation to an
    // earlier one (plan_select's Phase C.1 promotion rule, restated in
    // textual coordinates). Any join without a single `Column = Column`
    // equi ON after promotion bails (D8); the acceptance rule (one side
    // is the added relation, the other earlier) matches `build_join_graph`.
    let mut edges = Vec::with_capacity(joins.len());
    for (i, join) in joins.iter().enumerate() {
        let added = i + 1;
        let on = match &join.on {
            Some(p) => p.clone(),
            None => {
                let promoted = residual.iter().position(|c| {
                    matches!(
                        equi_endpoints(c, &textual_base, &widths),
                        Some(((ra, _), (rb, _)))
                            if (ra == added && rb < added) || (rb == added && ra < added)
                    )
                });
                match promoted {
                    Some(pos) => residual.remove(pos),
                    None => return Ok(None), // cross join — unsupported (D8)
                }
            }
        };
        let Some(((rel_a, loc_a), (rel_b, loc_b))) = equi_endpoints(&on, &textual_base, &widths)
        else {
            return Ok(None); // non-equi / composite / out-of-range ON (D8)
        };
        let ((early, early_loc), added_loc) = if rel_a == added && rel_b < added {
            ((rel_b, loc_b), loc_a)
        } else if rel_b == added && rel_a < added {
            ((rel_a, loc_a), loc_b)
        } else {
            return Ok(None); // ON does not connect this join's table to an earlier one (D8)
        };
        let ndv_left = stats.ndv(relations[early].schema.table_id, early_loc as u32);
        let ndv_right = stats.ndv(relations[added].schema.table_id, added_loc as u32);
        edges.push(Edge {
            left_rel: early,
            right_rel: added,
            left_col: early_loc,
            right_col: added_loc,
            left_indexed: has_single_col_index(&relations[early].indexes, early_loc),
            right_indexed: has_single_col_index(&relations[added].indexes, added_loc),
            selectivity: join_selectivity(ndv_left, ndv_right),
        });
    }
    // Positive space: every join contributed an edge (bail paths returned
    // above). This also guarantees the join graph is connected — each
    // relation r > 0 has an edge to some relation < r.
    assert_eq!(edges.len(), joins.len());

    // --- Local selectivities (D7 inputs): same estimator + independence
    // rule as `selinger.rs::apply_local_selectivities`, over the already-
    // rebased local conjuncts.
    for rel in &mut relations {
        if rel.local_preds.is_empty() {
            continue;
        }
        let column_stats: Vec<Option<ColumnStats>> = (0..rel.schema.columns.len())
            .map(|c| stats.column_stats(rel.schema.table_id, c as u32).cloned())
            .collect();
        for local in &mut rel.local_preds {
            local.selectivity = estimate_predicate_selectivity(&local.pred, &column_stats);
        }
        // Each factor is clamped to [MIN_SELECTIVITY, 1] — the product
        // must stay a usable cardinality multiplier.
        debug_assert!(rel.local_selectivity() > 0.0 && rel.local_selectivity() <= 1.0);
    }

    Ok(Some(NormalizedQuery {
        relations,
        edges,
        residual,
        spine: SpineParts {
            projection: projection.clone(),
            aggregates: aggregates.clone(),
            select_list: select_list.clone(),
            order_by: order_by.clone(),
            having: having.clone(),
            limit: *limit,
        },
    }))
}

/// Decompose a predicate of shape `Column(a) = Column(b)` into its two
/// `(relation, local_column)` endpoints. `None` for any other shape or
/// for out-of-range columns.
fn equi_endpoints(
    pred: &Predicate,
    textual_base: &[usize],
    widths: &[usize],
) -> Option<((RelId, usize), (RelId, usize))> {
    let (ga, gb) = match pred {
        Predicate::Compare {
            op: CompareOp::Eq,
            left: Expression::Column(a),
            right: Expression::Column(b),
        } => (*a, *b),
        _ => return None,
    };
    let a = decompose(ga, textual_base, widths)?;
    let b = decompose(gb, textual_base, widths)?;
    Some((a, b))
}

/// The single relation containing every referenced column, or `None` when
/// the columns span relations, fall out of range, or the predicate
/// references no columns at all (a constant — stays residual, matching
/// `plan_select`'s `Bucket::NoColumns` routing).
fn single_relation_of(cols: &[usize], textual_base: &[usize], widths: &[usize]) -> Option<RelId> {
    let (rel, _) = decompose(*cols.first()?, textual_base, widths)?;
    let all_in_rel = cols
        .iter()
        .all(|&c| matches!(decompose(c, textual_base, widths), Some((r, _)) if r == rel));
    if all_in_rel {
        Some(rel)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::TableId;
    use crate::engines::btree::BTreeEngine;
    use crate::sql::binder::Binder;
    use crate::sql::frontend::parse;
    use crate::sql::optimizer::memo::fixtures::{bind, default_stats, int32_col, setup};
    use crate::sql::optimizer::selectivity::{EQ_FALLBACK, JOIN_FALLBACK, RANGE_FALLBACK};
    use crate::sql::optimizer::selinger::build_join_graph;
    use crate::sql::optimizer::stats::{MockStatsProvider, DEFAULT_ROW_COUNT};
    use crate::storage::FileDiskManager;

    fn assert_eq_col_lit(pred: &Predicate, col: usize) {
        match pred {
            Predicate::Compare {
                op: CompareOp::Eq,
                left: Expression::Column(c),
                right: Expression::Literal(_),
            } => assert_eq!(*c, col),
            other => panic!("expected Column({col}) = Literal, got {other:?}"),
        }
    }

    #[test]
    fn single_table_select_normalizes_to_one_relation() {
        let env = setup();
        let logical = bind(&env, "SELECT w_id FROM wh WHERE w_ytd = 100 LIMIT 3");
        let nq = normalize(&logical, &env.catalog, &default_stats())
            .unwrap()
            .unwrap();

        assert_eq!(nq.relations.len(), 1);
        assert_eq!(nq.relations[0].name, "wh");
        assert_eq!(nq.relations[0].row_count, DEFAULT_ROW_COUNT);
        // w_ytd is global col 1 == local col 1 (relation 0 needs no rebase).
        assert_eq!(nq.relations[0].local_preds.len(), 1);
        assert_eq_col_lit(&nq.relations[0].local_preds[0].pred, 1);
        assert!((nq.relations[0].local_selectivity() - EQ_FALLBACK).abs() < 1e-12);
        assert!(nq.edges.is_empty());
        assert!(nq.residual.is_empty());
        assert_eq!(nq.spine.projection, vec![0]);
        assert_eq!(nq.spine.limit, Some(3));
        assert!(nq.spine.aggregates.is_empty());
        assert!(nq.spine.order_by.is_empty());
    }

    #[test]
    fn where_driven_join_promotes_edge_and_routes_locals() {
        // Comma join: `d_w_id = w_id` must be promoted to the edge (Phase
        // C.1 parity), the two single-table conjuncts must land on their
        // relations rebased to local columns, and nothing stays residual.
        let env = setup();
        let logical = bind(
            &env,
            "SELECT w_id FROM wh, dist WHERE w_ytd = 100 AND d_ytd = 5 AND d_w_id = w_id",
        );
        let nq = normalize(&logical, &env.catalog, &default_stats())
            .unwrap()
            .unwrap();

        assert_eq!(nq.relations.len(), 2);
        assert_eq!(nq.relations[0].local_preds.len(), 1);
        assert_eq_col_lit(&nq.relations[0].local_preds[0].pred, 1); // w_ytd local 1
        assert_eq!(nq.relations[1].local_preds.len(), 1);
        assert_eq_col_lit(&nq.relations[1].local_preds[0].pred, 2); // d_ytd global 4 → local 2

        assert_eq!(nq.edges.len(), 1);
        let e = &nq.edges[0];
        assert_eq!((e.left_rel, e.right_rel), (0, 1));
        assert_eq!((e.left_col, e.right_col), (0, 1)); // w_id local 0, d_w_id local 1
        assert!(!e.left_indexed, "w_id has no secondary index");
        assert!(e.right_indexed, "d_w_id carries dist_by_w");
        assert!((e.selectivity - JOIN_FALLBACK).abs() < 1e-12); // no NDVs

        assert!(nq.residual.is_empty());
    }

    #[test]
    fn explicit_on_edge_matches_build_join_graph() {
        // Parity fixture (plan §9 A.1 gate): same query through the
        // normalizer and Selinger's `build_join_graph` must agree on edge
        // endpoints, indexed flags, and per-relation local selectivities.
        let env = setup();
        let sql = "SELECT w_id FROM wh JOIN dist ON d_w_id = w_id WHERE w_ytd = 100";
        let logical = bind(&env, sql);
        let stats = default_stats();

        let LogicalPlan::Select {
            table,
            joins,
            filter,
            ..
        } = &logical
        else {
            panic!("expected SELECT");
        };
        let graph = build_join_graph(table, joins, filter.as_ref(), &env.catalog, &stats)
            .unwrap()
            .unwrap();
        let nq = normalize(&logical, &env.catalog, &stats).unwrap().unwrap();

        assert_eq!(nq.edges.len(), graph.edges.len());
        for (e, g) in nq.edges.iter().zip(&graph.edges) {
            assert_eq!(e.left_rel, g.left);
            assert_eq!(e.right_rel, g.right);
            assert_eq!(e.left_col, g.left_col as usize);
            assert_eq!(e.right_col, g.right_col as usize);
            assert_eq!(e.left_indexed, g.left_col_indexed);
            assert_eq!(e.right_indexed, g.right_col_indexed);
        }
        assert_eq!(nq.relations.len(), graph.relations.len());
        for (r, g) in nq.relations.iter().zip(&graph.relations) {
            assert!((r.local_selectivity() - g.local_selectivity).abs() < 1e-12);
        }
    }

    #[test]
    fn three_way_chain_extracts_two_edges() {
        let env = setup();
        let logical = bind(
            &env,
            "SELECT w_id FROM wh JOIN dist ON d_w_id = w_id JOIN cust ON c_d_id = d_id",
        );
        let nq = normalize(&logical, &env.catalog, &default_stats())
            .unwrap()
            .unwrap();

        assert_eq!(nq.relations.len(), 3);
        assert_eq!(nq.edges.len(), 2);
        assert_eq!((nq.edges[0].left_rel, nq.edges[0].right_rel), (0, 1));
        // c_d_id is global 6 → cust-local 1; d_id is global 2 → dist-local 0.
        assert_eq!((nq.edges[1].left_rel, nq.edges[1].right_rel), (1, 2));
        assert_eq!((nq.edges[1].left_col, nq.edges[1].right_col), (0, 1));
    }

    #[test]
    fn cross_table_non_equi_conjunct_stays_residual() {
        let env = setup();
        let logical = bind(
            &env,
            "SELECT w_id FROM wh, dist WHERE d_w_id = w_id AND w_ytd < d_ytd",
        );
        let nq = normalize(&logical, &env.catalog, &default_stats())
            .unwrap()
            .unwrap();

        assert_eq!(nq.edges.len(), 1);
        assert_eq!(nq.residual.len(), 1);
        // Residual keeps tuple-global coordinates: w_ytd = 1, d_ytd = 4.
        match &nq.residual[0] {
            Predicate::Compare {
                op: CompareOp::Lt,
                left: Expression::Column(l),
                right: Expression::Column(r),
            } => assert_eq!((*l, *r), (1, 4)),
            other => panic!("expected w_ytd < d_ytd residual, got {other:?}"),
        }
        assert!(nq.relations[0].local_preds.is_empty());
        assert!(nq.relations[1].local_preds.is_empty());
    }

    #[test]
    fn stats_feed_row_counts_and_selectivities() {
        // Real NDV / row-count wiring: mock stats keyed by the catalog's
        // assigned table ids must reach the right RelInfo / Edge fields.
        let env = setup();
        let provider = MockStatsProvider::new()
            .with_table_rows(env.wh_id, 10_000)
            .with_table_rows(env.dist_id, 500)
            .with_column_ndv(env.wh_id, 0, 10_000) // w_id
            .with_column_ndv(env.dist_id, 1, 100); // d_w_id
        let stats = QueryStats::gather(
            &provider,
            &[(env.wh_id, &[0, 1]), (env.dist_id, &[0, 1, 2])],
        )
        .unwrap();

        let logical = bind(
            &env,
            "SELECT w_id FROM wh, dist WHERE w_ytd = 100 AND w_id > 5 AND d_w_id = w_id",
        );
        let nq = normalize(&logical, &env.catalog, &stats).unwrap().unwrap();

        assert_eq!(nq.relations[0].row_count, 10_000.0);
        assert_eq!(nq.relations[1].row_count, 500.0);
        // w_ytd = 100 has no column stats → EQ_FALLBACK; w_id > 5 has NDV
        // but no histogram → RANGE_FALLBACK. Each conjunct carries its
        // own estimate; the combined value is their product.
        let sels: Vec<f64> = nq.relations[0]
            .local_preds
            .iter()
            .map(|p| p.selectivity)
            .collect();
        assert!((sels[0] - EQ_FALLBACK).abs() < 1e-12);
        assert!((sels[1] - RANGE_FALLBACK).abs() < 1e-12);
        let expected = EQ_FALLBACK * RANGE_FALLBACK;
        assert!((nq.relations[0].local_selectivity() - expected).abs() < 1e-12);
        // Edge selectivity = 1 / max(ndv(w_id), ndv(d_w_id)) = 1e-4.
        assert!((nq.edges[0].selectivity - 1e-4).abs() < 1e-12);
    }

    // ---- Bail cases (D8) ----

    #[test]
    fn cross_join_without_equi_conjunct_bails() {
        let env = setup();
        let logical = bind(&env, "SELECT w_id FROM wh, dist");
        assert!(normalize(&logical, &env.catalog, &default_stats())
            .unwrap()
            .is_none());
    }

    #[test]
    fn non_equi_on_bails() {
        let env = setup();
        let logical = bind(&env, "SELECT w_id FROM wh JOIN dist ON d_w_id < w_id");
        assert!(normalize(&logical, &env.catalog, &default_stats())
            .unwrap()
            .is_none());
    }

    #[test]
    fn composite_on_bails() {
        let env = setup();
        let logical = bind(
            &env,
            "SELECT w_id FROM wh JOIN dist ON d_w_id = w_id AND d_ytd = w_ytd",
        );
        assert!(normalize(&logical, &env.catalog, &default_stats())
            .unwrap()
            .is_none());
    }

    #[test]
    fn non_select_bails() {
        let env = setup();
        let logical = bind(&env, "INSERT INTO wh VALUES (1, 10)");
        assert!(normalize(&logical, &env.catalog, &default_stats())
            .unwrap()
            .is_none());
    }

    #[test]
    fn over_max_relations_bails() {
        // 33 single-column tables chained by 32 equi-joins: one over the
        // u32-bitmask cap (MAX_RELATIONS = 31) → explicit-bound bail.
        let dir = tempfile::tempdir().unwrap();
        let dm = FileDiskManager::create(dir.path().join("t.db")).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());
        for i in 0..33 {
            catalog
                .create_table(
                    format!("t{i}"),
                    Schema {
                        name: format!("t{i}"),
                        table_id: TableId(0),
                        columns: vec![int32_col(&format!("c{i}"))],
                        primary_key: vec![0],
                    },
                )
                .unwrap();
        }
        let mut sql = String::from("SELECT c0 FROM t0");
        for i in 1..33 {
            sql += &format!(" JOIN t{i} ON c{i} = c{}", i - 1);
        }
        let binder = Binder::new(catalog.clone());
        let stmts = parse(&sql).unwrap();
        let logical = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
        assert!(normalize(&logical, &catalog, &default_stats())
            .unwrap()
            .is_none());
    }
}
