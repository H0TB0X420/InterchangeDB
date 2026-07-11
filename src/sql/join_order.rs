//! P14.11 — Selinger join-order enumeration (the DP search core).
//!
//! This is the Optimizer layer of the cost-based planner. It is fully
//! decoupled from the catalog and from `LogicalPlan` / executor
//! plumbing: it consumes an abstract join graph plus a `QueryStats`
//! snapshot and a `CostModel`, and emits an abstract [`JoinOrder`] IR.
//! The caller (P14.13) maps `RelId`s back to real tables and lowers the
//! IR into a `PhysicalPlan`.
//!
//! ## Algorithm
//!
//! Classic System R bottom-up dynamic programming over subsets of base
//! relations, **left-deep only** (the right input of every join is a
//! single base relation — matching what `NestedLoopJoin` /
//! `IndexNestedLoopJoin` / `HashJoin` consume today). For each connected
//! subset we keep the single cheapest plan; we extend a subset by joining
//! it with one more relation that an equi-join edge connects to it. Cross
//! products are never formed inside the DP — a disconnected graph falls
//! through to the greedy path.
//!
//! Per join we pick the cheapest of `NestedLoop` / `Hash` /
//! `IndexNestedLoop` (the last only when the added relation's join column
//! is indexed). Selectivity comes from [`join_selectivity`] over the
//! snapshot's NDVs; cardinality is the running product of base rows and
//! applied selectivities.
//!
//! ## Bounds (CLAUDE.md "limit on everything")
//!
//! - `MAX_RELATIONS` (31) — hard cap; the subset bitset is a `u32`.
//! - `MAX_DP_RELATIONS` (12) — above this, exhaustive DP's `2^n` subsets
//!   are too many, so we fall back to a greedy left-deep order. TPC-H's
//!   widest query is 8-way, comfortably inside the DP regime.
//! - `time_budget` — optional wall-clock guard; if exhaustive DP exceeds
//!   it mid-build we bail to the greedy order. Plumbed from
//!   `SelingerPlanner::time_budget_ms` (P14.13).
//!
//! ## Deviation from the Phase 14 plan: interesting orders deferred
//!
//! The plan listed "cheapest plan per interesting order" here. None of
//! the three join operators delivers or exploits a sort order, and no
//! ordered access path feeds the DP yet, so an interesting-order table
//! would be untestable dead code today (violating the minimal-code
//! rule). The hook lands with the first order-aware operator (merge join,
//! Phase 17). Recorded here and surfaced to the user rather than built
//! speculatively.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::catalog::TableId;
use crate::sql::cost::{Cost, CostModel};
use crate::sql::selectivity::join_selectivity;
use crate::sql::stats::QueryStats;

/// Index of a base relation within the `relations` slice.
pub type RelId = usize;

/// Subset of base relations as a bitmask (`RelId` i → bit i).
/// `pub(crate)`: the memo planner (Phase 17) keys its groups by the same
/// convention (D2).
pub(crate) type RelSet = u32;

/// Hard cap on relation count: the subset bitset is a `u32`.
pub const MAX_RELATIONS: usize = 31;

/// Above this many relations, exhaustive DP is abandoned for a greedy
/// left-deep order (`2^12 = 4096` subsets is the most we enumerate).
pub const MAX_DP_RELATIONS: usize = 12;

/// Rows-per-page assumption for turning a row count into a page count for
/// `cost_seq_scan`. NOTE (P14.11): a rough constant — real per-table page
/// counts arrive when `StorageStatus` is wired into stats (Phase 16).
/// `pub(crate)`: the memo planner's leaf costing uses the same constant
/// so cost comparisons across planners stay apples-to-apples (D7/G3).
pub(crate) const ROWS_PER_PAGE_ESTIMATE: f64 = 50.0;

/// Cardinality floor — never let an estimate reach zero, which would make
/// a plan look free and poison the ranking. `pub(crate)`: the memo
/// planner's D7 group cardinality applies the same floor.
pub(crate) const MIN_CARD: f64 = 1.0;

/// One base relation participating in the join. Abstract w.r.t.
/// execution: `table_id` ties it to the stats snapshot here and to a
/// physical scan in the caller.
pub struct JoinRelation {
    pub table_id: TableId,
    /// Combined selectivity of this relation's local (single-table)
    /// predicates, `1.0` if none. Estimated by the caller before
    /// enumeration; applied to the base row count.
    pub local_selectivity: f64,
}

/// One equi-join edge `left.left_col = right.right_col`. The `*_indexed`
/// flags say whether that side's join column has a single-column index,
/// which gates `IndexNestedLoopJoin` costing when that side is the inner
/// (added) relation.
pub struct JoinEdge {
    pub left: RelId,
    pub right: RelId,
    pub left_col: u32,
    pub right_col: u32,
    pub left_col_indexed: bool,
    pub right_col_indexed: bool,
}

/// Join algorithm chosen for one join node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinAlgorithm {
    NestedLoop,
    IndexNestedLoop,
    Hash,
}

/// Abstract join order — a left-deep tree. `Join.right` is always a base
/// relation (left-deep invariant); `edges` are the indices into the
/// caller's edge slice applied at this join, so the caller can rebuild
/// the ON predicate.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinOrder {
    Scan {
        rel: RelId,
    },
    Join {
        left: Box<JoinOrder>,
        right: RelId,
        algorithm: JoinAlgorithm,
        edges: Vec<usize>,
    },
}

/// Result of enumeration: the chosen order plus its estimated cost and
/// output cardinality.
pub struct JoinPlan {
    pub order: JoinOrder,
    pub cost: Cost,
    pub cardinality: f64,
}

/// Internal: the cheapest plan found for one relation subset.
struct SubPlan {
    cardinality: f64,
    cost: Cost,
    order: JoinOrder,
}

/// Enumerate join orders for `relations` connected by `edges`, returning
/// the cheapest left-deep plan under `cost_model`.
///
/// `relations` must be non-empty and within `MAX_RELATIONS`. With one
/// relation the result is a bare scan; with `> MAX_DP_RELATIONS`, or a
/// disconnected graph, or an exceeded `time_budget`, the greedy
/// left-deep fallback is used.
pub fn enumerate_join_orders(
    relations: &[JoinRelation],
    edges: &[JoinEdge],
    stats: &QueryStats,
    cost_model: &dyn CostModel,
    time_budget: Option<Duration>,
) -> JoinPlan {
    let n = relations.len();
    assert!(n >= 1, "join enumeration needs at least one relation");
    assert!(
        n <= MAX_RELATIONS,
        "join enumeration capped at {} relations",
        MAX_RELATIONS
    );

    if n == 1 {
        let leaf = sub_leaf(0, relations, stats, cost_model);
        return JoinPlan {
            order: leaf.order,
            cost: leaf.cost,
            cardinality: leaf.cardinality,
        };
    }
    if n > MAX_DP_RELATIONS {
        return greedy_left_deep(relations, edges, stats, cost_model);
    }

    let start = Instant::now();
    let full: RelSet = ((1u64 << n) - 1) as RelSet;
    let mut best: HashMap<RelSet, SubPlan> = HashMap::new();

    for mask in 1..=full {
        if mask.count_ones() == 1 {
            let rel = mask.trailing_zeros() as RelId;
            best.insert(mask, sub_leaf(rel, relations, stats, cost_model));
            continue;
        }
        if let Some(budget) = time_budget {
            if start.elapsed() > budget {
                return greedy_left_deep(relations, edges, stats, cost_model);
            }
        }

        // Extend every smaller-by-one subset `mask \ {r}` with relation r,
        // keeping the cheapest result for `mask`.
        let mut chosen: Option<SubPlan> = None;
        let mut bits = mask;
        while bits != 0 {
            let r = bits.trailing_zeros() as RelId;
            bits &= bits - 1;
            let left_set = mask & !(1u32 << r);
            let left = match best.get(&left_set) {
                Some(p) => p,
                None => continue, // left subset was itself disconnected
            };
            let connecting = connecting_edges(edges, r, left_set);
            if connecting.is_empty() {
                continue; // no equi-join edge → don't form a cross product
            }
            let cand = build_join(left, r, &connecting, relations, edges, stats, cost_model);
            chosen = Some(match chosen {
                None => cand,
                Some(cur) => cheaper(cur, cand, cost_model),
            });
        }
        if let Some(plan) = chosen {
            best.insert(mask, plan);
        }
    }

    match best.remove(&full) {
        Some(sp) => JoinPlan {
            order: sp.order,
            cost: sp.cost,
            cardinality: sp.cardinality,
        },
        // Disconnected join graph: no DP plan spans every relation. Fall
        // back to greedy, which permits cross products to complete.
        None => greedy_left_deep(relations, edges, stats, cost_model),
    }
}

/// Cost of one *specific* left-deep order (typically the textual order),
/// for deciding whether the DP's best is an actual improvement. Returns
/// `None` if `order` is not a connected left-deep sequence. Because the DP
/// minimizes over all orders, `enumerate_join_orders(...).cost <=
/// cost_of_order(textual, ...)`, so a strict `<` means a genuinely cheaper
/// reordering exists (ties — e.g. unanalyzed, all-equal tables — do not).
pub fn cost_of_order(
    order: &[RelId],
    relations: &[JoinRelation],
    edges: &[JoinEdge],
    stats: &QueryStats,
    cost_model: &dyn CostModel,
) -> Option<Cost> {
    if order.is_empty() {
        return None;
    }
    let mut plan = sub_leaf(order[0], relations, stats, cost_model);
    let mut joined: RelSet = 1u32 << order[0];
    for &r in &order[1..] {
        let connecting = connecting_edges(edges, r, joined);
        if connecting.is_empty() {
            return None; // disconnected at this step
        }
        plan = build_join(&plan, r, &connecting, relations, edges, stats, cost_model);
        joined |= 1u32 << r;
    }
    Some(plan.cost)
}

/// Leaf access plan for one relation: a seq scan over the full table,
/// plus a filter charge when a local predicate narrows it. Output
/// cardinality is the filtered row count.
fn sub_leaf(
    r: RelId,
    relations: &[JoinRelation],
    stats: &QueryStats,
    cost_model: &dyn CostModel,
) -> SubPlan {
    let raw_rows = stats.row_count(relations[r].table_id);
    let pages = (raw_rows / ROWS_PER_PAGE_ESTIMATE).ceil().max(1.0);
    let mut cost = cost_model.cost_seq_scan(pages, raw_rows);

    let local_sel = relations[r].local_selectivity;
    if local_sel < 1.0 {
        cost = cost.add(cost_model.cost_filter(raw_rows));
    }
    let out_card = (raw_rows * local_sel).max(MIN_CARD);
    SubPlan {
        cardinality: out_card,
        cost,
        order: JoinOrder::Scan { rel: r },
    }
}

/// Build the join of sub-plan `left` with base relation `r` over
/// `connecting` edges, choosing the cheapest algorithm.
fn build_join(
    left: &SubPlan,
    r: RelId,
    connecting: &[usize],
    relations: &[JoinRelation],
    edges: &[JoinEdge],
    stats: &QueryStats,
    cost_model: &dyn CostModel,
) -> SubPlan {
    let r_leaf = sub_leaf(r, relations, stats, cost_model);
    let sel: f64 = connecting
        .iter()
        .map(|&e| edge_selectivity(&edges[e], relations, stats))
        .product();
    let out_card = (left.cardinality * r_leaf.cardinality * sel).max(MIN_CARD);

    let (algorithm, incremental) = best_algorithm(
        left.cardinality,
        &r_leaf,
        sel,
        r,
        connecting,
        edges,
        cost_model,
    );
    SubPlan {
        cardinality: out_card,
        cost: left.cost.add(incremental),
        order: JoinOrder::Join {
            left: Box::new(left.order.clone()),
            right: r,
            algorithm,
            edges: connecting.to_vec(),
        },
    }
}

/// Pick the cheapest join algorithm for `left ⋈ r`, returning it with the
/// incremental cost (which already includes how `r` is accessed). NLJ and
/// Hash materialize `r` via its leaf scan; INLJ replaces that with index
/// probes and so omits the leaf cost.
fn best_algorithm(
    left_card: f64,
    r_leaf: &SubPlan,
    sel: f64,
    r: RelId,
    connecting: &[usize],
    edges: &[JoinEdge],
    cost_model: &dyn CostModel,
) -> (JoinAlgorithm, Cost) {
    let right_card = r_leaf.cardinality;

    let nlj = r_leaf
        .cost
        .add(cost_model.cost_nested_loop_join(left_card, right_card));
    let (build, probe) = if left_card <= right_card {
        (left_card, right_card)
    } else {
        (right_card, left_card)
    };
    let hash = r_leaf.cost.add(cost_model.cost_hash_join(build, probe));

    let mut best = (JoinAlgorithm::NestedLoop, nlj);
    if cost_model.scalar(hash) < cost_model.scalar(best.1) {
        best = (JoinAlgorithm::Hash, hash);
    }

    // INLJ only if r's join column is indexed on a connecting edge: r is
    // the inner (probed) side in left-deep, so the index must be on r.
    let r_indexed = connecting.iter().any(|&e| {
        let edge = &edges[e];
        (edge.right == r && edge.right_col_indexed) || (edge.left == r && edge.left_col_indexed)
    });
    if r_indexed {
        let avg_matches = (right_card * sel).max(0.0);
        let inlj = cost_model.cost_index_nested_loop_join(left_card, avg_matches);
        if cost_model.scalar(inlj) < cost_model.scalar(best.1) {
            best = (JoinAlgorithm::IndexNestedLoop, inlj);
        }
    }
    best
}

/// Equi-join selectivity for one edge from the snapshot's column NDVs.
fn edge_selectivity(edge: &JoinEdge, relations: &[JoinRelation], stats: &QueryStats) -> f64 {
    let ndv_left = stats.ndv(relations[edge.left].table_id, edge.left_col);
    let ndv_right = stats.ndv(relations[edge.right].table_id, edge.right_col);
    join_selectivity(ndv_left, ndv_right)
}

/// Edge indices connecting relation `r` to any relation already in
/// `left_set`.
fn connecting_edges(edges: &[JoinEdge], r: RelId, left_set: RelSet) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, e) in edges.iter().enumerate() {
        let connects = (e.right == r && (left_set & (1u32 << e.left)) != 0)
            || (e.left == r && (left_set & (1u32 << e.right)) != 0);
        if connects {
            out.push(i);
        }
    }
    out
}

/// Keep the cheaper of two sub-plans under the model's scalarization.
fn cheaper(a: SubPlan, b: SubPlan, cost_model: &dyn CostModel) -> SubPlan {
    if cost_model.scalar(b.cost) < cost_model.scalar(a.cost) {
        b
    } else {
        a
    }
}

/// Greedy left-deep fallback: join relations smallest-first, applying an
/// edge's selectivity where one connects and otherwise crossing (so the
/// result always spans every relation — used for wide queries, exceeded
/// budgets, and disconnected graphs).
fn greedy_left_deep(
    relations: &[JoinRelation],
    edges: &[JoinEdge],
    stats: &QueryStats,
    cost_model: &dyn CostModel,
) -> JoinPlan {
    let n = relations.len();
    let mut order: Vec<RelId> = (0..n).collect();
    order.sort_by(|&a, &b| {
        base_cardinality(a, relations, stats)
            .partial_cmp(&base_cardinality(b, relations, stats))
            .expect("cardinalities are finite")
    });

    let mut plan = sub_leaf(order[0], relations, stats, cost_model);
    let mut joined: RelSet = 1u32 << order[0];
    for &r in &order[1..] {
        let connecting = connecting_edges(edges, r, joined);
        let r_leaf = sub_leaf(r, relations, stats, cost_model);
        let sel: f64 = if connecting.is_empty() {
            1.0 // cross product
        } else {
            connecting
                .iter()
                .map(|&e| edge_selectivity(&edges[e], relations, stats))
                .product()
        };
        let out_card = (plan.cardinality * r_leaf.cardinality * sel).max(MIN_CARD);
        let (algorithm, incremental) = best_algorithm(
            plan.cardinality,
            &r_leaf,
            sel,
            r,
            &connecting,
            edges,
            cost_model,
        );
        plan = SubPlan {
            cardinality: out_card,
            cost: plan.cost.add(incremental),
            order: JoinOrder::Join {
                left: Box::new(plan.order),
                right: r,
                algorithm,
                edges: connecting,
            },
        };
        joined |= 1u32 << r;
    }
    JoinPlan {
        order: plan.order,
        cost: plan.cost,
        cardinality: plan.cardinality,
    }
}

/// Base output cardinality of one relation after its local predicate.
fn base_cardinality(r: RelId, relations: &[JoinRelation], stats: &QueryStats) -> f64 {
    (stats.row_count(relations[r].table_id) * relations[r].local_selectivity).max(MIN_CARD)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::cost::DefaultCostModel;
    use crate::sql::stats::MockStatsProvider;

    /// `(table_id, row_count, &[(col, ndv)])` — terse table spec for DP tests.
    type TableSpec<'a> = (u32, i64, &'a [(u32, i64)]);

    /// Build a `QueryStats` from `(table_id, row_count, &[(col, ndv)])`
    /// triples — terse setup for the DP tests.
    fn stats_for(tables: &[TableSpec]) -> QueryStats {
        let mut provider = MockStatsProvider::new();
        let mut requests: Vec<(TableId, Vec<u32>)> = Vec::new();
        for &(tid, rows, cols) in tables {
            provider = provider.with_table_rows(TableId(tid), rows);
            let mut col_ids = Vec::new();
            for &(col, ndv) in cols {
                provider = provider.with_column_ndv(TableId(tid), col, ndv);
                col_ids.push(col);
            }
            requests.push((TableId(tid), col_ids));
        }
        let req_refs: Vec<(TableId, &[u32])> =
            requests.iter().map(|(t, c)| (*t, c.as_slice())).collect();
        QueryStats::gather(&provider, &req_refs).unwrap()
    }

    fn rel(table_id: u32) -> JoinRelation {
        JoinRelation {
            table_id: TableId(table_id),
            local_selectivity: 1.0,
        }
    }

    /// Count the leaf scans in an order — i.e. relations covered.
    fn scan_count(order: &JoinOrder) -> usize {
        match order {
            JoinOrder::Scan { .. } => 1,
            JoinOrder::Join { left, .. } => 1 + scan_count(left),
        }
    }

    #[test]
    fn single_relation_is_a_bare_scan() {
        let stats = stats_for(&[(1, 100, &[])]);
        let plan = enumerate_join_orders(&[rel(1)], &[], &stats, &DefaultCostModel::new(), None);
        assert_eq!(plan.order, JoinOrder::Scan { rel: 0 });
        assert_eq!(plan.cardinality, 100.0);
    }

    #[test]
    fn two_relations_pick_hash_at_scale() {
        // 10k × 10k equi-join: NLJ is 1e8 CPU; hash is linear → hash wins.
        let stats = stats_for(&[(1, 10_000, &[(0, 10_000)]), (2, 10_000, &[(0, 10_000)])]);
        let edges = vec![JoinEdge {
            left: 0,
            right: 1,
            left_col: 0,
            right_col: 0,
            left_col_indexed: false,
            right_col_indexed: false,
        }];
        let plan = enumerate_join_orders(
            &[rel(1), rel(2)],
            &edges,
            &stats,
            &DefaultCostModel::new(),
            None,
        );
        match plan.order {
            JoinOrder::Join { algorithm, .. } => assert_eq!(algorithm, JoinAlgorithm::Hash),
            _ => panic!("expected a join"),
        }
    }

    #[test]
    fn three_way_chain_joins_big_table_last() {
        // A(1) — B(10) — C(1_000_000). The only cheap left-deep order
        // builds the tiny {A,B} first and joins the huge C last.
        let stats = stats_for(&[
            (1, 1, &[(0, 1)]),
            (2, 10, &[(0, 10), (1, 10)]),
            (3, 1_000_000, &[(0, 1_000_000)]),
        ]);
        let edges = vec![
            JoinEdge {
                left: 0,
                right: 1,
                left_col: 0,
                right_col: 0,
                left_col_indexed: false,
                right_col_indexed: false,
            },
            JoinEdge {
                left: 1,
                right: 2,
                left_col: 1,
                right_col: 0,
                left_col_indexed: false,
                right_col_indexed: false,
            },
        ];
        let plan = enumerate_join_orders(
            &[rel(1), rel(2), rel(3)],
            &edges,
            &stats,
            &DefaultCostModel::new(),
            None,
        );
        match plan.order {
            JoinOrder::Join { right, .. } => {
                assert_eq!(right, 2, "big table C should be joined last")
            }
            _ => panic!("expected a join"),
        }
        assert_eq!(scan_count(&plan.order), 3);
    }

    #[test]
    fn index_nested_loop_chosen_for_tiny_outer_indexed_inner() {
        // A(5) probing B(1e6) on an indexed, high-NDV column: ~1 match per
        // outer row over only 5 rows → INLJ's few index probes beat both
        // a full hash build of B and a 5e6-CPU nested loop.
        let stats = stats_for(&[(1, 5, &[(0, 5)]), (2, 1_000_000, &[(0, 1_000_000)])]);
        let edges = vec![JoinEdge {
            left: 0,
            right: 1,
            left_col: 0,
            right_col: 0,
            left_col_indexed: false,
            right_col_indexed: true,
        }];
        let plan = enumerate_join_orders(
            &[rel(1), rel(2)],
            &edges,
            &stats,
            &DefaultCostModel::new(),
            None,
        );
        match plan.order {
            JoinOrder::Join {
                right, algorithm, ..
            } => {
                assert_eq!(right, 1, "indexed B should be the inner/probed side");
                assert_eq!(algorithm, JoinAlgorithm::IndexNestedLoop);
            }
            _ => panic!("expected a join"),
        }
    }

    #[test]
    fn disconnected_graph_falls_back_to_complete_greedy_plan() {
        // Two relations, no edge → DP forms no spanning plan; greedy
        // crosses them and still returns a complete order.
        let stats = stats_for(&[(1, 10, &[]), (2, 20, &[])]);
        let plan = enumerate_join_orders(
            &[rel(1), rel(2)],
            &[],
            &stats,
            &DefaultCostModel::new(),
            None,
        );
        assert_eq!(scan_count(&plan.order), 2);
    }

    #[test]
    fn wide_query_over_dp_cap_still_returns_complete_plan() {
        // 13 relations (> MAX_DP_RELATIONS) chained → greedy path. The
        // result must still cover every relation.
        let n = MAX_DP_RELATIONS + 1;
        let mut tables: Vec<TableSpec> = Vec::new();
        for i in 0..n {
            tables.push(((i + 1) as u32, (i as i64 + 1) * 100, &[]));
        }
        let stats = stats_for(&tables);
        let relations: Vec<JoinRelation> = (0..n).map(|i| rel((i + 1) as u32)).collect();
        let edges: Vec<JoinEdge> = (0..n - 1)
            .map(|i| JoinEdge {
                left: i,
                right: i + 1,
                left_col: 0,
                right_col: 0,
                left_col_indexed: false,
                right_col_indexed: false,
            })
            .collect();
        let plan =
            enumerate_join_orders(&relations, &edges, &stats, &DefaultCostModel::new(), None);
        assert_eq!(scan_count(&plan.order), n);
    }
}
