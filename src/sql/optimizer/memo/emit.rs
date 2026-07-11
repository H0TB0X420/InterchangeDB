//! T17-A.4 — emission: the memo's winner tree → `PhysOp`.
//!
//! The winner tree is implicit in the memo: a join `PhysChoice`
//! references child *groups*, whose recorded winners are the subtrees.
//! Emission walks it once (§6):
//!
//!   1. derive the physical relation order (in-order leaf traversal) and
//!      build a `ColumnRemap` textual → physical;
//!   2. leaves keep relation-local column indices (their tuple IS the
//!      relation) — recheck/leftover Filters included;
//!   3. join predicates are rewritten to **node coordinates**: a node's
//!      output tuple is a contiguous slice of the full physical layout
//!      (its subtree's leaves), so a column's index in that tuple is
//!      `remap(textual_global) − node_start`. The outer child starts at
//!      the node's own offset, the inner at `node_start + width(outer)`.
//!      For left-deep plans this degenerates to `column_map.rs`'s
//!      documented prefix property; bushy plans need the general rule.
//!   4. the statement spine is applied by the shared
//!      `apply_select_spine` with every index remapped — mirroring
//!      `selinger::rewrite_select`.

use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
use crate::sql::ir::physical::PhysOp;
use crate::sql::optimizer::column_map::ColumnRemap;
use crate::sql::optimizer::memo::memo::{full_relset, GroupId, Memo, PhysChoice, Winner};
use crate::sql::optimizer::memo::normalize::NormalizedQuery;
use crate::sql::optimizer::memo::props::{merge_requirements, OrderKey};
use crate::sql::optimizer::selinger::remap_aggregate;
use crate::sql::planner::{and_all, apply_select_spine};

/// Convert the memo's root winner into an executable `PhysOp` tree,
/// spine included. `optimize_root` must have run on `memo` first; when
/// `root_required` is `Some`, the matching ordered search must have run
/// too, and the delivered order CONSUMES the spine Sort (§7.5).
pub(crate) fn emit(
    memo: &Memo,
    query: &NormalizedQuery,
    root_required: &Option<OrderKey>,
) -> PhysOp {
    let relation_count = query.relations.len();
    let root_gid = memo
        .group_id_for(full_relset(relation_count))
        .expect("optimize_root materialized the full-query group");
    let root = winner_of(memo, root_gid, root_required);

    // 1. Physical relation order = in-order leaves of the winner tree.
    // Positive + negative space: exactly one leaf per relation;
    // `ColumnRemap::new` re-verifies the permutation and panics on a
    // duplicate (crash-on-corruption).
    let mut order = Vec::with_capacity(relation_count);
    collect_leaf_order(memo, query, &root.plan, &mut order);
    assert_eq!(order.len(), relation_count, "every relation exactly once");

    let widths = query.widths();
    let context = EmitContext {
        memo,
        query,
        remap: ColumnRemap::new(&widths, &order),
        widths,
        textual_base: query.textual_base(),
    };
    let core = context.build_choice(&root.plan, 0);

    // Spine (D3), every column index remapped to the physical layout.
    let residual: Vec<Predicate> = query
        .residual
        .iter()
        .cloned()
        .map(|p| context.remap.apply_predicate(p))
        .collect();
    let aggregates = query
        .spine
        .aggregates
        .iter()
        .cloned()
        .map(|a| remap_aggregate(a, &context.remap))
        .collect();
    // §7.5: a root winner searched under (and therefore delivering) the
    // ORDER BY requirement consumes the spine Sort — the user-visible
    // interesting-orders payoff.
    let order_by = if root_required.is_some() {
        Vec::new()
    } else {
        query
            .spine
            .order_by
            .iter()
            .map(|&(col, dir)| (context.remap.apply_index(col), dir))
            .collect()
    };
    let projection = query
        .spine
        .projection
        .iter()
        .map(|&col| context.remap.apply_index(col))
        .collect();
    apply_select_spine(
        core,
        residual,
        aggregates,
        order_by,
        projection,
        query.spine.limit,
    )
}

/// A group's recorded winner for one order requirement — present for
/// every `(group, requirement)` on the winner path once `optimize_root`
/// has run.
fn winner_of<'a>(memo: &'a Memo, gid: GroupId, required: &Option<OrderKey>) -> &'a Winner {
    memo.group(gid)
        .winners
        .get(required)
        .expect("winner-path group has a recorded winner for its requirement")
}

/// In-order leaf traversal (outer/left before inner/right) — defines the
/// physical column layout. Child winners resolve under the same order
/// requirements the search imposed (`merge_requirements` for Merge,
/// unordered for everything else).
fn collect_leaf_order(
    memo: &Memo,
    query: &NormalizedQuery,
    choice: &PhysChoice,
    out: &mut Vec<usize>,
) {
    match choice {
        PhysChoice::SeqScan { rel }
        | PhysChoice::IndexScan { rel, .. }
        | PhysChoice::PkLookup { rel, .. } => out.push(*rel),
        PhysChoice::NestedLoop { outer, inner, .. } | PhysChoice::Hash { outer, inner, .. } => {
            collect_leaf_order(memo, query, &winner_of(memo, *outer, &None).plan, out);
            collect_leaf_order(memo, query, &winner_of(memo, *inner, &None).plan, out);
        }
        PhysChoice::IndexNested {
            outer, inner_rel, ..
        } => {
            collect_leaf_order(memo, query, &winner_of(memo, *outer, &None).plan, out);
            out.push(*inner_rel);
        }
        PhysChoice::Merge { left, right, edges } => {
            let key_edge = &query.edges[edges[0]];
            let (left_required, right_required) =
                merge_requirements(key_edge, memo.group(*left).relset);
            collect_leaf_order(
                memo,
                query,
                &winner_of(memo, *left, &Some(left_required)).plan,
                out,
            );
            collect_leaf_order(
                memo,
                query,
                &winner_of(memo, *right, &Some(right_required)).plan,
                out,
            );
        }
        PhysChoice::SortEnforcer { child, .. } => {
            // The enforcer reorders rows, not columns — its layout is
            // its child's unordered winner's.
            collect_leaf_order(memo, query, &winner_of(memo, *child, &None).plan, out);
        }
    }
}

/// Shared emission state: the remap plus the layout tables node-
/// coordinate math needs.
struct EmitContext<'a> {
    memo: &'a Memo,
    query: &'a NormalizedQuery,
    remap: ColumnRemap,
    widths: Vec<usize>,
    textual_base: Vec<usize>,
}

impl EmitContext<'_> {
    /// A textual-global column's index within the tuple of the node
    /// whose subtree starts at `node_start` in the physical layout.
    fn node_coord(&self, textual_global: usize, node_start: usize) -> usize {
        let physical = self.remap.apply_index(textual_global);
        debug_assert!(
            physical >= node_start,
            "column {textual_global} does not belong to this subtree"
        );
        physical - node_start
    }

    /// Total column width of a group's relations.
    fn group_width(&self, gid: GroupId) -> usize {
        let relset = self.memo.group(gid).relset;
        self.widths
            .iter()
            .enumerate()
            .filter(|(rel, _)| relset & (1u32 << rel) != 0)
            .map(|(_, w)| w)
            .sum()
    }

    /// One equi-edge as a `Column = Column` compare in node coordinates
    /// (mirrors `selinger::build_on_predicate`).
    fn edge_compare(&self, edge_index: usize, node_start: usize) -> Predicate {
        let edge = &self.query.edges[edge_index];
        let ga = self.textual_base[edge.left_rel] + edge.left_col;
        let gb = self.textual_base[edge.right_rel] + edge.right_col;
        Predicate::Compare {
            op: CompareOp::Eq,
            left: Expression::Column(self.node_coord(ga, node_start)),
            right: Expression::Column(self.node_coord(gb, node_start)),
        }
    }

    /// An edge's endpoints as textual-global columns, ordered
    /// (outer-side, inner-side) for the join whose outer group is given.
    fn oriented_endpoints(&self, edge_index: usize, outer: GroupId) -> (usize, usize) {
        let edge = &self.query.edges[edge_index];
        let ga = self.textual_base[edge.left_rel] + edge.left_col;
        let gb = self.textual_base[edge.right_rel] + edge.right_col;
        let outer_set = self.memo.group(outer).relset;
        if outer_set & (1u32 << edge.left_rel) != 0 {
            debug_assert!(outer_set & (1u32 << edge.right_rel) == 0, "edge must cross");
            (ga, gb)
        } else {
            debug_assert!(outer_set & (1u32 << edge.right_rel) != 0, "edge must cross");
            (gb, ga)
        }
    }

    /// Build the `PhysOp` subtree for one group's winner under an order
    /// requirement, whose output slice starts at `node_start` in the
    /// full physical layout.
    fn build_group(&self, gid: GroupId, node_start: usize, required: &Option<OrderKey>) -> PhysOp {
        self.build_choice(&winner_of(self.memo, gid, required).plan, node_start)
    }

    fn build_choice(&self, choice: &PhysChoice, node_start: usize) -> PhysOp {
        match choice {
            // Leaves: relation-local coordinates, no remap (§6.2). The
            // parent join accounts for their placement.
            PhysChoice::SeqScan { rel } => {
                let info = &self.query.relations[*rel];
                let preds: Vec<Predicate> =
                    info.local_preds.iter().map(|p| p.pred.clone()).collect();
                filter_above(
                    PhysOp::SeqScan {
                        table: info.name.clone(),
                    },
                    and_all(preds),
                )
            }
            PhysChoice::IndexScan {
                rel,
                index,
                prefix,
                filter,
            } => {
                let info = &self.query.relations[*rel];
                // The recheck Filter is a correctness invariant (E1/O12)
                // — `filter` always carries at least the recheck.
                PhysOp::Filter {
                    input: Box::new(PhysOp::IndexScan {
                        table: info.name.clone(),
                        index: index.clone(),
                        prefix: prefix.clone(),
                    }),
                    predicate: filter.clone(),
                }
            }
            PhysChoice::PkLookup { rel, pk, residual } => {
                let info = &self.query.relations[*rel];
                filter_above(
                    PhysOp::PkLookup {
                        table: info.name.clone(),
                        pk: pk.clone(),
                    },
                    residual.clone(),
                )
            }

            PhysChoice::NestedLoop {
                outer,
                inner,
                edges,
            } => {
                let outer_width = self.group_width(*outer);
                let outer_op = self.build_group(*outer, node_start, &None);
                let inner_op = self.build_group(*inner, node_start + outer_width, &None);
                let on = and_all(
                    edges
                        .iter()
                        .map(|&e| self.edge_compare(e, node_start))
                        .collect(),
                );
                debug_assert!(on.is_some(), "split invariant: at least one edge");
                PhysOp::NestedLoopJoin {
                    outer: Box::new(outer_op),
                    inner: Box::new(inner_op),
                    on,
                }
            }

            PhysChoice::Hash {
                outer,
                inner,
                edges,
            } => {
                let outer_width = self.group_width(*outer);
                let outer_op = self.build_group(*outer, node_start, &None);
                let inner_op = self.build_group(*inner, node_start + outer_width, &None);
                // edges[0] is the hash key; the executor's inner key is
                // local to the inner subtree's own tuple.
                let (g_outer, g_inner) = self.oriented_endpoints(edges[0], *outer);
                let join = PhysOp::HashJoin {
                    outer: Box::new(outer_op),
                    inner: Box::new(inner_op),
                    outer_key_col: self.node_coord(g_outer, node_start),
                    inner_key_col: self.node_coord(g_inner, node_start + outer_width),
                };
                // Non-key edges: residual join filter (single-key
                // HashJoin limitation, §5).
                let residual = and_all(
                    edges[1..]
                        .iter()
                        .map(|&e| self.edge_compare(e, node_start))
                        .collect(),
                );
                filter_above(join, residual)
            }

            PhysChoice::IndexNested {
                outer,
                inner_rel,
                index,
                probe,
                edges,
            } => {
                let outer_op = self.build_group(*outer, node_start, &None);
                let (g_outer, _) = self.oriented_endpoints(*probe, *outer);
                let info = &self.query.relations[*inner_rel];
                let join = PhysOp::IndexNestedLoopJoin {
                    outer: Box::new(outer_op),
                    inner_table: info.name.clone(),
                    inner_index: index.clone(),
                    outer_key_cols: vec![self.node_coord(g_outer, node_start)],
                };
                // Probes carry neither the non-probe edges nor the
                // inner's local predicates — both filter above the join
                // (plan_select's INLJ path kicks them upward the same
                // way, just to the top of the tree instead).
                let mut parts: Vec<Predicate> = edges
                    .iter()
                    .filter(|&&e| e != *probe)
                    .map(|&e| self.edge_compare(e, node_start))
                    .collect();
                let inner_base = self.node_coord(self.textual_base[*inner_rel], node_start);
                parts.extend(
                    info.local_preds
                        .iter()
                        .map(|p| raise_predicate(p.pred.clone(), inner_base)),
                );
                filter_above(join, and_all(parts))
            }

            PhysChoice::Merge { left, right, edges } => {
                let left_width = self.group_width(*left);
                // Children resolve under the same requirements the
                // search imposed — merge_requirements is the single
                // source of truth for both sides.
                let key_edge = &self.query.edges[edges[0]];
                let (left_required, right_required) =
                    merge_requirements(key_edge, self.memo.group(*left).relset);
                let left_op = self.build_group(*left, node_start, &Some(left_required));
                let right_op =
                    self.build_group(*right, node_start + left_width, &Some(right_required));
                let (g_left, g_right) = self.oriented_endpoints(edges[0], *left);
                let join = PhysOp::MergeJoin {
                    left: Box::new(left_op),
                    right: Box::new(right_op),
                    left_key_col: self.node_coord(g_left, node_start),
                    right_key_col: self.node_coord(g_right, node_start + left_width),
                };
                // Non-key edges: residual join filter (single-key merge,
                // same limitation as Hash).
                let residual = and_all(
                    edges[1..]
                        .iter()
                        .map(|&e| self.edge_compare(e, node_start))
                        .collect(),
                );
                filter_above(join, residual)
            }

            PhysChoice::SortEnforcer { child, key } => {
                // The enforcer sorts its group's unordered winner; the
                // layout (and therefore `node_start`) is unchanged, so
                // the key columns land in the child's own coordinates.
                let child_op = self.build_group(*child, node_start, &None);
                let keys = key
                    .iter()
                    .map(|&((rel, col), dir)| {
                        (
                            self.node_coord(self.textual_base[rel] + col, node_start),
                            dir,
                        )
                    })
                    .collect();
                PhysOp::Sort {
                    input: Box::new(child_op),
                    keys,
                }
            }
        }
    }
}

/// Wrap `op` in a Filter when there is a predicate to apply.
fn filter_above(op: PhysOp, predicate: Option<Predicate>) -> PhysOp {
    match predicate {
        Some(predicate) => PhysOp::Filter {
            input: Box::new(op),
            predicate,
        },
        None => op,
    }
}

/// Shift every column index in an expression up by `offset` — the
/// inverse of `planner::rebase_expr`, turning a relation-local index
/// into a join-tuple index.
fn raise_expr(expr: Expression, offset: usize) -> Expression {
    match expr {
        Expression::Column(i) => Expression::Column(i + offset),
        lit_or_param @ (Expression::Literal(_) | Expression::Parameter(_)) => lit_or_param,
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op,
            left: Box::new(raise_expr(*left, offset)),
            right: Box::new(raise_expr(*right, offset)),
        },
    }
}

/// Raise every column in a predicate by `offset` (see `raise_expr`).
fn raise_predicate(pred: Predicate, offset: usize) -> Predicate {
    match pred {
        Predicate::Compare { op, left, right } => Predicate::Compare {
            op,
            left: raise_expr(left, offset),
            right: raise_expr(right, offset),
        },
        Predicate::And(a, b) => Predicate::And(
            Box::new(raise_predicate(*a, offset)),
            Box::new(raise_predicate(*b, offset)),
        ),
        Predicate::Or(a, b) => Predicate::Or(
            Box::new(raise_predicate(*a, offset)),
            Box::new(raise_predicate(*b, offset)),
        ),
        Predicate::Not(p) => Predicate::Not(Box::new(raise_predicate(*p, offset))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::cost::DefaultCostModel;
    use crate::sql::optimizer::memo::fixtures::{bind, default_stats, setup, Env};
    use crate::sql::optimizer::memo::normalize::normalize;
    use crate::sql::optimizer::memo::search::optimize_root;
    use crate::sql::optimizer::stats::{MockStatsProvider, QueryStats};
    use crate::sql::planner::plan;
    use crate::sql::planner::PhysicalPlan;

    /// Full memo pipeline: bind → normalize → search → emit (no ORDER BY
    /// consumption — that decision belongs to `VolcanoPlanner`).
    fn memo_plan(env: &Env, sql: &str, stats: &QueryStats) -> PhysOp {
        let logical = bind(env, sql);
        let query = normalize(&logical, &env.catalog, stats).unwrap().unwrap();
        let mut memo = Memo::new(&query);
        optimize_root(&mut memo, &query, &DefaultCostModel::new());
        emit(&memo, &query, &None)
    }

    fn rule_based_explain(env: &Env, sql: &str) -> String {
        match plan(bind(env, sql), &env.catalog).unwrap() {
            PhysicalPlan::Query(op) => op.explain(0),
            _ => panic!("expected a query plan"),
        }
    }

    #[test]
    fn single_table_pk_explain_matches_rule_based() {
        // The simplest parity case: identical tree, byte-identical
        // EXPLAIN (a G2 teaser at unit scope).
        let env = setup();
        let sql = "SELECT w_id FROM wh WHERE w_id = 1";
        let memo_tree = memo_plan(&env, sql, &default_stats()).explain(0);
        assert_eq!(memo_tree, rule_based_explain(&env, sql));
        assert_eq!(memo_tree, "Projection([0])\n  PkLookup(wh)\n");
    }

    #[test]
    fn index_scan_recheck_filter_survives_emission() {
        // The E1/O12 invariant (§10): the Filter above IndexScan must
        // exist in the emitted plan.
        let env = setup();
        let provider = MockStatsProvider::new()
            .with_table_rows(env.dist_id, 1000)
            .with_column_ndv(env.dist_id, 1, 1000);
        let stats = QueryStats::gather(&provider, &[(env.dist_id, &[0, 1, 2])]).unwrap();
        let tree = memo_plan(&env, "SELECT d_id FROM dist WHERE d_w_id = 7", &stats).explain(0);
        let expected = "\
Projection([0])
  Filter
    IndexScan(dist, on dist_by_w)
";
        assert_eq!(tree, expected);
    }

    #[test]
    fn spine_applies_aggregate_sort_and_limit() {
        let env = setup();
        let stats = default_stats();
        let agg = memo_plan(&env, "SELECT COUNT(*) FROM wh WHERE w_ytd = 100", &stats).explain(0);
        assert_eq!(agg, "HashAggregate[COUNT(*)]\n  Filter\n    SeqScan(wh)\n");

        let sorted = memo_plan(
            &env,
            "SELECT w_id FROM wh WHERE w_ytd = 100 ORDER BY w_id DESC LIMIT 3",
            &stats,
        )
        .explain(0);
        let expected = "\
Limit(3)
  Projection([0])
    Sort[0 DESC]
      Filter
        SeqScan(wh)
";
        assert_eq!(sorted, expected);
    }

    #[test]
    fn inlj_emission_filters_inner_local_preds_above_join() {
        // INLJ winner with a local predicate on the probed relation: the
        // predicate cannot ride the probe, so it must appear as a Filter
        // above the join, raised to join-tuple coordinates.
        let env = setup();
        let provider = MockStatsProvider::new()
            .with_table_rows(env.wh_id, 5)
            .with_column_ndv(env.wh_id, 0, 5)
            .with_table_rows(env.dist_id, 1_000_000)
            .with_column_ndv(env.dist_id, 1, 1_000_000);
        let stats = QueryStats::gather(
            &provider,
            &[(env.wh_id, &[0, 1]), (env.dist_id, &[0, 1, 2])],
        )
        .unwrap();
        let op = memo_plan(
            &env,
            "SELECT w_id FROM wh JOIN dist ON d_w_id = w_id WHERE d_ytd = 5",
            &stats,
        );
        let tree = op.explain(0);
        let expected = "\
Projection([0])
  Filter
    IndexNestedLoopJoin(probe via dist_by_w)
      SeqScan(wh)
      <inner: IndexScan on dist_by_w>
";
        assert_eq!(tree, expected);
        // The raised predicate must address d_ytd at join-tuple index
        // wh(2) + dist-local(2) = 4.
        fn find_filter(op: &PhysOp) -> &Predicate {
            match op {
                PhysOp::Projection { input, .. } => find_filter(input),
                PhysOp::Filter { predicate, .. } => predicate,
                other => panic!("expected Filter above the join, got {other:?}"),
            }
        }
        match find_filter(&op) {
            Predicate::Compare {
                left: Expression::Column(c),
                ..
            } => assert_eq!(*c, 4),
            other => panic!("expected raised d_ytd compare, got {other:?}"),
        }
    }

    #[test]
    fn residual_cross_table_filter_lands_in_spine() {
        let env = setup();
        let tree = memo_plan(
            &env,
            "SELECT w_id FROM wh, dist WHERE d_w_id = w_id AND w_ytd < d_ytd",
            &default_stats(),
        )
        .explain(0);
        // Whatever join the search picks, the non-equi cross-table
        // conjunct must surface as a spine Filter above it.
        assert!(
            tree.starts_with("Projection([0])\n  Filter\n"),
            "residual filter missing from spine: {tree}"
        );
    }

    #[test]
    fn bushy_plan_emitted_when_cheapest() {
        // cust(1 row) shrinks dist to ~1 row when joined first, so the
        // cheapest tree is right-deep bushy — wh ⋈ (dist ⋈ cust) — a
        // shape the left-deep Selinger planner cannot produce (D4). Leaf
        // order stays textual, so the projection is unremapped.
        let env = setup();
        let cust_id = env.catalog.get_table("cust").unwrap().table_id;
        let provider = MockStatsProvider::new()
            .with_table_rows(env.wh_id, 1_000_000)
            .with_column_ndv(env.wh_id, 0, 1_000_000)
            .with_table_rows(env.dist_id, 100_000)
            .with_column_ndv(env.dist_id, 0, 100_000)
            .with_column_ndv(env.dist_id, 1, 1_000_000)
            .with_table_rows(cust_id, 1)
            .with_column_ndv(cust_id, 1, 1);
        let stats = QueryStats::gather(
            &provider,
            &[
                (env.wh_id, &[0, 1]),
                (env.dist_id, &[0, 1, 2]),
                (cust_id, &[0, 1]),
            ],
        )
        .unwrap();
        let tree = memo_plan(
            &env,
            "SELECT w_id FROM wh JOIN dist ON d_w_id = w_id JOIN cust ON c_d_id = d_id",
            &stats,
        )
        .explain(0);
        let expected = "\
Projection([0])
  NestedLoopJoin
    SeqScan(wh)
    NestedLoopJoin
      SeqScan(dist)
      SeqScan(cust)
";
        assert_eq!(tree, expected);
    }

    #[test]
    fn reordered_leaves_remap_projection() {
        // Textual order dist(1M, indexed) → wh(5): the winner drives the
        // probes from tiny wh, so the leaf order flips to [wh, dist] and
        // d_id (textual-global 0) must be remapped to physical 2 (after
        // wh's two columns).
        let env = setup();
        let provider = MockStatsProvider::new()
            .with_table_rows(env.wh_id, 5)
            .with_column_ndv(env.wh_id, 0, 5)
            .with_table_rows(env.dist_id, 1_000_000)
            .with_column_ndv(env.dist_id, 1, 1_000_000);
        let stats = QueryStats::gather(
            &provider,
            &[(env.wh_id, &[0, 1]), (env.dist_id, &[0, 1, 2])],
        )
        .unwrap();
        let tree = memo_plan(
            &env,
            "SELECT d_id FROM dist JOIN wh ON d_w_id = w_id",
            &stats,
        )
        .explain(0);
        let expected = "\
Projection([2])
  IndexNestedLoopJoin(probe via dist_by_w)
    SeqScan(wh)
    <inner: IndexScan on dist_by_w>
";
        assert_eq!(tree, expected);
    }
}
