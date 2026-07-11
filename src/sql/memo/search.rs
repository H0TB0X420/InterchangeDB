//! T17-A.3 — top-down search with branch-and-bound over the memo.
//!
//! `optimize_root` drives `optimize(group, budget)`: enumerate a group's
//! physical candidates (§5), recurse into each candidate's child groups
//! under a shrinking scalar budget, and record the cheapest complete
//! plan as the group's winner. Budgets follow §4: the root runs
//! unbounded; a child's budget is what remains of the parent's after the
//! cost accumulated so far; sibling lower bounds are 0 (documented
//! simplification — tightening is a recorded future lever).
//!
//! The §4 `optimal` flag guards the classic B&B-memoization bug: a
//! winner recorded under a finite budget answers only callers whose
//! budget it fits — a larger budget re-searches instead of trusting it.
//!
//! NOTE (deviation from the plan §4 pseudocode): the pseudocode prunes
//! against the caller's fixed budget only; here the bound also tightens
//! to the best complete candidate found so far. Without that, nothing
//! ever prunes under the root's infinite budget — best-so-far tightening
//! is what the A.3 counting test measures. Pruning stays sound: a
//! candidate is discarded only when it provably cannot beat the current
//! best.
//!
//! NOTE (deviation from the plan §2 layout): the transient `Candidate`
//! struct ("PhysicalCandidate") lives here rather than in `memo.rs` —
//! search is its only producer and consumer.

use crate::sql::cost::{Cost, CostModel};
use crate::sql::join_order::{RelId, RelSet, ROWS_PER_PAGE_ESTIMATE};
use crate::sql::memo::memo::{full_relset, GroupId, Memo, PhysChoice, Winner};
use crate::sql::memo::normalize::{Edge, NormalizedQuery, RelInfo};
use crate::sql::memo::props::{merge_requirements, OrderKey};
use crate::sql::planner::{and_all, try_lower_index_predicate, try_lower_pk_lookup, IndexLowering};

/// One physical implementation alternative for a group: the operator's
/// own cost plus the child groups (with their order requirements) still
/// to optimize.
struct Candidate {
    local_cost: Cost,
    children: Vec<(GroupId, Option<OrderKey>)>,
    plan: PhysChoice,
}

/// Optimize the full-query group under an infinite budget with no order
/// requirement and return its winner. Every group's recorded winner is
/// optimal afterwards — the invariant emission (T17-A.4) walks on.
pub(crate) fn optimize_root(
    memo: &mut Memo,
    query: &NormalizedQuery,
    cost_model: &dyn CostModel,
) -> Winner {
    let root = memo.group_for(full_relset(query.relations.len()), query);
    let winner = optimize(memo, root, f64::INFINITY, None, query, cost_model)
        .expect("an unbounded search always finds a plan — SeqScan/NestedLoop are total");
    debug_assert!(winner.optimal, "the root search runs unbounded");
    winner
}

/// Optimize the full-query group under `required` with an infinite
/// budget — T17-B.3's ORDER BY consumption entry point. Total because
/// the enforcer always qualifies; the caller compares the result's cost
/// against unordered-plus-Sort before committing to it.
pub(crate) fn optimize_root_ordered(
    memo: &mut Memo,
    query: &NormalizedQuery,
    cost_model: &dyn CostModel,
    required: &OrderKey,
) -> Winner {
    let root = memo.group_for(full_relset(query.relations.len()), query);
    optimize(memo, root, f64::INFINITY, Some(required), query, cost_model)
        .expect("the enforcer makes every ordered search total")
}

/// Best plan for `gid` that delivers `required` (`None` = any order)
/// with scalar cost ≤ `budget`, or `None` if no candidate fits. Records
/// the group's winner for that requirement (§4, per-order in 17-B).
fn optimize(
    memo: &mut Memo,
    gid: GroupId,
    budget: f64,
    required: Option<&OrderKey>,
    query: &NormalizedQuery,
    cost_model: &dyn CostModel,
) -> Option<Winner> {
    let requirement: Option<OrderKey> = required.cloned();
    // Cached winner (§4): an optimal one answers any budget; a bounded
    // one answers only budgets it fits — for a larger budget we fall
    // through and re-search rather than trust it.
    if let Some(winner) = memo.group(gid).winners.get(&requirement) {
        if winner.optimal {
            return if cost_model.scalar(winner.cost) <= budget {
                Some(winner.clone())
            } else {
                None
            };
        }
        if cost_model.scalar(winner.cost) <= budget {
            return Some(winner.clone());
        }
    }

    let candidates = candidates_for(memo, gid, required, query, cost_model);
    assert!(
        !candidates.is_empty(),
        "every group has a total candidate (SeqScan / NestedLoop / SortEnforcer)"
    );

    // Branch-and-bound: `bound` starts at the caller's budget and
    // tightens to the best complete candidate so far; it prunes both the
    // local-cost check and the children's remaining budgets.
    let mut bound = budget;
    let mut best: Option<Winner> = None;
    for candidate in candidates {
        let mut total = candidate.local_cost;
        if cost_model.scalar(total) > bound {
            continue; // prune before recursing into any child
        }
        let mut complete = true;
        for (child, child_required) in &candidate.children {
            let child_budget = bound - cost_model.scalar(total);
            match optimize(
                memo,
                *child,
                child_budget,
                child_required.as_ref(),
                query,
                cost_model,
            ) {
                Some(child_winner) => total = total.add(child_winner.cost),
                None => {
                    // Child has no plan within the remaining budget →
                    // this candidate cannot beat the current bound.
                    complete = false;
                    break;
                }
            }
        }
        if !complete {
            continue;
        }
        let improves = match &best {
            None => true,
            Some(current) => cost_model.scalar(total) < cost_model.scalar(current.cost),
        };
        if improves {
            bound = cost_model.scalar(total);
            best = Some(Winner {
                cost: total,
                plan: candidate.plan,
                optimal: budget.is_infinite(),
            });
        }
    }

    if let Some(winner) = &best {
        // Record only improvements — a bounded re-search must never
        // clobber a better cached winner.
        let group = memo.group_mut(gid);
        let improves = group
            .winners
            .get(&requirement)
            .is_none_or(|c| cost_model.scalar(winner.cost) < cost_model.scalar(c.cost));
        if improves {
            group.winners.insert(requirement, winner.clone());
        }
    }
    best
}

/// Physical candidates for one group under a requirement (§5 + §7).
/// Creates child groups lazily (costing needs their cardinality).
fn candidates_for(
    memo: &mut Memo,
    gid: GroupId,
    required: Option<&OrderKey>,
    query: &NormalizedQuery,
    cost_model: &dyn CostModel,
) -> Vec<Candidate> {
    let relset = memo.group(gid).relset;
    let mut out = Vec::new();
    match required {
        // Unordered: every access path / join algorithm qualifies —
        // whatever order a plan happens to deliver satisfies "none".
        None => {
            if relset.count_ones() == 1 {
                let rel = relset.trailing_zeros() as RelId;
                return leaf_candidates(rel, &query.relations[rel], cost_model);
            }
            let splits = memo.group(gid).splits.clone();
            for (left, right) in splits {
                split_candidates(memo, left, right, query, cost_model, &mut out);
            }
        }
        // Ordered: only plans that DELIVER the key (§7.4) — merge joins
        // passing it through from their left input, plus the enforcer,
        // which sorts this group's unordered winner. Leaves deliver
        // nothing (index-order delivery is a non-goal, D9).
        Some(key) => {
            if relset.count_ones() > 1 {
                let splits = memo.group(gid).splits.clone();
                for (left, right) in splits {
                    merge_candidates_delivering(
                        memo, left, right, key, query, cost_model, &mut out,
                    );
                }
            }
            out.push(Candidate {
                local_cost: cost_model.cost_sort(memo.group(gid).cardinality),
                children: vec![(gid, None)],
                plan: PhysChoice::SortEnforcer {
                    child: gid,
                    key: key.clone(),
                },
            });
        }
    }
    out
}

/// Leaf access-path candidates (§5), sharing `plan_select`'s lowering
/// verbatim (D6) and `build_left_leaf`'s access/leftover split: the
/// claiming conjunct becomes the access path, every other conjunct stays
/// on the leaf Filter.
fn leaf_candidates(rel: RelId, info: &RelInfo, cost_model: &dyn CostModel) -> Vec<Candidate> {
    let raw_rows = info.row_count;
    let mut candidates = Vec::new();

    // 1. PkLookup — the first conjunct pinning the PK claims the access
    // path. Cost per §5: one index probe (`cost_index_scan(1.0)`).
    for (claimed, local) in info.local_preds.iter().enumerate() {
        if let Some(pk) = try_lower_pk_lookup(&local.pred, &info.schema) {
            let leftover: Vec<_> = info
                .local_preds
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != claimed)
                .map(|(_, other)| other.pred.clone())
                .collect();
            candidates.push(Candidate {
                local_cost: cost_model.cost_index_scan(1.0),
                children: Vec::new(),
                plan: PhysChoice::PkLookup {
                    rel,
                    pk,
                    residual: and_all(leftover),
                },
            });
            break;
        }
    }

    // 2. IndexScan — the first index-lowerable conjunct. The mandatory
    // E1/O12 recheck ANDs with the leftovers into the leaf filter. Cost:
    // a probe for the rows the *index* matches (this conjunct's own
    // selectivity — the filter narrows further) plus the filter pass.
    for (claimed, local) in info.local_preds.iter().enumerate() {
        if let IndexLowering::Matched {
            handle,
            prefix,
            recheck,
        } = try_lower_index_predicate(local.pred.clone(), &info.indexes)
        {
            let mut filter_parts = vec![recheck];
            filter_parts.extend(
                info.local_preds
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != claimed)
                    .map(|(_, other)| other.pred.clone()),
            );
            let filter = and_all(filter_parts).expect("recheck is always present");
            let matching_rows = raw_rows * local.selectivity;
            candidates.push(Candidate {
                local_cost: cost_model
                    .cost_index_scan(matching_rows)
                    .add(cost_model.cost_filter(matching_rows)),
                children: Vec::new(),
                plan: PhysChoice::IndexScan {
                    rel,
                    index: handle.def.name.clone(),
                    prefix,
                    filter,
                },
            });
            break;
        }
    }

    // 3. SeqScan — always available. Argument-for-argument parity with
    // `join_order::sub_leaf` (G3): pages via the shared rows-per-page
    // constant, a filter charge only when the predicates narrow.
    let pages = (raw_rows / ROWS_PER_PAGE_ESTIMATE).ceil().max(1.0);
    let mut seq_cost = cost_model.cost_seq_scan(pages, raw_rows);
    if info.local_selectivity() < 1.0 {
        seq_cost = seq_cost.add(cost_model.cost_filter(raw_rows));
    }
    candidates.push(Candidate {
        local_cost: seq_cost,
        children: Vec::new(),
        plan: PhysChoice::SeqScan { rel },
    });

    candidates
}

/// Join candidates for one split, both orientations (D5): Hash always
/// (an equi-edge exists by the split invariant), IndexNested when the
/// inner side is a single indexed relation, NestedLoop as the total
/// fallback. Cost formulas keep argument parity with
/// `join_order::best_algorithm` (G3).
fn split_candidates(
    memo: &mut Memo,
    left: RelSet,
    right: RelSet,
    query: &NormalizedQuery,
    cost_model: &dyn CostModel,
    out: &mut Vec<Candidate>,
) {
    let crossing = crossing_edges(left, right, &query.edges);
    assert!(
        !crossing.is_empty(),
        "split invariant: at least one crossing edge"
    );
    // `best_algorithm`'s `sel` argument: product over connecting edges.
    let crossing_selectivity: f64 = crossing
        .iter()
        .map(|&e| query.edges[e].selectivity)
        .product();

    for (outer_set, inner_set) in [(left, right), (right, left)] {
        let outer = memo.group_for(outer_set, query);
        let inner = memo.group_for(inner_set, query);
        let outer_card = memo.group(outer).cardinality;
        let inner_card = memo.group(inner).cardinality;

        // T17-A.5 promise ordering: optimize the smaller-cardinality
        // child first — its (cheap) winner is subtracted from the budget
        // before the expensive child's search begins, so that search
        // runs under the tightest bound and prunes hardest.
        let children: Vec<(GroupId, Option<OrderKey>)> = if inner_card < outer_card {
            vec![(inner, None), (outer, None)]
        } else {
            vec![(outer, None), (inner, None)]
        };

        // Hash: `edges[0]` is the key. Costed for the honest orientation
        // (build = inner, the side the executor materializes). NOTE:
        // `best_algorithm` costs min/max instead — identical under the
        // symmetric default model, and the min emerges here from
        // generating both orientations.
        out.push(Candidate {
            local_cost: cost_model.cost_hash_join(inner_card, outer_card),
            children: children.clone(),
            plan: PhysChoice::Hash {
                outer,
                inner,
                edges: crossing.clone(),
            },
        });

        // IndexNested: inner must be a single relation probed via a
        // single-column index on its side of a crossing edge. No inner
        // child — probes replace the inner scan (`best_algorithm` omits
        // the leaf cost the same way).
        if inner_set.count_ones() == 1 {
            let inner_rel = inner_set.trailing_zeros() as RelId;
            if let Some((probe, index)) = probe_edge(inner_rel, &crossing, query) {
                let avg_matches = (inner_card * crossing_selectivity).max(0.0);
                out.push(Candidate {
                    local_cost: cost_model.cost_index_nested_loop_join(outer_card, avg_matches),
                    children: vec![(outer, None)],
                    plan: PhysChoice::IndexNested {
                        outer,
                        inner_rel,
                        index,
                        probe,
                        edges: crossing.clone(),
                    },
                });
            }
        }

        // NestedLoop: always available; the ON carries every crossing edge.
        out.push(Candidate {
            local_cost: cost_model.cost_nested_loop_join(outer_card, inner_card),
            children,
            plan: PhysChoice::NestedLoop {
                outer,
                inner,
                edges: crossing.clone(),
            },
        });

        // Merge (T17-B.2): a valid unordered plan too — the children
        // acquire their sortedness internally (native delivery or a
        // SortEnforcer). Under the default cost model Sort+Merge is
        // Hash plus two n·log n sorts, so it never wins here; an
        // IO-heavier hash model flips that (the B.2 cost-flip test).
        let (candidate, _delivered) =
            merge_candidate(memo, outer_set, inner_set, &crossing, query, cost_model);
        out.push(candidate);
    }
}

/// The merge candidate for one *oriented* split, plus the order it
/// delivers (its left input's — §7.4). `crossing[0]` is the merge key,
/// the remaining crossing edges become a residual filter at emission
/// (same single-key limitation as Hash).
fn merge_candidate(
    memo: &mut Memo,
    left_set: RelSet,
    right_set: RelSet,
    crossing: &[usize],
    query: &NormalizedQuery,
    cost_model: &dyn CostModel,
) -> (Candidate, OrderKey) {
    let key_edge = &query.edges[crossing[0]];
    let (left_required, right_required) = merge_requirements(key_edge, left_set);
    let left = memo.group_for(left_set, query);
    let right = memo.group_for(right_set, query);
    let left_card = memo.group(left).cardinality;
    let right_card = memo.group(right).cardinality;
    // T17-A.5 promise ordering, same rule as the other join candidates.
    let children = if right_card < left_card {
        vec![
            (right, Some(right_required)),
            (left, Some(left_required.clone())),
        ]
    } else {
        vec![
            (left, Some(left_required.clone())),
            (right, Some(right_required)),
        ]
    };
    (
        Candidate {
            local_cost: cost_model.cost_merge_join(left_card, right_card),
            children,
            plan: PhysChoice::Merge {
                left,
                right,
                edges: crossing.to_vec(),
            },
        },
        left_required,
    )
}

/// Merge candidates for one split that DELIVER `key` — both
/// orientations tried, kept only when the key edge's left-side order
/// equals the requirement.
fn merge_candidates_delivering(
    memo: &mut Memo,
    left: RelSet,
    right: RelSet,
    key: &OrderKey,
    query: &NormalizedQuery,
    cost_model: &dyn CostModel,
    out: &mut Vec<Candidate>,
) {
    let crossing = crossing_edges(left, right, &query.edges);
    assert!(
        !crossing.is_empty(),
        "split invariant: at least one crossing edge"
    );
    for (left_set, right_set) in [(left, right), (right, left)] {
        let (candidate, delivered) =
            merge_candidate(memo, left_set, right_set, &crossing, query, cost_model);
        if delivered == *key {
            out.push(candidate);
        }
    }
}

/// Indices (into `edges`) of every edge with one endpoint in `a` and the
/// other in `b` — the memo sibling of `join_order::connecting_edges`.
fn crossing_edges(a: RelSet, b: RelSet, edges: &[Edge]) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, edge) in edges.iter().enumerate() {
        let l = 1u32 << edge.left_rel;
        let r = 1u32 << edge.right_rel;
        let connects = (a & l != 0 && b & r != 0) || (a & r != 0 && b & l != 0);
        if connects {
            out.push(i);
        }
    }
    out
}

/// First crossing edge whose `inner_rel`-side column carries a
/// single-column index, with that index's name.
fn probe_edge(
    inner_rel: RelId,
    crossing: &[usize],
    query: &NormalizedQuery,
) -> Option<(usize, String)> {
    for &e in crossing {
        let edge = &query.edges[e];
        let col = if edge.right_rel == inner_rel && edge.right_indexed {
            Some(edge.right_col)
        } else if edge.left_rel == inner_rel && edge.left_indexed {
            Some(edge.left_col)
        } else {
            None
        };
        if let Some(col) = col {
            let index = query.relations[inner_rel]
                .indexes
                .iter()
                .find(|ix| ix.def.columns == [col])
                .expect("indexed flag implies a matching single-column index");
            return Some((e, index.def.name.clone()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::sql::cost::DefaultCostModel;
    use crate::sql::memo::fixtures::{bind, default_stats, edge, query, rel, setup};
    use crate::sql::memo::normalize::normalize;
    use crate::sql::memo::props::edge_order;
    use crate::sql::stats::{MockStatsProvider, QueryStats};
    use crate::types::Value;

    // ---- Brute-force reference (independent of the search machinery) ----

    /// Adjacency expansion, re-implemented so the reference does not lean
    /// on the code under test.
    fn connected(set: RelSet, q: &NormalizedQuery) -> bool {
        let mut reached = 1u32 << set.trailing_zeros();
        for _ in 0..32 {
            let before = reached;
            for e in &q.edges {
                let l = 1u32 << e.left_rel;
                let r = 1u32 << e.right_rel;
                if set & l != 0 && set & r != 0 && (reached & (l | r)) != 0 {
                    reached |= l | r;
                }
            }
            if reached == before {
                return reached == set;
            }
        }
        unreachable!("bounded by relation count");
    }

    /// D7 cardinality, re-implemented for the reference.
    fn card_of(set: RelSet, q: &NormalizedQuery) -> f64 {
        let mut card = 1.0;
        for (i, r) in q.relations.iter().enumerate() {
            if set & (1u32 << i) != 0 {
                card *= r.row_count * r.local_selectivity();
            }
        }
        for e in &q.edges {
            if set & (1u32 << e.left_rel) != 0 && set & (1u32 << e.right_rel) != 0 {
                card *= e.selectivity;
            }
        }
        card.max(1.0)
    }

    /// Minimum scalar cost over EVERY bushy plan — exhaustive recursion,
    /// no memoization, no pruning. Handles SeqScan leaves and Hash/NLJ
    /// joins only, so it guards fixtures without indexes.
    fn brute_force_scalar(set: RelSet, q: &NormalizedQuery, cm: &dyn CostModel) -> f64 {
        if set.count_ones() == 1 {
            let info = &q.relations[set.trailing_zeros() as usize];
            assert!(
                info.indexes.is_empty(),
                "brute force models SeqScan leaves only"
            );
            let pages = (info.row_count / ROWS_PER_PAGE_ESTIMATE).ceil().max(1.0);
            let mut cost = cm.cost_seq_scan(pages, info.row_count);
            if info.local_selectivity() < 1.0 {
                cost = cost.add(cm.cost_filter(info.row_count));
            }
            return cm.scalar(cost);
        }
        let low = 1u32 << set.trailing_zeros();
        let mut best = f64::INFINITY;
        let mut sub = (set - 1) & set;
        while sub != 0 {
            if sub & low != 0 {
                let rest = set ^ sub;
                let has_cross = !crossing_edges(sub, rest, &q.edges).is_empty();
                if has_cross && connected(sub, q) && connected(rest, q) {
                    let cost_sub = brute_force_scalar(sub, q, cm);
                    let cost_rest = brute_force_scalar(rest, q, cm);
                    let (card_sub, card_rest) = (card_of(sub, q), card_of(rest, q));
                    for (outer_card, inner_card) in [(card_sub, card_rest), (card_rest, card_sub)] {
                        let hash = cm.scalar(cm.cost_hash_join(inner_card, outer_card));
                        let nlj = cm.scalar(cm.cost_nested_loop_join(outer_card, inner_card));
                        best = best.min(cost_sub + cost_rest + hash.min(nlj));
                    }
                }
            }
            sub = (sub - 1) & set;
        }
        best
    }

    /// Cost model that counts every operator-cost estimate — the probe
    /// for "did the search do less cost work than exhaustion".
    struct CountingCostModel {
        inner: DefaultCostModel,
        calls: AtomicUsize,
    }

    impl CountingCostModel {
        fn new() -> Self {
            CountingCostModel {
                inner: DefaultCostModel::new(),
                calls: AtomicUsize::new(0),
            }
        }
        fn count(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
        fn bump(&self) {
            self.calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl CostModel for CountingCostModel {
        fn cost_seq_scan(&self, p: f64, r: f64) -> Cost {
            self.bump();
            self.inner.cost_seq_scan(p, r)
        }
        fn cost_index_scan(&self, m: f64) -> Cost {
            self.bump();
            self.inner.cost_index_scan(m)
        }
        fn cost_filter(&self, r: f64) -> Cost {
            self.bump();
            self.inner.cost_filter(r)
        }
        fn cost_projection(&self, r: f64) -> Cost {
            self.bump();
            self.inner.cost_projection(r)
        }
        fn cost_limit(&self, r: f64) -> Cost {
            self.bump();
            self.inner.cost_limit(r)
        }
        fn cost_sort(&self, r: f64) -> Cost {
            self.bump();
            self.inner.cost_sort(r)
        }
        fn cost_nested_loop_join(&self, o: f64, i: f64) -> Cost {
            self.bump();
            self.inner.cost_nested_loop_join(o, i)
        }
        fn cost_index_nested_loop_join(&self, o: f64, m: f64) -> Cost {
            self.bump();
            self.inner.cost_index_nested_loop_join(o, m)
        }
        fn cost_hash_join(&self, b: f64, p: f64) -> Cost {
            self.bump();
            self.inner.cost_hash_join(b, p)
        }
        fn cost_merge_join(&self, l: f64, r: f64) -> Cost {
            self.bump();
            self.inner.cost_merge_join(l, r)
        }
        fn scalar(&self, c: Cost) -> f64 {
            self.inner.scalar(c) // comparisons are free — only estimates count
        }
    }

    /// Chain 0—1—2 tuned so the ({0,1} first) order wins by a wide
    /// margin: e01 is very selective, e12 barely narrows.
    fn pruning_chain() -> NormalizedQuery {
        query(
            vec![rel(1000.0, 1.0), rel(1000.0, 1.0), rel(1000.0, 1.0)],
            vec![edge(0, 1, 0.001), edge(1, 2, 0.5)],
        )
    }

    // ---- §9 A.3: winner equals brute force ----

    #[test]
    fn winner_cost_matches_brute_force_on_chain() {
        let q = pruning_chain();
        let cm = DefaultCostModel::new();
        let mut memo = Memo::new(&q);
        let winner = optimize_root(&mut memo, &q, &cm);
        let brute = brute_force_scalar(0b111, &q, &cm);
        assert!(
            (cm.scalar(winner.cost) - brute).abs() < 1e-6,
            "search found {} but brute force found {}",
            cm.scalar(winner.cost),
            brute
        );
    }

    #[test]
    fn winner_cost_matches_brute_force_on_star() {
        // Hub 0 with three spokes of very different sizes and a local
        // filter on the hub — exercises bushy-vs-left-deep choice.
        let q = query(
            vec![
                rel(1000.0, 0.1),
                rel(10_000.0, 1.0),
                rel(10.0, 1.0),
                rel(500.0, 1.0),
            ],
            vec![edge(0, 1, 1e-4), edge(0, 2, 0.1), edge(0, 3, 1e-3)],
        );
        let cm = DefaultCostModel::new();
        let mut memo = Memo::new(&q);
        let winner = optimize_root(&mut memo, &q, &cm);
        let brute = brute_force_scalar(0b1111, &q, &cm);
        assert!(
            (cm.scalar(winner.cost) - brute).abs() < 1e-6,
            "search found {} but brute force found {}",
            cm.scalar(winner.cost),
            brute
        );
    }

    // ---- §9 A.3: pruning actually skips ----

    #[test]
    fn pruning_skips_unpromising_groups() {
        let q = pruning_chain();

        // The expensive sub-join {1,2} (500k rows) is created for
        // costing but must never be optimized (under any requirement):
        // every candidate that needs it — Hash, NLJ, and Merge alike —
        // is pruned at its local-cost check against the cheap
        // ({0,1} first) bound, before recursing into children.
        // NOTE: the original A.3 version also compared cost-estimate
        // counts against the brute-force reference; that comparison
        // died with B.2 — the search's space (merge + order
        // requirements) is strictly larger than the reference's, so the
        // counts no longer measure the same thing. The counting model
        // still guards promise ordering below.
        let mut memo = Memo::new(&q);
        optimize_root(&mut memo, &q, &DefaultCostModel::new());
        let g12 = memo.group_for(0b110, &q);
        assert!(
            memo.group(g12).winners.is_empty(),
            "pruned group {{1,2}} must never be searched"
        );
    }

    // ---- §4: the optimal flag and budget refusal ----

    #[test]
    fn optimal_flag_and_budget_refusal() {
        let q = pruning_chain();
        let cm = DefaultCostModel::new();
        let mut memo = Memo::new(&q);
        let winner = optimize_root(&mut memo, &q, &cm);
        assert!(winner.optimal);

        // A cached optimal winner refuses a budget below its cost…
        let root = memo.group_for(0b111, &q);
        assert!(optimize(&mut memo, root, 0.0, None, &q, &cm).is_none());
        // …and a fresh group searched under an impossible budget records
        // nothing (never clobbers with a worse result either).
        let mut fresh = Memo::new(&q);
        assert!(optimize(&mut fresh, 0, 0.0, None, &q, &cm).is_none());
        assert!(fresh.group(0).winners.is_empty());
    }

    // ---- §7.4 (T17-B.2): the ordered search is genuinely cost-based ----

    /// Delegates everywhere except hash joins, which it makes ruinous —
    /// flipping the Sort+Merge vs Hash choice without touching the
    /// search machinery.
    struct HashHostileModel(DefaultCostModel);

    impl CostModel for HashHostileModel {
        fn cost_seq_scan(&self, p: f64, r: f64) -> Cost {
            self.0.cost_seq_scan(p, r)
        }
        fn cost_index_scan(&self, m: f64) -> Cost {
            self.0.cost_index_scan(m)
        }
        fn cost_filter(&self, r: f64) -> Cost {
            self.0.cost_filter(r)
        }
        fn cost_projection(&self, r: f64) -> Cost {
            self.0.cost_projection(r)
        }
        fn cost_limit(&self, r: f64) -> Cost {
            self.0.cost_limit(r)
        }
        fn cost_sort(&self, r: f64) -> Cost {
            self.0.cost_sort(r)
        }
        fn cost_nested_loop_join(&self, o: f64, i: f64) -> Cost {
            self.0.cost_nested_loop_join(o, i)
        }
        fn cost_index_nested_loop_join(&self, o: f64, m: f64) -> Cost {
            self.0.cost_index_nested_loop_join(o, m)
        }
        fn cost_hash_join(&self, _b: f64, _p: f64) -> Cost {
            Cost {
                io_units: 1e9,
                cpu_units: 0.0,
            }
        }
        fn cost_merge_join(&self, l: f64, r: f64) -> Cost {
            self.0.cost_merge_join(l, r)
        }
        fn scalar(&self, c: Cost) -> f64 {
            self.0.scalar(c)
        }
    }

    #[test]
    fn ordered_search_flips_between_merge_and_enforced_hash() {
        // Two 100-row relations joined on their (0,0)/(1,0) columns; the
        // root is searched WITH a required order on relation 0's key.
        let q = query(
            vec![rel(100.0, 1.0), rel(100.0, 1.0)],
            vec![edge(0, 1, 0.01)],
        );
        let required = edge_order(0, 0);

        // Hash-hostile model: Merge delivers the order natively and wins.
        let cm = HashHostileModel(DefaultCostModel::new());
        let mut memo = Memo::new(&q);
        let gid = memo.group_for(0b11, &q);
        let winner = optimize(&mut memo, gid, f64::INFINITY, Some(&required), &q, &cm)
            .expect("ordered search always has the enforcer fallback");
        assert!(
            matches!(winner.plan, PhysChoice::Merge { .. }),
            "expected Merge under a hash-hostile model, got {:?}",
            winner.plan
        );

        // Default model: hashing is cheap, so the enforcer sorts the
        // unordered Hash winner instead.
        let cm = DefaultCostModel::new();
        let mut memo = Memo::new(&q);
        let gid = memo.group_for(0b11, &q);
        let winner = optimize(&mut memo, gid, f64::INFINITY, Some(&required), &q, &cm)
            .expect("ordered search always has the enforcer fallback");
        assert!(
            matches!(winner.plan, PhysChoice::SortEnforcer { .. }),
            "expected SortEnforcer under the default model, got {:?}",
            winner.plan
        );
        assert!(
            matches!(
                memo.group(gid).winners.get(&None).unwrap().plan,
                PhysChoice::Hash { .. }
            ),
            "the enforcer must wrap the unordered Hash winner"
        );
    }

    // ---- §5 leaf candidates through the shared lowering (D6) ----

    fn normalized(
        env: &crate::sql::memo::fixtures::Env,
        sql: &str,
        stats: &QueryStats,
    ) -> NormalizedQuery {
        let logical = bind(env, sql);
        normalize(&logical, &env.catalog, stats).unwrap().unwrap()
    }

    #[test]
    fn leaf_pk_lookup_wins() {
        let env = setup();
        let q = normalized(&env, "SELECT w_id FROM wh WHERE w_id = 1", &default_stats());
        let cm = DefaultCostModel::new();
        let mut memo = Memo::new(&q);
        let winner = optimize_root(&mut memo, &q, &cm);
        match winner.plan {
            PhysChoice::PkLookup { rel, pk, residual } => {
                assert_eq!(rel, 0);
                assert_eq!(pk, vec![Value::Int32(1)]); // coerce_exact to the PK type
                assert!(residual.is_none());
            }
            other => panic!("expected PkLookup, got {other:?}"),
        }
    }

    #[test]
    fn leaf_index_scan_choice_follows_cost() {
        let env = setup();

        // High NDV (few matches per key) → the index probe beats the scan.
        let provider = MockStatsProvider::new()
            .with_table_rows(env.dist_id, 1000)
            .with_column_ndv(env.dist_id, 1, 1000);
        let stats = QueryStats::gather(&provider, &[(env.dist_id, &[0, 1, 2])]).unwrap();
        let q = normalized(&env, "SELECT d_id FROM dist WHERE d_w_id = 7", &stats);
        let cm = DefaultCostModel::new();
        let mut memo = Memo::new(&q);
        let winner = optimize_root(&mut memo, &q, &cm);
        match &winner.plan {
            PhysChoice::IndexScan { index, filter, .. } => {
                assert_eq!(index, "dist_by_w");
                // The recheck predicate (E1/O12) must ride the winner.
                assert!(matches!(
                    filter,
                    crate::sql::expr::Predicate::Compare { .. }
                ));
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }

        // Unanalyzed (fallback selectivity 0.1 → 100 matches at the
        // default row count): per-row probes cost more than one scan.
        let q = normalized(
            &env,
            "SELECT d_id FROM dist WHERE d_w_id = 7",
            &default_stats(),
        );
        let mut memo = Memo::new(&q);
        let winner = optimize_root(&mut memo, &q, &cm);
        assert!(
            matches!(winner.plan, PhysChoice::SeqScan { rel: 0 }),
            "expected SeqScan at fallback selectivity, got {:?}",
            winner.plan
        );
    }

    #[test]
    fn inlj_chosen_for_tiny_outer_indexed_inner() {
        // wh(5 rows) probing dist(1M rows, d_w_id indexed, NDV 1M): ~1
        // match per probe over 5 rows beats hashing or scanning 1M rows
        // — and INLJ's plan must skip the inner leaf entirely.
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
        let q = normalized(
            &env,
            "SELECT w_id FROM wh JOIN dist ON d_w_id = w_id",
            &stats,
        );
        let cm = DefaultCostModel::new();
        let mut memo = Memo::new(&q);
        let winner = optimize_root(&mut memo, &q, &cm);
        match &winner.plan {
            PhysChoice::IndexNested {
                outer,
                inner_rel,
                index,
                probe,
                edges,
            } => {
                assert_eq!(memo.group(*outer).relset, 0b001, "wh drives the probes");
                assert_eq!(*inner_rel, 1);
                assert_eq!(index, "dist_by_w");
                assert_eq!(*probe, 0);
                assert_eq!(edges.as_slice(), &[0]);
            }
            other => panic!("expected IndexNested, got {other:?}"),
        }
    }

    #[test]
    fn promise_ordering_reduces_search_work() {
        // 4-relation clique with skewed sizes: many orientation
        // candidates whose expensive child is searched under a budget
        // already reduced by the cheap child's winner. Measured
        // cost-estimate counts on this fixture: 1350 with unordered
        // children, 843 with ascending-cardinality promise ordering
        // (re-measured after B.2 widened the space with merge + order
        // requirements; pre-B.2 the pair was 312 vs 236). The bound
        // guards the property, not the exact figure.
        let q = query(
            vec![
                rel(1000.0, 1.0),
                rel(2000.0, 1.0),
                rel(50.0, 1.0),
                rel(80_000.0, 1.0),
            ],
            vec![
                edge(0, 1, 0.001),
                edge(0, 2, 0.02),
                edge(1, 3, 1e-4),
                edge(2, 3, 0.01),
                edge(0, 3, 1e-3),
                edge(1, 2, 0.05),
            ],
        );
        let cm = CountingCostModel::new();
        let mut memo = Memo::new(&q);
        optimize_root(&mut memo, &q, &cm);
        assert!(
            cm.count() < 1350,
            "promise ordering regressed: {} estimates (unordered baseline 1350)",
            cm.count()
        );
    }
}
