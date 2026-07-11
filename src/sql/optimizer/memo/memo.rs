//! T17-A.2 — the memo: groups keyed by relation-set bitmask.
//!
//! One [`Group`] per connected subset of the query's relations (D2).
//! Because the optimizable core is exactly the equi-join graph, keying
//! groups by `RelSet` IS the closure of join commutativity/associativity
//! for this fragment: two logically equivalent join trees over the same
//! relations always land in the same group, so deduplication is a
//! `HashMap<RelSet, GroupId>` lookup — no expression-tree hashing (the
//! classic memo failure mode).
//!
//! A group's logical alternatives are its **splits** `(left, right)`:
//! partitions of the relation set where both sides are connected and at
//! least one equi-edge crosses them. Splits are stored unordered-
//! canonical (D5): `left` is the side containing the smallest relation
//! index; physical candidates (T17-A.3) generate both orientations.
//!
//! Group **cardinality is a group property** (D7), computed once at
//! creation from the normalized snapshot:
//! `Π row_count(rel) × Π local_sel(rel) × Π edge_sel(internal edges)`,
//! floored at `MIN_CARD` — identical inputs and floor as the Selinger
//! DP, so cost comparisons across planners are apples-to-apples.

use std::collections::HashMap;

use crate::sql::ir::expr::Predicate;
use crate::sql::optimizer::cost::Cost;
use crate::sql::optimizer::join_order::{RelId, RelSet, MAX_RELATIONS, MIN_CARD};
use crate::sql::optimizer::memo::normalize::{Edge, NormalizedQuery};
use crate::sql::optimizer::memo::props::OrderKey;
use crate::types::Value;

/// Index of a group within `Memo::groups`.
pub(crate) type GroupId = usize;

/// Bitmask spanning every relation of a query core.
pub(crate) fn full_relset(relation_count: usize) -> RelSet {
    assert!((1..=MAX_RELATIONS).contains(&relation_count));
    (1u32 << relation_count) - 1
}

/// Hard cap on materialized groups — the "put a limit on everything"
/// guard against accidental exponential blowup (§4). Sized to the
/// Selinger DP's exhaustive regime (`2^MAX_DP_RELATIONS`); realistic
/// OLTP/TPC-C shapes materialize a few dozen groups at most.
pub(crate) const GROUP_COUNT_MAX: usize = 4096;

/// One memo group: every logical join tree spanning exactly `relset`.
pub(crate) struct Group {
    pub(crate) relset: RelSet,
    /// Estimated output row count — a group property (D7), identical for
    /// every split, so the search can never "shop" for optimistic
    /// estimates between alternatives.
    pub(crate) cardinality: f64,
    /// Canonical logical splits (D5). Empty for leaf (single-relation)
    /// groups.
    pub(crate) splits: Vec<(RelSet, RelSet)>,
    /// Cheapest physical plan per required order (T17-B.2): `None` is
    /// the unordered requirement, `Some(key)` a plan that delivers
    /// `key`. Bounded by the distinct edge-column orders + 1.
    pub(crate) winners: HashMap<Option<OrderKey>, Winner>,
}

/// A group's best physical plan, with the §4 budget-honesty flag.
#[derive(Clone)]
pub(crate) struct Winner {
    pub(crate) cost: Cost,
    pub(crate) plan: PhysChoice,
    /// True when computed under an infinite budget — safe to reuse for
    /// any caller. False means "best found under some finite budget": a
    /// caller with a larger budget must re-search rather than trust it
    /// (the classic branch-and-bound memoization bug, §4).
    pub(crate) optimal: bool,
}

/// Physical choice tree — small and memo-internal; `emit.rs` (T17-A.4)
/// converts winners into `PhysOp`. Join children reference *groups*: the
/// full winner tree is implicit in the child groups' recorded winners.
///
/// NOTE (deviation from the plan §3 sketch): join variants carry `edges`
/// — every crossing edge index of their split — rather than a lone key
/// edge, because emission needs the non-key edges too (ON conjunction
/// for NestedLoop, residual join filter for Hash/IndexNested) and they
/// are cheaper stored once than re-derived from the group relsets.
#[derive(Clone, Debug)]
pub(crate) enum PhysChoice {
    /// Full scan; the relation's local predicates become the leaf Filter.
    SeqScan { rel: RelId },
    /// Secondary-index equality scan (D6 shared lowering). `filter` is
    /// the E1/O12 recheck predicate ANDed with the non-lowered local
    /// conjuncts — emission MUST place it as a Filter above the scan.
    IndexScan {
        rel: RelId,
        index: String,
        prefix: Vec<Value>,
        filter: Predicate,
    },
    /// Single-row PK lookup; `residual` is the non-PK local conjuncts
    /// (`build_left_leaf`'s access/leftover split).
    PkLookup {
        rel: RelId,
        pk: Vec<Value>,
        residual: Option<Predicate>,
    },
    /// Orientation is explicit — both are generated per split (D5).
    /// `edges` (indices into `NormalizedQuery::edges`) all become the ON
    /// conjunction.
    NestedLoop {
        outer: GroupId,
        inner: GroupId,
        edges: Vec<usize>,
    },
    /// `edges[0]` is the hash key; the rest become a residual join
    /// filter at emission (same single-key limitation as today's
    /// HashJoin lowering).
    Hash {
        outer: GroupId,
        inner: GroupId,
        edges: Vec<usize>,
    },
    /// Inner is always a single relation probed via `index` on edge
    /// `probe` (an index into `NormalizedQuery::edges`); the other
    /// crossing edges and the inner's local predicates become a Filter
    /// above the join — probes cannot carry them.
    IndexNested {
        outer: GroupId,
        inner_rel: RelId,
        index: String,
        probe: usize,
        edges: Vec<usize>,
    },
    /// Merge join (T17-B.2). `edges[0]` is the merge key; the rest
    /// become a residual join filter (same shape as `Hash`). Children
    /// were searched under `Order([their side of edges[0]])` — emission
    /// resolves their winners under that same requirement.
    Merge {
        left: GroupId,
        right: GroupId,
        edges: Vec<usize>,
    },
    /// Order enforcer (T17-B.2): sorts this group's *unordered* winner
    /// into `key`. `child` is always the enforcer's own group — the
    /// child plan is `winners[None]`.
    SortEnforcer { child: GroupId, key: OrderKey },
}

/// The memo: groups indexed by `GroupId`, deduplicated by relation set.
pub(crate) struct Memo {
    groups: Vec<Group>,
    by_relset: HashMap<RelSet, GroupId>,
}

impl Memo {
    /// Build a memo seeded with one leaf group per relation, so leaf
    /// `GroupId`s equal `RelId`s. Join groups are created lazily by
    /// `group_for` as the search reaches them.
    pub(crate) fn new(query: &NormalizedQuery) -> Memo {
        let relation_count = query.relations.len();
        assert!(
            relation_count >= 1,
            "a query core spans at least one relation"
        );
        assert!(
            relation_count <= MAX_RELATIONS,
            "normalization enforces the bitmask cap"
        );

        let mut memo = Memo {
            groups: Vec::with_capacity(2 * relation_count),
            by_relset: HashMap::new(),
        };
        for rel in 0..relation_count {
            let gid = memo.insert_group(1u32 << rel, query);
            debug_assert_eq!(gid, rel, "leaf GroupId equals RelId");
        }
        memo
    }

    /// `GroupId` for `relset`, creating the group (and enumerating its
    /// splits) on first request.
    pub(crate) fn group_for(&mut self, relset: RelSet, query: &NormalizedQuery) -> GroupId {
        if let Some(&gid) = self.by_relset.get(&relset) {
            return gid;
        }
        self.insert_group(relset, query)
    }

    pub(crate) fn group(&self, gid: GroupId) -> &Group {
        &self.groups[gid]
    }

    /// Mutable access for winner recording (T17-A.3).
    pub(crate) fn group_mut(&mut self, gid: GroupId) -> &mut Group {
        &mut self.groups[gid]
    }

    /// Read-only lookup — emission (T17-A.4) walks recorded winners
    /// without creating groups.
    pub(crate) fn group_id_for(&self, relset: RelSet) -> Option<GroupId> {
        self.by_relset.get(&relset).copied()
    }

    /// Test-only diagnostic — production callers track groups via ids.
    #[cfg(test)]
    pub(crate) fn group_count(&self) -> usize {
        self.groups.len()
    }

    fn insert_group(&mut self, relset: RelSet, query: &NormalizedQuery) -> GroupId {
        assert!(relset != 0, "a group spans at least one relation");
        assert!(
            self.groups.len() < GROUP_COUNT_MAX,
            "memo group count exceeded {GROUP_COUNT_MAX} — pathological query shape"
        );

        let splits = if relset.count_ones() == 1 {
            Vec::new()
        } else {
            enumerate_splits(relset, &query.edges)
        };
        // Negative space: a connected multi-relation set always has at
        // least one split (peel a spanning-tree leaf); an empty list here
        // means the caller requested a disconnected set — a search bug,
        // since splits only ever produce connected sides.
        debug_assert!(
            relset.count_ones() == 1 || !splits.is_empty(),
            "requested group {relset:#b} is disconnected"
        );

        let gid = self.groups.len();
        self.groups.push(Group {
            relset,
            cardinality: cardinality(relset, query),
            splits,
            winners: HashMap::new(),
        });
        self.by_relset.insert(relset, gid);
        gid
    }
}

/// D7 group cardinality over the normalized snapshot: base rows × local
/// selectivities of member relations × selectivities of internal edges
/// (both endpoints inside `relset`), floored at `MIN_CARD`.
fn cardinality(relset: RelSet, query: &NormalizedQuery) -> f64 {
    let mut card = 1.0;
    for (rel, info) in query.relations.iter().enumerate() {
        if relset & (1u32 << rel) != 0 {
            card *= info.row_count * info.local_selectivity();
        }
    }
    for edge in &query.edges {
        let both_inside =
            relset & (1u32 << edge.left_rel) != 0 && relset & (1u32 << edge.right_rel) != 0;
        if both_inside {
            card *= edge.selectivity;
        }
    }
    card.max(MIN_CARD)
}

/// All canonical splits of `relset`: partitions `(left, right)` where
/// `left` holds the smallest relation index (D5 — each unordered
/// partition appears exactly once), both sides are connected, and at
/// least one edge crosses them. Cross-product splits are discarded —
/// normalization already bailed on disconnected queries (D8).
fn enumerate_splits(relset: RelSet, edges: &[Edge]) -> Vec<(RelSet, RelSet)> {
    debug_assert!(relset.count_ones() >= 2, "leaf groups have no splits");
    let low = 1u32 << relset.trailing_zeros();

    let mut splits = Vec::new();
    // Standard submask walk: `(sub - 1) & relset` visits every proper
    // non-empty submask exactly once, strictly decreasing (terminates).
    let mut sub = (relset - 1) & relset;
    while sub != 0 {
        if sub & low != 0 {
            let rest = relset ^ sub;
            if is_connected(sub, edges) && is_connected(rest, edges) && crosses(sub, rest, edges) {
                splits.push((sub, rest));
            }
        }
        sub = (sub - 1) & relset;
    }
    splits
}

/// Whether the relations in `set` form a connected subgraph of `edges`.
fn is_connected(set: RelSet, edges: &[Edge]) -> bool {
    debug_assert!(set != 0);
    if set.count_ones() == 1 {
        return true;
    }
    let mut reached = 1u32 << set.trailing_zeros();
    // Fixpoint expansion, explicitly bounded: each pass either reaches a
    // new relation or proves the remainder unreachable, so `set`'s bit
    // count (≤ MAX_RELATIONS) bounds the passes.
    for _ in 0..MAX_RELATIONS {
        let mut grew = false;
        for edge in edges {
            let l = 1u32 << edge.left_rel;
            let r = 1u32 << edge.right_rel;
            if set & l != 0 && set & r != 0 {
                // Edge internal to `set`: reaches across iff exactly one
                // endpoint is already reached.
                if (reached & l != 0) != (reached & r != 0) {
                    reached |= l | r;
                    grew = true;
                }
            }
        }
        if reached == set {
            return true;
        }
        if !grew {
            return false;
        }
    }
    unreachable!("expansion is bounded by the relation count");
}

/// Whether at least one equi-edge connects side `a` to side `b`.
fn crosses(a: RelSet, b: RelSet, edges: &[Edge]) -> bool {
    edges.iter().any(|edge| {
        let l = 1u32 << edge.left_rel;
        let r = 1u32 << edge.right_rel;
        (a & l != 0 && b & r != 0) || (a & r != 0 && b & l != 0)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::memo::fixtures::{edge, query, rel};

    /// Chain 0—1—2 with distinct rows/selectivities for cardinality math.
    fn chain3() -> NormalizedQuery {
        query(
            vec![rel(100.0, 0.5), rel(10.0, 1.0), rel(1000.0, 1.0)],
            vec![edge(0, 1, 0.01), edge(1, 2, 0.001)],
        )
    }

    #[test]
    fn leaf_groups_seeded_with_relid_group_ids() {
        let q = chain3();
        let memo = Memo::new(&q);
        assert_eq!(memo.group_count(), 3);
        for rel_id in 0..3 {
            let g = memo.group(rel_id);
            assert_eq!(g.relset, 1u32 << rel_id);
            assert!(g.splits.is_empty(), "leaf groups have no splits");
        }
        // Leaf cardinality = rows × local selectivity.
        assert_eq!(memo.group(0).cardinality, 50.0);
        assert_eq!(memo.group(1).cardinality, 10.0);
    }

    #[test]
    fn chain_full_relset_has_two_splits() {
        // {0,1,2} on a chain: {0}|{1,2} and {0,1}|{2} are valid;
        // {0,2}|{1} is discarded — {0,2} has no internal edge.
        let q = chain3();
        let mut memo = Memo::new(&q);
        let gid = memo.group_for(0b111, &q);
        let splits = &memo.group(gid).splits;
        assert_eq!(splits.len(), 2);
        assert!(splits.contains(&(0b001, 0b110)));
        assert!(splits.contains(&(0b011, 0b100)));
    }

    #[test]
    fn clique_full_relset_has_three_splits() {
        let q = query(
            vec![rel(10.0, 1.0), rel(10.0, 1.0), rel(10.0, 1.0)],
            vec![edge(0, 1, 0.1), edge(0, 2, 0.1), edge(1, 2, 0.1)],
        );
        let mut memo = Memo::new(&q);
        let gid = memo.group_for(0b111, &q);
        assert_eq!(memo.group(gid).splits.len(), 3);
    }

    #[test]
    fn star_filters_disconnected_sides() {
        // Hub 0 with spokes 1,2,3: any split separating two spokes from
        // the hub leaves a disconnected side, so only the three
        // spoke-vs-rest splits survive.
        let q = query(
            (0..4).map(|_| rel(10.0, 1.0)).collect(),
            vec![edge(0, 1, 0.1), edge(0, 2, 0.1), edge(0, 3, 0.1)],
        );
        let mut memo = Memo::new(&q);
        let gid = memo.group_for(0b1111, &q);
        let splits = &memo.group(gid).splits;
        assert_eq!(splits.len(), 3);
        assert!(splits.contains(&(0b0111, 0b1000)));
        assert!(splits.contains(&(0b1011, 0b0100)));
        assert!(splits.contains(&(0b1101, 0b0010)));
    }

    #[test]
    fn canonical_left_contains_min_bit_of_relset() {
        // Sub-relset {1,2} (no bit 0): the canonical left side must hold
        // the set's own minimum (relation 1), not global relation 0.
        let q = chain3();
        let mut memo = Memo::new(&q);
        let gid = memo.group_for(0b110, &q);
        let splits = &memo.group(gid).splits;
        assert_eq!(splits.as_slice(), &[(0b010, 0b100)]);
        // Positive + negative space: every split's left holds the min bit
        // and each unordered partition appears exactly once.
        for &(left, right) in splits {
            assert!(left & 0b010 != 0);
            assert!(!splits.contains(&(right, left)));
        }
    }

    #[test]
    fn cardinality_follows_d7_formula() {
        let q = chain3();
        let mut memo = Memo::new(&q);
        // {0,1}: (100×0.5) × 10 × edge01(0.01) = 5.
        let g01 = memo.group_for(0b011, &q);
        assert!((memo.group(g01).cardinality - 5.0).abs() < 1e-9);
        // {0,1,2}: 5 × 1000 × edge12(0.001) = 5. Both edges internal.
        let g012 = memo.group_for(0b111, &q);
        assert!((memo.group(g012).cardinality - 5.0).abs() < 1e-9);
        // {0,2} never materializes (disconnected) — its edge product
        // would skip both edges; no group to assert, by design.
    }

    #[test]
    fn cardinality_floors_at_min_card() {
        let q = query(
            vec![rel(10.0, 0.001), rel(10.0, 0.001)],
            vec![edge(0, 1, 0.0001)],
        );
        let mut memo = Memo::new(&q);
        let gid = memo.group_for(0b11, &q);
        assert_eq!(memo.group(gid).cardinality, MIN_CARD);
    }

    #[test]
    fn group_for_is_idempotent() {
        let q = chain3();
        let mut memo = Memo::new(&q);
        let a = memo.group_for(0b011, &q);
        let count_after_first = memo.group_count();
        let b = memo.group_for(0b011, &q);
        assert_eq!(a, b);
        assert_eq!(memo.group_count(), count_after_first);
    }

    #[test]
    #[should_panic(expected = "memo group count exceeded")]
    fn group_count_cap_panics_on_pathological_shape() {
        // 13-relation clique: every one of the 2^13 - 1 subsets is
        // connected, so materializing them all must trip GROUP_COUNT_MAX.
        let n = 13;
        let mut edges = Vec::new();
        for a in 0..n {
            for b in (a + 1)..n {
                edges.push(edge(a, b, 0.1));
            }
        }
        let q = query((0..n).map(|_| rel(10.0, 1.0)).collect(), edges);
        let mut memo = Memo::new(&q);
        for relset in 1u32..(1 << n) {
            memo.group_for(relset, &q);
        }
    }
}
