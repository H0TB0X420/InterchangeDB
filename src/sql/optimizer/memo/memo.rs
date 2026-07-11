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
/// guard against exponential blowup (§4). Sized to the Selinger DP's
/// exhaustive regime (`2^MAX_DP_RELATIONS`); realistic OLTP/TPC-C shapes
/// materialize a few dozen groups at most. Exceeding it makes
/// `Memo::try_new` bail so the planner falls back (D8) — never a panic
/// on valid SQL (review fix #1).
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
    /// Cheapest unordered physical plan (T17-A.3) — the requirement
    /// every search hits by default, kept out of the hash map so the
    /// hot path neither hashes nor allocates a key (review fix #4).
    pub(crate) unordered_winner: Option<Winner>,
    /// Cheapest plan per delivered order (T17-B.2). Bounded by the
    /// distinct edge-column orders.
    pub(crate) ordered_winners: HashMap<OrderKey, Winner>,
}

impl Group {
    /// The recorded winner for a requirement, if any.
    pub(crate) fn winner(&self, required: Option<&OrderKey>) -> Option<&Winner> {
        match required {
            None => self.unordered_winner.as_ref(),
            Some(key) => self.ordered_winners.get(key),
        }
    }

    /// Record `winner` for a requirement — only if it improves on what
    /// is already recorded (a bounded re-search must never clobber a
    /// better cached winner). `scalar` supplies the ranking.
    pub(crate) fn record_winner(
        &mut self,
        required: Option<&OrderKey>,
        winner: Winner,
        scalar: impl Fn(Cost) -> f64,
    ) {
        let slot = match required {
            None => &mut self.unordered_winner,
            Some(key) => match self.ordered_winners.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let slot = entry.into_mut();
                    if scalar(winner.cost) < scalar(slot.cost) {
                        *slot = winner;
                    }
                    return;
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(winner);
                    return;
                }
            },
        };
        let improves = slot
            .as_ref()
            .is_none_or(|current| scalar(winner.cost) < scalar(current.cost));
        if improves {
            *slot = Some(winner);
        }
    }

    /// Whether any requirement has been searched to a winner — the
    /// pruning tests' "this group was never optimized" probe.
    #[cfg(test)]
    pub(crate) fn has_winners(&self) -> bool {
        self.unordered_winner.is_some() || !self.ordered_winners.is_empty()
    }
}

/// A group's best physical plan.
///
/// NOTE (deviation from plan §4): the plan mandated an `optimal` flag
/// guarding budget-bounded winners against reuse under larger budgets.
/// With exact additive costs (no admissible lower-bound heuristic),
/// branch-and-bound only ever prunes candidates that provably cannot
/// win, so ANY recorded winner is already the group's true optimum —
/// the flag was dead machinery and was removed in the review fixes.
/// Re-add it together with sibling lower bounds if that recorded
/// future lever ever lands.
#[derive(Clone)]
pub(crate) struct Winner {
    pub(crate) cost: Cost,
    pub(crate) plan: PhysChoice,
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
    /// Build the memo with EVERY connected relation subset materialized
    /// up front: leaf groups first (so leaf `GroupId`s equal `RelId`s),
    /// then a worklist from the full set through each group's splits.
    /// Returns `None` when the join graph is dense enough to exceed
    /// `GROUP_COUNT_MAX` — the caller falls back to the textual planner
    /// (D8), the same treatment as any other unsupported shape.
    ///
    /// NOTE (deviation from plan §3): the plan called for lazy group
    /// creation during search. Lazy creation discovers pathological
    /// density only mid-search, where the only remaining option was a
    /// panic on valid SQL. Eager materialization is bounded by the same
    /// cap, turns the overflow into a graceful bail, and leaves the
    /// search free of group mutation (groups become lookup-only).
    pub(crate) fn try_new(query: &NormalizedQuery) -> Option<Memo> {
        let relation_count = query.relations.len();
        assert!(
            (1..=MAX_RELATIONS).contains(&relation_count),
            "normalization enforces the bitmask cap"
        );

        let mut memo = Memo {
            groups: Vec::with_capacity(2 * relation_count),
            by_relset: HashMap::new(),
        };
        for rel in 0..relation_count {
            let group_id = memo.insert_group(1u32 << rel, query);
            debug_assert_eq!(group_id, rel, "leaf GroupId equals RelId");
        }

        // Worklist over split closures. Every push is a side of some
        // materialized group's split, so total pushes are bounded by
        // Σ splits over ≤ GROUP_COUNT_MAX groups — no explicit counter
        // needed beyond the cap check itself.
        let mut pending = vec![full_relset(relation_count)];
        while let Some(relset) = pending.pop() {
            if memo.by_relset.contains_key(&relset) {
                continue;
            }
            if memo.groups.len() == GROUP_COUNT_MAX {
                return None; // dense graph — caller takes the D8 fallback
            }
            let group_id = memo.insert_group(relset, query);
            for &(left, right) in &memo.groups[group_id].splits {
                pending.push(left);
                pending.push(right);
            }
        }
        Some(memo)
    }

    pub(crate) fn group(&self, group_id: GroupId) -> &Group {
        &self.groups[group_id]
    }

    /// Mutable access for winner recording (T17-A.3).
    pub(crate) fn group_mut(&mut self, group_id: GroupId) -> &mut Group {
        &mut self.groups[group_id]
    }

    /// `GroupId` of a relation set — total for every connected subset
    /// once `try_new` succeeds; a miss is a search/emission bug (crash
    /// on corruption).
    pub(crate) fn group_id(&self, relset: RelSet) -> GroupId {
        *self
            .by_relset
            .get(&relset)
            .expect("connected subsets are total after Memo::try_new")
    }

    /// Test-only diagnostic — production callers track groups via ids.
    #[cfg(test)]
    pub(crate) fn group_count(&self) -> usize {
        self.groups.len()
    }

    fn insert_group(&mut self, relset: RelSet, query: &NormalizedQuery) -> GroupId {
        assert!(relset != 0, "a group spans at least one relation");
        debug_assert!(
            self.groups.len() < GROUP_COUNT_MAX,
            "try_new enforces the group cap before inserting"
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

        let group_id = self.groups.len();
        self.groups.push(Group {
            relset,
            cardinality: cardinality(relset, query),
            splits,
            unordered_winner: None,
            ordered_winners: HashMap::new(),
        });
        self.by_relset.insert(relset, group_id);
        group_id
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
        let memo = Memo::try_new(&q).expect("under the group cap");
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
        let memo = Memo::try_new(&q).expect("under the group cap");
        let group_id = memo.group_id(0b111);
        let splits = &memo.group(group_id).splits;
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
        let memo = Memo::try_new(&q).expect("under the group cap");
        let group_id = memo.group_id(0b111);
        assert_eq!(memo.group(group_id).splits.len(), 3);
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
        let memo = Memo::try_new(&q).expect("under the group cap");
        let group_id = memo.group_id(0b1111);
        let splits = &memo.group(group_id).splits;
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
        let memo = Memo::try_new(&q).expect("under the group cap");
        let group_id = memo.group_id(0b110);
        let splits = &memo.group(group_id).splits;
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
        let memo = Memo::try_new(&q).expect("under the group cap");
        // {0,1}: (100×0.5) × 10 × edge01(0.01) = 5.
        let g01 = memo.group_id(0b011);
        assert!((memo.group(g01).cardinality - 5.0).abs() < 1e-9);
        // {0,1,2}: 5 × 1000 × edge12(0.001) = 5. Both edges internal.
        let g012 = memo.group_id(0b111);
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
        let memo = Memo::try_new(&q).expect("under the group cap");
        let group_id = memo.group_id(0b11);
        assert_eq!(memo.group(group_id).cardinality, MIN_CARD);
    }

    #[test]
    fn try_new_materializes_exactly_the_connected_subsets() {
        // chain3's connected subsets: 3 leaves + {0,1} + {1,2} + {0,1,2}.
        // Negative space: the disconnected {0,2} never materializes.
        let q = chain3();
        let memo = Memo::try_new(&q).expect("under the group cap");
        assert_eq!(memo.group_count(), 6);
        for relset in [0b001u32, 0b010, 0b100, 0b011, 0b110, 0b111] {
            assert_eq!(memo.group(memo.group_id(relset)).relset, relset);
        }
    }

    /// Hub-and-spokes star with `spokes` spokes: the 2^spokes subsets
    /// containing the hub plus the spoke singletons are connected.
    fn star(spokes: usize) -> NormalizedQuery {
        let edges = (1..=spokes).map(|s| edge(0, s, 0.1)).collect();
        let relations = (0..=spokes).map(|_| rel(10.0, 1.0)).collect();
        query(relations, edges)
    }

    #[test]
    fn group_cap_bails_gracefully_on_dense_graphs() {
        // Boundary pair around GROUP_COUNT_MAX = 4096 (review fix #1 —
        // a bail the planner turns into the D8 fallback, never a panic
        // on valid SQL): an 11-spoke star materializes 2^11 + 11 = 2059
        // groups and succeeds; a 12-spoke star needs 2^12 + 12 = 4108
        // and must return None.
        let under = star(11);
        let memo = Memo::try_new(&under).expect("2059 groups fit the cap");
        assert_eq!(memo.group_count(), 2059);

        let over = star(12);
        assert!(Memo::try_new(&over).is_none());
    }
}
