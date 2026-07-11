//! T17-B.2 — physical order properties.
//!
//! An [`OrderKey`] names a sort order over **stable column references**
//! `(relation, local_column)` — textual coordinates, independent of join
//! order, so the same key means the same thing in every group and
//! survives emission's remapping.
//!
//! 17-B's property algebra is deliberately small (D9):
//!   - Merge join *requires* `Order([edge col])` ascending from both
//!     children and *delivers* its left input's order.
//!   - A `SortEnforcer` delivers exactly its key.
//!   - Every other operator delivers nothing (index-order delivery is a
//!     recorded non-goal).

use crate::sql::ir::logical::OrderDir;
use crate::sql::optimizer::join_order::{RelId, RelSet};
use crate::sql::optimizer::memo::normalize::{Edge, NormalizedQuery};
use crate::sql::optimizer::selinger::decompose;

/// A stable column reference: `(relation index, relation-local column)`.
pub(crate) type TextualColRef = (RelId, usize);

/// A required or delivered sort order, major-to-minor.
pub(crate) type OrderKey = Vec<(TextualColRef, OrderDir)>;

/// The single-column ascending order a merge join requires from (and
/// delivers through) one side of an equi-edge.
pub(crate) fn edge_order(rel: RelId, col: usize) -> OrderKey {
    vec![((rel, col), OrderDir::Asc)]
}

/// The query's ORDER BY as an [`OrderKey`], when it is consumable
/// (§7.5): every key column must map to ONE relation, and the query
/// must not aggregate. `None` keeps the spine Sort unconditionally.
///
/// Aggregates exclude consumption (review fix #3): the spine Sort sits
/// ABOVE HashAggregate, whose whole-table output is a single row — the
/// Sort being "avoided" is near-free, while the gate would price it at
/// the join core's cardinality and phantom-inflate the ordered plan.
pub(crate) fn order_by_requirement(query: &NormalizedQuery) -> Option<OrderKey> {
    if query.spine.order_by.is_empty() {
        return None;
    }
    if !query.spine.aggregates.is_empty() {
        return None;
    }
    let textual_base = query.textual_base();
    let widths = query.widths();
    let mut key = Vec::with_capacity(query.spine.order_by.len());
    let mut single_rel: Option<RelId> = None;
    for &(global, dir) in &query.spine.order_by {
        let (rel, local) = decompose(global, &textual_base, &widths)?;
        if *single_rel.get_or_insert(rel) != rel {
            return None; // spans relations — not consumable (§7.5)
        }
        key.push(((rel, local), dir));
    }
    Some(key)
}

/// The `(left, right)` orders a merge on `edge` requires of its
/// children, oriented for the join whose left group spans `left_set`.
/// Shared by the search (candidate generation) and emission (resolving
/// each child's ordered winner) so the two can never disagree.
pub(crate) fn merge_requirements(edge: &Edge, left_set: RelSet) -> (OrderKey, OrderKey) {
    if left_set & (1u32 << edge.left_rel) != 0 {
        (
            edge_order(edge.left_rel, edge.left_col),
            edge_order(edge.right_rel, edge.right_col),
        )
    } else {
        (
            edge_order(edge.right_rel, edge.right_col),
            edge_order(edge.left_rel, edge.left_col),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ir::logical::AggregateSpec;
    use crate::sql::optimizer::memo::fixtures::{edge, query, rel};

    // HOW: two single-column relations — global column 0 is (rel 0, 0),
    // global 1 is (rel 1, 0) — so mapping and the span check are both
    // visible in one fixture.
    #[test]
    fn order_by_requirement_maps_single_relation_keys() {
        let mut q = query(vec![rel(10.0, 1.0), rel(10.0, 1.0)], vec![edge(0, 1, 0.1)]);
        q.spine.order_by = vec![(1, OrderDir::Desc)];
        assert_eq!(
            order_by_requirement(&q),
            Some(vec![((1, 0), OrderDir::Desc)])
        );

        // Keys spanning two relations are not consumable (§7.5).
        q.spine.order_by = vec![(0, OrderDir::Asc), (1, OrderDir::Asc)];
        assert_eq!(order_by_requirement(&q), None);
    }

    #[test]
    fn aggregate_queries_are_never_consumable() {
        // Review fix #3: with aggregates the spine Sort covers a single
        // HashAggregate output row, so there is nothing worth consuming
        // — and pricing the avoided Sort at join cardinality would bias
        // plan choice toward needlessly order-enforced joins.
        let mut q = query(vec![rel(10.0, 1.0), rel(10.0, 1.0)], vec![edge(0, 1, 0.1)]);
        q.spine.order_by = vec![(1, OrderDir::Asc)];
        q.spine.aggregates = vec![AggregateSpec::CountStar];
        assert_eq!(order_by_requirement(&q), None);
    }
}
