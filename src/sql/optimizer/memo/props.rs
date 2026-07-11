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
/// (§7.5): every key column must map to ONE relation. `None` for
/// unordered queries or keys spanning relations — those keep the spine
/// Sort unconditionally.
pub(crate) fn order_by_requirement(query: &NormalizedQuery) -> Option<OrderKey> {
    if query.spine.order_by.is_empty() {
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
