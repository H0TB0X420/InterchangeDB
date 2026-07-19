//! P14.12 — column-index remapping for join reordering.
//!
//! Column references in predicates / projection / order-by / aggregates
//! are tuple-global indices the binder assigns in **textual** join order
//! (table 0's columns, then table 1's, …). When a planner reorders joins
//! (Selinger, P14.13b; the memo planners, Phase 17), the physical tuple
//! layout changes, so those indices must be rewritten. This module is the
//! rewrite — and the *only* new machinery join reordering needs.
//!
//! ## Why a remap, not a table-relative IR
//!
//! The alternative was to make every column reference carry `(table,
//! local)` through the binder and `LogicalPlan`. That touches the
//! 1500-line binder, the `Expression`/`Predicate` IR, and the golden
//! tests that pin index values — large, risky churn for a representation
//! change with no behavioral payoff until a reorderer exists. Instead we
//! keep references global and *rebind* them here at lowering: decompose a
//! textual-order global index into `(table, local)` via the per-table
//! column widths, then recompose against the chosen physical order. The
//! execution layer (positional tuple access) and every existing plan are
//! untouched, and the identity order is a no-op — so the rule-based
//! planner and the TPC-C path are provably unaffected.
//!
//! ## Left-deep correctness
//!
//! A reordered left-deep plan's running tuple after *k* joins is the
//! physical tables `0..k` concatenated in physical order. Because those
//! tables form a *prefix* of the full physical layout, a table's base
//! offset in the full layout equals its offset in the running tuple. So a
//! predicate placed where all its tables are available (P14.13b's job)
//! reads correct positions from the full-layout remap. The inner side of
//! a hash/index join is a fresh leaf and keeps local indices — that's the
//! caller's concern, not the remap's.

use crate::sql::ir::expr::{Expression, Predicate};

/// Tuple-global offset of each table's first column in textual (binder)
/// order — the prefix sum of `widths`. The single home for this layout
/// table (review fix #5): the remap below, the Selinger join-graph
/// builder, and the memo normalizer all derive coordinates from it.
pub(crate) fn textual_base(widths: &[usize]) -> Vec<usize> {
    let mut base = vec![0usize; widths.len()];
    for table in 1..widths.len() {
        base[table] = base[table - 1] + widths[table - 1];
    }
    base
}

/// A rewrite of tuple-global column indices from textual order to a chosen
/// physical (reordered) layout. Built once per reordered plan; applied to
/// every column reference the plan carries.
pub struct ColumnRemap {
    /// `map[textual_global] == physical_global`. Length equals the total
    /// column count across all tables in the query.
    map: Vec<usize>,
}

impl ColumnRemap {
    /// Build the remap from per-table column `widths` in textual (binder)
    /// order and a `physical_order` — a permutation of `0..widths.len()`
    /// giving the chosen left-deep table sequence.
    ///
    /// Panics if `physical_order` is not a permutation of the table
    /// ordinals (a planner bug — crash rather than silently mis-map).
    pub fn new(widths: &[usize], physical_order: &[usize]) -> Self {
        let table_count = widths.len();
        assert_eq!(
            physical_order.len(),
            table_count,
            "physical_order must cover every table"
        );

        let textual_base = textual_base(widths);

        // Physical base offset of each table = prefix sum along the chosen
        // order. `seen` doubles as the permutation check.
        let mut physical_base = vec![0usize; table_count];
        let mut seen = vec![false; table_count];
        let mut acc = 0usize;
        for &t in physical_order {
            assert!(
                t < table_count && !seen[t],
                "physical_order is not a permutation"
            );
            seen[t] = true;
            physical_base[t] = acc;
            acc += widths[t];
        }
        let total = acc;

        let mut map = vec![0usize; total];
        for t in 0..table_count {
            for local in 0..widths[t] {
                map[textual_base[t] + local] = physical_base[t] + local;
            }
        }
        ColumnRemap { map }
    }

    /// True when the physical order equals the textual order, so applying
    /// the remap is a no-op. Lets the caller skip rewriting entirely (the
    /// rule-based / unreordered path).
    pub fn is_identity(&self) -> bool {
        self.map.iter().enumerate().all(|(i, &m)| i == m)
    }

    /// Remap one tuple-global index (a projection / order-by / aggregate
    /// column).
    pub fn apply_index(&self, index: usize) -> usize {
        assert!(
            index < self.map.len(),
            "column index {} out of range",
            index
        );
        self.map[index]
    }

    /// Remap every `Column` reference inside an expression.
    pub fn apply_expression(&self, expr: Expression) -> Expression {
        match expr {
            Expression::Column(i) => Expression::Column(self.apply_index(i)),
            Expression::Literal(_) | Expression::Parameter(_) => expr,
            Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
                op,
                left: Box::new(self.apply_expression(*left)),
                right: Box::new(self.apply_expression(*right)),
            },
            Expression::ExtractYear(arg) => {
                Expression::ExtractYear(Box::new(self.apply_expression(*arg)))
            }
        }
    }

    /// Remap every `Column` reference inside a predicate.
    pub fn apply_predicate(&self, pred: Predicate) -> Predicate {
        match pred {
            Predicate::Compare { op, left, right } => Predicate::Compare {
                op,
                left: self.apply_expression(left),
                right: self.apply_expression(right),
            },
            Predicate::And(a, b) => Predicate::And(
                Box::new(self.apply_predicate(*a)),
                Box::new(self.apply_predicate(*b)),
            ),
            Predicate::Or(a, b) => Predicate::Or(
                Box::new(self.apply_predicate(*a)),
                Box::new(self.apply_predicate(*b)),
            ),
            Predicate::Not(p) => Predicate::Not(Box::new(self.apply_predicate(*p))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
    use crate::types::Value;

    fn col(i: usize) -> Expression {
        Expression::Column(i)
    }
    fn eq(l: Expression, r: Expression) -> Predicate {
        Predicate::Compare {
            op: CompareOp::Eq,
            left: l,
            right: r,
        }
    }

    #[test]
    fn identity_order_is_a_no_op() {
        // Three tables, physical == textual order.
        let remap = ColumnRemap::new(&[2, 3, 1], &[0, 1, 2]);
        assert!(remap.is_identity());
        for i in 0..6 {
            assert_eq!(remap.apply_index(i), i);
        }
    }

    #[test]
    fn two_table_swap_remaps_offsets() {
        // widths [2, 3], physical order [1, 0]: table 1 (width 3) goes
        // first → base 0; table 0 (width 2) → base 3.
        // textual: t0 = {0,1}, t1 = {2,3,4}. physical: t1 → {0,1,2},
        // t0 → {3,4}. So map = [3, 4, 0, 1, 2].
        let remap = ColumnRemap::new(&[2, 3], &[1, 0]);
        assert!(!remap.is_identity());
        assert_eq!(remap.apply_index(0), 3);
        assert_eq!(remap.apply_index(1), 4);
        assert_eq!(remap.apply_index(2), 0);
        assert_eq!(remap.apply_index(3), 1);
        assert_eq!(remap.apply_index(4), 2);
    }

    #[test]
    fn remaps_predicate_column_references() {
        // Predicate `t0.c0 = t1.c0` is global `col(0) = col(2)`. After
        // swapping the tables it becomes `col(3) = col(0)`.
        let remap = ColumnRemap::new(&[2, 3], &[1, 0]);
        let pred = eq(col(0), col(2));
        let remapped = remap.apply_predicate(pred);
        match remapped {
            Predicate::Compare {
                left: Expression::Column(l),
                right: Expression::Column(r),
                ..
            } => {
                assert_eq!(l, 3);
                assert_eq!(r, 0);
            }
            _ => panic!("shape changed"),
        }
    }

    #[test]
    fn remaps_nested_expression_but_leaves_literals() {
        // (col(0) + 5) under the swap → (col(3) + 5); literal untouched.
        let remap = ColumnRemap::new(&[2, 3], &[1, 0]);
        let expr = Expression::BinaryOp {
            op: crate::sql::ir::expr::BinaryOp::Add,
            left: Box::new(col(0)),
            right: Box::new(Expression::Literal(Value::Int32(5))),
        };
        match remap.apply_expression(expr) {
            Expression::BinaryOp { left, right, .. } => {
                assert!(matches!(*left, Expression::Column(3)));
                assert!(matches!(*right, Expression::Literal(Value::Int32(5))));
            }
            _ => panic!("shape changed"),
        }
    }

    #[test]
    fn three_table_rotation() {
        // widths [1,1,1], physical order [2,0,1]: t2→0, t0→1, t1→2.
        let remap = ColumnRemap::new(&[1, 1, 1], &[2, 0, 1]);
        assert_eq!(remap.apply_index(0), 1); // t0
        assert_eq!(remap.apply_index(1), 2); // t1
        assert_eq!(remap.apply_index(2), 0); // t2
    }

    #[test]
    #[should_panic(expected = "not a permutation")]
    fn rejects_non_permutation_order() {
        // Table 0 listed twice, table 1 missing.
        ColumnRemap::new(&[1, 1], &[0, 0]);
    }
}
