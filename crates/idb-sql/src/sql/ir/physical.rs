//! `PhysOp` — the model-neutral physical plan IR.
//!
//! A planner emits a `PhysOp` tree; an `ExecutionModel` compiles it into its
//! own runnable form (`Volcano` → a pull operator tree, `Push` → a push
//! pipeline). It is the seam that makes execution-model interchange *real*:
//! both models read the same neutral plan and build differently.
//!
//! ## Data-only by design
//!
//! `PhysOp` carries **names, ASTs, and column indices** — never `Arc<Table>`
//! handles, never closures, never engine generics. Runtime handles are
//! re-resolved against the catalog/engine by the builder; the `Predicate` /
//! `Expression` ASTs are re-compiled to closures at build time (see
//! `Predicate::compile`). That keeps the IR a pure value: cloneable,
//! inspectable, and serde-clean (the Phase-19 plan-store / workload-log
//! prerequisite).
//!
//! ## What the planner decides vs. what the builder does
//!
//! The planner keeps all its intelligence — cost-based join-algorithm choice,
//! index selection — and *records the decision in the variant it emits*
//! (`NestedLoopJoin` vs `HashJoin` vs `IndexNestedLoopJoin`). The builder is
//! mechanical: it constructs exactly the operator the variant names.
//!
//! ## PK lookup
//!
//! `PkLookup` is a single-row primary-key access — the planner emits it when a
//! `WHERE` pins a single-column PK by equality (`WHERE pk = lit`), in place of
//! `SeqScan + Filter`. Composite PKs fall back to scan+filter for now.

use serde::{Deserialize, Serialize};

use crate::sql::ir::expr::{Expression, Predicate};
use crate::sql::ir::logical::{AggregateSpec, JoinKind, OrderDir};
use crate::types::Value;

/// One node of a physical plan. Children are owned `Box<PhysOp>`, so a plan
/// is a self-contained value tree. Tables and indexes are referenced by name
/// and resolved against the catalog when an `ExecutionModel` builds the node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysOp {
    /// Full-table scan in PK order.
    SeqScan { table: String },

    /// Range scan over a secondary index whose leading columns equal `prefix`.
    IndexScan {
        table: String,
        index: String,
        prefix: Vec<Value>,
    },

    /// Single-row lookup by primary key. `pk` holds the PK column values,
    /// coerced to the PK column types.
    PkLookup { table: String, pk: Vec<Value> },

    /// Materialized FROM-subquery (H4a). A leaf of the outer plan: the inner
    /// `subplan` runs, its rows are buffered, and the outer plan reads them as
    /// though scanning a table of `schema`. `alias` is the name the outer
    /// query used (rendered by EXPLAIN). The schema travels in the IR, so the
    /// executor builds this leaf without any catalog lookup. No predicate
    /// pushdown into `subplan` and no index paths (recorded lever): a WHERE on
    /// a derived column becomes a `Filter` ABOVE this leaf, never below the
    /// materialization boundary.
    DerivedScan {
        alias: String,
        subplan: Box<PhysOp>,
        schema: Vec<crate::catalog::ColumnDef>,
    },

    /// Row-wise selection: keep rows where `predicate` holds.
    Filter {
        input: Box<PhysOp>,
        predicate: Predicate,
    },

    /// Narrow / reorder columns. `cols` indexes into the input's schema.
    Projection {
        input: Box<PhysOp>,
        cols: Vec<usize>,
    },

    /// Row-level computed projection (H2b): emit one output column per
    /// `Expression`, evaluated against the input tuple. Subsumes
    /// `Projection` when the display is more than a column subset/reorder
    /// (`SELECT a + b`, `SELECT 100.00 * SUM(x) / SUM(y)`, reorderings like
    /// `SELECT COUNT(*), region`). Coordinates match the binder rule: input
    /// tuple when the plan is unaggregated, aggregate output row otherwise.
    Compute {
        input: Box<PhysOp>,
        exprs: Vec<Expression>,
    },

    /// Block nested-loop join. `inner` is the materialized side (today always
    /// a `SeqScan` of the right table). `on` is the ON predicate over the
    /// concatenated `outer || inner` tuple; `None` is a cross join. `kind`
    /// selects inner vs `LEFT OUTER` (H3b): under `LeftOuter`, the full `on`
    /// runs per pair (R1) and any left row that matches nothing emits padded
    /// with a right side of NULLs.
    NestedLoopJoin {
        outer: Box<PhysOp>,
        inner: Box<PhysOp>,
        on: Option<Predicate>,
        kind: JoinKind,
    },

    /// Index nested-loop join: probe `inner_index` on `inner_table` once per
    /// outer row, keyed by the outer columns `outer_key_cols`.
    IndexNestedLoopJoin {
        outer: Box<PhysOp>,
        inner_table: String,
        inner_index: String,
        outer_key_cols: Vec<usize>,
    },

    /// Hash equi-join: build a hash table on `inner` keyed by `inner_key_col`,
    /// probe with `outer_key_col`. NULL keys never match (SQL equi-join).
    /// `kind` selects inner vs `LEFT OUTER` (H3b). `residual` is the rest of
    /// a compound ON after the hashed equi conjunct is removed (Q13's
    /// `o_comment NOT LIKE …`) — evaluated per candidate pair over the
    /// concatenated `outer || inner` tuple; a `LeftOuter` probe that finds no
    /// surviving pair emits the left row padded with NULLs. `None` for a bare
    /// equi join (inner, or a single-conjunct outer ON).
    HashJoin {
        outer: Box<PhysOp>,
        inner: Box<PhysOp>,
        outer_key_col: usize,
        inner_key_col: usize,
        kind: JoinKind,
        residual: Option<Predicate>,
    },

    /// Merge equi-join (17-B): both inputs must arrive sorted ascending on
    /// their key columns — the planner guarantees it (Sort enforcer).
    /// NULL keys never match (SQL equi-join).
    MergeJoin {
        left: Box<PhysOp>,
        right: Box<PhysOp>,
        left_key_col: usize,
        right_key_col: usize,
    },

    /// Aggregation. `group_by` empty: whole-relation, one output row.
    /// Non-empty: one row per distinct key — key columns (input
    /// coordinates, in `group_by` order) then aggregates.
    HashAggregate {
        input: Box<PhysOp>,
        group_by: Vec<usize>,
        aggregates: Vec<AggregateSpec>,
    },

    /// In-memory sort by `keys` (column index + direction), major-to-minor.
    Sort {
        input: Box<PhysOp>,
        keys: Vec<(usize, OrderDir)>,
    },

    /// Pass through at most `max_rows` rows, then stop.
    Limit { input: Box<PhysOp>, max_rows: usize },

    /// Insert pre-evaluated literal rows (INSERT VALUES). Yields a row count.
    Insert {
        table: String,
        rows: Vec<Vec<Value>>,
    },

    /// Update columns of rows produced by `input`. Each `(col, expr)` computes
    /// the new value of `col` from the input row. Yields a row count.
    Update {
        table: String,
        input: Box<PhysOp>,
        set: Vec<(usize, Expression)>,
    },

    /// Delete rows produced by `input` (by their PK). Yields a row count.
    Delete { table: String, input: Box<PhysOp> },
}

impl PhysOp {
    /// Render this plan as an indented tree, one operator per line, each line
    /// terminated by `\n`. Engine-free — reads only the IR — so EXPLAIN and
    /// plan-shape tests never have to build or scan.
    ///
    /// This IR renderer is the source of truth for SQL `EXPLAIN` output; its
    /// line formats may differ from the runtime operators' own
    /// `Executor::explain` debug rendering. An operator's explain can print
    /// runtime-only facts the IR cannot know — e.g. `HashJoin` prints its
    /// `build_size` (and a materialized-inner row/key-group count), whereas the
    /// IR prints `inner_key` and renders the inner subtree instead.
    pub fn explain(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let inner_pad = "  ".repeat(indent + 1);
        match self {
            PhysOp::SeqScan { table } => format!("{pad}SeqScan({table})\n"),
            PhysOp::IndexScan { table, index, .. } => {
                format!("{pad}IndexScan({table}, on {index})\n")
            }
            PhysOp::PkLookup { table, .. } => format!("{pad}PkLookup({table})\n"),
            PhysOp::DerivedScan { alias, subplan, .. } => {
                // `DerivedScan(alias)` header, then the inner subplan indented
                // one level — the materialized subquery reads as a subtree.
                format!("{pad}DerivedScan({alias})\n{}", subplan.explain(indent + 1))
            }
            PhysOp::Filter { input, .. } => {
                format!("{pad}Filter\n{}", input.explain(indent + 1))
            }
            PhysOp::Projection { input, cols } => {
                format!("{pad}Projection({cols:?})\n{}", input.explain(indent + 1))
            }
            PhysOp::Compute { input, exprs } => {
                // Each expr renders through `Expression::Display` (`Column(i)
                // → c{i}`), so `SELECT a + b` reads `Compute[(c0 + c1)]`.
                let items: Vec<String> = exprs.iter().map(|e| e.to_string()).collect();
                format!(
                    "{pad}Compute[{}]\n{}",
                    items.join(", "),
                    input.explain(indent + 1)
                )
            }
            PhysOp::NestedLoopJoin {
                outer, inner, kind, ..
            } => {
                // Inner rendering byte-unchanged (golden stability); outer
                // gets a marker.
                let marker = match kind {
                    JoinKind::Inner => "",
                    JoinKind::LeftOuter => "(outer)",
                };
                format!(
                    "{pad}NestedLoopJoin{marker}\n{}{}",
                    outer.explain(indent + 1),
                    inner.explain(indent + 1)
                )
            }
            PhysOp::IndexNestedLoopJoin {
                outer, inner_index, ..
            } => format!(
                "{pad}IndexNestedLoopJoin(probe via {inner_index})\n{}{inner_pad}<inner: IndexScan on {inner_index}>\n",
                outer.explain(indent + 1)
            ),
            PhysOp::HashJoin {
                outer,
                inner,
                outer_key_col,
                inner_key_col,
                kind,
                ..
            } => {
                // Inner rendering byte-unchanged (golden stability); outer
                // prepends a `left_outer` marker before the keys.
                let marker = match kind {
                    JoinKind::Inner => "",
                    JoinKind::LeftOuter => "left_outer, ",
                };
                format!(
                    "{pad}HashJoin({marker}outer_key={outer_key_col}, inner_key={inner_key_col})\n{}{}",
                    outer.explain(indent + 1),
                    inner.explain(indent + 1)
                )
            }
            PhysOp::MergeJoin {
                left,
                right,
                left_key_col,
                right_key_col,
            } => format!(
                "{pad}MergeJoin(left_key={left_key_col}, right_key={right_key_col})\n{}{}",
                left.explain(indent + 1),
                right.explain(indent + 1)
            ),
            PhysOp::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                // group segment only when grouped — ungrouped rendering
                // stays byte-identical (EXPLAIN golden stability).
                let mut labels: Vec<String> = Vec::with_capacity(aggregates.len() + 1);
                if !group_by.is_empty() {
                    // `c{i}` matches how aggregate args render their columns,
                    // so group keys read as columns, not integer literals.
                    let keys: Vec<String> = group_by.iter().map(|c| format!("c{c}")).collect();
                    labels.push(format!("group=({})", keys.join(",")));
                }
                labels.extend(aggregates.iter().map(agg_label));
                format!(
                    "{pad}HashAggregate[{}]\n{}",
                    labels.join(", "),
                    input.explain(indent + 1)
                )
            }
            PhysOp::Sort { input, keys } => {
                let keys: Vec<String> = keys
                    .iter()
                    .map(|(col, dir)| {
                        let dir = match dir {
                            OrderDir::Asc => " ASC",
                            OrderDir::Desc => " DESC",
                        };
                        format!("{col}{dir}")
                    })
                    .collect();
                format!(
                    "{pad}Sort[{}]\n{}",
                    keys.join(", "),
                    input.explain(indent + 1)
                )
            }
            PhysOp::Limit { input, max_rows } => {
                format!("{pad}Limit({max_rows})\n{}", input.explain(indent + 1))
            }
            PhysOp::Insert { table, rows } => {
                format!("{pad}Insert({} rows → {table})\n", rows.len())
            }
            PhysOp::Update { table, input, set } => {
                let cols: Vec<usize> = set.iter().map(|(i, _)| *i).collect();
                format!(
                    "{pad}Update({table}, set_cols={cols:?})\n{}",
                    input.explain(indent + 1)
                )
            }
            PhysOp::Delete { table, input } => {
                format!("{pad}Delete({table})\n{}", input.explain(indent + 1))
            }
        }
    }
}

/// Aggregate label for EXPLAIN, mirroring `HashAggregate`'s operator
/// labels. Args render through `Expression::Display`, whose `Column(i) →
/// c{i}` renders a column-only arg as `SUM(c3)`, not the ambiguous `SUM(3)`.
fn agg_label(spec: &AggregateSpec) -> String {
    match spec {
        AggregateSpec::CountStar => "COUNT(*)".to_string(),
        AggregateSpec::Count {
            arg,
            distinct: false,
        } => format!("COUNT({arg})"),
        AggregateSpec::Count {
            arg,
            distinct: true,
        } => format!("COUNT(DISTINCT {arg})"),
        AggregateSpec::Sum(arg) => format!("SUM({arg})"),
        AggregateSpec::Min(arg) => format!("MIN({arg})"),
        AggregateSpec::Max(arg) => format!("MAX({arg})"),
        AggregateSpec::Avg(arg) => format!("AVG({arg})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
    use crate::types::Value;

    // Proves the IR is serde-clean end to end: a tree carrying a `Predicate`
    // and an `Expression` AST round-trips through JSON unchanged. `PhysOp`
    // has no `PartialEq` (its AST leaves don't), so equality is checked by
    // re-serializing the decoded value and comparing the wire forms.
    #[test]
    fn phys_op_tree_round_trips_through_serde() {
        let plan = PhysOp::Update {
            table: "warehouse".into(),
            set: vec![(1, Expression::Literal(Value::Int64(9999)))],
            input: Box::new(PhysOp::Filter {
                predicate: Predicate::Compare {
                    op: CompareOp::Eq,
                    left: Expression::Column(0),
                    right: Expression::Literal(Value::Int32(1)),
                },
                input: Box::new(PhysOp::SeqScan {
                    table: "warehouse".into(),
                }),
            }),
        };

        let encoded = serde_json::to_string(&plan).unwrap();
        let decoded: PhysOp = serde_json::from_str(&encoded).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);
    }
}
