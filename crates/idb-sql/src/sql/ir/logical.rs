//! `LogicalPlan` — our internal, catalog-resolved query representation.
//!
//! Sits between sqlparser's AST (generic SQL) and the executor tree (our
//! `Box<dyn Executor>`). The AST → LogicalPlan conversion (Step 4)
//! resolves table & column names against the `Catalog`, validates types,
//! and produces this IR. The planner (Step 5) lowers each variant into a
//! `Box<dyn Executor>`.
//!
//! ## Why a separate IR
//!
//! - **Resolution happens once.** Names become indices; type checks run
//!   at bind time, not per-tuple.
//! - **Decouples from sqlparser.** If we ever swap parser libraries,
//!   only the binder changes — the planner is unaffected.
//! - **Optimizer-ready.** Phase 14 (Selinger) and Phase 17/18 (Cascades)
//!   operate on `LogicalPlan`, not on parser AST. The shape is ours to
//!   evolve as the optimizer needs.

use serde::{Deserialize, Serialize};

use crate::catalog::{ColumnDef, IndexBackend, Schema};
use crate::common::{Error, Result};
use crate::sql::ir::expr::{Expression, Predicate};
use crate::types::{ColumnType, Value};

/// One catalog-resolved SQL statement.
// NOTE (plan deviation, H4a): `Select` is inherently the fat variant of this
// flat query-block IR (a documented design choice — docs/plan-tpch.md
// deviation #1). Adding the `derived` field tipped the `large_enum_variant`
// size gap just past clippy's 200-byte threshold. Boxing individual `Select`
// fields to shave bytes would add indirection to the hottest IR node for no
// runtime benefit; we allow the lint on the whole IR instead.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    // --- DDL (catalog-only; not executed by operator tree) -----------------
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        /// Column indices that compose the primary key.
        primary_key: Vec<usize>,
    },

    /// `CREATE [UNIQUE] INDEX name ON table [USING backend] (columns)`.
    /// Catalog-only like `CreateTable`; the session additionally backfills
    /// entries for rows that existed before the index.
    CreateIndex {
        name: String,
        table: String,
        /// Column indices in the target table's schema.
        columns: Vec<usize>,
        unique: bool,
        backend: IndexBackend,
    },

    // --- DML (run through executor tree) ----------------------------------
    /// `SELECT [projection|aggregates] FROM table [JOIN …]* [WHERE filter] [LIMIT limit]`.
    ///
    /// `joins` is empty for a single-table SELECT (today's common case).
    /// Column indices in `projection` and `filter` are tuple-global: they
    /// index into the concatenated `table || joins[0] || joins[1] || …`
    /// row produced by the operator tree.
    ///
    /// Projection/aggregates combinations:
    /// - `projection` non-empty, `aggregates` empty: column-projection (or
    ///   `SELECT *` when both are empty).
    /// - `aggregates` non-empty, `projection` empty: whole-table
    ///   aggregation (no GROUP BY).
    /// - Both non-empty: GROUP BY — `projection` holds the group-key
    ///   columns (the binder enforces that the GROUP BY clause and the
    ///   projected plain columns coincide). The aggregate output row is
    ///   keys in projection order, then aggregates in `aggregates` order.
    ///   H2b lifts H1's keys-before-aggregates ordering: display order is
    ///   free (`SELECT COUNT(*), region`), carried by `select_list`.
    ///
    /// Coordinate rule: when `aggregates` is non-empty, `order_by`,
    /// `having`, and `select_list` index the aggregate OUTPUT row (keys ++
    /// aggregates), not the input tuple — the input scope no longer exists
    /// above the aggregate. When `aggregates` is empty they index the input
    /// tuple like `filter` does.
    ///
    /// `select_list` is H2b's display layer — the final output row as a
    /// list of `Expression`s (`SELECT a + b`, `SELECT 100.00 * SUM(x) /
    /// SUM(y)`, or a mere reordering `SELECT COUNT(*), region`). It is
    /// EMPTY for every query whose display is the identity over its space
    /// (bare projection, or keys-then-aggregates in order) — the zero-churn
    /// invariant: an empty `select_list` reproduces exactly the pre-H2b
    /// plan shape. When non-empty, the spine lowers it to a `Compute`
    /// (above Sort, below Limit) that replaces the bare Projection.
    Select {
        table: String,
        joins: Vec<JoinClause>,
        /// FROM-subqueries (H4a). Each entry is an uncorrelated derived table
        /// bound to an alias; `table` / `joins[i].right_table` may name one of
        /// these aliases instead of a catalog table (a derived alias shadows a
        /// catalog name within the statement). EMPTY for every query without a
        /// derived table — the zero-churn invariant: existing plans are byte
        /// identical. The binder computes each derived table's output `schema`
        /// so WHERE / GROUP BY / aggregates over its columns resolve like any
        /// catalog table's.
        derived: Vec<DerivedTable>,
        /// Column indices in the requested output order. Empty = `SELECT *`
        /// (when aggregates is also empty) or whole-table aggregation (when
        /// aggregates is non-empty). H1 meaning unchanged: bare column
        /// projection (ungrouped) or the group keys in first-appearance
        /// order (grouped). Empty in the ungrouped-computed case (`Compute`
        /// replaces the Projection).
        projection: Vec<usize>,
        /// Aggregate functions to compute over the rows. Empty means no
        /// aggregation.
        aggregates: Vec<AggregateSpec>,
        /// H2b computed projections — the display row. EMPTY = identity
        /// display (see the type-level coordinate rule). Non-empty =
        /// per-column output `Expression`s; when `aggregates` is empty they
        /// index the input tuple (like `filter`), otherwise the aggregate
        /// output row (like `order_by` / `having`).
        select_list: Vec<Expression>,
        filter: Option<Predicate>,
        /// `ORDER BY (col, dir)+`. Column indices are tuple-global (same
        /// scope as `projection` / `filter`) — except under GROUP BY; see
        /// the coordinate rule above. Empty means unsorted.
        order_by: Vec<(usize, OrderDir)>,
        /// `HAVING` predicate over the aggregate output row (coordinate
        /// rule above). `None` when absent; the binder only produces
        /// `Some` alongside non-empty `aggregates`.
        having: Option<Predicate>,
        limit: Option<usize>,
    },

    /// `INSERT INTO table VALUES (row1), (row2), …`.
    ///
    /// Each row is a vector of *expressions* in schema order. Most are
    /// literals; `Expression::Parameter(i)` slots support prepared
    /// statements. Computed inserts (`INSERT … SELECT`) are Phase 14.
    Insert {
        table: String,
        rows: Vec<Vec<Expression>>,
    },

    /// `UPDATE table SET col = expr, … [WHERE filter]`.
    Update {
        table: String,
        /// `(column_index, value_expression)` pairs. Indices into the
        /// target table's schema.
        set_clauses: Vec<(usize, Expression)>,
        filter: Option<Predicate>,
    },

    /// `DELETE FROM table [WHERE filter]`.
    Delete {
        table: String,
        filter: Option<Predicate>,
    },

    // --- Catalog admin ----------------------------------------------------
    /// `ANALYZE TABLE t` (P14.2) — scan `t` once, compute row count,
    /// per-column NDV, and per-column equi-width histogram, persist into
    /// `__sys_table_stats` and `__sys_column_stats`.
    Analyze {
        table: String,
    },

    // --- Transaction control ----------------------------------------------
    BeginTxn,
    CommitTxn,
    AbortTxn,

    // --- Plan introspection -----------------------------------------------
    /// `EXPLAIN <stmt>` — wraps any other plan; planner returns the
    /// child's executor-tree explain string instead of executing it.
    Explain(Box<LogicalPlan>),
}

/// Whether a join preserves unmatched rows of its left input (H3b). `Inner`
/// keeps only matched pairs; `LeftOuter` additionally pads every left row
/// that matched nothing with a right side of NULLs. Derives mirror
/// `OrderDir` (a small copy enum carried in the serde-clean physical IR) so
/// the same value threads the logical `JoinClause` and `PhysOp` alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    LeftOuter,
}

/// One JOIN clause attached to a SELECT. The right side is always a
/// single table at this stage (3+-way joins compose by repeating). The
/// `on` predicate is `None` for implicit `FROM a, b` (cross product) and
/// `Some(p)` for explicit `JOIN b ON p`. WHERE filters are kept separate
/// — Phase 14's cost-based planner will revisit predicate pushdown.
///
/// `kind` distinguishes an inner join from a `LEFT OUTER JOIN` (H3b). For
/// `LeftOuter` the binder guarantees `on` is `Some` (an outer join without
/// an ON is meaningless — a cross product can never leave a row unmatched).
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub right_table: String,
    pub right_alias: Option<String>,
    pub on: Option<Predicate>,
    pub kind: JoinKind,
}

/// One FROM-subquery (H4a, uncorrelated). `alias` is the name the outer
/// statement refers to it by (and the name that shadows any catalog table);
/// `plan` is the already-bound inner query; `schema` is its output columns,
/// computed by the binder (`select_output_schema` + captured output names /
/// the optional column-list alias) so the outer scope treats the derived
/// table exactly like a catalog table. Uncorrelated by construction — the
/// inner query is bound in a fresh scope with no reference to outer columns.
#[derive(Debug, Clone)]
pub struct DerivedTable {
    pub alias: String,
    pub plan: Box<LogicalPlan>,
    pub schema: Vec<ColumnDef>,
}

/// SQL-level aggregate function spec. The planner translates each
/// variant into the corresponding `execution::AggregateFn`. Each payload
/// is an `Expression` (H2a: aggregates over arithmetic, e.g. Q1's
/// `SUM(l_extendedprice * (1 - l_discount))`); any column references
/// inside are tuple-global indices (same convention as `Predicate`).
/// `PartialEq`/`Eq` so the binder can match an ORDER BY / HAVING
/// aggregate expression against the SELECT list's specs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateSpec {
    CountStar,
    Count { arg: Expression, distinct: bool },
    Sum(Expression),
    Min(Expression),
    Max(Expression),
    Avg(Expression),
}

impl AggregateSpec {
    /// The argument expression, or `None` for `COUNT(*)`. Mirrors the
    /// executor's `AggregateFn::arg`.
    pub fn arg(&self) -> Option<&Expression> {
        match self {
            AggregateSpec::CountStar => None,
            AggregateSpec::Count { arg, .. }
            | AggregateSpec::Sum(arg)
            | AggregateSpec::Min(arg)
            | AggregateSpec::Max(arg)
            | AggregateSpec::Avg(arg) => Some(arg),
        }
    }

    /// Output column type over a row of `input_types`. This is the single
    /// source of the aggregate type-promotion rules (the executor's
    /// `build_aggregate_schema` and the binder's `select_output_schema` both
    /// route through here): `COUNT`→`Int64`; `SUM(Int32/Int64)` promotes to
    /// `Int64` (overflow head room), `SUM(Decimal)` keeps its type;
    /// `MIN`/`MAX` preserve the argument type; `AVG` is `Decimal{18,4}` for
    /// integer inputs (a fractional result) and the input scale for Decimal.
    /// An argument whose type can't be inferred, or a non-numeric SUM/AVG,
    /// errors loudly rather than mistyping the output.
    pub fn output_type(&self, input_types: &[ColumnType]) -> Result<ColumnType> {
        match self {
            // COUNT is always Int64, but the argument must still be a typed
            // expression — matches the pre-refactor infer-then-type guard
            // (COUNT of an uninferable expression is a bad plan).
            AggregateSpec::CountStar => Ok(ColumnType::Int64),
            AggregateSpec::Count { arg, distinct } => {
                let label = if *distinct {
                    "COUNT(DISTINCT)"
                } else {
                    "COUNT"
                };
                require_arg_type(arg, input_types, label)?;
                Ok(ColumnType::Int64)
            }
            AggregateSpec::Sum(arg) => match require_arg_type(arg, input_types, "SUM")? {
                ColumnType::Int32 | ColumnType::Int64 => Ok(ColumnType::Int64),
                ty @ ColumnType::Decimal { .. } => Ok(ty),
                other => Err(Error::SqlParse(format!(
                    "SUM of non-numeric type {:?} not supported",
                    other
                ))),
            },
            AggregateSpec::Min(arg) => require_arg_type(arg, input_types, "MIN"),
            AggregateSpec::Max(arg) => require_arg_type(arg, input_types, "MAX"),
            AggregateSpec::Avg(arg) => match require_arg_type(arg, input_types, "AVG")? {
                ColumnType::Int32 | ColumnType::Int64 => Ok(ColumnType::Decimal {
                    precision: 18,
                    scale: 4,
                }),
                ty @ ColumnType::Decimal { .. } => Ok(ty),
                other => Err(Error::SqlParse(format!(
                    "AVG of non-numeric type {:?} not supported",
                    other
                ))),
            },
        }
    }

    /// Column name for the synthetic schema entry this aggregate emits. A
    /// bare-column argument keeps the "{fn}_{column}" naming (schema
    /// stability); any richer expression is "{fn}_expr".
    pub fn output_name(&self, input_columns: &[ColumnDef]) -> String {
        let prefix = match self {
            AggregateSpec::CountStar => return "count_star".to_string(),
            AggregateSpec::Count {
                distinct: false, ..
            } => "count",
            AggregateSpec::Count { distinct: true, .. } => "count_distinct",
            AggregateSpec::Sum(_) => "sum",
            AggregateSpec::Min(_) => "min",
            AggregateSpec::Max(_) => "max",
            AggregateSpec::Avg(_) => "avg",
        };
        match self.arg() {
            Some(Expression::Column(i)) => format!("{}_{}", prefix, input_columns[*i].name),
            _ => format!("{}_expr", prefix),
        }
    }
}

/// The static type an aggregate argument produces over `input_types`, or a
/// loud error naming the aggregate when it can't be inferred (mirrors
/// `eval_binary_op` — the accumulator and output type are picked from this
/// and MUST match the runtime values).
fn require_arg_type(
    arg: &Expression,
    input_types: &[ColumnType],
    label: &str,
) -> Result<ColumnType> {
    arg.column_type(input_types)
        .ok_or_else(|| Error::SqlParse(format!("{}: cannot infer argument type", label)))
}

/// SQL-level sort direction. Separate from `execution::SortDir` so the
/// SQL IR has no executor dependency; the planner translates between
/// them. `Hash` because the memo keys winners by order requirement
/// (17-B), and `OrderDir` is part of that key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderDir {
    Asc,
    Desc,
}

impl LogicalPlan {
    /// Walk the plan tree and substitute every `Expression::Parameter(i)`
    /// with `Expression::Literal(params[i])`. Used by
    /// `PreparedStatement::execute` to bind parameters before planning.
    pub fn substitute_params(self, params: &[Value]) -> crate::common::Result<LogicalPlan> {
        match self {
            LogicalPlan::Select {
                table,
                joins,
                derived,
                projection,
                aggregates,
                select_list,
                filter,
                order_by,
                having,
                limit,
            } => {
                // Parameters can appear inside a derived table's inner query
                // (e.g. `WHERE col = ?`), so substitute recursively there too.
                let derived = derived
                    .into_iter()
                    .map(|d| {
                        Ok::<DerivedTable, crate::common::Error>(DerivedTable {
                            alias: d.alias,
                            plan: Box::new(d.plan.substitute_params(params)?),
                            schema: d.schema,
                        })
                    })
                    .collect::<crate::common::Result<Vec<_>>>()?;
                let joins = joins
                    .into_iter()
                    .map(|j| {
                        Ok::<JoinClause, crate::common::Error>(JoinClause {
                            right_table: j.right_table,
                            right_alias: j.right_alias,
                            on: match j.on {
                                Some(p) => Some(p.substitute_params(params)?),
                                None => None,
                            },
                            kind: j.kind,
                        })
                    })
                    .collect::<crate::common::Result<Vec<_>>>()?;
                let filter = match filter {
                    Some(p) => Some(p.substitute_params(params)?),
                    None => None,
                };
                let having = match having {
                    Some(p) => Some(p.substitute_params(params)?),
                    None => None,
                };
                let select_list = select_list
                    .into_iter()
                    .map(|e| e.substitute_params(params))
                    .collect::<crate::common::Result<Vec<_>>>()?;
                Ok(LogicalPlan::Select {
                    table,
                    joins,
                    derived,
                    projection,
                    aggregates,
                    select_list,
                    filter,
                    order_by,
                    having,
                    limit,
                })
            }
            LogicalPlan::Insert { table, rows } => {
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| e.substitute_params(params))
                            .collect::<crate::common::Result<Vec<_>>>()
                    })
                    .collect::<crate::common::Result<Vec<_>>>()?;
                Ok(LogicalPlan::Insert { table, rows })
            }
            LogicalPlan::Update {
                table,
                set_clauses,
                filter,
            } => {
                let set_clauses = set_clauses
                    .into_iter()
                    .map(|(idx, expr)| {
                        Ok::<_, crate::common::Error>((idx, expr.substitute_params(params)?))
                    })
                    .collect::<crate::common::Result<Vec<_>>>()?;
                let filter = match filter {
                    Some(p) => Some(p.substitute_params(params)?),
                    None => None,
                };
                Ok(LogicalPlan::Update {
                    table,
                    set_clauses,
                    filter,
                })
            }
            LogicalPlan::Delete { table, filter } => {
                let filter = match filter {
                    Some(p) => Some(p.substitute_params(params)?),
                    None => None,
                };
                Ok(LogicalPlan::Delete { table, filter })
            }
            // No parameters in DDL or TC.
            other => Ok(other),
        }
    }
}

/// Output columns of a grouped / whole-table aggregate: the group-key
/// columns (cloned from `input` at `group_by` indices, in that order) then
/// one synthetic column per aggregate, typed and named via `AggregateSpec`.
/// Aggregates unify as nullable (empty input → SUM/AVG/MIN/MAX NULL; COUNT
/// returns 0, but nullable keeps the schema simple and matches PostgreSQL).
/// One source of truth for aggregate output typing — the executor's
/// `build_aggregate_schema` and the binder's `select_output_schema` both
/// route through here.
pub fn aggregate_output_columns(
    input: &Schema,
    group_by: &[usize],
    aggregates: &[AggregateSpec],
) -> Result<Vec<ColumnDef>> {
    let input_types = input.column_types();
    let mut columns: Vec<ColumnDef> = group_by.iter().map(|&c| input.columns[c].clone()).collect();
    for agg in aggregates {
        columns.push(ColumnDef {
            name: agg.output_name(&input.columns),
            ty: agg.output_type(&input_types)?,
            nullable: true,
            default: None,
        });
    }
    Ok(columns)
}

/// Output columns of a row-level computed projection (H2b): one column per
/// expression, typed via `Expression::column_type` over `input`. A `None`
/// inference (e.g. `Varchar + Int`, or a scale-mismatched division) is a bad
/// plan and errors loudly. Nullability splits by arm: a bare `Column(i)` is a
/// passthrough and inherits the input column's flag; a computed column is
/// nullable (arithmetic can produce NULL even from non-nullable inputs). One
/// source of truth — the executor's `build_compute_schema` delegates here.
pub fn compute_output_columns(input: &Schema, exprs: &[Expression]) -> Result<Vec<ColumnDef>> {
    let types = input.column_types();
    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let ty = expr.column_type(&types).ok_or_else(|| {
            Error::SqlParse(format!(
                "compute: cannot infer type of expression `{}`",
                expr
            ))
        })?;
        let (name, nullable) = match expr {
            Expression::Column(i) => (input.columns[*i].name.clone(), input.columns[*i].nullable),
            _ => ("expr".to_string(), true),
        };
        columns.push(ColumnDef {
            name,
            ty,
            nullable,
            default: None,
        });
    }
    Ok(columns)
}

/// Output columns of a bound `Select`, computed from the join-tuple `input`
/// schema (the FROM relations concatenated — the coordinate space the
/// binder resolves against). Mirrors the executor's final output shape
/// WITHOUT building an operator tree, so the binder can type a derived
/// table's schema:
///   - bare projection (aggregates + select_list empty): the selected input
///     columns (`SELECT *` = all input columns);
///   - ungrouped computed (select_list non-empty): a Compute over the input;
///   - aggregated: the aggregate output row (keys ++ aggregates), then a
///     Compute over that row when `select_list` carries a display.
pub fn select_output_schema(
    input: &Schema,
    projection: &[usize],
    aggregates: &[AggregateSpec],
    select_list: &[Expression],
) -> Result<Vec<ColumnDef>> {
    if aggregates.is_empty() {
        if select_list.is_empty() {
            // Bare projection; an empty projection is `SELECT *`.
            if projection.is_empty() {
                Ok(input.columns.clone())
            } else {
                Ok(projection
                    .iter()
                    .map(|&i| input.columns[i].clone())
                    .collect())
            }
        } else {
            // Ungrouped computed display: Compute over the input tuple.
            compute_output_columns(input, select_list)
        }
    } else {
        // The aggregate output row: group keys (in `projection`) ++ aggregates.
        let agg_columns = aggregate_output_columns(input, projection, aggregates)?;
        if select_list.is_empty() {
            Ok(agg_columns)
        } else {
            // Computed display sits above the aggregate, so it types against
            // the aggregate output row, not the input.
            let agg_schema = Schema {
                name: input.name.clone(),
                table_id: input.table_id,
                columns: agg_columns,
                primary_key: vec![],
            };
            compute_output_columns(&agg_schema, select_list)
        }
    }
}
