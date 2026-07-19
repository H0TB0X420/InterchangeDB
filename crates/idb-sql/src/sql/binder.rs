//! Binder — sqlparser AST → catalog-resolved `LogicalPlan`.
//!
//! Resolves table & column names against `Catalog<E>`, maps sqlparser data
//! types and literals to our `ColumnType` / `Value`, and rejects shapes we
//! don't yet support with `Error::SqlParse`.
//!
//! Phase 11 surface — what binds:
//!   CREATE TABLE, INSERT VALUES, SELECT, UPDATE, DELETE,
//!   BEGIN / COMMIT / ROLLBACK, EXPLAIN `<stmt>`.
//!
//! Joins, aggregates, ORDER BY, subqueries — deferred to Phase 12/13.
//! Each unsupported variant errors with a descriptive message so the
//! caller learns *which* shape the binder rejected.

use std::sync::Arc;

use sqlparser::ast::{
    self, BinaryOperator, ColumnDef as AstColumnDef, ColumnOption, DataType as AstDataType,
    Expr as AstExpr, ObjectName, Query, SetExpr, Statement, TableConstraint, TableFactor,
    UnaryOperator, Value as AstValue, Values,
};

use crate::catalog::{Catalog, ColumnDef, IndexBackend, Schema, TableId};
use crate::common::{Error, Result};
use crate::sql::ir::expr::{BinaryOp, CompareOp, Expression, Predicate};
use crate::sql::ir::logical::{
    select_output_schema, AggregateSpec, DerivedTable, JoinKind, LogicalPlan, OrderDir,
};
use crate::storage::StorageEngine;
use crate::types::{ColumnType, Decimal, Value};

/// Maximum nesting depth for derived tables / FROM-subqueries (H4a),
/// enforced at bind time. A bounded limit on bind recursion — a query
/// nesting derived tables deeper than this is rejected loudly rather than
/// risking a stack-deep bind. Four levels covers the TPC-H shapes we target
/// (Q13/Q7/Q8/Q9 nest one level; Q22's inner shells two).
const MAX_DERIVED_DEPTH: usize = 4;

/// Bound SELECT items: `(projection, aggregates, select_list, output_aliases)`.
/// The per-item output `AS` aliases (H4a step 1) ride alongside the three
/// coordinate vectors so a derived table can name its output schema.
type BoundSelectItems = (
    Vec<usize>,
    Vec<AggregateSpec>,
    Vec<Expression>,
    Vec<Option<String>>,
);

/// Bind one SQL statement against the catalog.
pub struct Binder<E: StorageEngine> {
    catalog: Arc<Catalog<E>>,
}

/// A scope a statement's expressions resolve against. May span multiple
/// tables (joins). Column references resolve to *tuple-global* indices —
/// each table contributes a contiguous run of columns starting at its
/// `column_offset`.
///
/// Single-table queries have `tables.len() == 1`; column indices then
/// match the schema's column order directly (`column_offset == 0`).
struct Scope {
    tables: Vec<ScopedTable>,
    /// Column names resolvable in ENCLOSING scopes (H4b correlation
    /// detection). A subquery is bound in a FRESH scope; if it references a
    /// column absent from its own `tables` but present here, it is a
    /// correlated reference — unsupported this phase, rejected with the column
    /// named. Never used for RESOLUTION (uncorrelated by construction), only to
    /// turn a "column not found" into the precise "correlated" diagnosis.
    /// Empty for every top-level statement (no enclosing scope).
    correlated_names: Vec<String>,
    /// Table/alias QUALIFIERS exposed by ENCLOSING scopes. A qualified ref
    /// `q.col` whose `q` matches one of these is a correlated reference
    /// (unsupported); a `q` matching NOTHING — this scope or any enclosing —
    /// is a typo, always the plain "not in FROM scope" error. Keyed on the
    /// QUALIFIER (not the column name), so `outer_col`-shaped typos aren't
    /// misdiagnosed as correlation. Empty for every top-level statement.
    correlated_qualifiers: Vec<String>,
}

/// Statement-level accumulator threaded through predicate/comparison binding
/// so an uncorrelated subquery (scalar, IN, or EXISTS) in a WHERE / HAVING /
/// ON position collects its already-bound inner plan into the right list and
/// yields the matching IR leaf (`SubqueryResult` for scalar, `InSubquery` for
/// IN/EXISTS). `binder` recurses into the inner query; `depth` bounds that
/// recursion alongside `MAX_DERIVED_DEPTH`.
struct SubqueryCtx<'a, E: StorageEngine> {
    binder: &'a Binder<E>,
    depth: usize,
    scalar_subqueries: Vec<LogicalPlan>,
    in_subqueries: Vec<LogicalPlan>,
}

impl<'a, E: StorageEngine> SubqueryCtx<'a, E> {
    fn new(binder: &'a Binder<E>, depth: usize) -> Self {
        Self {
            binder,
            depth,
            scalar_subqueries: Vec::new(),
            in_subqueries: Vec::new(),
        }
    }
}

struct ScopedTable {
    /// Original catalog table name (used for qualified refs like `t.col`
    /// when no alias is given). For a derived table this is its alias — the
    /// alias IS the relation's name (there is no catalog name).
    table_name: String,
    /// FROM-clause alias, if any. Qualified refs prefer alias over name.
    alias: Option<String>,
    schema: Arc<Schema>,
    /// Start index of this table's columns in the joined tuple.
    column_offset: usize,
}

/// The result of binding a SELECT body: the plan plus the ingredients to
/// type its output as a derived table's schema. Top-level queries use only
/// `plan`; a derived table computes its output columns from `input` +
/// `output_aliases` (so the schema-typing pass — which can error on
/// uninferable aggregate/computed types — runs ONLY for FROM-subqueries,
/// leaving top-level typing where it was: at executor build time).
struct BoundSelect {
    plan: LogicalPlan,
    /// The join-tuple input schema (the FROM relations concatenated) — the
    /// coordinate space `select_output_schema` types the output against.
    input: Schema,
    /// Per-output-column `AS` alias, positionally aligned with the output
    /// columns (`Some` overrides the derived-schema default name; `None`
    /// keeps it). Empty for `SELECT *` (no items to name).
    output_aliases: Vec<Option<String>>,
}

impl BoundSelect {
    /// The output columns this SELECT presents as a derived table: types
    /// from `select_output_schema`, names overridden by any explicit per-item
    /// `AS` alias. The column-list alias (`AS d (a, b)`), when present, is a
    /// further positional override applied by the caller.
    fn derived_columns(&self) -> Result<Vec<ColumnDef>> {
        let LogicalPlan::Select {
            projection,
            aggregates,
            select_list,
            ..
        } = &self.plan
        else {
            unreachable!("bind_select_query always yields LogicalPlan::Select");
        };
        let mut columns = select_output_schema(&self.input, projection, aggregates, select_list)?;
        for (column, alias) in columns.iter_mut().zip(&self.output_aliases) {
            if let Some(name) = alias {
                column.name = name.clone();
            }
        }
        Ok(columns)
    }
}

impl Scope {
    /// Single-table scope. Identical resolution to pre-P13.1 binder.
    fn single(table_name: String, schema: Arc<Schema>) -> Self {
        Self {
            tables: vec![ScopedTable {
                table_name,
                alias: None,
                schema,
                column_offset: 0,
            }],
            correlated_names: Vec::new(),
            correlated_qualifiers: Vec::new(),
        }
    }

    /// Every column name this scope can RESOLVE (its own tables' columns),
    /// plus the names it inherited from enclosing scopes — the set a nested
    /// subquery consults to distinguish a genuine unknown column from a
    /// correlated outer reference.
    fn resolvable_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .tables
            .iter()
            .flat_map(|t| t.schema.columns.iter().map(|c| c.name.clone()))
            .collect();
        names.extend(self.correlated_names.iter().cloned());
        names
    }

    /// Every table/alias QUALIFIER a nested subquery could see in enclosing
    /// scopes: this scope's own exposed relation names (its alias if aliased,
    /// else its table name) plus the qualifiers it inherited. A subquery
    /// consults this to tell a correlated `outer_tbl.col` reference
    /// (unsupported) from a simple typo'd qualifier ("not in FROM scope").
    fn resolvable_qualifiers(&self) -> Vec<String> {
        let mut quals: Vec<String> = self
            .tables
            .iter()
            .map(|t| t.alias.clone().unwrap_or_else(|| t.table_name.clone()))
            .collect();
        quals.extend(self.correlated_qualifiers.iter().cloned());
        quals
    }

    /// Append another table to the scope. The new table's
    /// `column_offset` is the sum of all prior tables' column counts.
    ///
    /// Rejects a relation whose *exposed* name (its alias if aliased, else its
    /// name) duplicates one already in this statement's scope. Two relations
    /// sharing an exposed name make every qualified ref resolve to whichever
    /// the resolver hits first — silent wrong results: `SELECT x, y FROM
    /// (SELECT 1 AS x) d, (SELECT 2 AS y) d` returned the second subquery's row
    /// for both columns. SQL requires each FROM item be uniquely named; this is
    /// the one point every relation (catalog table, aliased table, or derived
    /// alias) enters the scope, so enforce it here. Catalog-SHADOWING by a
    /// single derived alias stays legal — that is one relation named `t`, not
    /// two — because it is only ever one push into an otherwise-clear scope.
    ///
    /// NOTE: this also closes a pre-H4a latent bug. Before derived tables, a
    /// duplicate catalog-table alias (`FROM t AS a, u AS a`) was likewise never
    /// rejected — `column_index_qualified` returns the first match, so `a.col`
    /// silently resolved to `t`. The exposed-name check catches that case too;
    /// it was not introduced by H4a but is fixed by the same guard.
    fn push(
        &mut self,
        table_name: String,
        alias: Option<String>,
        schema: Arc<Schema>,
    ) -> Result<()> {
        let exposed = alias.as_deref().unwrap_or(&table_name);
        for existing in &self.tables {
            let existing_exposed = existing.alias.as_deref().unwrap_or(&existing.table_name);
            if existing_exposed == exposed {
                return Err(Error::SqlParse(format!(
                    "binder: relation name '{}' specified more than once in FROM",
                    exposed
                )));
            }
        }
        let column_offset: usize = self.tables.iter().map(|t| t.schema.columns.len()).sum();
        self.tables.push(ScopedTable {
            table_name,
            alias,
            schema,
            column_offset,
        });
        Ok(())
    }
}

impl<E: StorageEngine> Binder<E> {
    pub fn new(catalog: Arc<Catalog<E>>) -> Self {
        Self { catalog }
    }

    pub fn bind(&self, stmt: Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::CreateTable(ct) => self.bind_create_table(ct),
            Statement::CreateIndex(ci) => self.bind_create_index(ci),
            Statement::Query(q) => self.bind_query(*q),
            Statement::Insert(ins) => self.bind_insert(ins),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.bind_update(table, assignments, selection),
            Statement::Delete(del) => self.bind_delete(del),
            Statement::StartTransaction { .. } => Ok(LogicalPlan::BeginTxn),
            Statement::Commit { .. } => Ok(LogicalPlan::CommitTxn),
            Statement::Rollback { .. } => Ok(LogicalPlan::AbortTxn),
            Statement::Explain { statement, .. } => {
                let inner = self.bind(*statement)?;
                Ok(LogicalPlan::Explain(Box::new(inner)))
            }
            Statement::Analyze { table_name, .. } => {
                // We only care about the table name; partitions / for_columns
                // / cache_metadata / noscan / compute_statistics options
                // are silently accepted (Phase 14 always does full table-scan
                // + all-columns analysis).
                let name = object_name_to_string(&table_name);
                // Validate the table exists at bind time so the SQL surface
                // returns a clean error rather than dispatching an executor
                // that immediately fails.
                self.catalog.get_table(&name)?;
                Ok(LogicalPlan::Analyze { table: name })
            }
            other => Err(Error::SqlParse(format!(
                "binder: unsupported statement: {}",
                other
            ))),
        }
    }

    // -----------------------------------------------------------------------
    // CREATE TABLE
    // -----------------------------------------------------------------------

    /// `CREATE [UNIQUE] INDEX name ON table [USING btree|lsm] (col, …)`.
    /// Options we don't implement (CONCURRENTLY, IF NOT EXISTS, INCLUDE,
    /// partial WHERE, per-column DESC / NULLS ordering) are rejected
    /// loudly — silently ignoring index options would corrupt the user's
    /// expectations about what the index provides.
    fn bind_create_index(&self, ci: ast::CreateIndex) -> Result<LogicalPlan> {
        let name = match &ci.name {
            Some(n) => object_name_to_string(n),
            None => {
                return Err(Error::SqlParse(
                    "CREATE INDEX requires an index name".into(),
                ))
            }
        };
        if ci.concurrently || ci.if_not_exists || !ci.include.is_empty() {
            return Err(Error::SqlParse(
                "unsupported CREATE INDEX option (CONCURRENTLY / IF NOT EXISTS / INCLUDE)".into(),
            ));
        }
        if ci.nulls_distinct.is_some() || ci.predicate.is_some() {
            return Err(Error::SqlParse(
                "unsupported CREATE INDEX option (NULLS DISTINCT / partial WHERE)".into(),
            ));
        }
        let backend = match &ci.using {
            None => IndexBackend::BTree,
            Some(id) => match id.value.to_ascii_lowercase().as_str() {
                "btree" => IndexBackend::BTree,
                "lsm" => IndexBackend::Lsm,
                other => {
                    return Err(Error::SqlParse(format!(
                        "unknown index backend `{other}`; expected btree or lsm"
                    )))
                }
            },
        };

        let table = object_name_to_string(&ci.table_name);
        let schema = self.catalog.get_table(&table)?;
        let mut columns = Vec::with_capacity(ci.columns.len());
        for obe in &ci.columns {
            if obe.asc == Some(false) || obe.nulls_first.is_some() {
                return Err(Error::SqlParse(
                    "index columns support neither DESC nor NULLS ordering".into(),
                ));
            }
            let AstExpr::Identifier(ident) = &obe.expr else {
                return Err(Error::SqlParse(
                    "index columns must be plain column names".into(),
                ));
            };
            let idx = schema
                .columns
                .iter()
                .position(|c| c.name == ident.value)
                .ok_or_else(|| {
                    Error::SqlParse(format!(
                        "unknown column `{}` in CREATE INDEX on `{table}`",
                        ident.value
                    ))
                })?;
            columns.push(idx);
        }
        if columns.is_empty() {
            return Err(Error::SqlParse(
                "CREATE INDEX requires at least one column".into(),
            ));
        }

        Ok(LogicalPlan::CreateIndex {
            name,
            table,
            columns,
            unique: ci.unique,
            backend,
        })
    }

    fn bind_create_table(&self, ct: ast::CreateTable) -> Result<LogicalPlan> {
        let name = object_name_to_string(&ct.name);
        let mut columns: Vec<ColumnDef> = Vec::with_capacity(ct.columns.len());
        let mut inline_pk: Option<Vec<usize>> = None;

        for (idx, col) in ct.columns.iter().enumerate() {
            let coldef = self.bind_column_def(col)?;
            // Detect inline `PRIMARY KEY` column option.
            let is_inline_pk = col.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                )
            });
            if is_inline_pk {
                if inline_pk.is_some() {
                    return Err(Error::SqlParse(
                        "binder: multiple inline PRIMARY KEY columns".into(),
                    ));
                }
                inline_pk = Some(vec![idx]);
            }
            columns.push(coldef);
        }

        // Table-level PRIMARY KEY (...) constraint.
        let mut table_pk: Option<Vec<usize>> = None;
        for c in &ct.constraints {
            if let TableConstraint::PrimaryKey {
                columns: pk_cols, ..
            } = c
            {
                let mut indices = Vec::with_capacity(pk_cols.len());
                for name in pk_cols {
                    let pos = columns
                        .iter()
                        .position(|cd| cd.name == name.value)
                        .ok_or_else(|| {
                            Error::SqlParse(format!(
                                "PRIMARY KEY references unknown column '{}'",
                                name.value
                            ))
                        })?;
                    indices.push(pos);
                }
                table_pk = Some(indices);
                break;
            }
        }

        let primary_key = match (inline_pk, table_pk) {
            (Some(_), Some(_)) => {
                return Err(Error::SqlParse(
                    "binder: both inline and table-level PRIMARY KEY".into(),
                ))
            }
            (Some(pk), None) | (None, Some(pk)) => pk,
            (None, None) => {
                return Err(Error::SqlParse(format!(
                    "table '{}' must declare a PRIMARY KEY",
                    name
                )))
            }
        };

        // Force PK columns non-nullable. Inline PK already handled this in
        // `bind_column_def`; table-level `PRIMARY KEY (col)` doesn't touch
        // the column's options, so we patch here. Catalog rejects nullable
        // PK columns at create_table.
        for &i in &primary_key {
            columns[i].nullable = false;
        }

        Ok(LogicalPlan::CreateTable {
            name,
            columns,
            primary_key,
        })
    }

    fn bind_column_def(&self, c: &AstColumnDef) -> Result<ColumnDef> {
        let ty = map_data_type(&c.data_type)?;
        // ANSI SQL: inline PRIMARY KEY implies NOT NULL. Our catalog
        // enforces non-nullable PK columns, so collapse both cases here.
        let nullable = !c.options.iter().any(|o| {
            matches!(
                o.option,
                ColumnOption::NotNull
                    | ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
            )
        });
        Ok(ColumnDef {
            name: c.name.value.clone(),
            ty,
            nullable,
            default: None,
        })
    }

    // -----------------------------------------------------------------------
    // SELECT (Query)
    // -----------------------------------------------------------------------

    fn bind_query(&self, q: Query) -> Result<LogicalPlan> {
        // Top-level: bind at depth 0 and keep only the plan. The derived-table
        // schema ingredients (`input` / `output_aliases`) are discarded, so a
        // top-level query never runs the output-typing pass — its type errors
        // stay at executor build time, exactly as before.
        Ok(self.bind_select_query(q, 0, Vec::new(), Vec::new())?.plan)
    }

    /// Bind a SELECT body, returning the plan plus what a derived table needs
    /// to type its output schema. `depth` is the FROM-subquery / subquery
    /// nesting level (0 = top level); each derived table or subquery recurses
    /// at `depth + 1`, bounded by `MAX_DERIVED_DEPTH`. `correlated_names` /
    /// `correlated_qualifiers` are the column names / table-alias qualifiers
    /// resolvable in ENCLOSING scopes — non-empty only when this is a subquery,
    /// and used purely to diagnose a correlated reference (this phase binds
    /// uncorrelated subqueries only).
    fn bind_select_query(
        &self,
        q: Query,
        depth: usize,
        correlated_names: Vec<String>,
        correlated_qualifiers: Vec<String>,
    ) -> Result<BoundSelect> {
        let limit = match q.limit {
            Some(AstExpr::Value(AstValue::Number(n, _))) => {
                Some(n.parse::<usize>().map_err(|e| {
                    Error::SqlParse(format!("LIMIT must be a non-negative integer: {}", e))
                })?)
            }
            Some(other) => {
                return Err(Error::SqlParse(format!(
                    "binder: only literal integer LIMIT supported, got {:?}",
                    other
                )))
            }
            None => None,
        };

        let select = match *q.body {
            SetExpr::Select(s) => s,
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: only SELECT supported, got {:?}",
                    other
                )))
            }
        };

        // Silently dropping a clause returns confidently wrong rows —
        // every unbound clause must reject loudly.
        if select.distinct.is_some() {
            return Err(Error::SqlParse(
                "binder: SELECT DISTINCT not supported".into(),
            ));
        }

        if select.from.is_empty() {
            return Err(Error::SqlParse("binder: SELECT needs a FROM clause".into()));
        }

        // FROM clause. Two forms produce multi-table queries:
        //   - Implicit cross join: `FROM a, b, c` → from.len() > 1, joins empty.
        //   - Explicit join: `FROM a JOIN b ON …` → from.len() == 1, joins
        //     attached to that entry.
        // We support both, plus mixtures (`FROM a, b JOIN c ON …`).
        //
        // Build a scope incrementally + a parallel `JoinClause` list. The
        // first table becomes `LogicalPlan::Select::table`; the rest become
        // entries in `joins`.
        let mut scope = Scope {
            tables: Vec::new(),
            correlated_names,
            correlated_qualifiers,
        };
        let mut joined_tables: Vec<(String, Option<String>, Option<AstExpr>, JoinKind)> =
            Vec::new();
        // Subquery accumulator for this SELECT's WHERE / HAVING / ON positions.
        let mut ctx = SubqueryCtx::new(self, depth);
        // FROM-subqueries collected as we resolve the FROM factors (H4a).
        let mut derived: Vec<DerivedTable> = Vec::new();

        for (twj_idx, twj) in select.from.into_iter().enumerate() {
            // The "head" relation of this TableWithJoins — a catalog table or a
            // derived table (FROM-subquery).
            let (head_name, head_alias, head_schema) =
                self.resolve_from_factor(twj.relation, depth, &mut derived)?;
            if twj_idx == 0 {
                scope.push(head_name.clone(), head_alias.clone(), head_schema)?;
            } else {
                // Implicit cross join with the running scope.
                scope.push(head_name.clone(), head_alias.clone(), head_schema)?;
                joined_tables.push((head_name, head_alias, None, JoinKind::Inner));
            }

            // Explicit joins attached to this entry.
            for j in twj.joins {
                let (right_name, right_alias, right_schema) =
                    self.resolve_from_factor(j.relation, depth, &mut derived)?;
                // The ON predicate (when present) must bind against a scope
                // that already includes the right side, so push first.
                scope.push(right_name.clone(), right_alias.clone(), right_schema)?;

                // R1 (H3b): a LEFT OUTER join's ON filters the *match*, so it
                // must carry an ON — an outer join without one can never leave
                // a row unmatched and is meaningless; reject it loudly. RIGHT
                // and FULL OUTER are named rejections (unsupported, not silent
                // mis-plans).
                let (kind, on_expr) = match j.join_operator {
                    ast::JoinOperator::Inner(ast::JoinConstraint::On(e)) => {
                        (JoinKind::Inner, Some(e))
                    }
                    ast::JoinOperator::Inner(ast::JoinConstraint::None) => (JoinKind::Inner, None),
                    ast::JoinOperator::CrossJoin => (JoinKind::Inner, None),
                    ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(e)) => {
                        (JoinKind::LeftOuter, Some(e))
                    }
                    ast::JoinOperator::LeftOuter(_) => {
                        return Err(Error::SqlParse(
                            "binder: LEFT OUTER JOIN requires an ON clause".into(),
                        ))
                    }
                    ast::JoinOperator::RightOuter(_) => {
                        return Err(Error::SqlParse(
                            "binder: RIGHT OUTER JOIN not supported".into(),
                        ))
                    }
                    ast::JoinOperator::FullOuter(_) => {
                        return Err(Error::SqlParse(
                            "binder: FULL OUTER JOIN not supported".into(),
                        ))
                    }
                    other => {
                        return Err(Error::SqlParse(format!(
                            "binder: only INNER, CROSS, and LEFT OUTER JOIN supported, got {:?}",
                            other
                        )))
                    }
                };
                joined_tables.push((right_name, right_alias, on_expr, kind));
            }
        }

        // The first scoped table becomes the LogicalPlan's `table`.
        let table_name = scope.tables[0].table_name.clone();

        // Bind each join's ON predicate against the scope built up to
        // that join's position. Easiest: bind against the *final* scope
        // — predicates over earlier tables still work, and over later
        // tables that haven't been joined yet would be a forward
        // reference (unusual, the binder rejects via column-not-found).
        let mut joins: Vec<crate::sql::ir::logical::JoinClause> =
            Vec::with_capacity(joined_tables.len());
        for (right_table, right_alias, on_expr, kind) in joined_tables {
            let on = match on_expr {
                Some(e) => Some(bind_predicate(&scope, e, &mut ctx)?),
                None => None,
            };
            joins.push(crate::sql::ir::logical::JoinClause {
                right_table,
                right_alias,
                on,
                kind,
            });
        }

        // GROUP BY clause → input-space plain columns. H1 scope:
        // expressions and Snowflake-style ALL reject loudly. Parsed BEFORE
        // the SELECT items because whether the query is aggregated (which
        // fixes each item's coordinate space) depends on the GROUP BY
        // clause as well as on aggregate calls in the list.
        let group_cols: Vec<usize> = match &select.group_by {
            ast::GroupByExpr::Expressions(exprs, modifiers) => {
                if !modifiers.is_empty() {
                    return Err(Error::SqlParse(
                        "binder: GROUP BY modifiers (ROLLUP/CUBE/…) not supported".into(),
                    ));
                }
                exprs
                    .iter()
                    .map(|e| resolve_column_expr(&scope, e))
                    .collect::<Result<Vec<_>>>()?
            }
            ast::GroupByExpr::All(_) => {
                return Err(Error::SqlParse("binder: GROUP BY ALL not supported".into()))
            }
        };

        // Projection + aggregates + display list + captured output-column
        // aliases. Empty projection/aggregates = SELECT *; empty select_list =
        // identity display (the zero-churn invariant — see LogicalPlan::Select).
        // `output_aliases` (H4a step 1) names the derived-table schema when
        // this SELECT is a FROM-subquery; it is positionally aligned with the
        // output columns and ignored for top-level queries.
        let (projection, mut aggregates, mut select_list, output_aliases) =
            bind_select_items(&scope, &select.projection, &group_cols, self)?;

        // A query is aggregated iff it groups OR the SELECT list already
        // contains an aggregate. HAVING / ORDER BY may reference an aggregate
        // that is NOT in the SELECT list (Q18's `HAVING SUM(l_quantity) > n`
        // over a subquery projecting only `l_orderkey`); those aggregates are
        // COMPUTED (appended below) and PROJECTED OUT of the display. Remember
        // how many aggregates are display aggregates so the trailing
        // computed-only ones can be dropped.
        let is_aggregated = !group_cols.is_empty() || !aggregates.is_empty();
        let display_agg_count = aggregates.len();

        // Grouping rules. The IR carries group keys IN `projection`
        // (LogicalPlan::Select's contract), so the GROUP BY clause and the
        // projected plain columns must name the same set — each direction
        // violated gets its own blame. The "GROUP BY needs an aggregate"
        // check is deferred until after HAVING/ORDER BY (either may supply the
        // sole aggregate).
        if group_cols.is_empty() {
            if !projection.is_empty() && !aggregates.is_empty() {
                return Err(Error::SqlParse(
                    "binder: mixed column-and-aggregate projection requires GROUP BY".into(),
                ));
            }
        } else {
            for &p in &projection {
                if !group_cols.contains(&p) {
                    return Err(Error::SqlParse(format!(
                        "binder: column '{}' must appear in GROUP BY",
                        column_name_at(&scope, p)
                    )));
                }
            }
            for &g in &group_cols {
                if !projection.contains(&g) {
                    return Err(Error::SqlParse(format!(
                        "binder: GROUP BY column '{}' must appear in the SELECT list",
                        column_name_at(&scope, g)
                    )));
                }
            }
        }

        // WHERE clause.
        let filter = match select.selection {
            Some(e) => Some(bind_predicate(&scope, e, &mut ctx)?),
            None => None,
        };

        // HAVING filters the aggregate output row — legal only for an
        // aggregated query (GROUP BY or a whole-table aggregate). A HAVING
        // aggregate absent from the SELECT list is appended to `aggregates`
        // (computed, then projected out).
        let having = match select.having {
            Some(e) => {
                if !is_aggregated {
                    return Err(Error::SqlParse(
                        "binder: HAVING requires an aggregate in the SELECT list".into(),
                    ));
                }
                Some(bind_having_predicate(
                    &scope,
                    e,
                    &projection,
                    &mut aggregates,
                    &mut ctx,
                )?)
            }
            None => None,
        };

        // P13.6: ORDER BY. sqlparser exposes `Option<OrderBy>` on Query;
        // each `OrderByExpr` has the expression + an optional ASC/DESC
        // flag (None defaults to ASC). Coordinate rule: without
        // aggregates, keys are input-space columns; with aggregates the
        // Sort runs above the HashAggregate, so keys resolve into its
        // output row (group key position, or SELECT-list aggregate).
        let order_by = match q.order_by {
            Some(ob) => {
                let mut keys: Vec<(usize, OrderDir)> = Vec::with_capacity(ob.exprs.len());
                for obe in ob.exprs {
                    let col = if !is_aggregated {
                        resolve_column_expr(&scope, &obe.expr)?
                    } else {
                        resolve_aggregate_output_column(
                            &scope,
                            &obe.expr,
                            &projection,
                            &mut aggregates,
                            self,
                        )?
                    };
                    let dir = match obe.asc {
                        Some(false) => OrderDir::Desc,
                        _ => OrderDir::Asc, // None defaults to ASC per SQL spec
                    };
                    keys.push((col, dir));
                }
                keys
            }
            None => Vec::new(),
        };

        // Deferred grouping check: a GROUP BY that has produced no aggregate
        // anywhere (SELECT list, HAVING, or ORDER BY) is meaningless.
        if !group_cols.is_empty() && aggregates.is_empty() {
            return Err(Error::SqlParse(
                "binder: GROUP BY without aggregates not supported (did you mean DISTINCT?)".into(),
            ));
        }

        // If HAVING / ORDER BY appended aggregates the SELECT list does not
        // display, make the display explicit so those trailing aggregates are
        // projected out. An already-explicit `select_list` references only
        // display positions (which are unchanged), so leave it alone.
        if is_aggregated && aggregates.len() > display_agg_count && select_list.is_empty() {
            select_list = (0..projection.len() + display_agg_count)
                .map(Expression::Column)
                .collect();
        }

        // The join-tuple input schema: every scoped relation's columns
        // concatenated in tuple-global order. This is the coordinate space the
        // output columns are typed against when this SELECT is a derived table.
        //
        // Join-kind-aware nullability: a LEFT OUTER join pads its right side
        // with NULLs for outer rows that found no match, so those columns are
        // nullable in the joined tuple regardless of their base declaration.
        // This mirrors the executor's `concat_schemas`, which ORs a
        // `right_nullable` flag into each right column of an outer join; the
        // derived schema must agree, or an outer-join-padded column would be
        // reported `nullable: false` through the derived table while the
        // runtime can hand back NULLs there. `scope.tables[0]` is the left
        // relation (never padded); each later `scope.tables[i]` pairs with
        // `joins[i - 1]` in textual order (every push past the first appended
        // exactly one join clause above).
        debug_assert_eq!(
            joins.len(),
            scope.tables.len() - 1,
            "each scoped relation past the first must pair with one join clause"
        );
        let mut input_columns: Vec<ColumnDef> =
            Vec::with_capacity(scope.tables.iter().map(|t| t.schema.columns.len()).sum());
        for (i, scoped) in scope.tables.iter().enumerate() {
            let right_nullable = i > 0 && joins[i - 1].kind == JoinKind::LeftOuter;
            for col in &scoped.schema.columns {
                input_columns.push(ColumnDef {
                    nullable: col.nullable || right_nullable,
                    ..col.clone()
                });
            }
        }
        let input = Schema {
            name: "derived-input".into(),
            table_id: TableId(0),
            columns: input_columns,
            primary_key: vec![],
        };

        let plan = LogicalPlan::Select {
            table: table_name,
            joins,
            derived,
            scalar_subqueries: ctx.scalar_subqueries,
            in_subqueries: ctx.in_subqueries,
            projection,
            aggregates,
            select_list,
            filter,
            order_by,
            having,
            limit,
        };
        Ok(BoundSelect {
            plan,
            input,
            output_aliases,
        })
    }

    /// Bind an uncorrelated subquery's inner query in a FRESH scope, depth-
    /// bounded. `outer.resolvable_names()` travels into the inner scope for
    /// correlation DETECTION only (a reference to one becomes the loud
    /// "correlated subqueries not yet supported" error). Shared by the scalar,
    /// IN, and EXISTS binders below.
    fn bind_subquery_inner(
        &self,
        subquery: Query,
        outer: &Scope,
        depth: usize,
    ) -> Result<BoundSelect> {
        if depth + 1 > MAX_DERIVED_DEPTH {
            return Err(Error::SqlParse(format!(
                "binder: subqueries nested deeper than {} levels not supported",
                MAX_DERIVED_DEPTH
            )));
        }
        self.bind_select_query(
            subquery,
            depth + 1,
            outer.resolvable_names(),
            outer.resolvable_qualifiers(),
        )
    }

    /// Uncorrelated scalar subquery (a compare or HAVING operand). Asserts
    /// exactly one output column; the caller allocates a slot and emits
    /// `Expression::SubqueryResult(slot)`.
    fn bind_scalar_subquery(
        &self,
        subquery: Query,
        outer: &Scope,
        depth: usize,
    ) -> Result<LogicalPlan> {
        let bound = self.bind_subquery_inner(subquery, outer, depth)?;
        let column_count = bound.derived_columns()?.len();
        if column_count != 1 {
            return Err(Error::SqlParse(format!(
                "binder: scalar subquery must return exactly one column, got {}",
                column_count
            )));
        }
        Ok(bound.plan)
    }

    /// Uncorrelated `IN` subquery. Asserts exactly one output column (the
    /// value set the probe matches against); the caller emits
    /// `Predicate::InSubquery`.
    fn bind_in_subquery(
        &self,
        subquery: Query,
        outer: &Scope,
        depth: usize,
    ) -> Result<LogicalPlan> {
        let bound = self.bind_subquery_inner(subquery, outer, depth)?;
        let column_count = bound.derived_columns()?.len();
        if column_count != 1 {
            return Err(Error::SqlParse(format!(
                "binder: IN subquery must return exactly one column, got {}",
                column_count
            )));
        }
        Ok(bound.plan)
    }

    /// Uncorrelated `EXISTS` subquery. No output-arity constraint — EXISTS
    /// only observes whether ANY row survives — so cap the inner plan at one
    /// row, letting materialization early-exit at the first row.
    fn bind_exists_subquery(
        &self,
        subquery: Query,
        outer: &Scope,
        depth: usize,
    ) -> Result<LogicalPlan> {
        let bound = self.bind_subquery_inner(subquery, outer, depth)?;
        let mut plan = bound.plan;
        if let LogicalPlan::Select { limit, .. } = &mut plan {
            *limit = Some(match *limit {
                Some(existing) => existing.min(1),
                None => 1,
            });
        }
        Ok(plan)
    }

    /// Resolve one FROM `TableFactor` into a scope entry
    /// `(scope_name, scope_alias, schema)`. A catalog table resolves against
    /// the catalog. A derived table (FROM-subquery, H4a) is bound recursively
    /// here in a FRESH scope (uncorrelated), its output schema computed, and
    /// its `(alias, plan, schema)` recorded in `derived`; the ALIAS becomes the
    /// scope name (there is no catalog name), so the outer query refers to it
    /// by that alias and it shadows any catalog table of the same name.
    fn resolve_from_factor(
        &self,
        factor: TableFactor,
        depth: usize,
        derived: &mut Vec<DerivedTable>,
    ) -> Result<(String, Option<String>, Arc<Schema>)> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = object_name_to_string(&name);
                let alias = alias.as_ref().map(|a| a.name.value.clone());
                let schema = self.catalog.get_table(&table_name)?;
                Ok((table_name, alias, schema))
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                // LATERAL is correlated (the subquery may reference columns of
                // earlier FROM items) — out of scope for uncorrelated H4a.
                if lateral {
                    return Err(Error::SqlParse(
                        "binder: LATERAL derived tables not supported".into(),
                    ));
                }
                // A derived table with no alias has no name to reference its
                // columns by — required, rejected loudly.
                let alias = alias.ok_or_else(|| {
                    Error::SqlParse(
                        "binder: derived table (FROM-subquery) requires an alias".into(),
                    )
                })?;
                // Depth bound: the inner query sits one level deeper. Reject
                // before recursing so nesting can't blow the bind stack.
                if depth + 1 > MAX_DERIVED_DEPTH {
                    return Err(Error::SqlParse(format!(
                        "binder: derived tables nested deeper than {} levels not supported",
                        MAX_DERIVED_DEPTH
                    )));
                }

                // Bind the inner query in a fresh scope (uncorrelated, no
                // enclosing names) and compute its output columns (types +
                // per-item `AS` names).
                let bound = self.bind_select_query(*subquery, depth + 1, Vec::new(), Vec::new())?;
                // A derived table's inner query is materialized at executor
                // build time (a `DerivedScan` leaf), NOT through the session's
                // subquery-resolution pass — so a scalar/IN/EXISTS subquery
                // INSIDE it would never have its parameter/set resolved. Reject
                // loudly rather than silently mis-evaluate (recorded lever:
                // subqueries inside a derived table).
                if let LogicalPlan::Select {
                    scalar_subqueries,
                    in_subqueries,
                    ..
                } = &bound.plan
                {
                    if !scalar_subqueries.is_empty() || !in_subqueries.is_empty() {
                        return Err(Error::SqlParse(
                            "binder: subqueries inside a derived table (FROM-subquery) are not \
                             supported yet"
                                .into(),
                        ));
                    }
                }
                let mut columns = bound.derived_columns()?;

                // Column-list alias `AS d (c1, c2, …)` (Q13 verbatim): a
                // positional rename of EVERY output column — the count must
                // match exactly, or it is a loud error, never a silent partial.
                if !alias.columns.is_empty() {
                    if alias.columns.len() != columns.len() {
                        return Err(Error::SqlParse(format!(
                            "binder: derived table '{}' column list has {} names but the \
                             subquery produces {} columns",
                            alias.name.value,
                            alias.columns.len(),
                            columns.len()
                        )));
                    }
                    for (column, ident) in columns.iter_mut().zip(&alias.columns) {
                        column.name = ident.value.clone();
                    }
                }

                let alias_name = alias.name.value.clone();
                let schema = Arc::new(Schema {
                    name: alias_name.clone(),
                    table_id: TableId(0),
                    columns: columns.clone(),
                    primary_key: vec![],
                });
                derived.push(DerivedTable {
                    alias: alias_name.clone(),
                    plan: Box::new(bound.plan),
                    schema: columns,
                });
                // A derived table's alias IS its relation name (no catalog
                // name), so `scope_alias` is None — qualified refs match on the
                // scope name.
                Ok((alias_name, None, schema))
            }
            other => Err(Error::SqlParse(format!(
                "binder: FROM/JOIN must be a named table or derived table, got {:?}",
                other
            ))),
        }
    }

    // -----------------------------------------------------------------------
    // INSERT
    // -----------------------------------------------------------------------

    fn bind_insert(&self, ins: ast::Insert) -> Result<LogicalPlan> {
        let table_name = object_name_to_string(&ins.table_name);
        let schema = self.catalog.get_table(&table_name)?;

        let source = ins
            .source
            .ok_or_else(|| Error::SqlParse("binder: INSERT without source not supported".into()))?;
        let values = match *source.body {
            SetExpr::Values(Values { rows, .. }) => rows,
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: INSERT … SELECT is Phase 12; got {:?}",
                    other
                )))
            }
        };

        // Column list. INSERT INTO t VALUES … → schema order;
        // INSERT INTO t (a, b) VALUES … → those columns in that order.
        let col_indices: Vec<usize> = if ins.columns.is_empty() {
            (0..schema.columns.len()).collect()
        } else {
            ins.columns
                .iter()
                .map(|ident| {
                    schema
                        .columns
                        .iter()
                        .position(|c| c.name == ident.value)
                        .ok_or_else(|| {
                            Error::SqlParse(format!(
                                "column '{}' not found in table '{}'",
                                ident.value, table_name
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?
        };

        let mut rows: Vec<Vec<Expression>> = Vec::with_capacity(values.len());
        for row in values {
            if row.len() != col_indices.len() {
                return Err(Error::SqlParse(format!(
                    "INSERT row arity {} doesn't match column list arity {}",
                    row.len(),
                    col_indices.len()
                )));
            }
            // Build a full-width row of Expressions; fill specified columns,
            // leave others as Literal(Null) (column list semantics).
            let mut full: Vec<Expression> = (0..schema.columns.len())
                .map(|_| Expression::Literal(Value::Null))
                .collect();
            for (src_pos, expr) in row.into_iter().enumerate() {
                let dst = col_indices[src_pos];
                full[dst] = insert_value_expr(expr, &schema.columns[dst].ty)?;
            }
            rows.push(full);
        }

        Ok(LogicalPlan::Insert {
            table: table_name,
            rows,
        })
    }

    // -----------------------------------------------------------------------
    // UPDATE
    // -----------------------------------------------------------------------

    fn bind_update(
        &self,
        table: ast::TableWithJoins,
        assignments: Vec<ast::Assignment>,
        selection: Option<AstExpr>,
    ) -> Result<LogicalPlan> {
        if !table.joins.is_empty() {
            return Err(Error::SqlParse(
                "binder: UPDATE with JOIN is Phase 12".into(),
            ));
        }
        let table_name = match table.relation {
            TableFactor::Table { name, .. } => object_name_to_string(&name),
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: UPDATE target must be a named table, got {:?}",
                    other
                )))
            }
        };
        let schema = self.catalog.get_table(&table_name)?;
        let scope = Scope::single(table_name.clone(), schema.clone());

        let mut set_clauses: Vec<(usize, Expression)> = Vec::with_capacity(assignments.len());
        for a in assignments {
            // sqlparser 0.50: `target: AssignmentTarget`. We only support
            // single-column targets in Phase 11.
            let col_name = match a.target {
                ast::AssignmentTarget::ColumnName(ObjectName(parts)) => {
                    if parts.len() != 1 {
                        return Err(Error::SqlParse(
                            "binder: qualified SET targets unsupported".into(),
                        ));
                    }
                    parts.into_iter().next().unwrap().value
                }
                other => {
                    return Err(Error::SqlParse(format!(
                        "binder: SET target shape unsupported: {:?}",
                        other
                    )))
                }
            };
            let idx = column_index(&scope, &col_name)?;
            // Single-table scope in UPDATE — table[0] is the target.
            let target_ty = scope.tables[0].schema.columns[idx].ty;
            let expr = bind_expression(&scope, a.value, self)?;
            // The SET result must match the target column's type — Table's
            // update_columns rejects type mismatch outright. Narrow Int64
            // literals (the unconstrained-default for whole numbers) down
            // to Int32 when the target is Int32, recursively through any
            // BinaryOp so `col = col + 1` works on Int32 columns too.
            let expr = narrow_expression(expr, &target_ty);
            set_clauses.push((idx, expr));
        }

        // UPDATE has no place to carry subquery plans (its LogicalPlan has no
        // subquery lists) and no session resolution path, so a WHERE subquery
        // is rejected via `reject_dml_subqueries` after binding.
        let mut ctx = SubqueryCtx::new(self, 0);
        let filter = match selection {
            Some(e) => Some(bind_predicate(&scope, e, &mut ctx)?),
            None => None,
        };
        reject_unsupported_subqueries(&ctx, "an UPDATE WHERE clause")?;

        Ok(LogicalPlan::Update {
            table: table_name,
            set_clauses,
            filter,
        })
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    fn bind_delete(&self, del: ast::Delete) -> Result<LogicalPlan> {
        // sqlparser 0.50 Delete.from is FromTable, which contains the table list.
        let tables = match del.from {
            ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
        };
        if tables.len() != 1 {
            return Err(Error::SqlParse(
                "binder: DELETE supports exactly one table".into(),
            ));
        }
        let twj = &tables[0];
        if !twj.joins.is_empty() {
            return Err(Error::SqlParse(
                "binder: DELETE with JOIN is Phase 12".into(),
            ));
        }
        let table_name = match &twj.relation {
            TableFactor::Table { name, .. } => object_name_to_string(name),
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: DELETE target must be a named table, got {:?}",
                    other
                )))
            }
        };
        let schema = self.catalog.get_table(&table_name)?;
        let scope = Scope::single(table_name.clone(), schema);

        let mut ctx = SubqueryCtx::new(self, 0);
        let filter = match del.selection {
            Some(e) => Some(bind_predicate(&scope, e, &mut ctx)?),
            None => None,
        };
        reject_unsupported_subqueries(&ctx, "a DELETE WHERE clause")?;

        Ok(LogicalPlan::Delete {
            table: table_name,
            filter,
        })
    }
}

// ===========================================================================
// Free helpers
// ===========================================================================

fn object_name_to_string(n: &ObjectName) -> String {
    n.0.iter()
        .map(|i| i.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

/// Resolve an unqualified column name to a tuple-global index. Errors
/// on "not found" and "ambiguous" (column present in multiple scoped
/// tables — caller must use the qualified `t.col` form to disambiguate).
fn column_index(scope: &Scope, name: &str) -> Result<usize> {
    let mut hits: Vec<usize> = Vec::new();
    for t in &scope.tables {
        if let Some(local) = t.schema.columns.iter().position(|c| c.name == name) {
            hits.push(t.column_offset + local);
        }
    }
    match hits.len() {
        0 => {
            // A miss that the ENCLOSING scope could resolve is a correlated
            // reference — unsupported this phase. Name the column so the
            // limitation is actionable (H4b handles uncorrelated forms only).
            if scope.correlated_names.iter().any(|n| n == name) {
                return Err(Error::SqlParse(format!(
                    "correlated subqueries not yet supported: column '{}' refers to an outer query",
                    name
                )));
            }
            let names: Vec<String> = scope.tables.iter().map(|t| t.table_name.clone()).collect();
            Err(Error::SqlParse(format!(
                "column '{}' not found in scope (tables: {:?})",
                name, names
            )))
        }
        1 => Ok(hits[0]),
        _ => Err(Error::SqlParse(format!(
            "column '{}' is ambiguous across joined tables — qualify it (e.g. `t.{}`)",
            name, name
        ))),
    }
}

/// Resolve a qualified column reference (`table_or_alias.col`) to a
/// tuple-global index. Alias matches take precedence over table-name
/// matches (matches SQL's standard scoping rule).
fn column_index_qualified(scope: &Scope, qualifier: &str, name: &str) -> Result<usize> {
    for t in &scope.tables {
        let matches_qualifier = t.alias.as_deref() == Some(qualifier) || t.table_name == qualifier;
        if matches_qualifier {
            let local = t
                .schema
                .columns
                .iter()
                .position(|c| c.name == name)
                .ok_or_else(|| {
                    Error::SqlParse(format!(
                        "column '{}' not found in table '{}'",
                        name, qualifier
                    ))
                })?;
            return Ok(t.column_offset + local);
        }
    }
    // A qualified reference is correlated only when its QUALIFIER resolves to
    // an enclosing scope's table/alias (e.g. `WHERE o.x = (SELECT … WHERE
    // t.y = o.z)` — `o` is the outer relation). Keying on the qualifier, not
    // the column name, is the fix: a typo'd qualifier whose column name happens
    // to match an outer column (`zzz.b` where `b` exists outside) is a plain
    // scope error, never a correlation misdiagnosis.
    if scope.correlated_qualifiers.iter().any(|q| q == qualifier) {
        return Err(Error::SqlParse(format!(
            "correlated subqueries not yet supported: column '{}.{}' refers to an outer query",
            qualifier, name
        )));
    }
    Err(Error::SqlParse(format!(
        "table or alias '{}' not in FROM scope",
        qualifier
    )))
}

/// Bind SELECT items into `(projection, aggregates, select_list)`.
///
/// - `projection` (H1 meaning): grouped → the group keys in
///   first-appearance order; ungrouped bare-only list → the projected
///   columns; ungrouped with any computed item → EMPTY (the `Compute`
///   built from `select_list` replaces the Projection).
/// - `aggregates`: aggregate specs extracted from the items, deduped by
///   `PartialEq`, in first-encounter order.
/// - `select_list` (H2b): the display row — one `Expression` per item, or
///   EMPTY when the display is the identity over its coordinate space (the
///   zero-churn invariant; see `LogicalPlan::Select`). Space follows the
///   aggregate rule: input tuple when unaggregated, aggregate output row
///   (keys ++ aggregates) otherwise.
///
/// A query is *aggregated* iff a GROUP BY clause is present or any item
/// contains an aggregate call. The two regimes bind separately: the
/// aggregated path resolves items into aggregate-output coordinates; the
/// unaggregated path binds items straight against the input scope.
///
/// The fourth return value (H4a step 1) captures each item's explicit `AS`
/// alias — `Some(name)` overrides the derived-table schema's default column
/// name for that item, `None` keeps it. It is positionally aligned with the
/// output columns (one per item, in item order) in every non-wildcard case;
/// empty for `SELECT *`. Top-level queries ignore it (their output naming is
/// unchanged); only derived tables consume it.
fn bind_select_items<E: StorageEngine>(
    scope: &Scope,
    items: &[ast::SelectItem],
    group_cols: &[usize],
    binder: &Binder<E>,
) -> Result<BoundSelectItems> {
    // Single Wildcard → SELECT * (columns keep their catalog names, so no
    // aliases to capture).
    if items.len() == 1 && matches!(items[0], ast::SelectItem::Wildcard(_)) {
        return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new()));
    }

    // Unwrap each item to its expression, capturing its `AS` alias alongside.
    let mut exprs: Vec<&AstExpr> = Vec::with_capacity(items.len());
    let mut aliases: Vec<Option<String>> = Vec::with_capacity(items.len());
    for it in items {
        match it {
            ast::SelectItem::UnnamedExpr(e) => {
                exprs.push(e);
                aliases.push(None);
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                exprs.push(expr);
                aliases.push(Some(alias.value.clone()));
            }
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: projection item shape unsupported: {:?}",
                    other
                )))
            }
        }
    }

    let aggregated = !group_cols.is_empty() || exprs.iter().any(|e| expr_contains_aggregate(e));
    let (projection, aggregates, select_list) = if aggregated {
        bind_select_items_grouped(scope, &exprs, binder)?
    } else {
        bind_select_items_ungrouped(scope, &exprs, binder)?
    };
    Ok((projection, aggregates, select_list, aliases))
}

/// Unaggregated display: bind each item against the input scope. A list of
/// only bare columns keeps today's projection path (a `Projection` selects
/// / reorders them, `select_list` empty — zero churn, including pure
/// reorderings like `SELECT b, a`). Any computed item switches to the
/// `Compute` path: `projection` empty, `select_list` carries every item in
/// input coordinates.
fn bind_select_items_ungrouped<E: StorageEngine>(
    scope: &Scope,
    exprs: &[&AstExpr],
    binder: &Binder<E>,
) -> Result<(Vec<usize>, Vec<AggregateSpec>, Vec<Expression>)> {
    let bound: Vec<Expression> = exprs
        .iter()
        .map(|e| bind_expression(scope, (*e).clone(), binder))
        .collect::<Result<Vec<_>>>()?;
    if bound.iter().all(|e| matches!(e, Expression::Column(_))) {
        let projection = bound
            .iter()
            .map(|e| match e {
                Expression::Column(i) => *i,
                _ => unreachable!("guarded by the all-Column check above"),
            })
            .collect();
        Ok((projection, Vec::new(), Vec::new()))
    } else {
        Ok((Vec::new(), Vec::new(), bound))
    }
}

/// Aggregated display, in two passes. Pass 1 collects the top-level
/// bare-column items — the group keys, first-appearance order — into
/// `projection`; the aggregate output row is exactly these keys followed
/// by the aggregates, so a key's output position is its index in
/// `projection`. Pass 2 binds every item into that output space,
/// extracting aggregate calls into `aggregates` (deduped) and resolving
/// each bare column — wherever it appears — to its key position (a non-key
/// column errors). An identity display (keys then aggregates, in order)
/// elides to an empty `select_list`. Two passes so pass 2's key lookups see
/// the complete key set regardless of display order (`SELECT COUNT(*),
/// region`).
fn bind_select_items_grouped<E: StorageEngine>(
    scope: &Scope,
    exprs: &[&AstExpr],
    binder: &Binder<E>,
) -> Result<(Vec<usize>, Vec<AggregateSpec>, Vec<Expression>)> {
    // Pass 1: group keys = the top-level bare-column items, deduped.
    let mut projection: Vec<usize> = Vec::new();
    for e in exprs {
        if let Some(col) = as_bare_column(scope, e)? {
            if !projection.contains(&col) {
                projection.push(col);
            }
        }
    }
    // Pass 2: bind each item into aggregate-output coordinates.
    let mut aggregates: Vec<AggregateSpec> = Vec::new();
    let mut select_list: Vec<Expression> = Vec::with_capacity(exprs.len());
    for e in exprs {
        select_list.push(bind_output_expr(
            scope,
            e,
            &projection,
            &mut aggregates,
            binder,
        )?);
    }
    if is_identity_select_list(&select_list) {
        select_list.clear();
    }
    Ok((projection, aggregates, select_list))
}

/// The tuple-global column index if `e` is a plain column reference
/// (`Identifier` or two-part `CompoundIdentifier`), else `None`. `Err` only
/// when it *is* a column shape but names an unknown column.
fn as_bare_column(scope: &Scope, e: &AstExpr) -> Result<Option<usize>> {
    match e {
        AstExpr::Identifier(ident) => Ok(Some(column_index(scope, &ident.value)?)),
        AstExpr::CompoundIdentifier(parts) if parts.len() == 2 => Ok(Some(column_index_qualified(
            scope,
            &parts[0].value,
            &parts[1].value,
        )?)),
        // A parenthesized column is still a bare column: unwrap so pass 1
        // registers `(region)` as a group key (matching `bind_output_expr`,
        // `bind_having_predicate`, `bind_expression`, and `bind_predicate`,
        // which all unwrap `Nested` before resolving columns).
        AstExpr::Nested(inner) => as_bare_column(scope, inner),
        _ => Ok(None),
    }
}

/// Bind one SELECT item into aggregate-output coordinates. Aggregate calls
/// are extracted into `aggregates` (deduped by `PartialEq`) and referenced
/// by their output slot (`projection.len()` + position); bare columns
/// resolve to their group-key position; arithmetic recurses.
fn bind_output_expr<E: StorageEngine>(
    scope: &Scope,
    e: &AstExpr,
    projection: &[usize],
    aggregates: &mut Vec<AggregateSpec>,
    binder: &Binder<E>,
) -> Result<Expression> {
    match e {
        AstExpr::Function(func) => {
            let spec = bind_aggregate_function(scope, func, binder)?;
            let pos = match aggregates.iter().position(|a| *a == spec) {
                Some(pos) => pos,
                None => {
                    aggregates.push(spec);
                    aggregates.len() - 1
                }
            };
            Ok(Expression::Column(projection.len() + pos))
        }
        AstExpr::Identifier(_) | AstExpr::CompoundIdentifier(_) => {
            let col = as_bare_column(scope, e)?.ok_or_else(|| {
                Error::SqlParse(format!("binder: unsupported column reference: {:?}", e))
            })?;
            match projection.iter().position(|&p| p == col) {
                Some(pos) => Ok(Expression::Column(pos)),
                None => Err(Error::SqlParse(format!(
                    "binder: column '{}' must be a group key",
                    column_name_at(scope, col)
                ))),
            }
        }
        AstExpr::BinaryOp { left, op, right } => {
            let arith_op = map_arith_op(op)?;
            let l = bind_output_expr(scope, left, projection, aggregates, binder)?;
            let r = bind_output_expr(scope, right, projection, aggregates, binder)?;
            // NO literal alignment in output space (H2b): aggregate outputs
            // carry their own scales. Div is now native max-scale
            // (`Decimal::div`), so the left-associative `100.00 * SUM(x) /
            // SUM(y)` — TPC-H Q14's verbatim shape, a scale-4 ÷ scale-2
            // division — infers a scale-4 result and runs without alignment.
            Ok(Expression::BinaryOp {
                op: arith_op,
                left: Box::new(l),
                right: Box::new(r),
            })
        }
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match &**expr {
            AstExpr::Value(AstValue::Number(n, _)) => Ok(Expression::Literal(
                ast_value_to_value_unconstrained(AstValue::Number(format!("-{}", n), false))?,
            )),
            other => {
                let inner = bind_output_expr(scope, other, projection, aggregates, binder)?;
                Ok(Expression::BinaryOp {
                    op: BinaryOp::Sub,
                    left: Box::new(Expression::Literal(Value::Int64(0))),
                    right: Box::new(inner),
                })
            }
        },
        AstExpr::Value(v) => Ok(Expression::Literal(ast_value_to_value_unconstrained(
            v.clone(),
        )?)),
        AstExpr::Nested(inner) => bind_output_expr(scope, inner, projection, aggregates, binder),
        other => Err(Error::SqlParse(format!(
            "binder: SELECT item shape unsupported: {:?}",
            other
        ))),
    }
}

/// True if an AST expression contains an aggregate call anywhere — the
/// signal (with a GROUP BY clause) that a query is aggregated. Only
/// aggregate-named functions count; unsupported non-aggregate functions are
/// rejected later by binding.
fn expr_contains_aggregate(e: &AstExpr) -> bool {
    match e {
        AstExpr::Function(func) => is_aggregate_name(&object_name_to_string(&func.name)),
        AstExpr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        AstExpr::UnaryOp { expr, .. } => expr_contains_aggregate(expr),
        AstExpr::Nested(inner) => expr_contains_aggregate(inner),
        _ => false,
    }
}

/// The function names the binder treats as aggregates.
fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "MIN" | "MAX" | "AVG"
    )
}

/// A `select_list` that is exactly `[Column(0), Column(1), …, Column(n-1)]`
/// — the identity display over its coordinate space — elides to empty (the
/// zero-churn invariant): the plan then reproduces the pre-H2b shape (bare
/// projection, or keys-then-aggregates straight out of the aggregate).
fn is_identity_select_list(list: &[Expression]) -> bool {
    list.iter()
        .enumerate()
        .all(|(i, e)| matches!(e, Expression::Column(c) if *c == i))
}

/// Reverse of `column_index`: the display name of joined-tuple position
/// `idx`, for error messages. Positions come from successful binding, so
/// out-of-range is a binder bug.
fn column_name_at(scope: &Scope, idx: usize) -> String {
    for t in &scope.tables {
        if idx >= t.column_offset && idx < t.column_offset + t.schema.columns.len() {
            return t.schema.columns[idx - t.column_offset].name.clone();
        }
    }
    unreachable!("column_name_at: {} out of scope", idx)
}

/// Resolve an ORDER BY / HAVING operand into the aggregate OUTPUT row
/// (keys ++ aggregates): a plain column must be a group key (→ its
/// position in `projection`); an aggregate call must match a SELECT-list
/// spec (→ `projection.len()` + its position).
fn resolve_aggregate_output_column<E: StorageEngine>(
    scope: &Scope,
    e: &AstExpr,
    projection: &[usize],
    aggregates: &mut Vec<AggregateSpec>,
    binder: &Binder<E>,
) -> Result<usize> {
    match e {
        AstExpr::Function(func) => {
            let spec = bind_aggregate_function(scope, func, binder)?;
            // An aggregate the SELECT list does not display is still legal in
            // HAVING / ORDER BY: compute it (append to the aggregate row) and
            // reference its slot; the caller projects trailing computed-only
            // aggregates back out.
            let pos = match aggregates.iter().position(|a| *a == spec) {
                Some(pos) => pos,
                None => {
                    aggregates.push(spec);
                    aggregates.len() - 1
                }
            };
            Ok(projection.len() + pos)
        }
        other => {
            let input_col = resolve_column_expr(scope, other)?;
            match projection.iter().position(|&p| p == input_col) {
                Some(pos) => Ok(pos),
                None => Err(Error::SqlParse(format!(
                    "binder: column '{}' in ORDER BY/HAVING must be a group key",
                    column_name_at(scope, input_col)
                ))),
            }
        }
    }
}

/// Bind a HAVING predicate over the aggregate output row. Mirrors
/// `bind_predicate`'s shape (And/Or/Not over comparisons) but resolves
/// operands through `resolve_aggregate_output_column` — the input scope
/// no longer exists above the aggregate.
fn bind_having_predicate<E: StorageEngine>(
    scope: &Scope,
    e: AstExpr,
    projection: &[usize],
    aggregates: &mut Vec<AggregateSpec>,
    ctx: &mut SubqueryCtx<E>,
) -> Result<Predicate> {
    match e {
        AstExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let l = bind_having_predicate(scope, *left, projection, aggregates, ctx)?;
                let r = bind_having_predicate(scope, *right, projection, aggregates, ctx)?;
                Ok(Predicate::And(Box::new(l), Box::new(r)))
            }
            BinaryOperator::Or => {
                let l = bind_having_predicate(scope, *left, projection, aggregates, ctx)?;
                let r = bind_having_predicate(scope, *right, projection, aggregates, ctx)?;
                Ok(Predicate::Or(Box::new(l), Box::new(r)))
            }
            cmp => {
                let cmp_op = map_compare_op(&cmp)?;
                let l = bind_having_operand(scope, *left, projection, aggregates, ctx)?;
                let r = bind_having_operand(scope, *right, projection, aggregates, ctx)?;
                Ok(Predicate::Compare {
                    op: cmp_op,
                    left: l,
                    right: r,
                })
            }
        },
        AstExpr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let inner = bind_having_predicate(scope, *expr, projection, aggregates, ctx)?;
            Ok(Predicate::Not(Box::new(inner)))
        }
        AstExpr::Nested(inner) => bind_having_predicate(scope, *inner, projection, aggregates, ctx),
        other => Err(Error::SqlParse(format!(
            "binder: HAVING shape unsupported: {:?}",
            other
        ))),
    }
}

/// One comparison operand inside HAVING: a literal, an uncorrelated scalar
/// subquery (→ `SubqueryResult`, resolved by the session like a WHERE scalar),
/// or anything `resolve_aggregate_output_column` accepts.
fn bind_having_operand<E: StorageEngine>(
    scope: &Scope,
    e: AstExpr,
    projection: &[usize],
    aggregates: &mut Vec<AggregateSpec>,
    ctx: &mut SubqueryCtx<E>,
) -> Result<Expression> {
    match e {
        AstExpr::Value(v) => Ok(Expression::Literal(ast_value_to_value_unconstrained(v)?)),
        AstExpr::Nested(inner) => bind_having_operand(scope, *inner, projection, aggregates, ctx),
        AstExpr::Subquery(q) => {
            let plan = ctx.binder.bind_scalar_subquery(*q, scope, ctx.depth)?;
            let slot = ctx.scalar_subqueries.len();
            ctx.scalar_subqueries.push(plan);
            Ok(Expression::SubqueryResult(slot))
        }
        other => Ok(Expression::Column(resolve_aggregate_output_column(
            scope, &other, projection, aggregates, ctx.binder,
        )?)),
    }
}

/// Translate a sqlparser `Function` AST node into our `AggregateSpec`.
/// Supported: `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`,
/// `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`. Anything else is a
/// non-aggregate function call — we error rather than silently treating
/// it as a row-level expression (no scalar-function support yet).
fn bind_aggregate_function<E: StorageEngine>(
    scope: &Scope,
    func: &ast::Function,
    binder: &Binder<E>,
) -> Result<AggregateSpec> {
    let name = object_name_to_string(&func.name).to_uppercase();
    let (args, distinct) = match &func.args {
        ast::FunctionArguments::List(list) => {
            let distinct = matches!(
                list.duplicate_treatment,
                Some(ast::DuplicateTreatment::Distinct)
            );
            (&list.args, distinct)
        }
        ast::FunctionArguments::None => {
            return Err(Error::SqlParse(format!(
                "binder: function '{}' with no argument list not supported",
                name
            )));
        }
        ast::FunctionArguments::Subquery(_) => {
            return Err(Error::SqlParse(format!(
                "binder: function '{}' with subquery argument not supported",
                name
            )));
        }
    };

    // Reject window-function modifiers (OVER), FILTER, WITHIN GROUP, etc.
    if func.over.is_some() {
        return Err(Error::SqlParse(format!(
            "binder: window functions ('{}' OVER ...) not supported",
            name
        )));
    }
    if func.filter.is_some() {
        return Err(Error::SqlParse(format!(
            "binder: FILTER clauses on '{}' not supported",
            name
        )));
    }

    match name.as_str() {
        "COUNT" => {
            if args.len() != 1 {
                return Err(Error::SqlParse(
                    "COUNT takes exactly one argument (* or a column)".into(),
                ));
            }
            match &args[0] {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                    if distinct {
                        return Err(Error::SqlParse("COUNT(DISTINCT *) is not valid SQL".into()));
                    }
                    Ok(AggregateSpec::CountStar)
                }
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    let arg = bind_expression(scope, e.clone(), binder)?;
                    Ok(AggregateSpec::Count { arg, distinct })
                }
                other => Err(Error::SqlParse(format!(
                    "binder: unsupported COUNT argument shape: {:?}",
                    other
                ))),
            }
        }
        "SUM" | "MIN" | "MAX" | "AVG" => {
            if distinct {
                return Err(Error::SqlParse(format!(
                    "{}(DISTINCT …) not supported (Phase 13 only handles COUNT(DISTINCT))",
                    name
                )));
            }
            if args.len() != 1 {
                return Err(Error::SqlParse(format!(
                    "{} takes exactly one column argument",
                    name
                )));
            }
            let arg = match &args[0] {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    bind_expression(scope, e.clone(), binder)?
                }
                other => {
                    return Err(Error::SqlParse(format!(
                        "binder: unsupported {} argument shape: {:?}",
                        name, other
                    )));
                }
            };
            Ok(match name.as_str() {
                "SUM" => AggregateSpec::Sum(arg),
                "MIN" => AggregateSpec::Min(arg),
                "MAX" => AggregateSpec::Max(arg),
                "AVG" => AggregateSpec::Avg(arg),
                _ => unreachable!(),
            })
        }
        other => Err(Error::SqlParse(format!(
            "binder: unsupported function '{}'",
            other
        ))),
    }
}

/// Resolve an AST expression that must be a plain column reference to a
/// tuple-global index. Serves GROUP BY and ORDER BY keys (which are
/// column-only); aggregate arguments now bind through `bind_expression`.
fn resolve_column_expr(scope: &Scope, e: &AstExpr) -> Result<usize> {
    match e {
        AstExpr::Identifier(ident) => column_index(scope, &ident.value),
        AstExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
            column_index_qualified(scope, &parts[0].value, &parts[1].value)
        }
        other => Err(Error::SqlParse(format!(
            "binder: expected a plain column reference, got {:?}",
            other
        ))),
    }
}

/// Convert an INSERT VALUES expression into an `Expression`. Most cases
/// are literals (typed against the target column); placeholders become
/// `Expression::Parameter(i)` for prepared statements (P13.7).
fn insert_value_expr(e: AstExpr, target_ty: &ColumnType) -> Result<Expression> {
    if let AstExpr::Value(AstValue::Placeholder(s)) = &e {
        if let Some(rest) = s.strip_prefix('$') {
            let n: usize = rest
                .parse()
                .map_err(|err| Error::SqlParse(format!("invalid placeholder '{}': {}", s, err)))?;
            if n == 0 {
                return Err(Error::SqlParse(
                    "parameter placeholders are 1-based ($1, $2, …)".into(),
                ));
            }
            return Ok(Expression::Parameter(n - 1));
        }
        return Err(Error::SqlParse(
            "use $1, $2, … for parameter placeholders in INSERT VALUES".into(),
        ));
    }
    literal_from_expr(e, target_ty).map(Expression::Literal)
}

/// Convert an AST expression that must be a literal (used by INSERT VALUES).
fn literal_from_expr(e: AstExpr, target_ty: &ColumnType) -> Result<Value> {
    // A `DATE 'YYYY-MM-DD'` literal binds only against a Date target column.
    if let AstExpr::TypedString {
        data_type: AstDataType::Date,
        value,
    } = &e
    {
        if !matches!(target_ty, ColumnType::Date) {
            return Err(Error::SqlParse(format!(
                "binder: DATE literal '{value}' cannot bind to column type {target_ty:?}"
            )));
        }
        return Ok(Value::Date(parse_date_literal(value)?));
    }
    let inner = match e {
        AstExpr::Value(v) => v,
        // Unary minus on a number literal — handle `-5`.
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match *expr {
            AstExpr::Value(AstValue::Number(n, _)) => AstValue::Number(format!("-{}", n), false),
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: VALUES literal expected, got -{:?}",
                    other
                )))
            }
        },
        other => {
            return Err(Error::SqlParse(format!(
                "binder: VALUES literal expected, got {:?}",
                other
            )))
        }
    };
    ast_value_to_value(inner, target_ty)
}

/// Map a sqlparser literal to our `Value`, coerced to `target_ty`.
fn ast_value_to_value(v: AstValue, target_ty: &ColumnType) -> Result<Value> {
    match (v, target_ty) {
        (AstValue::Null, _) => Ok(Value::Null),
        (AstValue::Boolean(b), ColumnType::Boolean) => Ok(Value::Boolean(b)),
        (AstValue::Number(n, _), ColumnType::Int32) => n
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|e| Error::SqlParse(format!("parse Int32 '{}': {}", n, e))),
        (AstValue::Number(n, _), ColumnType::Int64) => n
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|e| Error::SqlParse(format!("parse Int64 '{}': {}", n, e))),
        (AstValue::Number(n, _), ColumnType::Decimal { scale, .. }) => {
            decimal_from_str(&n, *scale).map(Value::Decimal)
        }
        (AstValue::SingleQuotedString(s), ColumnType::Varchar(_)) => Ok(Value::Varchar(s)),
        (AstValue::SingleQuotedString(s), ColumnType::Char(_)) => Ok(Value::Char(s)),
        (AstValue::SingleQuotedString(s), _) => Ok(Value::Varchar(s)), // best-effort
        (other_v, other_ty) => Err(Error::SqlParse(format!(
            "binder: cannot bind literal {:?} to column type {:?}",
            other_v, other_ty
        ))),
    }
}

/// Parse a decimal string ("123.45") at a target scale.
fn decimal_from_str(s: &str, scale: u8) -> Result<Decimal> {
    let (sign, body) = if let Some(rest) = s.strip_prefix('-') {
        (-1i64, rest)
    } else {
        (1i64, s)
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if frac_part.len() > scale as usize {
        return Err(Error::SqlParse(format!(
            "decimal literal '{}' has more fractional digits than scale {}",
            s, scale
        )));
    }
    let mut digits = String::with_capacity(int_part.len() + scale as usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    // Right-pad with zeros to reach `scale` fractional digits.
    for _ in frac_part.len()..scale as usize {
        digits.push('0');
    }
    let mantissa: i64 = digits
        .parse::<i64>()
        .map_err(|e| Error::SqlParse(format!("parse decimal '{}': {}", s, e)))?;
    Ok(Decimal::from_i64_with_scale(sign * mantissa, scale))
}

// -------- expression binding (in-WHERE / RHS-of-SET) -----------------------

fn bind_expression<E: StorageEngine>(
    scope: &Scope,
    e: AstExpr,
    binder: &Binder<E>,
) -> Result<Expression> {
    // DATE literals and `date_literal ± INTERVAL 'n' DAY|MONTH|YEAR` fold to a
    // plain `Value::Date` here, at bind time — planners and executors then see
    // an ordinary literal, with zero date-arithmetic machinery downstream.
    if let Some(days) = fold_date_expr(&e)? {
        return Ok(Expression::Literal(Value::Date(days)));
    }
    match e {
        AstExpr::Value(AstValue::Placeholder(s)) => {
            // P13.7: SQL parameter placeholder. Accept both `?` (anonymous,
            // index-by-occurrence) and `$N` (PostgreSQL-style positional).
            // For `?`, we use the running placeholder count tracked by the
            // binder's recursion order; here the simplest correct approach
            // is to require `$N` syntax for explicit indexing.
            //
            // sqlparser passes the source token verbatim, so `?` arrives as
            // "?". For Phase 13 we support both styles:
            // - "?": treated as $1 if it's the only one, else require $N.
            //   To keep things simple we assign indices in the order they
            //   appear via a counter on the binder. But Binder is &self
            //   here, not &mut self — so we use atomic interior mutability
            //   later. For now, accept only $N to keep this stateless.
            if let Some(rest) = s.strip_prefix('$') {
                let n: usize = rest.parse().map_err(|e| {
                    Error::SqlParse(format!("invalid parameter placeholder '{}': {}", s, e))
                })?;
                if n == 0 {
                    return Err(Error::SqlParse(
                        "parameter placeholders are 1-based ($1, $2, …)".into(),
                    ));
                }
                Ok(Expression::Parameter(n - 1))
            } else if s == "?" {
                // For "?" without explicit numbering, we'd need to track
                // occurrence order in the binder. Defer to a Phase 14
                // refactor; for now require explicit `$N`.
                Err(Error::SqlParse(
                    "use $1, $2, … for parameter placeholders (anonymous `?` not supported yet)"
                        .into(),
                ))
            } else {
                Err(Error::SqlParse(format!(
                    "unrecognized placeholder syntax: '{}'",
                    s
                )))
            }
        }
        AstExpr::Value(v) => {
            // Literals in expression position get an inferred type from
            // their syntactic shape — narrowing happens at compile time
            // via NULL semantics if the receiving op expects a different
            // shape. For Phase 11 we keep this simple.
            Ok(Expression::Literal(ast_value_to_value_unconstrained(v)?))
        }
        AstExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match *expr {
            AstExpr::Value(AstValue::Number(n, _)) => {
                let neg = format!("-{}", n);
                Ok(Expression::Literal(ast_value_to_value_unconstrained(
                    AstValue::Number(neg, false),
                )?))
            }
            other => {
                let inner = bind_expression(scope, other, binder)?;
                Ok(Expression::BinaryOp {
                    op: BinaryOp::Sub,
                    left: Box::new(Expression::Literal(Value::Int64(0))),
                    right: Box::new(inner),
                })
            }
        },
        AstExpr::Identifier(ident) => {
            let idx = column_index(scope, &ident.value)?;
            Ok(Expression::Column(idx))
        }
        AstExpr::CompoundIdentifier(parts) => {
            // `qualifier.column` — qualifier is a table name or alias.
            // Two parts: standard SQL. Three+: schema-qualified (we don't
            // support a notion of SQL schemas yet — error).
            match parts.len() {
                1 => {
                    let idx = column_index(scope, &parts[0].value)?;
                    Ok(Expression::Column(idx))
                }
                2 => {
                    let idx = column_index_qualified(scope, &parts[0].value, &parts[1].value)?;
                    Ok(Expression::Column(idx))
                }
                _ => Err(Error::SqlParse(format!(
                    "binder: compound identifier with {} parts not supported",
                    parts.len()
                ))),
            }
        }
        AstExpr::BinaryOp { left, op, right } => {
            let arith_op = map_arith_op(&op)?;
            let l = bind_expression(scope, *left, binder)?;
            let r = bind_expression(scope, *right, binder)?;
            // Align an integer literal to the other operand's numeric type
            // so the arithmetic is well-typed under eval_binary_op (which
            // otherwise resolves the mismatch to NULL).
            let (l, r) = align_numeric_literal(scope, arith_op, l, r);
            Ok(Expression::BinaryOp {
                op: arith_op,
                left: Box::new(l),
                right: Box::new(r),
            })
        }
        AstExpr::Extract { field, expr, .. } => {
            // Only YEAR is supported; other fields are a loud, named refusal.
            if !matches!(field, ast::DateTimeField::Year) {
                return Err(Error::SqlParse(format!(
                    "binder: EXTRACT({field}) not supported (only YEAR)"
                )));
            }
            let arg = bind_expression(scope, *expr, binder)?;
            // Bind-time typing: EXTRACT(YEAR FROM …) is defined only over a
            // DATE argument. Refuse anything else here rather than let the
            // runtime silently return NULL.
            match arg.column_type(&scope_column_types(scope)) {
                Some(ColumnType::Date) => Ok(Expression::ExtractYear(Box::new(arg))),
                other => Err(Error::SqlParse(format!(
                    "binder: EXTRACT(YEAR FROM …) requires a DATE argument, got {other:?}"
                ))),
            }
        }
        // A bare INTERVAL (not folded into a literal DATE above) has no runtime
        // type in this engine — refuse it loudly with the supported shape.
        AstExpr::Interval(_) => Err(Error::SqlParse(
            "binder: INTERVAL is only supported as `DATE '…' ± INTERVAL '…' DAY|MONTH|YEAR`".into(),
        )),
        AstExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => bind_case(scope, operand, conditions, results, else_result, binder),
        AstExpr::Nested(inner) => bind_expression(scope, *inner, binder),
        other => Err(Error::SqlParse(format!(
            "binder: expression shape unsupported: {:?}",
            other
        ))),
    }
}

/// Bind-time integer-literal alignment for arithmetic. `eval_binary_op`
/// only combines matching numeric representations, so a whole-number
/// literal — bound at the default width (Int64) — can silently resolve an
/// entire arithmetic expression to NULL against an Int32 or Decimal
/// column. Coerce the literal to the other operand's type when
/// value-preserving (`coerce_exact`); an unrepresentable literal (e.g. an
/// out-of-i32 value, or too many integer digits for the scale) is left
/// untouched, preserving today's NULL for that case.
///
/// NOTE (plan deviation): step 3c specified only the Decimal Add/Sub
/// landmine (Q1's `1 - l_discount`); the Int32 case (any op) is required
/// for the `SUM(c_val * 2)` parity corpus — same landmine (Int32 column ×
/// default-Int64 literal), same value-preserving fix.
fn align_numeric_literal(
    scope: &Scope,
    op: BinaryOp,
    l: Expression,
    r: Expression,
) -> (Expression, Expression) {
    let types = scope_column_types(scope);
    if let Expression::Literal(v @ (Value::Int32(_) | Value::Int64(_))) = &l {
        if let Some(target) = align_target(op, r.column_type(&types)) {
            if let Some(coerced) = v.coerce_exact(&target) {
                return (Expression::Literal(coerced), r);
            }
        }
    }
    if let Expression::Literal(v @ (Value::Int32(_) | Value::Int64(_))) = &r {
        if let Some(target) = align_target(op, l.column_type(&types)) {
            if let Some(coerced) = v.coerce_exact(&target) {
                return (l, Expression::Literal(coerced));
            }
        }
    }
    (l, r)
}

/// The type an integer literal should coerce toward, given the other
/// operand's inferred type and the operator: Int32 for any op; Decimal for
/// Add/Sub but NOT Mul or Div. The split follows the runtime decimal algebra:
/// - Mul *promotes* (result scale = sum of operand scales), so an Int literal
///   already promotes to a scale-0 Decimal in `eval_binary_op` and coercing it
///   to the column's scale would change the result scale.
/// - Add/Sub *require equal scales* at runtime, so coercing the literal to the
///   Decimal side's scale is what makes the operation legal instead of NULL.
/// - Div is native max-scale (`Decimal::div`): `price / 2` promotes `2` to a
///   scale-0 Decimal and takes scale = max(price_scale, 0) = price_scale with
///   the identical value, so it needs NO alignment.
fn align_target(op: BinaryOp, other: Option<ColumnType>) -> Option<ColumnType> {
    match other {
        Some(ty @ ColumnType::Int32) => Some(ty),
        Some(ty @ ColumnType::Decimal { .. }) if matches!(op, BinaryOp::Add | BinaryOp::Sub) => {
            Some(ty)
        }
        _ => None,
    }
}

/// Bind a searched `CASE WHEN … THEN … [ELSE …] END`. Simple CASE
/// (`CASE <operand> WHEN v THEN …`) is a loud refusal. Each WHEN binds as a
/// predicate and each THEN/ELSE as an expression, all against the input
/// `scope` (so a CASE inside `SUM(…)` resolves the same columns the
/// aggregate sees). Bare integer-literal branches are then coerced toward
/// the non-literal branches' type (see `coerce_case_literal_branches`).
fn bind_case<E: StorageEngine>(
    scope: &Scope,
    operand: Option<Box<AstExpr>>,
    conditions: Vec<AstExpr>,
    results: Vec<AstExpr>,
    else_result: Option<Box<AstExpr>>,
    binder: &Binder<E>,
) -> Result<Expression> {
    if operand.is_some() {
        return Err(Error::SqlParse(
            "binder: only searched CASE (CASE WHEN … THEN … END) is supported, not simple CASE \
             (CASE <expr> WHEN …)"
                .into(),
        ));
    }
    // sqlparser pairs conditions[i] with results[i] for a searched CASE.
    let mut branches: Vec<(Predicate, Expression)> = Vec::with_capacity(conditions.len());
    for (cond, result) in conditions.into_iter().zip(results.into_iter()) {
        // A CASE branch predicate cannot carry a subquery (the compiled CASE
        // closure has no set to capture) — bind through a local ctx that
        // rejects one loudly rather than dropping it into an unmaterialized
        // slot.
        let mut cond_ctx = SubqueryCtx::new(binder, 0);
        let pred = bind_predicate(scope, cond, &mut cond_ctx)?;
        reject_unsupported_subqueries(&cond_ctx, "a CASE branch condition")?;
        let expr = bind_expression(scope, result, binder)?;
        branches.push((pred, expr));
    }
    let mut else_expr = match else_result {
        Some(e) => Some(Box::new(bind_expression(scope, *e, binder)?)),
        None => None,
    };
    coerce_case_literal_branches(scope, &mut branches, &mut else_expr);
    Ok(Expression::Case {
        branches,
        else_expr,
    })
}

/// Coerce every bare integer-literal CASE result branch toward the type the
/// NON-literal branches agree on, when that shared type is Int32 or a
/// Decimal. This is the TPC-H Q12/Q14 shape: `CASE WHEN … THEN price ELSE 0
/// END` binds `0` at the default Int64 width, which would fail
/// `Expression::column_type`'s strict same-type unification against a
/// `Decimal` THEN branch and make the CASE (and any aggregate over it)
/// uninferable. Coercing `0` to the Decimal side via `coerce_exact` — the
/// same value-preserving mechanism `align_numeric_literal` uses for
/// arithmetic — lets it unify to the Decimal type instead.
///
/// If the non-literal branches DON'T agree on a single Int32/Decimal type
/// (e.g. a Varchar branch, or mixed types), no coercion happens and the
/// strict unification in `column_type` errors loudly at build — the
/// intended type-mismatch contract.
fn coerce_case_literal_branches(
    scope: &Scope,
    branches: &mut [(Predicate, Expression)],
    else_expr: &mut Option<Box<Expression>>,
) {
    let types = scope_column_types(scope);

    // Pass 1: the single Int32/Decimal type shared by all non-integer-literal
    // result branches, or `None` if they disagree / aren't Int32/Decimal.
    let mut target: Option<ColumnType> = None;
    {
        let results = branches
            .iter()
            .map(|(_, result)| result)
            .chain(else_expr.as_deref());
        for result in results {
            if is_bare_integer_literal(result) {
                continue;
            }
            // Same rule as `Expression::column_type`'s Case arm: a bare NULL
            // literal carries no type. Skip it so its `None` isn't read as a
            // non-Int32/Decimal branch that bails the pass (leaving genuine
            // integer literals uncoerced). It is never itself coerced — pass 2
            // only rewrites integer literals.
            if matches!(result, Expression::Literal(Value::Null)) {
                continue;
            }
            match result.column_type(&types) {
                Some(ty @ (ColumnType::Int32 | ColumnType::Decimal { .. })) => match target {
                    None => target = Some(ty),
                    Some(existing) if existing == ty => {}
                    // Disagreement — bail; the build-time unification errors.
                    Some(_) => return,
                },
                // A non-Int32/Decimal (or uninferable) non-literal branch:
                // there's nothing to coerce integer literals toward.
                _ => return,
            }
        }
    }
    let target = match target {
        Some(ty) => ty,
        // Every branch is a bare integer literal (e.g. `THEN 1 ELSE 0`) —
        // they already share Int64 and unify on their own; nothing to do.
        None => return,
    };

    // Pass 2: rewrite each bare integer-literal branch to the target type.
    for (_, result) in branches.iter_mut() {
        coerce_integer_literal_to(result, &target);
    }
    if let Some(result) = else_expr.as_deref_mut() {
        coerce_integer_literal_to(result, &target);
    }
}

/// True when `e` is a bare `Int32`/`Int64` literal — the branch shape
/// `coerce_case_literal_branches` may retype.
fn is_bare_integer_literal(e: &Expression) -> bool {
    matches!(e, Expression::Literal(Value::Int32(_) | Value::Int64(_)))
}

/// Replace a bare integer-literal expression with the same value coerced to
/// `target`, when `coerce_exact` finds a value-preserving representation.
/// Non-literals and unrepresentable literals are left untouched.
fn coerce_integer_literal_to(e: &mut Expression, target: &ColumnType) {
    if let Expression::Literal(v @ (Value::Int32(_) | Value::Int64(_))) = e {
        if let Some(coerced) = v.coerce_exact(target) {
            *e = Expression::Literal(coerced);
        }
    }
}

/// Flatten the scope's tables into a tuple-global `Vec<ColumnType>`, the
/// view `Expression::column_type` infers against. Order matches
/// `column_index`'s tuple-global layout (each table contributes a
/// contiguous run at its `column_offset`).
fn scope_column_types(scope: &Scope) -> Vec<ColumnType> {
    scope
        .tables
        .iter()
        .flat_map(|t| t.schema.columns.iter().map(|c| c.ty))
        .collect()
}

/// Map a literal without a target column type. Used inside expressions
/// where the receiver context isn't known at parse time.
fn ast_value_to_value_unconstrained(v: AstValue) -> Result<Value> {
    match v {
        AstValue::Null => Ok(Value::Null),
        AstValue::Boolean(b) => Ok(Value::Boolean(b)),
        AstValue::Number(n, _) => {
            // Prefer Int64 for whole numbers; Decimal scale=2 for fractional
            // (a hint that money is the common case for fractional literals).
            if let Ok(i) = n.parse::<i64>() {
                Ok(Value::Int64(i))
            } else if n.contains('.') {
                let scale = n.split_once('.').map(|(_, f)| f.len() as u8).unwrap_or(0);
                Ok(Value::Decimal(decimal_from_str(&n, scale)?))
            } else {
                Err(Error::SqlParse(format!(
                    "cannot parse number literal: {}",
                    n
                )))
            }
        }
        AstValue::SingleQuotedString(s) => Ok(Value::Varchar(s)),
        other => Err(Error::SqlParse(format!(
            "binder: literal unsupported in expression: {:?}",
            other
        ))),
    }
}

// -------- DATE literals, INTERVAL folding ----------------------------------

/// The three `INTERVAL` units we fold. DAY shifts the day count directly;
/// MONTH/YEAR decompose to civil (y, m, d) and shift the month, clamping the
/// day into the target month.
enum IntervalUnit {
    Day,
    Month,
    Year,
}

/// Try to fold an AST expression to a `Date` day-count at bind time.
///
/// - `Ok(Some(days))` — a `DATE 'YYYY-MM-DD'` literal, or a
///   `date_literal ± INTERVAL 'n' DAY|MONTH|YEAR` chain (folded recursively,
///   so `date … - interval … + interval …` works). Every TPC-H date
///   expression is literal-only, so planners only ever see a `Value::Date`.
/// - `Ok(None)` — not a date-shaped expression; the caller binds it normally.
/// - `Err` — date-shaped but malformed, or `INTERVAL` in an unsupported
///   position (loud, naming the limitation).
fn fold_date_expr(e: &AstExpr) -> Result<Option<i32>> {
    match e {
        AstExpr::TypedString {
            data_type: AstDataType::Date,
            value,
        } => Ok(Some(parse_date_literal(value)?)),
        AstExpr::Nested(inner) => fold_date_expr(inner),
        AstExpr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::Plus | BinaryOperator::Minus) =>
        {
            match fold_date_expr(left)? {
                // Literal date on the left: the right MUST be an INTERVAL.
                Some(base) => {
                    let iv = match &**right {
                        AstExpr::Interval(iv) => iv,
                        other => {
                            return Err(Error::SqlParse(format!(
                                "binder: DATE arithmetic expects an INTERVAL operand, got {other:?}"
                            )))
                        }
                    };
                    let (amount, unit) = interval_amount(iv)?;
                    let signed = if matches!(op, BinaryOperator::Minus) {
                        -amount
                    } else {
                        amount
                    };
                    Ok(Some(apply_interval(base, signed, unit)?))
                }
                // Left didn't fold to a literal DATE. An INTERVAL on the right
                // still makes this DATE-arithmetic shaped, but the left is
                // unusable — split the diagnostic so the two failure modes read
                // differently:
                //   - the left references a column (`o_date + INTERVAL …`): it
                //     may well be DATE-typed, but only *literal* dates fold at
                //     bind time.
                //   - the left is anything else (`1 + INTERVAL …`): it isn't a
                //     DATE expression at all.
                None => {
                    if matches!(**right, AstExpr::Interval(_)) {
                        let left_is_column = matches!(
                            **left,
                            AstExpr::Identifier(_) | AstExpr::CompoundIdentifier(_)
                        );
                        if left_is_column {
                            Err(Error::SqlParse(
                                "binder: INTERVAL arithmetic requires a literal DATE operand \
                                 (`DATE '…' ± INTERVAL '…' DAY|MONTH|YEAR`); the left side is a \
                                 DATE expression but not a literal one"
                                    .into(),
                            ))
                        } else {
                            Err(Error::SqlParse(
                                "binder: INTERVAL arithmetic requires a literal DATE operand \
                                 (`DATE '…' ± INTERVAL '…' DAY|MONTH|YEAR`); the left operand is \
                                 not a DATE expression"
                                    .into(),
                            ))
                        }
                    } else {
                        Ok(None)
                    }
                }
            }
        }
        _ => Ok(None),
    }
}

/// Parse a strict `YYYY-MM-DD` date literal to days-since-epoch. Validates the
/// calendar via the civil round-trip (a day that normalizes forward — e.g.
/// 1900-02-29 → 1900-03-01 — isn't a real date and is rejected). Malformed
/// input is a loud error, never a silent coercion.
fn parse_date_literal(value: &str) -> Result<i32> {
    let bytes = value.as_bytes();
    // Exactly 10 chars, dashes at [4] and [7], digits everywhere else.
    let well_formed = bytes.len() == 10
        && bytes.iter().enumerate().all(|(i, &b)| {
            if i == 4 || i == 7 {
                b == b'-'
            } else {
                b.is_ascii_digit()
            }
        });
    if !well_formed {
        return Err(Error::SqlParse(format!(
            "invalid DATE literal '{value}': expected strict YYYY-MM-DD"
        )));
    }
    // Slices are ASCII-digit-validated above, so these parses cannot fail.
    let year: i32 = value[0..4].parse().unwrap();
    let month: u32 = value[5..7].parse().unwrap();
    let day: u32 = value[8..10].parse().unwrap();
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(Error::SqlParse(format!(
            "invalid DATE literal '{value}': month/day out of range"
        )));
    }
    let days = crate::types::civil::days_from_ymd(year, month, day);
    if crate::types::civil::ymd_from_days(days) != (year, month, day) {
        return Err(Error::SqlParse(format!(
            "invalid DATE literal '{value}': not a real calendar date"
        )));
    }
    Ok(days)
}

/// Extract the signed amount and unit from a sqlparser `INTERVAL` node. Only
/// integer-literal amounts and DAY/MONTH/YEAR units fold; anything else is a
/// loud error (matching the "fold at bind time, else refuse" contract).
fn interval_amount(iv: &ast::Interval) -> Result<(i64, IntervalUnit)> {
    let amount = match &*iv.value {
        AstExpr::Value(AstValue::SingleQuotedString(s))
        | AstExpr::Value(AstValue::Number(s, _)) => s
            .trim()
            .parse::<i64>()
            .map_err(|e| Error::SqlParse(format!("invalid INTERVAL amount '{s}': {e}")))?,
        other => {
            return Err(Error::SqlParse(format!(
                "binder: INTERVAL amount must be an integer literal, got {other:?}"
            )))
        }
    };
    let unit = match &iv.leading_field {
        Some(ast::DateTimeField::Day) => IntervalUnit::Day,
        Some(ast::DateTimeField::Month) => IntervalUnit::Month,
        Some(ast::DateTimeField::Year) => IntervalUnit::Year,
        other => {
            return Err(Error::SqlParse(format!(
                "binder: INTERVAL unit {other:?} not supported (DAY, MONTH, YEAR only)"
            )))
        }
    };
    Ok((amount, unit))
}

/// Apply a signed interval to a day count. DAY is exact addition. MONTH/YEAR
/// decompose to civil (y, m, d), shift the absolute month index, then clamp
/// the day into the target month: `1996-01-31 + 1 MONTH = 1996-02-29`
/// (February has no 31st, so SQL pins it to the month's last valid day).
fn apply_interval(base_days: i32, signed_amount: i64, unit: IntervalUnit) -> Result<i32> {
    match unit {
        IntervalUnit::Day => {
            let delta = i32::try_from(signed_amount).map_err(|_| interval_overflow())?;
            base_days.checked_add(delta).ok_or_else(interval_overflow)
        }
        IntervalUnit::Month => add_months(base_days, signed_amount),
        IntervalUnit::Year => add_months(
            base_days,
            signed_amount
                .checked_mul(12)
                .ok_or_else(interval_overflow)?,
        ),
    }
}

/// Shift a day count by `delta_months`, clamping the day-of-month (see
/// `apply_interval` for the clamp rule).
fn add_months(base_days: i32, delta_months: i64) -> Result<i32> {
    let (year, month, day) = crate::types::civil::ymd_from_days(base_days);
    // Absolute 0-based month index (year*12 + month-1), shifted, then split
    // back with Euclidean division so negative deltas borrow correctly.
    let total = (year as i64) * 12 + (month as i64 - 1) + delta_months;
    let new_year = i32::try_from(total.div_euclid(12)).map_err(|_| interval_overflow())?;
    let new_month = (total.rem_euclid(12)) as u32 + 1;
    let clamped_day = day.min(last_day_of_month(new_year, new_month));
    Ok(crate::types::civil::days_from_ymd(
        new_year,
        new_month,
        clamped_day,
    ))
}

/// The last valid day of `(year, month)` — computed as the day before the
/// first of the following month, so leap Februaries fall out for free.
fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_of_next = crate::types::civil::days_from_ymd(next_year, next_month, 1);
    crate::types::civil::ymd_from_days(first_of_next - 1).2
}

fn interval_overflow() -> Error {
    Error::SqlParse("INTERVAL arithmetic overflowed the DATE range".into())
}

// -------- predicate binding -------------------------------------------------

fn bind_predicate<E: StorageEngine>(
    scope: &Scope,
    e: AstExpr,
    ctx: &mut SubqueryCtx<E>,
) -> Result<Predicate> {
    match e {
        AstExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let l = bind_predicate(scope, *left, ctx)?;
                let r = bind_predicate(scope, *right, ctx)?;
                Ok(Predicate::And(Box::new(l), Box::new(r)))
            }
            BinaryOperator::Or => {
                let l = bind_predicate(scope, *left, ctx)?;
                let r = bind_predicate(scope, *right, ctx)?;
                Ok(Predicate::Or(Box::new(l), Box::new(r)))
            }
            cmp => {
                let cmp_op = map_compare_op(&cmp)?;
                bind_compare(scope, cmp_op, *left, *right, ctx)
            }
        },
        AstExpr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let inner = bind_predicate(scope, *expr, ctx)?;
            Ok(Predicate::Not(Box::new(inner)))
        }
        // `[NOT] IN (subquery)` (H4b): bind the probe against the current
        // scope, bind the (uncorrelated) inner query into `in_subqueries`, and
        // emit an `InSubquery` leaf indexing its slot. The set is materialized
        // by the session; NULL-aware 3VL lives in `eval_in`.
        AstExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let probe = bind_expression(scope, *expr, ctx.binder)?;
            let plan = ctx.binder.bind_in_subquery(*subquery, scope, ctx.depth)?;
            let slot = ctx.in_subqueries.len();
            ctx.in_subqueries.push(plan);
            Ok(Predicate::InSubquery {
                expr: Some(probe),
                subquery: slot,
                negated,
            })
        }
        // `[NOT] EXISTS (subquery)` (H4b): no probe; the inner (capped at one
        // row) drives a statement-constant true/false.
        AstExpr::Exists { subquery, negated } => {
            let plan = ctx
                .binder
                .bind_exists_subquery(*subquery, scope, ctx.depth)?;
            let slot = ctx.in_subqueries.len();
            ctx.in_subqueries.push(plan);
            Ok(Predicate::InSubquery {
                expr: None,
                subquery: slot,
                negated,
            })
        }
        // BETWEEN desugars to `expr >= low AND expr <= high`; NOT BETWEEN
        // wraps the whole thing in NOT. Kleene-correct: a NULL `expr` makes
        // both comparisons UNKNOWN, so BETWEEN is UNKNOWN and NOT BETWEEN is
        // NOT(UNKNOWN)=UNKNOWN — both drop the row under WHERE, matching SQL.
        // Range selectivity falls out of the existing And/compare estimation.
        AstExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let lower = bind_compare(scope, CompareOp::Gte, (*expr).clone(), *low, ctx)?;
            let upper = bind_compare(scope, CompareOp::Lte, *expr, *high, ctx)?;
            let between = Predicate::And(Box::new(lower), Box::new(upper));
            Ok(maybe_negate(between, negated))
        }
        // IN-list desugars to an OR-chain of equalities: `expr = a OR expr = b
        // OR …`. This is 3VL-EXACT for SQL: with a NULL in the list,
        // `x IN (1, NULL)` is TRUE when x=1 else UNKNOWN (never FALSE), and
        // `x NOT IN (1, NULL)` is NOT(UNKNOWN)=UNKNOWN → zero rows — the
        // classic behavior, reproduced for free by Kleene OR/NOT (pinned in
        // scalar.slt). Selectivity rides the existing Or-chain estimator.
        AstExpr::InList {
            expr,
            list,
            negated,
        } => {
            if list.is_empty() {
                return Err(Error::SqlParse(
                    "binder: IN-list requires at least one value".into(),
                ));
            }
            // WHY bound: the desugared Or-chain's depth equals the item count,
            // and every later pass that walks a Predicate — compile_3vl,
            // substitute_params, apply_predicate, shift_predicate, and Drop —
            // recurses to that same depth, so an unbounded list is an
            // unbounded native-stack risk (the repo rule that took
            // `flatten_conjuncts` iterative). 1000 is orders of magnitude above
            // TPC-H's largest IN-list (3), so it never refuses a real query.
            const MAX_IN_LIST_ITEMS: usize = 1000;
            if list.len() > MAX_IN_LIST_ITEMS {
                return Err(Error::SqlParse(format!(
                    "binder: IN-list has {} items, exceeding MAX_IN_LIST_ITEMS ({}) — the \
                     desugared OR-chain and the predicate passes that walk it recurse to a depth \
                     equal to the item count",
                    list.len(),
                    MAX_IN_LIST_ITEMS
                )));
            }
            let mut chain: Option<Predicate> = None;
            for item in list {
                let eq = bind_compare(scope, CompareOp::Eq, (*expr).clone(), item, ctx)?;
                chain = Some(match chain {
                    None => eq,
                    Some(acc) => Predicate::Or(Box::new(acc), Box::new(eq)),
                });
            }
            let chain = chain.expect("non-empty list guarantees a chain");
            Ok(maybe_negate(chain, negated))
        }
        AstExpr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => bind_like(scope, negated, *expr, *pattern, escape_char, ctx.binder),
        // ILIKE (case-insensitive) has different matching semantics we don't
        // implement — a loud, named refusal rather than a silent LIKE.
        AstExpr::ILike { .. } => Err(Error::SqlParse(
            "binder: ILIKE (case-insensitive LIKE) is not supported".into(),
        )),
        // IS NULL / IS NOT NULL over any expression. The predicate is total
        // (never UNKNOWN), so IS NOT NULL is a plain NOT wrap.
        AstExpr::IsNull(inner) => Ok(Predicate::IsNull(bind_expression(
            scope, *inner, ctx.binder,
        )?)),
        AstExpr::IsNotNull(inner) => Ok(Predicate::Not(Box::new(Predicate::IsNull(
            bind_expression(scope, *inner, ctx.binder)?,
        )))),
        AstExpr::Nested(inner) => bind_predicate(scope, *inner, ctx),
        other => Err(Error::SqlParse(format!(
            "binder: predicate shape unsupported: {:?}",
            other
        ))),
    }
}

/// Reject any subquery collected in a position whose bound IR has no place to
/// carry the inner plan and no session resolution path (a DML `WHERE`, a
/// `CASE` branch): a subquery there is a loud, named refusal rather than a
/// silently dropped filter or an unmaterialized set.
fn reject_unsupported_subqueries<E: StorageEngine>(
    ctx: &SubqueryCtx<E>,
    position: &str,
) -> Result<()> {
    if ctx.scalar_subqueries.is_empty() && ctx.in_subqueries.is_empty() {
        Ok(())
    } else {
        Err(Error::SqlParse(format!(
            "binder: subqueries in {} are not supported yet",
            position
        )))
    }
}

/// Bind a comparison `left <op> right` with O13 operand narrowing — the
/// shared path for a plain comparison and for BETWEEN/IN desugaring.
/// Narrowing reconciles a literal operand with the compared column's type
/// (where the type is known here): runtime comparison is already exact
/// across numeric representations, but PkLookup/IndexScan lowering
/// key-encodes the literal against the column type strictly.
fn bind_compare<E: StorageEngine>(
    scope: &Scope,
    op: CompareOp,
    left: AstExpr,
    right: AstExpr,
    ctx: &mut SubqueryCtx<E>,
) -> Result<Predicate> {
    let l = bind_compare_operand(scope, left, ctx)?;
    let r = bind_compare_operand(scope, right, ctx)?;
    let (l, r) = narrow_compare_operands(scope, l, r);
    Ok(Predicate::Compare {
        op,
        left: l,
        right: r,
    })
}

/// One comparison operand: an uncorrelated scalar subquery collapses to an
/// `Expression::SubqueryResult(slot)` (the session substitutes its value before
/// planning); anything else binds as an ordinary expression. A subquery is
/// supported only as a DIRECT operand here — one buried inside arithmetic
/// (`x = (SELECT …) + 1`) falls through to `bind_expression`'s loud refusal.
fn bind_compare_operand<E: StorageEngine>(
    scope: &Scope,
    e: AstExpr,
    ctx: &mut SubqueryCtx<E>,
) -> Result<Expression> {
    match e {
        AstExpr::Subquery(q) => {
            let plan = ctx.binder.bind_scalar_subquery(*q, scope, ctx.depth)?;
            let slot = ctx.scalar_subqueries.len();
            ctx.scalar_subqueries.push(plan);
            Ok(Expression::SubqueryResult(slot))
        }
        AstExpr::Nested(inner) => bind_compare_operand(scope, *inner, ctx),
        other => bind_expression(scope, other, ctx.binder),
    }
}

/// Wrap `pred` in `Not` when `negated` — the shared tail of BETWEEN, IN,
/// and LIKE desugaring.
fn maybe_negate(pred: Predicate, negated: bool) -> Predicate {
    if negated {
        Predicate::Not(Box::new(pred))
    } else {
        pred
    }
}

/// Bind `expr LIKE pattern` into `Predicate::Like` (NOT LIKE → `Not`).
/// The pattern must be a string literal and `expr` must infer Varchar/Char;
/// ESCAPE is refused. All three refusals are loud and named.
fn bind_like<E: StorageEngine>(
    scope: &Scope,
    negated: bool,
    expr: AstExpr,
    pattern: AstExpr,
    escape_char: Option<String>,
    binder: &Binder<E>,
) -> Result<Predicate> {
    if escape_char.is_some() {
        return Err(Error::SqlParse(
            "binder: LIKE ... ESCAPE is not supported".into(),
        ));
    }
    let pattern = match pattern {
        AstExpr::Value(AstValue::SingleQuotedString(s)) => s,
        other => {
            return Err(Error::SqlParse(format!(
                "binder: LIKE pattern must be a string literal, got {other:?}"
            )))
        }
    };
    let operand = bind_expression(scope, expr, binder)?;
    // The operand must be a string; refuse anything else at bind time rather
    // than let a non-string silently evaluate to UNKNOWN every row.
    match operand.column_type(&scope_column_types(scope)) {
        Some(ColumnType::Varchar(_) | ColumnType::Char(_)) => {}
        other => {
            return Err(Error::SqlParse(format!(
                "binder: LIKE requires a Varchar/Char operand, got {other:?}"
            )))
        }
    }
    Ok(maybe_negate(
        Predicate::Like {
            expr: operand,
            pattern,
        },
        negated,
    ))
}

/// When one comparison operand is a column and the other a literal, narrow
/// the literal to the column's type — but only when the conversion is
/// value-preserving (`Value::coerce_exact`): `WHERE int32_col = 5` carries
/// Int32(5) into plan lowering, `WHERE char_col = 'x'` a Char. A literal no
/// value of the column type can equal (`int32_col = 5000000000`) is left
/// untouched: it correctly matches nothing at runtime instead of erroring.
fn narrow_compare_operands(
    scope: &Scope,
    l: Expression,
    r: Expression,
) -> (Expression, Expression) {
    match (&l, &r) {
        (Expression::Column(i), Expression::Literal(v)) => {
            match column_type_at(scope, *i).and_then(|ty| v.coerce_exact(&ty)) {
                Some(narrowed) => (l, Expression::Literal(narrowed)),
                None => (l, r),
            }
        }
        (Expression::Literal(v), Expression::Column(i)) => {
            match column_type_at(scope, *i).and_then(|ty| v.coerce_exact(&ty)) {
                Some(narrowed) => (Expression::Literal(narrowed), r),
                None => (l, r),
            }
        }
        _ => (l, r),
    }
}

/// Column type at joined-tuple position `idx`, resolved through the
/// scope's table layout. `None` when out of range — the caller leaves the
/// operand untouched.
fn column_type_at(scope: &Scope, idx: usize) -> Option<ColumnType> {
    for t in &scope.tables {
        if idx >= t.column_offset && idx < t.column_offset + t.schema.columns.len() {
            return Some(t.schema.columns[idx - t.column_offset].ty);
        }
    }
    None
}

fn map_arith_op(op: &BinaryOperator) -> Result<BinaryOp> {
    match op {
        BinaryOperator::Plus => Ok(BinaryOp::Add),
        BinaryOperator::Minus => Ok(BinaryOp::Sub),
        BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        BinaryOperator::Divide => Ok(BinaryOp::Div),
        other => Err(Error::SqlParse(format!(
            "binder: arithmetic op {:?} unsupported",
            other
        ))),
    }
}

fn map_compare_op(op: &BinaryOperator) -> Result<CompareOp> {
    match op {
        BinaryOperator::Eq => Ok(CompareOp::Eq),
        BinaryOperator::NotEq => Ok(CompareOp::Neq),
        BinaryOperator::Lt => Ok(CompareOp::Lt),
        BinaryOperator::LtEq => Ok(CompareOp::Lte),
        BinaryOperator::Gt => Ok(CompareOp::Gt),
        BinaryOperator::GtEq => Ok(CompareOp::Gte),
        other => Err(Error::SqlParse(format!(
            "binder: comparison op {:?} unsupported",
            other
        ))),
    }
}

// -------- DataType mapping --------------------------------------------------

fn map_data_type(t: &AstDataType) -> Result<ColumnType> {
    match t {
        AstDataType::Int(_) | AstDataType::Integer(_) => Ok(ColumnType::Int32),
        AstDataType::BigInt(_) => Ok(ColumnType::Int64),
        AstDataType::Varchar(n) => Ok(ColumnType::Varchar(character_length(n.as_ref()))),
        AstDataType::Char(n) | AstDataType::Character(n) => {
            Ok(ColumnType::Char(character_length(n.as_ref())))
        }
        AstDataType::Boolean | AstDataType::Bool => Ok(ColumnType::Boolean),
        AstDataType::Decimal(info) | AstDataType::Numeric(info) => {
            let (precision, scale) = decimal_args(info)?;
            Ok(ColumnType::Decimal { precision, scale })
        }
        AstDataType::Timestamp(_, _) => Ok(ColumnType::Timestamp),
        AstDataType::Date => Ok(ColumnType::Date),
        other => Err(Error::SqlParse(format!(
            "binder: data type {:?} unsupported",
            other
        ))),
    }
}

fn character_length(n: Option<&ast::CharacterLength>) -> u16 {
    match n {
        Some(ast::CharacterLength::IntegerLength { length, .. }) => *length as u16,
        Some(ast::CharacterLength::Max) | None => 255,
    }
}

/// Recursively narrow Int64 literals to Int32 when the target type is
/// Int32. Used by UPDATE SET binding to coerce the unconstrained-default
/// Int64 literal type to match the target column. Only narrows when the
/// value fits — out-of-range values are left as Int64 and surface as a
/// type-mismatch error at update time, matching SQL semantics for
/// `SET col = <too-big-literal>`.
fn narrow_expression(e: Expression, target: &ColumnType) -> Expression {
    match e {
        Expression::Literal(v) => Expression::Literal(narrow_value(v, target)),
        Expression::BinaryOp { op, left, right } => {
            let l = narrow_expression(*left, target);
            let r = narrow_expression(*right, target);
            Expression::BinaryOp {
                op,
                left: Box::new(l),
                right: Box::new(r),
            }
        }
        other => other,
    }
}

fn narrow_value(v: Value, target: &ColumnType) -> Value {
    match (v, target) {
        (Value::Int64(n), ColumnType::Int32)
            if (i32::MIN as i64..=i32::MAX as i64).contains(&n) =>
        {
            Value::Int32(n as i32)
        }
        (other, _) => other,
    }
}

fn decimal_args(info: &ast::ExactNumberInfo) -> Result<(u8, u8)> {
    // Keep these as the parser's `u64` until validated — casting to `u8`
    // first would alias out-of-range values (e.g. `274 as u8 == 18`) past
    // the bounds check.
    let (precision, scale): (u64, u64) = match info {
        ast::ExactNumberInfo::None => (Decimal::MAX_PRECISION as u64, 0),
        ast::ExactNumberInfo::Precision(p) => (*p, 0),
        ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p, *s),
    };
    // The on-disk Decimal is i64-backed (Decimal::MAX_PRECISION / MAX_SCALE).
    // Validate at the SQL surface so an out-of-range type can never reach
    // `Decimal::from_i64_with_scale`, whose precondition `assert!` would
    // otherwise crash the process on a later INSERT or read.
    if precision < 1 || precision > Decimal::MAX_PRECISION as u64 {
        return Err(Error::SqlParse(format!(
            "DECIMAL precision {} out of range 1..={}",
            precision,
            Decimal::MAX_PRECISION
        )));
    }
    if scale > precision {
        return Err(Error::SqlParse(format!(
            "DECIMAL scale {} exceeds precision {}",
            scale, precision
        )));
    }
    Ok((precision as u8, scale as u8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::engines::btree::BTreeEngine;
    use crate::sql::frontend::parse;
    use crate::storage::FileDiskManager;
    use tempfile::TempDir;

    fn fresh_catalog() -> (Binder<BTreeEngine>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Arc::new(Catalog::open(engine).unwrap());
        (Binder::new(catalog), dir)
    }

    fn bind_first(binder: &Binder<BTreeEngine>, sql: &str) -> LogicalPlan {
        let stmts = parse(sql).unwrap();
        binder.bind(stmts.into_iter().next().unwrap()).unwrap()
    }

    // ---- CREATE TABLE ----

    #[test]
    fn create_table_inline_pk() {
        let (binder, _dir) = fresh_catalog();
        let plan = bind_first(
            &binder,
            "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))",
        );
        match plan {
            LogicalPlan::CreateTable {
                name,
                columns,
                primary_key,
            } => {
                assert_eq!(name, "t");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].ty, ColumnType::Int32);
                assert_eq!(columns[1].name, "name");
                assert_eq!(columns[1].ty, ColumnType::Varchar(20));
                assert_eq!(primary_key, vec![0]);
            }
            other => panic!("expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn create_table_decimal_and_table_pk() {
        let (binder, _dir) = fresh_catalog();
        let plan = bind_first(
            &binder,
            "CREATE TABLE warehouse (\
                 w_id INT NOT NULL, \
                 w_ytd DECIMAL(12,2) NOT NULL, \
                 PRIMARY KEY (w_id))",
        );
        match plan {
            LogicalPlan::CreateTable {
                columns,
                primary_key,
                ..
            } => {
                assert_eq!(columns[0].ty, ColumnType::Int32);
                assert!(!columns[0].nullable);
                assert_eq!(
                    columns[1].ty,
                    ColumnType::Decimal {
                        precision: 12,
                        scale: 2
                    }
                );
                assert_eq!(primary_key, vec![0]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn create_table_rejects_decimal_scale_over_max() {
        // Q-31 regression: DECIMAL with precision/scale > 18 was accepted by
        // the binder, then panicked `Decimal::from_i64_with_scale`'s assert on
        // a subsequent INSERT/read. Found by the Q-29 cargo-fuzz targets
        // (`tuple_decode` / `keyenc_decode`), which fed raw u8 scales.
        let (binder, _dir) = fresh_catalog();
        let stmts = parse("CREATE TABLE t (x DECIMAL(30, 25), id INT, PRIMARY KEY (id))").unwrap();
        let err = binder
            .bind(stmts.into_iter().next().unwrap())
            .expect_err("DECIMAL(30,25) must be rejected at bind time");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("DECIMAL"),
            "expected a DECIMAL range error, got: {msg}"
        );
    }

    #[test]
    fn create_table_accepts_decimal_at_max_scale() {
        // The boundary (scale == precision == MAX_SCALE) must still bind.
        let (binder, _dir) = fresh_catalog();
        let plan = bind_first(
            &binder,
            "CREATE TABLE t (x DECIMAL(18, 18) NOT NULL, id INT, PRIMARY KEY (id))",
        );
        match plan {
            LogicalPlan::CreateTable { columns, .. } => assert_eq!(
                columns[0].ty,
                ColumnType::Decimal {
                    precision: 18,
                    scale: 18
                }
            ),
            other => panic!("expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn create_table_requires_primary_key() {
        let (binder, _dir) = fresh_catalog();
        let stmts = parse("CREATE TABLE t (id INT)").unwrap();
        let err = binder.bind(stmts.into_iter().next().unwrap()).unwrap_err();
        assert!(matches!(err, Error::SqlParse(ref m) if m.contains("PRIMARY KEY")));
    }

    // Helper: install a warehouse schema so the remaining tests can resolve names.
    fn binder_with_warehouse() -> (Binder<BTreeEngine>, TempDir) {
        let (binder, dir) = fresh_catalog();
        let plan = bind_first(
            &binder,
            "CREATE TABLE warehouse (\
                 w_id INT NOT NULL, \
                 w_ytd DECIMAL(12,2) NOT NULL, \
                 w_name VARCHAR(10) NOT NULL, \
                 PRIMARY KEY (w_id))",
        );
        // We need to actually create the table in the catalog so subsequent
        // binds can resolve it. Run the CreateTable plan through the catalog.
        match plan {
            LogicalPlan::CreateTable {
                name,
                columns,
                primary_key,
            } => {
                let schema = Schema {
                    name: name.clone(),
                    table_id: crate::catalog::TableId(0), // will be assigned by catalog
                    columns,
                    primary_key,
                };
                binder.catalog.create_table(name, schema).unwrap();
            }
            _ => panic!(),
        }
        (binder, dir)
    }

    // ---- INSERT ----

    #[test]
    fn insert_values_in_schema_order() {
        let (binder, _dir) = binder_with_warehouse();
        let plan = bind_first(
            &binder,
            "INSERT INTO warehouse VALUES (1, 1000.00, 'north')",
        );
        match plan {
            LogicalPlan::Insert { table, rows } => {
                assert_eq!(table, "warehouse");
                assert_eq!(rows.len(), 1);
                assert!(matches!(&rows[0][0], Expression::Literal(Value::Int32(1))));
                assert!(matches!(
                    &rows[0][1],
                    Expression::Literal(Value::Decimal(d)) if d.mantissa() == 100000 && d.scale() == 2
                ));
                assert!(matches!(
                    &rows[0][2],
                    Expression::Literal(Value::Varchar(s)) if s == "north"
                ));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn insert_with_column_list_reorders() {
        let (binder, _dir) = binder_with_warehouse();
        let plan = bind_first(
            &binder,
            "INSERT INTO warehouse (w_name, w_id, w_ytd) VALUES ('south', 2, 500.00)",
        );
        match plan {
            LogicalPlan::Insert { rows, .. } => {
                // Always materialized in schema order [w_id, w_ytd, w_name].
                assert!(matches!(&rows[0][0], Expression::Literal(Value::Int32(2))));
                assert!(matches!(
                    &rows[0][2],
                    Expression::Literal(Value::Varchar(s)) if s == "south"
                ));
            }
            _ => panic!(),
        }
    }

    // ---- SELECT ----

    #[test]
    fn select_star_no_where_no_limit() {
        let (binder, _dir) = binder_with_warehouse();
        let plan = bind_first(&binder, "SELECT * FROM warehouse");
        match plan {
            LogicalPlan::Select {
                table,
                joins,
                derived,
                scalar_subqueries: _,
                in_subqueries: _,
                projection,
                aggregates,
                select_list,
                filter,
                order_by,
                having,
                limit,
            } => {
                assert_eq!(table, "warehouse");
                assert!(joins.is_empty());
                assert!(derived.is_empty());
                assert!(aggregates.is_empty());
                assert!(order_by.is_empty());
                assert!(projection.is_empty(), "SELECT * → empty projection");
                assert!(select_list.is_empty(), "SELECT * → identity display");
                assert!(filter.is_none());
                assert!(having.is_none());
                assert!(limit.is_none());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn select_with_projection_where_and_limit() {
        let (binder, _dir) = binder_with_warehouse();
        let plan = bind_first(
            &binder,
            "SELECT w_name, w_ytd FROM warehouse WHERE w_id = 1 LIMIT 5",
        );
        match plan {
            LogicalPlan::Select {
                projection,
                filter,
                limit,
                ..
            } => {
                assert_eq!(projection, vec![2, 1]);
                assert!(matches!(filter, Some(Predicate::Compare { .. })));
                assert_eq!(limit, Some(5));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn select_where_and_or_not_compose() {
        let (binder, _dir) = binder_with_warehouse();
        let plan = bind_first(
            &binder,
            "SELECT w_id FROM warehouse WHERE w_id > 0 AND NOT (w_id = 5 OR w_id = 7)",
        );
        match plan {
            LogicalPlan::Select {
                filter: Some(Predicate::And(_, b)),
                ..
            } => {
                assert!(matches!(*b, Predicate::Not(_)));
            }
            _ => panic!("expected And(_, Not(...))"),
        }
    }

    #[test]
    fn select_unknown_column_errors() {
        let (binder, _dir) = binder_with_warehouse();
        let stmts = parse("SELECT bogus FROM warehouse").unwrap();
        let err = binder.bind(stmts.into_iter().next().unwrap()).unwrap_err();
        assert!(matches!(err, Error::SqlParse(ref m) if m.contains("bogus")));
    }

    #[test]
    fn select_in_list_over_max_items_errors() {
        // The IN-list desugars to an Or-chain whose depth equals the item
        // count, and the predicate passes that later walk it recurse that
        // deep, so the binder refuses a list past MAX_IN_LIST_ITEMS (1000)
        // rather than risk a native-stack overflow. A 1001-item list
        // (`0..=1000`) trips the named refusal. No 1001-item slt exists (it
        // would be unwieldy); this unit test is the guard instead.
        let (binder, _dir) = binder_with_warehouse();
        let items = (0..=1000)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT w_id FROM warehouse WHERE w_id IN ({items})");
        let stmts = parse(&sql).unwrap();
        let err = binder.bind(stmts.into_iter().next().unwrap()).unwrap_err();
        assert!(
            matches!(err, Error::SqlParse(ref m)
                if m.contains("MAX_IN_LIST_ITEMS") && m.contains("1001") && m.contains("1000")),
            "expected a MAX_IN_LIST_ITEMS refusal naming the count and limit, got: {err:?}"
        );
    }

    // ---- UPDATE ----

    #[test]
    fn update_set_with_compound_expr() {
        // TPC-C Payment shape: UPDATE warehouse SET w_ytd = w_ytd + 100 WHERE w_id = 1
        let (binder, _dir) = binder_with_warehouse();
        let plan = bind_first(
            &binder,
            "UPDATE warehouse SET w_ytd = w_ytd + 100.00 WHERE w_id = 1",
        );
        match plan {
            LogicalPlan::Update {
                set_clauses,
                filter,
                ..
            } => {
                assert_eq!(set_clauses.len(), 1);
                assert_eq!(set_clauses[0].0, 1); // w_ytd is index 1
                assert!(matches!(
                    set_clauses[0].1,
                    Expression::BinaryOp {
                        op: BinaryOp::Add,
                        ..
                    }
                ));
                assert!(filter.is_some());
            }
            _ => panic!(),
        }
    }

    // ---- DELETE ----

    #[test]
    fn delete_with_where() {
        let (binder, _dir) = binder_with_warehouse();
        let plan = bind_first(&binder, "DELETE FROM warehouse WHERE w_id = 99");
        match plan {
            LogicalPlan::Delete { table, filter } => {
                assert_eq!(table, "warehouse");
                assert!(filter.is_some());
            }
            _ => panic!(),
        }
    }

    // ---- Transaction control + EXPLAIN ----

    #[test]
    fn transaction_control_binds() {
        let (binder, _dir) = fresh_catalog();
        assert!(matches!(
            bind_first(&binder, "BEGIN"),
            LogicalPlan::BeginTxn
        ));
        assert!(matches!(
            bind_first(&binder, "COMMIT"),
            LogicalPlan::CommitTxn
        ));
        assert!(matches!(
            bind_first(&binder, "ROLLBACK"),
            LogicalPlan::AbortTxn
        ));
    }

    #[test]
    fn explain_wraps_inner_plan() {
        let (binder, _dir) = binder_with_warehouse();
        let plan = bind_first(&binder, "EXPLAIN SELECT w_id FROM warehouse");
        match plan {
            LogicalPlan::Explain(inner) => {
                assert!(matches!(*inner, LogicalPlan::Select { .. }));
            }
            _ => panic!(),
        }
    }

    // ---- Unsupported shapes ----

    #[test]
    fn self_join_binds_with_alias_and_qualified_predicate() {
        // Replaces the legacy "JOINs reject" test — P13.1 adds the JOIN
        // surface. Self-join with aliases is the strictest qualification
        // test: same table on both sides, so resolution must rely on
        // aliases (`w` vs `w2`) to disambiguate columns.
        let (binder, _dir) = binder_with_warehouse();
        let stmts =
            parse("SELECT * FROM warehouse w JOIN warehouse w2 ON w.w_id = w2.w_id").unwrap();
        let plan = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
        match plan {
            LogicalPlan::Select { table, joins, .. } => {
                assert_eq!(table, "warehouse");
                assert_eq!(joins.len(), 1);
                assert_eq!(joins[0].right_table, "warehouse");
                assert_eq!(joins[0].right_alias.as_deref(), Some("w2"));
                let on = joins[0].on.as_ref().expect("ON predicate");
                match on {
                    Predicate::Compare {
                        op: crate::sql::ir::expr::CompareOp::Eq,
                        left,
                        right,
                    } => {
                        // Left is `w.w_id` (column 0 of warehouse; tuple
                        // offset 0). Right is `w2.w_id` (column 0 of the
                        // second `warehouse`; tuple offset 3 — warehouse
                        // has 3 columns).
                        match (left, right) {
                            (Expression::Column(l), Expression::Column(r)) => {
                                assert_eq!(*l, 0);
                                assert_eq!(*r, 3);
                            }
                            _ => panic!("expected Column-Column ON"),
                        }
                    }
                    _ => panic!("expected Eq ON predicate"),
                }
            }
            _ => panic!(),
        }
    }
}
