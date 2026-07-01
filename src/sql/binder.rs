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

use crate::catalog::{Catalog, ColumnDef, Schema};
use crate::common::{Error, Result};
use crate::sql::expr::{BinaryOp, CompareOp, Expression, Predicate};
use crate::sql::logical::{AggregateSpec, LogicalPlan, OrderDir};
use crate::storage::StorageEngine;
use crate::types::{ColumnType, Decimal, Value};

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
}

struct ScopedTable {
    /// Original catalog table name (used for qualified refs like `t.col`
    /// when no alias is given).
    table_name: String,
    /// FROM-clause alias, if any. Qualified refs prefer alias over name.
    alias: Option<String>,
    schema: Arc<Schema>,
    /// Start index of this table's columns in the joined tuple.
    column_offset: usize,
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
        }
    }

    /// Append another table to the scope. The new table's
    /// `column_offset` is the sum of all prior tables' column counts.
    fn push(&mut self, table_name: String, alias: Option<String>, schema: Arc<Schema>) {
        let column_offset: usize = self.tables.iter().map(|t| t.schema.columns.len()).sum();
        self.tables.push(ScopedTable {
            table_name,
            alias,
            schema,
            column_offset,
        });
    }
}

impl<E: StorageEngine> Binder<E> {
    pub fn new(catalog: Arc<Catalog<E>>) -> Self {
        Self { catalog }
    }

    pub fn bind(&self, stmt: Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::CreateTable(ct) => self.bind_create_table(ct),
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
        let mut scope = Scope { tables: Vec::new() };
        let mut joined_tables: Vec<(String, Option<String>, Option<AstExpr>)> = Vec::new();

        for (twj_idx, twj) in select.from.into_iter().enumerate() {
            // The "head" relation of this TableWithJoins.
            let (head_name, head_alias) = extract_table_and_alias(&twj.relation)?;
            let head_schema = self.catalog.get_table(&head_name)?;
            if twj_idx == 0 {
                scope.push(head_name.clone(), head_alias.clone(), head_schema);
            } else {
                // Implicit cross join with the running scope.
                scope.push(head_name.clone(), head_alias.clone(), head_schema);
                joined_tables.push((head_name, head_alias, None));
            }

            // Explicit joins attached to this entry.
            for j in twj.joins {
                let (right_name, right_alias) = extract_table_and_alias(&j.relation)?;
                let right_schema = self.catalog.get_table(&right_name)?;
                // The ON predicate (when present) must bind against a scope
                // that already includes the right side, so push first.
                scope.push(right_name.clone(), right_alias.clone(), right_schema);

                let on_expr = match j.join_operator {
                    ast::JoinOperator::Inner(ast::JoinConstraint::On(e)) => Some(e),
                    ast::JoinOperator::Inner(ast::JoinConstraint::None) => None,
                    ast::JoinOperator::CrossJoin => None,
                    other => {
                        return Err(Error::SqlParse(format!(
                            "binder: only INNER JOIN and CROSS JOIN supported, got {:?}",
                            other
                        )))
                    }
                };
                joined_tables.push((right_name, right_alias, on_expr));
            }
        }

        // The first scoped table becomes the LogicalPlan's `table`.
        let table_name = scope.tables[0].table_name.clone();

        // Bind each join's ON predicate against the scope built up to
        // that join's position. Easiest: bind against the *final* scope
        // — predicates over earlier tables still work, and over later
        // tables that haven't been joined yet would be a forward
        // reference (unusual, the binder rejects via column-not-found).
        let mut joins: Vec<crate::sql::logical::JoinClause> =
            Vec::with_capacity(joined_tables.len());
        for (right_table, right_alias, on_expr) in joined_tables {
            let on = match on_expr {
                Some(e) => Some(bind_predicate(&scope, e)?),
                None => None,
            };
            joins.push(crate::sql::logical::JoinClause {
                right_table,
                right_alias,
                on,
            });
        }

        // Projection + aggregates: empty Vec / empty Vec = SELECT *.
        let (projection, aggregates) = bind_select_items(&scope, &select.projection)?;

        // WHERE clause.
        let filter = match select.selection {
            Some(e) => Some(bind_predicate(&scope, e)?),
            None => None,
        };

        // P13.6: ORDER BY. sqlparser exposes `Option<OrderBy>` on Query;
        // each `OrderByExpr` has the expression + an optional ASC/DESC
        // flag (None defaults to ASC).
        let order_by = match q.order_by {
            Some(ob) => {
                let mut keys: Vec<(usize, OrderDir)> = Vec::with_capacity(ob.exprs.len());
                for obe in ob.exprs {
                    let col = resolve_column_expr(&scope, &obe.expr)?;
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

        Ok(LogicalPlan::Select {
            table: table_name,
            joins,
            projection,
            aggregates,
            filter,
            order_by,
            limit,
        })
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
            let expr = bind_expression(&scope, a.value)?;
            // The SET result must match the target column's type — Table's
            // update_columns rejects type mismatch outright. Narrow Int64
            // literals (the unconstrained-default for whole numbers) down
            // to Int32 when the target is Int32, recursively through any
            // BinaryOp so `col = col + 1` works on Int32 columns too.
            let expr = narrow_expression(expr, &target_ty);
            set_clauses.push((idx, expr));
        }

        let filter = match selection {
            Some(e) => Some(bind_predicate(&scope, e)?),
            None => None,
        };

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

        let filter = match del.selection {
            Some(e) => Some(bind_predicate(&scope, e)?),
            None => None,
        };

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

/// Extract the table name and optional alias from a `TableFactor`. Used
/// when walking a `TableWithJoins`. Errors on derived tables, subqueries,
/// table functions — those land in later phases.
fn extract_table_and_alias(tf: &TableFactor) -> Result<(String, Option<String>)> {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_to_string(name);
            let alias = alias.as_ref().map(|a| a.name.value.clone());
            Ok((table_name, alias))
        }
        other => Err(Error::SqlParse(format!(
            "binder: FROM/JOIN must be a named table, got {:?}",
            other
        ))),
    }
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
    Err(Error::SqlParse(format!(
        "table or alias '{}' not in FROM scope",
        qualifier
    )))
}

/// Bind SELECT items into projection columns + aggregate specs.
///
/// Returns `(projection, aggregates)`:
/// - Both empty: `SELECT *`.
/// - Only projection non-empty: column projection (`SELECT a, b.c FROM …`).
/// - Only aggregates non-empty: whole-table aggregation
///   (`SELECT COUNT(*), SUM(x) FROM …`).
/// - Both non-empty: GROUP BY semantics, which Phase 13 doesn't support
///   (rejected here; lands in Phase 14).
fn bind_select_items(
    scope: &Scope,
    items: &[ast::SelectItem],
) -> Result<(Vec<usize>, Vec<AggregateSpec>)> {
    // Single Wildcard → SELECT *.
    if items.len() == 1 && matches!(items[0], ast::SelectItem::Wildcard(_)) {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut projection: Vec<usize> = Vec::new();
    let mut aggregates: Vec<AggregateSpec> = Vec::new();
    for it in items {
        let expr = match it {
            ast::SelectItem::UnnamedExpr(e) => e,
            ast::SelectItem::ExprWithAlias { expr, .. } => expr,
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: projection item shape unsupported: {:?}",
                    other
                )))
            }
        };
        match expr {
            AstExpr::Identifier(ident) => {
                projection.push(column_index(scope, &ident.value)?);
            }
            AstExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                projection.push(column_index_qualified(
                    scope,
                    &parts[0].value,
                    &parts[1].value,
                )?);
            }
            AstExpr::Function(func) => {
                aggregates.push(bind_aggregate_function(scope, func)?);
            }
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: projection expression shape unsupported: {:?}",
                    other
                )))
            }
        }
    }

    if !projection.is_empty() && !aggregates.is_empty() {
        return Err(Error::SqlParse(
            "binder: mixed column-and-aggregate projection requires GROUP BY (Phase 14)".into(),
        ));
    }
    Ok((projection, aggregates))
}

/// Translate a sqlparser `Function` AST node into our `AggregateSpec`.
/// Supported: `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`,
/// `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`. Anything else is a
/// non-aggregate function call — we error rather than silently treating
/// it as a row-level expression (no scalar-function support yet).
fn bind_aggregate_function(scope: &Scope, func: &ast::Function) -> Result<AggregateSpec> {
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
                    let col = resolve_column_expr(scope, e)?;
                    Ok(AggregateSpec::Count { col, distinct })
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
            let col = match &args[0] {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    resolve_column_expr(scope, e)?
                }
                other => {
                    return Err(Error::SqlParse(format!(
                        "binder: unsupported {} argument shape: {:?}",
                        name, other
                    )));
                }
            };
            Ok(match name.as_str() {
                "SUM" => AggregateSpec::Sum(col),
                "MIN" => AggregateSpec::Min(col),
                "MAX" => AggregateSpec::Max(col),
                "AVG" => AggregateSpec::Avg(col),
                _ => unreachable!(),
            })
        }
        other => Err(Error::SqlParse(format!(
            "binder: unsupported function '{}'",
            other
        ))),
    }
}

/// Aggregates only accept a column reference as the argument expression
/// — not arbitrary expressions (yet). This helper centralizes that
/// resolution and the corresponding error message.
fn resolve_column_expr(scope: &Scope, e: &AstExpr) -> Result<usize> {
    match e {
        AstExpr::Identifier(ident) => column_index(scope, &ident.value),
        AstExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
            column_index_qualified(scope, &parts[0].value, &parts[1].value)
        }
        other => Err(Error::SqlParse(format!(
            "binder: aggregate argument must be a column reference, got {:?}",
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

fn bind_expression(scope: &Scope, e: AstExpr) -> Result<Expression> {
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
                let inner = bind_expression(scope, other)?;
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
            let l = bind_expression(scope, *left)?;
            let r = bind_expression(scope, *right)?;
            Ok(Expression::BinaryOp {
                op: arith_op,
                left: Box::new(l),
                right: Box::new(r),
            })
        }
        AstExpr::Nested(inner) => bind_expression(scope, *inner),
        other => Err(Error::SqlParse(format!(
            "binder: expression shape unsupported: {:?}",
            other
        ))),
    }
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

// -------- predicate binding -------------------------------------------------

fn bind_predicate(scope: &Scope, e: AstExpr) -> Result<Predicate> {
    match e {
        AstExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let l = bind_predicate(scope, *left)?;
                let r = bind_predicate(scope, *right)?;
                Ok(Predicate::And(Box::new(l), Box::new(r)))
            }
            BinaryOperator::Or => {
                let l = bind_predicate(scope, *left)?;
                let r = bind_predicate(scope, *right)?;
                Ok(Predicate::Or(Box::new(l), Box::new(r)))
            }
            cmp => {
                let cmp_op = map_compare_op(&cmp)?;
                let l = bind_expression(scope, *left)?;
                let r = bind_expression(scope, *right)?;
                // O13: reconcile a literal operand with the compared
                // column's type here, where the type is known. Runtime
                // comparison is already exact across numeric
                // representations; the narrowing matters for the ACCESS
                // PATH — PkLookup/IndexScan lowering key-encodes the
                // literal against the column type strictly.
                let (l, r) = narrow_compare_operands(scope, l, r);
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
            let inner = bind_predicate(scope, *expr)?;
            Ok(Predicate::Not(Box::new(inner)))
        }
        AstExpr::Nested(inner) => bind_predicate(scope, *inner),
        other => Err(Error::SqlParse(format!(
            "binder: predicate shape unsupported: {:?}",
            other
        ))),
    }
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
    use crate::index::btree::BTreeEngine;
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
                projection,
                aggregates,
                filter,
                order_by,
                limit,
            } => {
                assert_eq!(table, "warehouse");
                assert!(joins.is_empty());
                assert!(aggregates.is_empty());
                assert!(order_by.is_empty());
                assert!(projection.is_empty(), "SELECT * → empty projection");
                assert!(filter.is_none());
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
                        op: crate::sql::expr::CompareOp::Eq,
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
