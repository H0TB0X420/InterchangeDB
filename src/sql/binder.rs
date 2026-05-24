//! Binder — sqlparser AST → catalog-resolved `LogicalPlan`.
//!
//! Resolves table & column names against `Catalog<E>`, maps sqlparser data
//! types and literals to our `ColumnType` / `Value`, and rejects shapes we
//! don't yet support with `Error::SqlParse`.
//!
//! Phase 11 surface — what binds:
//!   CREATE TABLE, INSERT VALUES, SELECT, UPDATE, DELETE,
//!   BEGIN / COMMIT / ROLLBACK, EXPLAIN <stmt>.
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
use crate::sql::logical::LogicalPlan;
use crate::storage::StorageEngine;
use crate::types::{ColumnType, Decimal, Value};

/// Bind one SQL statement against the catalog.
pub struct Binder<E: StorageEngine> {
    catalog: Arc<Catalog<E>>,
}

/// The single-table scope a statement's expressions resolve against.
/// Phase 12 will generalize this to a join tree.
struct Scope {
    table_name: String,
    schema: Arc<Schema>,
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
                matches!(opt.option, ColumnOption::Unique { is_primary: true, .. })
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
            if let TableConstraint::PrimaryKey { columns: pk_cols, .. } = c {
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
                    | ColumnOption::Unique { is_primary: true, .. }
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
            Some(AstExpr::Value(AstValue::Number(n, _))) => Some(
                n.parse::<usize>()
                    .map_err(|e| Error::SqlParse(format!("LIMIT must be a non-negative integer: {}", e)))?,
            ),
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

        // FROM clause — exactly one table.
        if select.from.len() != 1 {
            return Err(Error::SqlParse(
                "binder: SELECT requires exactly one FROM table (joins are Phase 12)".into(),
            ));
        }
        let twj = &select.from[0];
        if !twj.joins.is_empty() {
            return Err(Error::SqlParse(
                "binder: JOINs are not yet supported (Phase 12)".into(),
            ));
        }
        let table_name = match &twj.relation {
            TableFactor::Table { name, .. } => object_name_to_string(name),
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: only named tables in FROM, got {:?}",
                    other
                )))
            }
        };
        let schema = self.catalog.get_table(&table_name)?;
        let scope = Scope {
            table_name: table_name.clone(),
            schema: schema.clone(),
        };

        // Projection: empty Vec = SELECT *.
        let projection = bind_projection(&scope, &select.projection)?;

        // WHERE clause.
        let filter = match select.selection {
            Some(e) => Some(bind_predicate(&scope, e)?),
            None => None,
        };

        Ok(LogicalPlan::Select {
            table: table_name,
            projection,
            filter,
            limit,
        })
    }

    // -----------------------------------------------------------------------
    // INSERT
    // -----------------------------------------------------------------------

    fn bind_insert(&self, ins: ast::Insert) -> Result<LogicalPlan> {
        let table_name = object_name_to_string(&ins.table_name);
        let schema = self.catalog.get_table(&table_name)?;

        let source = ins.source.ok_or_else(|| {
            Error::SqlParse("binder: INSERT without source not supported".into())
        })?;
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

        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(values.len());
        for row in values {
            if row.len() != col_indices.len() {
                return Err(Error::SqlParse(format!(
                    "INSERT row arity {} doesn't match column list arity {}",
                    row.len(),
                    col_indices.len()
                )));
            }
            // Build a full-width row in schema order; fill specified columns,
            // leave others as NULL (column list semantics).
            let mut full = vec![Value::Null; schema.columns.len()];
            for (src_pos, expr) in row.into_iter().enumerate() {
                let dst = col_indices[src_pos];
                full[dst] = literal_from_expr(expr, &schema.columns[dst].ty)?;
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
        let scope = Scope {
            table_name: table_name.clone(),
            schema: schema.clone(),
        };

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
            let target_ty = scope.schema.columns[idx].ty.clone();
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
        let scope = Scope {
            table_name: table_name.clone(),
            schema,
        };

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

fn column_index(scope: &Scope, name: &str) -> Result<usize> {
    scope
        .schema
        .columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| {
            Error::SqlParse(format!(
                "column '{}' not found in table '{}'",
                name, scope.table_name
            ))
        })
}

/// SELECT projection: empty Vec encodes `SELECT *`.
fn bind_projection(scope: &Scope, items: &[ast::SelectItem]) -> Result<Vec<usize>> {
    // Single Wildcard → SELECT *.
    if items.len() == 1 && matches!(items[0], ast::SelectItem::Wildcard(_)) {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        match it {
            ast::SelectItem::UnnamedExpr(AstExpr::Identifier(ident)) => {
                out.push(column_index(scope, &ident.value)?);
            }
            ast::SelectItem::ExprWithAlias {
                expr: AstExpr::Identifier(ident),
                ..
            } => {
                out.push(column_index(scope, &ident.value)?);
            }
            other => {
                return Err(Error::SqlParse(format!(
                    "binder: projection item shape unsupported: {:?}",
                    other
                )))
            }
        }
    }
    Ok(out)
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
                Ok(Expression::Literal(
                    ast_value_to_value_unconstrained(AstValue::Number(neg, false))?,
                ))
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
            // For Phase 11 single-table scope, ignore table qualifier;
            // resolve the column part only.
            let col = parts
                .last()
                .ok_or_else(|| Error::SqlParse("empty compound identifier".into()))?;
            let idx = column_index(scope, &col.value)?;
            Ok(Expression::Column(idx))
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
                Err(Error::SqlParse(format!("cannot parse number literal: {}", n)))
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
    match info {
        ast::ExactNumberInfo::None => Ok((18, 0)),
        ast::ExactNumberInfo::Precision(p) => Ok((*p as u8, 0)),
        ast::ExactNumberInfo::PrecisionAndScale(p, s) => Ok((*p as u8, *s as u8)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::index::btree::BTreeEngine;
    use crate::sql::frontend::parse;
    use crate::storage::DiskManager;
    use tempfile::TempDir;

    fn fresh_catalog() -> (Binder<BTreeEngine>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::create(&path).unwrap();
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
        let plan = bind_first(&binder, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))");
        match plan {
            LogicalPlan::CreateTable { name, columns, primary_key } => {
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
            LogicalPlan::CreateTable { columns, primary_key, .. } => {
                assert_eq!(columns[0].ty, ColumnType::Int32);
                assert!(!columns[0].nullable);
                assert_eq!(
                    columns[1].ty,
                    ColumnType::Decimal { precision: 12, scale: 2 }
                );
                assert_eq!(primary_key, vec![0]);
            }
            _ => panic!(),
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
            LogicalPlan::CreateTable { name, columns, primary_key } => {
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
                assert_eq!(rows[0][0], Value::Int32(1));
                assert_eq!(
                    rows[0][1],
                    Value::Decimal(Decimal::from_i64_with_scale(100000, 2))
                );
                assert_eq!(rows[0][2], Value::Varchar("north".into()));
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
                assert_eq!(rows[0][0], Value::Int32(2));
                assert_eq!(rows[0][2], Value::Varchar("south".into()));
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
            LogicalPlan::Select { table, projection, filter, limit } => {
                assert_eq!(table, "warehouse");
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
            LogicalPlan::Select { projection, filter, limit, .. } => {
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
            LogicalPlan::Select { filter: Some(Predicate::And(_, b)), .. } => {
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
            LogicalPlan::Update { set_clauses, filter, .. } => {
                assert_eq!(set_clauses.len(), 1);
                assert_eq!(set_clauses[0].0, 1); // w_ytd is index 1
                assert!(matches!(
                    set_clauses[0].1,
                    Expression::BinaryOp { op: BinaryOp::Add, .. }
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
    fn join_in_select_rejected() {
        let (binder, _dir) = binder_with_warehouse();
        let stmts =
            parse("SELECT * FROM warehouse w JOIN warehouse w2 ON w.w_id = w2.w_id").unwrap();
        let err = binder.bind(stmts.into_iter().next().unwrap()).unwrap_err();
        assert!(matches!(err, Error::SqlParse(ref m) if m.contains("JOIN")));
    }
}
