//! Expression and Predicate IR + closure compilation.
//!
//! Step 2 (types): `Expression`, `Predicate`, and their operator enums.
//! Step 3 (compile): consume the IR to produce closures that operators
//! drive per-tuple.
//!
//! Step 4's binder produces these from sqlparser's AST.
//!
//! ## Why columns are positional, not named
//!
//! After catalog resolution, every column reference is an index into the
//! source tuple. Storing names here would force the executor to do
//! per-tuple name lookups (slow) or carry a schema with each tuple
//! (memory). Indices are resolved once at plan time.
//!
//! ## NULL semantics
//!
//! - `BinaryOp` with any NULL operand → `Value::Null`.
//! - Predicates evaluate in Kleene three-valued logic: a comparison with a
//!   NULL operand (or an incomparable type pair) is UNKNOWN; `AND`/`OR`/
//!   `NOT` combine UNKNOWN per Kleene; the compiled `WHERE` closure keeps
//!   only TRUE rows. Collapsing UNKNOWN→false at the leaf would be wrong
//!   under `NOT`: `NOT (x = 5)` with `x` NULL must drop the row
//!   (`NOT UNKNOWN = UNKNOWN`), not keep it.
//! - Type mismatch → `Value::Null` for arithmetic, UNKNOWN for compare.
//! - Division by zero → `Value::Null`.

use serde::{Deserialize, Serialize};

use crate::types::Tuple;
use crate::types::{ColumnType, Decimal, Value};

/// An expression that evaluates to a `Value` against an input tuple.
/// `PartialEq`/`Eq` so the binder can match ORDER BY / HAVING aggregate
/// expressions against the SELECT list's specs (and so `AggregateSpec`,
/// which now carries expressions, keeps its derived `Eq`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expression {
    /// A constant value embedded in the plan (e.g., `42`, `'alice'`).
    Literal(Value),
    /// Index into the input tuple (column position in the child's schema).
    Column(usize),
    /// A parameter placeholder — `?` in SQL. The `usize` is the
    /// positional index (0-based) into the parameter array passed to
    /// `PreparedStatement::execute`. Must be substituted with a
    /// `Literal` before `compile()` is called.
    Parameter(usize),
    /// Binary arithmetic on two sub-expressions.
    BinaryOp {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
}

/// Arithmetic operator for `Expression::BinaryOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// A boolean predicate that evaluates against an input tuple.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Predicate {
    /// Comparison between two expressions.
    Compare {
        op: CompareOp,
        left: Expression,
        right: Expression,
    },
    /// Conjunction.
    And(Box<Predicate>, Box<Predicate>),
    /// Disjunction.
    Or(Box<Predicate>, Box<Predicate>),
    /// Logical negation.
    Not(Box<Predicate>),
}

/// Comparison operator for `Predicate::Compare`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// A compiled three-valued predicate: `None` is SQL UNKNOWN.
type Predicate3VL = Box<dyn Fn(&Tuple) -> Option<bool> + Send>;

// ===========================================================================
// Compilation: IR → closures
// ===========================================================================

impl Expression {
    /// Consume this expression, returning a closure that evaluates it
    /// against an input tuple. Boxing happens here so call sites pass a
    /// uniform type to operators.
    pub fn compile(self) -> Box<dyn Fn(&Tuple) -> Value + Send> {
        match self {
            Expression::Literal(v) => Box::new(move |_t| v.clone()),
            Expression::Column(i) => Box::new(move |t| t[i].clone()),
            Expression::Parameter(i) => {
                // Should have been substituted by PreparedStatement::execute
                // before reaching compile. Treat as NULL at runtime — keeps
                // closures total — but emit a debug_assert to surface the
                // bug during development.
                debug_assert!(false, "unsubstituted Parameter({}) reached compile()", i);
                Box::new(move |_t| Value::Null)
            }
            Expression::BinaryOp { op, left, right } => {
                let l = left.compile();
                let r = right.compile();
                Box::new(move |t| eval_binary_op(op, l(t), r(t)))
            }
        }
    }

    /// Recursively substitute `Parameter(i)` with `Literal(params[i])`.
    /// Used by `PreparedStatement::execute` to bind parameters before
    /// the plan is compiled. Returns an error if any parameter index is
    /// out of bounds.
    pub fn substitute_params(self, params: &[Value]) -> crate::common::Result<Expression> {
        match self {
            Expression::Parameter(i) => {
                params
                    .get(i)
                    .cloned()
                    .map(Expression::Literal)
                    .ok_or_else(|| {
                        crate::common::Error::SqlParse(format!(
                            "prepared statement: parameter ${} not bound (only {} provided)",
                            i + 1,
                            params.len()
                        ))
                    })
            }
            Expression::Literal(_) | Expression::Column(_) => Ok(self),
            Expression::BinaryOp { op, left, right } => Ok(Expression::BinaryOp {
                op,
                left: Box::new(left.substitute_params(params)?),
                right: Box::new(right.substitute_params(params)?),
            }),
        }
    }

    /// Static result type of this expression over a row of
    /// `column_types` — MUST mirror `eval_binary_op` exactly, or the
    /// aggregate accumulator (picked from this type) diverges from the
    /// values the closure produces at runtime. `None` = not statically
    /// inferable (NULL literal, unsubstituted parameter, or a type
    /// combination `eval_binary_op` resolves to NULL).
    pub fn column_type(&self, column_types: &[ColumnType]) -> Option<ColumnType> {
        match self {
            // `Null` literal → None (it has no intrinsic type).
            Expression::Literal(v) => v.type_of(),
            Expression::Column(i) => column_types.get(*i).copied(),
            Expression::Parameter(_) => None,
            Expression::BinaryOp { op, left, right } => {
                // None propagates: a NULL-typed sub-expression makes the
                // whole result NULL, exactly as `eval_binary_op` does.
                let l = left.column_type(column_types)?;
                let r = right.column_type(column_types)?;
                infer_binary_op_type(*op, l, r)
            }
        }
    }
}

/// The mirror of `eval_binary_op`'s type resolution: the static result
/// type of `l op r`, or `None` for any operand pair `eval_binary_op`
/// resolves to `Value::Null` (scale-mismatched Decimal Add/Sub, or an
/// unsupported type combination).
fn infer_binary_op_type(op: BinaryOp, l: ColumnType, r: ColumnType) -> Option<ColumnType> {
    use ColumnType::*;
    match (l, r) {
        (Int32, Int32) => Some(Int32),
        (Int64, Int64) => Some(Int64),
        (
            Decimal {
                precision,
                scale: s1,
            },
            Decimal { scale: s2, .. },
        ) => decimal_op_type(op, precision, s1, s2),
        // Int promotes to a scale-0 Decimal at the decimal side's
        // precision (see `eval_binary_op`'s Int×Decimal arms).
        (Int32 | Int64, Decimal { precision, scale }) => decimal_op_type(op, precision, 0, scale),
        (Decimal { precision, scale }, Int32 | Int64) => decimal_op_type(op, precision, scale, 0),
        // Every other pair is NULL in `eval_binary_op`.
        _ => None,
    }
}

/// Decimal-op scale algebra (`s1` = left scale, `s2` = right scale,
/// `precision` from the left operand), an exact mirror of the runtime
/// `Decimal` methods:
/// - Add/Sub require equal scales (else `None`, mirroring the runtime NULL —
///   `add`/`sub` reject a scale mismatch), keeping the shared scale.
/// - Mul promotes, summing the operand scales.
/// - Div is native max-scale: result scale = `max(s1, s2)`, no equal-scale
///   requirement (mirrors `Decimal::div`), so a scale-4 ÷ scale-2 division
///   (TPC-H Q14) infers a scale-4 result instead of bailing to `None`.
fn decimal_op_type(op: BinaryOp, precision: u8, s1: u8, s2: u8) -> Option<ColumnType> {
    let scale = match op {
        BinaryOp::Add | BinaryOp::Sub => {
            if s1 != s2 {
                return None;
            }
            s1
        }
        BinaryOp::Mul => {
            // Scales are individually catalog-bounded ≤ MAX_SCALE, so their
            // sum can't overflow the u8 — but (unlike Div's max()) the sum CAN
            // exceed MAX_SCALE, and `Decimal::mul` errors (→ NULL) in that case.
            // Inference must mirror that early-out or it predicts a Decimal type
            // the runtime resolves to NULL (the silent-NULL divergence class
            // this file's doc comment forbids).
            let sum = s1 + s2;
            if sum > Decimal::MAX_SCALE {
                return None;
            }
            sum
        }
        BinaryOp::Div => s1.max(s2),
    };
    Some(ColumnType::Decimal { precision, scale })
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // `c{i}` so a column reference can't be read as an integer
            // literal — `(c2 + 3)` distinguishes column 2 from the value 3.
            Expression::Column(i) => write!(f, "c{}", i),
            Expression::Literal(Value::Int32(n)) => write!(f, "{}", n),
            Expression::Literal(Value::Int64(n)) => write!(f, "{}", n),
            Expression::Literal(Value::Null) => write!(f, "NULL"),
            // `Value` has no `Display`; Debug is the honest fallback.
            Expression::Literal(v) => write!(f, "{:?}", v),
            // 1-based to match SQL's `$1` placeholder syntax.
            Expression::Parameter(i) => write!(f, "${}", i + 1),
            Expression::BinaryOp { op, left, right } => {
                let symbol = match op {
                    BinaryOp::Add => '+',
                    BinaryOp::Sub => '-',
                    BinaryOp::Mul => '*',
                    BinaryOp::Div => '/',
                };
                write!(f, "({} {} {})", left, symbol, right)
            }
        }
    }
}

impl Predicate {
    /// Recursively substitute parameters in nested expressions.
    pub fn substitute_params(self, params: &[Value]) -> crate::common::Result<Predicate> {
        match self {
            Predicate::Compare { op, left, right } => Ok(Predicate::Compare {
                op,
                left: left.substitute_params(params)?,
                right: right.substitute_params(params)?,
            }),
            Predicate::And(a, b) => Ok(Predicate::And(
                Box::new(a.substitute_params(params)?),
                Box::new(b.substitute_params(params)?),
            )),
            Predicate::Or(a, b) => Ok(Predicate::Or(
                Box::new(a.substitute_params(params)?),
                Box::new(b.substitute_params(params)?),
            )),
            Predicate::Not(p) => Ok(Predicate::Not(Box::new(p.substitute_params(params)?))),
        }
    }
}

impl Predicate {
    /// Compile to a `WHERE`-semantics closure: the row passes only when
    /// the predicate evaluates to TRUE — UNKNOWN and FALSE both drop it.
    pub fn compile(self) -> Box<dyn Fn(&Tuple) -> bool + Send> {
        let f = self.compile_3vl();
        Box::new(move |t| f(t) == Some(true))
    }

    /// Kleene three-valued compilation: `None` is UNKNOWN. The collapse
    /// to two values happens once, at the `WHERE` boundary in `compile` —
    /// never inside the tree, where `NOT` would invert a premature
    /// collapse into a wrong answer.
    fn compile_3vl(self) -> Predicate3VL {
        match self {
            Predicate::Compare { op, left, right } => {
                let l = left.compile();
                let r = right.compile();
                Box::new(move |t| eval_compare(op, &l(t), &r(t)))
            }
            Predicate::And(a, b) => {
                let a = a.compile_3vl();
                let b = b.compile_3vl();
                // Kleene AND: FALSE dominates (short-circuit), UNKNOWN
                // contaminates everything except FALSE.
                Box::new(move |t| {
                    let av = a(t);
                    if av == Some(false) {
                        return Some(false);
                    }
                    match (av, b(t)) {
                        (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    }
                })
            }
            Predicate::Or(a, b) => {
                let a = a.compile_3vl();
                let b = b.compile_3vl();
                // Kleene OR: TRUE dominates (short-circuit), UNKNOWN
                // contaminates everything except TRUE.
                Box::new(move |t| {
                    let av = a(t);
                    if av == Some(true) {
                        return Some(true);
                    }
                    match (av, b(t)) {
                        (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    }
                })
            }
            Predicate::Not(p) => {
                let p = p.compile_3vl();
                // NOT UNKNOWN = UNKNOWN — the case that forces real 3VL.
                Box::new(move |t| p(t).map(|b| !b))
            }
        }
    }
}

/// NULL-propagating arithmetic on `Value`. Type mismatch or div-by-zero
/// yields `Value::Null` rather than an error — closures are infallible
/// so SQL `WHERE` semantics are preserved (NULL → filtered row).
fn eval_binary_op(op: BinaryOp, l: Value, r: Value) -> Value {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Value::Null;
    }
    // NOTE (semantic, plan deviation): integer overflow yields NULL, not a
    // SQL-standard error — expression closures are infallible by design,
    // and NULL is this module's uniform "arithmetic failure" result
    // (div-by-zero and Decimal overflow already collapse to NULL via
    // `apply_decimal_op`). The prior `wrapping_*` silently returned a
    // WRONG NUMBER; NULL at least propagates as "no value" (O3/E13).
    match (l, r) {
        (Value::Int32(a), Value::Int32(b)) => match op {
            BinaryOp::Add => a.checked_add(b).map(Value::Int32).unwrap_or(Value::Null),
            BinaryOp::Sub => a.checked_sub(b).map(Value::Int32).unwrap_or(Value::Null),
            BinaryOp::Mul => a.checked_mul(b).map(Value::Int32).unwrap_or(Value::Null),
            // checked_div also covers i32::MIN / -1 (overflow), not just /0.
            BinaryOp::Div => a.checked_div(b).map(Value::Int32).unwrap_or(Value::Null),
        },
        (Value::Int64(a), Value::Int64(b)) => match op {
            BinaryOp::Add => a.checked_add(b).map(Value::Int64).unwrap_or(Value::Null),
            BinaryOp::Sub => a.checked_sub(b).map(Value::Int64).unwrap_or(Value::Null),
            BinaryOp::Mul => a.checked_mul(b).map(Value::Int64).unwrap_or(Value::Null),
            BinaryOp::Div => a.checked_div(b).map(Value::Int64).unwrap_or(Value::Null),
        },
        // Decimal × Decimal — uses the type's checked arithmetic. Scale
        // mismatch on add/sub/div surfaces as NULL (the planner is
        // responsible for inserting alignment when SQL requires it).
        (Value::Decimal(a), Value::Decimal(b)) => apply_decimal_op(op, &a, &b),
        // Int * Decimal (and symmetric) — TPC-C NewOrder needs this for
        // `ol_amount = ol_quantity * i_price`. We promote the Int to a
        // scale-0 Decimal so MUL produces the right sum-of-scales result.
        // ADD/SUB/DIV would scale-mismatch and yield NULL — that's correct
        // because mixing Int and Decimal additively requires an explicit
        // cast in real SQL.
        (Value::Int32(a), Value::Decimal(b)) => {
            apply_decimal_op(op, &Decimal::from_i64_with_scale(a as i64, 0), &b)
        }
        (Value::Int64(a), Value::Decimal(b)) => {
            apply_decimal_op(op, &Decimal::from_i64_with_scale(a, 0), &b)
        }
        (Value::Decimal(a), Value::Int32(b)) => {
            apply_decimal_op(op, &a, &Decimal::from_i64_with_scale(b as i64, 0))
        }
        (Value::Decimal(a), Value::Int64(b)) => {
            apply_decimal_op(op, &a, &Decimal::from_i64_with_scale(b, 0))
        }
        // Other type mismatches (e.g., Int32 + Varchar) — Phase 13 may
        // refuse these at bind time. For now: NULL keeps closures total.
        _ => Value::Null,
    }
}

/// Dispatch the four arithmetic ops to `Decimal`'s checked methods. Any
/// error (scale mismatch, overflow, div-by-zero) collapses to NULL.
fn apply_decimal_op(op: BinaryOp, a: &Decimal, b: &Decimal) -> Value {
    let result = match op {
        BinaryOp::Add => a.add(b),
        BinaryOp::Sub => a.sub(b),
        BinaryOp::Mul => a.mul(b),
        BinaryOp::Div => a.div(b),
    };
    match result {
        Ok(d) => Value::Decimal(d),
        Err(_) => Value::Null,
    }
}

/// Three-valued comparison over the canonical `Value::compare_sql` —
/// the single comparator Sort, MIN/MAX, and join-key matching share, so
/// every execution strategy answers comparisons identically. `None` is
/// UNKNOWN: a NULL operand, or a type pair no value coercion can relate
/// (Int vs Varchar). Covers the full orderable matrix — Int32↔Int64,
/// Int↔Decimal, cross-scale Decimal, Varchar/Char/Bytes/Timestamp/Boolean.
fn eval_compare(op: CompareOp, l: &Value, r: &Value) -> Option<bool> {
    use std::cmp::Ordering::*;
    let ord = l.compare_sql(r)?;
    Some(match op {
        CompareOp::Eq => ord == Equal,
        CompareOp::Neq => ord != Equal,
        CompareOp::Lt => ord == Less,
        CompareOp::Lte => ord != Greater,
        CompareOp::Gt => ord == Greater,
        CompareOp::Gte => ord != Less,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(i: usize) -> Expression {
        Expression::Column(i)
    }
    fn lit(v: Value) -> Expression {
        Expression::Literal(v)
    }
    fn add(l: Expression, r: Expression) -> Expression {
        binop(BinaryOp::Add, l, r)
    }
    fn binop(op: BinaryOp, l: Expression, r: Expression) -> Expression {
        Expression::BinaryOp {
            op,
            left: Box::new(l),
            right: Box::new(r),
        }
    }

    // ---- Expression ----

    #[test]
    fn literal_returns_its_value() {
        let f = lit(Value::Int32(7)).compile();
        assert_eq!(f(&vec![]), Value::Int32(7));
    }

    #[test]
    fn column_reads_from_tuple_by_index() {
        let f = col(1).compile();
        assert_eq!(
            f(&vec![Value::Int32(10), Value::Int64(20)]),
            Value::Int64(20)
        );
    }

    #[test]
    fn binary_op_add_int32() {
        // col(0) + 5
        let f = add(col(0), lit(Value::Int32(5))).compile();
        assert_eq!(f(&vec![Value::Int32(3)]), Value::Int32(8));
    }

    #[test]
    fn binary_op_div_by_zero_yields_null() {
        let f = Expression::BinaryOp {
            op: BinaryOp::Div,
            left: Box::new(lit(Value::Int32(10))),
            right: Box::new(lit(Value::Int32(0))),
        }
        .compile();
        assert_eq!(f(&vec![]), Value::Null);
    }

    #[test]
    fn int_overflow_yields_null_not_wrap() {
        // O3: checked arithmetic — overflow is "no value", never a wrapped
        // wrong number. (See the NOTE in eval_binary_op for why NULL
        // rather than a SQL error.)
        let f = add(lit(Value::Int64(i64::MAX)), lit(Value::Int64(1))).compile();
        assert_eq!(f(&vec![]), Value::Null);
        let g = add(lit(Value::Int32(i32::MAX)), lit(Value::Int32(1))).compile();
        assert_eq!(g(&vec![]), Value::Null);
        // i32::MIN / -1 overflows too — checked_div covers it, not just /0.
        let h = Expression::BinaryOp {
            op: BinaryOp::Div,
            left: Box::new(lit(Value::Int32(i32::MIN))),
            right: Box::new(lit(Value::Int32(-1))),
        }
        .compile();
        assert_eq!(h(&vec![]), Value::Null);
    }

    #[test]
    fn binary_op_propagates_null() {
        let f = add(lit(Value::Null), lit(Value::Int32(5))).compile();
        assert_eq!(f(&vec![]), Value::Null);
    }

    #[test]
    fn binary_op_type_mismatch_yields_null() {
        let f = add(lit(Value::Int32(1)), lit(Value::Varchar("x".into()))).compile();
        assert_eq!(f(&vec![]), Value::Null);
    }

    // ---- column_type: static type inference (mirrors eval_binary_op) ----

    #[test]
    fn column_type_literal_column_parameter() {
        let types = [ColumnType::Int32, ColumnType::Int64];
        assert_eq!(
            lit(Value::Int32(1)).column_type(&types),
            Some(ColumnType::Int32)
        );
        assert_eq!(col(0).column_type(&types), Some(ColumnType::Int32));
        assert_eq!(col(1).column_type(&types), Some(ColumnType::Int64));
        // Out-of-range column index and unsubstituted parameter → None.
        assert_eq!(col(9).column_type(&types), None);
        assert_eq!(Expression::Parameter(0).column_type(&types), None);
    }

    #[test]
    fn column_type_null_literal_is_none() {
        // NULL has no intrinsic type — and None must propagate up a tree.
        assert_eq!(lit(Value::Null).column_type(&[]), None);
        assert_eq!(
            add(lit(Value::Null), lit(Value::Int32(1))).column_type(&[]),
            None
        );
    }

    #[test]
    fn column_type_int_arithmetic() {
        assert_eq!(
            add(lit(Value::Int32(1)), lit(Value::Int32(2))).column_type(&[]),
            Some(ColumnType::Int32)
        );
        assert_eq!(
            add(lit(Value::Int64(1)), lit(Value::Int64(2))).column_type(&[]),
            Some(ColumnType::Int64)
        );
        // Int32 + Int64 has no `eval_binary_op` arm → NULL → None.
        assert_eq!(
            add(lit(Value::Int32(1)), lit(Value::Int64(2))).column_type(&[]),
            None
        );
    }

    #[test]
    fn column_type_decimal_rules() {
        let types = [
            ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
            ColumnType::Decimal {
                precision: 12,
                scale: 4,
            },
        ];
        // Add same scale → keeps scale.
        assert_eq!(
            add(col(0), col(0)).column_type(&types),
            Some(ColumnType::Decimal {
                precision: 12,
                scale: 2
            })
        );
        // Add scale mismatch → None (mirrors the runtime NULL).
        assert_eq!(add(col(0), col(1)).column_type(&types), None);
        // Mul sums scales: 2 + 4 = 6.
        assert_eq!(
            binop(BinaryOp::Mul, col(0), col(1)).column_type(&types),
            Some(ColumnType::Decimal {
                precision: 12,
                scale: 6
            })
        );
        // Div is native max-scale (no equal-scale requirement): mismatched
        // scale 2 / scale 4 → max(2,4) = scale 4 (mirrors `Decimal::div`).
        assert_eq!(
            binop(BinaryOp::Div, col(0), col(1)).column_type(&types),
            Some(ColumnType::Decimal {
                precision: 12,
                scale: 4
            })
        );
        // Equal-scale division keeps that scale: max(2,2) = scale 2.
        assert_eq!(
            binop(BinaryOp::Div, col(0), col(0)).column_type(&types),
            Some(ColumnType::Decimal {
                precision: 12,
                scale: 2
            })
        );
    }

    #[test]
    fn column_type_int_times_decimal() {
        // Int × Decimal(12,2): int promotes to scale-0 Decimal, Mul sums
        // scales → Decimal(12, 0+2) = Decimal(12,2). This is the TPC-C
        // `ol_quantity * i_price` shape.
        let types = [ColumnType::Decimal {
            precision: 12,
            scale: 2,
        }];
        assert_eq!(
            binop(BinaryOp::Mul, lit(Value::Int64(1)), col(0)).column_type(&types),
            Some(ColumnType::Decimal {
                precision: 12,
                scale: 2
            })
        );
        // Int − Decimal is scale 0 vs 2 → mismatch → None (why Q1's
        // `1 - l_discount` needs bind-time literal alignment).
        assert_eq!(
            binop(BinaryOp::Sub, lit(Value::Int64(1)), col(0)).column_type(&types),
            None
        );
    }

    #[test]
    fn runtime_inference_parity_grid() {
        // PARITY PROPERTY: for every (op, left-type, right-type) in a
        // representative grid, the runtime `eval_binary_op` value's `type_of`
        // MUST equal the static `infer_binary_op_type` — Some vs Some agree on
        // type, and inference `None` ⇔ runtime `Value::Null`. This kills the
        // whole inference-divergence class the H2a review caught: the
        // aggregate accumulator is picked from `infer_binary_op_type`, so any
        // op/type pair where the closure produces a value the inference didn't
        // predict (or vice versa) silently corrupts aggregation.
        //
        // The representative values all share magnitude 6, so no op divides by
        // zero or overflows i64. Decimal `precision` is MAX_PRECISION because
        // `Value::type_of` reports exactly that (a bare value can't recover its
        // column's declared precision); the inference arms carry the left
        // operand's precision, so both sides read MAX_PRECISION and match.
        let dec_ty = |scale: u8| ColumnType::Decimal {
            precision: Decimal::MAX_PRECISION,
            scale,
        };
        // The scale-10 entry (still value 6.0, mantissa 6·10^10) exists so Mul
        // can reach its overflow region: scale10 × scale10 sums to scale 20 >
        // MAX_SCALE (18), where `Decimal::mul` errors → runtime Null and
        // inference now returns None — a both-None ⇒ runtime-Null case that
        // regression-guards the Mul overflow early-out. The scales stopping at 4
        // left that region structurally unreachable. All entries remain value 6
        // (6·10^scale fits i64 comfortably), so no op divides by zero or
        // overflows the numerator.
        let grid: [(ColumnType, Value); 6] = [
            (ColumnType::Int32, Value::Int32(6)),
            (ColumnType::Int64, Value::Int64(6)),
            (dec_ty(0), dec(6, 0)),
            (dec_ty(2), dec(600, 2)),
            (dec_ty(4), dec(60000, 4)),
            (dec_ty(10), dec(60_000_000_000, 10)),
        ];
        for op in [BinaryOp::Add, BinaryOp::Sub, BinaryOp::Mul, BinaryOp::Div] {
            for (left_type, left_value) in &grid {
                for (right_type, right_value) in &grid {
                    let inferred = infer_binary_op_type(op, *left_type, *right_type);
                    let runtime = eval_binary_op(op, left_value.clone(), right_value.clone());
                    assert_eq!(
                        runtime.type_of(),
                        inferred,
                        "parity mismatch for {:?} {:?} {:?}: runtime {:?} vs inferred {:?}",
                        left_type,
                        op,
                        right_type,
                        runtime,
                        inferred
                    );
                }
            }
        }
    }

    // ---- Display ----

    #[test]
    fn display_renders_columns_c_prefixed_and_nests_binops() {
        // Column → `c{i}`, which can't collide with an integer literal.
        assert_eq!(col(2).to_string(), "c2");
        assert_eq!(lit(Value::Int64(5)).to_string(), "5");
        assert_eq!(lit(Value::Null).to_string(), "NULL");
        assert_eq!(Expression::Parameter(0).to_string(), "$1");
        // (c2 * (1 - c3)) — bare `1` is the literal, `c3` the column.
        let inner = binop(BinaryOp::Sub, lit(Value::Int64(1)), col(3));
        let outer = binop(BinaryOp::Mul, col(2), inner);
        assert_eq!(outer.to_string(), "(c2 * (1 - c3))");
    }

    // ---- Predicate ----

    fn compare(op: CompareOp, l: Expression, r: Expression) -> Predicate {
        Predicate::Compare {
            op,
            left: l,
            right: r,
        }
    }

    #[test]
    fn compare_eq_true_and_false() {
        let f = compare(CompareOp::Eq, col(0), lit(Value::Int32(1))).compile();
        assert!(f(&vec![Value::Int32(1)]));
        assert!(!f(&vec![Value::Int32(2)]));
    }

    #[test]
    fn compare_orderings_int32() {
        let f_lt = compare(CompareOp::Lt, col(0), lit(Value::Int32(5))).compile();
        let f_lte = compare(CompareOp::Lte, col(0), lit(Value::Int32(5))).compile();
        let f_gt = compare(CompareOp::Gt, col(0), lit(Value::Int32(5))).compile();
        let f_gte = compare(CompareOp::Gte, col(0), lit(Value::Int32(5))).compile();
        let row5 = vec![Value::Int32(5)];
        let row4 = vec![Value::Int32(4)];
        let row6 = vec![Value::Int32(6)];

        assert!(f_lt(&row4) && !f_lt(&row5) && !f_lt(&row6));
        assert!(f_lte(&row4) && f_lte(&row5) && !f_lte(&row6));
        assert!(!f_gt(&row4) && !f_gt(&row5) && f_gt(&row6));
        assert!(!f_gte(&row4) && f_gte(&row5) && f_gte(&row6));
    }

    #[test]
    fn compare_null_yields_false() {
        let f = compare(CompareOp::Eq, col(0), lit(Value::Null)).compile();
        assert!(!f(&vec![Value::Int32(1)]));
        // NULL = NULL is also false in WHERE semantics.
        let f2 = compare(CompareOp::Eq, lit(Value::Null), lit(Value::Null)).compile();
        assert!(!f2(&vec![]));
    }

    // ---- Decimal arithmetic ----

    fn dec(mantissa: i64, scale: u8) -> Value {
        Value::Decimal(Decimal::from_i64_with_scale(mantissa, scale))
    }

    #[test]
    fn decimal_add_same_scale() {
        // 1.50 + 0.25 = 1.75 (Payment: c_balance -= amount)
        let f = add(lit(dec(150, 2)), lit(dec(25, 2))).compile();
        assert_eq!(f(&vec![]), dec(175, 2));
    }

    #[test]
    fn decimal_sub_same_scale() {
        let f = Expression::BinaryOp {
            op: BinaryOp::Sub,
            left: Box::new(lit(dec(1000, 2))), // 10.00
            right: Box::new(lit(dec(225, 2))), // 2.25
        }
        .compile();
        assert_eq!(f(&vec![]), dec(775, 2)); // 7.75
    }

    #[test]
    fn decimal_mul_scales_sum() {
        // 1.50 * 2.00 = 3.0000 (mul produces sum-of-scales)
        let f = Expression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(lit(dec(150, 2))),
            right: Box::new(lit(dec(200, 2))),
        }
        .compile();
        assert_eq!(f(&vec![]), dec(30000, 4));
    }

    #[test]
    fn decimal_div_is_true_decimal_division() {
        // `Decimal::div` performs real decimal division at scale max(s1,s2),
        // rounded half away from zero — 10.00 / 4.00 = 2.50 (max(2,2)=2).
        let f = Expression::BinaryOp {
            op: BinaryOp::Div,
            left: Box::new(lit(dec(1000, 2))),
            right: Box::new(lit(dec(400, 2))),
        }
        .compile();
        assert_eq!(f(&vec![]), dec(250, 2));
        // Mixed scale: scale4 ÷ scale2 → scale4 (Q14's shape). 9000.0000 /
        // 200.00 = 45.0000 — the left-associative `100.00 * SUM(x) / SUM(y)`
        // core, now legal instead of NULL.
        let g = Expression::BinaryOp {
            op: BinaryOp::Div,
            left: Box::new(lit(dec(90000000, 4))),
            right: Box::new(lit(dec(20000, 2))),
        }
        .compile();
        assert_eq!(g(&vec![]), dec(450000, 4));
    }

    #[test]
    fn decimal_add_scale_mismatch_yields_null() {
        let f = add(lit(dec(150, 2)), lit(dec(15, 1))).compile();
        assert_eq!(f(&vec![]), Value::Null);
    }

    #[test]
    fn int_times_decimal_for_tpcc_ol_amount() {
        // TPC-C NewOrder: ol_amount = ol_quantity * i_price
        // ol_quantity is Int32 (=5), i_price is Decimal(5,2) (=12.34)
        // Expected: 61.70 stored as Decimal(_, 2) — but mul gives scale 0+2=2.
        let f = Expression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(lit(Value::Int32(5))),
            right: Box::new(lit(dec(1234, 2))),
        }
        .compile();
        assert_eq!(f(&vec![]), dec(6170, 2));
    }

    // ---- Decimal ordering ----

    fn cmp_op(op: CompareOp, l: Expression, r: Expression) -> Predicate {
        Predicate::Compare {
            op,
            left: l,
            right: r,
        }
    }

    #[test]
    fn decimal_lt_gt_same_scale() {
        // 5.00 < 10.00 ?
        let f_lt = cmp_op(CompareOp::Lt, lit(dec(500, 2)), lit(dec(1000, 2))).compile();
        let f_gt = cmp_op(CompareOp::Gt, lit(dec(500, 2)), lit(dec(1000, 2))).compile();
        assert!(f_lt(&vec![]));
        assert!(!f_gt(&vec![]));
    }

    #[test]
    fn decimal_cross_scale_compares_exactly() {
        // Cross-scale Decimals compare by mathematical value (via
        // compare_sql's i128 widening): 1.00 < 0.1 is false, 0.1 < 1.00
        // is true. The old comparator returned false for BOTH (silent
        // no-rows on any cross-scale predicate).
        let f = cmp_op(CompareOp::Lt, lit(dec(100, 2)), lit(dec(1, 1))).compile();
        assert!(!f(&vec![]));
        let g = cmp_op(CompareOp::Lt, lit(dec(1, 1)), lit(dec(100, 2))).compile();
        assert!(g(&vec![]));
    }

    #[test]
    fn int_decimal_ordering_works() {
        // O2 repro: `WHERE c_balance > 0` with a Decimal column and an
        // integer literal used to return no rows silently.
        let f = cmp_op(CompareOp::Gt, col(0), lit(Value::Int64(0))).compile();
        assert!(f(&vec![dec(1, 2)])); // 0.01 > 0
        assert!(!f(&vec![dec(-1, 2)])); // -0.01 > 0 is false
                                        // Equality across representations: 5 = 5.00.
        let g = cmp_op(CompareOp::Eq, col(0), lit(Value::Int64(5))).compile();
        assert!(g(&vec![dec(500, 2)]));
    }

    #[test]
    fn timestamp_ordering_works() {
        // O2 repro: Timestamp ordering used to return false for all rows.
        let f = cmp_op(CompareOp::Gt, col(0), lit(Value::Timestamp(100))).compile();
        assert!(f(&vec![Value::Timestamp(200)]));
        assert!(!f(&vec![Value::Timestamp(50)]));
    }

    // ---- three-valued logic (Kleene) ----

    #[test]
    fn not_over_null_comparison_drops_row() {
        // O1: `WHERE NOT (x = 5)` with x NULL. SQL: x = 5 is UNKNOWN,
        // NOT UNKNOWN is UNKNOWN, WHERE drops the row. The old 2VL
        // collapse (UNKNOWN→false at the leaf) kept it (`!false`).
        let inner = compare(CompareOp::Eq, col(0), lit(Value::Int32(5)));
        let f = Predicate::Not(Box::new(inner)).compile();
        assert!(!f(&vec![Value::Null]));
        // Non-NULL rows behave two-valued as before.
        assert!(f(&vec![Value::Int32(6)]));
        assert!(!f(&vec![Value::Int32(5)]));
    }

    #[test]
    fn kleene_and_or_with_unknown() {
        // UNKNOWN AND FALSE = FALSE, so NOT(...) = TRUE → row kept.
        // (col0 = 5) with col0 NULL → UNKNOWN; (col1 = 1) with col1=2 → FALSE.
        let a = compare(CompareOp::Eq, col(0), lit(Value::Int32(5)));
        let b = compare(CompareOp::Eq, col(1), lit(Value::Int32(1)));
        let not_and = Predicate::Not(Box::new(Predicate::And(Box::new(a), Box::new(b)))).compile();
        assert!(not_and(&vec![Value::Null, Value::Int32(2)]));
        // UNKNOWN AND TRUE = UNKNOWN → NOT → UNKNOWN → dropped.
        assert!(!not_and(&vec![Value::Null, Value::Int32(1)]));

        // UNKNOWN OR TRUE = TRUE → kept even under NOT-free WHERE.
        let c = compare(CompareOp::Eq, col(0), lit(Value::Int32(5)));
        let d = compare(CompareOp::Eq, col(1), lit(Value::Int32(1)));
        let or = Predicate::Or(Box::new(c), Box::new(d)).compile();
        assert!(or(&vec![Value::Null, Value::Int32(1)]));
        // UNKNOWN OR FALSE = UNKNOWN → dropped.
        assert!(!or(&vec![Value::Null, Value::Int32(2)]));
    }

    #[test]
    fn int32_int64_cross_type_compare() {
        // Critical for SQL: `WHERE w_id = 1` binds the literal as Int64
        // (unconstrained default), but w_id is typically Int32. Without
        // cross-promotion, the comparison silently returns false and no
        // rows match. TPC-C Payment/NewOrder break without this.
        let f_eq = cmp_op(CompareOp::Eq, col(0), lit(Value::Int64(5))).compile();
        assert!(f_eq(&vec![Value::Int32(5)]));
        assert!(!f_eq(&vec![Value::Int32(6)]));

        let f_lt = cmp_op(CompareOp::Lt, col(0), lit(Value::Int64(10))).compile();
        assert!(f_lt(&vec![Value::Int32(5)]));
        assert!(!f_lt(&vec![Value::Int32(15)]));

        // Symmetric: Int64 column vs Int32 literal.
        let f_eq2 = cmp_op(CompareOp::Eq, lit(Value::Int32(5)), col(0)).compile();
        assert!(f_eq2(&vec![Value::Int64(5)]));
    }

    #[test]
    fn predicate_and_or_not() {
        let a = compare(CompareOp::Eq, col(0), lit(Value::Int32(1)));
        let b = compare(CompareOp::Eq, col(1), lit(Value::Int32(2)));
        let and = Predicate::And(Box::new(a), Box::new(b)).compile();
        assert!(and(&vec![Value::Int32(1), Value::Int32(2)]));
        assert!(!and(&vec![Value::Int32(1), Value::Int32(3)]));
        assert!(!and(&vec![Value::Int32(0), Value::Int32(2)]));

        let c = compare(CompareOp::Eq, col(0), lit(Value::Int32(7)));
        let d = compare(CompareOp::Eq, col(0), lit(Value::Int32(9)));
        let or = Predicate::Or(Box::new(c), Box::new(d)).compile();
        assert!(or(&vec![Value::Int32(7)]));
        assert!(or(&vec![Value::Int32(9)]));
        assert!(!or(&vec![Value::Int32(5)]));

        let e = compare(CompareOp::Eq, col(0), lit(Value::Int32(1)));
        let not = Predicate::Not(Box::new(e)).compile();
        assert!(!not(&vec![Value::Int32(1)]));
        assert!(not(&vec![Value::Int32(2)]));
    }
}
