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
//! - `Compare` with any NULL operand → `false` (SQL `WHERE NULL` filters).
//! - Type mismatch → `Value::Null` for arithmetic, `false` for compare.
//! - Division by zero → `Value::Null`.

use serde::{Deserialize, Serialize};

use crate::execution::Tuple;
use crate::types::{Decimal, Value};

/// An expression that evaluates to a `Value` against an input tuple.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub fn compile(self) -> Box<dyn Fn(&Tuple) -> bool + Send> {
        match self {
            Predicate::Compare { op, left, right } => {
                let l = left.compile();
                let r = right.compile();
                Box::new(move |t| eval_compare(op, &l(t), &r(t)))
            }
            Predicate::And(a, b) => {
                let a = a.compile();
                let b = b.compile();
                // Short-circuit: don't evaluate b if a is false.
                Box::new(move |t| a(t) && b(t))
            }
            Predicate::Or(a, b) => {
                let a = a.compile();
                let b = b.compile();
                Box::new(move |t| a(t) || b(t))
            }
            Predicate::Not(p) => {
                let p = p.compile();
                Box::new(move |t| !p(t))
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
    match (l, r) {
        (Value::Int32(a), Value::Int32(b)) => match op {
            BinaryOp::Add => Value::Int32(a.wrapping_add(b)),
            BinaryOp::Sub => Value::Int32(a.wrapping_sub(b)),
            BinaryOp::Mul => Value::Int32(a.wrapping_mul(b)),
            BinaryOp::Div => {
                if b == 0 {
                    Value::Null
                } else {
                    Value::Int32(a / b)
                }
            }
        },
        (Value::Int64(a), Value::Int64(b)) => match op {
            BinaryOp::Add => Value::Int64(a.wrapping_add(b)),
            BinaryOp::Sub => Value::Int64(a.wrapping_sub(b)),
            BinaryOp::Mul => Value::Int64(a.wrapping_mul(b)),
            BinaryOp::Div => {
                if b == 0 {
                    Value::Null
                } else {
                    Value::Int64(a / b)
                }
            }
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
        BinaryOp::Div => a.div_keeping_scale(b),
    };
    match result {
        Ok(d) => Value::Decimal(d),
        Err(_) => Value::Null,
    }
}

/// NULL-as-false comparison. SQL `WHERE` clause discards NULL rows, so a
/// `bool` return matches that semantic directly.
fn eval_compare(op: CompareOp, l: &Value, r: &Value) -> bool {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return false;
    }
    // Cross-promote Int32 ↔ Int64 for ALL comparison ops. Without this,
    // `WHERE w_id = 1` fails whenever w_id is Int32 and the literal binds
    // as Int64 (the unconstrained-literal default). TPC-C predicates hit
    // this on every Payment/NewOrder.
    let (promoted_l, promoted_r) = match (l, r) {
        (Value::Int32(a), Value::Int64(_)) => (Value::Int64(*a as i64), r.clone()),
        (Value::Int64(_), Value::Int32(b)) => (l.clone(), Value::Int64(*b as i64)),
        _ => (l.clone(), r.clone()),
    };
    let l = &promoted_l;
    let r = &promoted_r;
    match op {
        CompareOp::Eq => l == r,
        CompareOp::Neq => l != r,
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            let ord = match (l, r) {
                (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
                (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
                (Value::Varchar(a), Value::Varchar(b)) => a.cmp(b),
                (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
                // Decimal ordering: same-scale → compare mantissas.
                // Cross-scale yields false (Phase 13 will add alignment).
                (Value::Decimal(a), Value::Decimal(b)) if a.scale() == b.scale() => {
                    a.mantissa().cmp(&b.mantissa())
                }
                // NOTE (Phase 13): Char/Bytes/Timestamp ordering. TPC-C uses
                // these only in ORDER BY clauses, which Phase 13 introduces
                // alongside Sort. Equality already works via `PartialEq`.
                _ => return false,
            };
            use std::cmp::Ordering::*;
            matches!(
                (op, ord),
                (CompareOp::Lt, Less)
                    | (CompareOp::Lte, Less)
                    | (CompareOp::Lte, Equal)
                    | (CompareOp::Gt, Greater)
                    | (CompareOp::Gte, Greater)
                    | (CompareOp::Gte, Equal)
            )
        }
    }
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
        Expression::BinaryOp {
            op: BinaryOp::Add,
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
    fn binary_op_propagates_null() {
        let f = add(lit(Value::Null), lit(Value::Int32(5))).compile();
        assert_eq!(f(&vec![]), Value::Null);
    }

    #[test]
    fn binary_op_type_mismatch_yields_null() {
        let f = add(lit(Value::Int32(1)), lit(Value::Varchar("x".into()))).compile();
        assert_eq!(f(&vec![]), Value::Null);
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
    fn decimal_div_dispatches_to_div_keeping_scale() {
        // NOTE (semantic, plan deviation): `Decimal::div_keeping_scale`
        // divides the raw mantissas (integer truncation) and preserves the
        // dividend's scale, NOT decimal-style division. So 10.00 / 4.00 =
        // 0.02, not 2.50. This matches the "INT/INT" semantic the type
        // documents but diverges from standard SQL decimal division.
        // TPC-C doesn't divide decimals — keep the wiring test, revisit the
        // semantic when Phase 13 SUM/AVG land (AVG may need true division).
        let f = Expression::BinaryOp {
            op: BinaryOp::Div,
            left: Box::new(lit(dec(1000, 2))),
            right: Box::new(lit(dec(400, 2))),
        }
        .compile();
        assert_eq!(f(&vec![]), dec(2, 2));
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
    fn decimal_cross_scale_compare_yields_false() {
        // 1.00 vs 0.1: scale mismatch → false (Phase 13 will align).
        let f = cmp_op(CompareOp::Lt, lit(dec(100, 2)), lit(dec(1, 1))).compile();
        assert!(!f(&vec![]));
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
