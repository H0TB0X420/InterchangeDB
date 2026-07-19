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
    /// `EXTRACT(YEAR FROM <date-expr>)` — the calendar year of a `Date`
    /// argument, as an `Int32`. The binder proves the argument infers `Date`
    /// before constructing this, so the runtime path is total by design.
    ExtractYear(Box<Expression>),
    /// Searched `CASE WHEN <pred> THEN <expr> [WHEN …] [ELSE <expr>] END`.
    /// Branches evaluate in order under 3VL: the first predicate that is
    /// `Some(true)` selects its result; `Some(false)` and UNKNOWN fall
    /// through. No branch matches → `else_expr`, or `Null` when absent.
    ///
    /// `Expression` now contains `Predicate` (the branch conditions) — the
    /// two IR types were already mutually recursive through `Box`, so this
    /// closes the cycle without introducing any new indirection. Appended
    /// LAST so bincode's discriminants for the pre-existing variants stay
    /// stable (plans round-trip through the workload log).
    Case {
        branches: Vec<(Predicate, Expression)>,
        else_expr: Option<Box<Expression>>,
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
///
/// `PartialEq`/`Eq` because `Expression::Case` embeds a `Predicate` and
/// `Expression` derives `Eq` (used for aggregate/SELECT-list dedup); the
/// derive is only sound if `Predicate` is `Eq` too. Every field is already
/// `Eq` (`Expression`, `String`, `CompareOp`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// `<expr> LIKE '<pattern>'` — SQL glob match: `%` matches any run of
    /// characters (including empty), `_` matches exactly one. `NOT LIKE`
    /// binds as `Not(Like)`. The pattern is a bind-time string literal,
    /// compiled once into a matcher. A NULL input evaluates to UNKNOWN.
    /// Appended (with `IsNull`) LAST for bincode discriminant stability.
    Like { expr: Expression, pattern: String },
    /// `<expr> IS NULL` — always evaluates to `Some(bool)`, never UNKNOWN
    /// (that totality is the point of the predicate). `IS NOT NULL` binds
    /// as `Not(IsNull)`.
    IsNull(Expression),
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

/// A compiled expression: evaluates to a `Value` against an input tuple.
/// The return type of `Expression::compile`; named so composite closures
/// (a CASE's per-branch results) don't spell the `dyn Fn` out inline.
type ExpressionFn = Box<dyn Fn(&Tuple) -> Value + Send>;

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
            Expression::ExtractYear(arg) => {
                let a = arg.compile();
                Box::new(move |t| match a(t) {
                    Value::Date(days) => Value::Int32(crate::types::civil::ymd_from_days(days).0),
                    // NULL in, NULL out.
                    Value::Null => Value::Null,
                    // Unreachable given the binder's Date-typing check, but the
                    // closure stays total: any non-Date value yields NULL.
                    _ => Value::Null,
                })
            }
            Expression::Case {
                branches,
                else_expr,
            } => {
                // Compile every branch predicate and result once, plus the
                // optional ELSE, so evaluation is closure calls with no
                // per-row IR walking.
                let compiled: Vec<(Predicate3VL, ExpressionFn)> = branches
                    .into_iter()
                    .map(|(pred, result)| (pred.compile_3vl(), result.compile()))
                    .collect();
                let else_f = else_expr.map(|e| e.compile());
                Box::new(move |t| {
                    // First branch whose predicate is TRUE wins; UNKNOWN and
                    // FALSE fall through (SQL searched-CASE semantics).
                    for (pred, result) in &compiled {
                        if pred(t) == Some(true) {
                            return result(t);
                        }
                    }
                    match &else_f {
                        Some(f) => f(t),
                        None => Value::Null,
                    }
                })
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
            Expression::ExtractYear(arg) => Ok(Expression::ExtractYear(Box::new(
                arg.substitute_params(params)?,
            ))),
            Expression::Case {
                branches,
                else_expr,
            } => {
                // Substitute in BOTH branch predicates and branch results.
                let branches = branches
                    .into_iter()
                    .map(|(pred, result)| {
                        Ok((
                            pred.substitute_params(params)?,
                            result.substitute_params(params)?,
                        ))
                    })
                    .collect::<crate::common::Result<Vec<_>>>()?;
                let else_expr = match else_expr {
                    Some(e) => Some(Box::new(e.substitute_params(params)?)),
                    None => None,
                };
                Ok(Expression::Case {
                    branches,
                    else_expr,
                })
            }
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
            // EXTRACT(YEAR FROM Date) is Int32; anything but a Date argument is
            // uninferable (`None`) — the binder turns that into a loud error at
            // its build sites, so this never silently mistypes an aggregate.
            Expression::ExtractYear(arg) => match arg.column_type(column_types) {
                Some(ColumnType::Date) => Some(ColumnType::Int32),
                _ => None,
            },
            // A CASE has a single well-defined type only if the result
            // branches that CARRY a type (and the ELSE, when present) agree on
            // the SAME type — that type is the CASE's type. A bare NULL-literal
            // branch (`THEN NULL` / `ELSE NULL`) carries no type and is
            // SKIPPED, exactly like an implicit missing ELSE, so `… END` and
            // the semantically-identical `… ELSE NULL END` infer the same
            // type. Disagreement, an uninferable non-NULL branch (a parameter
            // result), or an all-NULL / no-typed-branch CASE is `None`.
            // The binder pre-coerces bare integer-literal branches toward the
            // non-literal branches' Decimal/Int32 type (see
            // `coerce_case_literal_branches`), so TPC-H's
            // `THEN price ELSE 0` unifies here rather than erroring. Predicate
            // conditions do not contribute to the result type.
            Expression::Case {
                branches,
                else_expr,
            } => {
                let mut result_ty: Option<ColumnType> = None;
                let results = branches
                    .iter()
                    .map(|(_, result)| result)
                    .chain(else_expr.as_deref());
                for result in results {
                    // RULE: a bare NULL literal contributes no type. Skipping
                    // it (rather than letting its `None` collapse the whole
                    // CASE through `?`) is what makes an explicit `ELSE NULL`
                    // type identically to an omitted ELSE. At least one
                    // non-NULL-literal branch is needed to type from; all-NULL
                    // leaves `result_ty` at `None`.
                    if matches!(result, Expression::Literal(Value::Null)) {
                        continue;
                    }
                    let ty = result.column_type(column_types)?;
                    match result_ty {
                        None => result_ty = Some(ty),
                        Some(existing) => result_ty = Some(unify_case_type(existing, ty)?),
                    }
                }
                result_ty
            }
        }
    }
}

/// Unify two CASE result-branch types into the CASE's type, or `None` when
/// they're incompatible.
///
/// NOTE (deviation from the "SAME type" spec, forced by Decimal reality):
/// two Decimals unify on SCALE alone, carrying the wider precision. A
/// `Value`'s precision is unknowable at runtime — `Value::type_of` reports
/// `MAX_PRECISION` — so the TPC-H `THEN price ELSE 0` shape mixes a column's
/// declared `Decimal{12,2}` with a literal's `Decimal{MAX_PRECISION,2}`.
/// Requiring precision equality would make every Decimal CASE-with-literal
/// uninferable. This mirrors `infer_binary_op_type`, which also unifies
/// Decimals by scale and ignores the peer's precision. Every other type
/// unifies by strict equality (Varchar/Char keep their length — a CASE over
/// different-length strings stays a recorded limitation).
fn unify_case_type(a: ColumnType, b: ColumnType) -> Option<ColumnType> {
    match (a, b) {
        (
            ColumnType::Decimal {
                precision: p1,
                scale: s1,
            },
            ColumnType::Decimal {
                precision: p2,
                scale: s2,
            },
        ) if s1 == s2 => Some(ColumnType::Decimal {
            precision: p1.max(p2),
            scale: s1,
        }),
        (x, y) if x == y => Some(x),
        _ => None,
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
            // Render a folded date as its source literal (`DATE 'YYYY-MM-DD'`)
            // so EXPLAIN over a bind-time-folded date is readable and pins the
            // exact folded day — the folding-proof the H3.2 slt gate asserts.
            Expression::Literal(Value::Date(days)) => {
                let (y, m, d) = crate::types::civil::ymd_from_days(*days);
                write!(f, "DATE '{y:04}-{m:02}-{d:02}'")
            }
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
            Expression::ExtractYear(arg) => write!(f, "EXTRACT(YEAR FROM {})", arg),
            // `Predicate` has no `Display`, so we render a compact,
            // deterministic summary rather than the full WHEN/THEN tree:
            // the branch count plus an `, else` marker when an ELSE is
            // present — enough to identify the CASE in an EXPLAIN label.
            Expression::Case {
                branches,
                else_expr,
            } => {
                let else_marker = if else_expr.is_some() { ", else" } else { "" };
                write!(f, "CASE({} branches{})", branches.len(), else_marker)
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
            Predicate::Like { expr, pattern } => Ok(Predicate::Like {
                expr: expr.substitute_params(params)?,
                pattern,
            }),
            Predicate::IsNull(expr) => Ok(Predicate::IsNull(expr.substitute_params(params)?)),
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
            Predicate::Like { expr, pattern } => {
                let e = expr.compile();
                // Compile the glob pattern ONCE into segments; matching is
                // then a bounded scan per row (no per-row pattern parsing,
                // no exponential backtracking — see `like_match`).
                let segments = compile_like_pattern(&pattern);
                Box::new(move |t| match e(t) {
                    // Char and Varchar are one string domain (see `compare_sql`).
                    Value::Varchar(s) | Value::Char(s) => Some(like_match(&segments, &s)),
                    // NULL LIKE anything is UNKNOWN.
                    Value::Null => None,
                    // The binder proves the operand is Varchar/Char, so this
                    // is unreachable; keep the closure total as UNKNOWN rather
                    // than silently matching a non-string value.
                    _ => None,
                })
            }
            Predicate::IsNull(expr) => {
                let e = expr.compile();
                // ALWAYS Some(bool): IS NULL is the one predicate that turns a
                // NULL into a definite truth value instead of UNKNOWN.
                Box::new(move |t| Some(matches!(e(t), Value::Null)))
            }
        }
    }
}

// ===========================================================================
// LIKE pattern matching
// ===========================================================================

/// One position of a compiled LIKE segment: a literal character to match
/// exactly, or `_` (matches any single character).
#[derive(Debug, Clone, Copy)]
enum PatChar {
    Lit(char),
    AnyOne,
}

/// Split a LIKE pattern on `%` into fixed-width segments, translating each
/// `_` to `AnyOne` and every other character to a literal. The number of
/// segments is `(count of '%') + 1`, so `%abc%` → `["", "abc", ""]` and a
/// pattern with no `%` yields a single segment. `%` runs collapse naturally
/// (an empty segment between two `%`s matches trivially).
fn compile_like_pattern(pattern: &str) -> Vec<Vec<PatChar>> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    for ch in pattern.chars() {
        match ch {
            '%' => segments.push(std::mem::take(&mut current)),
            '_' => current.push(PatChar::AnyOne),
            other => current.push(PatChar::Lit(other)),
        }
    }
    segments.push(current);
    segments
}

/// Does `text` match the compiled LIKE `segments`?
///
/// Algorithm (bounded, no backtracking): a pattern `s0 % s1 % … % sk` means
/// "starts with `s0`, ends with `sk`, and contains `s1 … s_{k-1}` in order,
/// with any characters (including none) between them". With `n = segments`:
///
/// - `n == 1` (no `%`): the whole text must equal the single fixed-width
///   segment (same length, char-by-char with `_` wildcards).
/// - `n >= 2`: the first segment is anchored at the start and the last at
///   the end; each middle segment is found at its LEFTMOST occurrence at or
///   after the running cursor. Leftmost-greedy is correct here because the
///   segments are order-preserving and non-overlapping — taking the earliest
///   match for an earlier segment only leaves MORE room for later ones, so
///   it never rejects a matchable string. Each `find` scans the text once,
///   giving O(pattern_len · text_len) worst case with a fixed upper bound.
fn like_match(segments: &[Vec<PatChar>], text: &str) -> bool {
    let chars: Vec<char> = text.chars().collect();
    let n = segments.len();
    debug_assert!(n >= 1, "compile_like_pattern always yields >= 1 segment");

    // No `%`: the pattern is a single fixed-width template for the whole text.
    if n == 1 {
        return segments[0].len() == chars.len() && segment_matches_at(&segments[0], &chars, 0);
    }

    // Prefix (segment 0) anchored at the start.
    let prefix = &segments[0];
    if !segment_matches_at(prefix, &chars, 0) {
        return false;
    }
    let mut cursor = prefix.len();

    // Suffix (last segment) anchored at the end. `checked_sub` fails when the
    // suffix is longer than the text; the prefix/suffix must not overlap.
    let suffix = &segments[n - 1];
    let suffix_start = match chars.len().checked_sub(suffix.len()) {
        Some(start) => start,
        None => return false,
    };
    if suffix_start < cursor {
        return false;
    }
    if !segment_matches_at(suffix, &chars, suffix_start) {
        return false;
    }

    // Middle segments float between the cursor and the suffix, matched in
    // order at their leftmost feasible position.
    for seg in &segments[1..n - 1] {
        match find_segment(seg, &chars, cursor, suffix_start) {
            Some(pos) => cursor = pos + seg.len(),
            None => return false,
        }
    }
    true
}

/// Does `seg` match `chars[start..start + seg.len()]` exactly (with `_`
/// wildcards)? False when the segment would run past the end of `chars`.
fn segment_matches_at(seg: &[PatChar], chars: &[char], start: usize) -> bool {
    if start + seg.len() > chars.len() {
        return false;
    }
    seg.iter().enumerate().all(|(i, pat)| match pat {
        PatChar::Lit(c) => chars[start + i] == *c,
        PatChar::AnyOne => true,
    })
}

/// Leftmost start position in `[from, end_bound]` where `seg` fully matches
/// without running past `end_bound`. An empty segment matches at `from`.
fn find_segment(seg: &[PatChar], chars: &[char], from: usize, end_bound: usize) -> Option<usize> {
    // The last position the segment can start at and still end by `end_bound`.
    let last_start = end_bound.checked_sub(seg.len())?;
    let mut start = from;
    while start <= last_start {
        if segment_matches_at(seg, chars, start) {
            return Some(start);
        }
        start += 1;
    }
    None
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

    // ---- ExtractYear ----

    fn extract_year(arg: Expression) -> Expression {
        Expression::ExtractYear(Box::new(arg))
    }

    #[test]
    fn extract_year_compiles_over_date_and_null() {
        // 8766 = 1994-01-01, 10561 = 1998-12-01 — the year is the civil year.
        let f = extract_year(col(0)).compile();
        assert_eq!(f(&vec![Value::Date(8766)]), Value::Int32(1994));
        assert_eq!(f(&vec![Value::Date(10561)]), Value::Int32(1998));
        // Pre-epoch date: -1 = 1969-12-31.
        assert_eq!(f(&vec![Value::Date(-1)]), Value::Int32(1969));
        // NULL in → NULL out.
        assert_eq!(f(&vec![Value::Null]), Value::Null);
    }

    #[test]
    fn extract_year_type_is_int32_only_over_date() {
        // Arg infers Date → Int32; arg infers anything else → None (the binder
        // turns that None into a loud error, so it never mistypes downstream).
        assert_eq!(
            extract_year(col(0)).column_type(&[ColumnType::Date]),
            Some(ColumnType::Int32)
        );
        assert_eq!(extract_year(col(0)).column_type(&[ColumnType::Int64]), None);
        assert_eq!(extract_year(lit(Value::Null)).column_type(&[]), None);
    }

    #[test]
    fn extract_year_display() {
        assert_eq!(extract_year(col(3)).to_string(), "EXTRACT(YEAR FROM c3)");
        // A folded date literal renders as its source form (EXPLAIN readability).
        assert_eq!(lit(Value::Date(8766)).to_string(), "DATE '1994-01-01'");
        assert_eq!(lit(Value::Date(-1)).to_string(), "DATE '1969-12-31'");
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

    // ---- LIKE glob matcher ----

    /// Match `text` against `pattern` through the compiled segment matcher —
    /// the same path `Predicate::Like` compiles, isolated from tuple plumbing.
    fn like(pattern: &str, text: &str) -> bool {
        like_match(&compile_like_pattern(pattern), text)
    }

    #[test]
    fn like_prefix_contains_suffix_and_underscore() {
        // Exact (no wildcard): only the identical string matches.
        assert!(like("abc", "abc"));
        assert!(!like("abc", "abcd"));
        assert!(!like("abc", "ab"));
        // Prefix `abc%`: anchored at the start, any (or no) suffix.
        assert!(like("abc%", "abc"));
        assert!(like("abc%", "abcdef"));
        assert!(!like("abc%", "xabc"));
        // Suffix `%abc`: anchored at the end.
        assert!(like("%abc", "abc"));
        assert!(like("%abc", "xxabc"));
        assert!(!like("%abc", "abcx"));
        // Contains `%abc%`: anywhere, including the ends.
        assert!(like("%abc%", "abc"));
        assert!(like("%abc%", "xxabcyy"));
        assert!(!like("%abc%", "ab_c"));
        // `_` matches EXACTLY one character (never zero, never two).
        assert!(like("a_c", "abc"));
        assert!(like("a_c", "axc"));
        assert!(!like("a_c", "ac"));
        assert!(!like("a_c", "abbc"));
        // Combined `_` and `%`: `a_%z` = 'a', one char, anything, ending 'z'.
        assert!(like("a_%z", "abz"));
        assert!(like("a_%z", "abcdez"));
        assert!(!like("a_%z", "az")); // no char for the `_`
    }

    #[test]
    fn like_percent_edge_cases() {
        // A lone `%` matches everything, including the empty string.
        assert!(like("%", ""));
        assert!(like("%", "anything"));
        // `%%` collapses — still matches everything.
        assert!(like("%%", "xyz"));
        // The empty pattern matches ONLY the empty string.
        assert!(like("", ""));
        assert!(!like("", "x"));
        // Multiple floating segments in order: `%a%b%`.
        assert!(like("%a%b%", "xaxbx"));
        assert!(like("%a%b%", "ab"));
        assert!(!like("%a%b%", "ba")); // wrong order
    }

    #[test]
    fn like_predicate_null_input_is_unknown() {
        // NULL LIKE anything is UNKNOWN → dropped by WHERE, and NOT LIKE over
        // NULL is also UNKNOWN (not TRUE) → also dropped.
        let p = Predicate::Like {
            expr: col(0),
            pattern: "a%".into(),
        };
        let f = p.clone().compile();
        assert!(f(&vec![Value::Varchar("abc".into())]));
        assert!(!f(&vec![Value::Varchar("xbc".into())]));
        assert!(!f(&vec![Value::Null])); // UNKNOWN dropped
        let not = Predicate::Not(Box::new(p)).compile();
        assert!(!not(&vec![Value::Null])); // NOT UNKNOWN = UNKNOWN, dropped
        assert!(not(&vec![Value::Varchar("xbc".into())])); // NOT LIKE, TRUE
                                                           // Char and Varchar are one string domain, so a Char input matches too.
        assert!(f(&vec![Value::Char("abc".into())]));
    }

    // ---- IS NULL ----

    #[test]
    fn is_null_is_total_never_unknown() {
        // IS NULL turns a NULL into TRUE and anything else into FALSE — it is
        // the one predicate that never yields UNKNOWN.
        let f = Predicate::IsNull(col(0)).compile();
        assert!(f(&vec![Value::Null]));
        assert!(!f(&vec![Value::Int32(0)]));
        // IS NOT NULL is the NOT wrap: TRUE on a value, FALSE on NULL.
        let g = Predicate::Not(Box::new(Predicate::IsNull(col(0)))).compile();
        assert!(!g(&vec![Value::Null]));
        assert!(g(&vec![Value::Int32(0)]));
    }

    // ---- searched CASE ----

    fn case(branches: Vec<(Predicate, Expression)>, else_expr: Option<Expression>) -> Expression {
        Expression::Case {
            branches,
            else_expr: else_expr.map(Box::new),
        }
    }

    #[test]
    fn case_eval_order_first_true_wins_unknown_falls_through() {
        // CASE WHEN c0 > 5 THEN 1 WHEN c0 <= 5 THEN 2 ELSE 3 END.
        let e = case(
            vec![
                (
                    compare(CompareOp::Gt, col(0), lit(Value::Int32(5))),
                    lit(Value::Int32(1)),
                ),
                (
                    compare(CompareOp::Lte, col(0), lit(Value::Int32(5))),
                    lit(Value::Int32(2)),
                ),
            ],
            Some(lit(Value::Int32(3))),
        );
        let f = e.compile();
        // 10 > 5 → first branch wins → 1.
        assert_eq!(f(&vec![Value::Int32(10)]), Value::Int32(1));
        // 3 <= 5 (first FALSE, second TRUE) → 2.
        assert_eq!(f(&vec![Value::Int32(3)]), Value::Int32(2));
        // NULL: both comparisons UNKNOWN → fall through to ELSE → 3.
        assert_eq!(f(&vec![Value::Null]), Value::Int32(3));
    }

    #[test]
    fn case_no_else_falls_through_to_null() {
        // No ELSE, no branch TRUE → NULL.
        let e = case(
            vec![(
                compare(CompareOp::Eq, col(0), lit(Value::Int32(1))),
                lit(Value::Int32(9)),
            )],
            None,
        );
        let f = e.compile();
        assert_eq!(f(&vec![Value::Int32(1)]), Value::Int32(9));
        assert_eq!(f(&vec![Value::Int32(2)]), Value::Null);
    }

    #[test]
    fn case_column_type_unifies_or_is_none() {
        // Branches that agree on a type → that type. `_` predicate is a
        // placeholder true-ish comparison; only the RESULT types drive this.
        let cond = compare(CompareOp::Gt, col(0), lit(Value::Int32(0)));
        // Both results Int64 → Int64.
        let same = case(
            vec![(cond.clone(), lit(Value::Int64(1)))],
            Some(lit(Value::Int64(0))),
        );
        assert_eq!(
            same.column_type(&[ColumnType::Int32]),
            Some(ColumnType::Int64)
        );
        // Decimal THEN column + Decimal ELSE literal (same scale) → a Decimal
        // at that scale. This is the post-coercion Q14 shape: the binder turns
        // a bare `0` into `0.00`, whose `type_of` reports MAX_PRECISION (a
        // value can't recover a declared precision), so unification widens the
        // column's precision to MAX and keeps the shared scale 2. The point is
        // that it UNIFIES (scale-based) rather than erroring on precision.
        let dec_ty = ColumnType::Decimal {
            precision: 12,
            scale: 2,
        };
        let dec_case = case(vec![(cond.clone(), col(1))], Some(lit(dec(0, 2))));
        assert_eq!(
            dec_case.column_type(&[ColumnType::Int32, dec_ty]),
            Some(ColumnType::Decimal {
                precision: Decimal::MAX_PRECISION,
                scale: 2
            })
        );
        // Omitted ELSE vs explicit `ELSE NULL` MUST infer the same type: a
        // bare NULL branch carries no type and is skipped exactly like a
        // missing ELSE. Both `THEN c1 END` and `THEN c1 ELSE NULL END` should
        // report the typed branch's type (Int64), not collapse to None.
        let types = [ColumnType::Int32, ColumnType::Int64];
        let no_else = case(vec![(cond.clone(), col(1))], None);
        let else_null = case(vec![(cond.clone(), col(1))], Some(lit(Value::Null)));
        assert_eq!(no_else.column_type(&types), Some(ColumnType::Int64));
        assert_eq!(else_null.column_type(&types), no_else.column_type(&types));
        // A `THEN NULL` branch is skipped the same way, so a typed later branch
        // still drives the CASE's type instead of the NULL collapsing it.
        let then_null = case(
            vec![(cond.clone(), lit(Value::Null)), (cond.clone(), col(1))],
            None,
        );
        assert_eq!(then_null.column_type(&types), Some(ColumnType::Int64));
        // All branches NULL → still None (nothing to type from), as before.
        let all_null = case(
            vec![(cond.clone(), lit(Value::Null))],
            Some(lit(Value::Null)),
        );
        assert_eq!(all_null.column_type(&types), None);
        // Mismatched result types (Int64 vs Varchar) → None (the loud
        // "cannot infer" contract at build).
        let mismatch = case(
            vec![(cond, lit(Value::Int64(1)))],
            Some(lit(Value::Varchar("x".into()))),
        );
        assert_eq!(mismatch.column_type(&[ColumnType::Int32]), None);
    }

    #[test]
    fn case_display_is_deterministic_summary() {
        let with_else = case(
            vec![
                (
                    compare(CompareOp::Eq, col(0), lit(Value::Int32(1))),
                    lit(Value::Int32(1)),
                ),
                (
                    compare(CompareOp::Eq, col(0), lit(Value::Int32(2))),
                    lit(Value::Int32(2)),
                ),
            ],
            Some(lit(Value::Int32(0))),
        );
        assert_eq!(with_else.to_string(), "CASE(2 branches, else)");
        let no_else = case(
            vec![(
                compare(CompareOp::Eq, col(0), lit(Value::Int32(1))),
                lit(Value::Int32(1)),
            )],
            None,
        );
        assert_eq!(no_else.to_string(), "CASE(1 branches)");
    }
}
