//! `HashAggregate` — aggregation operator, whole-table or GROUP BY.
//!
//! Computes a fixed list of aggregates (`COUNT`, `SUM`, `MIN`, `MAX`,
//! `AVG`) over the child's full output. With `group_by` empty it emits
//! exactly one row of aggregate results (even on empty input — SQL's
//! whole-table rule). With `group_by` columns it buckets rows by key in
//! a `HashMap<Vec<Value>, Vec<AggState>>` and emits one row per group —
//! key values first, then aggregates — and zero rows on empty input.
//! Derived `Eq` on `Value` makes `Null == Null`, so SQL's "NULLs form
//! one group" rule falls out of the map for free.
//!
//! Groups are emitted in canonical key order (NULL first, then
//! `Value::compare_sql` — the same ordering Sort uses): SQL promises no
//! order, but deterministic emission keeps planner-parity and
//! exec-model-equivalence suites byte-comparable without sprinkling
//! ORDER BY everywhere.
//!
//! ## NULL semantics
//!
//! SQL aggregate semantics: aggregates skip NULL inputs except for
//! `COUNT(*)`, which counts every row including all-NULL rows.
//! `COUNT(col)` counts only non-NULL values. `MIN`/`MAX` ignore NULL.
//! `SUM`/`AVG` of an all-NULL or empty input is NULL (with the usual
//! exception that `COUNT` of an empty input is 0).
//!
//! ## Pull/finalize shape
//!
//! Eager-collect on first `next()`: drain the child, fold per-row into
//! accumulators, finalize to output rows, then drain that buffer across
//! subsequent `next()` calls.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::catalog::{Schema, TableId};
use crate::common::{Error, Result};
use crate::execution::{Executor, Tuple};
use crate::sql::ir::expr::Expression;
use crate::sql::ir::logical::AggregateSpec;
use crate::types::{ColumnType, Decimal, Value};

/// A per-row argument evaluator, compiled once from an aggregate's
/// argument `Expression`. `COUNT(*)` has none.
pub(crate) type ArgEvaluator = Box<dyn Fn(&Tuple) -> Value + Send>;

/// A single aggregate function applied to an argument expression (or the
/// entire row, for `COUNT(*)`). H2a: the argument is any `Expression`,
/// evaluated per row by a compiled closure — `SUM(price * (1 - disc))`.
#[derive(Debug, Clone)]
pub enum AggregateFn {
    /// `COUNT(*)` — counts every input row.
    CountStar,
    /// `COUNT(expr)` — counts non-NULL evaluated values.
    Count(Expression),
    /// `COUNT(DISTINCT expr)` — counts unique non-NULL values. Tracks a
    /// `HashSet<Value>` per evaluated value; finalizes to the set size.
    /// TPC-C StockLevel needs this for its `s_i_id` count.
    CountDistinct(Expression),
    /// `SUM(expr)` — running sum; NULL on empty / all-NULL input.
    Sum(Expression),
    /// `MIN(expr)` — smallest non-NULL value.
    Min(Expression),
    /// `MAX(expr)` — largest non-NULL value.
    Max(Expression),
    /// `AVG(expr)` — sum / count of non-NULL values. NULL on empty input.
    Avg(Expression),
}

impl AggregateFn {
    /// The argument expression, or `None` for `COUNT(*)`.
    pub(crate) fn arg(&self) -> Option<&Expression> {
        match self {
            AggregateFn::CountStar => None,
            AggregateFn::Count(e)
            | AggregateFn::CountDistinct(e)
            | AggregateFn::Sum(e)
            | AggregateFn::Min(e)
            | AggregateFn::Max(e)
            | AggregateFn::Avg(e) => Some(e),
        }
    }

    /// Short name for error messages ("SUM: cannot infer argument type").
    fn label(&self) -> &'static str {
        match self {
            AggregateFn::CountStar => "COUNT(*)",
            AggregateFn::Count(_) => "COUNT",
            AggregateFn::CountDistinct(_) => "COUNT(DISTINCT)",
            AggregateFn::Sum(_) => "SUM",
            AggregateFn::Min(_) => "MIN",
            AggregateFn::Max(_) => "MAX",
            AggregateFn::Avg(_) => "AVG",
        }
    }
}

/// The static type an aggregate's argument produces over `input_schema`,
/// or `None` for `COUNT(*)`. Errors loudly when an argument expression's
/// type can't be inferred (mirrors `eval_binary_op`) — the accumulator
/// and output type are picked from this and MUST match the runtime values.
fn infer_arg_type(agg: &AggregateFn, input_schema: &Schema) -> Result<Option<ColumnType>> {
    match agg.arg() {
        None => Ok(None),
        Some(expr) => expr
            .column_type(&input_schema.column_types())
            .map(Some)
            .ok_or_else(|| Error::SqlParse(format!("{}: cannot infer argument type", agg.label()))),
    }
}

/// The `AggregateSpec` (IR) equivalent of this executor aggregate — the
/// inverse of `translate_aggregate_spec`. Lets the schema builder delegate
/// output typing/naming to the single-source IR rules
/// (`AggregateSpec::output_type` / `output_name`).
fn spec_of(agg: &AggregateFn) -> AggregateSpec {
    match agg {
        AggregateFn::CountStar => AggregateSpec::CountStar,
        AggregateFn::Count(e) => AggregateSpec::Count {
            arg: e.clone(),
            distinct: false,
        },
        AggregateFn::CountDistinct(e) => AggregateSpec::Count {
            arg: e.clone(),
            distinct: true,
        },
        AggregateFn::Sum(e) => AggregateSpec::Sum(e.clone()),
        AggregateFn::Min(e) => AggregateSpec::Min(e.clone()),
        AggregateFn::Max(e) => AggregateSpec::Max(e.clone()),
        AggregateFn::Avg(e) => AggregateSpec::Avg(e.clone()),
    }
}

/// Per-aggregate accumulator state.
pub(crate) enum AggState {
    /// `COUNT(*)` and `COUNT(col)`.
    Count(u64),
    /// `COUNT(DISTINCT col)` — tracks seen non-NULL values.
    DistinctSet(HashSet<Value>),
    /// `SUM`. None until the first non-NULL value. Numeric variant kept
    /// in lock-step with the argument's inferred type.
    SumInt(Option<i64>),
    SumDecimal(Option<Decimal>),
    /// `MIN` / `MAX`. None until the first non-NULL value.
    MinMaxValue(Option<Value>),
    /// `AVG` tracks both running sum and non-NULL count. Integer-sourced
    /// AVG uses `SumInt`; Decimal-sourced AVG uses `SumDecimal`.
    AvgInt {
        sum: Option<i64>,
        count: u64,
    },
    AvgDecimal {
        sum: Option<Decimal>,
        count: u64,
    },
}

impl AggState {
    pub(crate) fn new_for(agg: &AggregateFn, input_schema: &Schema) -> Result<Self> {
        match agg {
            AggregateFn::CountStar | AggregateFn::Count(_) => Ok(AggState::Count(0)),
            AggregateFn::CountDistinct(_) => Ok(AggState::DistinctSet(HashSet::new())),
            AggregateFn::Min(_) | AggregateFn::Max(_) => Ok(AggState::MinMaxValue(None)),
            // The numeric accumulator must match the argument's inferred
            // type, not a bare column type — the argument may be arithmetic.
            AggregateFn::Sum(_) => {
                match infer_arg_type(agg, input_schema)?.expect("Sum has an arg") {
                    ColumnType::Int32 | ColumnType::Int64 => Ok(AggState::SumInt(None)),
                    ColumnType::Decimal { .. } => Ok(AggState::SumDecimal(None)),
                    other => Err(Error::SqlParse(format!(
                        "SUM of non-numeric type {:?} not supported",
                        other
                    ))),
                }
            }
            AggregateFn::Avg(_) => {
                match infer_arg_type(agg, input_schema)?.expect("Avg has an arg") {
                    ColumnType::Int32 | ColumnType::Int64 => Ok(AggState::AvgInt {
                        sum: None,
                        count: 0,
                    }),
                    ColumnType::Decimal { .. } => Ok(AggState::AvgDecimal {
                        sum: None,
                        count: 0,
                    }),
                    other => Err(Error::SqlParse(format!(
                        "AVG of non-numeric type {:?} not supported",
                        other
                    ))),
                }
            }
        }
    }
}

/// Explicit bound on distinct groups (limit on everything): 2^20 groups
/// ≈ TPC-H SF1's largest GROUP BY with two orders of magnitude of head
/// room. Exceeding it is a workload we never promised to handle — crash
/// loudly rather than grow without bound. Revisit trigger recorded in
/// docs/plan-tpch.md (sort-based grouping is the standard fallback).
pub(crate) const MAX_GROUP_COUNT: usize = 1 << 20;

pub struct HashAggregate {
    schema: Arc<Schema>,
    child: Box<dyn Executor>,
    group_by: Vec<usize>,
    aggregates: Vec<AggregateFn>,
    /// Compiled per-row argument evaluators, parallel to `aggregates`
    /// (`None` for `COUNT(*)`). Compiled once at construction so the
    /// hot loop just calls closures.
    args: Vec<Option<ArgEvaluator>>,
    /// `None` until first `next()` computes; then the buffered output rows.
    output: Option<std::vec::IntoIter<Tuple>>,
}

impl HashAggregate {
    pub fn new(
        child: Box<dyn Executor>,
        group_by: Vec<usize>,
        aggregates: Vec<AggregateFn>,
    ) -> Result<Self> {
        if aggregates.is_empty() {
            return Err(Error::SqlParse(
                "HashAggregate requires at least one aggregate".into(),
            ));
        }
        // Planner/binder guarantee in-range group columns; a violation
        // here is a coordinate-remap bug, not user error.
        let input_width = child.schema().columns.len();
        assert!(group_by.iter().all(|&c| c < input_width));
        let schema = Arc::new(build_aggregate_schema(
            child.schema(),
            &group_by,
            &aggregates,
        )?);
        let args: Vec<Option<ArgEvaluator>> = aggregates
            .iter()
            .map(|a| a.arg().cloned().map(Expression::compile))
            .collect();
        Ok(Self {
            schema,
            child,
            group_by,
            aggregates,
            args,
            output: None,
        })
    }

    fn new_states(&self, input_schema: &Schema) -> Result<Vec<AggState>> {
        self.aggregates
            .iter()
            .map(|a| AggState::new_for(a, input_schema))
            .collect()
    }

    /// Whole-table path: one output row, even on empty input.
    fn compute_ungrouped(&mut self) -> Result<Vec<Tuple>> {
        let input_schema = self.child.schema().clone();
        let mut states = self.new_states(&input_schema)?;

        while let Some(row) = self.child.next()? {
            for (k, (agg, state)) in self.aggregates.iter().zip(states.iter_mut()).enumerate() {
                let value = self.args[k].as_ref().map(|f| f(&row));
                update_state(agg, state, value)?;
            }
        }

        let mut output = Vec::with_capacity(states.len());
        for (agg, state) in self.aggregates.iter().zip(states.into_iter()) {
            output.push(finalize_state(agg, state)?);
        }
        Ok(vec![output])
    }

    /// GROUP BY path: one output row per distinct key (keys ++
    /// aggregates), zero rows on empty input, canonical key order.
    fn compute_grouped(&mut self) -> Result<Vec<Tuple>> {
        let input_schema = self.child.schema().clone();
        let mut groups: HashMap<Vec<Value>, Vec<AggState>> = HashMap::new();

        while let Some(row) = self.child.next()? {
            let key: Vec<Value> = self.group_by.iter().map(|&c| row[c].clone()).collect();
            let group_count = groups.len();
            let states = match groups.entry(key) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => {
                    assert!(
                        group_count < MAX_GROUP_COUNT,
                        "GROUP BY exceeded {} distinct groups",
                        MAX_GROUP_COUNT
                    );
                    v.insert(self.new_states(&input_schema)?)
                }
            };
            for (k, (agg, state)) in self.aggregates.iter().zip(states.iter_mut()).enumerate() {
                let value = self.args[k].as_ref().map(|f| f(&row));
                update_state(agg, state, value)?;
            }
        }

        let mut keyed: Vec<(Vec<Value>, Vec<AggState>)> = groups.into_iter().collect();
        keyed.sort_by(|(a, _), (b, _)| compare_keys(a, b));

        let mut output = Vec::with_capacity(keyed.len());
        for (key, states) in keyed {
            let mut row = key;
            for (agg, state) in self.aggregates.iter().zip(states.into_iter()) {
                row.push(finalize_state(agg, state)?);
            }
            output.push(row);
        }
        Ok(output)
    }
}

impl Executor for HashAggregate {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.output.is_none() {
            let rows = if self.group_by.is_empty() {
                self.compute_ungrouped()?
            } else {
                self.compute_grouped()?
            };
            self.output = Some(rows.into_iter());
        }
        Ok(self.output.as_mut().unwrap().next())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let mut labels: Vec<String> = Vec::with_capacity(self.aggregates.len() + 1);
        if !self.group_by.is_empty() {
            // `c{i}` matches how aggregate args render their columns, so a
            // group key reads as a column reference, not an integer literal.
            let keys: Vec<String> = self.group_by.iter().map(|c| format!("c{c}")).collect();
            labels.push(format!("group=({})", keys.join(",")));
        }
        // Args render through `Expression::Display` (`Column(i) → c{i}`), so
        // a column-only arg reads as `SUM(c3)`, not the ambiguous `SUM(3)`.
        labels.extend(self.aggregates.iter().map(|a| match a {
            AggregateFn::CountStar => "COUNT(*)".to_string(),
            AggregateFn::Count(arg) => format!("COUNT({arg})"),
            AggregateFn::CountDistinct(arg) => format!("COUNT(DISTINCT {arg})"),
            AggregateFn::Sum(arg) => format!("SUM({arg})"),
            AggregateFn::Min(arg) => format!("MIN({arg})"),
            AggregateFn::Max(arg) => format!("MAX({arg})"),
            AggregateFn::Avg(arg) => format!("AVG({arg})"),
        }));
        let mut out = format!("{}HashAggregate[{}]\n", pad, labels.join(", "));
        out.push_str(&self.child.explain(indent + 1));
        out
    }
}

/// Output schema: group-key columns (cloned from the input schema, in
/// `group_by` order) followed by one synthetic column per aggregate. The
/// column names and types come from the IR-level `aggregate_output_columns`
/// (single source for the type-promotion rules); a type that can't be
/// inferred or that the aggregate rejects (non-numeric SUM/AVG) errors
/// loudly rather than reaching the accumulator.
pub(crate) fn build_aggregate_schema(
    input: &Schema,
    group_by: &[usize],
    aggregates: &[AggregateFn],
) -> Result<Schema> {
    let specs: Vec<AggregateSpec> = aggregates.iter().map(spec_of).collect();
    let columns = crate::sql::ir::logical::aggregate_output_columns(input, group_by, &specs)?;
    Ok(Schema {
        name: "aggregate".into(),
        // Synthetic — not a real table.
        table_id: TableId(0),
        columns,
        primary_key: vec![],
    })
}

/// Total order over equal-length group keys: position by position, NULL
/// first (the Sort operator's NULL = −∞ convention), non-NULL pairs via
/// the canonical comparator.
pub(crate) fn compare_keys(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    debug_assert_eq!(a.len(), b.len());
    for (va, vb) in a.iter().zip(b.iter()) {
        let ord = match (matches!(va, Value::Null), matches!(vb, Value::Null)) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => compare_values(va, vb),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Fold one input row into each aggregate's accumulator. `arg_value` is
/// the aggregate's argument evaluated against the row (`None` only for
/// `COUNT(*)`); the caller compiles and runs the argument closure.
pub(crate) fn update_state(
    agg: &AggregateFn,
    state: &mut AggState,
    arg_value: Option<Value>,
) -> Result<()> {
    match (agg, state) {
        (AggregateFn::CountStar, AggState::Count(c)) => {
            *c += 1;
            Ok(())
        }
        (AggregateFn::Count(_), AggState::Count(c)) => {
            // COUNT(expr): count non-NULL evaluated values.
            if let Some(v) = &arg_value {
                if !matches!(v, Value::Null) {
                    *c += 1;
                }
            }
            Ok(())
        }
        (AggregateFn::CountDistinct(_), AggState::DistinctSet(set)) => {
            if let Some(v) = arg_value {
                if !matches!(v, Value::Null) {
                    set.insert(v);
                }
            }
            Ok(())
        }
        (AggregateFn::Sum(_), AggState::SumInt(acc)) => {
            // checked_add: a wrapped sum is a silently wrong answer — SQL
            // engines raise numeric overflow instead (E13).
            match arg_value {
                Some(Value::Int32(v)) => {
                    *acc = Some(checked_sum(acc.unwrap_or(0), v as i64, "SUM")?);
                }
                Some(Value::Int64(v)) => {
                    *acc = Some(checked_sum(acc.unwrap_or(0), v, "SUM")?);
                }
                Some(Value::Null) | None => {}
                Some(other) => {
                    return Err(Error::SqlParse(format!(
                        "SUM expected integer, got {:?}",
                        other
                    )));
                }
            }
            Ok(())
        }
        (AggregateFn::Sum(_), AggState::SumDecimal(acc)) => {
            match arg_value {
                Some(Value::Decimal(d)) => {
                    *acc = Some(match acc {
                        None => d,
                        Some(prev) => prev
                            .add(&d)
                            .map_err(|e| Error::SqlParse(format!("SUM: {}", e)))?,
                    });
                }
                Some(Value::Null) | None => {}
                Some(other) => {
                    return Err(Error::SqlParse(format!("SUM(Decimal) got {:?}", other)));
                }
            }
            Ok(())
        }
        (AggregateFn::Min(_), AggState::MinMaxValue(acc))
        | (AggregateFn::Max(_), AggState::MinMaxValue(acc)) => {
            let v = match arg_value {
                Some(v) if !matches!(v, Value::Null) => v,
                // NULL argument (or COUNT(*), which never lands here): skip.
                _ => return Ok(()),
            };
            *acc = Some(match acc {
                None => v,
                Some(prev) => {
                    let ord = compare_values(prev, &v);
                    let want_smaller = matches!(agg, AggregateFn::Min(_));
                    if (want_smaller && ord == std::cmp::Ordering::Greater)
                        || (!want_smaller && ord == std::cmp::Ordering::Less)
                    {
                        v
                    } else {
                        prev.clone()
                    }
                }
            });
            Ok(())
        }
        (AggregateFn::Avg(_), AggState::AvgInt { sum, count }) => {
            match arg_value {
                Some(Value::Int32(v)) => {
                    *sum = Some(checked_sum(sum.unwrap_or(0), v as i64, "AVG")?);
                    *count += 1;
                }
                Some(Value::Int64(v)) => {
                    *sum = Some(checked_sum(sum.unwrap_or(0), v, "AVG")?);
                    *count += 1;
                }
                Some(Value::Null) | None => {}
                Some(other) => {
                    return Err(Error::SqlParse(format!(
                        "AVG expected integer, got {:?}",
                        other
                    )));
                }
            }
            Ok(())
        }
        (AggregateFn::Avg(_), AggState::AvgDecimal { sum, count }) => {
            match arg_value {
                Some(Value::Decimal(d)) => {
                    *sum = Some(match sum {
                        None => d,
                        Some(prev) => prev
                            .add(&d)
                            .map_err(|e| Error::SqlParse(format!("AVG: {}", e)))?,
                    });
                    *count += 1;
                }
                Some(Value::Null) | None => {}
                Some(other) => {
                    return Err(Error::SqlParse(format!("AVG(Decimal) got {:?}", other)));
                }
            }
            Ok(())
        }
        _ => unreachable!("agg/state mismatch — constructed in lockstep"),
    }
}

pub(crate) fn finalize_state(agg: &AggregateFn, state: AggState) -> Result<Value> {
    Ok(match (agg, state) {
        (AggregateFn::CountStar | AggregateFn::Count(_), AggState::Count(c)) => {
            Value::Int64(c as i64)
        }
        (AggregateFn::CountDistinct(_), AggState::DistinctSet(set)) => {
            Value::Int64(set.len() as i64)
        }
        (AggregateFn::Sum(_), AggState::SumInt(None)) => Value::Null,
        (AggregateFn::Sum(_), AggState::SumInt(Some(v))) => Value::Int64(v),
        (AggregateFn::Sum(_), AggState::SumDecimal(None)) => Value::Null,
        (AggregateFn::Sum(_), AggState::SumDecimal(Some(d))) => Value::Decimal(d),
        (AggregateFn::Min(_) | AggregateFn::Max(_), AggState::MinMaxValue(opt)) => {
            opt.unwrap_or(Value::Null)
        }
        (AggregateFn::Avg(_), AggState::AvgInt { sum: None, .. }) => Value::Null,
        (
            AggregateFn::Avg(_),
            AggState::AvgInt {
                sum: Some(s),
                count,
            },
        ) => {
            if count == 0 {
                Value::Null
            } else {
                // Promote to Decimal with scale 4 for fractional result.
                // mantissa = round(sum·10_000 / count), half away from zero
                // (E14 — SQL rounds: AVG([1,2,2]) = 1.6667, not the
                // truncated 1.6666). Computed in i128; the i64 narrowing
                // must be checked (a near-i64::MAX sum over few rows
                // overflows the scale-4 mantissa).
                let mantissa = crate::types::decimal::div_i128_round_half_away(
                    s as i128 * 10_000,
                    count as i128,
                );
                let mantissa = i64::try_from(mantissa).map_err(|_| {
                    crate::common::Error::NumericOverflow(format!(
                        "AVG result mantissa {} exceeds i64",
                        mantissa
                    ))
                })?;
                Value::Decimal(Decimal::from_i64_with_scale(mantissa, 4))
            }
        }
        (AggregateFn::Avg(_), AggState::AvgDecimal { sum: None, .. }) => Value::Null,
        (
            AggregateFn::Avg(_),
            AggState::AvgDecimal {
                sum: Some(d),
                count,
            },
        ) => {
            if count == 0 {
                Value::Null
            } else {
                // Decimal AVG: round(sum / count), half away from zero,
                // preserving the input scale (E14 — was truncating).
                // This rounding MUST match `Decimal::div`'s (both use the
                // shared `div_i128_round_half_away`): a user may write either
                // `AVG(x)` or the explicit `SUM(x) / COUNT(x)` — the latter now
                // routes through `Decimal::div` — and the two spellings must
                // yield the same value.
                // NOTE (plan deviation): reference engines also WIDEN the
                // result scale; we keep the column's scale — the rounding
                // fix lands now, scale-widening waits for a typed-AVG
                // output-schema story.
                let mantissa = crate::types::decimal::div_i128_round_half_away(
                    d.mantissa() as i128,
                    count as i128,
                );
                // |round(m/c)| ≤ |m| for c ≥ 1, so the i64 cast is exact.
                Value::Decimal(Decimal::from_i64_with_scale(mantissa as i64, d.scale()))
            }
        }
        _ => unreachable!("finalize agg/state mismatch"),
    })
}

/// `acc + v` or a `NumericOverflow` error naming the aggregate — silent
/// wrapping would return a confidently wrong number (E13).
fn checked_sum(acc: i64, v: i64, agg_name: &str) -> Result<i64> {
    acc.checked_add(v).ok_or_else(|| {
        crate::common::Error::NumericOverflow(format!("{}: {} + {} exceeds i64", agg_name, acc, v))
    })
}

/// Type-aware ordering for MIN/MAX — the canonical `Value::compare_sql`
/// (same ordering as Sort and predicate evaluation). Callers skip NULLs
/// before reaching here, so an incomparable pair means mixed-type data
/// in one aggregated column — an upstream constraint bug. Crash in dev;
/// keep the current accumulator in release rather than mis-pick silently.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match a.compare_sql(b) {
        Some(ord) => ord,
        None => {
            debug_assert!(false, "MIN/MAX: incomparable values {:?} vs {:?}", a, b);
            std::cmp::Ordering::Equal
        }
    }
}
