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

use crate::catalog::{ColumnDef, Schema, TableId};
use crate::common::Result;
use crate::execution::{Executor, Tuple};
use crate::types::{ColumnType, Decimal, Value};

/// A single aggregate function applied to a column (or the entire row,
/// for `COUNT(*)`).
#[derive(Debug, Clone)]
pub enum AggregateFn {
    /// `COUNT(*)` — counts every input row.
    CountStar,
    /// `COUNT(col)` — counts non-NULL values of the given column.
    Count(usize),
    /// `COUNT(DISTINCT col)` — counts unique non-NULL values. Tracks a
    /// `HashSet<Value>` per row's column value; finalizes to the set
    /// size. TPC-C StockLevel needs this for its `s_i_id` count.
    CountDistinct(usize),
    /// `SUM(col)` — running sum; NULL on empty / all-NULL input.
    Sum(usize),
    /// `MIN(col)` — smallest non-NULL value.
    Min(usize),
    /// `MAX(col)` — largest non-NULL value.
    Max(usize),
    /// `AVG(col)` — sum / count of non-NULL values. NULL on empty input.
    Avg(usize),
}

impl AggregateFn {
    /// Column name for the synthetic schema entry this aggregate emits.
    fn output_name(&self, input_schema: &Schema) -> String {
        match self {
            AggregateFn::CountStar => "count_star".to_string(),
            AggregateFn::Count(i) => format!("count_{}", input_schema.columns[*i].name),
            AggregateFn::CountDistinct(i) => {
                format!("count_distinct_{}", input_schema.columns[*i].name)
            }
            AggregateFn::Sum(i) => format!("sum_{}", input_schema.columns[*i].name),
            AggregateFn::Min(i) => format!("min_{}", input_schema.columns[*i].name),
            AggregateFn::Max(i) => format!("max_{}", input_schema.columns[*i].name),
            AggregateFn::Avg(i) => format!("avg_{}", input_schema.columns[*i].name),
        }
    }

    /// Output column type. `COUNT` is always `Int64`. `SUM(Int32)`
    /// promotes to `Int64` to avoid overflow. `MIN`/`MAX` preserve the
    /// input type. `AVG` is `Decimal { scale: 4 }` for integer inputs
    /// (matches typical SQL behavior of producing a fractional result)
    /// and the input's scale for `Decimal` inputs.
    fn output_type(&self, input_schema: &Schema) -> ColumnType {
        match self {
            AggregateFn::CountStar | AggregateFn::Count(_) | AggregateFn::CountDistinct(_) => {
                ColumnType::Int64
            }
            AggregateFn::Sum(i) | AggregateFn::Max(i) | AggregateFn::Min(i) => {
                let input = input_schema.columns[*i].ty;
                if matches!(self, AggregateFn::Sum(_)) && matches!(input, ColumnType::Int32) {
                    // Promote to Int64 for sum-of-Int32.
                    ColumnType::Int64
                } else {
                    input
                }
            }
            AggregateFn::Avg(i) => match input_schema.columns[*i].ty {
                ColumnType::Int32 | ColumnType::Int64 => ColumnType::Decimal {
                    precision: 18,
                    scale: 4,
                },
                other => other,
            },
        }
    }
}

/// Per-aggregate accumulator state.
pub(crate) enum AggState {
    /// `COUNT(*)` and `COUNT(col)`.
    Count(u64),
    /// `COUNT(DISTINCT col)` — tracks seen non-NULL values.
    DistinctSet(HashSet<Value>),
    /// `SUM`. None until the first non-NULL value. Numeric variant kept
    /// in lock-step with the input column's type.
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
            AggregateFn::Sum(i) => match input_schema.columns[*i].ty {
                ColumnType::Int32 | ColumnType::Int64 => Ok(AggState::SumInt(None)),
                ColumnType::Decimal { .. } => Ok(AggState::SumDecimal(None)),
                other => Err(crate::common::Error::SqlParse(format!(
                    "SUM of non-numeric column type {:?} not supported",
                    other
                ))),
            },
            AggregateFn::Min(_) | AggregateFn::Max(_) => Ok(AggState::MinMaxValue(None)),
            AggregateFn::Avg(i) => match input_schema.columns[*i].ty {
                ColumnType::Int32 | ColumnType::Int64 => Ok(AggState::AvgInt {
                    sum: None,
                    count: 0,
                }),
                ColumnType::Decimal { .. } => Ok(AggState::AvgDecimal {
                    sum: None,
                    count: 0,
                }),
                other => Err(crate::common::Error::SqlParse(format!(
                    "AVG of non-numeric column type {:?} not supported",
                    other
                ))),
            },
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
            return Err(crate::common::Error::SqlParse(
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
        ));
        Ok(Self {
            schema,
            child,
            group_by,
            aggregates,
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
            for (agg, state) in self.aggregates.iter().zip(states.iter_mut()) {
                update_state(agg, state, &row)?;
            }
        }

        let mut output = Vec::with_capacity(states.len());
        for (agg, state) in self.aggregates.iter().zip(states.into_iter()) {
            output.push(finalize_state(agg, state, &input_schema)?);
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
            for (agg, state) in self.aggregates.iter().zip(states.iter_mut()) {
                update_state(agg, state, &row)?;
            }
        }

        let mut keyed: Vec<(Vec<Value>, Vec<AggState>)> = groups.into_iter().collect();
        keyed.sort_by(|(a, _), (b, _)| compare_keys(a, b));

        let mut output = Vec::with_capacity(keyed.len());
        for (key, states) in keyed {
            let mut row = key;
            for (agg, state) in self.aggregates.iter().zip(states.into_iter()) {
                row.push(finalize_state(agg, state, &input_schema)?);
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
            // Grouped-only segment keeps ungrouped EXPLAIN output
            // byte-identical to the pre-GROUP-BY rendering.
            let keys: Vec<String> = self.group_by.iter().map(|c| c.to_string()).collect();
            labels.push(format!("group=({})", keys.join(",")));
        }
        labels.extend(self.aggregates.iter().map(|a| match a {
            AggregateFn::CountStar => "COUNT(*)".to_string(),
            AggregateFn::Count(i) => format!("COUNT({})", i),
            AggregateFn::CountDistinct(i) => format!("COUNT(DISTINCT {})", i),
            AggregateFn::Sum(i) => format!("SUM({})", i),
            AggregateFn::Min(i) => format!("MIN({})", i),
            AggregateFn::Max(i) => format!("MAX({})", i),
            AggregateFn::Avg(i) => format!("AVG({})", i),
        }));
        let mut out = format!("{}HashAggregate[{}]\n", pad, labels.join(", "));
        out.push_str(&self.child.explain(indent + 1));
        out
    }
}

/// Output schema: group-key columns (cloned from the input schema, in
/// `group_by` order) followed by one synthetic column per aggregate.
pub(crate) fn build_aggregate_schema(
    input: &Schema,
    group_by: &[usize],
    aggregates: &[AggregateFn],
) -> Schema {
    let mut columns: Vec<ColumnDef> = group_by.iter().map(|&c| input.columns[c].clone()).collect();
    columns.extend(aggregates.iter().map(|a| ColumnDef {
        name: a.output_name(input),
        ty: a.output_type(input),
        // Aggregates can return NULL (empty input → SUM/AVG/MIN/MAX).
        // COUNT returns 0, never NULL, but unifying as nullable keeps
        // the schema simple and matches PostgreSQL.
        nullable: true,
        default: None,
    }));
    Schema {
        name: "aggregate".into(),
        // Synthetic — not a real table.
        table_id: TableId(0),
        columns,
        primary_key: vec![],
    }
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

/// Fold one input row into each aggregate's accumulator.
pub(crate) fn update_state(agg: &AggregateFn, state: &mut AggState, row: &Tuple) -> Result<()> {
    match (agg, state) {
        (AggregateFn::CountStar, AggState::Count(c)) => {
            *c += 1;
            Ok(())
        }
        (AggregateFn::Count(i), AggState::Count(c)) => {
            if !matches!(row[*i], Value::Null) {
                *c += 1;
            }
            Ok(())
        }
        (AggregateFn::CountDistinct(i), AggState::DistinctSet(set)) => {
            let v = &row[*i];
            if !matches!(v, Value::Null) {
                set.insert(v.clone());
            }
            Ok(())
        }
        (AggregateFn::Sum(i), AggState::SumInt(acc)) => {
            // checked_add: a wrapped sum is a silently wrong answer — SQL
            // engines raise numeric overflow instead (E13).
            match &row[*i] {
                Value::Int32(v) => {
                    *acc = Some(checked_sum(acc.unwrap_or(0), *v as i64, "SUM")?);
                }
                Value::Int64(v) => {
                    *acc = Some(checked_sum(acc.unwrap_or(0), *v, "SUM")?);
                }
                Value::Null => {}
                other => {
                    return Err(crate::common::Error::SqlParse(format!(
                        "SUM expected integer, got {:?}",
                        other
                    )));
                }
            }
            Ok(())
        }
        (AggregateFn::Sum(i), AggState::SumDecimal(acc)) => {
            match &row[*i] {
                Value::Decimal(d) => {
                    *acc = Some(match acc {
                        None => *d,
                        Some(prev) => prev
                            .add(d)
                            .map_err(|e| crate::common::Error::SqlParse(format!("SUM: {}", e)))?,
                    });
                }
                Value::Null => {}
                other => {
                    return Err(crate::common::Error::SqlParse(format!(
                        "SUM(Decimal) got {:?}",
                        other
                    )));
                }
            }
            Ok(())
        }
        (AggregateFn::Min(i), AggState::MinMaxValue(acc))
        | (AggregateFn::Max(i), AggState::MinMaxValue(acc)) => {
            let v = &row[*i];
            if matches!(v, Value::Null) {
                return Ok(());
            }
            *acc = Some(match acc {
                None => v.clone(),
                Some(prev) => {
                    let ord = compare_values(prev, v);
                    let want_smaller = matches!(agg, AggregateFn::Min(_));
                    if (want_smaller && ord == std::cmp::Ordering::Greater)
                        || (!want_smaller && ord == std::cmp::Ordering::Less)
                    {
                        v.clone()
                    } else {
                        prev.clone()
                    }
                }
            });
            Ok(())
        }
        (AggregateFn::Avg(i), AggState::AvgInt { sum, count }) => {
            match &row[*i] {
                Value::Int32(v) => {
                    *sum = Some(checked_sum(sum.unwrap_or(0), *v as i64, "AVG")?);
                    *count += 1;
                }
                Value::Int64(v) => {
                    *sum = Some(checked_sum(sum.unwrap_or(0), *v, "AVG")?);
                    *count += 1;
                }
                Value::Null => {}
                other => {
                    return Err(crate::common::Error::SqlParse(format!(
                        "AVG expected integer, got {:?}",
                        other
                    )));
                }
            }
            Ok(())
        }
        (AggregateFn::Avg(i), AggState::AvgDecimal { sum, count }) => {
            match &row[*i] {
                Value::Decimal(d) => {
                    *sum = Some(match sum {
                        None => *d,
                        Some(prev) => prev
                            .add(d)
                            .map_err(|e| crate::common::Error::SqlParse(format!("AVG: {}", e)))?,
                    });
                    *count += 1;
                }
                Value::Null => {}
                other => {
                    return Err(crate::common::Error::SqlParse(format!(
                        "AVG(Decimal) got {:?}",
                        other
                    )));
                }
            }
            Ok(())
        }
        _ => unreachable!("agg/state mismatch — constructed in lockstep"),
    }
}

pub(crate) fn finalize_state(agg: &AggregateFn, state: AggState, _input: &Schema) -> Result<Value> {
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
