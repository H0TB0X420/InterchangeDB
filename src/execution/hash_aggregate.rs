//! `HashAggregate` — whole-table aggregation operator (no GROUP BY yet).
//!
//! Computes a fixed list of aggregates (`COUNT`, `SUM`, `MIN`, `MAX`,
//! `AVG`) over the child's full output and emits a single row of
//! aggregate results.
//!
//! ## Why "Hash"?
//!
//! Phase 13's first cut has no GROUP BY, so this is effectively a
//! single-bucket aggregation. The "Hash" naming anchors the eventual
//! GROUP BY extension: same operator with a `Vec<usize>` of grouping
//! columns and a `HashMap<Vec<Value>, AggregateState>` for buckets.
//! For now there's exactly one (implicit) bucket.
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
//! accumulators, finalize each accumulator to a `Value`, return that
//! row. Subsequent `next()` calls return `None`.

use std::collections::HashSet;
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
            AggregateFn::CountStar
            | AggregateFn::Count(_)
            | AggregateFn::CountDistinct(_) => ColumnType::Int64,
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
                ColumnType::Int32 | ColumnType::Int64 => {
                    ColumnType::Decimal { precision: 18, scale: 4 }
                }
                other => other,
            },
        }
    }
}

/// Per-aggregate accumulator state.
enum AggState {
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
    AvgInt { sum: Option<i64>, count: u64 },
    AvgDecimal { sum: Option<Decimal>, count: u64 },
}

impl AggState {
    fn new_for(agg: &AggregateFn, input_schema: &Schema) -> Result<Self> {
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
                ColumnType::Int32 | ColumnType::Int64 => {
                    Ok(AggState::AvgInt { sum: None, count: 0 })
                }
                ColumnType::Decimal { .. } => Ok(AggState::AvgDecimal { sum: None, count: 0 }),
                other => Err(crate::common::Error::SqlParse(format!(
                    "AVG of non-numeric column type {:?} not supported",
                    other
                ))),
            },
        }
    }
}

pub struct HashAggregate {
    schema: Arc<Schema>,
    child: Box<dyn Executor>,
    aggregates: Vec<AggregateFn>,
    emitted: bool,
}

impl HashAggregate {
    pub fn new(child: Box<dyn Executor>, aggregates: Vec<AggregateFn>) -> Result<Self> {
        if aggregates.is_empty() {
            return Err(crate::common::Error::SqlParse(
                "HashAggregate requires at least one aggregate".into(),
            ));
        }
        let schema = Arc::new(build_aggregate_schema(child.schema(), &aggregates));
        Ok(Self {
            schema,
            child,
            aggregates,
            emitted: false,
        })
    }

    fn compute_row(&mut self) -> Result<Tuple> {
        let input_schema = self.child.schema().clone();
        let mut states: Vec<AggState> = self
            .aggregates
            .iter()
            .map(|a| AggState::new_for(a, &input_schema))
            .collect::<Result<Vec<_>>>()?;

        while let Some(row) = self.child.next()? {
            for (agg, state) in self.aggregates.iter().zip(states.iter_mut()) {
                update_state(agg, state, &row)?;
            }
        }

        let mut output = Vec::with_capacity(states.len());
        for (agg, state) in self.aggregates.iter().zip(states.into_iter()) {
            output.push(finalize_state(agg, state, &input_schema));
        }
        Ok(output)
    }
}

impl Executor for HashAggregate {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let row = self.compute_row()?;
        Ok(Some(row))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let labels: Vec<String> = self
            .aggregates
            .iter()
            .map(|a| match a {
                AggregateFn::CountStar => "COUNT(*)".to_string(),
                AggregateFn::Count(i) => format!("COUNT({})", i),
                AggregateFn::CountDistinct(i) => format!("COUNT(DISTINCT {})", i),
                AggregateFn::Sum(i) => format!("SUM({})", i),
                AggregateFn::Min(i) => format!("MIN({})", i),
                AggregateFn::Max(i) => format!("MAX({})", i),
                AggregateFn::Avg(i) => format!("AVG({})", i),
            })
            .collect();
        let mut out = format!("{}HashAggregate[{}]\n", pad, labels.join(", "));
        out.push_str(&self.child.explain(indent + 1));
        out
    }
}

fn build_aggregate_schema(input: &Schema, aggregates: &[AggregateFn]) -> Schema {
    let columns: Vec<ColumnDef> = aggregates
        .iter()
        .map(|a| ColumnDef {
            name: a.output_name(input),
            ty: a.output_type(input),
            // Aggregates can return NULL (empty input → SUM/AVG/MIN/MAX).
            // COUNT returns 0, never NULL, but unifying as nullable keeps
            // the schema simple and matches PostgreSQL.
            nullable: true,
            default: None,
        })
        .collect();
    Schema {
        name: "aggregate".into(),
        // Synthetic — not a real table.
        table_id: TableId(0),
        columns,
        primary_key: vec![],
    }
}

/// Fold one input row into each aggregate's accumulator.
fn update_state(agg: &AggregateFn, state: &mut AggState, row: &Tuple) -> Result<()> {
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
            match &row[*i] {
                Value::Int32(v) => {
                    *acc = Some(acc.unwrap_or(0).wrapping_add(*v as i64));
                }
                Value::Int64(v) => {
                    *acc = Some(acc.unwrap_or(0).wrapping_add(*v));
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
                        None => d.clone(),
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
                    *sum = Some(sum.unwrap_or(0).wrapping_add(*v as i64));
                    *count += 1;
                }
                Value::Int64(v) => {
                    *sum = Some(sum.unwrap_or(0).wrapping_add(*v));
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
                        None => d.clone(),
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

fn finalize_state(agg: &AggregateFn, state: AggState, _input: &Schema) -> Value {
    match (agg, state) {
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
        (AggregateFn::Avg(_), AggState::AvgInt { sum: Some(s), count }) => {
            if count == 0 {
                Value::Null
            } else {
                // Promote to Decimal with scale 4 for fractional result.
                // mantissa = (sum * 10_000) / count.
                let mantissa = (s as i128 * 10_000) / count as i128;
                Value::Decimal(Decimal::from_i64_with_scale(mantissa as i64, 4))
            }
        }
        (AggregateFn::Avg(_), AggState::AvgDecimal { sum: None, .. }) => Value::Null,
        (AggregateFn::Avg(_), AggState::AvgDecimal { sum: Some(d), count }) => {
            if count == 0 {
                Value::Null
            } else {
                // Decimal AVG: divide mantissa by count, preserve scale.
                let mantissa = d.mantissa() / count as i64;
                Value::Decimal(Decimal::from_i64_with_scale(mantissa, d.scale()))
            }
        }
        _ => unreachable!("finalize agg/state mismatch"),
    }
}

/// Type-aware ordering for MIN/MAX. Mirrors the existing `eval_compare`
/// rules: Int32/Int64 cross-promote; Decimal same-scale compares
/// mantissas; everything else uses derived ordering when available,
/// `Equal` otherwise (matches `eval_compare`'s false-on-mismatch).
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Value::Int32(x), Value::Int32(y)) => x.cmp(y),
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::Int32(x), Value::Int64(y)) => (*x as i64).cmp(y),
        (Value::Int64(x), Value::Int32(y)) => x.cmp(&(*y as i64)),
        (Value::Varchar(x), Value::Varchar(y)) => x.cmp(y),
        (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
        (Value::Decimal(x), Value::Decimal(y)) if x.scale() == y.scale() => {
            x.mantissa().cmp(&y.mantissa())
        }
        // Type mismatch or unsupported pair: keep current value (Equal
        // sorts as "no change"). Matches eval_compare's conservative
        // false-on-mismatch.
        _ => Equal,
    }
}
