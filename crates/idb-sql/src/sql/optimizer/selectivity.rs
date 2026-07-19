//! P14.4 — selectivity estimation for `Predicate`s.
//!
//! Given a predicate and a per-column stats lookup (NDV, histogram),
//! return the estimated fraction of rows that satisfy it. The
//! `SelingerPlanner` (P14.7) multiplies estimated selectivities into
//! cardinalities and feeds those to the `CostModel`.
//!
//! ## Estimation rules
//!
//! - `col = literal` → 1 / NDV; fallback 0.1 when stats absent.
//! - `col {<, <=, >, >=} literal` → histogram-bucket fraction for
//!   Int32/Int64 columns. Fallback to `RANGE_FALLBACK = 1/3` otherwise.
//! - `col != literal` → 1 - selectivity(col = literal).
//! - `AND a b` → independence: `s(a) * s(b)`.
//! - `OR  a b` → independence: `s(a) + s(b) - s(a)*s(b)`.
//! - `NOT a` → `1 - s(a)`.
//!
//! Independence is wrong in the general case (TPC-H notoriously breaks
//! it on `nation`/`region` joins), but it's the standard first-cut
//! approximation and Phase 14's plan calls it out as good enough until
//! Phase 16's tuning iteration.
//!
//! Selectivities are clamped to `[MIN_SELECTIVITY, 1.0]`. The lower
//! clamp avoids zero-row estimates that would short-circuit join
//! ordering with infinite cost ratios.

use crate::catalog::system_tables::{ColumnStats, HISTOGRAM_KIND_EQUI_WIDTH_INT};
use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
use crate::types::Value;

/// Fallback selectivity for `col = literal` when NDV is unknown. Matches
/// PostgreSQL's `DEFAULT_EQ_SEL`.
pub const EQ_FALLBACK: f64 = 0.1;

/// Fallback selectivity for `col {<,<=,>,>=} literal` when no histogram
/// is available. Matches PostgreSQL's `DEFAULT_INEQ_SEL`.
pub const RANGE_FALLBACK: f64 = 1.0 / 3.0;

/// Fallback equi-join selectivity when neither side has a usable NDV.
/// Same value as `EQ_FALLBACK` — an unknown join column is treated like
/// an unknown equality column, matching PostgreSQL's default.
pub const JOIN_FALLBACK: f64 = EQ_FALLBACK;

/// Minimum selectivity. Prevents zero-row estimates that would let
/// degenerate plans look infinitely cheap.
pub const MIN_SELECTIVITY: f64 = 1e-9;

/// Estimate the selectivity of `pred` against rows of a single
/// (possibly joined) schema. `column_stats[i]` is the stats row for
/// the column at tuple-global index `i`, or `None` if unanalyzed.
pub fn estimate_predicate_selectivity(
    pred: &Predicate,
    column_stats: &[Option<ColumnStats>],
) -> f64 {
    let raw = match pred {
        Predicate::Compare { op, left, right } => estimate_compare(*op, left, right, column_stats),
        Predicate::And(a, b) => {
            let sa = estimate_predicate_selectivity(a, column_stats);
            let sb = estimate_predicate_selectivity(b, column_stats);
            sa * sb
        }
        Predicate::Or(a, b) => {
            let sa = estimate_predicate_selectivity(a, column_stats);
            let sb = estimate_predicate_selectivity(b, column_stats);
            sa + sb - sa * sb
        }
        Predicate::Not(inner) => {
            let s = estimate_predicate_selectivity(inner, column_stats);
            1.0 - s
        }
    };
    raw.clamp(MIN_SELECTIVITY, 1.0)
}

/// Selinger-style equi-join selectivity for `left.a = right.b`:
/// `1 / max(ndv_a, ndv_b)`. This is the System R containment estimate —
/// the smaller-NDV column's values are assumed to be a subset of the
/// larger's, so the join's output cardinality is `|R| * |S| / max_ndv`.
/// Multiply this selectivity by `|R| * |S|` to get that cardinality.
///
/// An NDV `<= 0` means "unknown / unanalyzed" for that side. `max` makes
/// any positive NDV win over an unknown, so with one side known we use
/// it; only when *neither* side is known do we fall back to
/// `JOIN_FALLBACK`. Result is clamped to `[MIN_SELECTIVITY, 1.0]`.
///
/// NOTE (P14.10): NDV is taken from base-column stats and is NOT
/// propagated through prior joins — matching System R's original
/// simplification. A multi-join estimate therefore reuses base NDVs at
/// every level; revisiting this is a Phase 16 tuning item.
pub fn join_selectivity(ndv_left: i64, ndv_right: i64) -> f64 {
    let max_ndv = ndv_left.max(ndv_right);
    let raw = if max_ndv > 0 {
        1.0 / max_ndv as f64
    } else {
        JOIN_FALLBACK
    };
    raw.clamp(MIN_SELECTIVITY, 1.0)
}

fn estimate_compare(
    op: CompareOp,
    left: &Expression,
    right: &Expression,
    column_stats: &[Option<ColumnStats>],
) -> f64 {
    // Pull `(column_index, literal)` out of either ordering. If both
    // sides are columns or both are literals, fall back to the
    // grand-default — the planner can refine later.
    let (col_idx, lit, swapped) = match (left, right) {
        (Expression::Column(i), Expression::Literal(v)) => (*i, v, false),
        (Expression::Literal(v), Expression::Column(i)) => (*i, v, true),
        _ => return EQ_FALLBACK,
    };
    // Effective op when the literal is on the LEFT: invert the
    // comparison direction so the rest of this function can assume
    // `column op literal`.
    let effective_op = if swapped { flip_op(op) } else { op };

    let stats = match column_stats.get(col_idx).and_then(|s| s.as_ref()) {
        Some(s) => s,
        None => {
            // No stats → equality vs range fallback.
            return match effective_op {
                CompareOp::Eq => EQ_FALLBACK,
                CompareOp::Neq => 1.0 - EQ_FALLBACK,
                _ => RANGE_FALLBACK,
            };
        }
    };

    match effective_op {
        CompareOp::Eq => eq_selectivity(stats),
        CompareOp::Neq => 1.0 - eq_selectivity(stats),
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            range_selectivity(effective_op, lit, stats)
        }
    }
}

fn flip_op(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Neq => CompareOp::Neq,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Lte => CompareOp::Gte,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Gte => CompareOp::Lte,
    }
}

fn eq_selectivity(stats: &ColumnStats) -> f64 {
    if stats.ndv > 0 {
        1.0 / stats.ndv as f64
    } else {
        EQ_FALLBACK
    }
}

fn range_selectivity(op: CompareOp, lit: &Value, stats: &ColumnStats) -> f64 {
    if stats.histogram_kind != HISTOGRAM_KIND_EQUI_WIDTH_INT {
        return RANGE_FALLBACK;
    }
    let lit_int = match value_to_i64(lit) {
        Some(v) => v,
        None => return RANGE_FALLBACK,
    };
    let hist = match decode_equi_width_int_histogram(&stats.histogram_blob) {
        Some(h) => h,
        None => return RANGE_FALLBACK,
    };
    let total: u64 = hist.counts.iter().map(|c| *c as u64).sum();
    if total == 0 {
        return RANGE_FALLBACK;
    }

    // Fraction of rows strictly less than `lit_int`.
    let lt_count = fraction_below(&hist, lit_int);
    let lt = lt_count / total as f64;
    let eq = eq_selectivity(stats);
    let result = match op {
        CompareOp::Lt => lt,
        CompareOp::Lte => lt + eq,
        CompareOp::Gt => 1.0 - lt - eq,
        CompareOp::Gte => 1.0 - lt,
        _ => unreachable!(),
    };
    result.clamp(MIN_SELECTIVITY, 1.0)
}

struct EquiWidthIntHistogram {
    min: i64,
    max: i64,
    counts: Vec<u32>,
}

fn decode_equi_width_int_histogram(blob: &[u8]) -> Option<EquiWidthIntHistogram> {
    if blob.len() < 18 {
        return None;
    }
    let min = i64::from_le_bytes(blob[0..8].try_into().ok()?);
    let max = i64::from_le_bytes(blob[8..16].try_into().ok()?);
    let nb = u16::from_le_bytes(blob[16..18].try_into().ok()?) as usize;
    if blob.len() < 18 + 4 * nb {
        return None;
    }
    let mut counts = Vec::with_capacity(nb);
    for b in 0..nb {
        let off = 18 + b * 4;
        let c = u32::from_le_bytes(blob[off..off + 4].try_into().ok()?);
        counts.push(c);
    }
    Some(EquiWidthIntHistogram { min, max, counts })
}

/// Fractional row count strictly less than `target`, using the
/// histogram. Buckets entirely below `target` contribute their full
/// count; one partial bucket contributes proportionally.
fn fraction_below(hist: &EquiWidthIntHistogram, target: i64) -> f64 {
    if target <= hist.min {
        return 0.0;
    }
    if target > hist.max {
        return hist.counts.iter().map(|c| *c as u64).sum::<u64>() as f64;
    }
    let span = (hist.max - hist.min).max(0) as i128 + 1;
    let nb = hist.counts.len() as i128;
    let offset = (target - hist.min) as i128;
    let exact_bucket = (offset * nb) / span;
    let mut acc = 0.0;
    for (i, c) in hist.counts.iter().enumerate() {
        let i = i as i128;
        if i < exact_bucket {
            acc += *c as f64;
        } else if i == exact_bucket {
            // Pro-rate this bucket. bucket_low = min + i*span/nb;
            // bucket_high (exclusive) = min + (i+1)*span/nb.
            let bucket_low = hist.min as i128 + (i * span) / nb;
            let bucket_high = hist.min as i128 + ((i + 1) * span) / nb;
            let bucket_width = (bucket_high - bucket_low).max(1) as f64;
            let in_bucket = (target as i128 - bucket_low).max(0) as f64;
            let frac = (in_bucket / bucket_width).clamp(0.0, 1.0);
            acc += *c as f64 * frac;
            break;
        }
    }
    acc
}

fn value_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int32(n) => Some(*n as i64),
        Value::Int64(n) => Some(*n),
        // Date histograms are keyed by day-count, so a Date literal maps to
        // its i64 days for the same bucket math (see `analyze_table`).
        Value::Date(days) => Some(*days as i64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats_with_ndv(ndv: i64) -> Option<ColumnStats> {
        Some(ColumnStats {
            ndv,
            null_count: 0,
            histogram_kind: 0,
            histogram_blob: vec![],
        })
    }

    fn build_hist_blob(min: i64, max: i64, counts: &[u32]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&min.to_le_bytes());
        b.extend_from_slice(&max.to_le_bytes());
        b.extend_from_slice(&(counts.len() as u16).to_le_bytes());
        for c in counts {
            b.extend_from_slice(&c.to_le_bytes());
        }
        b
    }

    fn stats_with_hist(ndv: i64, min: i64, max: i64, counts: &[u32]) -> Option<ColumnStats> {
        Some(ColumnStats {
            ndv,
            null_count: 0,
            histogram_kind: HISTOGRAM_KIND_EQUI_WIDTH_INT,
            histogram_blob: build_hist_blob(min, max, counts),
        })
    }

    fn eq_pred(col: usize, lit: Value) -> Predicate {
        Predicate::Compare {
            op: CompareOp::Eq,
            left: Expression::Column(col),
            right: Expression::Literal(lit),
        }
    }

    fn cmp_pred(op: CompareOp, col: usize, lit: Value) -> Predicate {
        Predicate::Compare {
            op,
            left: Expression::Column(col),
            right: Expression::Literal(lit),
        }
    }

    #[test]
    fn missing_stats_yields_eq_fallback() {
        let pred = eq_pred(0, Value::Int32(1));
        let s = estimate_predicate_selectivity(&pred, &[None]);
        assert!((s - EQ_FALLBACK).abs() < 1e-9);
    }

    #[test]
    fn eq_uses_inverse_ndv_when_available() {
        let stats = vec![stats_with_ndv(5)];
        let s = estimate_predicate_selectivity(&eq_pred(0, Value::Int32(3)), &stats);
        assert!((s - 0.2).abs() < 1e-9);
    }

    #[test]
    fn neq_is_complement_of_eq() {
        let stats = vec![stats_with_ndv(4)];
        let eq_s = estimate_predicate_selectivity(&eq_pred(0, Value::Int32(1)), &stats);
        let neq = cmp_pred(CompareOp::Neq, 0, Value::Int32(1));
        let neq_s = estimate_predicate_selectivity(&neq, &stats);
        assert!((eq_s + neq_s - 1.0).abs() < 1e-9);
    }

    #[test]
    fn range_below_min_is_near_zero() {
        let stats = vec![stats_with_hist(50, 10, 100, &[5, 5, 5, 5])];
        let p = cmp_pred(CompareOp::Lt, 0, Value::Int32(5));
        let s = estimate_predicate_selectivity(&p, &stats);
        assert!(s < 0.01, "expected ~0 for below-min, got {}", s);
    }

    #[test]
    fn range_above_max_is_near_one() {
        let stats = vec![stats_with_hist(50, 10, 100, &[5, 5, 5, 5])];
        let p = cmp_pred(CompareOp::Lt, 0, Value::Int32(1_000_000));
        let s = estimate_predicate_selectivity(&p, &stats);
        assert!(s > 0.99, "expected ~1 above max, got {}", s);
    }

    #[test]
    fn range_at_midpoint_is_around_half() {
        let stats = vec![stats_with_hist(16, 1, 16, &[1; 16])];
        let p = cmp_pred(CompareOp::Lt, 0, Value::Int32(9));
        let s = estimate_predicate_selectivity(&p, &stats);
        assert!(
            (s - 0.5).abs() < 0.15,
            "expected ~0.5 at midpoint, got {}",
            s
        );
    }

    #[test]
    fn date_range_uses_histogram_not_fallback() {
        // A DATE column's histogram is an equi-width-INT histogram over
        // day-counts (see `analyze_table`), so a `Value::Date` literal drives
        // the same bucket math via `value_to_i64`. 16 consecutive days
        // [8760, 8775] with one row each; `col < DATE(8768)` cuts the 16-day
        // span at day 8 → fraction 8/16. The point of the test is that the
        // range estimate is the histogram-DERIVED fraction, not the
        // `RANGE_FALLBACK` constant a missing/mismatched histogram would yield.
        let stats = vec![stats_with_hist(16, 8760, 8775, &[1; 16])];
        let p = cmp_pred(CompareOp::Lt, 0, Value::Date(8768));
        let s = estimate_predicate_selectivity(&p, &stats);
        assert!(
            (s - 0.5).abs() < 0.05,
            "expected ~0.5 histogram fraction for a date range, got {s}"
        );
        assert!(
            (s - RANGE_FALLBACK).abs() > 1e-6,
            "date range fell back to RANGE_FALLBACK ({RANGE_FALLBACK}); histogram unused"
        );
    }

    #[test]
    fn literal_on_left_flips_correctly() {
        let stats = vec![stats_with_hist(16, 1, 16, &[1; 16])];
        let normal = cmp_pred(CompareOp::Gt, 0, Value::Int32(5));
        let flipped = Predicate::Compare {
            op: CompareOp::Lt,
            left: Expression::Literal(Value::Int32(5)),
            right: Expression::Column(0),
        };
        let a = estimate_predicate_selectivity(&normal, &stats);
        let b = estimate_predicate_selectivity(&flipped, &stats);
        assert!((a - b).abs() < 1e-9);
    }

    #[test]
    fn and_multiplies_selectivities() {
        let stats = vec![stats_with_ndv(10), stats_with_ndv(5)];
        let a = eq_pred(0, Value::Int32(1));
        let b = eq_pred(1, Value::Int32(2));
        let and = Predicate::And(Box::new(a), Box::new(b));
        let s = estimate_predicate_selectivity(&and, &stats);
        assert!((s - 0.02).abs() < 1e-9);
    }

    #[test]
    fn or_uses_inclusion_exclusion() {
        let stats = vec![stats_with_ndv(10), stats_with_ndv(10)];
        let a = eq_pred(0, Value::Int32(1));
        let b = eq_pred(1, Value::Int32(2));
        let or = Predicate::Or(Box::new(a), Box::new(b));
        let s = estimate_predicate_selectivity(&or, &stats);
        assert!((s - 0.19).abs() < 1e-9);
    }

    #[test]
    fn not_inverts_selectivity() {
        let stats = vec![stats_with_ndv(4)];
        let a = eq_pred(0, Value::Int32(1));
        let n = Predicate::Not(Box::new(a));
        let s = estimate_predicate_selectivity(&n, &stats);
        assert!((s - 0.75).abs() < 1e-9);
    }

    #[test]
    fn clamped_below_one_above_min() {
        let stats = vec![stats_with_hist(1, 1, 1, &[1])];
        let p = cmp_pred(CompareOp::Gte, 0, Value::Int32(1));
        let s = estimate_predicate_selectivity(&p, &stats);
        assert!(s <= 1.0 && s > 0.0);
    }

    // ---- join_selectivity (P14.10) ----

    #[test]
    fn join_selectivity_is_inverse_of_larger_ndv() {
        // max(100, 5) = 100 → 1/100. Containment: each row of the
        // 100-NDV side matches 1/100 of the other.
        let s = join_selectivity(100, 5);
        assert!((s - 0.01).abs() < 1e-9);
    }

    #[test]
    fn join_selectivity_is_symmetric() {
        assert!((join_selectivity(7, 42) - join_selectivity(42, 7)).abs() < 1e-12);
    }

    #[test]
    fn join_selectivity_uses_known_side_when_other_unknown() {
        // ndv <= 0 means unknown; the positive side wins via max().
        assert!((join_selectivity(0, 50) - 1.0 / 50.0).abs() < 1e-9);
        assert!((join_selectivity(50, -1) - 1.0 / 50.0).abs() < 1e-9);
    }

    #[test]
    fn join_selectivity_falls_back_when_both_unknown() {
        assert!((join_selectivity(0, 0) - JOIN_FALLBACK).abs() < 1e-9);
        assert!((join_selectivity(-3, -1) - JOIN_FALLBACK).abs() < 1e-9);
    }

    #[test]
    fn join_selectivity_ndv_one_is_full_pass() {
        // A constant column (NDV 1) on both sides → every row matches.
        assert!((join_selectivity(1, 1) - 1.0).abs() < 1e-9);
    }
}
