//! `CostModel` (P14.3) — operator-level cost estimation for plan choice.
//!
//! Phase 14 introduces this trait so planners (P14.6 onward) can compare
//! alternative plans by summing per-operator costs. The first impl
//! `DefaultCostModel` uses textbook formulas with fixed IO/CPU weights;
//! the calibration story (predicted vs actual IO) is a deferred item in
//! `ISSUES.md` and lands in Phase 16's tuning loop.
//!
//! ## What's cost in?
//!
//! Each operator reports a `Cost { io_units, cpu_units }`. The trait
//! `total(weights)` converts to a single `f64` the planner can rank by.
//! Tracking IO and CPU separately lets later phases re-weigh per
//! workload — TPC-C is latency-sensitive (CPU dominates after the
//! buffer pool is hot), TPC-H is bandwidth-bound (IO dominates).
//!
//! ## What's NOT in scope here
//!
//! - **Memory pressure / spill cost.** Phase 20 introduces the spill
//!   path; Phase 14 assumes everything fits in memory.
//! - **Cardinality estimation.** Selectivity (`P14.4`) is a separate
//!   concern that produces the row-count inputs to these formulas.
//! - **Plan-shape costing.** The trait is per-operator. A planner-side
//!   walker sums child costs + the operator's own contribution.

use std::fmt;

/// Two-axis operator cost. IO and CPU are tracked separately so the
/// concrete weighting can be tuned without changing the operator
/// formulas (Phase 16 calibration target).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost {
    pub io_units: f64,
    pub cpu_units: f64,
}

impl Cost {
    pub const ZERO: Cost = Cost {
        io_units: 0.0,
        cpu_units: 0.0,
    };

    /// Sum two costs (e.g., to total a child + its parent operator's
    /// own contribution).
    // CLIPPY-ALLOW(should_implement_trait): inherent `add` reads naturally at
    // the cost-summation call sites; a std::ops::Add impl would force `+` and a
    // trait import for no real gain.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Cost) -> Cost {
        Cost {
            io_units: self.io_units + other.io_units,
            cpu_units: self.cpu_units + other.cpu_units,
        }
    }

    /// Collapse to a single scalar using `weights`. The planner ranks
    /// plans by this value; lower is better.
    pub fn total(self, weights: &CostWeights) -> f64 {
        self.io_units * weights.io_weight + self.cpu_units * weights.cpu_weight
    }
}

impl Default for Cost {
    fn default() -> Self {
        Cost::ZERO
    }
}

impl fmt::Display for Cost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "io={:.1} cpu={:.1}", self.io_units, self.cpu_units)
    }
}

/// Relative weights for collapsing `Cost` to a scalar. Defaults match
/// a rough "one disk read ≈ 1000 instructions" rule of thumb that
/// Phase 16's tuning loop will refine.
#[derive(Debug, Clone, Copy)]
pub struct CostWeights {
    pub io_weight: f64,
    pub cpu_weight: f64,
}

impl Default for CostWeights {
    fn default() -> Self {
        CostWeights {
            io_weight: 1.0,
            cpu_weight: 0.001,
        }
    }
}

/// Operator-level cost estimator. Each method returns the cost the
/// *operator itself* adds to its child's cost — the planner sums
/// across the tree.
///
/// Numeric arguments are "card" (cardinality, i.e. row count) and
/// "page" (number of pages of input). All `f64` so fractional
/// selectivity propagates cleanly.
pub trait CostModel: Send + Sync {
    fn cost_seq_scan(&self, table_pages: f64, table_rows: f64) -> Cost;
    fn cost_index_scan(&self, matching_rows: f64) -> Cost;
    fn cost_filter(&self, input_rows: f64) -> Cost;
    fn cost_projection(&self, input_rows: f64) -> Cost;
    fn cost_limit(&self, output_rows: f64) -> Cost;
    fn cost_sort(&self, input_rows: f64) -> Cost;
    fn cost_hash_aggregate(&self, input_rows: f64, groups: f64) -> Cost;

    /// Nested-loop join: outer iterated once, inner re-iterated per
    /// outer row.
    fn cost_nested_loop_join(&self, outer_rows: f64, inner_rows: f64) -> Cost;

    /// Index nested-loop join: for each outer row, do one index probe
    /// returning ~`avg_matches_per_outer` rows.
    fn cost_index_nested_loop_join(&self, outer_rows: f64, avg_matches_per_outer: f64) -> Cost;

    /// Hash join: build hash table from `build_rows`, probe with
    /// `probe_rows`. Build side traditionally the smaller relation.
    fn cost_hash_join(&self, build_rows: f64, probe_rows: f64) -> Cost;

    /// Collapse a `Cost` to a single comparable scalar under this model's
    /// own weighting; lower is better. Kept on the model (rather than
    /// making callers hold a `CostWeights` and call `Cost::total`) so the
    /// DP core can rank two plans knowing only `&dyn CostModel`.
    fn scalar(&self, cost: Cost) -> f64;
}

/// Default cost model. Textbook formulas with fixed weights. Treats
/// the in-memory buffer pool as infinite (no IO double-counting on
/// re-scans within a query). Phase 16's tuning loop will replace
/// the constants here once we have actual TPC-C measurements.
#[derive(Debug, Clone)]
pub struct DefaultCostModel {
    pub weights: CostWeights,
    /// Tree height assumed for index lookups. Real BTrees in this
    /// project rarely exceed depth 4 even at TPC-C scale; we hardcode
    /// 3 as the average probe cost in IO.
    pub index_probe_io: f64,
}

impl DefaultCostModel {
    pub fn new() -> Self {
        DefaultCostModel {
            weights: CostWeights::default(),
            index_probe_io: 3.0,
        }
    }
}

impl Default for DefaultCostModel {
    fn default() -> Self {
        DefaultCostModel::new()
    }
}

impl CostModel for DefaultCostModel {
    fn cost_seq_scan(&self, table_pages: f64, table_rows: f64) -> Cost {
        // One IO per page, one CPU "tick" per row.
        Cost {
            io_units: table_pages,
            cpu_units: table_rows,
        }
    }

    fn cost_index_scan(&self, matching_rows: f64) -> Cost {
        // One probe (tree-height IO) plus per-row PK lookups
        // (`index_probe_io` per matched row — secondary index entries
        // reference the PK and require a PK-side lookup to materialize).
        Cost {
            io_units: self.index_probe_io + matching_rows * self.index_probe_io,
            cpu_units: matching_rows,
        }
    }

    fn cost_filter(&self, input_rows: f64) -> Cost {
        // No IO; one predicate evaluation per row.
        Cost {
            io_units: 0.0,
            cpu_units: input_rows,
        }
    }

    fn cost_projection(&self, input_rows: f64) -> Cost {
        Cost {
            io_units: 0.0,
            cpu_units: input_rows,
        }
    }

    fn cost_limit(&self, output_rows: f64) -> Cost {
        // Limit is essentially free; we still charge one tick per
        // emitted row so a Limit(0) plan ranks slightly cheaper than
        // Limit(1M).
        Cost {
            io_units: 0.0,
            cpu_units: output_rows.max(0.0),
        }
    }

    fn cost_sort(&self, input_rows: f64) -> Cost {
        // n log n CPU, no IO (in-memory sort; spill cost is Phase 20).
        let n = input_rows.max(1.0);
        Cost {
            io_units: 0.0,
            cpu_units: n * n.log2(),
        }
    }

    fn cost_hash_aggregate(&self, input_rows: f64, _groups: f64) -> Cost {
        // One probe per input row; output cardinality is `groups`
        // but the cost driver is the probe count.
        Cost {
            io_units: 0.0,
            cpu_units: input_rows,
        }
    }

    fn cost_nested_loop_join(&self, outer_rows: f64, inner_rows: f64) -> Cost {
        // outer * inner predicate evaluations. Inner is re-iterated
        // per outer row, but assume it's buffered after first scan so
        // we don't double-count IO.
        Cost {
            io_units: 0.0,
            cpu_units: outer_rows * inner_rows,
        }
    }

    fn cost_index_nested_loop_join(&self, outer_rows: f64, avg_matches_per_outer: f64) -> Cost {
        // Per outer row: one index probe + materialize matches.
        let per_outer = self.cost_index_scan(avg_matches_per_outer);
        Cost {
            io_units: outer_rows * per_outer.io_units,
            cpu_units: outer_rows * per_outer.cpu_units,
        }
    }

    fn cost_hash_join(&self, build_rows: f64, probe_rows: f64) -> Cost {
        // Build is one pass over build side; probe is one pass over
        // probe side. No re-iteration → ~linear total CPU. No IO
        // assumed (in-memory hash table; spill is Phase 20).
        Cost {
            io_units: 0.0,
            cpu_units: build_rows + probe_rows,
        }
    }

    fn scalar(&self, cost: Cost) -> f64 {
        cost.total(&self.weights)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_cost_sums_to_zero() {
        let c = Cost::ZERO.add(Cost::ZERO);
        assert_eq!(c, Cost::ZERO);
        assert_eq!(c.total(&CostWeights::default()), 0.0);
    }

    #[test]
    fn cost_add_is_component_wise() {
        let a = Cost {
            io_units: 1.0,
            cpu_units: 10.0,
        };
        let b = Cost {
            io_units: 2.0,
            cpu_units: 5.0,
        };
        assert_eq!(
            a.add(b),
            Cost {
                io_units: 3.0,
                cpu_units: 15.0
            }
        );
    }

    #[test]
    fn total_applies_weights() {
        let c = Cost {
            io_units: 100.0,
            cpu_units: 1_000.0,
        };
        let w = CostWeights {
            io_weight: 1.0,
            cpu_weight: 0.001,
        };
        // 100 * 1.0 + 1000 * 0.001 = 100 + 1 = 101.
        assert!((c.total(&w) - 101.0).abs() < 1e-9);
    }

    #[test]
    fn default_seq_scan_cost_grows_with_pages_and_rows() {
        let m = DefaultCostModel::new();
        let small = m.cost_seq_scan(1.0, 10.0);
        let big = m.cost_seq_scan(100.0, 1_000.0);
        assert!(big.io_units > small.io_units);
        assert!(big.cpu_units > small.cpu_units);
    }

    #[test]
    fn hash_join_beats_nested_loop_at_scale() {
        let m = DefaultCostModel::new();
        let n = 10_000.0;
        let nlj = m.cost_nested_loop_join(n, n);
        let hj = m.cost_hash_join(n, n);
        assert!(
            hj.total(&CostWeights::default()) < nlj.total(&CostWeights::default()),
            "hash join should out-cost NLJ at 10k×10k"
        );
    }

    #[test]
    fn index_nlj_beats_plain_nlj_when_matches_per_outer_is_low() {
        let m = DefaultCostModel::new();
        let outer = 1_000.0;
        let inner = 10_000.0;
        let nlj = m.cost_nested_loop_join(outer, inner);
        // 1 match per outer row on average → INLJ should dominate.
        let inlj = m.cost_index_nested_loop_join(outer, 1.0);
        let w = CostWeights::default();
        assert!(inlj.total(&w) < nlj.total(&w));
    }

    #[test]
    fn sort_cost_is_nlogn() {
        let m = DefaultCostModel::new();
        let small = m.cost_sort(100.0).cpu_units;
        let medium = m.cost_sort(1_000.0).cpu_units;
        let large = m.cost_sort(10_000.0).cpu_units;
        // CPU grows super-linearly — large/small > 100, large/medium > 10.
        assert!(large / small > 100.0);
        assert!(large / medium > 10.0);
    }
}
