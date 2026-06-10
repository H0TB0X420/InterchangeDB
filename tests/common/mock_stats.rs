//! Synthetic statistics provider for cost-model tests. No consumers yet —
//! Phase 14 (Selinger + `CostModel`) will be the first. The surface here is
//! deliberately minimal: row counts, NDV, range bounds. Histograms are
//! deferred to Phase 14 (where the cost model determines the representation).
//!
//! Design choice: this is a plain struct, not a trait impl. Phase 14 will
//! introduce a `StatsProvider` trait; at that point we add either a `From`
//! impl or a direct trait impl on `MockStats`. Speculatively defining the
//! trait now would risk pre-committing to a shape Phase 14's cost model
//! disagrees with.

use interchangedb::catalog::{ColumnId, TableId};
use interchangedb::types::Value;
use std::collections::HashMap;

#[derive(Default)]
pub struct MockStats {
    table_rows: HashMap<TableId, u64>,
    column_ndv: HashMap<(TableId, ColumnId), u64>,
    column_min: HashMap<(TableId, ColumnId), Value>,
    column_max: HashMap<(TableId, ColumnId), Value>,
    // NOTE: histograms deferred to Phase 14 — adding them now means designing
    // the bucket representation without a consumer. Equi-width is the planned
    // shape; the cost model will dictate the API.
}

impl MockStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_rows(mut self, table: TableId, rows: u64) -> Self {
        self.table_rows.insert(table, rows);
        self
    }

    pub fn with_ndv(mut self, table: TableId, col: ColumnId, ndv: u64) -> Self {
        self.column_ndv.insert((table, col), ndv);
        self
    }

    pub fn with_range(mut self, table: TableId, col: ColumnId, min: Value, max: Value) -> Self {
        self.column_min.insert((table, col), min);
        self.column_max.insert((table, col), max);
        self
    }

    pub fn rows(&self, table: TableId) -> Option<u64> {
        self.table_rows.get(&table).copied()
    }

    pub fn ndv(&self, table: TableId, col: ColumnId) -> Option<u64> {
        self.column_ndv.get(&(table, col)).copied()
    }

    pub fn min(&self, table: TableId, col: ColumnId) -> Option<&Value> {
        self.column_min.get(&(table, col))
    }

    pub fn max(&self, table: TableId, col: ColumnId) -> Option<&Value> {
        self.column_max.get(&(table, col))
    }

    /// Selectivity of an equality predicate on `(table, col)`. Returns
    /// `Some(1.0 / ndv)` if NDV is known and non-zero, else `None`. The
    /// caller (cost model) decides the fallback (typically 0.1).
    pub fn selectivity_eq(&self, table: TableId, col: ColumnId) -> Option<f64> {
        self.ndv(table, col)
            .filter(|&n| n > 0)
            .map(|n| 1.0 / n as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use interchangedb::types::Value;

    fn t() -> TableId {
        TableId(100)
    }
    fn c() -> ColumnId {
        ColumnId(0)
    }

    #[test]
    fn empty_stats_returns_none_everywhere() {
        let s = MockStats::new();
        assert_eq!(s.rows(t()), None);
        assert_eq!(s.ndv(t(), c()), None);
        assert!(s.min(t(), c()).is_none());
        assert!(s.max(t(), c()).is_none());
        assert_eq!(s.selectivity_eq(t(), c()), None);
    }

    #[test]
    fn builder_chains_all_fields() {
        let s = MockStats::new()
            .with_rows(t(), 10_000)
            .with_ndv(t(), c(), 100)
            .with_range(t(), c(), Value::Int32(1), Value::Int32(10_000));
        assert_eq!(s.rows(t()), Some(10_000));
        assert_eq!(s.ndv(t(), c()), Some(100));
        assert_eq!(s.min(t(), c()), Some(&Value::Int32(1)));
        assert_eq!(s.max(t(), c()), Some(&Value::Int32(10_000)));
    }

    #[test]
    fn selectivity_eq_is_reciprocal_of_ndv() {
        let s = MockStats::new().with_ndv(t(), c(), 4);
        assert_eq!(s.selectivity_eq(t(), c()), Some(0.25));
    }

    #[test]
    fn selectivity_eq_with_zero_ndv_returns_none() {
        // NDV=0 is nonsensical (a column always has at least one distinct
        // value if any rows exist). Test that the helper rejects it rather
        // than dividing by zero.
        let s = MockStats::new().with_ndv(t(), c(), 0);
        assert_eq!(s.selectivity_eq(t(), c()), None);
    }
}
