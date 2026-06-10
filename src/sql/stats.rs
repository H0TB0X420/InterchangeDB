//! `StatsProvider` (P14.9) — the optimizer's seam to table/column stats.
//!
//! Top of the cost-based planner's three-layer stats spine:
//!
//! ```text
//! Optimizer ─► Cardinality estimation ─► StatsProvider ─► stored facts
//!          └─► CostModel (numbers in, Cost out)
//! ```
//!
//! `StatsProvider` answers raw questions about stored data — row counts,
//! NDV, histograms — and knows nothing about predicates. That blindness
//! is deliberate: it lets the cardinality layer (`selectivity.rs`) sit
//! cleanly on top as the *only* layer that combines a predicate with a
//! histogram, and it keeps `CostModel` purely numeric below.
//!
//! Two impls:
//!   - [`CatalogStatsProvider`] — production; reads the P14.1 system
//!     tables through a borrowed [`Catalog`].
//!   - [`MockStatsProvider`] — hand-set cardinalities so the DP core
//!     (P14.11) can be asserted against exact, noise-free stats. This is
//!     the reason `StatsProvider` is a trait rather than a direct catalog
//!     call: a real-stats optimizer is untestable for *exact* join order.

use std::collections::HashMap;

use crate::catalog::system_tables::{ColumnStats, TableStats, HISTOGRAM_KIND_NONE};
use crate::catalog::{Catalog, TableId};
use crate::common::Result;
use crate::storage::StorageEngine;

/// Read-only access to persisted statistics, keyed by table and column.
///
/// Methods mirror the catalog's `Result<Option<_>>` shape:
///   - `Err` is a real storage fault — propagated, never swallowed
///     (corruption crashes per the safety policy).
///   - `Ok(None)` means "`ANALYZE` never ran for this target," which the
///     optimizer treats as unknown and fills with fallback defaults.
///
/// Object-safe: methods take `&self` and return owned values, so the DP
/// core can hold a `&dyn StatsProvider`.
pub trait StatsProvider {
    /// Row count for `table_id`, or `None` if never analyzed.
    fn table_stats(&self, table_id: TableId) -> Result<Option<TableStats>>;

    /// NDV / null-count / histogram for one column, or `None` if never
    /// analyzed.
    fn column_stats(&self, table_id: TableId, column_id: u32) -> Result<Option<ColumnStats>>;
}

/// Production `StatsProvider`: reads `__sys_table_stats` /
/// `__sys_column_stats` through a borrowed catalog. Cheap to construct
/// (just a reference) — built fresh inside each `plan()` call, then used
/// once to fill the `QueryStats` snapshot (P14.10).
pub struct CatalogStatsProvider<'a, E: StorageEngine> {
    catalog: &'a Catalog<E>,
}

impl<'a, E: StorageEngine> CatalogStatsProvider<'a, E> {
    pub fn new(catalog: &'a Catalog<E>) -> Self {
        CatalogStatsProvider { catalog }
    }
}

impl<'a, E: StorageEngine> StatsProvider for CatalogStatsProvider<'a, E> {
    fn table_stats(&self, table_id: TableId) -> Result<Option<TableStats>> {
        self.catalog.get_table_stats(table_id)
    }

    fn column_stats(&self, table_id: TableId, column_id: u32) -> Result<Option<ColumnStats>> {
        self.catalog.get_column_stats(table_id, column_id)
    }
}

/// Test `StatsProvider`: cardinalities set by hand via the builder
/// methods. Public (not `#[cfg(test)]`) so integration tests in `tests/`
/// can drive the optimizer without standing up a real catalog. Missing
/// entries return `Ok(None)`, exercising the optimizer's fallback path.
#[derive(Default)]
pub struct MockStatsProvider {
    table_stats: HashMap<TableId, TableStats>,
    column_stats: HashMap<(TableId, u32), ColumnStats>,
}

impl MockStatsProvider {
    pub fn new() -> Self {
        MockStatsProvider::default()
    }

    /// Set the row count for a table. Builder-style for terse setup.
    pub fn with_table_rows(mut self, table_id: TableId, row_count: i64) -> Self {
        self.table_stats.insert(table_id, TableStats { row_count });
        self
    }

    /// Set the NDV for a column (null_count 0, no histogram). NDV is the
    /// only column stat join-order DP keys off, so this is the common
    /// setup; use `with_column_stats` for the full struct.
    pub fn with_column_ndv(mut self, table_id: TableId, column_id: u32, ndv: i64) -> Self {
        self.column_stats.insert(
            (table_id, column_id),
            ColumnStats {
                ndv,
                null_count: 0,
                histogram_kind: HISTOGRAM_KIND_NONE,
                histogram_blob: Vec::new(),
            },
        );
        self
    }

    /// Set the full `ColumnStats` for a column — for selectivity tests
    /// that need a histogram blob, not just NDV.
    pub fn with_column_stats(
        mut self,
        table_id: TableId,
        column_id: u32,
        stats: ColumnStats,
    ) -> Self {
        self.column_stats.insert((table_id, column_id), stats);
        self
    }
}

impl StatsProvider for MockStatsProvider {
    fn table_stats(&self, table_id: TableId) -> Result<Option<TableStats>> {
        Ok(self.table_stats.get(&table_id).cloned())
    }

    fn column_stats(&self, table_id: TableId, column_id: u32) -> Result<Option<ColumnStats>> {
        Ok(self.column_stats.get(&(table_id, column_id)).cloned())
    }
}

/// Row count assumed for a table `ANALYZE` never touched. Non-trivial so
/// the DP doesn't treat an unanalyzed table as free (which would let any
/// plan joining it look infinitely cheap).
pub const DEFAULT_ROW_COUNT: f64 = 1_000.0;

/// An owned, query-scoped snapshot of the statistics the optimizer needs.
///
/// Built once, at the top of planning, by draining a [`StatsProvider`]
/// for every table (and the columns) referenced in one query. The DP
/// core (P14.11) then reads from this owned struct via the infallible
/// accessors below — it never re-touches the catalog, keeping
/// MVCC-decoded reads out of the enumeration hot loop (control-plane /
/// data-plane separation).
///
/// Fallbacks for unanalyzed targets live on the accessors, so the DP
/// never repeats "is this `None`?" logic: an unknown table reads as
/// `DEFAULT_ROW_COUNT`, an unknown NDV reads as `0` (which
/// `join_selectivity` interprets as "use the other side / fall back").
pub struct QueryStats {
    tables: HashMap<TableId, TableStats>,
    columns: HashMap<(TableId, u32), ColumnStats>,
}

impl QueryStats {
    /// Snapshot stats for the given `(table, columns)` requests in a
    /// single pass over `provider`. The only fallible / catalog-touching
    /// step in planning; everything downstream reads the owned result.
    /// Targets the provider has no row for are simply omitted — the
    /// accessors supply the fallback.
    pub fn gather(
        provider: &dyn StatsProvider,
        requests: &[(TableId, &[u32])],
    ) -> Result<QueryStats> {
        let mut tables = HashMap::new();
        let mut columns = HashMap::new();
        for &(table_id, cols) in requests {
            if let Some(ts) = provider.table_stats(table_id)? {
                tables.insert(table_id, ts);
            }
            for &col in cols {
                if let Some(cs) = provider.column_stats(table_id, col)? {
                    columns.insert((table_id, col), cs);
                }
            }
        }
        Ok(QueryStats { tables, columns })
    }

    /// Estimated row count for `table_id`; `DEFAULT_ROW_COUNT` if the
    /// table was never analyzed.
    pub fn row_count(&self, table_id: TableId) -> f64 {
        self.tables
            .get(&table_id)
            .map(|t| t.row_count as f64)
            .unwrap_or(DEFAULT_ROW_COUNT)
    }

    /// NDV for a column; `0` ("unknown") if never analyzed — the value
    /// `join_selectivity` treats as a fallback signal.
    pub fn ndv(&self, table_id: TableId, column_id: u32) -> i64 {
        self.columns
            .get(&(table_id, column_id))
            .map(|c| c.ndv)
            .unwrap_or(0)
    }

    /// Borrow full column stats (histogram included) for predicate
    /// selectivity, or `None` if the column was never analyzed.
    pub fn column_stats(&self, table_id: TableId, column_id: u32) -> Option<&ColumnStats> {
        self.columns.get(&(table_id, column_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_returns_set_table_rows() {
        let p = MockStatsProvider::new().with_table_rows(TableId(7), 30_000);
        assert_eq!(
            p.table_stats(TableId(7)).unwrap().unwrap().row_count,
            30_000
        );
    }

    #[test]
    fn mock_missing_table_is_none() {
        // Unknown target → Ok(None), the optimizer's "fall back to
        // defaults" signal, never an error.
        let p = MockStatsProvider::new();
        assert!(p.table_stats(TableId(99)).unwrap().is_none());
        assert!(p.column_stats(TableId(99), 0).unwrap().is_none());
    }

    #[test]
    fn mock_returns_set_column_ndv() {
        let p = MockStatsProvider::new().with_column_ndv(TableId(1), 2, 500);
        let s = p.column_stats(TableId(1), 2).unwrap().unwrap();
        assert_eq!(s.ndv, 500);
        assert_eq!(s.null_count, 0);
        assert_eq!(s.histogram_kind, HISTOGRAM_KIND_NONE);
    }

    #[test]
    fn mock_builder_chains_multiple_targets() {
        // Builder style composes; later sets don't clobber earlier ones
        // for distinct keys.
        let p = MockStatsProvider::new()
            .with_table_rows(TableId(1), 10)
            .with_table_rows(TableId(2), 20)
            .with_column_ndv(TableId(1), 0, 10)
            .with_column_ndv(TableId(2), 0, 5);
        assert_eq!(p.table_stats(TableId(1)).unwrap().unwrap().row_count, 10);
        assert_eq!(p.table_stats(TableId(2)).unwrap().unwrap().row_count, 20);
        assert_eq!(p.column_stats(TableId(1), 0).unwrap().unwrap().ndv, 10);
        assert_eq!(p.column_stats(TableId(2), 0).unwrap().unwrap().ndv, 5);
    }

    // ---- QueryStats snapshot (P14.10) ----

    #[test]
    fn query_stats_gather_snapshots_requested_targets() {
        let provider = MockStatsProvider::new()
            .with_table_rows(TableId(1), 30_000)
            .with_column_ndv(TableId(1), 0, 30_000)
            .with_column_ndv(TableId(1), 2, 10);
        let snap = QueryStats::gather(&provider, &[(TableId(1), &[0, 2])]).unwrap();
        assert_eq!(snap.row_count(TableId(1)), 30_000.0);
        assert_eq!(snap.ndv(TableId(1), 0), 30_000);
        assert_eq!(snap.ndv(TableId(1), 2), 10);
    }

    #[test]
    fn query_stats_unanalyzed_table_uses_default_row_count() {
        // Table present in the request but absent from the provider →
        // the default, not zero.
        let snap = QueryStats::gather(&MockStatsProvider::new(), &[(TableId(9), &[])]).unwrap();
        assert_eq!(snap.row_count(TableId(9)), DEFAULT_ROW_COUNT);
    }

    #[test]
    fn query_stats_unanalyzed_column_ndv_is_zero() {
        // Unknown NDV reads as 0, the "fall back" signal join_selectivity
        // keys off.
        let provider = MockStatsProvider::new().with_table_rows(TableId(1), 100);
        let snap = QueryStats::gather(&provider, &[(TableId(1), &[5])]).unwrap();
        assert_eq!(snap.ndv(TableId(1), 5), 0);
        assert!(snap.column_stats(TableId(1), 5).is_none());
    }

    #[test]
    fn query_stats_gather_is_single_pass_over_multiple_tables() {
        let provider = MockStatsProvider::new()
            .with_table_rows(TableId(1), 1)
            .with_table_rows(TableId(2), 6_000_000)
            .with_column_ndv(TableId(2), 0, 1_500_000);
        let snap = QueryStats::gather(&provider, &[(TableId(1), &[]), (TableId(2), &[0])]).unwrap();
        assert_eq!(snap.row_count(TableId(1)), 1.0);
        assert_eq!(snap.row_count(TableId(2)), 6_000_000.0);
        assert_eq!(snap.ndv(TableId(2), 0), 1_500_000);
    }
}
