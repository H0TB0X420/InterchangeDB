//! Physical data layout for storing rows in a `StorageEngine`.
//!
//! The `DataLayout` trait defines the contract between `Table<E, L>` and the
//! underlying engine. A layout decides:
//!   - How a logical row maps to one or more storage key/value pairs.
//!   - How to read those pairs back into a `Vec<Value>`.
//!
//! Phase 9 ships `RowLayout` (one storage entry per row, all columns in one
//! value blob). Phase 19 will add `ColumnLayout` (one storage entry per
//! column per row), validating that this trait shape generalizes.
//!
//! The trait is compile-time generic on `E: StorageEngine` so we get zero
//! dispatch overhead — every `Table<BTreeEngine, RowLayout>::insert` is a
//! direct call chain through monomorphized code.

use crate::catalog::TableId;
use crate::common::Result;
use crate::storage::StorageEngine;
use crate::types::{ColumnType, Value};

pub mod row;

pub use row::RowLayout;

/// Static context shared by every layout method call: the column types and
/// table id. Bundled to keep method signatures readable. `Copy` because both
/// fields are cheap.
#[derive(Debug, Clone, Copy)]
pub struct LayoutCtx<'a> {
    /// Column types in declaration order. Used to encode/decode tuples and
    /// to skip past columns during partial decode.
    pub column_types: &'a [ColumnType],
    /// Identifies the table whose rows are being read or written. Layouts
    /// embed this in storage keys to avoid cross-table collisions.
    pub table_id: TableId,
}

/// Maps logical rows to physical storage entries.
///
/// Implementors decide the storage key shape (e.g., `[table_id|pk]` for row
/// layout, `[table_id|col_id|pk]` for column layout) and the value shape
/// (one tuple blob vs. one column value per entry). The trait is the only
/// place in the codebase that knows about this distinction; everything
/// above (`Table`, executor, planner, SQL surface) speaks in `Value`.
pub trait DataLayout: Send + Sync + 'static {
    /// Diagnostic name (e.g., `"row"`, `"column"`). Used in EXPLAIN output
    /// and debug logs.
    fn name() -> &'static str;

    /// Insert or overwrite a single row at `pk_encoded`.
    ///
    /// `values` must satisfy `Value::matches` against `ctx.column_types` —
    /// the constraint layer (Task 9.6) ensures this before calling. Layout
    /// trusts the input shape.
    fn put_row<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
        values: &[Value],
    ) -> Result<()>;

    /// Read all columns of the row at `pk_encoded`. Returns `None` if the
    /// row is absent.
    fn get_row<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
    ) -> Result<Option<Vec<Value>>>;

    /// Read only the columns named in `cols` (positions in `ctx.column_types`).
    /// Returns `None` if the row is absent. The returned `Vec<Value>` is in
    /// the same order as `cols`.
    ///
    /// `RowLayout` reads the whole row blob and decodes only the requested
    /// columns (saves decode work). `ColumnLayout` issues only the requested
    /// per-column gets (saves I/O — the columnar win).
    fn get_projection<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
        cols: &[usize],
    ) -> Result<Option<Vec<Value>>>;

    /// Delete the row at `pk_encoded`. Idempotent: `Ok(())` if PK is absent
    /// (matches SQL `DELETE`'s zero-rows-affected semantic).
    fn delete_row<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
    ) -> Result<()>;

    /// Replace the entire row at `pk_encoded`. Errors with `RowNotFound`
    /// if the PK does not exist.
    fn update_row<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
        new_values: &[Value],
    ) -> Result<()>;

    /// Update specific columns of the row at `pk_encoded`. Errors with
    /// `RowNotFound` if the PK does not exist.
    ///
    /// `changes` is `(column_index, new_value)` pairs. Indexes are positions
    /// in `ctx.column_types`. Per-column type compatibility is the caller's
    /// responsibility (validated by `Table::update_columns` before this call).
    ///
    /// `RowLayout` reads existing row → merges → writes back (one read + one
    /// write). `ColumnLayout` writes only the changed columns directly
    /// (`changes.len()` writes, no read needed).
    fn update_columns<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
        changes: &[(usize, Value)],
    ) -> Result<()>;

    /// Scan every row of `ctx.table_id` in PK order. Each item is the row's
    /// encoded PK plus its decoded values.
    ///
    /// Layouts are responsible for constructing the table-prefix range
    /// internally — `RowLayout` scans `[tid|0]..[tid+1|0]`, `ColumnLayout`
    /// scans the analogous prefix and groups N consecutive entries back
    /// into one logical row.
    fn scan_table<'a, E: StorageEngine>(
        &self,
        engine: &'a E,
        ctx: LayoutCtx<'a>,
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<Value>)>> + 'a>;

    /// Scan rows whose PK falls in `[pk_start, pk_end)`. Half-open: `pk_end`
    /// is exclusive. Empty range yields zero rows.
    ///
    /// `RowLayout` translates to `engine.scan(make_row_key(tid, pk_start)..
    /// make_row_key(tid, pk_end))`. `ColumnLayout` does the same prefix-scan
    /// + per-row grouping it does for `scan_table`.
    fn scan_range<'a, E: StorageEngine>(
        &self,
        engine: &'a E,
        ctx: LayoutCtx<'a>,
        pk_start: &[u8],
        pk_end: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<Value>)>> + 'a>;
}
