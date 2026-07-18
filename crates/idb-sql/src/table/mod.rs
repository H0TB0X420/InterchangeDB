//! `Table<E, L>` — the access primitive that ties an engine + schema + layout
//! together for typed row access.
//!
//! Phase 9 final task. Composes:
//! - `E: StorageEngine` — where bytes live
//! - `Schema` — what the rows look like
//! - `L: DataLayout` — how rows map to storage entries
//!
//! Provides the row-level CRUD operations that Phase 11's executor will call:
//! `insert`, `upsert`, `update_by_pk`, `update_columns`, `get_by_pk`,
//! `delete_by_pk`, `scan`, `scan_range`. Constraint validation runs at the
//! `Table` boundary (one chokepoint, per H-decision in the planning doc).
//!
//! # Concurrency
//!
//! Phase 9 `Table` is **not** transactional — writes hit the engine directly,
//! and the get-then-put pair in `insert` / `update_*` has a TOCTOU window
//! (Issue 4 from the review). Phase 10's `TxnEngine<E>` wrapper closes the
//! window via `lock_for_write` before the get-then-put pair. Phase 9 callers
//! are us writing tests, so the race is academic for now.

use std::sync::Arc;

use crate::catalog::constraints::{
    apply_defaults, check_arity, check_nullability, check_pk_not_null, check_type_compat,
    check_type_compat_one, check_value_bounds, check_value_bounds_one,
};
use crate::catalog::Schema;
use crate::common::{Error, Result};
use crate::layout::{DataLayout, LayoutCtx};
use crate::storage::StorageEngine;
use crate::types::{keyenc, ColumnType, Value};

pub use crate::catalog::IndexHandle;

/// Typed row-level access to a single table backed by `engine` with layout `L`.
pub struct Table<E: StorageEngine, L: DataLayout> {
    engine: Arc<E>,
    schema: Arc<Schema>,
    layout: L,
    /// Secondary indexes maintained on every mutation. Empty for tables
    /// constructed without explicit index handles (legacy / test paths).
    indexes: Vec<IndexHandle>,
}

impl<E: StorageEngine, L: DataLayout> Table<E, L> {
    pub fn new(engine: Arc<E>, schema: Arc<Schema>, layout: L) -> Self {
        Self {
            engine,
            schema,
            layout,
            indexes: Vec::new(),
        }
    }

    /// Same as `new`, but with the table's secondary indexes attached.
    /// Each mutation will maintain entries in every listed index.
    pub fn with_indexes(
        engine: Arc<E>,
        schema: Arc<Schema>,
        layout: L,
        indexes: Vec<IndexHandle>,
    ) -> Self {
        Self {
            engine,
            schema,
            layout,
            indexes,
        }
    }

    /// Build a secondary index key from `row_values`. Format:
    /// `encode_key_components([indexed cols] ++ [pk cols])`. The PK
    /// suffix lets non-unique indexes hold multiple entries per
    /// secondary key value (distinguished by the pk-tiebreak suffix).
    fn build_index_key(&self, ix: &IndexHandle, row_values: &[Value]) -> Result<Vec<u8>> {
        let mut components: Vec<&Value> = ix.def.columns.iter().map(|&i| &row_values[i]).collect();
        for &pk_col in &self.schema.primary_key {
            components.push(&row_values[pk_col]);
        }
        keyenc::encode_key_components(&components, &ix.key_types)
    }

    /// Write secondary entries for `row_values` into every attached index.
    fn put_index_entries(&self, row_values: &[Value]) -> Result<()> {
        for ix in &self.indexes {
            let key = self.build_index_key(ix, row_values)?;
            // Value is empty: the PK is recoverable from the key's suffix
            // (see `build_index_key`). Phase 12's IndexScan operator
            // decodes the suffix back into PK components.
            ix.engine.put(&key, &[])?;
        }
        Ok(())
    }

    /// Enforce `UNIQUE` indexes for a row about to be written: reject if
    /// any unique index already holds an entry with the same indexed
    /// component values under a DIFFERENT primary key. Rows with NULL in
    /// any indexed column never conflict (SQL: NULLs are distinct). The
    /// self-key exclusion lets updates keep their own entry, and lets a
    /// same-PK re-insert surface as `DuplicateKey` from the PK path
    /// instead of being masked here.
    ///
    /// Consistency level: the index engines' current contents — the same
    /// as index maintenance itself (entries are unversioned; E1 recheck).
    fn check_unique_indexes(&self, new_row: &[Value]) -> Result<()> {
        for ix in &self.indexes {
            if !ix.def.unique {
                continue;
            }
            let components: Vec<&Value> = ix.def.columns.iter().map(|&i| &new_row[i]).collect();
            if components.iter().any(|v| matches!(v, Value::Null)) {
                continue;
            }
            let prefix =
                keyenc::encode_key_components(&components, &ix.key_types[..components.len()])?;
            let self_key = self.build_index_key(ix, new_row)?;
            // `prefix_increment`, not `byte_increment`: entries carry an
            // arbitrary PK suffix after the prefix, so the upper bound
            // must cover every extension (see keyenc's own warning).
            let upper = match keyenc::prefix_increment(&prefix) {
                Some(u) => std::ops::Bound::Excluded(u),
                None => std::ops::Bound::Unbounded,
            };
            // Bounded: at most the entries sharing this exact component
            // prefix — 0 or 1 when the invariant holds; the first foreign
            // key decides when it doesn't. The starts_with guard covers
            // the Unbounded arm (all-0xFF prefix), where the range would
            // otherwise run past the prefix.
            for entry in ix
                .engine
                .scan_range(std::ops::Bound::Included(prefix.clone()), upper)
            {
                let (key, _) = entry?;
                if !key.starts_with(&prefix) {
                    break;
                }
                if key != self_key {
                    return Err(Error::UniqueViolation {
                        index: ix.def.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Pre-flight for `CREATE UNIQUE INDEX`: verify no two existing rows
    /// share values on `columns` (NULL-containing rows exempt). Runs
    /// BEFORE the index is registered — there is no DROP INDEX yet, so a
    /// failing backfill must not strand a half-created index.
    pub fn assert_unique_feasible(&self, columns: &[usize], index_name: &str) -> Result<()> {
        use std::collections::HashSet;
        let types: Vec<crate::types::ColumnType> =
            columns.iter().map(|&i| self.schema.columns[i].ty).collect();
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        for row in self.scan()? {
            let components: Vec<&Value> = columns.iter().map(|&i| &row[i]).collect();
            if components.iter().any(|v| matches!(v, Value::Null)) {
                continue;
            }
            let key = keyenc::encode_key_components(&components, &types)?;
            if !seen.insert(key) {
                return Err(Error::UniqueViolation {
                    index: index_name.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Write index entries for every EXISTING row into the attached
    /// indexes. Used once at `CREATE INDEX` time — the session attaches
    /// only the newly created index, so rows inserted before it existed
    /// become reachable through it; later mutations maintain it via the
    /// normal paths. Returns the number of rows backfilled.
    pub fn backfill_indexes(&self) -> Result<u64> {
        let mut rows: u64 = 0;
        for values in self.scan()? {
            self.put_index_entries(&values)?;
            rows += 1;
        }
        Ok(rows)
    }

    /// Delete secondary entries for `row_values` from every attached index.
    fn delete_index_entries(&self, row_values: &[Value]) -> Result<()> {
        for ix in &self.indexes {
            let key = self.build_index_key(ix, row_values)?;
            ix.engine.delete(&key)?;
        }
        Ok(())
    }

    /// Strict insert. Errors with `DuplicateKey` if the PK already exists.
    pub fn insert(&self, values: &[Value]) -> Result<()> {
        let values = self.validate_and_default(values)?;
        let pk_encoded = self.encode_pk(&values)?;
        // No-op for raw engines; Phase 10's TxnEngine acquires X-lock here.
        self.engine.lock_for_write(&pk_encoded)?;
        let column_types = self.column_types_vec();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        if self
            .layout
            .get_row(&*self.engine, ctx, &pk_encoded)?
            .is_some()
        {
            return Err(Error::DuplicateKey {
                table: self.schema.name.clone(),
            });
        }
        self.check_unique_indexes(&values)?;
        self.layout
            .put_row(&*self.engine, ctx, &pk_encoded, &values)?;
        // P12.4: maintain secondary indexes. Done after the PK write so a
        // failed PK write doesn't leak index entries. Index writes
        // themselves can in principle fail mid-loop — that leaves a
        // partial state requiring transactional rollback to clean up.
        // For Phase 12 raw use this is documented; `TxnEngine`-wrapped
        // use gets atomicity via the surrounding txn.
        self.put_index_entries(&values)?;
        Ok(())
    }

    /// Upsert. Last writer wins on PK collision.
    pub fn upsert(&self, values: &[Value]) -> Result<()> {
        let values = self.validate_and_default(values)?;
        let pk_encoded = self.encode_pk(&values)?;
        self.engine.lock_for_write(&pk_encoded)?;
        let column_types = self.column_types_vec();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        // If there's a prior row at this PK, remove its old index entries
        // first (they may have different indexed-column values).
        self.check_unique_indexes(&values)?;
        if !self.indexes.is_empty() {
            if let Some(prev) = self.layout.get_row(&*self.engine, ctx, &pk_encoded)? {
                self.delete_index_entries(&prev)?;
            }
        }
        self.layout
            .put_row(&*self.engine, ctx, &pk_encoded, &values)?;
        self.put_index_entries(&values)?;
        Ok(())
    }

    /// Replace the entire row at the given PK. Errors with `RowNotFound`
    /// if the PK is absent.
    pub fn update_by_pk(&self, pk_values: &[Value], new_values: &[Value]) -> Result<()> {
        let new_values = self.validate_and_default(new_values)?;
        let pk_encoded = self.encode_pk_components(pk_values)?;
        self.engine.lock_for_write(&pk_encoded)?;
        let column_types = self.column_types_vec();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        // Capture old row for index maintenance before mutating.
        let prev = if !self.indexes.is_empty() {
            self.layout.get_row(&*self.engine, ctx, &pk_encoded)?
        } else {
            None
        };
        self.check_unique_indexes(&new_values)?;
        self.layout
            .update_row(&*self.engine, ctx, &pk_encoded, &new_values)?;
        if let Some(prev) = prev {
            self.delete_index_entries(&prev)?;
        }
        self.put_index_entries(&new_values)?;
        Ok(())
    }

    /// Update specific columns at the given PK. Per-column type/bounds
    /// validation runs first; nullability is *not* re-checked (the existing
    /// row already passed it; partial update doesn't reset that invariant).
    pub fn update_columns(&self, pk_values: &[Value], changes: &[(usize, Value)]) -> Result<()> {
        for (col_idx, new_val) in changes {
            check_type_compat_one(&self.schema, *col_idx, new_val)?;
            check_value_bounds_one(&self.schema, *col_idx, new_val)?;
        }
        let pk_encoded = self.encode_pk_components(pk_values)?;
        self.engine.lock_for_write(&pk_encoded)?;
        let column_types = self.column_types_vec();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        // For index maintenance: pre-fetch the row, apply the column
        // changes locally to materialize what the new row will look like,
        // and use both to rewrite index entries.
        let prev = if !self.indexes.is_empty() {
            self.layout.get_row(&*self.engine, ctx, &pk_encoded)?
        } else {
            None
        };
        // Materialize the post-update row BEFORE mutating so unique
        // enforcement can veto the write while nothing has changed.
        let new_row = prev.as_ref().map(|prev| {
            let mut row = prev.clone();
            for (col_idx, new_val) in changes {
                row[*col_idx] = new_val.clone();
            }
            row
        });
        if let Some(new_row) = &new_row {
            self.check_unique_indexes(new_row)?;
        }
        self.layout
            .update_columns(&*self.engine, ctx, &pk_encoded, changes)?;
        if let (Some(prev), Some(new_row)) = (prev, new_row) {
            self.delete_index_entries(&prev)?;
            self.put_index_entries(&new_row)?;
        }
        Ok(())
    }

    /// Read the row at the given PK. Returns `None` if absent.
    pub fn get_by_pk(&self, pk_values: &[Value]) -> Result<Option<Vec<Value>>> {
        let pk_encoded = self.encode_pk_components(pk_values)?;
        let column_types = self.column_types_vec();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        self.layout.get_row(&*self.engine, ctx, &pk_encoded)
    }

    /// Idempotent delete: `Ok(())` whether the row existed or not.
    pub fn delete_by_pk(&self, pk_values: &[Value]) -> Result<()> {
        let pk_encoded = self.encode_pk_components(pk_values)?;
        self.engine.lock_for_write(&pk_encoded)?;
        let column_types = self.column_types_vec();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        // Index maintenance: capture row before the delete so we have the
        // indexed-column values to look up. After delete succeeds, remove
        // the corresponding index entries.
        let prev = if !self.indexes.is_empty() {
            self.layout.get_row(&*self.engine, ctx, &pk_encoded)?
        } else {
            None
        };
        self.layout.delete_row(&*self.engine, ctx, &pk_encoded)?;
        if let Some(prev) = prev {
            self.delete_index_entries(&prev)?;
        }
        Ok(())
    }

    /// Full-table scan in PK order. Returns rows as `Vec<Value>` (PK bytes
    /// dropped — caller has the PK back via the row values themselves).
    pub fn scan(&self) -> Result<Vec<Vec<Value>>> {
        let column_types = self.column_types_vec();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        self.layout
            .scan_table(&*self.engine, ctx)
            .map(|r| r.map(|(_pk, vs)| vs))
            .collect()
    }

    /// Range scan over PKs in `[pk_start, pk_end)`. Half-open.
    pub fn scan_range(&self, pk_start: &[Value], pk_end: &[Value]) -> Result<Vec<Vec<Value>>> {
        let start_encoded = self.encode_pk_components(pk_start)?;
        let end_encoded = self.encode_pk_components(pk_end)?;
        let column_types = self.column_types_vec();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        self.layout
            .scan_range(&*self.engine, ctx, &start_encoded, &end_encoded)
            .map(|r| r.map(|(_pk, vs)| vs))
            .collect()
    }

    /// Schema accessor.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    // ---- private helpers --------------------------------------------------

    /// Run the constraint pipeline that's shared between insert / upsert /
    /// update_by_pk: defaults, then nullability + types + bounds + PK-not-null.
    fn validate_and_default(&self, values: &[Value]) -> Result<Vec<Value>> {
        check_arity(&self.schema, values)?;
        let values = apply_defaults(&self.schema, values);
        check_nullability(&self.schema, &values)?;
        check_type_compat(&self.schema, &values)?;
        check_value_bounds(&self.schema, &values)?;
        check_pk_not_null(&self.schema, &values)?;
        Ok(values)
    }

    /// Extract PK columns from a full-row value vector and encode them.
    fn encode_pk(&self, values: &[Value]) -> Result<Vec<u8>> {
        let pk_refs: Vec<&Value> = self
            .schema
            .primary_key
            .iter()
            .map(|&i| &values[i])
            .collect();
        let pk_types = self.schema.pk_types();
        keyenc::encode_key_components(&pk_refs, &pk_types)
    }

    /// Encode a PK directly from caller-provided PK values (length must
    /// match `primary_key.len()`).
    fn encode_pk_components(&self, pk_values: &[Value]) -> Result<Vec<u8>> {
        if pk_values.len() != self.schema.primary_key.len() {
            return Err(Error::StorageCorrupted(format!(
                "Table::pk: expected {} PK components, got {}",
                self.schema.primary_key.len(),
                pk_values.len()
            )));
        }
        let pk_refs: Vec<&Value> = pk_values.iter().collect();
        let pk_types = self.schema.pk_types();
        keyenc::encode_key_components(&pk_refs, &pk_types)
    }

    fn column_types_vec(&self) -> Vec<ColumnType> {
        self.schema.column_types()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{ColumnDef, TableId};
    use crate::common::ConstraintRule;
    use crate::engines::btree::BTreeEngine;
    use crate::layout::RowLayout;
    use crate::storage::FileDiskManager;
    use crate::types::Decimal;
    use tempfile::TempDir;

    fn fresh_engine() -> (Arc<BTreeEngine>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        (Arc::new(BTreeEngine::new(bpm).unwrap()), dir)
    }

    /// Schema: (id Int32 PK, name Varchar(20) nullable, balance Decimal(10,2) NOT NULL default 0).
    fn account_schema() -> Schema {
        Schema {
            name: "account".into(),
            table_id: TableId(1),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "name".into(),
                    ty: ColumnType::Varchar(20),
                    nullable: true,
                    default: None,
                },
                ColumnDef {
                    name: "balance".into(),
                    ty: ColumnType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    nullable: false,
                    default: Some(Value::Decimal(Decimal::from_i64_with_scale(0, 2))),
                },
            ],
            primary_key: vec![0],
        }
    }

    fn account_table() -> (Table<BTreeEngine, RowLayout>, TempDir) {
        let (engine, dir) = fresh_engine();
        let schema = Arc::new(account_schema());
        (Table::new(engine, schema, RowLayout), dir)
    }

    fn row(id: i32, name: Option<&str>, balance_cents: i64) -> Vec<Value> {
        vec![
            Value::Int32(id),
            name.map(|s| Value::Varchar(s.into()))
                .unwrap_or(Value::Null),
            Value::Decimal(Decimal::from_i64_with_scale(balance_cents, 2)),
        ]
    }

    #[test]
    fn insert_then_get_roundtrip() {
        let (table, _dir) = account_table();
        let r = row(1, Some("alice"), 12345);
        table.insert(&r).unwrap();

        let back = table.get_by_pk(&[Value::Int32(1)]).unwrap();
        assert_eq!(back, Some(r));
    }

    #[test]
    fn insert_applies_defaults() {
        let (table, _dir) = account_table();
        // balance is NULL but has a default of 0.00.
        table
            .insert(&[Value::Int32(1), Value::Varchar("bob".into()), Value::Null])
            .unwrap();
        let back = table.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap();
        // Default substituted: Decimal mantissa=0 scale=2.
        assert_eq!(back[2], Value::Decimal(Decimal::from_i64_with_scale(0, 2)));
    }

    #[test]
    fn insert_rejects_arity_mismatch() {
        let (table, _dir) = account_table();
        let err = table
            .insert(&[Value::Int32(1)])
            .expect_err("arity mismatch should error");
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::Arity { .. },
                ..
            }
        ));
    }

    #[test]
    fn insert_rejects_null_in_non_nullable_no_default() {
        let (table, _dir) = account_table();
        // Column 0 (id) is non-nullable, no default.
        let err = table
            .insert(&[Value::Null, Value::Null, Value::Null])
            .expect_err("NULL in non-nullable PK should error");
        assert!(matches!(err, Error::ConstraintViolation { .. }));
    }

    #[test]
    fn insert_rejects_oversize_varchar() {
        let (table, _dir) = account_table();
        let long = "x".repeat(50); // > 20
        let err = table
            .insert(&row(1, Some(&long), 0))
            .expect_err("oversize Varchar should error");
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::VarcharTooLong { .. },
                ..
            }
        ));
    }

    #[test]
    fn insert_strict_duplicate_pk_errors() {
        let (table, _dir) = account_table();
        table.insert(&row(1, Some("alice"), 100)).unwrap();
        let err = table
            .insert(&row(1, Some("bob"), 200))
            .expect_err("duplicate PK should error");
        assert!(matches!(err, Error::DuplicateKey { .. }));
    }

    #[test]
    fn upsert_overwrites_on_duplicate_pk() {
        let (table, _dir) = account_table();
        table.insert(&row(1, Some("alice"), 100)).unwrap();
        table.upsert(&row(1, Some("bob"), 200)).unwrap();

        let back = table.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap();
        assert_eq!(back[1], Value::Varchar("bob".into()));
    }

    #[test]
    fn update_by_pk_replaces_existing() {
        let (table, _dir) = account_table();
        table.insert(&row(1, Some("alice"), 100)).unwrap();
        table
            .update_by_pk(&[Value::Int32(1)], &row(1, Some("alice2"), 999))
            .unwrap();

        let back = table.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap();
        assert_eq!(back[1], Value::Varchar("alice2".into()));
    }

    #[test]
    fn update_by_pk_absent_errors() {
        let (table, _dir) = account_table();
        let err = table
            .update_by_pk(&[Value::Int32(99)], &row(99, None, 0))
            .expect_err("update on missing PK should error");
        assert!(matches!(err, Error::RowNotFound { .. }));
    }

    #[test]
    fn update_columns_changes_only_listed() {
        let (table, _dir) = account_table();
        table.insert(&row(1, Some("alice"), 100)).unwrap();
        table
            .update_columns(
                &[Value::Int32(1)],
                &[(2, Value::Decimal(Decimal::from_i64_with_scale(500, 2)))],
            )
            .unwrap();

        let back = table.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap();
        assert_eq!(back[1], Value::Varchar("alice".into())); // unchanged
        assert_eq!(
            back[2],
            Value::Decimal(Decimal::from_i64_with_scale(500, 2))
        );
    }

    #[test]
    fn update_columns_rejects_wrong_type() {
        let (table, _dir) = account_table();
        table.insert(&row(1, Some("alice"), 100)).unwrap();
        let err = table
            .update_columns(&[Value::Int32(1)], &[(2, Value::Boolean(true))])
            .expect_err("wrong type should error");
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::TypeMismatch { .. },
                ..
            }
        ));
    }

    #[test]
    fn delete_by_pk_idempotent() {
        let (table, _dir) = account_table();
        table.insert(&row(1, Some("alice"), 100)).unwrap();
        table.delete_by_pk(&[Value::Int32(1)]).unwrap();
        // Second delete: still Ok.
        table.delete_by_pk(&[Value::Int32(1)]).unwrap();
        assert!(table.get_by_pk(&[Value::Int32(1)]).unwrap().is_none());
    }

    #[test]
    fn scan_returns_rows_in_pk_order() {
        let (table, _dir) = account_table();
        for id in [3, 1, 4, 1, 5, 9, 2, 6] {
            // upsert dedupes on collision; we'll see distinct ids.
            table.upsert(&row(id, Some("x"), 0)).unwrap();
        }
        let rows = table.scan().unwrap();
        let ids: Vec<i32> = rows
            .iter()
            .map(|r| match r[0] {
                Value::Int32(n) => n,
                _ => panic!(),
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 9]);
    }

    #[test]
    fn scan_range_subset() {
        let (table, _dir) = account_table();
        for id in 1..=5 {
            table.insert(&row(id, Some("x"), 0)).unwrap();
        }
        let rows = table
            .scan_range(&[Value::Int32(2)], &[Value::Int32(4)])
            .unwrap();
        let ids: Vec<i32> = rows
            .iter()
            .map(|r| match r[0] {
                Value::Int32(n) => n,
                _ => panic!(),
            })
            .collect();
        assert_eq!(ids, vec![2, 3]); // half-open: 4 excluded
    }

    #[test]
    fn pk_arity_mismatch_errors() {
        let (table, _dir) = account_table();
        let err = table
            .get_by_pk(&[Value::Int32(1), Value::Int32(2)])
            .expect_err("wrong PK arity should error");
        assert!(matches!(err, Error::StorageCorrupted(_)));
    }
}
