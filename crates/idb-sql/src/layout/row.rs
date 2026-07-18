//! Row-oriented data layout.                                                                         
//!                                                       
//! Stores each logical row as a single engine entry:
//!   key   = [table_id_be:4 | pk_encoded]                                                            
//!   value = tuple::encode(column_types, values)                                                     
//!                                                                                                   
//! Best for OLTP workloads — point lookups read one engine entry, full-row                           
//! reads decode one tuple blob, full-table scans iterate one prefix range.                           
//! Partial-column reads (`get_projection`) read the whole blob and decode                            
//! only the requested columns; the I/O isn't optimized but the decode is.                            
//!                                                                                                   
//! Phase 19's `ColumnLayout` will store one engine entry per column per row;                         
//! that's where the columnar I/O win lives. Both impls satisfy the same                              
//! `DataLayout` trait, so `Table` doesn't change.                                                    

use crate::catalog::TableId;
use crate::common::{Error, Result};
use crate::layout::{DataLayout, LayoutCtx, RowScan};
use crate::storage::StorageEngine;
use crate::types::{tuple, Value};

/// Row-oriented layout: one engine entry per row.                                                    
pub struct RowLayout;

/// Build the storage key for `(table_id, pk_encoded)`.                                               
///
/// Layout: `table_id` as 4 big-endian bytes, then `pk_encoded` verbatim.                             
/// Big-endian so byte-lex order on the encoded prefix matches numeric order                          
/// on `table_id` — required for `scan_table`'s `[tid]..[tid+1]` range to                             
/// cover exactly that table's rows and no others.                                                    
fn make_row_key(table_id: TableId, pk_encoded: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + pk_encoded.len());
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.extend_from_slice(pk_encoded);
    key
}

impl DataLayout for RowLayout {
    fn name() -> &'static str {
        "row"
    }

    fn put_row<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
        values: &[Value],
    ) -> Result<()> {
        let key = make_row_key(ctx.table_id, pk_encoded);
        let blob = tuple::encode(ctx.column_types, values)?;
        engine.put(&key, &blob)
    }

    fn get_row<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
    ) -> Result<Option<Vec<Value>>> {
        let key = make_row_key(ctx.table_id, pk_encoded);
        match engine.get(&key)? {
            Some(blob) => Ok(Some(tuple::decode(ctx.column_types, &blob)?)),
            None => Ok(None),
        }
    }

    fn get_projection<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
        cols: &[usize],
    ) -> Result<Option<Vec<Value>>> {
        let key = make_row_key(ctx.table_id, pk_encoded);
        match engine.get(&key)? {
            Some(blob) => {
                let mut out = Vec::with_capacity(cols.len());
                for &col_idx in cols {
                    out.push(tuple::decode_column(ctx.column_types, &blob, col_idx)?);
                }
                Ok(Some(out))
            }
            None => Ok(None),
        }
    }

    fn delete_row<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
    ) -> Result<()> {
        let key = make_row_key(ctx.table_id, pk_encoded);
        match engine.delete(&key) {
            Ok(()) => Ok(()),
            Err(Error::KeyNotFound(_)) => Ok(()), // idempotent: absent is fine
            Err(e) => Err(e),
        }
    }

    fn update_row<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
        new_values: &[Value],
    ) -> Result<()> {
        let key = make_row_key(ctx.table_id, pk_encoded);
        // Verify presence — update is strict, unlike upsert.
        if engine.get(&key)?.is_none() {
            return Err(Error::RowNotFound {
                table: format!("id={}", ctx.table_id.0),
            });
        }
        let blob = tuple::encode(ctx.column_types, new_values)?;
        engine.put(&key, &blob)
    }

    fn update_columns<E: StorageEngine>(
        &self,
        engine: &E,
        ctx: LayoutCtx,
        pk_encoded: &[u8],
        changes: &[(usize, Value)],
    ) -> Result<()> {
        let key = make_row_key(ctx.table_id, pk_encoded);
        // Read existing row (and verify it exists).
        let mut merged = match engine.get(&key)? {
            Some(blob) => tuple::decode(ctx.column_types, &blob)?,
            None => {
                return Err(Error::RowNotFound {
                    table: format!("id={}", ctx.table_id.0),
                });
            }
        };
        // Merge each change into the existing row.
        for (col_idx, new_val) in changes {
            if *col_idx >= merged.len() {
                return Err(Error::StorageCorrupted(format!(
                    "update_columns: column index {} out of range for {} columns",
                    col_idx,
                    merged.len()
                )));
            }
            merged[*col_idx] = new_val.clone();
        }
        // Re-encode and write back.
        let blob = tuple::encode(ctx.column_types, &merged)?;
        engine.put(&key, &blob)
    }

    fn scan_table<'a, E: StorageEngine>(&self, engine: &'a E, ctx: LayoutCtx<'a>) -> RowScan<'a> {
        // Build the half-open prefix range [tid | …] .. [tid+1 | …].
        // tid+1 can overflow u32; surface that as an immediate error item
        // rather than panicking inside the iterator.
        let next_tid = match ctx.table_id.next() {
            Ok(t) => t,
            Err(e) => return Box::new(std::iter::once(Err(e))),
        };
        let start = make_row_key(ctx.table_id, &[]);
        let end = make_row_key(next_tid, &[]);
        let column_types = ctx.column_types;
        let iter = engine.scan(start..end).map(move |result| {
            let (storage_key, blob) = result?;
            // Strip the 4-byte table_id prefix to recover the encoded PK.
            let pk_encoded = storage_key[4..].to_vec();
            let values = tuple::decode(column_types, &blob)?;
            Ok((pk_encoded, values))
        });
        Box::new(iter)
    }

    fn scan_range<'a, E: StorageEngine>(
        &self,
        engine: &'a E,
        ctx: LayoutCtx<'a>,
        pk_start: &[u8],
        pk_end: &[u8],
    ) -> RowScan<'a> {
        // Half-open: [make_row_key(tid, pk_start), make_row_key(tid, pk_end)).
        // The same table_id prefix is used on both bounds so we never cross
        // into adjacent tables' keyspaces.
        let start = make_row_key(ctx.table_id, pk_start);
        let end = make_row_key(ctx.table_id, pk_end);
        let column_types = ctx.column_types;
        let iter = engine.scan(start..end).map(move |result| {
            let (storage_key, blob) = result?;
            let pk_encoded = storage_key[4..].to_vec();
            let values = tuple::decode(column_types, &blob)?;
            Ok((pk_encoded, values))
        });
        Box::new(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::engines::btree::BTreeEngine;
    use crate::storage::FileDiskManager;
    use crate::types::{keyenc, ColumnType};
    use tempfile::TempDir;

    /// Stand up a fresh BTreeEngine on a temp file. Returns the dir so the
    /// test scope keeps it alive for the duration of the test.
    fn fresh_engine() -> (BTreeEngine, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        (engine, dir)
    }

    /// Encode an i32 PK using the keyenc primitive.
    fn pk_int32(n: i32) -> Vec<u8> {
        keyenc::encode_key_components(&[&Value::Int32(n)], &[ColumnType::Int32]).unwrap()
    }

    /// A 3-column schema we reuse: (id Int32, name Varchar(64), active Boolean).
    fn person_schema() -> Vec<ColumnType> {
        vec![
            ColumnType::Int32,
            ColumnType::Varchar(64),
            ColumnType::Boolean,
        ]
    }

    fn person_row(id: i32, name: &str, active: bool) -> Vec<Value> {
        vec![
            Value::Int32(id),
            Value::Varchar(name.into()),
            Value::Boolean(active),
        ]
    }

    fn make_ctx<'a>(types: &'a [ColumnType], table_id: u32) -> LayoutCtx<'a> {
        LayoutCtx {
            column_types: types,
            table_id: TableId(table_id),
        }
    }

    /// Static method works as expected.
    #[test]
    fn name_returns_row() {
        assert_eq!(<RowLayout as DataLayout>::name(), "row");
    }

    /// The headline integration test: put a row, get it back unchanged.
    #[test]
    fn put_then_get_roundtrip() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let row = person_row(42, "alice", true);
        let pk = pk_int32(42);

        RowLayout.put_row(&engine, ctx, &pk, &row).unwrap();
        let back = RowLayout.get_row(&engine, ctx, &pk).unwrap();
        assert_eq!(back, Some(row));
    }

    /// `get_row` on a PK that was never inserted must return `Ok(None)`,
    /// not an error. SQL `SELECT … WHERE id = ?` returning zero rows is
    /// not an error condition.
    #[test]
    fn get_absent_returns_none() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        let back = RowLayout.get_row(&engine, ctx, &pk).unwrap();
        assert_eq!(back, None);
    }

    /// Projection returns columns in the order the caller requested, not
    /// schema order. A SELECT-clause "name, id" produces a different output
    /// shape than "id, name"; layout must preserve caller's column order.
    #[test]
    fn get_projection_returns_subset_in_requested_order() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let row = person_row(42, "alice", true);
        let pk = pk_int32(42);

        RowLayout.put_row(&engine, ctx, &pk, &row).unwrap();

        // Schema order subset: columns 1 and 2.
        let back = RowLayout
            .get_projection(&engine, ctx, &pk, &[1, 2])
            .unwrap();
        assert_eq!(
            back,
            Some(vec![Value::Varchar("alice".into()), Value::Boolean(true)])
        );

        // Reversed: column 2 then column 0. Output order matches request.
        let back = RowLayout
            .get_projection(&engine, ctx, &pk, &[2, 0])
            .unwrap();
        assert_eq!(back, Some(vec![Value::Boolean(true), Value::Int32(42)]));
    }

    /// Projection on a missing PK returns `None`, same `Option` semantics
    /// as `get_row`.
    #[test]
    fn get_projection_absent_returns_none() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        let back = RowLayout
            .get_projection(&engine, ctx, &pk, &[0, 1])
            .unwrap();
        assert_eq!(back, None);
    }

    /// Round trip including destruction: put → delete → get returns None.
    #[test]
    fn delete_then_get_returns_none() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let row = person_row(42, "alice", true);
        let pk = pk_int32(42);

        RowLayout.put_row(&engine, ctx, &pk, &row).unwrap();
        RowLayout.delete_row(&engine, ctx, &pk).unwrap();

        let back = RowLayout.get_row(&engine, ctx, &pk).unwrap();
        assert_eq!(back, None);
    }

    /// Idempotent delete: removing a non-existent row returns `Ok(())`,
    /// not an error. Layout swallows the engine's strict `KeyNotFound`
    /// to give the layout-level idempotent contract.
    #[test]
    fn delete_absent_is_idempotent() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        RowLayout.delete_row(&engine, ctx, &pk).unwrap();
    }

    /// Two different tables can have the same encoded PK without collision.
    /// This validates that the table_id prefix in `make_row_key` actually
    /// separates table keyspaces — the foundation for multi-table storage.
    #[test]
    fn multi_table_isolation() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx_t1 = make_ctx(&types, 1);
        let ctx_t2 = make_ctx(&types, 2);
        let pk = pk_int32(42); // Same PK for both tables.

        let row_t1 = person_row(42, "table-one", true);
        let row_t2 = person_row(42, "table-two", false);

        RowLayout.put_row(&engine, ctx_t1, &pk, &row_t1).unwrap();
        RowLayout.put_row(&engine, ctx_t2, &pk, &row_t2).unwrap();

        assert_eq!(
            RowLayout.get_row(&engine, ctx_t1, &pk).unwrap(),
            Some(row_t1)
        );
        assert_eq!(
            RowLayout.get_row(&engine, ctx_t2, &pk).unwrap(),
            Some(row_t2)
        );
    }

    /// `put_row` is upsert-at-the-storage-level: a second `put_row` on the
    /// same PK silently overwrites. Strict-insert (error on dup) is
    /// `Table::insert`'s responsibility, layered above.
    #[test]
    fn put_overwrites_existing() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        let original = person_row(42, "alice", true);
        let updated = person_row(42, "bob", false);

        RowLayout.put_row(&engine, ctx, &pk, &original).unwrap();
        RowLayout.put_row(&engine, ctx, &pk, &updated).unwrap();

        let back = RowLayout.get_row(&engine, ctx, &pk).unwrap();
        assert_eq!(back, Some(updated));
    }

    // ---- Step D: update methods ------------------------------------------

    /// `update_row` replaces the entire row at an existing PK.
    #[test]
    fn update_row_replaces_existing() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        RowLayout
            .put_row(&engine, ctx, &pk, &person_row(42, "alice", true))
            .unwrap();
        let new_row = person_row(42, "bob", false);
        RowLayout.update_row(&engine, ctx, &pk, &new_row).unwrap();

        let back = RowLayout.get_row(&engine, ctx, &pk).unwrap();
        assert_eq!(back, Some(new_row));
    }

    /// `update_row` on a PK that was never inserted returns `RowNotFound`,
    /// not a silent insert. This is what makes update strict (vs upsert).
    #[test]
    fn update_row_absent_returns_row_not_found() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        let err = RowLayout
            .update_row(&engine, ctx, &pk, &person_row(42, "ghost", true))
            .expect_err("update on missing PK should error");
        assert!(matches!(err, Error::RowNotFound { .. }));
    }

    /// `update_columns` mutates only the listed columns; others are preserved
    /// byte-equal. The read-merge-write path on RowLayout.
    #[test]
    fn update_columns_merges_subset() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        RowLayout
            .put_row(&engine, ctx, &pk, &person_row(42, "alice", true))
            .unwrap();
        // Change only column 1 (name).
        RowLayout
            .update_columns(&engine, ctx, &pk, &[(1, Value::Varchar("bob".into()))])
            .unwrap();

        let back = RowLayout.get_row(&engine, ctx, &pk).unwrap();
        // Col 0 (id) and col 2 (active) unchanged; col 1 (name) is "bob".
        assert_eq!(back, Some(person_row(42, "bob", true)));
    }

    /// `update_columns` on absent PK returns `RowNotFound`, same as
    /// `update_row`'s strict semantic.
    #[test]
    fn update_columns_absent_returns_row_not_found() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        let err = RowLayout
            .update_columns(&engine, ctx, &pk, &[(0, Value::Int32(0))])
            .expect_err("update on missing PK should error");
        assert!(matches!(err, Error::RowNotFound { .. }));
    }

    /// `update_columns` with `col_idx >= row.len()` is a programmer bug;
    /// surface as `StorageCorrupted` rather than panicking.
    #[test]
    fn update_columns_invalid_index_errors() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);
        let pk = pk_int32(42);

        RowLayout
            .put_row(&engine, ctx, &pk, &person_row(42, "alice", true))
            .unwrap();
        let err = RowLayout
            .update_columns(&engine, ctx, &pk, &[(99, Value::Int32(0))])
            .expect_err("out-of-range col_idx should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("out of range")));
    }

    // ---- Step E: scan methods --------------------------------------------

    /// Insert several rows in non-monotonic order; `scan_table` must yield
    /// them in PK order (numeric, since keyenc preserves order via sign-XOR).
    #[test]
    fn scan_table_yields_all_rows_in_pk_order() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);

        // Insert PKs in scrambled order to ensure the scan really sorts.
        for id in [3, 1, 4, 1, 5, 9, 2, 6] {
            let pk = pk_int32(id);
            RowLayout
                .put_row(&engine, ctx, &pk, &person_row(id, "x", true))
                .unwrap();
        }

        let rows: Vec<(Vec<u8>, Vec<Value>)> = RowLayout
            .scan_table(&engine, ctx)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Deduped and sorted: [1, 2, 3, 4, 5, 6, 9].
        let ids: Vec<i32> = rows
            .iter()
            .map(|(_, v)| match &v[0] {
                Value::Int32(n) => *n,
                _ => panic!("expected Int32"),
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 9]);
    }

    /// Two tables, same encoded PKs. `scan_table` must isolate to the
    /// requested table_id and never bleed into adjacent tables. This proves
    /// the prefix-range bounds (`[tid | ] .. [tid+1 | ]`) actually work.
    #[test]
    fn scan_table_isolates_to_table_id() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx_t1 = make_ctx(&types, 1);
        let ctx_t2 = make_ctx(&types, 2);

        for id in 0..3 {
            let pk = pk_int32(id);
            RowLayout
                .put_row(&engine, ctx_t1, &pk, &person_row(id, "t1", true))
                .unwrap();
            RowLayout
                .put_row(&engine, ctx_t2, &pk, &person_row(id, "t2", false))
                .unwrap();
        }

        let t1_rows: Vec<_> = RowLayout
            .scan_table(&engine, ctx_t1)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let t2_rows: Vec<_> = RowLayout
            .scan_table(&engine, ctx_t2)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(t1_rows.len(), 3, "table 1 should yield 3 rows");
        assert_eq!(t2_rows.len(), 3, "table 2 should yield 3 rows");
        // Spot-check that t1's rows really are t1's (col 1 = "t1").
        for (_, vs) in &t1_rows {
            assert_eq!(vs[1], Value::Varchar("t1".into()));
        }
        for (_, vs) in &t2_rows {
            assert_eq!(vs[1], Value::Varchar("t2".into()));
        }
    }

    /// Empty engine: scan_table yields zero items, no error.
    #[test]
    fn scan_table_empty_returns_no_rows() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);

        let rows: Vec<_> = RowLayout
            .scan_table(&engine, ctx)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(rows.is_empty());
    }

    /// `scan_range` is half-open: `[pk_start, pk_end)`. Rows at `pk_end` are
    /// excluded. Rows before `pk_start` and at-or-after `pk_end` are excluded.
    #[test]
    fn scan_range_subset() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);

        for id in 1..=5 {
            let pk = pk_int32(id);
            RowLayout
                .put_row(&engine, ctx, &pk, &person_row(id, "x", true))
                .unwrap();
        }

        // [encode(2), encode(4)) should yield ids 2 and 3 only.
        let start = pk_int32(2);
        let end = pk_int32(4);
        let rows: Vec<_> = RowLayout
            .scan_range(&engine, ctx, &start, &end)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let ids: Vec<i32> = rows
            .iter()
            .map(|(_, v)| match &v[0] {
                Value::Int32(n) => *n,
                _ => panic!(),
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }

    /// Empty range: `scan_range(pk, pk)` returns nothing (half-open).
    #[test]
    fn scan_range_empty_when_start_equals_end() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);

        for id in 1..=3 {
            let pk = pk_int32(id);
            RowLayout
                .put_row(&engine, ctx, &pk, &person_row(id, "x", true))
                .unwrap();
        }

        let pk = pk_int32(2);
        let rows: Vec<_> = RowLayout
            .scan_range(&engine, ctx, &pk, &pk)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(rows.is_empty());
    }

    /// Scan must yield `pk_encoded` bytes that round-trip back through
    /// `get_row`. Locks in the prefix-strip math (key[4..] is the PK) and
    /// verifies the encoded PK is preserved across the scan boundary.
    #[test]
    fn scan_yields_pk_encoded_round_trippable() {
        let (engine, _dir) = fresh_engine();
        let types = person_schema();
        let ctx = make_ctx(&types, 1);

        let original = person_row(42, "alice", true);
        let inserted_pk = pk_int32(42);
        RowLayout
            .put_row(&engine, ctx, &inserted_pk, &original)
            .unwrap();

        let rows: Vec<_> = RowLayout
            .scan_table(&engine, ctx)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 1);
        let (scanned_pk, _) = &rows[0];
        // The PK bytes from scan must match the bytes we passed into put_row.
        assert_eq!(scanned_pk, &inserted_pk);
        // And feeding them back into get_row finds the row.
        let back = RowLayout.get_row(&engine, ctx, scanned_pk).unwrap();
        assert_eq!(back, Some(original));
    }
}
