//! Self-hosted system tables: `__sys_tables`, `__sys_columns`, `__sys_indexes`.
//!
//! These three tables hold the catalog. They are stored in the same engine
//! as user data, using `RowLayout` regardless of the user's chosen layout
//! (rationale: catalog access is point-lookup-heavy on small rows; row layout
//! is the right tool, and forcing every catalog read through `ColumnLayout`'s
//! per-column reassembly would slow Phase 11+'s query planning for no benefit).
//!
//! # Bootstrap chicken-and-egg
//!
//! Reading `__sys_tables` requires knowing its schema. We solve this with
//! hardcoded schemas (this module): `sys_tables_schema()`, `sys_columns_schema()`,
//! and `sys_indexes_schema()` are baked into the binary. On a fresh data dir,
//! `initialize_system_tables` materializes them on disk; on reopen,
//! `load_system_tables` reads them back and validates that the stored form
//! still matches the hardcoded form (drift detection).
//!
//! Same approach as Postgres (`pg_class.h` constants), SQLite (`sqlite_master`
//! at fixed root page 1), and Cockroach (`pkg/sql/catalog/systemschema/system.go`).
//!
//! # Plan deviation
//!
//! The Phase 9 plan specified `Int32` for table_id columns, but
//! `FIRST_SYSTEM_TABLE_ID = 0xFFFF_0001` overflows `i32::MAX`. Switched to
//! `Int64` for all id columns in system tables; `i64` holds `u32` cleanly
//! with no sign-bit reinterpretation.

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::ids::{SYS_COLUMNS_ID, SYS_INDEXES_ID, SYS_TABLES_ID};
use crate::catalog::{ColumnDef, Schema};
use crate::common::{Error, Result};
use crate::layout::{DataLayout, LayoutCtx, RowLayout};
use crate::storage::StorageEngine;
use crate::types::{keyenc, tuple, ColumnType, Value};

// ---- hardcoded schemas ----------------------------------------------------

/// Hardcoded schema for `__sys_tables`. Lists every table in the database
/// (including system tables, which describe themselves via this table).
pub fn sys_tables_schema() -> Schema {
    Schema {
        name: "__sys_tables".to_string(),
        table_id: SYS_TABLES_ID,
        columns: vec![
            ColumnDef {
                name: "table_id".to_string(),
                ty: ColumnType::Int64,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "name".to_string(),
                ty: ColumnType::Varchar(128),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "schema_blob".to_string(),
                ty: ColumnType::Bytes(u16::MAX),
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    }
}

/// Hardcoded schema for `__sys_columns`. One row per column per table.
pub fn sys_columns_schema() -> Schema {
    Schema {
        name: "__sys_columns".to_string(),
        table_id: SYS_COLUMNS_ID,
        columns: vec![
            ColumnDef {
                name: "table_id".to_string(),
                ty: ColumnType::Int64,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "column_id".to_string(),
                ty: ColumnType::Int64,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "name".to_string(),
                ty: ColumnType::Varchar(128),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "type_blob".to_string(),
                ty: ColumnType::Bytes(1024),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "nullable".to_string(),
                ty: ColumnType::Boolean,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "default_blob".to_string(),
                // Always present (encodes Option<Value>); None encodes to ~1 byte.
                ty: ColumnType::Bytes(u16::MAX),
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0, 1],
    }
}

/// Hardcoded schema for `__sys_indexes`. One row per index. Phase 9 doesn't
/// register any indexes, so this table is empty after `initialize_system_tables`.
pub fn sys_indexes_schema() -> Schema {
    Schema {
        name: "__sys_indexes".to_string(),
        table_id: SYS_INDEXES_ID,
        columns: vec![
            ColumnDef {
                name: "index_id".to_string(),
                ty: ColumnType::Int64,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "table_id".to_string(),
                ty: ColumnType::Int64,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "name".to_string(),
                ty: ColumnType::Varchar(128),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "columns_blob".to_string(),
                ty: ColumnType::Bytes(8192),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "unique".to_string(),
                ty: ColumnType::Boolean,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    }
}

/// All three system schemas, in initialization order.
fn all_system_schemas() -> [Schema; 3] {
    [
        sys_tables_schema(),
        sys_columns_schema(),
        sys_indexes_schema(),
    ]
}

// ---- public API -----------------------------------------------------------

/// Probe whether the engine has been initialized as a catalog.
///
/// Cheap: one engine.get for the `__sys_tables` row that describes itself.
pub fn is_initialized<E: StorageEngine>(engine: &E) -> Result<bool> {
    let sys_tables = sys_tables_schema();
    let column_types = sys_tables.column_types();
    let ctx = LayoutCtx {
        column_types: &column_types,
        table_id: SYS_TABLES_ID,
    };
    let pk = encode_int64_pk(SYS_TABLES_ID.0 as i64)?;
    Ok(RowLayout.get_row(engine, ctx, &pk)?.is_some())
}

/// Materialize the three system tables from the hardcoded schemas.
///
/// Idempotent: re-running on an already-initialized engine overwrites the
/// system rows with the same content. Catalog::open is the gatekeeper that
/// only calls this when `is_initialized` returns false.
pub fn initialize_system_tables<E: StorageEngine>(engine: &E) -> Result<()> {
    for schema in all_system_schemas() {
        write_sys_tables_row(engine, &schema)?;
        write_sys_columns_rows(engine, &schema)?;
    }
    // __sys_indexes intentionally has no rows here; Phase 9 doesn't register
    // indexes for system tables, and no user indexes exist yet.
    Ok(())
}

/// Read all schemas from `__sys_tables` and return them as a name → Schema map.
///
/// Performs drift detection: every system table's stored schema must equal
/// the hardcoded schema, byte-for-byte (via PartialEq). Errors with
/// `CatalogDrift` if any system schema is missing or differs.
pub fn load_system_tables<E: StorageEngine>(
    engine: &E,
) -> Result<HashMap<String, Arc<Schema>>> {
    let sys_tables = sys_tables_schema();
    let column_types = sys_tables.column_types();
    let ctx = LayoutCtx {
        column_types: &column_types,
        table_id: SYS_TABLES_ID,
    };

    let mut schemas: HashMap<String, Arc<Schema>> = HashMap::new();
    for row_result in RowLayout.scan_table(engine, ctx) {
        let (_pk_encoded, values) = row_result?;
        // Row shape: [Int64 table_id, Varchar name, Bytes schema_blob].
        let blob = match &values[2] {
            Value::Bytes(b) => b,
            other => {
                return Err(Error::StorageCorrupted(format!(
                    "__sys_tables.schema_blob is not Bytes: {:?}",
                    other
                )))
            }
        };
        let schema = Schema::deserialize_from_blob(blob)?;
        schemas.insert(schema.name.clone(), Arc::new(schema));
    }

    // Drift detection — every hardcoded system schema must be present and
    // match its stored form.
    for sys_schema in all_system_schemas() {
        match schemas.get(&sys_schema.name) {
            Some(stored) if stored.as_ref() == &sys_schema => { /* OK */ }
            Some(_) => {
                return Err(Error::CatalogDrift(format!(
                    "stored schema for {} differs from hardcoded",
                    sys_schema.name
                )))
            }
            None => {
                return Err(Error::CatalogDrift(format!(
                    "system table {} missing from __sys_tables",
                    sys_schema.name
                )))
            }
        }
    }

    Ok(schemas)
}

// ---- internals ------------------------------------------------------------

/// Encode a single Int64 PK component (used for __sys_tables and __sys_indexes).
fn encode_int64_pk(value: i64) -> Result<Vec<u8>> {
    keyenc::encode_key_components(&[&Value::Int64(value)], &[ColumnType::Int64])
}

/// Encode a (Int64, Int64) composite PK (used for __sys_columns).
fn encode_int64_pair_pk(a: i64, b: i64) -> Result<Vec<u8>> {
    keyenc::encode_key_components(
        &[&Value::Int64(a), &Value::Int64(b)],
        &[ColumnType::Int64, ColumnType::Int64],
    )
}

/// Bincode wrapper that maps the error to `StorageCorrupted` for caller uniformity.
fn to_blob<T: serde::Serialize>(value: &T, what: &str) -> Result<Vec<u8>> {
    bincode::serialize(value)
        .map_err(|e| Error::StorageCorrupted(format!("encode {}: {}", what, e)))
}

/// Insert / overwrite the row in `__sys_tables` that describes `described`.
fn write_sys_tables_row<E: StorageEngine>(engine: &E, described: &Schema) -> Result<()> {
    let sys_tables = sys_tables_schema();
    let column_types = sys_tables.column_types();
    let ctx = LayoutCtx {
        column_types: &column_types,
        table_id: SYS_TABLES_ID,
    };
    let pk = encode_int64_pk(described.table_id.0 as i64)?;
    let values = vec![
        Value::Int64(described.table_id.0 as i64),
        Value::Varchar(described.name.clone()),
        Value::Bytes(described.serialize_to_blob()?),
    ];
    // Sanity: the encoded tuple must round-trip through tuple::decode for
    // load_system_tables to work. We don't actually call decode here (waste
    // of cycles in the hot init path); tests cover the roundtrip property.
    let _ = tuple::encode(&column_types, &values)?; // catches arity / type mismatch early
    RowLayout.put_row(engine, ctx, &pk, &values)
}

/// Write the catalog rows for a user table (called by `Catalog::create_table`):
/// one row in `__sys_tables` plus one per column in `__sys_columns`.
pub fn write_user_table_rows<E: StorageEngine>(engine: &E, schema: &Schema) -> Result<()> {
    write_sys_tables_row(engine, schema)?;
    write_sys_columns_rows(engine, schema)?;
    Ok(())
}

/// Delete a user table's catalog rows (called by `Catalog::drop_table`).
/// Removes the `__sys_tables` row and every `__sys_columns` row for this
/// table_id. Does NOT touch user data — that's deferred.
pub fn delete_user_table_rows<E: StorageEngine>(engine: &E, schema: &Schema) -> Result<()> {
    // Remove the __sys_tables row.
    let sys_tables = sys_tables_schema();
    let st_types = sys_tables.column_types();
    let st_ctx = LayoutCtx {
        column_types: &st_types,
        table_id: SYS_TABLES_ID,
    };
    let st_pk = encode_int64_pk(schema.table_id.0 as i64)?;
    RowLayout.delete_row(engine, st_ctx, &st_pk)?;

    // Remove every __sys_columns row for this table_id.
    let sys_columns = sys_columns_schema();
    let sc_types = sys_columns.column_types();
    let sc_ctx = LayoutCtx {
        column_types: &sc_types,
        table_id: SYS_COLUMNS_ID,
    };
    for column_id in 0..schema.columns.len() {
        let pk = encode_int64_pair_pk(schema.table_id.0 as i64, column_id as i64)?;
        RowLayout.delete_row(engine, sc_ctx, &pk)?;
    }
    Ok(())
}

/// Insert one row into `__sys_indexes` for the given index definition.
pub fn write_index_row<E: StorageEngine>(
    engine: &E,
    index_id: crate::catalog::IndexId,
    def: &crate::catalog::IndexDef,
) -> Result<()> {
    let sys_indexes = sys_indexes_schema();
    let column_types = sys_indexes.column_types();
    let ctx = LayoutCtx {
        column_types: &column_types,
        table_id: SYS_INDEXES_ID,
    };
    let pk = encode_int64_pk(index_id.0 as i64)?;
    let columns_blob = to_blob(&def.columns, "Vec<usize>")?;
    let values = vec![
        Value::Int64(index_id.0 as i64),
        Value::Int64(def.table_id.0 as i64),
        Value::Varchar(def.name.clone()),
        Value::Bytes(columns_blob),
        Value::Boolean(def.unique),
    ];
    RowLayout.put_row(engine, ctx, &pk, &values)
}

/// Insert / overwrite all `__sys_columns` rows describing `table`'s columns.
fn write_sys_columns_rows<E: StorageEngine>(engine: &E, table: &Schema) -> Result<()> {
    let sys_columns = sys_columns_schema();
    let column_types = sys_columns.column_types();
    let ctx = LayoutCtx {
        column_types: &column_types,
        table_id: SYS_COLUMNS_ID,
    };

    for (column_id, col) in table.columns.iter().enumerate() {
        let pk = encode_int64_pair_pk(table.table_id.0 as i64, column_id as i64)?;
        let values = vec![
            Value::Int64(table.table_id.0 as i64),
            Value::Int64(column_id as i64),
            Value::Varchar(col.name.clone()),
            Value::Bytes(to_blob(&col.ty, "ColumnType")?),
            Value::Boolean(col.nullable),
            Value::Bytes(to_blob(&col.default, "Option<Value>")?),
        ];
        RowLayout.put_row(engine, ctx, &pk, &values)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::TableId;
    use crate::index::btree::BTreeEngine;
    use crate::storage::FileDiskManager;
    use tempfile::TempDir;

    fn fresh_engine() -> (BTreeEngine, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        (engine, dir)
    }

    // ---- hardcoded schemas: shape sanity ----------------------------------

    /// `sys_tables_schema()` describes `__sys_tables` itself: name, table_id,
    /// and PK shape must be self-consistent.
    #[test]
    fn sys_tables_schema_shape() {
        let s = sys_tables_schema();
        assert_eq!(s.name, "__sys_tables");
        assert_eq!(s.table_id, SYS_TABLES_ID);
        assert_eq!(s.primary_key, vec![0]);
        assert_eq!(s.columns.len(), 3);
    }

    #[test]
    fn sys_columns_schema_shape() {
        let s = sys_columns_schema();
        assert_eq!(s.name, "__sys_columns");
        assert_eq!(s.table_id, SYS_COLUMNS_ID);
        // Composite PK: (table_id, column_id).
        assert_eq!(s.primary_key, vec![0, 1]);
        assert_eq!(s.columns.len(), 6);
    }

    #[test]
    fn sys_indexes_schema_shape() {
        let s = sys_indexes_schema();
        assert_eq!(s.name, "__sys_indexes");
        assert_eq!(s.table_id, SYS_INDEXES_ID);
        assert_eq!(s.primary_key, vec![0]);
        assert_eq!(s.columns.len(), 5);
    }

    /// Hardcoded schemas must each round-trip through their own
    /// `serialize_to_blob` / `deserialize_from_blob`. (Bincode shape
    /// invariant — if this breaks, init/load both break.)
    #[test]
    fn each_system_schema_blob_roundtrips() {
        for schema in all_system_schemas() {
            let bytes = schema.serialize_to_blob().unwrap();
            let back = Schema::deserialize_from_blob(&bytes).unwrap();
            assert_eq!(back, schema);
        }
    }

    // ---- is_initialized ---------------------------------------------------

    #[test]
    fn is_initialized_false_on_empty_engine() {
        let (engine, _dir) = fresh_engine();
        assert!(!is_initialized(&engine).unwrap());
    }

    #[test]
    fn is_initialized_true_after_init() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();
        assert!(is_initialized(&engine).unwrap());
    }

    // ---- initialize -------------------------------------------------------

    /// After init, `__sys_tables` contains exactly 3 rows (one per system
    /// table). Verified via direct scan.
    #[test]
    fn init_populates_three_sys_tables_rows() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();

        let sys_tables = sys_tables_schema();
        let column_types = sys_tables.column_types();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: SYS_TABLES_ID,
        };
        let rows: Vec<_> = RowLayout
            .scan_table(&engine, ctx)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
    }

    /// After init, `__sys_columns` contains rows totaling the sum of every
    /// system schema's column count: 3 + 6 + 5 = 14.
    #[test]
    fn init_populates_correct_sys_columns_count() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();

        let sys_columns = sys_columns_schema();
        let column_types = sys_columns.column_types();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: SYS_COLUMNS_ID,
        };
        let rows: Vec<_> = RowLayout
            .scan_table(&engine, ctx)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let expected: usize = all_system_schemas().iter().map(|s| s.columns.len()).sum();
        assert_eq!(rows.len(), expected, "expected {} __sys_columns rows", expected);
    }

    /// After init, `__sys_indexes` is empty (Phase 9 registers no indexes).
    #[test]
    fn init_leaves_sys_indexes_empty() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();

        let sys_indexes = sys_indexes_schema();
        let column_types = sys_indexes.column_types();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: SYS_INDEXES_ID,
        };
        let rows: Vec<_> = RowLayout
            .scan_table(&engine, ctx)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(rows.is_empty());
    }

    /// `init` is idempotent — calling it twice on the same engine produces
    /// the same final state (the second call upserts identical rows).
    #[test]
    fn init_is_idempotent() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();
        initialize_system_tables(&engine).unwrap();

        // Row counts unchanged.
        let sys_tables = sys_tables_schema();
        let column_types = sys_tables.column_types();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: SYS_TABLES_ID,
        };
        let rows: Vec<_> = RowLayout
            .scan_table(&engine, ctx)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
    }

    // ---- load -------------------------------------------------------------

    /// Init → load returns a HashMap with all three system schemas, keyed
    /// by name, each Schema byte-equal to the hardcoded one.
    #[test]
    fn load_returns_three_system_schemas() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();
        let schemas = load_system_tables(&engine).unwrap();

        assert_eq!(schemas.len(), 3);
        assert_eq!(schemas.get("__sys_tables").unwrap().as_ref(), &sys_tables_schema());
        assert_eq!(schemas.get("__sys_columns").unwrap().as_ref(), &sys_columns_schema());
        assert_eq!(schemas.get("__sys_indexes").unwrap().as_ref(), &sys_indexes_schema());
    }

    /// User schemas inserted into __sys_tables (e.g., by Catalog::create_table
    /// in Phase 9.6) are also surfaced by load. This is the integration test
    /// for "system + user schemas coexist."
    #[test]
    fn load_surfaces_user_schemas_alongside_system() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();

        // Manually insert a user table row into __sys_tables (mimicking what
        // Catalog::create_table will do in 9.6).
        let user_schema = Schema {
            name: "users".to_string(),
            table_id: TableId(42),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            }],
            primary_key: vec![0],
        };
        write_sys_tables_row(&engine, &user_schema).unwrap();

        let schemas = load_system_tables(&engine).unwrap();
        assert_eq!(schemas.len(), 4);
        assert_eq!(schemas.get("users").unwrap().as_ref(), &user_schema);
    }

    // ---- drift detection --------------------------------------------------

    /// Tampering with __sys_tables's row for itself (replacing schema_blob
    /// with a different Schema's blob) must be caught by load.
    #[test]
    fn drift_detected_when_self_blob_replaced() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();

        // Build a tampered schema and write it to __sys_tables under
        // __sys_tables's PK. Stored shape: name field still says "__sys_tables",
        // but the embedded schema_blob is for a different schema.
        let mut wrong = sys_tables_schema();
        wrong.name = "WRONG".to_string();

        let sys_tables = sys_tables_schema();
        let column_types = sys_tables.column_types();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: SYS_TABLES_ID,
        };
        let pk = encode_int64_pk(SYS_TABLES_ID.0 as i64).unwrap();
        let bad_row = vec![
            Value::Int64(SYS_TABLES_ID.0 as i64),
            Value::Varchar(sys_tables.name.clone()),
            Value::Bytes(wrong.serialize_to_blob().unwrap()),
        ];
        RowLayout.put_row(&engine, ctx, &pk, &bad_row).unwrap();

        let err = load_system_tables(&engine).expect_err("drift must be caught");
        assert!(matches!(err, Error::CatalogDrift(_)));
    }

    /// A column-type change in a stored system schema is also caught.
    #[test]
    fn drift_detected_when_column_type_changes() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();

        let mut tampered = sys_columns_schema();
        // Change one column's nullability — different from the hardcoded form.
        tampered.columns[0].nullable = !tampered.columns[0].nullable;

        let sys_tables = sys_tables_schema();
        let column_types = sys_tables.column_types();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: SYS_TABLES_ID,
        };
        let pk = encode_int64_pk(SYS_COLUMNS_ID.0 as i64).unwrap();
        let bad_row = vec![
            Value::Int64(SYS_COLUMNS_ID.0 as i64),
            Value::Varchar("__sys_columns".to_string()),
            Value::Bytes(tampered.serialize_to_blob().unwrap()),
        ];
        RowLayout.put_row(&engine, ctx, &pk, &bad_row).unwrap();

        let err = load_system_tables(&engine).expect_err("drift must be caught");
        assert!(matches!(err, Error::CatalogDrift(ref msg) if msg.contains("__sys_columns")));
    }

    /// If a system table's row is missing from __sys_tables, load must error
    /// with a CatalogDrift mentioning the missing table — not silently
    /// produce a partial catalog.
    #[test]
    fn drift_detected_when_system_row_missing() {
        let (engine, _dir) = fresh_engine();
        initialize_system_tables(&engine).unwrap();

        // Delete the __sys_indexes self-row.
        let sys_tables = sys_tables_schema();
        let column_types = sys_tables.column_types();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: SYS_TABLES_ID,
        };
        let pk = encode_int64_pk(SYS_INDEXES_ID.0 as i64).unwrap();
        RowLayout.delete_row(&engine, ctx, &pk).unwrap();

        let err = load_system_tables(&engine).expect_err("missing system table must be caught");
        assert!(matches!(err, Error::CatalogDrift(ref m) if m.contains("__sys_indexes")));
    }

    // ---- end-to-end -------------------------------------------------------

    /// The full bootstrap loop: init populates, load reads back, every
    /// hardcoded schema matches byte-for-byte. This is the "if this passes,
    /// the catalog is healthy" canary test.
    #[test]
    fn end_to_end_init_then_load_roundtrip() {
        let (engine, _dir) = fresh_engine();
        assert!(!is_initialized(&engine).unwrap());

        initialize_system_tables(&engine).unwrap();
        assert!(is_initialized(&engine).unwrap());

        let schemas = load_system_tables(&engine).unwrap();
        for hardcoded in all_system_schemas() {
            let stored = schemas
                .get(&hardcoded.name)
                .unwrap_or_else(|| panic!("missing {}", hardcoded.name));
            assert_eq!(stored.as_ref(), &hardcoded);
        }
    }
}
