//! Table, column, and index metadata.
//!
//! Phase 9 Task 9.4 (this commit) defines the data shapes; Phase 9.6's
//! Catalog instantiates and persists them via the system tables defined
//! in Phase 9.5.

use serde::{Deserialize, Serialize};

use crate::catalog::TableId;
use crate::common::{Error, Result};
use crate::types::{ColumnType, Value};

/// A single column's declared metadata.
///
/// Persisted into `__sys_columns` (one row per column per table) — see
/// Phase 9.5. The `default` field carries any column default value declared
/// at `CREATE TABLE` time; `Some(Value::Null)` (default IS NULL) is distinct
/// from `None` (no default declared).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
    pub default: Option<Value>,
}

/// Full table schema: name, table id, columns, and which columns form the PK.
///
/// `primary_key` is a list of column indices into `columns`, in PK order.
/// Composite PKs are supported by construction (TPC-C `ORDER_LINE` uses a
/// 4-column PK; single-column PKs are the common case but not special-cased).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub table_id: TableId,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<usize>,
}

/// Per-index storage backend choice. Persisted in `__sys_indexes` so
/// reopens can instantiate the right engine. Phase 12 introduces this so
/// one table can have indexes split across multiple backends — e.g. a
/// hot lookup index on `BTreeEngine` and a write-heavy log index on
/// `LsmEngine`.
///
/// New variants are added by extending this enum + bumping the
/// `__sys_indexes` discriminator mapping in
/// `system_tables::write_index_row` / `read_index_row`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexBackend {
    BTree,
    Lsm,
}

impl IndexBackend {
    /// Stable discriminator used in `__sys_indexes`. Don't renumber; only
    /// append new variants with new ids.
    pub fn as_i32(self) -> i32 {
        match self {
            IndexBackend::BTree => 0,
            IndexBackend::Lsm => 1,
        }
    }

    pub fn from_i32(v: i32) -> Result<Self> {
        match v {
            0 => Ok(IndexBackend::BTree),
            1 => Ok(IndexBackend::Lsm),
            other => Err(Error::StorageCorrupted(format!(
                "unknown IndexBackend discriminator in __sys_indexes: {}",
                other
            ))),
        }
    }
}

/// Index declaration: name, target table, indexed columns, uniqueness,
/// and the per-index storage backend.
///
/// `columns` are positions in the target table's `Schema::columns`. The
/// same column may appear in arbitrarily many secondary indexes; the PK
/// is *not* represented as an `IndexDef` (it's implicit in
/// `Schema::primary_key`, though Phase 12 may materialize a unique PK
/// index — the `Schema` remains the source of truth for "what is the PK").
///
/// `backend` selects which storage engine type holds this index. Each
/// index gets its own engine instance allocated at create time and
/// re-instantiated on reopen. See `IndexBackend`.
///
/// No `id` field — `IndexId` is assigned by `Catalog::create_index` and
/// stored alongside this blob in `__sys_indexes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub table_id: TableId,
    pub columns: Vec<usize>,
    pub unique: bool,
    pub backend: IndexBackend,
}

impl Schema {
    /// Owned `Vec<ColumnType>` with the PK columns' types, in PK order.
    /// Used by `keyenc::encode_key_components` / `decode_key_components`,
    /// which take `&[ColumnType]` and don't know about `Schema`.
    pub fn pk_types(&self) -> Vec<ColumnType> {
        self.primary_key
            .iter()
            .map(|&i| self.columns[i].ty)
            .collect()
    }

    /// Owned `Vec<ColumnType>` with every column's type, in declaration order.
    /// Used by `tuple::encode` / `tuple::decode`.
    pub fn column_types(&self) -> Vec<ColumnType> {
        self.columns.iter().map(|c| c.ty).collect()
    }

    /// Serialize this schema to bytes for storage in `__sys_tables.schema_blob`.
    pub fn serialize_to_blob(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| Error::StorageCorrupted(format!("Schema::serialize_to_blob: {}", e)))
    }

    /// Inverse of `serialize_to_blob`. Used by `Catalog::open` to load
    /// table schemas back from `__sys_tables`.
    pub fn deserialize_from_blob(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes)
            .map_err(|e| Error::StorageCorrupted(format!("Schema::deserialize_from_blob: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a TPC-C-style WAREHOUSE schema for tests:
    /// (w_id Int32 PK, w_name Varchar(10), w_ytd Decimal(12,2) NOT NULL).
    fn warehouse_schema() -> Schema {
        Schema {
            name: "warehouse".to_string(),
            table_id: TableId(1),
            columns: vec![
                ColumnDef {
                    name: "w_id".to_string(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "w_name".to_string(),
                    ty: ColumnType::Varchar(10),
                    nullable: true,
                    default: None,
                },
                ColumnDef {
                    name: "w_ytd".to_string(),
                    ty: ColumnType::Decimal {
                        precision: 12,
                        scale: 2,
                    },
                    nullable: false,
                    default: Some(Value::Decimal(crate::types::Decimal::from_i64_with_scale(
                        0, 2,
                    ))),
                },
            ],
            primary_key: vec![0],
        }
    }

    #[test]
    fn schema_roundtrips_through_blob() {
        let schema = warehouse_schema();
        let bytes = schema.serialize_to_blob().unwrap();
        let back = Schema::deserialize_from_blob(&bytes).unwrap();
        assert_eq!(back, schema);
    }

    /// `pk_types` must return PK columns' types in PK order, not schema order.
    /// Build a schema with a composite PK whose declaration order differs
    /// from PK order to make this test meaningful.
    #[test]
    fn pk_types_in_pk_order_not_schema_order() {
        let schema = Schema {
            name: "order_line".to_string(),
            table_id: TableId(7),
            columns: vec![
                // Schema order: a, b, c
                ColumnDef {
                    name: "a".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "b".into(),
                    ty: ColumnType::Varchar(10),
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "c".into(),
                    ty: ColumnType::Boolean,
                    nullable: false,
                    default: None,
                },
            ],
            // PK order: c, a (skips b; reverses a/c)
            primary_key: vec![2, 0],
        };
        assert_eq!(
            schema.pk_types(),
            vec![ColumnType::Boolean, ColumnType::Int32]
        );
    }

    #[test]
    fn column_types_in_declaration_order() {
        let schema = warehouse_schema();
        assert_eq!(
            schema.column_types(),
            vec![
                ColumnType::Int32,
                ColumnType::Varchar(10),
                ColumnType::Decimal {
                    precision: 12,
                    scale: 2
                },
            ]
        );
    }

    /// `default: None` and `default: Some(Value::Null)` must round-trip
    /// distinguishably — they mean different things ("no default declared"
    /// vs "default IS NULL"). Bincode's Option encoding preserves this.
    #[test]
    fn column_def_default_none_vs_some_null_distinct() {
        let no_default = ColumnDef {
            name: "x".into(),
            ty: ColumnType::Int32,
            nullable: true,
            default: None,
        };
        let null_default = ColumnDef {
            name: "x".into(),
            ty: ColumnType::Int32,
            nullable: true,
            default: Some(Value::Null),
        };
        assert_ne!(no_default, null_default);

        let bytes_a = bincode::serialize(&no_default).unwrap();
        let bytes_b = bincode::serialize(&null_default).unwrap();
        assert_ne!(bytes_a, bytes_b, "encoded forms must differ");

        let back_a: ColumnDef = bincode::deserialize(&bytes_a).unwrap();
        let back_b: ColumnDef = bincode::deserialize(&bytes_b).unwrap();
        assert_eq!(back_a, no_default);
        assert_eq!(back_b, null_default);
    }

    #[test]
    fn index_def_roundtrips_through_bincode() {
        let def = IndexDef {
            name: "customer_by_last_name".to_string(),
            table_id: TableId(3),
            columns: vec![5, 1], // composite (c_last, c_w_id)
            unique: false,
            backend: IndexBackend::BTree,
        };
        let bytes = bincode::serialize(&def).unwrap();
        let back: IndexDef = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, def);
    }

    #[test]
    fn deserialize_corrupted_bytes_returns_storage_corrupted() {
        // Random bytes that aren't a valid bincode-encoded Schema.
        let garbage = vec![0xFF; 16];
        let err = Schema::deserialize_from_blob(&garbage)
            .expect_err("garbage bytes should not deserialize");
        assert!(matches!(err, Error::StorageCorrupted(_)));
    }

    #[test]
    fn empty_pk_is_constructible_at_type_level() {
        // Schema struct itself doesn't enforce "PK required" — that's
        // Catalog::create_table's job. The struct must allow constructing
        // an empty-PK Schema so tests can exercise rejection logic later.
        let schema = Schema {
            name: "tmp".into(),
            table_id: TableId(99),
            columns: vec![],
            primary_key: vec![],
        };
        assert!(schema.pk_types().is_empty());
        assert!(schema.column_types().is_empty());
        // And it round-trips fine.
        let bytes = schema.serialize_to_blob().unwrap();
        let back = Schema::deserialize_from_blob(&bytes).unwrap();
        assert_eq!(back, schema);
    }
}
