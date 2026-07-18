//! Newtype identifiers for catalog objects + the system-id-range constants.
//!
//! All ids are uniformly `u32`. The user-id range is `[FIRST_USER_TABLE_ID,
//! FIRST_SYSTEM_TABLE_ID)`; the system-id range is `[FIRST_SYSTEM_TABLE_ID,
//! u32::MAX]`. System tables (`__sys_tables`, `__sys_columns`, `__sys_indexes`)
//! have hardcoded ids in the system range so they sort high in the engine's
//! keyspace, away from user data.

use serde::{Deserialize, Serialize};

use crate::common::{Error, Result};

/// Lowest valid table id assignable to a user-defined table.
pub const FIRST_USER_TABLE_ID: u32 = 1;

/// Lowest table id reserved for the system catalog. User table ids are
/// strictly less than this; system tables are at or above.
pub const FIRST_SYSTEM_TABLE_ID: u32 = 0xFFFF_0001;

/// Hardcoded id of the `__sys_tables` system table.
pub const SYS_TABLES_ID: TableId = TableId(0xFFFF_0001);
/// Hardcoded id of the `__sys_columns` system table.
pub const SYS_COLUMNS_ID: TableId = TableId(0xFFFF_0002);
/// Hardcoded id of the `__sys_indexes` system table.
pub const SYS_INDEXES_ID: TableId = TableId(0xFFFF_0003);
/// Hardcoded id of the `__sys_table_stats` system table (P14.1).
/// One row per user table, carrying its current row count.
pub const SYS_TABLE_STATS_ID: TableId = TableId(0xFFFF_0004);
/// Hardcoded id of the `__sys_column_stats` system table (P14.1).
/// One row per (table_id, column_id), carrying per-column NDV +
/// histogram blob.
pub const SYS_COLUMN_STATS_ID: TableId = TableId(0xFFFF_0005);

/// Identifier for a table — user or system. Persisted in `__sys_tables` and
/// embedded as the prefix of every storage key for that table's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u32);

impl TableId {
    /// True if this id falls within the reserved system range.
    pub const fn is_system(&self) -> bool {
        self.0 >= FIRST_SYSTEM_TABLE_ID
    }

    /// Return the next table id (`self + 1`). Used by `RowLayout::scan_table`
    /// to compute the upper bound of a table-prefix scan range. Errors on
    /// `u32::MAX` to keep the function total — no system table reaches there
    /// in practice.
    pub fn next(&self) -> Result<TableId> {
        self.0
            .checked_add(1)
            .map(TableId)
            .ok_or_else(|| Error::StorageCorrupted("TableId::next: u32 overflow".into()))
    }
}

/// Identifier for an index (PK index or secondary index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexId(pub u32);

/// Externally-visible column identity. Distinct from a `usize` index into
/// `Schema::columns` — the index is the position now; `ColumnId` is the
/// durable identity that survives any future schema reordering. For Phase 9
/// they happen to coincide because we don't support DROP COLUMN yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnId(pub u32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_system_boundary() {
        assert!(!TableId(FIRST_USER_TABLE_ID).is_system());
        assert!(!TableId(FIRST_SYSTEM_TABLE_ID - 1).is_system());
        assert!(TableId(FIRST_SYSTEM_TABLE_ID).is_system());
        assert!(SYS_TABLES_ID.is_system());
        assert!(SYS_COLUMNS_ID.is_system());
        assert!(SYS_INDEXES_ID.is_system());
    }

    #[test]
    fn next_increments() {
        assert_eq!(TableId(5).next().unwrap(), TableId(6));
        assert_eq!(SYS_TABLES_ID.next().unwrap(), TableId(0xFFFF_0002));
    }

    #[test]
    fn next_errors_at_u32_max() {
        let err = TableId(u32::MAX).next().unwrap_err();
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("overflow")));
    }

    #[test]
    fn serde_roundtrip_via_bincode() {
        let id = TableId(42);
        let bytes = bincode::serialize(&id).unwrap();
        let back: TableId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, id);
    }
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
