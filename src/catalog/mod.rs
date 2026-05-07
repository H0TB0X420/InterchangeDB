//! Catalog: tables, columns, indexes, and the metadata that ties them together.
//!
//! Phase 9 build-out:
//! - Task 9.4 (this commit): `TableId` / `IndexId` / `ColumnId` newtypes +
//!   the system-id-range constants used by Task 9.5's bootstrap.
//! - Future Task 9.4: `Schema`, `ColumnDef`, `IndexDef`.
//! - Task 9.5: self-hosted system tables.
//! - Task 9.6: `Catalog<E>`, constraint helpers.

pub mod ids;

pub use ids::{ColumnId, IndexId, TableId, FIRST_SYSTEM_TABLE_ID, FIRST_USER_TABLE_ID};
