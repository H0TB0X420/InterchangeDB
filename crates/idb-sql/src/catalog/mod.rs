//! Catalog: tables, columns, indexes, and the metadata that ties them together.
//!
//! Phase 9 build-out:
//! - Task 9.4 (this commit): `TableId` / `IndexId` / `ColumnId` newtypes +
//!   the system-id-range constants used by Task 9.5's bootstrap; `Schema`,
//!   `ColumnDef`, `IndexDef` data shapes.
//! - Task 9.5: self-hosted system tables.
//! - Task 9.6: `Catalog<E>`, constraint helpers.

pub mod constraints;
pub mod manager;
pub mod schema;
pub mod system_tables;

pub use crate::common::ids;
pub use crate::common::ids::{
    ColumnId, IndexBackend, IndexId, TableId, FIRST_SYSTEM_TABLE_ID, FIRST_USER_TABLE_ID,
};
pub use manager::{Catalog, IndexEngineOpener, IndexHandle};
pub use schema::{ColumnDef, IndexDef, Schema};
