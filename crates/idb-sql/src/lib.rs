//! The SQL side: schema catalog, typed tables, row layouts, the SQL
//! pipeline (parse → bind → plan), and the executors. Depends on the
//! contracts in `idb-core` only — never on the storage implementations.
//! That property is enforced by this crate's `[dependencies]` and is the
//! workspace split's thesis.
pub mod catalog;
pub mod execution;
pub mod layout;
pub mod sql;
pub mod table;

// Dependency-module shims (see idb-storage/src/lib.rs for the pattern).
pub use idb_core::{common, types};

/// The contract-only view of storage this crate is allowed: the
/// `StorageEngine` trait family, nothing else. Impl types appear only
/// under `cfg(test)` via dev-dependencies.
pub mod storage {
    pub use idb_core::storage::engine::{
        IndexEngineOpener, ScanIterator, StorageEngine, StorageStatus,
    };
    #[cfg(test)]
    pub use idb_storage::storage::{FileDiskManager, MemoryDiskManager};
}

// Test-only shims: in-module unit tests construct real engines.
#[cfg(test)]
pub use idb_storage::{buffer, engines};
