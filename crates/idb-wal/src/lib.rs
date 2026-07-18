//! Write-ahead logging: records, segments, group commit, recovery.
//! Depends on `idb-core` and the `StorageEngine` trait from `idb-storage`
//! (recovery replays into any engine); knows nothing above that.
pub mod wal;

// Dependency-module shims (see idb-storage/src/lib.rs for the pattern).
pub use idb_core::common;
pub use idb_storage::storage;
