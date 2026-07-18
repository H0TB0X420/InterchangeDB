//! The storage half: `DiskManager` trait + impls, the buffer pool with its
//! swappable eviction policies, and the `StorageEngine` impls (B+Tree,
//! LSM). Depends only on `idb-core`; knows nothing of WAL, transactions,
//! or SQL.
pub mod buffer;
pub mod engines;
pub mod storage;

// Dependency-module shims: moved code keeps its `crate::common::…` /
// `crate::sync::…` paths, and `crate::sync_trace!` call sites keep
// resolving, without any edits inside the moved modules.
pub use idb_core::sync_trace;
pub use idb_core::{common, sync};
