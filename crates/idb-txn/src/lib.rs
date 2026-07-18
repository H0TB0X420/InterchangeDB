//! Transactions: the transaction manager, MVCC timestamps, lock manager,
//! isolation policies, and the `TxnEngine` wrapper over any
//! `StorageEngine`. Sits on `idb-wal`; knows nothing of SQL.
pub mod txn;

// Dependency-module shims (see idb-storage/src/lib.rs for the pattern).
pub use idb_core::{common, types};
pub use idb_storage::storage;
pub use idb_wal::wal;

// Test-only shims: the in-module unit tests construct real engines.
#[cfg(test)]
pub use idb_storage::{buffer, engines};
