//! InterchangeDB - A database with runtime-swappable buffer pool eviction policies.
//!
//! # Architecture
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         InterchangeDB                           │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │              Query Layer (execution/)                    │   │
//! │  │         SQL Parser → Planner → Executor                  │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! │                              ↓                                  │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │           Transaction Layer (concurrency/)               │   │
//! │  │      TransactionManager + MVCC + ConcurrencyControl      │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! │                              ↓                                  │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │       Index Layer (index/)  [Compile-Time Swappable]    │   │
//! │  │              B-tree  ←─OR─→  LSM-tree                    │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! │                              ↓                                  │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │       Buffer Pool (buffer/)  [Runtime Swappable]        │   │
//! │  │   ┌─────────────────────────────────────────────────┐   │   │
//! │  │   │  Eviction Policies: LRU | CLOCK | LRU-K | 2Q    │   │   │
//! │  │   │            (hot-swappable at runtime)            │   │   │
//! │  │   └─────────────────────────────────────────────────┘   │   │
//! │  │      BufferPoolManager + Frame + Statistics              │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! │                              ↓                                  │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │           Storage Layer (storage/)                       │   │
//! │  │     FileDiskManager + Page + PageHeader + recovery/WAL       │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Modules
//! - [`common`] - Shared primitives (PageId, FrameId, Error, config)
//! - [`buffer`] - Buffer pool management and eviction policies
//! - [`storage`] - Disk I/O and page formats
//! - [`index`] - Index structures (B-tree)
//! - [`wal`] - Write-ahead logging and crash recovery
//! - [`concurrency`] - Transaction management and MVCC
//! - [`execution`] - Query execution
//!
//! # Quick Start
//! ```no_run
//! use interchangedb::storage::{DiskManager, FileDiskManager};
//! use interchangedb::common::PageId;
//!
//! // Create a new database file
//! let mut dm = FileDiskManager::create("my_database.db").unwrap();
//!
//! // Allocate and write a page
//! let page_id = dm.allocate_page().unwrap();
//! ```

// Core modules
pub mod buffer;
pub mod catalog;
pub use idb_core::common;
pub mod database;
pub mod layout;
pub mod storage;

/// Synchronization-primitive shim (parking_lot in prod, shuttle under test).
pub(crate) use idb_core::sync;
pub use idb_core::sync_trace;

/// The pluggable `StorageEngine` implementations (B+Tree, LSM).
pub mod engines;
pub mod execution;
pub mod session;
pub mod sql;
pub mod table;
pub mod txn;
pub use idb_core::types;
pub mod wal;

// Re-export commonly used items at crate root for convenience
pub use common::config::PAGE_SIZE;
pub use common::{Error, FrameId, PageId, Result};

pub use buffer::{BufferPoolManager, BufferPoolStats, Frame, StatsSnapshot};
pub use database::{BTreeDatabase, Database, LsmDatabase};
pub use engines::btree::BTreeEngine;
pub use engines::lsm::LsmEngine;
pub use storage::page::{Page, PageHeader, PageType};
pub use storage::{DiskManager, FileDiskManager, MemoryDiskManager};
pub use storage::{ScanIterator, StorageEngine, StorageStatus};
pub use txn::{LockManager, LockMode, TransactionManager, TxnId, TxnMode, TxnState};
