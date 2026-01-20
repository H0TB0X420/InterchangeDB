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
//! │  │     DiskManager + Page + PageHeader + recovery/WAL       │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Modules
//! - [`common`] - Shared primitives (PageId, FrameId, Error, config)
//! - [`buffer`] - Buffer pool management and eviction policies
//! - [`storage`] - Disk I/O and page formats
//! - [`index`] - Index structures (B-tree)
//! - [`recovery`] - Write-ahead logging and crash recovery
//! - [`concurrency`] - Transaction management and MVCC
//! - [`execution`] - Query execution
//!
//! # Quick Start
//! ```no_run
//! use interchangedb::storage::DiskManager;
//! use interchangedb::common::PageId;
//!
//! // Create a new database file
//! let mut dm = DiskManager::create("my_database.db").unwrap();
//!
//! // Allocate and write a page
//! let page_id = dm.allocate_page().unwrap();
//! ```

// Core modules
pub mod buffer;
pub mod common;
pub mod storage;

// Future modules (placeholders)
pub mod concurrency;
pub mod execution;
pub mod index;
pub mod recovery;

// Re-export commonly used items at crate root for convenience
pub use common::config::PAGE_SIZE;
pub use common::{Error, FrameId, PageId, Result};

pub use buffer::{BufferPoolStats, Frame, StatsSnapshot, BufferPoolManager};
pub use storage::page::{Page, PageHeader, PageType};
pub use storage::DiskManager;
