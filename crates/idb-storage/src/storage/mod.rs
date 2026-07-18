//! Storage layer - disk I/O, page formats, and storage engines.
//!
//! This module handles persistent storage:
//! - [`DiskManager`] (trait) - the disk backend contract used by the BPM.
//! - [`FileDiskManager`] - production file-backed backend.
//! - [`MemoryDiskManager`] - in-memory backend for fast tests + Q-07 wrapping.
//! - [`page`] - Page types and layouts.
//! - [`engine`] - Storage engine trait and implementations.

mod disk_manager;
pub use idb_core::storage::engine;
pub mod page;

pub use disk_manager::{DiskManager, FileDiskManager, MemoryDiskManager};
pub use engine::{ScanIterator, StorageEngine, StorageStatus};
