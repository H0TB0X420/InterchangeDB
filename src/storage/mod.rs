//! Storage layer - disk I/O, page formats, and storage engines.
//!
//! This module handles persistent storage:
//! - [`DiskManager`] - Low-level file I/O
//! - [`page`] - Page types and layouts
//! - [`engine`] - Storage engine trait and implementations

mod disk_manager;
pub mod engine;
pub mod page;

pub use disk_manager::DiskManager;
pub use engine::{ScanIterator, StorageEngine, StorageStatus};