//! Storage layer - disk I/O and page formats.
//!
//! This module handles persistent storage:
//! - [`DiskManager`] - Low-level file I/O
//! - [`page`] - Page types and layouts

mod disk_manager;
pub mod page;

pub use disk_manager::DiskManager;