//! Core types and constants for InterchangeDB.
//!
//! This crate provides fundamental abstractions used throughout the database:
//!
//! # Types
//! - [`Page`] - A 4KB unit of storage (the fundamental I/O unit)
//! - [`PageId`] - Identifier for pages on disk
//! - [`FrameId`] - Identifier for frames in the buffer pool
//! - [`BufferPoolStats`] - Statistics tracking for buffer pool performance
//!
//! # Constants
//! - [`PAGE_SIZE`] - Size of a page in bytes (4096)
//!
//! # Error Handling
//! - [`Error`] - Unified error type
//! - [`Result<T>`] - Convenient result alias
//!
//! # Example
//! ```
//! use interchangedb_core::{Page, PageId, PAGE_SIZE};
//!
//! let page_id = PageId::new(42);
//! let mut page = Page::new();
//!
//! // Write some data
//! page.as_mut_slice()[0] = 0xFF;
//!
//! assert_eq!(PAGE_SIZE, 4096);
//! ```

// Declare modules
pub mod config;
pub mod error;
pub mod types;

// Re-export commonly used items at crate root
// This allows users to write: use interchangedb_core::PageId;
// Instead of:            use interchangedb_core::types::PageId;
pub use config::PAGE_SIZE;
pub use error::{Error, Result};
pub use types::{BufferPoolStats, FrameId, Page, PageId, StatsSnapshot};