//! Buffer pool management.
//!
//! The buffer pool is the in-memory cache layer between storage engines
//! and disk. It manages a fixed pool of frames, each holding one page.
//!
//! # Components
//! - [`BufferPoolManager`] - The main page cache
//! - [`Frame`] - A slot in the buffer pool holding a page + metadata
//! - [`PageReadGuard`] / [`PageWriteGuard`] - RAII guards for page access
//! - [`BufferPoolStats`] - Performance statistics
//! - [`replacer`] - Eviction policy implementations

mod buffer_pool_manager;
mod frame;
mod page_guard;
pub mod replacer;
mod stats;

pub use buffer_pool_manager::BufferPoolManager;
pub use frame::Frame;
pub use page_guard::{PageReadGuard, PageWriteGuard};
pub use stats::{BufferPoolStats, StatsSnapshot};