//! Common types and utilities shared across InterchangeDB.
//!
//! This module contains fundamental primitives used throughout the codebase:
//! - Configuration constants
//! - Error types
//! - Identifiers (PageId, FrameId)

pub mod config;
pub mod error;
mod frame_id;
mod page_id;

pub use error::{Error, Result};
pub use frame_id::FrameId;
pub use page_id::PageId;