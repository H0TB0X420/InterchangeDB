//! Page types and layout.
//!
//! This module contains:
//! - [`Page`] - The raw 4KB data container
//! - [`PageHeader`] - Metadata at the start of every page
//! - [`PageType`] - Discriminator for different page formats

#[allow(clippy::module_inception)]
mod page;
mod page_header;

pub use page::Page;
pub use page_header::{PageHeader, PageType};