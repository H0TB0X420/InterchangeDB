//! Page - the fundamental 4KB unit of storage.
//!
//! A [`Page`] is a raw 4KB byte array that serves as the unit of I/O
//! between disk and memory. Pages are stored in [`Frame`]s within the
//! buffer pool.

use crate::common::config::PAGE_SIZE;

use super::page_header::PageHeader;

/// A page of data (4KB, 4KB-aligned).
///
/// This is the fundamental unit of I/O between disk and memory.
/// The buffer pool manages these in frames.
///
/// # Memory Layout
/// - Size: 4096 bytes (4KB)
/// - Alignment: 4096 bytes (for efficient Direct I/O with O_DIRECT)
///
/// # Why 4KB Alignment?
/// Operating systems and storage devices work most efficiently with aligned I/O:
/// - SSDs have 4KB pages internally
/// - OS page cache uses 4KB pages
/// - O_DIRECT I/O requires alignment to sector size
///
/// # Clone Implementation
/// `Page` does NOT implement `Clone` in production code to match production
/// database behavior (copying 4KB is expensive and should be explicit).
/// A `#[cfg(test)]` Clone is provided for tests.
///
/// # Example
/// ```
/// use interchangedb::storage::page::Page;
///
/// let mut page = Page::new();
/// page.as_mut_slice()[0] = 0xFF;
/// assert_eq!(page.as_slice()[0], 0xFF);
/// ```
#[repr(align(4096))]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a new zeroed page.
    #[inline]
    pub fn new() -> Self {
        Self {
            data: [0u8; PAGE_SIZE],
        }
    }

    /// Get immutable slice of page data.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable slice of page data.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Zero out the entire page.
    pub fn reset(&mut self) {
        self.data.fill(0);
    }

    /// Get the size of a page.
    #[inline]
    pub const fn size() -> usize {
        PAGE_SIZE
    }

    /// Read the page header.
    pub fn header(&self) -> PageHeader {
        PageHeader::from_bytes(&self.data)
    }

    /// Write a page header.
    pub fn set_header(&mut self, header: &PageHeader) {
        header.write_to(&mut self.data);
    }

    /// Compute and store checksum in the header.
    ///
    /// Call this after all modifications to the page are complete.
    pub fn update_checksum(&mut self) {
        let checksum = PageHeader::compute_checksum(&self.data);
        let checksum_bytes = checksum.to_le_bytes();
        self.data[PageHeader::OFFSET_CHECKSUM..PageHeader::OFFSET_CHECKSUM + 4]
            .copy_from_slice(&checksum_bytes);
    }

    /// Verify the page checksum is valid.
    pub fn verify_checksum(&self) -> bool {
        self.header().verify_checksum(&self.data)
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

// Clone only available in tests - forces explicit copying in production
#[cfg(test)]
impl Clone for Page {
    fn clone(&self) -> Self {
        let mut new_page = Page::new();
        new_page.data.copy_from_slice(&self.data);
        new_page
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_size_and_alignment() {
        assert_eq!(std::mem::size_of::<Page>(), PAGE_SIZE);
        assert_eq!(std::mem::size_of::<Page>(), 4096);
        assert_eq!(std::mem::align_of::<Page>(), 4096);
    }

    #[test]
    fn test_page_new() {
        let page = Page::new();
        assert_eq!(page.as_slice()[0], 0);
        assert_eq!(page.as_slice()[4095], 0);
    }

    #[test]
    fn test_page_read_write() {
        let mut page = Page::new();

        page.as_mut_slice()[0] = 0xFF;
        page.as_mut_slice()[100] = 0xAB;
        page.as_mut_slice()[4095] = 0xCD;

        assert_eq!(page.as_slice()[0], 0xFF);
        assert_eq!(page.as_slice()[100], 0xAB);
        assert_eq!(page.as_slice()[4095], 0xCD);
    }

    #[test]
    fn test_page_reset() {
        let mut page = Page::new();
        page.as_mut_slice()[0] = 0xFF;
        page.as_mut_slice()[100] = 0xAB;

        page.reset();

        assert_eq!(page.as_slice()[0], 0);
        assert_eq!(page.as_slice()[100], 0);
    }

    #[test]
    fn test_page_clone_in_tests() {
        let mut page = Page::new();
        page.as_mut_slice()[0] = 0xAB;

        let cloned = page.clone();
        assert_eq!(cloned.as_slice()[0], 0xAB);
        assert_eq!(page.as_slice()[0], 0xAB);
    }
}