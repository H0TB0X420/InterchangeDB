//! Configuration constants for InterchangeDB.

/// Size of a page in bytes (4KB).
///
/// This value is chosen to match:
/// - OS page size on most systems (4096 bytes)
/// - Common database page sizes (PostgreSQL uses 8KB, but 4KB is also standard)
/// - BusTub's page size
///
/// # Memory Layout
/// With 4KB pages and 32-bit PageIds:
/// - Max pages: 2^32 = 4,294,967,296 pages
/// - Max database size: 4,294,967,296 × 4KB = 16TB
///
/// # Alignment
/// Pages are aligned to 4096 bytes for efficient Direct I/O (O_DIRECT).
pub const PAGE_SIZE: usize = 4096;

/// Maximum number of pages with u32 PageId.
pub const MAX_PAGES: u64 = (u32::MAX as u64) + 1;

/// Maximum theoretical database size in bytes.
pub const MAX_DB_SIZE_BYTES: u64 = MAX_PAGES * PAGE_SIZE as u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_size_is_power_of_two() {
        assert!(PAGE_SIZE.is_power_of_two());
        assert_eq!(PAGE_SIZE, 4096);
    }

    #[test]
    fn test_max_db_size() {
        // 16TB = 16 * 1024^4 bytes
        let expected = 16 * 1024u64 * 1024 * 1024 * 1024;
        assert_eq!(MAX_DB_SIZE_BYTES, expected);
    }
}