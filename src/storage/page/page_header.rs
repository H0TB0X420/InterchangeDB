//! Page header and type definitions.
//!
//! Every page starts with a [`PageHeader`] containing metadata:
//! - [`PageType`] discriminator
//! - CRC32 checksum for integrity
//! - LSN for WAL/recovery

/// Type of page stored on disk.
///
/// Uses `#[repr(u8)]` to guarantee a 1-byte representation for serialization.
#[repr(u8)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Uninitialized or corrupted page.
    #[default]
    Invalid = 0,
    /// Generic data page.
    Data = 1,
    /// B-tree internal (non-leaf) node.
    BTreeInternal = 2,
    /// B-tree leaf node.
    BTreeLeaf = 3,
    /// Page on the free list.
    Free = 4,
}

impl PageType {
    /// Convert from u8, returning Invalid for unknown values.
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => PageType::Data,
            2 => PageType::BTreeInternal,
            3 => PageType::BTreeLeaf,
            4 => PageType::Free,
            _ => PageType::Invalid,
        }
    }
}

/// Metadata stored at the beginning of every page.
///
/// # Layout (13 bytes)
/// ```text
/// Offset  Size  Field
/// ------  ----  -----
/// 0       1     page_type (PageType as u8)
/// 1       4     checksum (CRC32, little-endian)
/// 5       8     lsn (Log Sequence Number, little-endian)
/// ```
///
/// # Checksum
/// The checksum is computed over the entire page with the checksum field
/// itself set to zero. This allows verification without special handling.
///
/// # LSN (Log Sequence Number)
/// Included from day 1 for WAL/MVCC forward-compatibility, even though
/// it won't be used until the WAL implementation (Week 8).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    /// Type of this page.
    pub page_type: PageType,
    /// CRC32 checksum of the page contents.
    pub checksum: u32,
    /// Log Sequence Number of last modification.
    pub lsn: u64,
}

impl PageHeader {
    /// Size of the header in bytes.
    pub const SIZE: usize = 13;

    /// Offset of each field within the header.
    pub const OFFSET_PAGE_TYPE: usize = 0;
    pub const OFFSET_CHECKSUM: usize = 1;
    pub const OFFSET_LSN: usize = 5;

    /// Create a new header with the given page type.
    ///
    /// Checksum and LSN are initialized to zero.
    pub fn new(page_type: PageType) -> Self {
        Self {
            page_type,
            checksum: 0,
            lsn: 0,
        }
    }

    /// Read a header from the beginning of a byte slice.
    ///
    /// # Panics
    /// Panics if `data.len() < PageHeader::SIZE`.
    pub fn from_bytes(data: &[u8]) -> Self {
        assert!(data.len() >= Self::SIZE, "buffer too small for PageHeader");

        let page_type = PageType::from_u8(data[Self::OFFSET_PAGE_TYPE]);

        let checksum = u32::from_le_bytes([
            data[Self::OFFSET_CHECKSUM],
            data[Self::OFFSET_CHECKSUM + 1],
            data[Self::OFFSET_CHECKSUM + 2],
            data[Self::OFFSET_CHECKSUM + 3],
        ]);

        let lsn = u64::from_le_bytes([
            data[Self::OFFSET_LSN],
            data[Self::OFFSET_LSN + 1],
            data[Self::OFFSET_LSN + 2],
            data[Self::OFFSET_LSN + 3],
            data[Self::OFFSET_LSN + 4],
            data[Self::OFFSET_LSN + 5],
            data[Self::OFFSET_LSN + 6],
            data[Self::OFFSET_LSN + 7],
        ]);

        Self {
            page_type,
            checksum,
            lsn,
        }
    }

    /// Write this header to the beginning of a byte slice.
    ///
    /// # Panics
    /// Panics if `data.len() < PageHeader::SIZE`.
    pub fn write_to(&self, data: &mut [u8]) {
        assert!(data.len() >= Self::SIZE, "buffer too small for PageHeader");

        data[Self::OFFSET_PAGE_TYPE] = self.page_type as u8;

        let checksum_bytes = self.checksum.to_le_bytes();
        data[Self::OFFSET_CHECKSUM..Self::OFFSET_CHECKSUM + 4].copy_from_slice(&checksum_bytes);

        let lsn_bytes = self.lsn.to_le_bytes();
        data[Self::OFFSET_LSN..Self::OFFSET_LSN + 8].copy_from_slice(&lsn_bytes);
    }

    /// Compute CRC32 checksum of a page.
    ///
    /// The checksum is computed with the checksum field (bytes 1-4) zeroed out,
    /// so the checksum doesn't include itself.
    ///
    /// # Arguments
    /// * `page_data` - The full page data (PAGE_SIZE bytes)
    ///
    /// # Returns
    /// CRC32 checksum as u32
    pub fn compute_checksum(page_data: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();

        // Hash bytes before checksum field (just byte 0: page_type)
        hasher.update(&page_data[..Self::OFFSET_CHECKSUM]);

        // Skip checksum field by feeding zeros instead
        hasher.update(&[0u8; 4]);

        // Hash bytes after checksum field (from LSN to end of page)
        hasher.update(&page_data[Self::OFFSET_CHECKSUM + 4..]);

        hasher.finalize()
    }

    /// Verify that the stored checksum matches the computed checksum.
    ///
    /// # Arguments
    /// * `page_data` - The full page data (PAGE_SIZE bytes)
    ///
    /// # Returns
    /// `true` if checksum is valid, `false` otherwise
    pub fn verify_checksum(&self, page_data: &[u8]) -> bool {
        self.checksum == Self::compute_checksum(page_data)
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::PAGE_SIZE;

    // --- PageType tests ---

    #[test]
    fn test_page_type_from_u8() {
        assert_eq!(PageType::from_u8(0), PageType::Invalid);
        assert_eq!(PageType::from_u8(1), PageType::Data);
        assert_eq!(PageType::from_u8(2), PageType::BTreeInternal);
        assert_eq!(PageType::from_u8(3), PageType::BTreeLeaf);
        assert_eq!(PageType::from_u8(4), PageType::Free);
        assert_eq!(PageType::from_u8(255), PageType::Invalid);
    }

    #[test]
    fn test_page_type_default() {
        assert_eq!(PageType::default(), PageType::Invalid);
    }

    // --- PageHeader tests ---

    #[test]
    fn test_page_header_new() {
        let header = PageHeader::new(PageType::Data);
        assert_eq!(header.page_type, PageType::Data);
        assert_eq!(header.checksum, 0);
        assert_eq!(header.lsn, 0);
    }

    #[test]
    fn test_page_header_default() {
        let header = PageHeader::default();
        assert_eq!(header.page_type, PageType::Invalid);
        assert_eq!(header.checksum, 0);
        assert_eq!(header.lsn, 0);
    }

    #[test]
    fn test_page_header_roundtrip() {
        let original = PageHeader {
            page_type: PageType::BTreeLeaf,
            checksum: 0xDEADBEEF,
            lsn: 0x123456789ABCDEF0,
        };

        let mut buffer = [0u8; PageHeader::SIZE];
        original.write_to(&mut buffer);

        let recovered = PageHeader::from_bytes(&buffer);
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_page_header_byte_layout() {
        let header = PageHeader {
            page_type: PageType::Data,
            checksum: 0x04030201, // Little-endian: 01 02 03 04
            lsn: 0x0807060504030201, // Little-endian: 01 02 03 04 05 06 07 08
        };

        let mut buffer = [0u8; PageHeader::SIZE];
        header.write_to(&mut buffer);

        // Verify exact byte layout
        assert_eq!(buffer[0], 1); // PageType::Data
        assert_eq!(buffer[1], 0x01); // checksum byte 0 (LSB)
        assert_eq!(buffer[2], 0x02);
        assert_eq!(buffer[3], 0x03);
        assert_eq!(buffer[4], 0x04); // checksum byte 3 (MSB)
        assert_eq!(buffer[5], 0x01); // lsn byte 0 (LSB)
        assert_eq!(buffer[12], 0x08); // lsn byte 7 (MSB)
    }

    // --- Checksum tests ---

    #[test]
    fn test_checksum_deterministic() {
        let mut page_data = [0u8; PAGE_SIZE];
        page_data[100] = 0xAB;
        page_data[1000] = 0xCD;

        let checksum1 = PageHeader::compute_checksum(&page_data);
        let checksum2 = PageHeader::compute_checksum(&page_data);

        assert_eq!(checksum1, checksum2);
        assert_ne!(checksum1, 0);
    }

    #[test]
    fn test_checksum_changes_with_data() {
        let mut page1 = [0u8; PAGE_SIZE];
        let mut page2 = [0u8; PAGE_SIZE];

        page1[500] = 0xFF;
        page2[500] = 0xFE;

        let checksum1 = PageHeader::compute_checksum(&page1);
        let checksum2 = PageHeader::compute_checksum(&page2);

        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_checksum_ignores_checksum_field() {
        let mut page_data = [0u8; PAGE_SIZE];
        page_data[100] = 0xAB;

        let checksum1 = PageHeader::compute_checksum(&page_data);

        // Write different value in checksum field (bytes 1-4)
        page_data[1] = 0xFF;
        page_data[2] = 0xFF;
        page_data[3] = 0xFF;
        page_data[4] = 0xFF;

        let checksum2 = PageHeader::compute_checksum(&page_data);

        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_checksum_verify() {
        let mut page_data = [0u8; PAGE_SIZE];
        page_data[100] = 0xAB;

        let checksum = PageHeader::compute_checksum(&page_data);
        let header = PageHeader {
            page_type: PageType::Data,
            checksum,
            lsn: 0,
        };

        assert!(header.verify_checksum(&page_data));

        // Corrupt the page
        page_data[100] = 0xFF;
        assert!(!header.verify_checksum(&page_data));
    }
}