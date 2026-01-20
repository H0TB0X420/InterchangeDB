//! Disk Manager - low-level file I/O for database pages.
//!
//! The [`DiskManager`] handles all direct file operations:
//! - Reading and writing pages
//! - Allocating new pages
//! - Managing the database file

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::common::config::PAGE_SIZE;
use crate::common::{PageId, Result};
use crate::storage::page::Page;

/// Manages disk I/O for a single database file.
///
/// # File Layout
/// The database is stored as a single file with pages laid out sequentially:
/// ```text
/// ┌─────────┬─────────┬─────────┬─────────┬─────────┐
/// │ Page 0  │ Page 1  │ Page 2  │  ...    │ Page N  │
/// │ (4KB)   │ (4KB)   │ (4KB)   │         │ (4KB)   │
/// └─────────┴─────────┴─────────┴─────────┴─────────┘
/// Offset:  0      4096     8192    ...    N×4096
/// ```
///
/// Page N is located at file offset `N × PAGE_SIZE`.
///
/// # Thread Safety
/// `DiskManager` is **single-threaded**. The `BufferPoolManager` is responsible
/// for serializing access to the disk manager.
///
/// # Durability
/// All writes are followed by `fsync()` to ensure durability. This is
/// conservative and will be optimized when WAL group commit is implemented.
pub struct DiskManager {
    file: File,
    /// Number of pages in the file.
    page_count: u32,
}

impl DiskManager {
    /// Create a new database file.
    ///
    /// # Errors
    /// Returns an error if the file already exists or cannot be created.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        Ok(Self {
            file,
            page_count: 0,
        })
    }

    /// Open an existing database file.
    ///
    /// # Errors
    /// Returns an error if the file doesn't exist or cannot be opened.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        // Calculate page count from file size
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let page_count = (file_size / PAGE_SIZE as u64) as u32;

        Ok(Self { file, page_count })
    }

    /// Open an existing database file, or create if it doesn't exist.
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self> {
        if path.as_ref().exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
    }

    /// Read a page from disk.
    ///
    /// # Errors
    /// Returns `Error::PageNotFound` if the page doesn't exist.
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if page_id.0 >= self.page_count {
            return Err(crate::common::Error::PageNotFound(page_id.0));
        }

        let offset = (page_id.0 as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;

        let mut page = Page::new();
        self.file.read_exact(page.as_mut_slice())?;

        Ok(page)
    }

    /// Write a page to disk.
    ///
    /// The page must have been previously allocated with `allocate_page()`.
    ///
    /// # Durability
    /// This method calls `fsync()` after writing to ensure the data is
    /// persisted to disk.
    ///
    /// # Errors
    /// Returns `Error::PageNotFound` if the page hasn't been allocated.
    pub fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        if page_id.0 >= self.page_count {
            return Err(crate::common::Error::PageNotFound(page_id.0));
        }

        let offset = (page_id.0 as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page.as_slice())?;
        self.file.sync_all()?; // fsync for durability

        Ok(())
    }

    /// Allocate a new page on disk.
    ///
    /// Returns the `PageId` of the newly allocated page. The page is
    /// initialized with zeros.
    ///
    /// # Durability
    /// This method extends the file and calls `fsync()` to ensure the
    /// allocation is durable.
    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = PageId::new(self.page_count);

        // Extend file with a zeroed page
        let offset = (page_id.0 as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;

        let zeros = [0u8; PAGE_SIZE];
        self.file.write_all(&zeros)?;
        self.file.sync_all()?;

        self.page_count += 1;
        Ok(page_id)
    }

    /// Get the number of pages in the database.
    #[inline]
    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Get the total size of the database file in bytes.
    #[inline]
    pub fn file_size(&self) -> u64 {
        (self.page_count as u64) * (PAGE_SIZE as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_new_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let dm = DiskManager::create(&path).unwrap();
        assert_eq!(dm.page_count(), 0);
        assert_eq!(dm.file_size(), 0);
    }

    #[test]
    fn test_create_existing_fails() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        DiskManager::create(&path).unwrap();
        assert!(DiskManager::create(&path).is_err());
    }

    #[test]
    fn test_open_nonexistent_fails() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.db");

        assert!(DiskManager::open(&path).is_err());
    }

    #[test]
    fn test_allocate_and_read_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut dm = DiskManager::create(&path).unwrap();

        // Allocate first page
        let page_id = dm.allocate_page().unwrap();
        assert_eq!(page_id, PageId::new(0));
        assert_eq!(dm.page_count(), 1);

        // Read it back (should be zeros)
        let page = dm.read_page(page_id).unwrap();
        assert_eq!(page.as_slice()[0], 0);
        assert_eq!(page.as_slice()[4095], 0);
    }

    #[test]
    fn test_write_and_read_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut dm = DiskManager::create(&path).unwrap();
        let page_id = dm.allocate_page().unwrap();

        // Write some data
        let mut page = Page::new();
        page.as_mut_slice()[0] = 0xAB;
        page.as_mut_slice()[100] = 0xCD;
        page.as_mut_slice()[4095] = 0xEF;

        dm.write_page(page_id, &page).unwrap();

        // Read it back
        let read_page = dm.read_page(page_id).unwrap();
        assert_eq!(read_page.as_slice()[0], 0xAB);
        assert_eq!(read_page.as_slice()[100], 0xCD);
        assert_eq!(read_page.as_slice()[4095], 0xEF);
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create and write
        {
            let mut dm = DiskManager::create(&path).unwrap();
            let page_id = dm.allocate_page().unwrap();

            let mut page = Page::new();
            page.as_mut_slice()[0] = 0x42;
            dm.write_page(page_id, &page).unwrap();
        }

        // Reopen and verify
        {
            let mut dm = DiskManager::open(&path).unwrap();
            assert_eq!(dm.page_count(), 1);

            let page = dm.read_page(PageId::new(0)).unwrap();
            assert_eq!(page.as_slice()[0], 0x42);
        }
    }

    #[test]
    fn test_multiple_pages() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut dm = DiskManager::create(&path).unwrap();

        // Allocate and write 10 pages
        for i in 0..10 {
            let page_id = dm.allocate_page().unwrap();
            assert_eq!(page_id.0, i);

            let mut page = Page::new();
            page.as_mut_slice()[0] = i as u8;
            dm.write_page(page_id, &page).unwrap();
        }

        assert_eq!(dm.page_count(), 10);
        assert_eq!(dm.file_size(), 10 * PAGE_SIZE as u64);

        // Read them all back
        for i in 0..10 {
            let page = dm.read_page(PageId::new(i)).unwrap();
            assert_eq!(page.as_slice()[0], i as u8);
        }
    }

    #[test]
    fn test_read_invalid_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut dm = DiskManager::create(&path).unwrap();
        dm.allocate_page().unwrap(); // Page 0 exists

        // Page 1 doesn't exist
        let result = dm.read_page(PageId::new(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_write_invalid_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut dm = DiskManager::create(&path).unwrap();

        // No pages allocated yet
        let page = Page::new();
        let result = dm.write_page(PageId::new(0), &page);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_or_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // First call creates
        {
            let mut dm = DiskManager::open_or_create(&path).unwrap();
            assert_eq!(dm.page_count(), 0);
            dm.allocate_page().unwrap();
        }

        // Second call opens existing
        {
            let dm = DiskManager::open_or_create(&path).unwrap();
            assert_eq!(dm.page_count(), 1);
        }
    }
}
