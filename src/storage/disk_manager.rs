//! Disk manager trait + two implementations.
//!
//! - `DiskManager` (trait): the contract the BufferPoolManager talks to.
//!   Five methods: `read_page`, `write_page`, `allocate_page`, `page_count`,
//!   `file_size`. All implementations are single-threaded; BPM serializes
//!   access via a `Mutex`.
//! - `FileDiskManager`: production backend. Reads/writes pages to a file,
//!   fsyncs on every write and allocate. Identical behavior to the
//!   pre-Q-06 `DiskManager`.
//! - `MemoryDiskManager`: in-memory backend. Holds pages in a `Vec`. Used
//!   for fast tests (BusTub parity) and as the substrate Q-07 wraps with
//!   fault injection.
//!
//! Q-07 will add `FaultInjectionDiskManager<D: DiskManager>` as a third
//! implementation that wraps another backend and injects I/O errors at
//! configured points.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::common::config::PAGE_SIZE;
use crate::common::{PageId, Result};
use crate::storage::page::Page;

/// Storage backend for the BufferPoolManager.
///
/// Implementations are single-threaded by contract; the BPM holds a
/// `Mutex` to serialize calls.
pub trait DiskManager: Send {
    /// Read a previously-allocated page.
    fn read_page(&mut self, page_id: PageId) -> Result<Page>;

    /// Overwrite a previously-allocated page. Durable on return for
    /// disk-backed implementations; in-memory backends may no-op.
    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()>;

    /// Allocate a new page (extends backing storage). Returns the new id.
    fn allocate_page(&mut self) -> Result<PageId>;

    /// Number of pages currently allocated.
    fn page_count(&self) -> u32;

    /// Total backing-storage size in bytes (`page_count * PAGE_SIZE`).
    fn file_size(&self) -> u64;
}

// ---------------------------------------------------------------------------
// FileDiskManager — durable on-disk backend
// ---------------------------------------------------------------------------

/// File-backed disk manager. Reads/writes pages via random-access I/O on a
/// single file; fsyncs after every write and allocate.
///
/// Layout: pages are concatenated at offset `page_id * PAGE_SIZE`. Page 0
/// is the first allocated page.
pub struct FileDiskManager {
    file: File,
    page_count: u32,
}

impl FileDiskManager {
    /// Create a new database file. Errors if the file already exists.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(Self { file, page_count: 0 })
    }

    /// Open an existing database file. Errors if it doesn't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let page_count = (file_size / PAGE_SIZE as u64) as u32;
        Ok(Self { file, page_count })
    }

    /// Open existing, or create if absent.
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self> {
        if path.as_ref().exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
    }
}

impl DiskManager for FileDiskManager {
    fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if page_id.0 >= self.page_count {
            return Err(crate::common::Error::PageNotFound(page_id.0));
        }
        let offset = (page_id.0 as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        let mut page = Page::new();
        self.file.read_exact(page.as_mut_slice())?;
        Ok(page)
    }

    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        if page_id.0 >= self.page_count {
            return Err(crate::common::Error::PageNotFound(page_id.0));
        }
        let offset = (page_id.0 as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page.as_slice())?;
        self.file.sync_all()?;
        Ok(())
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = PageId::new(self.page_count);
        let offset = (page_id.0 as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        let zeros = [0u8; PAGE_SIZE];
        self.file.write_all(&zeros)?;
        self.file.sync_all()?;
        self.page_count += 1;
        Ok(page_id)
    }

    fn page_count(&self) -> u32 {
        self.page_count
    }

    fn file_size(&self) -> u64 {
        (self.page_count as u64) * (PAGE_SIZE as u64)
    }
}

// ---------------------------------------------------------------------------
// MemoryDiskManager — in-memory backend for fast tests + Q-07 substrate
// ---------------------------------------------------------------------------

/// In-memory disk manager. Pages live as raw `[u8; PAGE_SIZE]` arrays in a
/// `Vec`. Used by Q-07's fault-injection wrapper and by BusTub-parity
/// benchmarks that need to isolate algorithmic cost from real disk I/O.
///
/// Persistence: none. Dropping the struct loses all data. There is no
/// `create` / `open` distinction — just `new()`.
///
/// Why raw byte arrays instead of `Vec<Page>`: `Page` is intentionally
/// non-`Clone` in production (forces explicit copying). Storing the bytes
/// directly avoids that constraint without weakening the production
/// invariant; `read_page` builds a fresh `Page` on each call.
pub struct MemoryDiskManager {
    pages: Vec<[u8; PAGE_SIZE]>,
}

impl MemoryDiskManager {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }
}

impl Default for MemoryDiskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskManager for MemoryDiskManager {
    fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if (page_id.0 as usize) >= self.pages.len() {
            return Err(crate::common::Error::PageNotFound(page_id.0));
        }
        let mut page = Page::new();
        page.as_mut_slice().copy_from_slice(&self.pages[page_id.0 as usize]);
        Ok(page)
    }

    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        if (page_id.0 as usize) >= self.pages.len() {
            return Err(crate::common::Error::PageNotFound(page_id.0));
        }
        self.pages[page_id.0 as usize].copy_from_slice(page.as_slice());
        Ok(())
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = PageId::new(self.pages.len() as u32);
        self.pages.push([0u8; PAGE_SIZE]);
        Ok(page_id)
    }

    fn page_count(&self) -> u32 {
        self.pages.len() as u32
    }

    fn file_size(&self) -> u64 {
        (self.pages.len() as u64) * (PAGE_SIZE as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ---- FileDiskManager ----

    #[test]
    fn file_create_new_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        assert_eq!(dm.page_count(), 0);
        assert_eq!(dm.file_size(), 0);
    }

    #[test]
    fn file_create_existing_fails() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        FileDiskManager::create(&path).unwrap();
        assert!(FileDiskManager::create(&path).is_err());
    }

    #[test]
    fn file_open_nonexistent_fails() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.db");
        assert!(FileDiskManager::open(&path).is_err());
    }

    #[test]
    fn file_allocate_and_read_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut dm = FileDiskManager::create(&path).unwrap();
        let page_id = dm.allocate_page().unwrap();
        assert_eq!(page_id, PageId::new(0));
        assert_eq!(dm.page_count(), 1);
        let page = dm.read_page(page_id).unwrap();
        assert_eq!(page.as_slice()[0], 0);
        assert_eq!(page.as_slice()[4095], 0);
    }

    #[test]
    fn file_write_and_read_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut dm = FileDiskManager::create(&path).unwrap();
        let page_id = dm.allocate_page().unwrap();
        let mut page = Page::new();
        page.as_mut_slice()[0] = 0xAB;
        page.as_mut_slice()[100] = 0xCD;
        page.as_mut_slice()[4095] = 0xEF;
        dm.write_page(page_id, &page).unwrap();
        let read_page = dm.read_page(page_id).unwrap();
        assert_eq!(read_page.as_slice()[0], 0xAB);
        assert_eq!(read_page.as_slice()[100], 0xCD);
        assert_eq!(read_page.as_slice()[4095], 0xEF);
    }

    #[test]
    fn file_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        {
            let mut dm = FileDiskManager::create(&path).unwrap();
            let page_id = dm.allocate_page().unwrap();
            let mut page = Page::new();
            page.as_mut_slice()[0] = 0x42;
            dm.write_page(page_id, &page).unwrap();
        }
        {
            let mut dm = FileDiskManager::open(&path).unwrap();
            assert_eq!(dm.page_count(), 1);
            let page = dm.read_page(PageId::new(0)).unwrap();
            assert_eq!(page.as_slice()[0], 0x42);
        }
    }

    #[test]
    fn file_multiple_pages() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut dm = FileDiskManager::create(&path).unwrap();
        for i in 0..10 {
            let page_id = dm.allocate_page().unwrap();
            assert_eq!(page_id.0, i);
            let mut page = Page::new();
            page.as_mut_slice()[0] = i as u8;
            dm.write_page(page_id, &page).unwrap();
        }
        assert_eq!(dm.page_count(), 10);
        assert_eq!(dm.file_size(), 10 * PAGE_SIZE as u64);
        for i in 0..10 {
            let page = dm.read_page(PageId::new(i)).unwrap();
            assert_eq!(page.as_slice()[0], i as u8);
        }
    }

    #[test]
    fn file_read_invalid_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut dm = FileDiskManager::create(&path).unwrap();
        dm.allocate_page().unwrap();
        assert!(dm.read_page(PageId::new(1)).is_err());
    }

    #[test]
    fn file_write_invalid_page() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut dm = FileDiskManager::create(&path).unwrap();
        let page = Page::new();
        assert!(dm.write_page(PageId::new(0), &page).is_err());
    }

    #[test]
    fn file_open_or_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        {
            let mut dm = FileDiskManager::open_or_create(&path).unwrap();
            assert_eq!(dm.page_count(), 0);
            dm.allocate_page().unwrap();
        }
        {
            let dm = FileDiskManager::open_or_create(&path).unwrap();
            assert_eq!(dm.page_count(), 1);
        }
    }

    // ---- MemoryDiskManager ----

    #[test]
    fn memory_starts_empty() {
        let dm = MemoryDiskManager::new();
        assert_eq!(dm.page_count(), 0);
        assert_eq!(dm.file_size(), 0);
    }

    #[test]
    fn memory_allocate_and_read_zeros() {
        let mut dm = MemoryDiskManager::new();
        let pid = dm.allocate_page().unwrap();
        assert_eq!(pid, PageId::new(0));
        let page = dm.read_page(pid).unwrap();
        assert!(page.as_slice().iter().all(|&b| b == 0));
    }

    #[test]
    fn memory_write_then_read_roundtrip() {
        let mut dm = MemoryDiskManager::new();
        let pid = dm.allocate_page().unwrap();
        let mut page = Page::new();
        page.as_mut_slice()[0] = 0xAA;
        page.as_mut_slice()[4095] = 0xBB;
        dm.write_page(pid, &page).unwrap();
        let read = dm.read_page(pid).unwrap();
        assert_eq!(read.as_slice()[0], 0xAA);
        assert_eq!(read.as_slice()[4095], 0xBB);
    }

    #[test]
    fn memory_unallocated_page_errors() {
        let mut dm = MemoryDiskManager::new();
        assert!(dm.read_page(PageId::new(0)).is_err());
        let p = Page::new();
        assert!(dm.write_page(PageId::new(0), &p).is_err());
    }

    #[test]
    fn memory_file_size_tracks_pages() {
        let mut dm = MemoryDiskManager::new();
        for _ in 0..5 {
            dm.allocate_page().unwrap();
        }
        assert_eq!(dm.page_count(), 5);
        assert_eq!(dm.file_size(), 5 * PAGE_SIZE as u64);
    }

    // ---- Trait-object usage (proves BPM can dyn-dispatch) ----

    #[test]
    fn both_backends_usable_as_trait_objects() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let backends: Vec<Box<dyn DiskManager>> = vec![
            Box::new(FileDiskManager::create(&path).unwrap()),
            Box::new(MemoryDiskManager::new()),
        ];
        for mut backend in backends {
            let pid = backend.allocate_page().unwrap();
            let page = Page::new();
            backend.write_page(pid, &page).unwrap();
            assert_eq!(backend.page_count(), 1);
        }
    }
}
