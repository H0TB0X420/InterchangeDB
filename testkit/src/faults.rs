//! `FaultInjectionDiskManager<D>` — a `DiskManager` wrapper that injects
//! faults on demand: scheduled I/O errors and one-shot torn node-page writes.
//! Consumed by `tests/fault_injection_test.rs`, `tests/torn_page_recovery_test.rs`,
//! and any test/bench that wants to drive the BPM's failure paths.
//!
//! Configure faults *before* handing the wrapper to the BPM:
//!
//! ```ignore
//! let fault = FaultInjectionDiskManager::new(MemoryDiskManager::new())
//!     .with_write_errors(1);
//! let bpm = BufferPoolManager::new(16, fault);
//! ```
//!
//! Once the BPM owns it, the configured counter is consumed one-per-call
//! and the next operation of that kind returns `Error::Io`. Counters are
//! `AtomicUsize` so the wrapper is `Send` regardless of whether the BPM
//! actually shares it across threads (it doesn't — the BPM serializes
//! disk-manager access via `Mutex`).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use interchangedb::common::{Error, PageId, Result};
use interchangedb::storage::DiskManager;
use interchangedb::{Page, PageHeader, PageType};

pub struct FaultInjectionDiskManager<D: DiskManager> {
    inner: D,
    pending_read_errors: AtomicUsize,
    pending_write_errors: AtomicUsize,
    pending_allocate_errors: AtomicUsize,
    /// When armed, the next write of a B-tree node page persists only its first
    /// `torn_node_keep` bytes, zeroing the rest (a torn write — see
    /// `with_torn_next_node_write`).
    torn_node_armed: AtomicBool,
    torn_node_keep: AtomicUsize,
}

impl<D: DiskManager> FaultInjectionDiskManager<D> {
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            pending_read_errors: AtomicUsize::new(0),
            pending_write_errors: AtomicUsize::new(0),
            pending_allocate_errors: AtomicUsize::new(0),
            torn_node_armed: AtomicBool::new(false),
            torn_node_keep: AtomicUsize::new(0),
        }
    }

    /// Schedule the next `n` `read_page` calls to fail with `Error::Io`.
    pub fn with_read_errors(self, n: usize) -> Self {
        self.pending_read_errors.store(n, Ordering::SeqCst);
        self
    }

    /// Schedule the next `n` `write_page` calls to fail with `Error::Io`.
    pub fn with_write_errors(self, n: usize) -> Self {
        self.pending_write_errors.store(n, Ordering::SeqCst);
        self
    }

    /// Schedule the next `n` `allocate_page` calls to fail with `Error::Io`.
    pub fn with_allocate_errors(self, n: usize) -> Self {
        self.pending_allocate_errors.store(n, Ordering::SeqCst);
        self
    }

    /// Arm a one-shot **torn write** on the next B-tree node page written.
    ///
    /// Models a write interrupted mid-page (power loss, crash): only the first
    /// `keep` bytes reach disk, the rest read back as zero (an erased/never-
    /// written tail). The preserved header still carries the new, non-zero
    /// checksum, so a populated node's persisted body disagrees with its
    /// checksum — which a correct reader must detect rather than silently
    /// accept. Use `keep = PageHeader::SIZE` to preserve a valid header (page
    /// type + checksum) while zeroing the body, the most direct way to force a
    /// detectable mismatch.
    ///
    /// Only B-tree node pages (`BTreeLeaf`/`BTreeInternal`) are torn; other
    /// writes (the header page, raw data) pass through, so the tear lands on a
    /// checksummed structure and stays armed until one is written.
    pub fn with_torn_next_node_write(self, keep: usize) -> Self {
        assert!(
            keep <= interchangedb::PAGE_SIZE,
            "keep must be within a page"
        );
        self.torn_node_keep.store(keep, Ordering::SeqCst);
        self.torn_node_armed.store(true, Ordering::SeqCst);
        self
    }

    /// Number of read errors still queued. For assertions.
    pub fn pending_reads(&self) -> usize {
        self.pending_read_errors.load(Ordering::SeqCst)
    }

    /// Whether a torn-write injection is still armed (no node page written yet).
    pub fn torn_write_armed(&self) -> bool {
        self.torn_node_armed.load(Ordering::SeqCst)
    }
}

fn consume_one(counter: &AtomicUsize) -> bool {
    // Decrement only if positive. SeqCst is fine — fault injection isn't
    // a hot path and the contention model is single-threaded under the
    // BPM's Mutex anyway.
    let current = counter.load(Ordering::SeqCst);
    if current == 0 {
        return false;
    }
    counter.fetch_sub(1, Ordering::SeqCst);
    true
}

fn injected_error(op: &str) -> Error {
    Error::Io(std::io::Error::other(format!(
        "FaultInjection: injected failure on {}",
        op
    )))
}

impl<D: DiskManager> DiskManager for FaultInjectionDiskManager<D> {
    fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if consume_one(&self.pending_read_errors) {
            return Err(injected_error("read_page"));
        }
        self.inner.read_page(page_id)
    }

    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        if consume_one(&self.pending_write_errors) {
            return Err(injected_error("write_page"));
        }
        // Torn-write injection: only B-tree node pages are checksummed
        // structures worth tearing; everything else passes through and leaves
        // the tear armed.
        if self.torn_node_armed.load(Ordering::SeqCst) {
            let page_type = PageType::from_u8(page.as_slice()[PageHeader::OFFSET_PAGE_TYPE]);
            if page_type == PageType::BTreeLeaf || page_type == PageType::BTreeInternal {
                let keep = self.torn_node_keep.load(Ordering::SeqCst);
                self.torn_node_armed.store(false, Ordering::SeqCst);
                let mut torn = Page::new();
                torn.as_mut_slice()[..keep].copy_from_slice(&page.as_slice()[..keep]);
                return self.inner.write_page(page_id, &torn);
            }
        }
        self.inner.write_page(page_id, page)
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        if consume_one(&self.pending_allocate_errors) {
            return Err(injected_error("allocate_page"));
        }
        self.inner.allocate_page()
    }

    fn page_count(&self) -> u32 {
        self.inner.page_count()
    }

    fn file_size(&self) -> u64 {
        self.inner.file_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use interchangedb::storage::MemoryDiskManager;

    #[test]
    fn no_injection_is_pass_through() {
        let mut dm = FaultInjectionDiskManager::new(MemoryDiskManager::new());
        let pid = dm.allocate_page().unwrap();
        let mut p = Page::new();
        p.as_mut_slice()[0] = 0xAB;
        dm.write_page(pid, &p).unwrap();
        let read = dm.read_page(pid).unwrap();
        assert_eq!(read.as_slice()[0], 0xAB);
    }

    #[test]
    fn write_error_count_is_consumed_and_then_passes() {
        let mut dm = FaultInjectionDiskManager::new(MemoryDiskManager::new()).with_write_errors(2);
        let pid = dm.allocate_page().unwrap();
        let p = Page::new();
        assert!(matches!(dm.write_page(pid, &p), Err(Error::Io(_))));
        assert!(matches!(dm.write_page(pid, &p), Err(Error::Io(_))));
        // Third call succeeds — counter exhausted.
        assert!(dm.write_page(pid, &p).is_ok());
    }

    #[test]
    fn read_error_count_is_independent_of_writes() {
        let mut dm = FaultInjectionDiskManager::new(MemoryDiskManager::new()).with_read_errors(1);
        let pid = dm.allocate_page().unwrap();
        let p = Page::new();
        dm.write_page(pid, &p).unwrap();
        assert!(matches!(dm.read_page(pid), Err(Error::Io(_))));
        assert!(dm.read_page(pid).is_ok());
    }

    #[test]
    fn allocate_error_blocks_first_call() {
        let mut dm =
            FaultInjectionDiskManager::new(MemoryDiskManager::new()).with_allocate_errors(1);
        assert!(matches!(dm.allocate_page(), Err(Error::Io(_))));
        assert!(dm.allocate_page().is_ok());
    }

    #[test]
    fn pending_reads_reports_remaining_count() {
        let dm = FaultInjectionDiskManager::new(MemoryDiskManager::new()).with_read_errors(3);
        assert_eq!(dm.pending_reads(), 3);
    }

    /// A node page persisted with a torn write keeps its first `keep` bytes and
    /// reads back zero past that point, one-shot.
    #[test]
    fn torn_node_write_zeros_body_past_keep() {
        let mut dm = FaultInjectionDiskManager::new(MemoryDiskManager::new())
            .with_torn_next_node_write(PageHeader::SIZE);
        let pid = dm.allocate_page().unwrap();

        let mut node = Page::new();
        node.as_mut_slice()[PageHeader::OFFSET_PAGE_TYPE] = PageType::BTreeLeaf as u8;
        for byte in node.as_mut_slice()[PageHeader::SIZE..].iter_mut() {
            *byte = 0xAB;
        }

        assert!(dm.torn_write_armed());
        dm.write_page(pid, &node).unwrap();
        assert!(!dm.torn_write_armed(), "tear is one-shot");

        let read = dm.read_page(pid).unwrap();
        assert_eq!(
            read.as_slice()[PageHeader::OFFSET_PAGE_TYPE],
            PageType::BTreeLeaf as u8,
            "header is preserved"
        );
        assert!(
            read.as_slice()[PageHeader::SIZE..].iter().all(|&b| b == 0),
            "body past keep must be torn to zero"
        );

        // Second write is intact — the injection was consumed.
        dm.write_page(pid, &node).unwrap();
        let read2 = dm.read_page(pid).unwrap();
        assert!(read2.as_slice()[PageHeader::SIZE..].contains(&0xAB));
    }

    /// Non-node writes pass through untouched and leave the tear armed for a
    /// later node write.
    #[test]
    fn torn_write_skips_non_node_pages() {
        let mut dm = FaultInjectionDiskManager::new(MemoryDiskManager::new())
            .with_torn_next_node_write(PageHeader::SIZE);
        let pid = dm.allocate_page().unwrap();

        let mut data = Page::new();
        data.as_mut_slice()[PageHeader::OFFSET_PAGE_TYPE] = PageType::Data as u8;
        for byte in data.as_mut_slice()[PageHeader::SIZE..].iter_mut() {
            *byte = 0xCD;
        }

        dm.write_page(pid, &data).unwrap();
        assert!(
            dm.torn_write_armed(),
            "a non-node write must not consume the tear"
        );

        let read = dm.read_page(pid).unwrap();
        assert!(
            read.as_slice()[PageHeader::SIZE..]
                .iter()
                .all(|&b| b == 0xCD),
            "non-node page is written intact"
        );
    }
}
