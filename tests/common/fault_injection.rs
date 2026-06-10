//! `FaultInjectionDiskManager<D>` — a `DiskManager` wrapper that injects
//! I/O errors on demand. Used by `tests/fault_injection_test.rs` and any
//! later test that wants to drive the BPM's failure paths.
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

use std::sync::atomic::{AtomicUsize, Ordering};

use interchangedb::common::{Error, PageId, Result};
use interchangedb::storage::DiskManager;
use interchangedb::Page;

pub struct FaultInjectionDiskManager<D: DiskManager> {
    inner: D,
    pending_read_errors: AtomicUsize,
    pending_write_errors: AtomicUsize,
    pending_allocate_errors: AtomicUsize,
}

impl<D: DiskManager> FaultInjectionDiskManager<D> {
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            pending_read_errors: AtomicUsize::new(0),
            pending_write_errors: AtomicUsize::new(0),
            pending_allocate_errors: AtomicUsize::new(0),
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

    /// Number of read errors still queued. For assertions.
    pub fn pending_reads(&self) -> usize {
        self.pending_read_errors.load(Ordering::SeqCst)
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
}
