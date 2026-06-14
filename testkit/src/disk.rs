//! `DiskManager` axis: the three implementations (the registry's leaves) and
//! [`assert_contract`] — the in-session storage contract every backend must
//! satisfy. Driven from [`crate::for_each_disk`].
//!
//! The `fault` entry is `FaultInjectionDiskManager` with **no faults armed**:
//! running the same contract over it proves the wrapper is a transparent
//! pass-through. (File-specific *persistence across reopen* is not a universal
//! contract and stays in a File-only test.)

use interchangedb::storage::{DiskManager, FileDiskManager, MemoryDiskManager};
use interchangedb::{Page, PAGE_SIZE};

use crate::faults::FaultInjectionDiskManager;
use crate::handles::Built;

pub fn memory() -> Built<MemoryDiskManager> {
    Built::new(MemoryDiskManager::new())
}

pub fn file() -> Built<FileDiskManager> {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("disk.db")).unwrap();
    Built::with_dir(dm, dir)
}

pub fn fault() -> Built<FaultInjectionDiskManager<MemoryDiskManager>> {
    Built::new(FaultInjectionDiskManager::new(MemoryDiskManager::new()))
}

/// Assert the universal `DiskManager` contract on a fresh manager.
pub fn assert_contract<D: DiskManager>(name: &str, dm: &mut D) {
    // 1. Allocation yields distinct ids; page_count and file_size track it.
    let a = dm.allocate_page().unwrap();
    let b = dm.allocate_page().unwrap();
    assert_ne!(a, b, "{name}: allocated page ids must be distinct");
    assert_eq!(
        dm.page_count(),
        2,
        "{name}: page_count must track allocations"
    );
    assert_eq!(
        dm.file_size(),
        2 * PAGE_SIZE as u64,
        "{name}: file_size must be page_count * PAGE_SIZE"
    );

    // 2. A freshly allocated page reads back zeroed.
    assert!(
        dm.read_page(a).unwrap().as_slice().iter().all(|&x| x == 0),
        "{name}: a freshly allocated page must read as zeros"
    );

    // 3. Read-after-write round-trips (first and last byte).
    let mut page = Page::new();
    page.as_mut_slice()[0] = 0xAB;
    page.as_mut_slice()[PAGE_SIZE - 1] = 0xCD;
    dm.write_page(b, &page).unwrap();
    let read = dm.read_page(b).unwrap();
    assert_eq!(read.as_slice()[0], 0xAB, "{name}: read-after-write byte 0");
    assert_eq!(
        read.as_slice()[PAGE_SIZE - 1],
        0xCD,
        "{name}: read-after-write last byte"
    );

    // 4. Overwrite — last write wins.
    let mut page2 = Page::new();
    page2.as_mut_slice()[0] = 0x11;
    dm.write_page(b, &page2).unwrap();
    assert_eq!(
        dm.read_page(b).unwrap().as_slice()[0],
        0x11,
        "{name}: overwrite must win"
    );

    // 5. A write to one page leaves the others untouched.
    assert!(
        dm.read_page(a).unwrap().as_slice().iter().all(|&x| x == 0),
        "{name}: an unrelated page must be untouched by a write"
    );
}
