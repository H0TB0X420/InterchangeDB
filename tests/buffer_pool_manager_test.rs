//! Buffer Pool Manager Tests
//!
//! These tests follow BusTub's buffer_pool_manager_test.cpp closely.
//! Reference: test/buffer/buffer_pool_manager_test.cpp

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::PageId;
use interchangedb::storage::DiskManager;
use std::sync::Arc;
use tempfile::tempdir;

const FRAMES: usize = 10;

fn create_bpm(pool_size: usize) -> (BufferPoolManager, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = DiskManager::create(&path).unwrap();
    (BufferPoolManager::new(pool_size, dm), dir)
}

/// Helper to write a string to page data.
fn copy_string(data: &mut [u8], s: &str) {
    let bytes = s.as_bytes();
    data[..bytes.len()].copy_from_slice(bytes);
    data[bytes.len()] = 0; // null terminator
}

/// Helper to read a null-terminated string from page data.
fn read_string(data: &[u8]) -> String {
    let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    String::from_utf8_lossy(&data[..end]).to_string()
}

// ============================================================================
// BusTub: VeryBasicTest
// ============================================================================

/// A very basic test.
/// Reference: TEST(BufferPoolManagerTest, VeryBasicTest)
#[test]
fn test_very_basic() {
    let (bpm, _dir) = create_bpm(FRAMES);
    let str_data = "Hello, world!";

    // Allocate a new page (BusTub style: NewPage() just gets ID)
    let pid = bpm.allocate_page_id().unwrap();

    // Check WritePageGuard basic functionality.
    {
        let mut guard = bpm.fetch_page_write(pid).unwrap();
        copy_string(guard.as_mut_slice(), str_data);
        assert_eq!(read_string(guard.as_slice()), str_data);
    }

    // Check ReadPageGuard basic functionality.
    {
        let guard = bpm.fetch_page_read(pid).unwrap();
        assert_eq!(read_string(guard.as_slice()), str_data);
    }

    // Check ReadPageGuard basic functionality (again).
    {
        let guard = bpm.fetch_page_read(pid).unwrap();
        assert_eq!(read_string(guard.as_slice()), str_data);
    }

    assert!(bpm.delete_page(pid).is_ok());
}

// ============================================================================
// BusTub: PagePinEasyTest
// ============================================================================

/// Reference: TEST(BufferPoolManagerTest, PagePinEasyTest)
#[test]
fn test_page_pin_easy() {
    let (bpm, _dir) = create_bpm(2);

    // Allocate and load two pages
    let pageid0 = bpm.allocate_page_id().unwrap();
    let pageid1 = bpm.allocate_page_id().unwrap();

    let str0 = "page0";
    let str1 = "page1";
    let str0_updated = "page0updated";
    let str1_updated = "page1updated";

    // Also allocate the temp page IDs up front (BusTub style)
    let temp_page_id1 = bpm.allocate_page_id().unwrap();
    let temp_page_id2 = bpm.allocate_page_id().unwrap();

    {
        let page0_write_opt = bpm.checked_write_page(pageid0);
        assert!(page0_write_opt.is_some());
        let mut page0_write = page0_write_opt.unwrap();
        copy_string(page0_write.as_mut_slice(), str0);

        let page1_write_opt = bpm.checked_write_page(pageid1);
        assert!(page1_write_opt.is_some());
        let mut page1_write = page1_write_opt.unwrap();
        copy_string(page1_write.as_mut_slice(), str1);

        assert_eq!(bpm.get_pin_count(pageid0), Some(1));
        assert_eq!(bpm.get_pin_count(pageid1), Some(1));

        // All frames pinned - can't fetch new pages
        let temp_page1_opt = bpm.checked_read_page(temp_page_id1);
        assert!(temp_page1_opt.is_none());

        let temp_page2_opt = bpm.checked_write_page(temp_page_id2);
        assert!(temp_page2_opt.is_none());

        assert_eq!(bpm.get_pin_count(pageid0), Some(1));
        page0_write.drop_guard();
        assert_eq!(bpm.get_pin_count(pageid0), Some(0));

        assert_eq!(bpm.get_pin_count(pageid1), Some(1));
        page1_write.drop_guard();
        assert_eq!(bpm.get_pin_count(pageid1), Some(0));
    }

    {
        // Now we can fetch new pages (will evict pageid0 and pageid1)
        let temp_page1_opt = bpm.checked_read_page(temp_page_id1);
        assert!(temp_page1_opt.is_some());
        drop(temp_page1_opt);

        let temp_page2_opt = bpm.checked_write_page(temp_page_id2);
        assert!(temp_page2_opt.is_some());
        drop(temp_page2_opt);

        // pageid0 and pageid1 were evicted - GetPinCount returns None
        assert!(bpm.get_pin_count(pageid0).is_none());
        assert!(bpm.get_pin_count(pageid1).is_none());
    }

    {
        // Fetch original pages back - should reload from disk
        let page0_write_opt = bpm.checked_write_page(pageid0);
        assert!(page0_write_opt.is_some());
        let mut page0_write = page0_write_opt.unwrap();
        assert_eq!(read_string(page0_write.as_slice()), str0);
        copy_string(page0_write.as_mut_slice(), str0_updated);

        let page1_write_opt = bpm.checked_write_page(pageid1);
        assert!(page1_write_opt.is_some());
        let mut page1_write = page1_write_opt.unwrap();
        assert_eq!(read_string(page1_write.as_slice()), str1);
        copy_string(page1_write.as_mut_slice(), str1_updated);

        assert_eq!(bpm.get_pin_count(pageid0), Some(1));
        assert_eq!(bpm.get_pin_count(pageid1), Some(1));
    }

    assert_eq!(bpm.get_pin_count(pageid0), Some(0));
    assert_eq!(bpm.get_pin_count(pageid1), Some(0));

    {
        // Verify updated data persisted
        let page0_read_opt = bpm.checked_read_page(pageid0);
        assert!(page0_read_opt.is_some());
        let page0_read = page0_read_opt.unwrap();
        assert_eq!(read_string(page0_read.as_slice()), str0_updated);

        let page1_read_opt = bpm.checked_read_page(pageid1);
        assert!(page1_read_opt.is_some());
        let page1_read = page1_read_opt.unwrap();
        assert_eq!(read_string(page1_read.as_slice()), str1_updated);

        assert_eq!(bpm.get_pin_count(pageid0), Some(1));
        assert_eq!(bpm.get_pin_count(pageid1), Some(1));
    }

    assert_eq!(bpm.get_pin_count(pageid0), Some(0));
    assert_eq!(bpm.get_pin_count(pageid1), Some(0));
}

// ============================================================================
// BusTub: PagePinMediumTest
// ============================================================================

/// Reference: TEST(BufferPoolManagerTest, PagePinMediumTest)
#[test]
fn test_page_pin_medium() {
    let (bpm, _dir) = create_bpm(FRAMES);

    // Scenario: The buffer pool is empty. We should be able to create a new page.
    let pid0 = bpm.allocate_page_id().unwrap();
    let mut page0 = bpm.fetch_page_write(pid0).unwrap();

    // Scenario: Once we have a page, we should be able to read and write content.
    let hello = "Hello";
    copy_string(page0.as_mut_slice(), hello);
    assert_eq!(read_string(page0.as_slice()), hello);

    page0.drop_guard();

    // Create a vector of page guards to prevent them from being dropped.
    let mut pages = Vec::new();

    // Scenario: We should be able to create new pages until we fill up the buffer pool.
    for _ in 0..FRAMES {
        let pid = bpm.allocate_page_id().unwrap();
        let page = bpm.fetch_page_write(pid).unwrap();
        pages.push(page);
    }

    // Scenario: All of the pin counts should be 1.
    for page in &pages {
        let pid = page.page_id();
        assert_eq!(bpm.get_pin_count(pid), Some(1));
    }

    // Scenario: Once the buffer pool is full, we should not be able to fetch any new pages.
    for _ in 0..FRAMES {
        let pid = bpm.allocate_page_id().unwrap();
        let fail = bpm.checked_write_page(pid);
        assert!(fail.is_none());
    }

    // Scenario: Drop the first 5 pages to unpin them.
    for _ in 0..(FRAMES / 2) {
        let pid = pages[0].page_id();
        assert_eq!(bpm.get_pin_count(pid), Some(1));
        pages.remove(0);
        assert_eq!(bpm.get_pin_count(pid), Some(0));
    }

    // Scenario: All of the pin counts of the pages we haven't dropped yet should still be 1.
    for page in &pages {
        let pid = page.page_id();
        assert_eq!(bpm.get_pin_count(pid), Some(1));
    }

    // Scenario: After unpinning pages, we should be able to fetch new pages.
    // This evicts some of the unpinned pages.
    for _ in 0..((FRAMES / 2) - 1) {
        let pid = bpm.allocate_page_id().unwrap();
        let page = bpm.fetch_page_write(pid).unwrap();
        pages.push(page);
    }

    // Scenario: There should be one frame available, and we should be able to fetch the data
    // we wrote a while ago.
    {
        let original_page = bpm.fetch_page_read(pid0).unwrap();
        assert_eq!(read_string(original_page.as_slice()), hello);
    }

    // Scenario: Once we unpin page 0 and then make a new page, all the buffer pages should
    // now be pinned. Fetching page 0 again should fail.
    let last_pid = bpm.allocate_page_id().unwrap();
    let _last_page = bpm.fetch_page_read(last_pid).unwrap();

    let fail = bpm.checked_read_page(pid0);
    assert!(fail.is_none());
}

// ============================================================================
// BusTub: DropTest (from page_guard_test.cpp)
// ============================================================================

/// Reference: TEST(PageGuardTest, DropTest)
#[test]
fn test_drop() {
    let (bpm, _dir) = create_bpm(FRAMES);

    {
        let pid0 = bpm.allocate_page_id().unwrap();
        let mut page0 = bpm.fetch_page_write(pid0).unwrap();

        // The page should be pinned.
        assert_eq!(bpm.get_pin_count(pid0), Some(1));

        // A drop should unpin the page.
        page0.drop_guard();
        assert_eq!(bpm.get_pin_count(pid0), Some(0));

        // Another drop should have no effect.
        page0.drop_guard();
        assert_eq!(bpm.get_pin_count(pid0), Some(0));
    } // Destructor should be called. Useless but should not cause issues.

    let pid1 = bpm.allocate_page_id().unwrap();
    let pid2 = bpm.allocate_page_id().unwrap();

    {
        let mut read_guarded_page = bpm.fetch_page_read(pid1).unwrap();
        let mut write_guarded_page = bpm.fetch_page_write(pid2).unwrap();

        assert_eq!(bpm.get_pin_count(pid1), Some(1));
        assert_eq!(bpm.get_pin_count(pid2), Some(1));

        // Dropping should unpin the pages.
        read_guarded_page.drop_guard();
        write_guarded_page.drop_guard();
        assert_eq!(bpm.get_pin_count(pid1), Some(0));
        assert_eq!(bpm.get_pin_count(pid2), Some(0));

        // Another drop should have no effect.
        read_guarded_page.drop_guard();
        write_guarded_page.drop_guard();
        assert_eq!(bpm.get_pin_count(pid1), Some(0));
        assert_eq!(bpm.get_pin_count(pid2), Some(0));
    } // Destructor should be called. Useless but should not cause issues.

    // This will hang if the latches were not unlocked correctly in the destructors.
    {
        let _write_test1 = bpm.fetch_page_write(pid1).unwrap();
        let _write_test2 = bpm.fetch_page_write(pid2).unwrap();
    }

    let mut page_ids = Vec::new();
    {
        // Fill up the BPM.
        let mut guards = Vec::new();
        for _ in 0..FRAMES {
            let new_pid = bpm.allocate_page_id().unwrap();
            let guard = bpm.fetch_page_write(new_pid).unwrap();
            assert_eq!(bpm.get_pin_count(new_pid), Some(1));
            page_ids.push(new_pid);
            guards.push(guard);
        }
    } // This drops all of the guards.

    for i in 0..FRAMES {
        assert_eq!(bpm.get_pin_count(page_ids[i]), Some(0));
    }

    // Get a new write page and edit it. We will retrieve it later.
    let mutable_page_id = bpm.allocate_page_id().unwrap();
    let mut mutable_guard = bpm.fetch_page_write(mutable_page_id).unwrap();
    copy_string(mutable_guard.as_mut_slice(), "data");
    mutable_guard.drop_guard();

    {
        // Fill up the BPM again (evicts mutable_page).
        let mut guards = Vec::new();
        for _ in 0..FRAMES {
            let new_pid = bpm.allocate_page_id().unwrap();
            guards.push(bpm.fetch_page_write(new_pid).unwrap());
        }
    }

    // Retrieve the page we edited earlier.
    {
        let guard = bpm.fetch_page_read(mutable_page_id).unwrap();
        assert_eq!(read_string(guard.as_slice()), "data");
    }
}

// ============================================================================
// BusTub: EvictableTest
// ============================================================================

/// Test if the evictable status of a frame is always correct.
/// Reference: TEST(BufferPoolManagerTest, EvictableTest)
///
/// Core invariant: A pinned page cannot be evicted.
#[test]
fn test_evictable() {
    use std::sync::{Condvar, Mutex};
    use std::thread;

    const ROUNDS: usize = 50;
    const NUM_READERS: usize = 4;

    let (bpm, _dir) = create_bpm(1); // Only 1 frame
    let bpm = Arc::new(bpm);

    for round in 0..ROUNDS {
        // Create a page that will be the "winner" - it will occupy the only frame.
        let winner_pid = bpm.allocate_page_id().unwrap();
        let _winner_init = bpm.fetch_page_write(winner_pid).unwrap();
        drop(_winner_init);

        // Create a "loser" page - this evicts winner to make room.
        let loser_pid = bpm.allocate_page_id().unwrap();
        let _loser_init = bpm.fetch_page_write(loser_pid).unwrap();
        drop(_loser_init);
        // At this point: frame has loser, winner is on disk.

        let signal = Arc::new((Mutex::new(false), Condvar::new()));
        let mut readers = Vec::new();

        for _ in 0..NUM_READERS {
            let bpm_clone = Arc::clone(&bpm);
            let signal_clone = Arc::clone(&signal);
            let winner = winner_pid;
            let loser = loser_pid;

            readers.push(thread::spawn(move || {
                let (lock, cvar) = &*signal_clone;

                // Wait until main thread signals.
                {
                    let mut started = lock.lock().unwrap();
                    while !*started {
                        started = cvar.wait(started).unwrap();
                    }
                }

                // Main has loaded winner and is holding it pinned.
                // We should be able to read winner (cache hit, shared lock).
                let _read_guard = bpm_clone.fetch_page_read(winner).unwrap();

                // Since the only frame is pinned, we cannot bring in loser.
                assert!(
                    bpm_clone.checked_read_page(loser).is_none(),
                    "round {}: loser should not be fetchable while winner is pinned",
                    round
                );
            }));
        }

        // Main thread: fetch winner (evicts loser) and hold it.
        let winner_guard = bpm.fetch_page_read(winner_pid).unwrap();

        // Signal readers to start.
        {
            let (lock, cvar) = &*signal;
            let mut started = lock.lock().unwrap();
            *started = true;
            cvar.notify_all();
        }

        // Wait for all readers to complete while we still hold winner.
        for reader in readers {
            reader.join().unwrap();
        }

        // Now drop our guard.
        drop(winner_guard);
    }
}

// ============================================================================
// BusTub: PageAccessTest
// ============================================================================

/// Test that holding a write lock doesn't cause deadlock when acquiring another.
/// Reference: TEST(BufferPoolManagerTest, PageAccessTest)
#[test]
fn test_page_access() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    let (bpm, _dir) = create_bpm(FRAMES);
    let bpm = Arc::new(bpm);

    let pid0 = bpm.allocate_page_id().unwrap();
    let pid1 = bpm.allocate_page_id().unwrap();
    
    // Bring pages into pool
    drop(bpm.fetch_page_write(pid0).unwrap());
    drop(bpm.fetch_page_write(pid1).unwrap());

    // Take the write latch on page 0.
    let mut guard0 = bpm.fetch_page_write(pid0).unwrap();

    let start = Arc::new(AtomicBool::new(false));
    let start_clone = Arc::clone(&start);
    let bpm_clone = Arc::clone(&bpm);

    let child = thread::spawn(move || {
        start_clone.store(true, Ordering::SeqCst);

        // Attempt to write to page 0 (will block until main releases it).
        let _guard0 = bpm_clone.fetch_page_write(pid0).unwrap();
    });

    // Wait for the other thread to begin before we start the test.
    while !start.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    // Make the other thread wait for a bit.
    thread::sleep(Duration::from_millis(100));

    // If your latching mechanism is incorrect, the next line of code will deadlock.
    // While holding page 0, take the latch on page 1.
    let _guard1 = bpm.fetch_page_write(pid1).unwrap();

    // Let the child thread have the page 0 since we're done with it.
    guard0.drop_guard();

    child.join().unwrap();
}

// ============================================================================
// Additional: Test new_page() convenience method
// ============================================================================

/// Test the convenience method that combines allocate + fetch.
#[test]
fn test_new_page_convenience() {
    let (bpm, _dir) = create_bpm(FRAMES);
    let data = b"Hello, world!";

    // Create and write using convenience method
    let pid = {
        let mut guard = bpm.new_page().unwrap();
        assert_eq!(guard.page_id(), PageId::new(0));
        guard.as_mut_slice()[..data.len()].copy_from_slice(data);
        guard.page_id()
    };

    // Read back
    {
        let guard = bpm.fetch_page_read(pid).unwrap();
        assert_eq!(&guard.as_slice()[..data.len()], data);
    }

    // Delete
    bpm.delete_page(pid).unwrap();
    assert!(!bpm.contains_page(pid));
}
