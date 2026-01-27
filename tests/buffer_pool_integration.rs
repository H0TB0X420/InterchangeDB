//! Integration tests for the buffer pool manager.
//!
//! These tests verify cross-component behavior that unit tests don't cover.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::PageId;
use interchangedb::storage::DiskManager;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn create_bpm(pool_size: usize) -> (BufferPoolManager, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = DiskManager::create(&path).unwrap();
    (BufferPoolManager::new(pool_size, dm), dir)
}

/// Test data persistence across multiple eviction cycles.
#[test]
fn test_data_persistence_across_evictions() {
    let (bpm, _dir) = create_bpm(2);

    // Create 5 pages with unique data (forces evictions)
    let mut page_ids = vec![];
    for i in 0u8..5 {
        let mut guard = bpm.new_page().unwrap();
        guard.as_mut_slice()[0] = i;
        guard.as_mut_slice()[1] = i.wrapping_mul(3);
        page_ids.push(guard.page_id());
    }

    // Read all back - verifies evicted pages were flushed
    for (i, &pid) in page_ids.iter().enumerate() {
        let guard = bpm.fetch_page_read(pid).unwrap();
        assert_eq!(guard.as_slice()[0], i as u8);
        assert_eq!(guard.as_slice()[1], (i as u8).wrapping_mul(3));
    }
}

/// Test flush and reload across BPM instances.
#[test]
fn test_flush_and_reload() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let data = b"persistent!";

    let pid;

    // First session: create and write
    {
        let dm = DiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(10, dm);

        let mut guard = bpm.new_page().unwrap();
        pid = guard.page_id();
        guard.as_mut_slice()[..data.len()].copy_from_slice(data);
        drop(guard);

        bpm.flush_all_pages().unwrap();
    }

    // Second session: verify data
    {
        let dm = DiskManager::open(&path).unwrap();
        let bpm = BufferPoolManager::new(10, dm);

        let guard = bpm.fetch_page_read(pid).unwrap();
        assert_eq!(&guard.as_slice()[..data.len()], data);
    }
}

/// Test concurrent writers to different pages.
#[test]
fn test_concurrent_writers() {
    let (bpm, _dir) = create_bpm(10);
    let bpm = Arc::new(bpm);

    let page_ids: Vec<PageId> = (0..5)
        .map(|_| bpm.new_page().unwrap().page_id())
        .collect();

    let mut handles = vec![];

    for (i, pid) in page_ids.iter().enumerate() {
        let bpm_clone = Arc::clone(&bpm);
        let pid = *pid;

        handles.push(thread::spawn(move || {
            for j in 0..50 {
                let mut guard = bpm_clone.fetch_page_write(pid).unwrap();
                guard.as_mut_slice()[0] = ((i * 50 + j) % 256) as u8;
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify each page has last written value
    for (i, &pid) in page_ids.iter().enumerate() {
        let guard = bpm.fetch_page_read(pid).unwrap();
        assert_eq!(guard.as_slice()[0], ((i * 50 + 49) % 256) as u8);
    }
}

/// Test stats accuracy under load.
#[test]
fn test_stats_accuracy() {
    let (bpm, _dir) = create_bpm(2);

    let pid = bpm.new_page().unwrap().page_id();

    // Multiple fetches = cache hits
    for _ in 0..5 {
        let _ = bpm.fetch_page_read(pid).unwrap();
    }

    let stats = bpm.stats().snapshot();
    assert!(stats.cache_hits >= 5);

    // Force eviction
    let _ = bpm.new_page().unwrap();
    let _ = bpm.new_page().unwrap();

    let stats = bpm.stats().snapshot();
    assert!(stats.evictions >= 1);
}