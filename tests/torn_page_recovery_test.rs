//! Q-32 (pillar D, torn-page milestone): a torn write to a B-tree node page
//! must be **detected** on read — never silently accepted as valid data.
//!
//! ## What this exercises
//!
//! The WAL crash sweep (`dst_recovery_test.rs`) covers a *clean* tail loss. The
//! other half of durability is media-level corruption: a page write interrupted
//! mid-flush leaves a page whose body no longer matches its stored checksum.
//! Every B-tree node page carries a CRC32 in its header, verified at decode
//! time (`decode_{leaf,internal}_node` → `assert_eq!(stored, computed, "...
//! checksum mismatch")`), backed up by structural type checks during
//! traversal. The invariant: a torn node page is caught by one of those
//! backstops ("crash on corruption"), rather than feeding a reader garbage that
//! decodes to a structurally-plausible but wrong node.
//!
//! ## Method
//!
//! 1. Build a B-tree on a file-backed engine wrapped in
//!    `FaultInjectionDiskManager`, armed to **tear the next node-page write**
//!    (keep the header, zero the body — see `with_torn_next_node_write`).
//! 2. `flush()` — the first node page hits disk torn; its frame is then clean,
//!    so a re-read must come from the torn disk image.
//! 3. Drop the engine, reopen the tree over the same file (no injection), and
//!    scan it. The torn node must be **detected** — surfaced as a corruption
//!    `Err` from the scan or a decode-time panic — never silently accepted.
//!
//! A torn page can trip more than one backstop: decode's checksum assert
//! (`"... checksum mismatch"` panic) or a structural type check
//! (`StorageCorrupted` when the zeroed body breaks navigation). Either is valid
//! detection; the invariant we forbid is a clean scan that returns data as if
//! nothing happened. A clean control (no tear) scans successfully, proving the
//! detection is the corruption and not a harness artifact.

use std::panic::{self, AssertUnwindSafe};
use std::path::Path;

use tempfile::tempdir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::{FileDiskManager, StorageEngine};
use interchangedb::PageHeader;
use testkit::faults::FaultInjectionDiskManager;

const N_KEYS: u32 = 300;
const POOL_SIZE: usize = 1024;

fn key(i: u32) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("val{i:05}").into_bytes()
}

/// Reopen the tree at `path` (no fault injection) and force a full traversal by
/// consuming a scan over all keys. Returns `Ok(count)` if the scan completes
/// cleanly, or `Err(message)` if it surfaces a corruption error. A decode panic
/// (e.g. the checksum assert) propagates to the caller, caught separately.
fn reopen_and_scan(path: &Path) -> Result<usize, String> {
    let dm = FileDiskManager::open(path).unwrap();
    let bpm = BufferPoolManager::new(POOL_SIZE, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let mut count = 0usize;
    for item in engine.scan(..) {
        match item {
            Ok(_) => count += 1,
            Err(e) => return Err(format!("{e:?}")),
        }
    }
    Ok(count)
}

/// Extract a panic payload's message as a string.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[test]
fn torn_node_page_is_detected_on_read() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("torn.db");

    // --- Control: a clean tree reopens and scans without incident. ---
    {
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(POOL_SIZE, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        for i in 0..N_KEYS {
            engine.put(&key(i), &value(i)).unwrap();
        }
        engine.flush().unwrap();
    }
    assert_eq!(
        reopen_and_scan(&path),
        Ok(N_KEYS as usize),
        "clean tree must scan all keys — guards against a harness-side panic"
    );

    // --- Torn: tear the first node-page flush, then reopen + scan. ---
    let torn_path = dir.path().join("torn2.db");
    {
        let dm = FileDiskManager::create(&torn_path)
            .map(FaultInjectionDiskManager::new)
            .unwrap()
            // Keep a valid header (type + new checksum), zero the body — a
            // populated node's body then disagrees with its checksum.
            .with_torn_next_node_write(PageHeader::SIZE);
        let bpm = BufferPoolManager::new(POOL_SIZE, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        for i in 0..N_KEYS {
            engine.put(&key(i), &value(i)).unwrap();
        }
        // No eviction happens (pool >> tree), so node pages are written only
        // here — the first one is torn.
        engine.flush().unwrap();
    }

    // Reopening reads only the (untorn) header page; the torn node is hit when
    // the scan traverses the tree. Detection may surface as a panic (checksum
    // assert) or a corruption Err — both are acceptable. The one forbidden
    // outcome is a clean scan that returns data as if nothing was torn.
    match panic::catch_unwind(AssertUnwindSafe(|| reopen_and_scan(&torn_path))) {
        Err(payload) => {
            // Detected via a decode-time panic (e.g. the checksum assert).
            let message = panic_message(payload);
            assert!(
                message.contains("checksum mismatch") || message.contains("StorageCorrupted"),
                "torn page panicked, but not with a corruption message: {message:?}"
            );
        }
        Ok(Err(_corruption)) => {
            // Detected via a corruption Err surfaced from the scan. Good.
        }
        Ok(Ok(count)) => {
            panic!(
                "torn node page was SILENTLY ACCEPTED: scan returned {count} entries \
                 with no error or panic"
            );
        }
    }
}
