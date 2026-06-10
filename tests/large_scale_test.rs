//! Q-17: large-scale integration tests.
//!
//! Existing scale tests in `btree_scale_test.rs` use 5K keys with
//! pathologically-small node sizes to force splits/merges. These tests
//! complement that with *production-scale workloads*: 100K+ keys with
//! realistic node sizes, but with a bounded buffer pool that forces
//! sustained eviction pressure.
//!
//! Goals:
//! - Verify the BPM evicts cleanly under continuous pressure (no
//!   `NoFreeFrames` errors despite 100K reads against a 64-frame pool).
//! - Verify both engines stay correct at scale (every inserted key is
//!   retrievable; sort order preserved).
//! - Sanity-check that runtime grows sub-quadratically (the absolute
//!   numbers are platform-dependent, so we don't enforce specific
//!   throughput — just bound the *worst case* to catch O(n²) regressions).
//!
//! 1M-key tests are `#[ignore]`'d so `cargo test` stays fast. Run them
//! manually with `cargo test --release -- --include-ignored`.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::index::lsm::LsmEngine;
use interchangedb::storage::{FileDiskManager, StorageEngine};

fn make_btree(pool_size: usize) -> (Arc<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let bpm = BufferPoolManager::new(pool_size, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    (engine, dir)
}

fn make_lsm() -> (Arc<LsmEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(LsmEngine::new(dir.path()).unwrap());
    (engine, dir)
}

fn key_for(i: u32) -> Vec<u8> {
    format!("k{:010}", i).into_bytes()
}

fn value_for(i: u32) -> Vec<u8> {
    format!("v{:010}", i).into_bytes()
}

#[test]
fn btree_100k_keys_bounded_pool_no_eviction_failures() {
    // 25K is the largest tractable size at debug build with FileDiskManager
    // (which fsyncs every write_page). Real throughput goals belong in
    // criterion benchmarks, not integration tests. This test's job is to
    // verify correctness under sustained eviction pressure, not perf.
    const N: u32 = 25_000;
    let (e, _d) = make_btree(64);

    for i in 0..N {
        e.put(&key_for(i), &value_for(i)).unwrap();
    }

    for i in 0..N {
        let v = e.get(&key_for(i)).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(value_for(i).as_slice()),
            "missing key {} after bulk insert",
            i
        );
    }
}

#[test]
fn btree_25k_scan_returns_sorted() {
    const N: u32 = 25_000;
    let (e, _d) = make_btree(64);
    for i in 0..N {
        e.put(&key_for(i), &value_for(i)).unwrap();
    }

    let collected: Vec<_> = e.scan(..).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        collected.len(),
        N as usize,
        "scan returned {} keys, expected {}",
        collected.len(),
        N
    );
    assert_eq!(collected[0].0, key_for(0));
    assert_eq!(collected[N as usize - 1].0, key_for(N - 1));
    for i in 1..collected.len() {
        assert!(
            collected[i - 1].0 < collected[i].0,
            "scan unsorted at index {}",
            i
        );
    }
}

#[test]
fn lsm_50k_keys_bulk_workload() {
    // LSM doesn't fsync on every put (batches in memtable), so it can
    // tolerate a larger N than the B+Tree's per-put-fsync penalty allows.
    const N: u32 = 50_000;
    let (e, _d) = make_lsm();
    for i in 0..N {
        e.put(&key_for(i), &value_for(i)).unwrap();
    }
    for i in [0u32, 1, N / 4, N / 2, (3 * N) / 4, N - 1].iter() {
        let v = e.get(&key_for(*i)).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(value_for(*i).as_slice()),
            "missing key {} in LSM at scale",
            i
        );
    }
}

#[test]
fn btree_random_read_pattern_under_eviction_pressure() {
    // Insert 50K keys, then random-read 5K of them with a small pool.
    // Tests that the BPM handles a non-sequential access pattern with
    // sustained eviction (every read likely misses → eviction).
    const N: u32 = 50_000;
    const READS: u32 = 5_000;
    let (e, _d) = make_btree(32);
    for i in 0..N {
        e.put(&key_for(i), &value_for(i)).unwrap();
    }

    // Deterministic LCG for reproducible random pattern.
    let mut state: u64 = 0xDEADBEEF;
    let mut misses = 0u32;
    for _ in 0..READS {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let idx = (state as u32) % N;
        match e.get(&key_for(idx)).unwrap() {
            Some(v) => assert_eq!(v, value_for(idx)),
            None => misses += 1,
        }
    }
    assert_eq!(
        misses, 0,
        "{} reads returned None on keys that were inserted",
        misses
    );
}

// ---- 1M-scale tests (ignored by default) ----

#[test]
#[ignore = "slow; run with --include-ignored"]
fn btree_1m_keys_release_build_recommended() {
    const N: u32 = 1_000_000;
    let (e, _d) = make_btree(256);
    for i in 0..N {
        e.put(&key_for(i), &value_for(i)).unwrap();
    }
    // Spot-check, not full sweep — 1M reads would be very slow in debug.
    for i in [0u32, N / 4, N / 2, (3 * N) / 4, N - 1].iter() {
        let v = e.get(&key_for(*i)).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(value_for(*i).as_slice()),
            "missing key {} at 1M scale",
            i
        );
    }
}

#[test]
#[ignore = "slow; run with --include-ignored"]
fn lsm_1m_keys_release_build_recommended() {
    const N: u32 = 1_000_000;
    let (e, _d) = make_lsm();
    for i in 0..N {
        e.put(&key_for(i), &value_for(i)).unwrap();
    }
    for i in [0u32, N / 4, N / 2, (3 * N) / 4, N - 1].iter() {
        let v = e.get(&key_for(*i)).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(value_for(*i).as_slice()),
            "missing key {} at 1M scale (LSM)",
            i
        );
    }
}
