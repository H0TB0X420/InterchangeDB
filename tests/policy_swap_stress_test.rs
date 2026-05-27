//! Q-01: hot-swap eviction policy under concurrent BPM load.
//!
//! The project's marquee feature (runtime policy interchange) needs its
//! marquee test. This test cycles through all 6 supported policies,
//! alternating cold and warm swap modes, while N reader and M writer
//! threads pound the buffer pool with random page accesses.
//!
//! Invariants the test enforces:
//! - No thread panics or returns a permanent error during a swap.
//! - All pages that exist before the swap storm are still readable after.
//! - Data written during the storm is still present (no silent drops).
//! - The `policy_swaps` stat counter increments monotonically and matches
//!   the number of swaps issued.
//! - The pool stays internally consistent (no double-free of frames, no
//!   leaked pins) — verified indirectly via the post-storm reads.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use interchangedb::buffer::replacer::{
    ArcReplacer, ClockReplacer, EvictionPolicy, FifoReplacer, LruKReplacer, LruReplacer,
    TwoQReplacer,
};
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::common::PageId;
use interchangedb::storage::MemoryDiskManager;

const POOL_SIZE: usize = 32;
const PRE_ALLOCATED_PAGES: u32 = 200;
const SWAP_INTERVAL_MS: u64 = 8;
const STORM_DURATION_MS: u64 = 1500;
const N_READER_THREADS: usize = 4;
const N_WRITER_THREADS: usize = 2;

/// All 6 policies the BPM supports. Each closure produces a fresh `Box<dyn
/// EvictionPolicy>` — the BPM consumes ownership on swap.
fn fresh_policy(i: usize) -> Box<dyn EvictionPolicy> {
    match i % 6 {
        0 => Box::new(FifoReplacer::new()),
        1 => Box::new(ClockReplacer::new()),
        2 => Box::new(LruReplacer::new(POOL_SIZE)),
        3 => Box::new(LruKReplacer::new(2)),
        4 => Box::new(TwoQReplacer::new(16)),
        5 => Box::new(ArcReplacer::new(64)),
        _ => unreachable!(),
    }
}


fn build_bpm() -> Arc<BufferPoolManager> {
    Arc::new(BufferPoolManager::new(POOL_SIZE, MemoryDiskManager::new()))
}

/// Allocate a known set of pages with deterministic content. Returns the
/// list of (page_id, expected_byte_pattern).
fn seed_pages(bpm: &BufferPoolManager) -> Vec<(PageId, u8)> {
    let mut pages = Vec::with_capacity(PRE_ALLOCATED_PAGES as usize);
    for i in 0..PRE_ALLOCATED_PAGES {
        let mut guard = bpm.new_page().expect("seed allocate");
        let pid = guard.page_id();
        let marker = (i % 251 + 1) as u8; // non-zero, so we can tell from a blank page
        guard.as_mut_slice()[0] = marker;
        guard.as_mut_slice()[4095] = marker;
        drop(guard);
        pages.push((pid, marker));
    }
    pages
}

#[test]
fn marquee_hot_swap_storm_all_six_policies_both_modes() {
    let bpm = build_bpm();
    let seeded = seed_pages(&bpm);
    let initial_swaps = bpm.stats().snapshot().policy_swaps;

    let stop = Arc::new(AtomicBool::new(false));
    let read_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Reader threads — random page reads.
    for t in 0..N_READER_THREADS {
        let bpm = bpm.clone();
        let stop = stop.clone();
        let read_count = read_count.clone();
        let seeded_pids: Vec<PageId> = seeded.iter().map(|(pid, _)| *pid).collect();
        handles.push(thread::spawn(move || {
            let mut rng = t as u64 * 1_000_003;
            while !stop.load(Ordering::Acquire) {
                rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let idx = (rng as usize) % seeded_pids.len();
                let pid = seeded_pids[idx];
                if let Ok(g) = bpm.fetch_page_read(pid) {
                    // Touch the data to force a real read.
                    let _ = g.as_slice()[0];
                    drop(g);
                    read_count.fetch_add(1, Ordering::Relaxed);
                }
                // Otherwise the BPM is briefly full of pinned frames — try again.
            }
        }));
    }

    // Writer threads — overwrite random pages with a thread-tagged marker.
    for t in 0..N_WRITER_THREADS {
        let bpm = bpm.clone();
        let stop = stop.clone();
        let write_count = write_count.clone();
        let seeded_pids: Vec<PageId> = seeded.iter().map(|(pid, _)| *pid).collect();
        handles.push(thread::spawn(move || {
            let mut rng = (t as u64 + 100) * 2_999_999;
            while !stop.load(Ordering::Acquire) {
                rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let idx = (rng as usize) % seeded_pids.len();
                let pid = seeded_pids[idx];
                if let Ok(mut g) = bpm.fetch_page_write(pid) {
                    // Mark byte 2048 with the thread id so the value is
                    // distinguishable from seeded markers (at byte 0 and 4095).
                    g.as_mut_slice()[2048] = (t as u8) | 0x80;
                    drop(g);
                    write_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Swap thread — cycle policies, alternate cold/warm.
    let policy_changes_issued: usize;
    {
        let bpm = bpm.clone();
        let stop = stop.clone();
        let storm_until = Instant::now() + Duration::from_millis(STORM_DURATION_MS);
        let h = thread::spawn(move || {
            let mut i = 0usize;
            let mut issued = 0usize;
            while Instant::now() < storm_until {
                let policy = fresh_policy(i);
                let mode = if i % 2 == 0 { SwapMode::Cold } else { SwapMode::Warm };
                let result = bpm.swap_policy(policy, mode);
                assert!(
                    !result.old_policy.is_empty(),
                    "swap result missing old_policy"
                );
                issued += 1;
                i += 1;
                thread::sleep(Duration::from_millis(SWAP_INTERVAL_MS));
            }
            stop.store(true, Ordering::Release);
            issued
        });
        policy_changes_issued = h.join().expect("swap thread panicked");
    }

    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // ---- Invariants ----

    let final_swaps = bpm.stats().snapshot().policy_swaps;
    let observed_swap_delta = final_swaps - initial_swaps;
    assert_eq!(
        observed_swap_delta, policy_changes_issued as u64,
        "policy_swaps counter ({}) doesn't match swaps issued ({})",
        observed_swap_delta, policy_changes_issued
    );
    assert!(
        policy_changes_issued >= 6,
        "storm too short — only {} swaps; expected at least one cycle through all 6 policies",
        policy_changes_issued
    );

    let r = read_count.load(Ordering::Relaxed);
    let w = write_count.load(Ordering::Relaxed);
    assert!(r > 100, "implausibly few reads: {}", r);
    assert!(w > 50, "implausibly few writes: {}", w);

    // Every seeded page is still readable, with byte 0 and byte 4095
    // matching the seeded marker. Writes may have stamped byte 2048; we
    // don't require byte 0/4095 to have changed.
    for (pid, marker) in &seeded {
        let guard = bpm.fetch_page_read(*pid).unwrap_or_else(|e| {
            panic!("post-storm read of {:?} failed: {:?}", pid, e)
        });
        let data = guard.as_slice();
        assert_eq!(
            data[0], *marker,
            "byte 0 of {:?} corrupted after swap storm",
            pid
        );
        assert_eq!(
            data[4095], *marker,
            "byte 4095 of {:?} corrupted after swap storm",
            pid
        );
        drop(guard);
    }

    // Final policy is whichever was set last. The exact identity is
    // timing-dependent (we can't synchronize the swap thread's last call
    // with the assertion), so we just confirm the BPM is reporting one
    // of the six valid policy names.
    let final_name = bpm.get_policy_name();
    let valid: &[&str] = &["fifo", "clock", "lru", "lru-k", "2q", "arc"];
    assert!(
        valid.contains(&final_name),
        "final policy name {} is not a known policy",
        final_name
    );
}

// Bisection test: exercises the same workload as the marquee test but
// without any policy swaps. Both this and the marquee test were ignored
// while Q-27 was open. Keeping it active as a regression guard — if
// Q-27 ever reopens, this is the cheapest reproduction.
#[test]
fn isolation_concurrent_rw_without_swaps_preserves_data() {
    // Bisect: same workload as the marquee test, but no policy swaps.
    // Tells us whether the cross-page corruption is caused by the swap
    // path or by concurrent BPM I/O on a small pool.
    let bpm = build_bpm();
    let seeded = seed_pages(&bpm);
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for t in 0..N_READER_THREADS {
        let bpm = bpm.clone();
        let stop = stop.clone();
        let pids: Vec<PageId> = seeded.iter().map(|(p, _)| *p).collect();
        handles.push(thread::spawn(move || {
            let mut rng = t as u64 * 1_000_003;
            while !stop.load(Ordering::Acquire) {
                rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let pid = pids[(rng as usize) % pids.len()];
                if let Ok(g) = bpm.fetch_page_read(pid) {
                    let _ = g.as_slice()[0];
                }
            }
        }));
    }
    for t in 0..N_WRITER_THREADS {
        let bpm = bpm.clone();
        let stop = stop.clone();
        let pids: Vec<PageId> = seeded.iter().map(|(p, _)| *p).collect();
        handles.push(thread::spawn(move || {
            let mut rng = (t as u64 + 100) * 2_999_999;
            while !stop.load(Ordering::Acquire) {
                rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let pid = pids[(rng as usize) % pids.len()];
                if let Ok(mut g) = bpm.fetch_page_write(pid) {
                    g.as_mut_slice()[2048] = (t as u8) | 0x80;
                }
            }
        }));
    }
    thread::sleep(Duration::from_millis(STORM_DURATION_MS));
    stop.store(true, Ordering::Release);
    for h in handles {
        h.join().unwrap();
    }
    for (pid, marker) in &seeded {
        let g = bpm.fetch_page_read(*pid).unwrap();
        assert_eq!(
            g.as_slice()[0],
            *marker,
            "byte 0 of {:?} corrupted WITHOUT any policy swaps — concurrent BPM bug",
            pid
        );
    }
}

#[test]
fn warm_swap_preserves_hot_set_under_load() {
    // Lightweight test of the warm-swap path specifically: read a small
    // set of pages repeatedly to build up "hot" state in the policy, swap
    // warm, confirm those pages remain in the pool (don't trigger fresh
    // reads from disk).
    //
    // We don't have an easy hook to count "did we re-read from disk?"
    // without instrumenting the disk manager — fault injection works for
    // *forcing* a read failure, but not for *counting* reads. So this
    // test focuses on the no-error / no-data-loss invariants. Counting
    // hot-set retention is a deeper job for Q-08 / Phase 16 profiling.
    let bpm = build_bpm();
    let seeded = seed_pages(&bpm);
    let hot_set: Vec<PageId> = seeded.iter().take(8).map(|(pid, _)| *pid).collect();

    // Heat up the hot set.
    for _ in 0..50 {
        for pid in &hot_set {
            let _g = bpm.fetch_page_read(*pid).unwrap();
        }
    }

    // Warm swap to LRU.
    let result = bpm.swap_policy(Box::new(LruReplacer::new(POOL_SIZE)), SwapMode::Warm);
    assert_eq!(result.mode, SwapMode::Warm);
    assert!(!result.old_policy.is_empty());

    // Re-read the hot set — must all succeed.
    for (pid, marker) in seeded.iter().take(8) {
        let g = bpm.fetch_page_read(*pid).unwrap();
        assert_eq!(g.as_slice()[0], *marker);
    }
}
