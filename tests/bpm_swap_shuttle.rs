//! Q-30: deterministic model check of the BPM policy-swap vs. eviction race
//! (stability.md pillar C). Run with the `shuttle` feature + nightly-not-
//! required (stable works for shuttle itself):
//!
//! ```sh
//! cargo test --features shuttle --release --test bpm_swap_shuttle
//! ```
//!
//! shuttle replaces the buffer pool's `parking_lot`/atomic primitives (via
//! `crate::sync`) with instrumented versions and explores thread interleavings
//! under a controlled scheduler. A failing schedule is reported with a seed
//! that reproduces it exactly — the bridge from "fails ~25% under load" to a
//! single deterministic repro we can debug.
//!
//! The invariant: a page's content is never corrupted by a concurrent swap —
//! every fetch of page P returns P's own marker, never another page's.

#![cfg(feature = "shuttle")]

use interchangedb::buffer::replacer::{ClockReplacer, FifoReplacer, LruReplacer};
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::storage::MemoryDiskManager;

use shuttle::scheduler::{PctScheduler, RandomScheduler, ReplayScheduler};
use shuttle::sync::Arc;
use shuttle::{Config, Runner};

const POOL_SIZE: usize = 2;
const N_PAGES: u8 = 3; // > POOL_SIZE so fetches force evictions

/// Seed `N_PAGES` pages, each with a distinct non-zero marker in byte 0 and
/// byte 4095. Returns (page_id, marker) pairs. Pool ends holding the last
/// `POOL_SIZE` pages; the rest live on disk.
fn seed(bpm: &BufferPoolManager) -> Vec<(interchangedb::PageId, u8)> {
    let mut pages = Vec::new();
    for i in 0..N_PAGES {
        let mut guard = bpm.new_page().expect("seed allocate");
        let pid = guard.page_id();
        let marker = i + 1; // 1..=N_PAGES, never 0 (a blank frame reads 0)
        guard.as_mut_slice()[0] = marker;
        guard.as_mut_slice()[4095] = marker;
        drop(guard);
        pages.push((pid, marker));
    }
    pages
}

/// A worker that fetches each of its pages for write, dirties byte 2048, and
/// checks the seeded markers (byte 0 / 4095) are intact while it holds the
/// page. Dirtying is essential — only a dirty frame is *flushed* on eviction,
/// and the Q-30 corruption is wrong data flushed to a page's disk slot.
fn write_worker(bpm: Arc<BufferPoolManager>, pages: Vec<(interchangedb::PageId, u8)>) {
    for (pid, marker) in &pages {
        if let Ok(mut guard) = bpm.fetch_page_write(*pid) {
            let data = guard.as_mut_slice();
            assert_eq!(data[0], *marker, "byte 0 of {:?} corrupted (held)", pid);
            assert_eq!(
                data[4095], *marker,
                "byte 4095 of {:?} corrupted (held)",
                pid
            );
            data[2048] = 0xCC;
        }
    }
}

/// Two writers contend for the small pool's frames (forcing concurrent dirty
/// evictions, with overlap on a shared page) while a swapper cycles policies —
/// each swap re-registers frames in the new replacer, the operation that races
/// eviction. After all join, every page must read back its own marker.
fn swap_vs_eviction_scenario() {
    let bpm = Arc::new(BufferPoolManager::new(POOL_SIZE, MemoryDiskManager::new()));
    let pages = seed(&bpm); // markers 1..=N_PAGES; pool holds the last POOL_SIZE

    // Overlapping page sets so the writers also double-load the same page.
    let w1_pages = vec![pages[0], pages[1]];
    let w2_pages = vec![pages[1], pages[2]];

    let b1 = bpm.clone();
    let w1 = shuttle::thread::spawn(move || write_worker(b1, w1_pages));
    let b2 = bpm.clone();
    let w2 = shuttle::thread::spawn(move || write_worker(b2, w2_pages));

    let bs = bpm.clone();
    let swapper = shuttle::thread::spawn(move || {
        let _ = bs.swap_policy(Box::new(LruReplacer::new(POOL_SIZE)), SwapMode::Cold);
        let _ = bs.swap_policy(Box::new(FifoReplacer::new()), SwapMode::Warm);
        let _ = bs.swap_policy(Box::new(ClockReplacer::new()), SwapMode::Cold);
    });

    w1.join().unwrap();
    w2.join().unwrap();
    swapper.join().unwrap();

    // Final pass: nothing pinned now; every page must be intact.
    for (pid, marker) in &pages {
        let guard = bpm.fetch_page_read(*pid).expect("final fetch");
        assert_eq!(
            guard.as_slice()[0],
            *marker,
            "byte 0 of {:?} corrupted (final)",
            pid
        );
        assert_eq!(
            guard.as_slice()[4095],
            *marker,
            "byte 4095 of {:?} corrupted (final)",
            pid
        );
    }
}

fn config_with_stack() -> Config {
    let mut config = Config::new();
    // Pages are 4 KiB and copied on the stack during fetch/eviction; shuttle's
    // default 60 KiB green-thread stack overflows (SIGBUS). Give it headroom.
    config.stack_size = 4 * 1024 * 1024;
    config
}

// IGNORED until Q-30 is fixed: at 100k iterations on this scenario, even
// uniform-random reproduces the race (PCT finds it far faster). Un-ignore once
// the BPM fix lands.
#[ignore = "Q-30: reproduces the open BPM swap/eviction race; un-ignore after fix"]
#[test]
fn shuttle_swap_vs_eviction_random() {
    // Randomized interleaving search — prints a reproducing seed on failure.
    let scheduler = RandomScheduler::new(100_000);
    Runner::new(scheduler, config_with_stack()).run(swap_vs_eviction_scenario);
}

// IGNORED until Q-30 is fixed: PCT reliably finds the swap↔eviction race, so
// this currently fails by design. Un-ignore once the BPM fix lands — it then
// becomes the robust regression guard (explores and finds nothing). Run now
// with `cargo test --features shuttle -- --ignored` to reproduce.
#[ignore = "Q-30: reproduces the open BPM swap/eviction race; un-ignore after fix"]
#[test]
fn shuttle_swap_vs_eviction_pct() {
    // PCT is biased toward bugs that need only a few preemptions — often finds
    // concurrency bugs that uniform-random misses.
    let scheduler = PctScheduler::new(5, 50_000);
    Runner::new(scheduler, config_with_stack()).run(swap_vs_eviction_scenario);
}

/// A schedule (found by `shuttle_swap_vs_eviction_pct`) that deterministically
/// triggers the Q-30 race: a writer holding `fetch_page_write(P2)` observes a
/// zeroed frame (byte 0 == 0) — P2's dirty page was lost and reloaded as the
/// blank on-disk version. Replays in a single execution for debugging and as a
/// hard regression: once the race is fixed this must run clean.
const Q30_RACE_SCHEDULE: &str = "91028f02a3cc9ce495e9ddd17f0000000000000000000000000000000000000000000000000000000000489224499224499224499224818445dbb66ddbb66d5b922449922489a42449924892244992244992244992244992244992244992244992244992242549922449922489922449204912";

// IGNORED until Q-30 is fixed: deterministically reproduces the race in one
// execution. After the fix, replaying this exact schedule should no longer
// corrupt (or the schedule may legitimately diverge if the fix changes
// scheduling points — at which point PCT is the regression of record).
#[ignore = "Q-30: deterministic repro of the open race; un-ignore/re-evaluate after fix"]
#[test]
fn shuttle_replay_q30_race() {
    let scheduler = ReplayScheduler::new_from_encoded(Q30_RACE_SCHEDULE);
    Runner::new(scheduler, config_with_stack()).run(swap_vs_eviction_scenario);
}
