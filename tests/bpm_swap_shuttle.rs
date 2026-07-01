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
//! Three models live here — one per Q-30 root cause, plus the Q-35 composite:
//!   1. `swap_vs_eviction_scenario` — a policy swap re-registering frames races
//!      a concurrent dirty eviction (the original Q-30 symptom).
//!   2. `same_page_miss_scenario` — two threads missing the *same* page at once
//!      both load it and one is left a "ghost" frame (the second root cause the
//!      first model structurally missed).
//!   3. `three_writer_storm_scenario` (Q-35) — three dirty writers + a swapper,
//!      composing concurrent evictions, same-page misses, guard drops, and
//!      policy swaps SIMULTANEOUSLY. The marquee's third interleaving needs a
//!      third concurrent evictor, which both pairwise models structurally lack.
//!
//! The invariants: every fetch of page P returns P's own marker (never another
//! page's), and at rest every frame is either free or mapped (no ghost).

#![cfg(feature = "shuttle")]

use interchangedb::buffer::replacer::{ClockReplacer, FifoReplacer, LruReplacer};
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::storage::MemoryDiskManager;

use shuttle::scheduler::{PctScheduler, RandomScheduler};
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

// Q-30 regression guard. Before the eviction-flush-ordering fix, uniform-random
// reproduced the race at 100k iterations (PCT found it far faster); both now
// explore clean. A failure here means the swap↔eviction race has regressed.
#[test]
fn shuttle_swap_vs_eviction_random() {
    let scheduler = RandomScheduler::new(100_000);
    Runner::new(scheduler, config_with_stack()).run(swap_vs_eviction_scenario);
}

// Q-30 regression guard. PCT is biased toward bugs needing only a few
// preemptions — it found this race in milliseconds where uniform-random needed
// 100k iterations. This is the regression of record.
#[test]
fn shuttle_swap_vs_eviction_pct() {
    let scheduler = PctScheduler::new(5, 50_000);
    Runner::new(scheduler, config_with_stack()).run(swap_vs_eviction_scenario);
}

/// Q-30 (second root cause): two threads missing the SAME page concurrently
/// must not create a duplicate "ghost" frame.
///
/// `fetch_page_internal` releases the page-table read lock the instant it sees
/// a miss, so two threads can both reach `handle_cache_miss` for the same page,
/// both allocate a frame, both load from disk, and both insert into the page
/// table. The loser's frame then holds the page but is mapped nowhere — a
/// ghost — and evicting a ghost unmaps a live page, surfacing as the marquee's
/// cross-page corruption. No swap is involved; the swap storm only amplified
/// the eviction churn that exposed it.
///
/// The `swap_vs_eviction_scenario` above does NOT exercise this: its two
/// writers each touch the shared page exactly once, so the scheduler is never
/// free to land both of them in the load path for the same page at the same
/// time. This scenario targets it directly — both threads fetch one
/// guaranteed-missing page at once — and detects the ghost via **frame
/// conservation**: with nothing pinned at rest, every frame is free or mapped,
/// so `free + mapped == POOL_SIZE`. A ghost is neither, dropping the sum below
/// pool size; a double-listed frame would push it above.
fn same_page_miss_scenario() {
    let bpm = Arc::new(BufferPoolManager::new(POOL_SIZE, MemoryDiskManager::new()));
    let pages = seed(&bpm); // pool holds the last POOL_SIZE; pages[0] is evicted to disk

    // pages[0] is the coldest page and no longer resident, so fetching it is a
    // guaranteed miss for both threads — the concurrent-load race we target.
    let (target, target_marker) = pages[0];

    let b1 = bpm.clone();
    let t1 = shuttle::thread::spawn(move || {
        if let Ok(g) = b1.fetch_page_read(target) {
            assert_eq!(g.as_slice()[0], target_marker, "byte 0 of target corrupted");
        }
    });
    let b2 = bpm.clone();
    let t2 = shuttle::thread::spawn(move || {
        if let Ok(g) = b2.fetch_page_read(target) {
            assert_eq!(g.as_slice()[0], target_marker, "byte 0 of target corrupted");
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // Frame conservation: a ghost frame is neither free nor mapped.
    let free = bpm.free_frame_count();
    let mapped = bpm.page_count();
    assert_eq!(
        free + mapped,
        POOL_SIZE,
        "ghost frame: free {} + mapped {} != pool {}",
        free,
        mapped,
        POOL_SIZE
    );

    // Content integrity: every page still reads its own marker.
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

/// Q-35: the composite the marquee runs but the pairwise models can't reach —
/// THREE dirty writers over a cyclically-overlapping page set, plus a swapper.
///
/// Why three: every suspicious window in the eviction protocol is guarded by
/// a pin check under `pt.write` — except the handoff in `handle_cache_miss`
/// between `get_free_frame` returning a frame and `frame.pin()`, where the
/// frame is legitimately pin-0 and invisible (unmapped, un-tracked). Reaching
/// that window with a *second* evictor requires: a guard-drop whose deferred
/// `set_evictable(true)` fires stale (it runs outside any page-table lock,
/// and LRU/FIFO insert unconditionally — no membership check), an eviction
/// that completes and hands the frame to a miss handler, and another eviction
/// selecting the same frame — three concurrent actors beyond the swapper.
/// The pairwise models top out at two.
///
/// The cyclic page assignment (w1: 0,1 · w2: 1,2 · w3: 2,0) makes every page
/// shared by exactly two writers and every fetch a likely miss (pool 2,
/// 3 pages), so dirty evictions, same-page misses, and guard drops all stay
/// in flight together while the swapper churns the replacer identity.
fn three_writer_storm_scenario() {
    let bpm = Arc::new(BufferPoolManager::new(POOL_SIZE, MemoryDiskManager::new()));
    let pages = seed(&bpm);

    let w1_pages = vec![pages[0], pages[1]];
    let w2_pages = vec![pages[1], pages[2]];
    let w3_pages = vec![pages[2], pages[0]];

    let b1 = bpm.clone();
    let w1 = shuttle::thread::spawn(move || write_worker(b1, w1_pages));
    let b2 = bpm.clone();
    let w2 = shuttle::thread::spawn(move || write_worker(b2, w2_pages));
    let b3 = bpm.clone();
    let w3 = shuttle::thread::spawn(move || write_worker(b3, w3_pages));

    let bs = bpm.clone();
    let swapper = shuttle::thread::spawn(move || {
        let _ = bs.swap_policy(Box::new(LruReplacer::new(POOL_SIZE)), SwapMode::Cold);
        let _ = bs.swap_policy(Box::new(FifoReplacer::new()), SwapMode::Warm);
    });

    w1.join().unwrap();
    w2.join().unwrap();
    w3.join().unwrap();
    swapper.join().unwrap();

    // Frame conservation: at rest every frame is free or mapped; a ghost or
    // a double-owned frame breaks the sum.
    let free = bpm.free_frame_count();
    let mapped = bpm.page_count();
    assert_eq!(
        free + mapped,
        POOL_SIZE,
        "ghost/double-owned frame: free {} + mapped {} != pool {}",
        free,
        mapped,
        POOL_SIZE
    );

    // Content integrity: every page reads back its own marker.
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

// Q-35 hunt: PCT first — history says it finds BPM races orders of magnitude
// faster than uniform-random (Q-30: milliseconds vs 100k iterations).
#[test]
fn shuttle_three_writer_storm_pct() {
    let scheduler = PctScheduler::new(5, 50_000);
    Runner::new(scheduler, config_with_stack()).run(three_writer_storm_scenario);
}

// Q-35 replay harness. `#[ignore]`d — this is the debugging tool, not CI:
// when a hunt above fails, paste its printed schedule here and run
//   cargo test --features shuttle --release --test bpm_swap_shuttle \
//     shuttle_q35_replay -- --ignored --nocapture
// to get the deterministic `sync_trace!` interleaving. NOTE: a schedule is
// only valid for the exact code it was captured against — any change to the
// BPM's sync operations (locks/atomics) invalidates it. The seed below is
// the SECOND Q-35 schedule (the abandon-path evictability clobber), kept as
// a historical artifact; it predates the fix and will diverge if replayed.
#[test]
#[ignore]
fn shuttle_q35_replay() {
    // shuttle::replay uses the default config; the scenario needs the larger
    // stack, so go through Runner + ReplayScheduler explicitly.
    use shuttle::scheduler::ReplayScheduler;
    let seed = "91038e02fab2cebba0c6bfc08101000000000000000000000000000000000000000000000000\
                0000000000000000000000000000000000002222022224002862222282828828224886824246\
                6464446624266624446426646222222882888822222822624264644462244622224246224244\
                2426226262626666662266662222626222262662262200666666660600000000000000";
    let scheduler = ReplayScheduler::new_from_encoded(seed);
    Runner::new(scheduler, config_with_stack()).run(three_writer_storm_scenario);
}

// Q-35 hunt: uniform-random sweep as the complementary explorer.
#[test]
fn shuttle_three_writer_storm_random() {
    let scheduler = RandomScheduler::new(100_000);
    Runner::new(scheduler, config_with_stack()).run(three_writer_storm_scenario);
}

// Q-30 regression guard for the same-page-miss race (the CI-escaping second
// root cause). Uniform-random sweep.
#[test]
fn shuttle_same_page_miss_random() {
    let scheduler = RandomScheduler::new(100_000);
    Runner::new(scheduler, config_with_stack()).run(same_page_miss_scenario);
}

// Q-30 regression guard. PCT needs only one preemption to put both threads in
// the load path at once, so it finds the ghost near-instantly when present.
#[test]
fn shuttle_same_page_miss_pct() {
    let scheduler = PctScheduler::new(5, 50_000);
    Runner::new(scheduler, config_with_stack()).run(same_page_miss_scenario);
}
