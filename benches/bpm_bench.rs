//! Buffer Pool Manager benchmark.
//!
//! Ported from BusTub's bpm_bench.cpp (single-threaded).
//! Measures page scan and random get-modify throughput.
//!
//! BusTub runs 8 scan + 8 get threads for 30s with in-memory disk.
//! We run single-threaded with real disk I/O. Concurrent version will
//! be added when thread-safe BPM is implemented.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::PageId;
use interchangedb::storage::FileDiskManager;
use tempfile::tempdir;

const PAGE_COUNT: usize = 640;
const BPM_SIZE: usize = 64;

// ============================================================================
// Page data helpers (matches BusTub's BusTubBenchPageHeader layout)
// ============================================================================

/// Write seed and verification data into a page.
///
/// Layout: [seed:u64][page_idx:u64][...data...]
/// Sets data[seed % 4000] = seed % 256.
fn modify_page(data: &mut [u8], page_idx: usize, seed: u64) {
    data[0..8].copy_from_slice(&seed.to_le_bytes());
    data[8..16].copy_from_slice(&(page_idx as u64).to_le_bytes());
    let offset = 16 + (seed % 4000) as usize;
    data[offset] = (seed % 256) as u8;
}

/// Verify page data consistency (seed-agnostic — reads seed from page).
fn check_page_consistent(data: &[u8], _page_idx: usize) {
    let seed = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let offset = 16 + (seed % 4000) as usize;
    let actual = data[offset];
    let expected = (seed % 256) as u8;
    assert_eq!(actual, expected, "page data inconsistent");
}

/// Verify page data with expected seed.
fn check_page_with_seed(data: &[u8], page_idx: usize, seed: u64) {
    let stored_seed = u64::from_le_bytes(data[0..8].try_into().unwrap());
    assert_eq!(stored_seed, seed, "page {} seed mismatch", page_idx);
}

// ============================================================================
// Setup
// ============================================================================

struct BpmBenchState {
    bpm: BufferPoolManager,
    page_ids: Vec<PageId>,
    _dir: tempfile::TempDir,
}

fn setup_bench(pool_size: usize, page_count: usize) -> BpmBenchState {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bench.db");
    let dm = FileDiskManager::create(&path).unwrap();
    let bpm = BufferPoolManager::new(pool_size, dm);

    let mut page_ids = Vec::with_capacity(page_count);
    for i in 0..page_count {
        let page = bpm.new_page().unwrap();
        let pid = page.page_id();
        {
            let mut guard = page;
            modify_page(guard.as_mut_slice(), i, 0);
        }
        page_ids.push(pid);
    }

    BpmBenchState { bpm, page_ids, _dir: dir }
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_bpm_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("bpm_scan");
    group.throughput(Throughput::Elements(PAGE_COUNT as u64));

    let state = setup_bench(BPM_SIZE, PAGE_COUNT);

    group.bench_function(
        BenchmarkId::new("sequential", format!("{}pages_{}pool", PAGE_COUNT, BPM_SIZE)),
        |b| {
            b.iter(|| {
                // One full scan of all pages.
                for (i, &pid) in state.page_ids.iter().enumerate() {
                    let guard = state.bpm.fetch_page_read(pid).unwrap();
                    check_page_consistent(guard.as_slice(), i);
                }
            });
        },
    );

    group.finish();
}

fn bench_bpm_get_modify(c: &mut Criterion) {
    let mut group = c.benchmark_group("bpm_get_modify");

    let batch_size: usize = 100;
    group.throughput(Throughput::Elements(batch_size as u64));

    let state = setup_bench(BPM_SIZE, PAGE_COUNT);

    // Track seeds for each page to verify consistency.
    let mut seeds: Vec<u64> = vec![0; PAGE_COUNT];

    // Simple LCG for deterministic random access.
    let mut rng_state: u64 = 12345;

    group.bench_function(
        BenchmarkId::new("random_rw", format!("{}pages_{}pool", PAGE_COUNT, BPM_SIZE)),
        |b| {
            b.iter(|| {
                for _ in 0..batch_size {
                    // Pseudo-random page index.
                    rng_state = rng_state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let page_idx = (rng_state >> 33) as usize % PAGE_COUNT;

                    let mut guard = state.bpm.fetch_page_write(state.page_ids[page_idx]).unwrap();
                    check_page_with_seed(guard.as_slice(), page_idx, seeds[page_idx]);
                    seeds[page_idx] += 1;
                    modify_page(guard.as_mut_slice(), page_idx, seeds[page_idx]);
                }
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_bpm_scan, bench_bpm_get_modify);
criterion_main!(benches);
