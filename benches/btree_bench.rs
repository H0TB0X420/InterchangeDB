//! B+Tree benchmark.
//!
//! Ported from BusTub's btree_bench.cpp (single-threaded).
//! Measures insert, point lookup, and mixed write throughput.
//!
//! BusTub runs 4 read + 2 write threads for 30s with in-memory disk.
//! We run single-threaded with real disk I/O. Concurrent version will
//! be added when latch crabbing is implemented.
//!
//! ## Fairness contract (must match `lsm_bench.rs`)
//! - Same key/value sizes (8 / 8 bytes), same key count, same RNG seed.
//! - Same predicates (key_will_vanish, key_will_change).
//! - Engine-direct: no `Database` wrapper, no WAL.
//! - Cache budget: BPM = 1024 frames * 4 KB ≈ 4 MB. LSM matches via
//!   `LsmTree::open_with_memtable_size(_, 4 * 1024 * 1024)`.
//! - Durability at iter end: insert iter calls `bpm.flush_all_pages()`
//!   to match LSM's explicit `flush_memtable()`.
//! - End-of-bench prints `bpm.stats().snapshot()` for cache hit rate
//!   and pages-read/written observability.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::PageId;
use interchangedb::index::btree::{BTree, BTreeHeaderPage};
use interchangedb::storage::FileDiskManager;
use tempfile::tempdir;

/// Total keys to insert. BusTub uses 100K; we use 10K to keep bench
/// setup time reasonable with real disk I/O.
const TOTAL_KEYS: usize = 10_000;

/// BPM size — large enough to hold all pages in memory.
/// With leaf_max=203, 10K keys ≈ 50 leaves + a few internals.
const BPM_SIZE: usize = 1024;

/// Leaf and internal max sizes for 8-byte keys/values in a 4096-byte page.
/// Must leave room for 1 temporary overflow entry during split.
///   Leaf:     floor((4096 - 20) / 20) - 1 = 202
///   Internal: floor((4096 - 12) / 14) - 1 = 290
const LEAF_MAX: u16 = 202;
const INTERNAL_MAX: u16 = 290;

/// Number of keys to read/write per benchmark iteration.
const BATCH_SIZE: usize = 100;

// ============================================================================
// Key encoding (same as other tests)
// ============================================================================

fn encode_key(val: i64) -> [u8; 8] {
    let unsigned = (val as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes()
}

fn encode_value(val: u64) -> [u8; 8] {
    val.to_le_bytes()
}

// ============================================================================
// Predicates (from BusTub)
// ============================================================================

/// Keys divisible by 7 will be deleted and re-inserted.
fn key_will_vanish(key: usize) -> bool {
    key % 7 == 0
}

/// Keys divisible by 5 will be overwritten with new values.
fn key_will_change(key: usize) -> bool {
    key % 5 == 0
}

// ============================================================================
// Setup
// ============================================================================

struct BTreeBenchState {
    bpm: BufferPoolManager,
    header_id: PageId,
    _dir: tempfile::TempDir,
}

fn setup_tree() -> BTreeBenchState {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bench.db");
    let dm = FileDiskManager::create(&path).unwrap();
    let bpm = BufferPoolManager::new(BPM_SIZE, dm);

    let header_page = bpm.new_page().unwrap();
    let header_id = header_page.page_id();
    {
        let mut guard = header_page;
        BTreeHeaderPage::new().encode(guard.as_mut_slice());
    }

    // Bulk insert all keys.
    let tree = BTree::with_sizes(&bpm, header_id, LEAF_MAX, INTERNAL_MAX);
    for key in 0..TOTAL_KEYS {
        let k = encode_key(key as i64);
        let v = encode_value(key as u64);
        tree.insert(&k, &v).unwrap();
    }

    BTreeBenchState {
        bpm,
        header_id,
        _dir: dir,
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_btree_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_insert");
    group.throughput(Throughput::Elements(TOTAL_KEYS as u64));

    group.bench_function(
        BenchmarkId::new("bulk", format!("{}keys", TOTAL_KEYS)),
        |b| {
            b.iter_with_setup(
                || {
                    // Fresh tree for each iteration.
                    let dir = tempdir().unwrap();
                    let path = dir.path().join("bench.db");
                    let dm = FileDiskManager::create(&path).unwrap();
                    let bpm = BufferPoolManager::new(BPM_SIZE, dm);
                    let header_page = bpm.new_page().unwrap();
                    let hid = header_page.page_id();
                    {
                        let mut g = header_page;
                        BTreeHeaderPage::new().encode(g.as_mut_slice());
                    }
                    (bpm, hid, dir)
                },
                |(bpm, hid, _dir)| {
                    let tree = BTree::with_sizes(&bpm, hid, LEAF_MAX, INTERNAL_MAX);
                    for key in 0..TOTAL_KEYS {
                        let k = encode_key(key as i64);
                        let v = encode_value(key as u64);
                        tree.insert(&k, &v).unwrap();
                    }
                    // Match LSM's explicit `flush_memtable()`: force all dirty
                    // pages to disk inside the timed region. Without this, the
                    // B+Tree's "insert" measurement excludes the durability cost
                    // that the LSM bench is paying.
                    bpm.flush_all_pages().unwrap();
                },
            );
        },
    );

    group.finish();
}

fn bench_btree_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_read");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let state = setup_tree();
    let tree = BTree::with_sizes(&state.bpm, state.header_id, LEAF_MAX, INTERNAL_MAX);

    // Simple LCG for deterministic random keys.
    let mut rng_state: u64 = 42;

    // Reset BPM stats so the read snapshot reflects only this benchmark
    // (setup_tree already did the inserts and warmed the cache).
    state.bpm.stats().reset();

    group.bench_function(
        BenchmarkId::new("random_lookup", format!("{}keys", TOTAL_KEYS)),
        |b| {
            b.iter(|| {
                for _ in 0..BATCH_SIZE {
                    rng_state = rng_state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let key = (rng_state >> 33) as usize % TOTAL_KEYS;
                    let k = encode_key(key as i64);
                    let result = tree.get(&k).unwrap();

                    // Non-vanished, non-changed keys should have original value.
                    if !key_will_vanish(key) && !key_will_change(key) {
                        assert!(result.is_some());
                    }
                }
            });
        },
    );

    group.finish();
    eprintln!("[btree_read] {}", state.bpm.stats().snapshot());
}

fn bench_btree_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_write");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let state = setup_tree();
    let tree = BTree::with_sizes(&state.bpm, state.header_id, LEAF_MAX, INTERNAL_MAX);

    let mut rng_state: u64 = 99;
    let mut do_insert = false;

    state.bpm.stats().reset();

    group.bench_function(
        BenchmarkId::new("mixed_delete_insert", format!("{}keys", TOTAL_KEYS)),
        |b| {
            b.iter(|| {
                // Pick a random starting key.
                rng_state = rng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let base_key = (rng_state >> 33) as usize % TOTAL_KEYS;

                let mut count = 0;
                let mut key = base_key;
                while key < TOTAL_KEYS && count < BATCH_SIZE {
                    if key_will_vanish(key) {
                        let k = encode_key(key as i64);
                        let v = encode_value(key as u64);
                        if do_insert {
                            tree.insert(&k, &v).unwrap();
                        } else {
                            tree.delete(&k).unwrap();
                        }
                        count += 1;
                    } else if key_will_change(key) {
                        // Overwrite with new value.
                        rng_state = rng_state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        let k = encode_key(key as i64);
                        let new_val = encode_value(rng_state);
                        tree.insert(&k, &new_val).unwrap();
                        count += 1;
                    }
                    key += 1;
                }
                do_insert = !do_insert;
            });
        },
    );

    group.finish();
    eprintln!("[btree_write] {}", state.bpm.stats().snapshot());
}

criterion_group!(
    benches,
    bench_btree_insert,
    bench_btree_read,
    bench_btree_write
);
criterion_main!(benches);
