//! LSM-tree benchmark.
//!
//! Mirrors btree_bench.rs for direct comparison between the two engines.
//! Same key count, encoding, batch size, RNG, and predicates.
//!
//! Key differences from B+Tree:
//! - LSM writes go to an in-memory memtable first (fast), then flush to SSTables.
//! - LSM reads check memtable → immutable memtables → L0 SSTables → L1+.
//! - The "insert" benchmark measures memtable + flush throughput.
//! - The "read" benchmark reads from flushed SSTables (not memtable-hot).
//!
//! ## Fairness contract (must match `btree_bench.rs`)
//! - Same key/value sizes (8 / 8 bytes), same key count, same RNG seed.
//! - Same predicates (key_will_vanish, key_will_change).
//! - Engine-direct: no `Database` wrapper, no WAL.
//! - Cache budget: memtable = 4 MB, matching the B+Tree's 1024-frame BPM.
//! - Durability at iter end: insert iter calls `flush_memtable()`, matching
//!   the B+Tree's `bpm.flush_all_pages()`.
//! - End-of-bench prints `level_state()` for level distribution and
//!   on-disk size observability.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use interchangedb::index::lsm::LsmTree;
use tempfile::tempdir;

/// Total keys to insert. Matches btree_bench.
const TOTAL_KEYS: usize = 10_000;

/// Number of keys to read/write per benchmark iteration. Matches btree_bench.
const BATCH_SIZE: usize = 100;

/// Memtable byte budget. Matches the B+Tree's BPM budget
/// (1024 frames * 4 KB = 4 MB) so both engines get the same RAM.
const MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

// ============================================================================
// Key encoding (identical to btree_bench)
// ============================================================================

fn encode_key(val: i64) -> Vec<u8> {
    let unsigned = (val as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes().to_vec()
}

fn encode_value(val: u64) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

// ============================================================================
// Predicates (identical to btree_bench)
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

struct LsmBenchState {
    tree: LsmTree,
    _dir: tempfile::TempDir,
}

fn setup_tree() -> LsmBenchState {
    let dir = tempdir().unwrap();
    let tree = LsmTree::open_with_memtable_size(dir.path(), MEMTABLE_SIZE).unwrap();

    // Bulk insert all keys.
    for key in 0..TOTAL_KEYS {
        let k = encode_key(key as i64);
        let v = encode_value(key as u64);
        tree.put(k, v).unwrap();
    }

    // Flush to SSTables so reads exercise the on-disk path,
    // matching B+Tree where all data lives in BPM-backed pages.
    tree.flush_memtable().unwrap();

    LsmBenchState { tree, _dir: dir }
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_lsm_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_insert");
    group.throughput(Throughput::Elements(TOTAL_KEYS as u64));

    group.bench_function(
        BenchmarkId::new("bulk", format!("{}keys", TOTAL_KEYS)),
        |b| {
            b.iter_with_setup(
                || {
                    // Fresh tree for each iteration.
                    let dir = tempdir().unwrap();
                    let tree = LsmTree::open_with_memtable_size(dir.path(), MEMTABLE_SIZE).unwrap();
                    (tree, dir)
                },
                |(tree, _dir)| {
                    for key in 0..TOTAL_KEYS {
                        let k = encode_key(key as i64);
                        let v = encode_value(key as u64);
                        tree.put(k, v).unwrap();
                    }
                    // Flush to ensure all data hits disk, matching B+Tree
                    // which writes through the BPM to DiskManager.
                    tree.flush_memtable().unwrap();
                },
            );
        },
    );

    group.finish();
}

fn bench_lsm_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_read");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let state = setup_tree();

    // Same LCG as btree_bench for deterministic random keys.
    let mut rng_state: u64 = 42;

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
                    let result = state.tree.get(&k).unwrap();

                    // Non-vanished, non-changed keys should have original value.
                    if !key_will_vanish(key) && !key_will_change(key) {
                        assert!(result.is_some());
                    }
                }
            });
        },
    );

    group.finish();
    let ls = state.tree.level_state();
    eprintln!(
        "[lsm_read] disk_size_bytes={} levels={}",
        ls.total_disk_size(),
        format_levels(&ls)
    );
}

fn bench_lsm_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_write");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let state = setup_tree();

    let mut rng_state: u64 = 99;
    let mut do_insert = false;

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
                            state.tree.put(k, v).unwrap();
                        } else {
                            state.tree.delete(k).unwrap();
                        }
                        count += 1;
                    } else if key_will_change(key) {
                        // Overwrite with new value.
                        rng_state = rng_state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        let k = encode_key(key as i64);
                        let new_val = encode_value(rng_state);
                        state.tree.put(k, new_val).unwrap();
                        count += 1;
                    }
                    key += 1;
                }
                do_insert = !do_insert;
            });
        },
    );

    group.finish();
    let ls = state.tree.level_state();
    eprintln!(
        "[lsm_write] disk_size_bytes={} levels={}",
        ls.total_disk_size(),
        format_levels(&ls)
    );
}

/// Format level table count per level as `L0=2,L1=4,...`.
/// `LevelState::level_size(i)` returns bytes per level; we report it directly.
fn format_levels(ls: &interchangedb::index::lsm::manifest::LevelState) -> String {
    let mut parts = Vec::new();
    for i in 0..7 {
        let bytes = ls.level_size(i);
        if bytes > 0 {
            parts.push(format!("L{}={}B", i, bytes));
        }
    }
    if parts.is_empty() { "empty".into() } else { parts.join(",") }
}

criterion_group!(benches, bench_lsm_insert, bench_lsm_read, bench_lsm_write);
criterion_main!(benches);
