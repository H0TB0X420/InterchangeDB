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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use interchangedb::index::lsm::LsmTree;
use tempfile::tempdir;

/// Total keys to insert. Matches btree_bench.
const TOTAL_KEYS: usize = 10_000;

/// Number of keys to read/write per benchmark iteration. Matches btree_bench.
const BATCH_SIZE: usize = 100;

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
    let tree = LsmTree::open(dir.path()).unwrap();

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
                    let tree = LsmTree::open(dir.path()).unwrap();
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
}

criterion_group!(benches, bench_lsm_insert, bench_lsm_read, bench_lsm_write);
criterion_main!(benches);
