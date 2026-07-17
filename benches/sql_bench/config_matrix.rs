//! Q-33 / stability.md pillar G: head-to-head configuration benchmarks. The
//! same seeded workload that the correctness matrix *asserts* (testkit
//! differentials) is *measured* here across every configuration — so a new
//! policy or engine gets a comparison slot for free, and correctness and
//! performance share one config registry and one workload.
//!
//! Run: `cargo bench --bench config_matrix`. Each criterion group puts every
//! config in one comparison chart.

use criterion::{criterion_group, BatchSize, Criterion};

use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::MemoryDiskManager;

use testkit::workload;

/// B-tree throughput under each eviction policy, with a pool small enough that
/// eviction is on the hot path — the marquee comparison. Same workload for all
/// six policies; only the policy differs.
fn by_policy(c: &mut Criterion) {
    let ops = workload::seeded(0x1234_5678, 2_000, 600);
    let mut group = c.benchmark_group("btree_apply_2k/by_policy");
    for (name, make) in testkit::policy::makers() {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let bpm = BufferPoolManager::new(8, MemoryDiskManager::new());
                    bpm.swap_policy(make(), SwapMode::Cold);
                    BTreeEngine::new(bpm).unwrap()
                },
                |engine| workload::apply(&engine, &ops),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Engine throughput head-to-head — the same workload on each storage engine.
fn by_engine(c: &mut Criterion) {
    let ops = workload::seeded(0x1234_5678, 2_000, 600);
    let mut group = c.benchmark_group("apply_2k/by_engine");
    macro_rules! bench_engine {
        ($name:ident, $ty:ty, $ctor:path) => {
            group.bench_function(stringify!($name), |b| {
                b.iter_batched(
                    $ctor,
                    |built| workload::apply(built.get(), &ops),
                    BatchSize::SmallInput,
                );
            });
        };
    }
    testkit::for_each_engine!(bench_engine);
    group.finish();
}

criterion_group!(benches, by_policy, by_engine);
