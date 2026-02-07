//! Eviction policy benchmarks.
//!
//! Compares all six eviction policies across four workload patterns.
//! Tracks both throughput (ops/sec via criterion) and hit rate.
//!
//! ## Workloads
//! - **Sequential**: linear scan through pages 0..N
//! - **Random**: uniform random page access
//! - **Zipfian**: skewed access — a few hot pages get most accesses
//! - **LoopingScan**: repeated sequential scans (exposes LRU thrashing)
//! - **ScanPlusHot**: hot pages mixed with a large sequential scan (classic scan resistance test)
//!
//! ## Expected relative performance
//! | Workload     | Best performers         | Worst performers       |
//! |-------------|------------------------|------------------------|
//! | Sequential  | All similar (no reuse) | —                      |
//! | Random      | All similar            | —                      |
//! | Zipfian     | LRU, LRU-K, 2Q, ARC   | FIFO                   |
//! | LoopingScan | 2Q                     | LRU, FIFO, Clock       |
//! | ScanPlusHot | LRU-K, 2Q, ARC        | FIFO, Clock, LRU       |
//!
//! ## Running
//! ```sh
//! cargo bench -- --output-format bencher   # criterion benchmarks
//! cargo bench -- summary                   # hit rate summary table
//! ```

use std::collections::HashMap;

use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};

use interchangedb::buffer::replacer::*;
use interchangedb::common::{FrameId, PageId};

// ============================================================================
// Workload definitions
// ============================================================================

/// Workload type for benchmarking eviction policies.
#[derive(Debug, Clone, Copy)]
enum Workload {
    /// Linear scan: pages 0, 1, 2, ..., N-1, 0, 1, ...
    Sequential,
    /// Uniform random page access.
    Random,
    /// Skewed access: ~80% of accesses hit ~20% of pages.
    Zipfian,
    /// Repeated sequential scans: 0..S, 0..S, 0..S, ...
    /// where S > pool_size. Exposes LRU thrashing.
    LoopingScan,
    /// Hot pages mixed with a sequential scan.
    /// ~50% of accesses hit a small hot set, ~50% are a linear scan
    /// over a range larger than the pool. Scan-resistant policies
    /// protect the hot set; LRU/FIFO/Clock evict hot pages during sweeps.
    ScanPlusHot,
}

impl Workload {
    fn name(&self) -> &'static str {
        match self {
            Workload::Sequential => "sequential",
            Workload::Random => "random",
            Workload::Zipfian => "zipfian",
            Workload::LoopingScan => "looping_scan",
            Workload::ScanPlusHot => "scan+hot",
        }
    }

    fn all() -> &'static [Workload] {
        &[
            Workload::Sequential,
            Workload::Random,
            Workload::Zipfian,
            Workload::LoopingScan,
            Workload::ScanPlusHot,
        ]
    }
}

/// Generates a sequence of page accesses for a given workload.
///
/// `pool_size` is used by LoopingScan to set the scan range just above the pool capacity.
fn generate_access_sequence(
    workload: Workload,
    num_pages: usize,
    pool_size: usize,
    length: usize,
) -> Vec<PageId> {
    let mut seq = Vec::with_capacity(length);

    match workload {
        Workload::Sequential => {
            for i in 0..length {
                seq.push(PageId::new((i % num_pages) as u32));
            }
        }
        Workload::Random => {
            // Simple LCG for deterministic "random" sequence
            let mut state: u64 = 12345;
            for _ in 0..length {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let page = ((state >> 33) as usize) % num_pages;
                seq.push(PageId::new(page as u32));
            }
        }
        Workload::Zipfian => {
            // Approximate Zipfian: pick from hot set (20% of pages) ~80% of the time
            let hot_set_size = (num_pages / 5).max(1);
            let mut state: u64 = 67890;
            for _ in 0..length {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let r = ((state >> 33) as usize) % 100;
                let page = if r < 80 {
                    // Hot page
                    ((state >> 17) as usize) % hot_set_size
                } else {
                    // Cold page
                    hot_set_size + ((state >> 17) as usize) % (num_pages - hot_set_size).max(1)
                };
                seq.push(PageId::new(page as u32));
            }
        }
        Workload::LoopingScan => {
            // Scan range slightly larger than pool, creating repeated full scans.
            // This is the classic scenario where LRU thrashes but scan-resistant
            // policies (LRU-K, 2Q, ARC) can retain some useful pages.
            // The margin is kept small (12.5%) so adaptive algorithms have a
            // realistic chance to converge — too large and everything thrashes.
            let scan_size = pool_size + pool_size / 8;
            for i in 0..length {
                seq.push(PageId::new((i % scan_size) as u32));
            }
        }
        Workload::ScanPlusHot => {
            // The textbook scan-resistance scenario: alternating phases of
            // a sequential scan burst and hot page accesses.
            //
            // 1. SCAN PHASE: sequential scan of pool_size cold pages
            // 2. HOT PHASE: access each hot page twice (gives LRU-K K=2 history)
            // 3. Repeat
            //
            // Why this works:
            // - FIFO/Clock/LRU: scan burst pushes hot pages off the end,
            //   causing misses in every hot phase.
            // - LRU-K: hot pages have K=2 accesses (finite distance), scan
            //   pages have 1 access (infinite distance) → scan evicted first.
            // - 2Q: hot pages promoted to Am via ghost hits after first eviction,
            //   scan stays in A1in → Am protected during scan bursts.
            // - ARC: hot pages in T2 (frequent), scan in T1 (recent).
            //   REPLACE evicts from T1 when |T1| > p, protecting T2.
            let hot_set_size = pool_size / 4; // 16 hot pages
            let scan_burst_len = pool_size; // scan burst = pool capacity
            let cold_range = num_pages - hot_set_size;
            let mut scan_cursor = 0usize;
            let mut i = 0;

            while i < length {
                // Scan phase: sequential scan over cold pages
                for _ in 0..scan_burst_len {
                    if i >= length { break; }
                    let page = hot_set_size + (scan_cursor % cold_range);
                    scan_cursor += 1;
                    seq.push(PageId::new(page as u32));
                    i += 1;
                }

                // Hot phase: access each hot page twice
                // Two accesses gives LRU-K (K=2) a finite backward distance
                for _ in 0..2 {
                    for h in 0..hot_set_size {
                        if i >= length { break; }
                        seq.push(PageId::new(h as u32));
                        i += 1;
                    }
                }
            }
        }
    }

    seq
}

// ============================================================================
// Simulated buffer pool (lightweight, no disk I/O)
// ============================================================================

/// Lightweight BPM simulator for benchmarking eviction policies in isolation.
///
/// No disk I/O, no locking — just the eviction policy logic and a page table.
struct SimulatedPool {
    policy: Box<dyn EvictionPolicy>,
    page_table: HashMap<PageId, FrameId>,
    frame_to_page: Vec<Option<PageId>>,
    free_list: Vec<FrameId>,
    hits: u64,
    misses: u64,
}

impl SimulatedPool {
    fn new(policy: Box<dyn EvictionPolicy>, pool_size: usize) -> Self {
        let free_list: Vec<FrameId> = (0..pool_size).map(FrameId::new).collect();
        Self {
            policy,
            page_table: HashMap::new(),
            frame_to_page: vec![None; pool_size],
            free_list,
            hits: 0,
            misses: 0,
        }
    }

    fn access(&mut self, page_id: PageId) {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            // Cache hit
            self.hits += 1;
            self.policy.record_access(frame_id, page_id);
            // Simulate pin/unpin cycle: briefly pin then unpin
            self.policy.set_evictable(frame_id, true);
            return;
        }

        // Cache miss
        self.misses += 1;

        let frame_id = if let Some(fid) = self.free_list.pop() {
            fid
        } else {
            // Evict
            let victim = self.policy.evict().expect("no evictable frames");
            if let Some(old_page) = self.frame_to_page[victim.0].take() {
                self.page_table.remove(&old_page);
            }
            victim
        };

        self.page_table.insert(page_id, frame_id);
        self.frame_to_page[frame_id.0] = Some(page_id);
        self.policy.record_access(frame_id, page_id);
        self.policy.set_evictable(frame_id, true);
    }

    fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

// ============================================================================
// Policy constructors
// ============================================================================

struct PolicyFactory {
    name: &'static str,
    make: fn(usize) -> Box<dyn EvictionPolicy>,
}

fn policy_factories() -> Vec<PolicyFactory> {
    vec![
        PolicyFactory {
            name: "fifo",
            make: |_pool_size| Box::new(FifoReplacer::new()),
        },
        PolicyFactory {
            name: "clock",
            make: |_pool_size| Box::new(ClockReplacer::new()),
        },
        PolicyFactory {
            name: "lru",
            make: |pool_size| Box::new(LruReplacer::new(pool_size)),
        },
        PolicyFactory {
            name: "lru-k",
            make: |_pool_size| Box::new(LruKReplacer::new_lru2()),
        },
        PolicyFactory {
            name: "2q",
            make: |pool_size| Box::new(TwoQReplacer::new(pool_size)),
        },
        PolicyFactory {
            name: "arc",
            make: |pool_size| Box::new(ArcReplacer::new(pool_size)),
        },
    ]
}

// ============================================================================
// Benchmark parameters
// ============================================================================

const POOL_SIZE: usize = 64;
const NUM_PAGES: usize = 256;
const ACCESS_LENGTH: usize = 100_000;

// ============================================================================
// Criterion benchmarks
// ============================================================================

fn bench_eviction_policies(c: &mut Criterion) {
    for workload in Workload::all() {
        let seq = generate_access_sequence(*workload, NUM_PAGES, POOL_SIZE, ACCESS_LENGTH);

        let mut group = c.benchmark_group(workload.name());
        group.throughput(Throughput::Elements(ACCESS_LENGTH as u64));

        for factory in policy_factories() {
            group.bench_with_input(
                BenchmarkId::new(factory.name, ""),
                &seq,
                |b, seq| {
                    b.iter(|| {
                        let policy = (factory.make)(POOL_SIZE);
                        let mut pool = SimulatedPool::new(policy, POOL_SIZE);
                        for &page_id in seq {
                            pool.access(page_id);
                        }
                        pool.hit_rate()
                    });
                },
            );
        }

        group.finish();
    }
}

// ============================================================================
// Hit rate summary report
// ============================================================================

/// Run all policy × workload combinations and print a markdown hit rate table.
fn print_hit_rate_summary() {
    let workloads = Workload::all();
    let factories = policy_factories();

    // Header
    print!("| {:>10} |", "Policy");
    for w in workloads {
        print!(" {:>13} |", w.name());
    }
    println!();

    // Separator
    print!("|{:-<12}|", "");
    for _ in workloads {
        print!("{:-<15}|", "");
    }
    println!();

    // Rows
    for factory in &factories {
        print!("| {:>10} |", factory.name);
        for workload in workloads {
            let seq = generate_access_sequence(*workload, NUM_PAGES, POOL_SIZE, ACCESS_LENGTH);
            let policy = (factory.make)(POOL_SIZE);
            let mut pool = SimulatedPool::new(policy, POOL_SIZE);
            for &page_id in &seq {
                pool.access(page_id);
            }
            print!(" {:>12.1}% |", pool.hit_rate() * 100.0);
        }
        println!();
    }
}

criterion_group!(benches, bench_eviction_policies);

fn main() {
    // Check if user wants the summary report instead of criterion benchmarks
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "summary") {
        print_hit_rate_summary();
        return;
    }

    // Run criterion benchmarks (default)
    // criterion_main! equivalent for custom main
    benches();
    Criterion::default().configure_from_args().final_summary();
}
