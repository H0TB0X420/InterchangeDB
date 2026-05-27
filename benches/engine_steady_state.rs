//! Sustained-load tail-latency comparison — B+Tree vs LSM.
//!
//! Mean throughput numbers hide the "tax" each engine pays for its design
//! choice. This bench surfaces it by recording every operation's latency
//! during a sustained run and reporting the percentiles users actually feel
//! (p99, p99.9, p99.99).
//!
//! ## What we expect
//! - B+Tree: relatively uniform latency. Tail spikes correspond to root
//!   splits and BPM evictions of dirty pages.
//! - LSM: bursty latency. Most ops hit the memtable (fast), but writes that
//!   trigger a flush, or reads that have to probe many SSTables, spike hard.
//!   Compaction storms widen p99.99 considerably.
//!
//! ## Method
//! - Pre-fill each engine to ~10× cache-budget so reads exercise on-disk
//!   pages and the LSM is past L0→L1 compaction onset.
//! - For each (engine, workload), run for a fixed wall-clock window
//!   (`RUN_SECS`), recording every op's latency until either the window
//!   expires or the histogram cap is reached.
//! - Three workloads:
//!   - `uniform_writes` — 100% writes, key uniform over `[0, n_keys)`
//!   - `read_heavy_95_5` — 95% reads, 5% writes
//!   - `mixed_50_50` — 50% reads, 50% writes
//!
//! ## Output
//! - Markdown table to stdout with ops/s, p50, p99, p99.9, p99.99, max.
//! - CSV at `target/bench-results/engine_steady_state.csv`.

use interchangedb::buffer::replacer::ArcReplacer;
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::index::btree::{BTree, BTreeHeaderPage};
use interchangedb::index::lsm::LsmTree;
use interchangedb::storage::FileDiskManager;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[path = "common/mod.rs"]
mod common;

use common::budget::{bytes_to_pool, PAGE_SIZE};
use common::encoding::{encode_key_i64, encode_key_vec, encode_value_u64, encode_value_vec};
use common::hist::LatencyHistogram;
use common::output::{results_path, CsvWriter, MarkdownTable};
use common::rng::Lcg64;

/// Cache budget shared by both engines.
const CACHE_BYTES: usize = 256 * 1024;

/// Pre-fill ratio over cache. 10× ensures reads miss the cache, and the LSM
/// has triggered compaction at least once before the timed run begins.
const PREFILL_RATIO: usize = 10;

/// Bytes per record (8-byte key + 8-byte value).
const RECORD_BYTES: usize = 16;

/// Wall-clock budget per (engine, workload) cell.
/// 30 s × 3 workloads × 2 engines = 3 min of timed work, plus pre-fill.
const RUN_SECS: u64 = 30;

/// Per-cell histogram cap (samples). At 8 bytes/sample this is 16 MB —
/// plenty for accurate p99.99 estimates and bounded memory use.
const HIST_CAP: usize = 2_000_000;

/// B+Tree page-layout constants. Match `btree_bench.rs`.
const LEAF_MAX: u16 = 202;
const INTERNAL_MAX: u16 = 290;

#[derive(Copy, Clone)]
enum Workload {
    UniformWrites,
    ReadHeavy95_5,
    Mixed50_50,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Workload::UniformWrites => "uniform_writes",
            Workload::ReadHeavy95_5 => "read_heavy_95_5",
            Workload::Mixed50_50 => "mixed_50_50",
        }
    }

    /// Probability that a given op should be a write, in `[0.0, 1.0]`.
    fn write_prob(self) -> f64 {
        match self {
            Workload::UniformWrites => 1.0,
            Workload::ReadHeavy95_5 => 0.05,
            Workload::Mixed50_50 => 0.5,
        }
    }
}

const WORKLOADS: &[Workload] = &[
    Workload::UniformWrites,
    Workload::ReadHeavy95_5,
    Workload::Mixed50_50,
];

fn main() {
    let n_keys = (CACHE_BYTES / RECORD_BYTES) * PREFILL_RATIO;
    let bpm_frames = bytes_to_pool(CACHE_BYTES);

    println!(
        "engine_steady_state: cache={} KB ({} BPM frames @ {} B), prefill={} keys ({}× cache), run={} s/cell",
        CACHE_BYTES / 1024,
        bpm_frames,
        PAGE_SIZE,
        n_keys,
        PREFILL_RATIO,
        RUN_SECS,
    );

    let csv_path = results_path("engine_steady_state");
    let mut csv = CsvWriter::create(
        &csv_path,
        &[
            "engine",
            "workload",
            "ops",
            "ops_per_sec",
            "p50_us",
            "p99_us",
            "p999_us",
            "p9999_us",
            "max_us",
            "samples_dropped",
        ],
    );
    let mut table = MarkdownTable::new(
        &[
            "Engine", "Workload", "ops/s", "p50 µs", "p99 µs", "p99.9 µs", "p99.99 µs", "max µs",
        ],
        &[false, false, true, true, true, true, true, true],
    );

    for &workload in WORKLOADS {
        eprintln!("\n=== workload: {} ===", workload.name());

        // B+Tree
        eprintln!("[btree] pre-fill...");
        let (btree_results, btree_drop) = run_btree(n_keys, bpm_frames, workload);
        emit_row(
            &mut table,
            &mut csv,
            "btree",
            workload,
            &btree_results,
            btree_drop,
        );

        // LSM
        eprintln!("[lsm  ] pre-fill...");
        let (lsm_results, lsm_drop) = run_lsm(n_keys, CACHE_BYTES, workload);
        emit_row(&mut table, &mut csv, "lsm", workload, &lsm_results, lsm_drop);
    }

    println!("\n=== engine_steady_state summary ===");
    table.print();
    println!("\nCSV: {}", csv_path.display());
}

/// One cell's measured outcome.
struct CellResult {
    ops: u64,
    ops_per_sec: f64,
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    p9999_ns: u64,
    max_ns: u64,
}

fn emit_row(
    table: &mut MarkdownTable,
    csv: &mut CsvWriter,
    engine: &str,
    workload: Workload,
    r: &CellResult,
    samples_dropped: u64,
) {
    let to_us = |ns: u64| ns as f64 / 1_000.0;
    eprintln!(
        "  {} {} : ops/s={:.0}  p50={:.1}µs p99={:.1}µs p99.9={:.1}µs p99.99={:.1}µs max={:.1}µs (dropped {} samples)",
        engine,
        workload.name(),
        r.ops_per_sec,
        to_us(r.p50_ns),
        to_us(r.p99_ns),
        to_us(r.p999_ns),
        to_us(r.p9999_ns),
        to_us(r.max_ns),
        samples_dropped,
    );

    csv.row(&[
        engine,
        workload.name(),
        &r.ops.to_string(),
        &format!("{:.0}", r.ops_per_sec),
        &format!("{:.2}", to_us(r.p50_ns)),
        &format!("{:.2}", to_us(r.p99_ns)),
        &format!("{:.2}", to_us(r.p999_ns)),
        &format!("{:.2}", to_us(r.p9999_ns)),
        &format!("{:.2}", to_us(r.max_ns)),
        &samples_dropped.to_string(),
    ]);

    table.row(&[
        engine,
        workload.name(),
        &format!("{:.0}", r.ops_per_sec),
        &format!("{:.1}", to_us(r.p50_ns)),
        &format!("{:.1}", to_us(r.p99_ns)),
        &format!("{:.1}", to_us(r.p999_ns)),
        &format!("{:.1}", to_us(r.p9999_ns)),
        &format!("{:.1}", to_us(r.max_ns)),
    ]);
}

/// Run one B+Tree (workload, pre-fill, timed) cell.
fn run_btree(n_keys: usize, bpm_frames: usize, workload: Workload) -> (CellResult, u64) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("steady.db");
    let dm = FileDiskManager::create(&path).unwrap();
    let bpm = BufferPoolManager::new(bpm_frames, dm);
    let _ = bpm.swap_policy(Box::new(ArcReplacer::new(bpm_frames)), SwapMode::Cold);

    let header_page = bpm.new_page().unwrap();
    let header_id = header_page.page_id();
    {
        let mut g = header_page;
        BTreeHeaderPage::new().encode(g.as_mut_slice());
    }

    let tree = BTree::with_sizes(&bpm, header_id, LEAF_MAX, INTERNAL_MAX);

    // Pre-fill.
    for key in 0..n_keys {
        let k = encode_key_i64(key as i64);
        let v = encode_value_u64(key as u64);
        tree.insert(&k, &v).unwrap();
    }
    bpm.flush_all_pages().unwrap();

    // Reset stats so the timed phase is clean.
    bpm.stats().reset();

    // Timed phase.
    let mut hist = LatencyHistogram::with_cap(HIST_CAP);
    let mut ops = 0u64;
    let mut rng = Lcg64::new(0xDEAD_BEEF);
    let write_threshold = (workload.write_prob() * u64::MAX as f64) as u64;
    let deadline = Instant::now() + Duration::from_secs(RUN_SECS);
    let start = Instant::now();

    while Instant::now() < deadline {
        let key = rng.gen_range(n_keys);
        let is_write = rng.next_u64() < write_threshold;
        let k = encode_key_i64(key as i64);

        let t0 = Instant::now();
        if is_write {
            // For writes, overwrite via insert-with-same-value path: the
            // B+Tree returns false on duplicate key, which is fine — what we
            // want to measure is the leaf-touch cost, not the optimistic
            // delete + reinsert dance from `BTreeEngine`.
            let v = encode_value_u64(rng.next_u64());
            let _ = tree.insert(&k, &v);
        } else {
            let _ = tree.get(&k).unwrap();
        }
        hist.record(t0.elapsed());
        ops += 1;
    }

    let elapsed_secs = start.elapsed().as_secs_f64();

    eprintln!(
        "    btree bpm: hits={} misses={} evictions={} pages_read={} pages_written={} hit_rate={:.1}%",
        bpm.stats().cache_hits.load(std::sync::atomic::Ordering::Relaxed),
        bpm.stats().cache_misses.load(std::sync::atomic::Ordering::Relaxed),
        bpm.stats().evictions.load(std::sync::atomic::Ordering::Relaxed),
        bpm.stats().pages_read.load(std::sync::atomic::Ordering::Relaxed),
        bpm.stats().pages_written.load(std::sync::atomic::Ordering::Relaxed),
        bpm.stats().hit_rate() * 100.0,
    );

    let dropped = hist.dropped();
    (build_cell_result(&mut hist, ops, elapsed_secs), dropped)
}

/// Run one LSM (workload, pre-fill, timed) cell.
fn run_lsm(n_keys: usize, memtable_bytes: usize, workload: Workload) -> (CellResult, u64) {
    let dir = tempdir().unwrap();
    let tree = LsmTree::open_with_memtable_size(dir.path(), memtable_bytes).unwrap();

    // Pre-fill.
    for key in 0..n_keys {
        let k = encode_key_vec(key as i64);
        let v = encode_value_vec(key as u64);
        tree.put(k, v).unwrap();
    }
    tree.flush_memtable().unwrap();

    // Timed phase.
    let mut hist = LatencyHistogram::with_cap(HIST_CAP);
    let mut ops = 0u64;
    let mut rng = Lcg64::new(0xDEAD_BEEF);
    let write_threshold = (workload.write_prob() * u64::MAX as f64) as u64;
    let deadline = Instant::now() + Duration::from_secs(RUN_SECS);
    let start = Instant::now();

    while Instant::now() < deadline {
        let key = rng.gen_range(n_keys);
        let is_write = rng.next_u64() < write_threshold;
        let k = encode_key_vec(key as i64);

        let t0 = Instant::now();
        if is_write {
            let v = encode_value_vec(rng.next_u64());
            tree.put(k, v).unwrap();
        } else {
            let _ = tree.get(&k).unwrap();
        }
        hist.record(t0.elapsed());
        ops += 1;
    }

    let elapsed_secs = start.elapsed().as_secs_f64();

    let ls = tree.level_state();
    let mut levels = Vec::new();
    for i in 0..7 {
        let bytes = ls.level_size(i);
        if bytes > 0 {
            levels.push(format!("L{i}={bytes}B"));
        }
    }
    eprintln!(
        "    lsm levels: {}",
        if levels.is_empty() { "empty".into() } else { levels.join(",") }
    );

    let dropped = hist.dropped();
    (build_cell_result(&mut hist, ops, elapsed_secs), dropped)
}

fn build_cell_result(hist: &mut LatencyHistogram, ops: u64, elapsed_secs: f64) -> CellResult {
    CellResult {
        ops,
        ops_per_sec: ops as f64 / elapsed_secs,
        p50_ns: hist.percentile(0.50).as_nanos() as u64,
        p99_ns: hist.percentile(0.99).as_nanos() as u64,
        p999_ns: hist.percentile(0.999).as_nanos() as u64,
        p9999_ns: hist.percentile(0.9999).as_nanos() as u64,
        max_ns: hist.max().as_nanos() as u64,
    }
}
