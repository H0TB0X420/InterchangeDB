//! YCSB-style workloads on B+Tree and LSM.
//!
//! YCSB (Yahoo Cloud Serving Benchmark, Cooper et al. 2010) is the de-facto
//! reference workload set in the storage-engine literature. Numbers are
//! directly comparable across published RocksDB / LevelDB / WiredTiger
//! results, so this gives readers an immediate sense of where the project
//! stands.
//!
//! Workloads:
//! - A — 50% read, 50% update    (Zipfian)
//! - B — 95% read,  5% update    (Zipfian)
//! - C — 100% read               (Zipfian)
//! - D — 95% read,  5% insert    (read latest)
//! - E — 95% scan(50), 5% insert (latest insert; uniform scan start)
//! - F — 50% read, 50% RMW       (Zipfian)
//!
//! ## Method
//! - Pre-fill 100 K keys, ascending. Same dataset for all workloads.
//! - Run 100 K ops per (engine, workload).
//! - For each op: time it and add to a latency histogram.
//! - Workload D "latest" approximation: inserts append IDs starting at
//!   `PREFILL_KEYS`; reads sample from the upper tail (top 10% of inserted
//!   IDs) to mimic "most recent."
//! - Workload E scan length is fixed at 50 (the YCSB default).
//!
//! ## Output
//! - Markdown table: rows = (engine, workload), cols = ops/s, p50, p99, p99.9.
//! - CSV at `target/bench-results/engine_ycsb.csv`.
//!
//! ## Caveats
//! - We use a pre-built Zipfian over the static prefill range. Real YCSB
//!   has a "live keys" feature; for a fixed-size workload with our op count
//!   the difference is small.
//! - Workload D's "latest" distribution is the simplified "tail uniform"
//!   approximation, not YCSB's exact skew formula. The shape (recent reads
//!   are hot) is preserved.

use interchangedb::buffer::replacer::ArcReplacer;
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::index::btree::{BTree, BTreeHeaderPage};
use interchangedb::index::lsm::LsmTree;
use interchangedb::storage::FileDiskManager;
use std::time::Instant;
use tempfile::tempdir;

#[path = "common/mod.rs"]
mod common;

use common::budget::bytes_to_pool;
use common::encoding::{encode_key_i64, encode_key_vec, encode_value_u64, encode_value_vec};
use common::hist::LatencyHistogram;
use common::output::{results_path, CsvWriter, MarkdownTable};
use common::rng::{Lcg64, Zipfian};

/// Cache budget shared by both engines.
const CACHE_BYTES: usize = 256 * 1024;

/// Pre-fill keys and ops per workload. Sized so the B+Tree's slow random-op
/// path on a thrashing BPM doesn't dominate total bench time. With these
/// numbers the bench runs ~10 min total.
const PREFILL_KEYS: usize = 25_000;
const OPS_PER_WORKLOAD: usize = 5_000;

/// YCSB scan length (workload E).
const SCAN_LENGTH: usize = 50;

/// Zipfian skew (YCSB default).
const ZIPFIAN_THETA: f64 = 0.99;

/// Fraction of inserted-keys range from which workload D reads.
/// 0.10 ≈ "latest 10% of inserts." Approximates "read latest."
const LATEST_TAIL_FRACTION: f64 = 0.10;

/// Per-cell histogram cap (samples). 8 bytes/sample → 8 MB.
const HIST_CAP: usize = 1_000_000;

/// B+Tree page-layout constants.
const LEAF_MAX: u16 = 202;
const INTERNAL_MAX: u16 = 290;

#[derive(Copy, Clone)]
enum Ycsb {
    A,
    B,
    C,
    D,
    E,
    F,
}

impl Ycsb {
    fn name(self) -> &'static str {
        match self {
            Ycsb::A => "A_50r_50u",
            Ycsb::B => "B_95r_5u",
            Ycsb::C => "C_100r",
            Ycsb::D => "D_95r_5i_latest",
            Ycsb::E => "E_95scan_5i",
            Ycsb::F => "F_50r_50rmw",
        }
    }
}

const WORKLOADS: &[Ycsb] = &[Ycsb::A, Ycsb::B, Ycsb::C, Ycsb::D, Ycsb::E, Ycsb::F];

/// Result of one (engine, workload) cell.
struct CellResult {
    ops: u64,
    ops_per_sec: f64,
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
}

fn main() {
    let bpm_frames = bytes_to_pool(CACHE_BYTES);
    println!(
        "engine_ycsb: cache={} KB ({} BPM frames @ {} B), prefill={} keys, ops/workload={}",
        CACHE_BYTES / 1024,
        bpm_frames,
        common::budget::PAGE_SIZE,
        PREFILL_KEYS,
        OPS_PER_WORKLOAD,
    );

    let csv_path = results_path("engine_ycsb");
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
            "max_us",
        ],
    );
    let mut table = MarkdownTable::new(
        &[
            "Engine",
            "Workload",
            "ops/s",
            "p50 µs",
            "p99 µs",
            "p99.9 µs",
            "max µs",
        ],
        &[false, false, true, true, true, true, true],
    );

    for &workload in WORKLOADS {
        eprintln!("\n=== {} ===", workload.name());

        eprintln!("[btree] running...");
        let r = run_btree(workload, bpm_frames);
        log_cell("btree", workload, &r);
        emit_row(&mut csv, &mut table, "btree", workload, &r);

        eprintln!("[lsm  ] running...");
        let r = run_lsm(workload, CACHE_BYTES);
        log_cell("lsm", workload, &r);
        emit_row(&mut csv, &mut table, "lsm", workload, &r);
    }

    println!("\n=== engine_ycsb summary ===");
    table.print();
    println!("\nCSV: {}", csv_path.display());
}

fn log_cell(engine: &str, workload: Ycsb, r: &CellResult) {
    let to_us = |ns: u64| ns as f64 / 1_000.0;
    eprintln!(
        "  {} {}: ops/s={:>10.0}  p50={:>6.1}µs p99={:>7.1}µs p99.9={:>8.1}µs max={:>8.1}µs",
        engine,
        workload.name(),
        r.ops_per_sec,
        to_us(r.p50_ns),
        to_us(r.p99_ns),
        to_us(r.p999_ns),
        to_us(r.max_ns),
    );
}

fn emit_row(
    csv: &mut CsvWriter,
    table: &mut MarkdownTable,
    engine: &str,
    workload: Ycsb,
    r: &CellResult,
) {
    let to_us = |ns: u64| ns as f64 / 1_000.0;
    csv.row(&[
        engine,
        workload.name(),
        &r.ops.to_string(),
        &format!("{:.0}", r.ops_per_sec),
        &format!("{:.2}", to_us(r.p50_ns)),
        &format!("{:.2}", to_us(r.p99_ns)),
        &format!("{:.2}", to_us(r.p999_ns)),
        &format!("{:.2}", to_us(r.max_ns)),
    ]);
    table.row(&[
        engine,
        workload.name(),
        &format!("{:.0}", r.ops_per_sec),
        &format!("{:.1}", to_us(r.p50_ns)),
        &format!("{:.1}", to_us(r.p99_ns)),
        &format!("{:.1}", to_us(r.p999_ns)),
        &format!("{:.1}", to_us(r.max_ns)),
    ]);
}

/// Pick a key for "latest" reads: uniform over the top tail of inserted IDs.
fn pick_latest(rng: &mut Lcg64, max_id: usize) -> usize {
    if max_id == 0 {
        return 0;
    }
    let tail_size = ((max_id as f64) * LATEST_TAIL_FRACTION) as usize;
    let tail_size = tail_size.max(1).min(max_id);
    let lo = max_id.saturating_sub(tail_size);
    lo + rng.gen_range(tail_size)
}

// ============================================================================
// B+Tree
// ============================================================================

fn run_btree(workload: Ycsb, bpm_frames: usize) -> CellResult {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ycsb.db");
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
    for key in 0..PREFILL_KEYS {
        let k = encode_key_i64(key as i64);
        let v = encode_value_u64(key as u64);
        tree.insert(&k, &v).unwrap();
    }
    bpm.flush_all_pages().unwrap();

    let mut hist = LatencyHistogram::with_cap(HIST_CAP);
    let mut rng = Lcg64::new(0xFEED_FACE);
    let zipf = Zipfian::new(PREFILL_KEYS, ZIPFIAN_THETA);
    let mut next_insert_id = PREFILL_KEYS;
    let mut ops = 0u64;

    let start = Instant::now();
    for _ in 0..OPS_PER_WORKLOAD {
        // Pick the operation type per workload's distribution.
        let r = (rng.next_u64() as f64) / (u64::MAX as f64);
        let t0 = Instant::now();
        match workload {
            Ycsb::A => {
                // 50% read, 50% update; Zipfian
                let key = zipf.sample(&mut rng);
                let k = encode_key_i64(key as i64);
                if r < 0.5 {
                    let _ = tree.get(&k).unwrap();
                } else {
                    let v = encode_value_u64(rng.next_u64());
                    let _ = tree.insert(&k, &v);
                }
            }
            Ycsb::B => {
                // 95% read, 5% update; Zipfian
                let key = zipf.sample(&mut rng);
                let k = encode_key_i64(key as i64);
                if r < 0.95 {
                    let _ = tree.get(&k).unwrap();
                } else {
                    let v = encode_value_u64(rng.next_u64());
                    let _ = tree.insert(&k, &v);
                }
            }
            Ycsb::C => {
                // 100% read; Zipfian
                let key = zipf.sample(&mut rng);
                let k = encode_key_i64(key as i64);
                let _ = tree.get(&k).unwrap();
            }
            Ycsb::D => {
                // 95% read (latest), 5% insert (append next id)
                if r < 0.95 {
                    let key = pick_latest(&mut rng, next_insert_id);
                    let k = encode_key_i64(key as i64);
                    let _ = tree.get(&k).unwrap();
                } else {
                    let key = next_insert_id;
                    next_insert_id += 1;
                    let k = encode_key_i64(key as i64);
                    let v = encode_value_u64(rng.next_u64());
                    let _ = tree.insert(&k, &v);
                }
            }
            Ycsb::E => {
                // 95% scan(50), 5% insert
                if r < 0.95 {
                    let max_start = next_insert_id.saturating_sub(SCAN_LENGTH);
                    let start_key_id = if max_start > 0 {
                        rng.gen_range(max_start)
                    } else {
                        0
                    };
                    let end_key_id = start_key_id + SCAN_LENGTH;
                    let start_key = encode_key_i64(start_key_id as i64).to_vec();
                    let end_key = encode_key_i64(end_key_id as i64).to_vec();
                    let iter = tree.scan(start_key..end_key).unwrap();
                    let mut produced = 0;
                    for entry in iter {
                        let _ = entry.unwrap();
                        produced += 1;
                        if produced >= SCAN_LENGTH {
                            break;
                        }
                    }
                } else {
                    let key = next_insert_id;
                    next_insert_id += 1;
                    let k = encode_key_i64(key as i64);
                    let v = encode_value_u64(rng.next_u64());
                    let _ = tree.insert(&k, &v);
                }
            }
            Ycsb::F => {
                // 50% read, 50% read-modify-write; Zipfian
                let key = zipf.sample(&mut rng);
                let k = encode_key_i64(key as i64);
                if r < 0.5 {
                    let _ = tree.get(&k).unwrap();
                } else {
                    // Read then overwrite.
                    let _ = tree.get(&k).unwrap();
                    let v = encode_value_u64(rng.next_u64());
                    let _ = tree.insert(&k, &v);
                }
            }
        }
        hist.record(t0.elapsed());
        ops += 1;
    }
    let elapsed_secs = start.elapsed().as_secs_f64();

    build_cell_result(&mut hist, ops, elapsed_secs)
}

// ============================================================================
// LSM
// ============================================================================

fn run_lsm(workload: Ycsb, memtable_bytes: usize) -> CellResult {
    let dir = tempdir().unwrap();
    let tree = LsmTree::open_with_memtable_size(dir.path(), memtable_bytes).unwrap();

    // Pre-fill.
    for key in 0..PREFILL_KEYS {
        let k = encode_key_vec(key as i64);
        let v = encode_value_vec(key as u64);
        tree.put(k, v).unwrap();
    }
    tree.flush_memtable().unwrap();

    let mut hist = LatencyHistogram::with_cap(HIST_CAP);
    let mut rng = Lcg64::new(0xFEED_FACE);
    let zipf = Zipfian::new(PREFILL_KEYS, ZIPFIAN_THETA);
    let mut next_insert_id = PREFILL_KEYS;
    let mut ops = 0u64;

    let start = Instant::now();
    for _ in 0..OPS_PER_WORKLOAD {
        let r = (rng.next_u64() as f64) / (u64::MAX as f64);
        let t0 = Instant::now();
        match workload {
            Ycsb::A => {
                let key = zipf.sample(&mut rng);
                let k = encode_key_vec(key as i64);
                if r < 0.5 {
                    let _ = tree.get(&k).unwrap();
                } else {
                    let v = encode_value_vec(rng.next_u64());
                    tree.put(k, v).unwrap();
                }
            }
            Ycsb::B => {
                let key = zipf.sample(&mut rng);
                let k = encode_key_vec(key as i64);
                if r < 0.95 {
                    let _ = tree.get(&k).unwrap();
                } else {
                    let v = encode_value_vec(rng.next_u64());
                    tree.put(k, v).unwrap();
                }
            }
            Ycsb::C => {
                let key = zipf.sample(&mut rng);
                let k = encode_key_vec(key as i64);
                let _ = tree.get(&k).unwrap();
            }
            Ycsb::D => {
                if r < 0.95 {
                    let key = pick_latest(&mut rng, next_insert_id);
                    let k = encode_key_vec(key as i64);
                    let _ = tree.get(&k).unwrap();
                } else {
                    let key = next_insert_id;
                    next_insert_id += 1;
                    let k = encode_key_vec(key as i64);
                    let v = encode_value_vec(rng.next_u64());
                    tree.put(k, v).unwrap();
                }
            }
            Ycsb::E => {
                if r < 0.95 {
                    let max_start = next_insert_id.saturating_sub(SCAN_LENGTH);
                    let start_key_id = if max_start > 0 {
                        rng.gen_range(max_start)
                    } else {
                        0
                    };
                    let end_key_id = start_key_id + SCAN_LENGTH;
                    let start_key = encode_key_vec(start_key_id as i64);
                    let end_key = encode_key_vec(end_key_id as i64);
                    let _ = tree.scan(start_key..end_key).unwrap();
                } else {
                    let key = next_insert_id;
                    next_insert_id += 1;
                    let k = encode_key_vec(key as i64);
                    let v = encode_value_vec(rng.next_u64());
                    tree.put(k, v).unwrap();
                }
            }
            Ycsb::F => {
                let key = zipf.sample(&mut rng);
                let k = encode_key_vec(key as i64);
                if r < 0.5 {
                    let _ = tree.get(&k).unwrap();
                } else {
                    let _ = tree.get(&k).unwrap();
                    let v = encode_value_vec(rng.next_u64());
                    tree.put(k, v).unwrap();
                }
            }
        }
        hist.record(t0.elapsed());
        ops += 1;
    }
    let elapsed_secs = start.elapsed().as_secs_f64();

    build_cell_result(&mut hist, ops, elapsed_secs)
}

fn build_cell_result(hist: &mut LatencyHistogram, ops: u64, elapsed_secs: f64) -> CellResult {
    CellResult {
        ops,
        ops_per_sec: ops as f64 / elapsed_secs,
        p50_ns: hist.percentile(0.50).as_nanos() as u64,
        p99_ns: hist.percentile(0.99).as_nanos() as u64,
        p999_ns: hist.percentile(0.999).as_nanos() as u64,
        max_ns: hist.max().as_nanos() as u64,
    }
}
