//! Zipfian-skewed update workload — uniform vs Zipfian.
//!
//! The contrarian bench. Common wisdom says Zipfian skew is bad for cache-
//! based systems (long tail) but here the relevant question is: does it help
//! the LSM *more* than the B+Tree?
//!
//! ## Hypothesis
//! - **LSM**: Compaction collapses duplicate updates of hot keys into a
//!   single entry per level. Zipfian heavily concentrates writes on a small
//!   key range, so compaction's dedup work pays off — the LSM should produce
//!   a *smaller* on-disk footprint and a *lower* write-amp than under uniform
//!   updates.
//! - **B+Tree**: Hot keys stay in the BPM, so reads/writes are cheap. But
//!   on-disk size is identical to uniform — it's the same set of pages
//!   getting rewritten. WAF should be similar to or *higher* than uniform
//!   because hot pages eviction-bounce more.
//!
//! Net: the LSM should *gain* on Zipfian relative to uniform, and the gain
//! should be larger than what the B+Tree sees.
//!
//! ## Method
//! - Pre-fill 50 K keys (≈ 800 KB, 3× the 256 KB cache for the B+Tree).
//! - Run 200 K updates, twice per engine: once uniform, once Zipfian
//!   (theta = 0.99 — top 1% of keys see ~50% of writes).
//! - Measure: throughput, post-flush disk size, write amp.
//!
//! ## Output
//! - Markdown table: rows = (engine, distribution), cols = ops/s, disk MB,
//!   WAF.
//! - CSV at `target/bench-results/engine_zipfian_updates.csv`.

use interchangedb::buffer::replacer::ArcReplacer;
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::index::btree::{BTree, BTreeHeaderPage};
use interchangedb::index::lsm::LsmTree;
use interchangedb::storage::DiskManager;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[path = "common/mod.rs"]
mod common;

use common::budget::{bytes_to_pool, PAGE_SIZE};
use common::encoding::{encode_key_i64, encode_key_vec, encode_value_u64, encode_value_vec};
use common::output::{results_path, CsvWriter, MarkdownTable};
use common::poller::SstDirPoller;
use common::rng::{Lcg64, Zipfian};

/// Cache budget shared by both engines.
const CACHE_BYTES: usize = 256 * 1024;

/// Bytes per record.
const RECORD_BYTES: usize = 16;

/// Pre-fill key count.
const PREFILL_KEYS: usize = 25_000;

/// Update operations per cell. Sized for the B+Tree's ~85 ops/s random-update
/// rate (~2 min per cell × 4 cells ≈ 8 min total).
const UPDATE_OPS: usize = 10_000;

/// Zipfian skew. 0.99 matches YCSB.
const ZIPFIAN_THETA: f64 = 0.99;

/// Polling cadence for LSM SSTable directory.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// B+Tree page-layout constants.
const LEAF_MAX: u16 = 202;
const INTERNAL_MAX: u16 = 290;

#[derive(Copy, Clone)]
enum Distribution {
    Uniform,
    Zipfian,
}

impl Distribution {
    fn name(self) -> &'static str {
        match self {
            Distribution::Uniform => "uniform",
            Distribution::Zipfian => "zipfian",
        }
    }
}

const DISTRIBUTIONS: &[Distribution] = &[Distribution::Uniform, Distribution::Zipfian];

struct CellResult {
    ops: u64,
    ops_per_sec: f64,
    disk_bytes: u64,
    bytes_written_to_disk: u64,
    user_bytes_written: u64,
    waf: f64,
}

fn main() {
    let bpm_frames = bytes_to_pool(CACHE_BYTES);
    println!(
        "engine_zipfian_updates: cache={} KB ({} BPM frames), prefill={} keys, updates={}/cell, theta={}",
        CACHE_BYTES / 1024,
        bpm_frames,
        PREFILL_KEYS,
        UPDATE_OPS,
        ZIPFIAN_THETA,
    );

    let csv_path = results_path("engine_zipfian_updates");
    let mut csv = CsvWriter::create(
        &csv_path,
        &[
            "engine",
            "distribution",
            "ops",
            "ops_per_sec",
            "disk_bytes",
            "bytes_written_to_disk",
            "user_bytes_written",
            "WAF",
        ],
    );
    let mut table = MarkdownTable::new(
        &[
            "Engine",
            "Distribution",
            "ops/s",
            "Disk MB",
            "Disk MB written",
            "WAF",
        ],
        &[false, false, true, true, true, true],
    );

    for &dist in DISTRIBUTIONS {
        eprintln!("\n=== distribution: {} ===", dist.name());

        eprintln!("[btree] running...");
        let r_btree = run_btree(dist, bpm_frames);
        log_cell("btree", dist, &r_btree);
        emit_row(&mut csv, &mut table, "btree", dist, &r_btree);

        eprintln!("[lsm  ] running...");
        let r_lsm = run_lsm(dist, CACHE_BYTES);
        log_cell("lsm", dist, &r_lsm);
        emit_row(&mut csv, &mut table, "lsm", dist, &r_lsm);
    }

    println!("\n=== engine_zipfian_updates summary ===");
    table.print();
    println!("\nCSV: {}", csv_path.display());
    println!("\nNote: LSM WAF is a lower bound (poller can miss sub-{:?} flush/compact storms).", POLL_INTERVAL);
}

fn log_cell(engine: &str, dist: Distribution, r: &CellResult) {
    eprintln!(
        "  {} {}: {:>10.0} ops/s   disk={:>6.2} MB   WAF={:.2}",
        engine,
        dist.name(),
        r.ops_per_sec,
        r.disk_bytes as f64 / (1024.0 * 1024.0),
        r.waf,
    );
}

fn emit_row(
    csv: &mut CsvWriter,
    table: &mut MarkdownTable,
    engine: &str,
    dist: Distribution,
    r: &CellResult,
) {
    csv.row(&[
        engine,
        dist.name(),
        &r.ops.to_string(),
        &format!("{:.0}", r.ops_per_sec),
        &r.disk_bytes.to_string(),
        &r.bytes_written_to_disk.to_string(),
        &r.user_bytes_written.to_string(),
        &format!("{:.3}", r.waf),
    ]);
    table.row(&[
        engine,
        dist.name(),
        &format!("{:.0}", r.ops_per_sec),
        &format!("{:.2}", r.disk_bytes as f64 / (1024.0 * 1024.0)),
        &format!("{:.2}", r.bytes_written_to_disk as f64 / (1024.0 * 1024.0)),
        &format!("{:.2}", r.waf),
    ]);
}

// ============================================================================
// B+Tree
// ============================================================================

fn run_btree(dist: Distribution, bpm_frames: usize) -> CellResult {
    let dir = tempdir().unwrap();
    let path = dir.path().join("zipf.db");
    let dm = DiskManager::create(&path).unwrap();
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

    // Reset stats so we measure only the workload phase.
    bpm.stats().reset();

    let mut rng = Lcg64::new(0xBADC_0DE);
    let zipf = Zipfian::new(PREFILL_KEYS, ZIPFIAN_THETA);

    let start = Instant::now();
    for _ in 0..UPDATE_OPS {
        let key = match dist {
            Distribution::Uniform => rng.gen_range(PREFILL_KEYS),
            Distribution::Zipfian => zipf.sample(&mut rng),
        };
        let k = encode_key_i64(key as i64);
        let v = encode_value_u64(rng.next_u64());
        let _ = tree.insert(&k, &v);
    }
    bpm.flush_all_pages().unwrap();
    let elapsed_secs = start.elapsed().as_secs_f64();

    let pages_written = bpm.stats().pages_written.load(Ordering::Relaxed);
    let bytes_written_to_disk = pages_written * PAGE_SIZE as u64;
    let user_bytes_written = (UPDATE_OPS * RECORD_BYTES) as u64;
    let disk_bytes = bpm.disk_page_count() as u64 * PAGE_SIZE as u64;

    CellResult {
        ops: UPDATE_OPS as u64,
        ops_per_sec: UPDATE_OPS as f64 / elapsed_secs,
        disk_bytes,
        bytes_written_to_disk,
        user_bytes_written,
        waf: bytes_written_to_disk as f64 / user_bytes_written.max(1) as f64,
    }
}

// ============================================================================
// LSM
// ============================================================================

fn run_lsm(dist: Distribution, memtable_bytes: usize) -> CellResult {
    let dir = tempdir().unwrap();
    let tree = LsmTree::open_with_memtable_size(dir.path(), memtable_bytes).unwrap();

    // Pre-fill.
    for key in 0..PREFILL_KEYS {
        let k = encode_key_vec(key as i64);
        let v = encode_value_vec(key as u64);
        tree.put(k, v).unwrap();
    }
    tree.flush_memtable().unwrap();

    let pre_workload_disk = tree.level_state().total_disk_size();

    // Start poller for write-amp.
    let sst_dir = dir.path().join("sst");
    let poller = SstDirPoller::start(sst_dir, POLL_INTERVAL);

    let mut rng = Lcg64::new(0xBADC_0DE);
    let zipf = Zipfian::new(PREFILL_KEYS, ZIPFIAN_THETA);

    let start = Instant::now();
    for _ in 0..UPDATE_OPS {
        let key = match dist {
            Distribution::Uniform => rng.gen_range(PREFILL_KEYS),
            Distribution::Zipfian => zipf.sample(&mut rng),
        };
        let k = encode_key_vec(key as i64);
        let v = encode_value_vec(rng.next_u64());
        tree.put(k, v).unwrap();
    }
    tree.flush_memtable().unwrap();
    let elapsed_secs = start.elapsed().as_secs_f64();

    // Allow the poller to capture the final flush.
    std::thread::sleep(POLL_INTERVAL * 3);
    let (deleted_bytes, current_bytes) = poller.finish();
    let bytes_written_to_disk =
        (deleted_bytes + current_bytes).saturating_sub(pre_workload_disk);
    let user_bytes_written = (UPDATE_OPS * RECORD_BYTES) as u64;
    let disk_bytes = tree.level_state().total_disk_size();

    CellResult {
        ops: UPDATE_OPS as u64,
        ops_per_sec: UPDATE_OPS as f64 / elapsed_secs,
        disk_bytes,
        bytes_written_to_disk,
        user_bytes_written,
        waf: bytes_written_to_disk as f64 / user_bytes_written.max(1) as f64,
    }
}
