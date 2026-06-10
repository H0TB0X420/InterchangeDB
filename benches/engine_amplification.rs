//! Write amplification (WAF) and read amplification (RAF) measurement.
//!
//! Reference points from the literature:
//! - RocksDB (leveled compaction): WAF ≈ 10–30× on large datasets
//! - PostgreSQL B-tree: WAF ≈ 2–5× (page rewrites + WAL)
//! - LevelDB: WAF ≈ 10× (size-tiered)
//!
//! Our measurement is bench-only — no source-code instrumentation. The B+Tree
//! is straightforward: `bpm.stats()` already exposes `pages_written` and
//! `pages_read`. The LSM is harder: `LsmTree` has no disk-bytes counter, so
//! we run a background `SstDirPoller` that snapshots the SST directory every
//! ~10 ms and accumulates the sizes of files that vanished between snapshots
//! (i.e. were compacted away).
//!
//! ## Workloads
//! - `bulk_insert`        — N unique keys, each value fresh.
//! - `uniform_updates`    — pre-fill K keys, then update random keys uniformly.
//! - `zipfian_updates`    — pre-fill K keys, then update keys with theta=0.99
//!                          (top 1% of keys see ~50% of writes).
//!
//! ## Caveats
//! - LSM WAF is a *lower bound*. Sub-poll-interval flush+compact storms can
//!   produce files that exist only briefly and never get snapshotted.
//! - LSM RAF is approximated as `level_count + 1` (memtable + per-level
//!   probe). Bloom filters reduce real RAF further, but we cannot reach the
//!   per-block read counter from bench code.
//! - B+Tree numbers exclude WAL. With WAL the B+Tree WAF would roughly
//!   double; this is a "raw engine" comparison.
//!
//! ## Output
//! - Markdown table: rows = (engine, workload), columns = WAF, RAF, bytes
//!   written, bytes read.
//! - CSV at `target/bench-results/engine_amplification.csv`.

use interchangedb::buffer::replacer::ArcReplacer;
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::index::btree::{BTree, BTreeHeaderPage};
use interchangedb::index::lsm::LsmTree;
use interchangedb::storage::FileDiskManager;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tempfile::tempdir;

#[path = "common/mod.rs"]
mod common;

use common::budget::{bytes_to_pool, PAGE_SIZE};
use common::encoding::{encode_key_i64, encode_key_vec, encode_value_u64, encode_value_vec};
use common::output::{results_path, CsvWriter, MarkdownTable};
use common::poller::SstDirPoller;
use common::rng::{Lcg64, Zipfian};

/// RAM budget shared by both engines.
const CACHE_BYTES: usize = 256 * 1024;

/// Bytes per record (8-byte key + 8-byte value).
const RECORD_BYTES: usize = 16;

/// Bulk-insert workload: 100 K records ≈ 1.6 MB of user data, 6× cache.
/// Monotonic inserts so even with BPM=64 the rightmost-leaf hot path runs
/// at ~3 K ops/s; this finishes in ~30 s per engine.
const BULK_KEYS: usize = 100_000;

/// Update-workload pre-fill key count.
const UPDATE_PREFILL: usize = 25_000;

/// Number of update operations applied after pre-fill.
/// Random updates on a thrashing B+Tree run at ~85 ops/s, so 10 K ops ≈
/// 2 min per cell. Smaller than ideal for tail latency, but the WAF/RAF
/// *ratios* converge well below this.
const UPDATE_OPS: usize = 10_000;

/// Zipfian skew (matches YCSB's standard).
const ZIPFIAN_THETA: f64 = 0.99;

/// Read phase ops for RAF measurement.
const READS_FOR_RAF: usize = 50_000;

/// Polling cadence for the LSM SSTable directory.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// B+Tree page-layout constants.
const LEAF_MAX: u16 = 202;
const INTERNAL_MAX: u16 = 290;

#[derive(Copy, Clone)]
enum Workload {
    BulkInsert,
    UniformUpdates,
    ZipfianUpdates,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Workload::BulkInsert => "bulk_insert",
            Workload::UniformUpdates => "uniform_updates",
            Workload::ZipfianUpdates => "zipfian_updates",
        }
    }
}

const WORKLOADS: &[Workload] = &[
    Workload::BulkInsert,
    Workload::UniformUpdates,
    Workload::ZipfianUpdates,
];

struct AmpResult {
    user_bytes_written: u64,
    bytes_written_to_disk: u64,
    waf: f64,
    user_bytes_read: u64,
    bytes_read_from_disk: u64,
    raf: f64,
}

fn main() {
    let bpm_frames = bytes_to_pool(CACHE_BYTES);
    println!(
        "engine_amplification: cache={} KB ({} BPM frames @ {} B), poll={:?}",
        CACHE_BYTES / 1024,
        bpm_frames,
        PAGE_SIZE,
        POLL_INTERVAL,
    );

    let csv_path = results_path("engine_amplification");
    let mut csv = CsvWriter::create(
        &csv_path,
        &[
            "engine",
            "workload",
            "user_bytes_written",
            "bytes_written_to_disk",
            "WAF",
            "user_bytes_read",
            "bytes_read_from_disk",
            "RAF",
        ],
    );
    let mut table = MarkdownTable::new(
        &[
            "Engine",
            "Workload",
            "WAF",
            "RAF",
            "Disk MB written",
            "Disk MB read",
        ],
        &[false, false, true, true, true, true],
    );

    for &workload in WORKLOADS {
        eprintln!("\n=== workload: {} ===", workload.name());

        eprintln!("[btree] running...");
        let btree_r = run_btree(workload, bpm_frames);
        log_amp("btree", workload, &btree_r);
        emit_row(&mut csv, &mut table, "btree", workload, &btree_r);

        eprintln!("[lsm  ] running...");
        let lsm_r = run_lsm(workload, CACHE_BYTES);
        log_amp("lsm", workload, &lsm_r);
        emit_row(&mut csv, &mut table, "lsm", workload, &lsm_r);
    }

    println!("\n=== engine_amplification summary ===");
    table.print();
    println!("\nCSV: {}", csv_path.display());
    println!(
        "\nNote: LSM WAF is a lower bound (poller can miss sub-{:?} flush/compact storms).",
        POLL_INTERVAL
    );
    println!("Note: LSM RAF is approximated as level_count + 1; real RAF is lower thanks to bloom filters.");
}

fn log_amp(engine: &str, workload: Workload, r: &AmpResult) {
    eprintln!(
        "  {} {}: WAF={:.2} (user={:.2}MB / disk={:.2}MB)  RAF={:.2} (user={:.2}MB / disk={:.2}MB)",
        engine,
        workload.name(),
        r.waf,
        r.user_bytes_written as f64 / 1e6,
        r.bytes_written_to_disk as f64 / 1e6,
        r.raf,
        r.user_bytes_read as f64 / 1e6,
        r.bytes_read_from_disk as f64 / 1e6,
    );
}

fn emit_row(
    csv: &mut CsvWriter,
    table: &mut MarkdownTable,
    engine: &str,
    workload: Workload,
    r: &AmpResult,
) {
    csv.row(&[
        engine,
        workload.name(),
        &r.user_bytes_written.to_string(),
        &r.bytes_written_to_disk.to_string(),
        &format!("{:.3}", r.waf),
        &r.user_bytes_read.to_string(),
        &r.bytes_read_from_disk.to_string(),
        &format!("{:.3}", r.raf),
    ]);
    table.row(&[
        engine,
        workload.name(),
        &format!("{:.2}", r.waf),
        &format!("{:.2}", r.raf),
        &format!("{:.2}", r.bytes_written_to_disk as f64 / (1024.0 * 1024.0)),
        &format!("{:.2}", r.bytes_read_from_disk as f64 / (1024.0 * 1024.0)),
    ]);
}

// ============================================================================
// B+Tree path
// ============================================================================

fn run_btree(workload: Workload, bpm_frames: usize) -> AmpResult {
    let dir = tempdir().unwrap();
    let path = dir.path().join("amp.db");
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

    // --- Pre-fill (for update workloads) ---
    if matches!(
        workload,
        Workload::UniformUpdates | Workload::ZipfianUpdates
    ) {
        for key in 0..UPDATE_PREFILL {
            let k = encode_key_i64(key as i64);
            let v = encode_value_u64(key as u64);
            tree.insert(&k, &v).unwrap();
        }
        bpm.flush_all_pages().unwrap();
    }

    // --- Reset stats so the timed phase is clean ---
    bpm.stats().reset();
    let user_bytes_written;

    // --- Workload phase ---
    match workload {
        Workload::BulkInsert => {
            for key in 0..BULK_KEYS {
                let k = encode_key_i64(key as i64);
                let v = encode_value_u64(key as u64);
                tree.insert(&k, &v).unwrap();
            }
            user_bytes_written = (BULK_KEYS * RECORD_BYTES) as u64;
        }
        Workload::UniformUpdates => {
            let mut rng = Lcg64::new(0xA1B2_C3D4);
            for _ in 0..UPDATE_OPS {
                let key = rng.gen_range(UPDATE_PREFILL);
                let k = encode_key_i64(key as i64);
                let v = encode_value_u64(rng.next_u64());
                let _ = tree.insert(&k, &v);
            }
            user_bytes_written = (UPDATE_OPS * RECORD_BYTES) as u64;
        }
        Workload::ZipfianUpdates => {
            let mut rng = Lcg64::new(0xA1B2_C3D4);
            let zipf = Zipfian::new(UPDATE_PREFILL, ZIPFIAN_THETA);
            for _ in 0..UPDATE_OPS {
                let key = zipf.sample(&mut rng);
                let k = encode_key_i64(key as i64);
                let v = encode_value_u64(rng.next_u64());
                let _ = tree.insert(&k, &v);
            }
            user_bytes_written = (UPDATE_OPS * RECORD_BYTES) as u64;
        }
    }
    bpm.flush_all_pages().unwrap();

    let pages_written = bpm.stats().pages_written.load(Ordering::Relaxed);
    let bytes_written_to_disk = pages_written * PAGE_SIZE as u64;

    // --- Read phase for RAF ---
    bpm.stats().reset();
    let mut rng = Lcg64::new(0xCAFE_BABE);
    let n_keys = match workload {
        Workload::BulkInsert => BULK_KEYS,
        _ => UPDATE_PREFILL,
    };
    for _ in 0..READS_FOR_RAF {
        let key = rng.gen_range(n_keys);
        let k = encode_key_i64(key as i64);
        let _ = tree.get(&k).unwrap();
    }

    let pages_read = bpm.stats().pages_read.load(Ordering::Relaxed);
    let user_bytes_read = (READS_FOR_RAF * RECORD_BYTES) as u64;
    let bytes_read_from_disk = pages_read * PAGE_SIZE as u64;

    AmpResult {
        user_bytes_written,
        bytes_written_to_disk,
        waf: bytes_written_to_disk as f64 / user_bytes_written.max(1) as f64,
        user_bytes_read,
        bytes_read_from_disk,
        raf: bytes_read_from_disk as f64 / user_bytes_read.max(1) as f64,
    }
}

// ============================================================================
// LSM path
// ============================================================================

fn run_lsm(workload: Workload, memtable_bytes: usize) -> AmpResult {
    let dir = tempdir().unwrap();
    let tree = LsmTree::open_with_memtable_size(dir.path(), memtable_bytes).unwrap();

    // --- Pre-fill (for update workloads) ---
    if matches!(
        workload,
        Workload::UniformUpdates | Workload::ZipfianUpdates
    ) {
        for key in 0..UPDATE_PREFILL {
            let k = encode_key_vec(key as i64);
            let v = encode_value_vec(key as u64);
            tree.put(k, v).unwrap();
        }
        tree.flush_memtable().unwrap();
    }

    // Now reset the disk-byte tracker and run the workload.
    let sst_dir = dir.path().join("sst");
    let poller = SstDirPoller::start(sst_dir, POLL_INTERVAL);

    // Snapshot pre-workload disk size as the baseline; we'll subtract it so
    // the result reflects only the workload phase.
    let pre_workload_disk = tree.level_state().total_disk_size();

    let user_bytes_written;
    match workload {
        Workload::BulkInsert => {
            for key in 0..BULK_KEYS {
                let k = encode_key_vec(key as i64);
                let v = encode_value_vec(key as u64);
                tree.put(k, v).unwrap();
            }
            user_bytes_written = (BULK_KEYS * RECORD_BYTES) as u64;
        }
        Workload::UniformUpdates => {
            let mut rng = Lcg64::new(0xA1B2_C3D4);
            for _ in 0..UPDATE_OPS {
                let key = rng.gen_range(UPDATE_PREFILL);
                let k = encode_key_vec(key as i64);
                let v = encode_value_vec(rng.next_u64());
                tree.put(k, v).unwrap();
            }
            user_bytes_written = (UPDATE_OPS * RECORD_BYTES) as u64;
        }
        Workload::ZipfianUpdates => {
            let mut rng = Lcg64::new(0xA1B2_C3D4);
            let zipf = Zipfian::new(UPDATE_PREFILL, ZIPFIAN_THETA);
            for _ in 0..UPDATE_OPS {
                let key = zipf.sample(&mut rng);
                let k = encode_key_vec(key as i64);
                let v = encode_value_vec(rng.next_u64());
                tree.put(k, v).unwrap();
            }
            user_bytes_written = (UPDATE_OPS * RECORD_BYTES) as u64;
        }
    }
    // Drain the memtable so its bytes hit disk and get counted by the poller.
    tree.flush_memtable().unwrap();

    // Give the poller a chance to capture the final flush before we stop it.
    std::thread::sleep(POLL_INTERVAL * 3);
    let (deleted_bytes, current_bytes) = poller.finish();
    let bytes_written_to_disk = (deleted_bytes + current_bytes).saturating_sub(pre_workload_disk);

    // --- Read phase for RAF (approximation: level_count + 1) ---
    let level_state = tree.level_state();
    let mut active_levels = 0u64;
    for i in 0..7 {
        if level_state.level_size(i) > 0 {
            active_levels += 1;
        }
    }
    // Per-lookup pages-touched: 1 (memtable) + active_levels (assuming each
    // level needs one block read on a hit; bloom filters cut false probes).
    let approx_pages_per_read = 1 + active_levels;

    // For the read phase, just exercise reads so the timing is comparable.
    let mut rng = Lcg64::new(0xCAFE_BABE);
    let n_keys = match workload {
        Workload::BulkInsert => BULK_KEYS,
        _ => UPDATE_PREFILL,
    };
    for _ in 0..READS_FOR_RAF {
        let key = rng.gen_range(n_keys);
        let k = encode_key_vec(key as i64);
        let _ = tree.get(&k).unwrap();
    }

    let user_bytes_read = (READS_FOR_RAF * RECORD_BYTES) as u64;
    // Assume each "page read" equals a 4 KB block worth of I/O for direct
    // comparability with the B+Tree number.
    let bytes_read_from_disk = approx_pages_per_read * PAGE_SIZE as u64 * READS_FOR_RAF as u64;

    AmpResult {
        user_bytes_written,
        bytes_written_to_disk,
        waf: bytes_written_to_disk as f64 / user_bytes_written.max(1) as f64,
        user_bytes_read,
        bytes_read_from_disk,
        raf: bytes_read_from_disk as f64 / user_bytes_read.max(1) as f64,
    }
}
