//! Range scan comparison — B+Tree vs LSM.
//!
//! Sequential reads are the workload where engines diverge most predictably:
//! B+Tree leaves are a sorted linked list — a scan is a single descent plus
//! pointer chasing — while an LSM merge iterator pays per-level dedup cost
//! across memtable + every SSTable.
//!
//! ## Heads-up about this codebase's LSM `scan`
//! Looking at `src/index/lsm/mod.rs::scan`, the current implementation does
//! a *full-tree merge* and then filters by range. That makes short range
//! queries dramatically expensive. Real-world LSMs (RocksDB, LevelDB) seek
//! to the start key at the block level. This bench will surface that
//! asymmetry honestly — the post can frame it either as "current LSM
//! limitation" or "merge-cost demonstration." Either way the numbers are
//! a real measurement of *what's there*, not the textbook LSM scan path.
//!
//! ## Method
//! - Pre-fill each engine to 2× cache so data is on disk but the bench is
//!   fast to set up.
//! - Sweep scan length: 10, 100, 1 K, 10 K, 100 K keys.
//! - For each (engine, length): 100 random-offset scans, record duration of
//!   each.
//! - Report: keys-per-second (so cells across lengths are directly comparable).
//!
//! ## Output
//! - Markdown table to stdout.
//! - CSV at `target/bench-results/engine_range_scan.csv`.

use interchangedb::buffer::replacer::ArcReplacer;
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::index::btree::{BTree, BTreeHeaderPage};
use interchangedb::index::lsm::LsmTree;
use interchangedb::storage::DiskManager;
use std::time::Instant;
use tempfile::tempdir;

#[path = "common/mod.rs"]
mod common;

use common::budget::{bytes_to_pool, PAGE_SIZE};
use common::encoding::{encode_key_i64, encode_key_vec, encode_value_u64, encode_value_vec};
use common::hist::LatencyHistogram;
use common::output::{results_path, CsvWriter, MarkdownTable};
use common::rng::Lcg64;

/// RAM budget shared by both engines.
const CACHE_BYTES: usize = 256 * 1024;

/// Pre-fill ratio. 2× ensures data spills past cache for both engines while
/// still keeping the dataset small enough that the bench runs quickly.
const PREFILL_RATIO: usize = 2;

/// Bytes per record (8-byte key + 8-byte value).
const RECORD_BYTES: usize = 16;

/// Scan lengths to sweep.
const SCAN_LENGTHS: &[usize] = &[10, 100, 1_000, 10_000, 100_000];

/// Number of scans per (engine, length) cell.
const SCANS_PER_CELL: usize = 100;

/// B+Tree page-layout constants. Match `btree_bench.rs`.
const LEAF_MAX: u16 = 202;
const INTERNAL_MAX: u16 = 290;

fn main() {
    let n_keys = (CACHE_BYTES / RECORD_BYTES) * PREFILL_RATIO;
    let bpm_frames = bytes_to_pool(CACHE_BYTES);

    println!(
        "engine_range_scan: cache={} KB ({} frames @ {} B), prefill={} keys ({}× cache), scans/cell={}",
        CACHE_BYTES / 1024,
        bpm_frames,
        PAGE_SIZE,
        n_keys,
        PREFILL_RATIO,
        SCANS_PER_CELL,
    );

    let csv_path = results_path("engine_range_scan");
    let mut csv = CsvWriter::create(
        &csv_path,
        &[
            "engine",
            "scan_length",
            "scans",
            "median_us",
            "p99_us",
            "keys_per_sec",
        ],
    );
    let mut table = MarkdownTable::new(
        &[
            "Scan length",
            "B+Tree median µs",
            "LSM median µs",
            "B+Tree keys/s",
            "LSM keys/s",
            "B+Tree advantage",
        ],
        &[true, true, true, true, true, true],
    );

    // --- Pre-fill engines once; all sweep cells share the same dataset. ---
    let btree_dir = tempdir().unwrap();
    let btree_path = btree_dir.path().join("range.db");
    let dm = DiskManager::create(&btree_path).unwrap();
    let bpm = BufferPoolManager::new(bpm_frames, dm);
    let _ = bpm.swap_policy(Box::new(ArcReplacer::new(bpm_frames)), SwapMode::Cold);
    let header_page = bpm.new_page().unwrap();
    let header_id = header_page.page_id();
    {
        let mut g = header_page;
        BTreeHeaderPage::new().encode(g.as_mut_slice());
    }
    let btree = BTree::with_sizes(&bpm, header_id, LEAF_MAX, INTERNAL_MAX);
    eprintln!("[btree] pre-fill {} keys...", n_keys);
    for key in 0..n_keys {
        let k = encode_key_i64(key as i64);
        let v = encode_value_u64(key as u64);
        btree.insert(&k, &v).unwrap();
    }
    bpm.flush_all_pages().unwrap();

    let lsm_dir = tempdir().unwrap();
    let lsm = LsmTree::open_with_memtable_size(lsm_dir.path(), CACHE_BYTES).unwrap();
    eprintln!("[lsm  ] pre-fill {} keys...", n_keys);
    for key in 0..n_keys {
        let k = encode_key_vec(key as i64);
        let v = encode_value_vec(key as u64);
        lsm.put(k, v).unwrap();
    }
    lsm.flush_memtable().unwrap();

    // --- Sweep scan lengths. ---
    for &length in SCAN_LENGTHS {
        // Random offsets so we hit different parts of the keyspace each scan.
        // Use the same seed for both engines so they scan identical ranges.
        eprintln!("\n=== scan_length = {} ===", length);

        let (btree_med_ns, btree_p99_ns) =
            measure_btree_scan(&btree, n_keys, length, SCANS_PER_CELL);
        let (lsm_med_ns, lsm_p99_ns) = measure_lsm_scan(&lsm, n_keys, length, SCANS_PER_CELL);

        let btree_kps = (length as f64) / (btree_med_ns as f64 / 1e9);
        let lsm_kps = (length as f64) / (lsm_med_ns as f64 / 1e9);

        eprintln!(
            "  btree: median={:>8.1}µs p99={:>8.1}µs  ({:>10.0} keys/s)",
            btree_med_ns as f64 / 1e3,
            btree_p99_ns as f64 / 1e3,
            btree_kps,
        );
        eprintln!(
            "  lsm:   median={:>8.1}µs p99={:>8.1}µs  ({:>10.0} keys/s)",
            lsm_med_ns as f64 / 1e3,
            lsm_p99_ns as f64 / 1e3,
            lsm_kps,
        );

        csv.row(&[
            "btree",
            &length.to_string(),
            &SCANS_PER_CELL.to_string(),
            &format!("{:.2}", btree_med_ns as f64 / 1e3),
            &format!("{:.2}", btree_p99_ns as f64 / 1e3),
            &format!("{:.0}", btree_kps),
        ]);
        csv.row(&[
            "lsm",
            &length.to_string(),
            &SCANS_PER_CELL.to_string(),
            &format!("{:.2}", lsm_med_ns as f64 / 1e3),
            &format!("{:.2}", lsm_p99_ns as f64 / 1e3),
            &format!("{:.0}", lsm_kps),
        ]);

        let advantage = if btree_med_ns > 0 && lsm_med_ns > 0 {
            format!("{:.1}x", lsm_med_ns as f64 / btree_med_ns as f64)
        } else {
            "n/a".into()
        };

        table.row(&[
            &length.to_string(),
            &format!("{:.1}", btree_med_ns as f64 / 1e3),
            &format!("{:.1}", lsm_med_ns as f64 / 1e3),
            &format!("{:.0}", btree_kps),
            &format!("{:.0}", lsm_kps),
            &advantage,
        ]);
    }

    println!("\n=== engine_range_scan summary ===");
    table.print();
    println!("\nCSV: {}", csv_path.display());
}

/// Measure B+Tree range scan. Returns (median_ns, p99_ns).
fn measure_btree_scan<'a>(
    tree: &BTree<'a>,
    n_keys: usize,
    length: usize,
    scans: usize,
) -> (u64, u64) {
    let mut hist = LatencyHistogram::with_capacity(scans);
    let mut rng = Lcg64::new(0x5CA5_5CA5);
    let max_start = n_keys.saturating_sub(length);

    for _ in 0..scans {
        let start = if max_start > 0 { rng.gen_range(max_start) } else { 0 };
        let end = start + length;
        let start_key = encode_key_i64(start as i64).to_vec();
        let end_key = encode_key_i64(end as i64).to_vec();

        let t0 = Instant::now();
        let iter = tree.scan(start_key..end_key).unwrap();
        let mut produced = 0usize;
        for entry in iter {
            let _ = entry.unwrap();
            produced += 1;
            // Defensive cap: matches the requested length and prevents
            // surprises if scan() over-runs.
            if produced >= length {
                break;
            }
        }
        hist.record(t0.elapsed());
    }

    (
        hist.percentile(0.50).as_nanos() as u64,
        hist.percentile(0.99).as_nanos() as u64,
    )
}

/// Measure LSM range scan. Returns (median_ns, p99_ns).
fn measure_lsm_scan(tree: &LsmTree, n_keys: usize, length: usize, scans: usize) -> (u64, u64) {
    let mut hist = LatencyHistogram::with_capacity(scans);
    let mut rng = Lcg64::new(0x5CA5_5CA5);
    let max_start = n_keys.saturating_sub(length);

    for _ in 0..scans {
        let start = if max_start > 0 { rng.gen_range(max_start) } else { 0 };
        let end = start + length;
        let start_key = encode_key_vec(start as i64);
        let end_key = encode_key_vec(end as i64);

        let t0 = Instant::now();
        let _entries = tree.scan(start_key..end_key).unwrap();
        hist.record(t0.elapsed());
    }

    (
        hist.percentile(0.50).as_nanos() as u64,
        hist.percentile(0.99).as_nanos() as u64,
    )
}
