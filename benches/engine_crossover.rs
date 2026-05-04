//! Working-set crossover sweep — B+Tree vs LSM.
//!
//! The headline graph for the engines post: how does each engine degrade as
//! the dataset outgrows the in-memory cache? The expectation from the
//! literature:
//! - LSM keeps writes ~flat (memtable + sequential SSTable flush, regardless
//!   of dataset size).
//! - B+Tree write throughput collapses once the working set exceeds the BPM,
//!   because every insert pulls in a leaf from disk.
//! - B+Tree reads stay ~log(N) because tree height grows slowly.
//! - LSM reads degrade as level count grows (more SSTables to probe).
//!
//! ## Method
//! - Both engines share the same RAM budget: 256 KB.
//!   - B+Tree: BPM = 64 frames * 4 KB.
//!   - LSM:    memtable_size_limit = 256 KB.
//! - Sweep dataset size as a multiple of cache: 0.5× through 25×.
//! - For each cell:
//!   1. Bulk insert N keys, time the insert phase.
//!   2. 50 K random point lookups, time the read phase.
//! - Engine-direct: no `Database` wrapper, no WAL.
//! - Single run per cell. The *shape* of the curve matters more than ±5 %
//!   noise on individual points; user can re-run for variance.
//!
//! ## Output
//! - Markdown table to stdout (paste into post).
//! - CSV at `target/bench-results/engine_crossover.csv` for plotting.

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
use common::output::{results_path, CsvWriter, MarkdownTable};
use common::rng::Lcg64;

/// RAM budget shared by both engines.
const CACHE_BYTES: usize = 256 * 1024;

/// Bytes of user-visible key+value per record (8 + 8). Used to convert ratios
/// to key counts.
const RECORD_BYTES: usize = 16;

/// Random reads performed per cell.
const READS_PER_CELL: usize = 50_000;

/// Sweep ratios. `n_keys = ratio * (CACHE_BYTES / RECORD_BYTES)`.
/// Keep the high end at 25× — beyond that, the B+Tree thrashes hard enough
/// that the bench takes longer than the post is worth.
const RATIOS: &[f64] = &[0.5, 1.0, 2.0, 5.0, 10.0, 25.0];

/// B+Tree page-layout constants. Matches `btree_bench.rs`.
const LEAF_MAX: u16 = 202;
const INTERNAL_MAX: u16 = 290;

fn main() {
    let bpm_frames = bytes_to_pool(CACHE_BYTES);
    println!(
        "engine_crossover: cache_budget={} KB ({} BPM frames @ {} B), reads/cell={}",
        CACHE_BYTES / 1024,
        bpm_frames,
        PAGE_SIZE,
        READS_PER_CELL,
    );

    let csv_path = results_path("engine_crossover");
    let mut csv = CsvWriter::create(
        &csv_path,
        &[
            "engine",
            "ratio",
            "n_keys",
            "insert_ops_per_sec",
            "read_ops_per_sec",
        ],
    );

    let mut table = MarkdownTable::new(
        &[
            "Ratio",
            "n_keys",
            "B+Tree insert ops/s",
            "LSM insert ops/s",
            "B+Tree read ops/s",
            "LSM read ops/s",
        ],
        &[true, true, true, true, true, true],
    );

    for &ratio in RATIOS {
        let n_keys = ((CACHE_BYTES as f64 / RECORD_BYTES as f64) * ratio) as usize;
        eprintln!("\n=== ratio={ratio:.1}x  n_keys={n_keys} ===");

        let (btree_ins, btree_read) = run_btree(n_keys, bpm_frames);
        eprintln!(
            "  btree:  insert={:>10.0} ops/s   read={:>10.0} ops/s",
            btree_ins, btree_read
        );
        csv.row(&[
            "btree",
            &format!("{ratio}"),
            &n_keys.to_string(),
            &format!("{btree_ins:.0}"),
            &format!("{btree_read:.0}"),
        ]);

        let (lsm_ins, lsm_read) = run_lsm(n_keys, CACHE_BYTES);
        eprintln!(
            "  lsm:    insert={:>10.0} ops/s   read={:>10.0} ops/s",
            lsm_ins, lsm_read
        );
        csv.row(&[
            "lsm",
            &format!("{ratio}"),
            &n_keys.to_string(),
            &format!("{lsm_ins:.0}"),
            &format!("{lsm_read:.0}"),
        ]);

        table.row(&[
            &format!("{ratio:.1}x"),
            &n_keys.to_string(),
            &format!("{btree_ins:.0}"),
            &format!("{lsm_ins:.0}"),
            &format!("{btree_read:.0}"),
            &format!("{lsm_read:.0}"),
        ]);
    }

    println!("\n=== engine_crossover summary ===");
    table.print();
    println!("\nCSV: {}", csv_path.display());
}

/// Run one B+Tree cell. Returns (insert_ops_per_sec, read_ops_per_sec).
fn run_btree(n_keys: usize, bpm_frames: usize) -> (f64, f64) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("crossover.db");
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

    // --- Insert phase ---
    let t0 = Instant::now();
    for key in 0..n_keys {
        let k = encode_key_i64(key as i64);
        let v = encode_value_u64(key as u64);
        tree.insert(&k, &v).unwrap();
    }
    // Match the durability semantics of LSM's flush_memtable().
    bpm.flush_all_pages().unwrap();
    let insert_secs = t0.elapsed().as_secs_f64();
    let insert_ops = n_keys as f64 / insert_secs;

    // --- Read phase ---
    bpm.stats().reset();
    let mut rng = Lcg64::new(0xCAFE_BABE);
    let t0 = Instant::now();
    for _ in 0..READS_PER_CELL {
        let key = rng.gen_range(n_keys);
        let k = encode_key_i64(key as i64);
        let _ = tree.get(&k).unwrap();
    }
    let read_secs = t0.elapsed().as_secs_f64();
    let read_ops = READS_PER_CELL as f64 / read_secs;

    eprintln!(
        "    btree bpm: {} (hit_rate={:.1}%)",
        bpm.stats().snapshot(),
        bpm.stats().hit_rate() * 100.0
    );

    (insert_ops, read_ops)
}

/// Run one LSM cell. Returns (insert_ops_per_sec, read_ops_per_sec).
fn run_lsm(n_keys: usize, memtable_bytes: usize) -> (f64, f64) {
    let dir = tempdir().unwrap();
    let tree = LsmTree::open_with_memtable_size(dir.path(), memtable_bytes).unwrap();

    // --- Insert phase ---
    let t0 = Instant::now();
    for key in 0..n_keys {
        let k = encode_key_vec(key as i64);
        let v = encode_value_vec(key as u64);
        tree.put(k, v).unwrap();
    }
    // Force durability so reads exercise the on-disk path and the timed
    // region matches the B+Tree's flush_all_pages() cost.
    tree.flush_memtable().unwrap();
    let insert_secs = t0.elapsed().as_secs_f64();
    let insert_ops = n_keys as f64 / insert_secs;

    // --- Read phase ---
    let mut rng = Lcg64::new(0xCAFE_BABE);
    let t0 = Instant::now();
    for _ in 0..READS_PER_CELL {
        let key = rng.gen_range(n_keys);
        let k = encode_key_vec(key as i64);
        let _ = tree.get(&k).unwrap();
    }
    let read_secs = t0.elapsed().as_secs_f64();
    let read_ops = READS_PER_CELL as f64 / read_secs;

    let ls = tree.level_state();
    let mut levels = Vec::new();
    for i in 0..7 {
        let bytes = ls.level_size(i);
        if bytes > 0 {
            levels.push(format!("L{i}={bytes}B"));
        }
    }
    eprintln!(
        "    lsm   levels: {}",
        if levels.is_empty() { "empty".into() } else { levels.join(",") }
    );

    (insert_ops, read_ops)
}
