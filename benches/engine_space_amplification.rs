//! Space amplification: how much disk does each engine consume relative to
//! the user's logical data?
//!
//! Two snapshots per engine:
//! 1. After bulk insert of N unique keys (flushed). The "clean steady state."
//! 2. After M random updates (flushed). Shows how much the engine bloats
//!    when overwriting existing keys.
//!
//! Expectations:
//! - B+Tree: in-place updates keep disk size ~constant. Space amp is
//!   determined by page fill factor (typically 67–75%) plus header overhead.
//! - LSM: obsolete versions accumulate in higher levels until compaction
//!   garbage-collects them. Post-update size will be visibly larger than
//!   post-insert until enough compaction passes have run.
//!
//! ## Method
//! - Insert 250 K unique keys, flush, measure disk size.
//! - 250 K random updates (uniform), flush, measure disk size.
//! - Report `bytes-on-disk / bytes-of-user-data` (space amp) at each phase.
//!
//! ## Output
//! - Markdown table: rows = engine, columns = post-insert MB, post-update MB,
//!   space-amp post-insert, space-amp post-update.
//! - CSV at `target/bench-results/engine_space_amplification.csv`.

use interchangedb::buffer::replacer::ArcReplacer;
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::engines::btree::{BTree, BTreeHeaderPage};
use interchangedb::engines::lsm::LsmTree;
use interchangedb::storage::FileDiskManager;
use tempfile::tempdir;

#[path = "common/mod.rs"]
mod common;

use common::budget::{bytes_to_pool, PAGE_SIZE};
use common::encoding::{encode_key_i64, encode_key_vec, encode_value_u64, encode_value_vec};
use common::output::{results_path, CsvWriter, MarkdownTable};
use common::rng::Lcg64;

/// Cache budget shared by both engines.
const CACHE_BYTES: usize = 256 * 1024;

/// Bytes per record (8-byte key + 8-byte value).
const RECORD_BYTES: usize = 16;

/// Unique keys for the insert phase. Monotonic inserts at ~3 K ops/s on
/// the thrashing B+Tree finish in ~30 s.
const INSERT_KEYS: usize = 100_000;

/// Random update operations after insert. Sized so the B+Tree's ~85 ops/s
/// random-update rate keeps the bench under ~2 min per engine.
const UPDATE_OPS: usize = 10_000;

/// B+Tree page-layout constants.
const LEAF_MAX: u16 = 202;
const INTERNAL_MAX: u16 = 290;

struct PhaseResult {
    user_bytes: u64,
    disk_bytes: u64,
    space_amp: f64,
}

fn main() {
    let bpm_frames = bytes_to_pool(CACHE_BYTES);
    println!(
        "engine_space_amplification: cache={} KB ({} BPM frames @ {} B), {} inserts + {} updates",
        CACHE_BYTES / 1024,
        bpm_frames,
        PAGE_SIZE,
        INSERT_KEYS,
        UPDATE_OPS,
    );

    let csv_path = results_path("engine_space_amplification");
    let mut csv = CsvWriter::create(
        &csv_path,
        &["engine", "phase", "user_bytes", "disk_bytes", "space_amp"],
    );
    let mut table = MarkdownTable::new(
        &[
            "Engine",
            "Post-insert MB",
            "Post-update MB",
            "Space amp (insert)",
            "Space amp (update)",
            "Bloat factor",
        ],
        &[false, true, true, true, true, true],
    );

    eprintln!("\n[btree] running...");
    let (btree_post_ins, btree_post_upd) = run_btree(bpm_frames);
    log_result("btree", &btree_post_ins, &btree_post_upd);
    csv_phase(&mut csv, "btree", "post_insert", &btree_post_ins);
    csv_phase(&mut csv, "btree", "post_update", &btree_post_upd);

    eprintln!("\n[lsm  ] running...");
    let (lsm_post_ins, lsm_post_upd) = run_lsm(CACHE_BYTES);
    log_result("lsm", &lsm_post_ins, &lsm_post_upd);
    csv_phase(&mut csv, "lsm", "post_insert", &lsm_post_ins);
    csv_phase(&mut csv, "lsm", "post_update", &lsm_post_upd);

    push_table_row(&mut table, "btree", &btree_post_ins, &btree_post_upd);
    push_table_row(&mut table, "lsm", &lsm_post_ins, &lsm_post_upd);

    println!("\n=== engine_space_amplification summary ===");
    table.print();
    println!("\nCSV: {}", csv_path.display());
    println!("\nNote: 'Bloat factor' = (disk after updates) / (disk after insert).");
    println!("      A factor near 1.0 means in-place behavior (B+Tree).");
    println!("      A factor > 1.0 means accumulating obsolete versions (LSM, before compaction catches up).");
}

fn log_result(engine: &str, post_ins: &PhaseResult, post_upd: &PhaseResult) {
    let bloat = post_upd.disk_bytes as f64 / post_ins.disk_bytes.max(1) as f64;
    eprintln!(
        "  {} post-insert: user={:.2}MB disk={:.2}MB amp={:.2}x",
        engine,
        post_ins.user_bytes as f64 / 1e6,
        post_ins.disk_bytes as f64 / 1e6,
        post_ins.space_amp,
    );
    eprintln!(
        "  {} post-update: user={:.2}MB disk={:.2}MB amp={:.2}x  (bloat vs post-insert: {:.2}x)",
        engine,
        post_upd.user_bytes as f64 / 1e6,
        post_upd.disk_bytes as f64 / 1e6,
        post_upd.space_amp,
        bloat,
    );
}

fn csv_phase(csv: &mut CsvWriter, engine: &str, phase: &str, r: &PhaseResult) {
    csv.row(&[
        engine,
        phase,
        &r.user_bytes.to_string(),
        &r.disk_bytes.to_string(),
        &format!("{:.3}", r.space_amp),
    ]);
}

fn push_table_row(
    table: &mut MarkdownTable,
    engine: &str,
    post_ins: &PhaseResult,
    post_upd: &PhaseResult,
) {
    let bloat = post_upd.disk_bytes as f64 / post_ins.disk_bytes.max(1) as f64;
    table.row(&[
        engine,
        &format!("{:.2}", post_ins.disk_bytes as f64 / (1024.0 * 1024.0)),
        &format!("{:.2}", post_upd.disk_bytes as f64 / (1024.0 * 1024.0)),
        &format!("{:.2}", post_ins.space_amp),
        &format!("{:.2}", post_upd.space_amp),
        &format!("{:.2}", bloat),
    ]);
}

// ============================================================================
// B+Tree path
// ============================================================================

fn run_btree(bpm_frames: usize) -> (PhaseResult, PhaseResult) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("space.db");
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

    // --- Phase 1: insert ---
    for key in 0..INSERT_KEYS {
        let k = encode_key_i64(key as i64);
        let v = encode_value_u64(key as u64);
        tree.insert(&k, &v).unwrap();
    }
    bpm.flush_all_pages().unwrap();

    // `disk_page_count()` returns total pages allocated to the file, which
    // matches what's on disk for the BPM's FileDiskManager.
    let post_insert_disk = bpm.disk_page_count() as u64 * PAGE_SIZE as u64;
    let post_insert_user = (INSERT_KEYS * RECORD_BYTES) as u64;
    let post_insert = PhaseResult {
        user_bytes: post_insert_user,
        disk_bytes: post_insert_disk,
        space_amp: post_insert_disk as f64 / post_insert_user.max(1) as f64,
    };

    // --- Phase 2: random updates ---
    let mut rng = Lcg64::new(0xDEAD_BEEF);
    for _ in 0..UPDATE_OPS {
        let key = rng.gen_range(INSERT_KEYS);
        let k = encode_key_i64(key as i64);
        let v = encode_value_u64(rng.next_u64());
        let _ = tree.insert(&k, &v);
    }
    bpm.flush_all_pages().unwrap();

    let post_update_disk = bpm.disk_page_count() as u64 * PAGE_SIZE as u64;
    // After updates, the user-visible data is still INSERT_KEYS records.
    let post_update_user = post_insert_user;
    let post_update = PhaseResult {
        user_bytes: post_update_user,
        disk_bytes: post_update_disk,
        space_amp: post_update_disk as f64 / post_update_user.max(1) as f64,
    };

    (post_insert, post_update)
}

// ============================================================================
// LSM path
// ============================================================================

fn run_lsm(memtable_bytes: usize) -> (PhaseResult, PhaseResult) {
    let dir = tempdir().unwrap();
    let tree = LsmTree::open_with_memtable_size(dir.path(), memtable_bytes).unwrap();

    // --- Phase 1: insert ---
    for key in 0..INSERT_KEYS {
        let k = encode_key_vec(key as i64);
        let v = encode_value_vec(key as u64);
        tree.put(k, v).unwrap();
    }
    tree.flush_memtable().unwrap();

    let post_insert_disk = tree.level_state().total_disk_size();
    let post_insert_user = (INSERT_KEYS * RECORD_BYTES) as u64;
    let post_insert = PhaseResult {
        user_bytes: post_insert_user,
        disk_bytes: post_insert_disk,
        space_amp: post_insert_disk as f64 / post_insert_user.max(1) as f64,
    };

    // --- Phase 2: random updates ---
    let mut rng = Lcg64::new(0xDEAD_BEEF);
    for _ in 0..UPDATE_OPS {
        let key = rng.gen_range(INSERT_KEYS);
        let k = encode_key_vec(key as i64);
        let v = encode_value_vec(rng.next_u64());
        tree.put(k, v).unwrap();
    }
    tree.flush_memtable().unwrap();

    let post_update_disk = tree.level_state().total_disk_size();
    let post_update_user = post_insert_user;
    let post_update = PhaseResult {
        user_bytes: post_update_user,
        disk_bytes: post_update_disk,
        space_amp: post_update_disk as f64 / post_update_user.max(1) as f64,
    };

    (post_insert, post_update)
}
