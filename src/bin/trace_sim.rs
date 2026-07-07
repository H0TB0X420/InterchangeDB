//! trace_sim — trace-driven eviction-policy replay (paper reproduction).
//!
//! Produces the Clock2Q+ paper's data points (arXiv 2511.21958, §5):
//! miss ratio per policy at cache sizes {0.005, 0.01, 0.05, 0.1} × trace
//! footprint, the paper's Improvement metric vs Clock, and the Table-1
//! movement counters (claim C8). Methodology and the A#/C# references
//! below live in `docs/reproductions/clock2q-plus/REPRODUCTION.md`.
//!
//! Pipeline:
//!   trace file ──parse──▶ LBN stream ──÷ fanout──▶ metadata blocks
//!    ──dense remap──▶ replay at each fraction × footprint ──▶ CSV
//!
//! The replay is a pure cache simulation: no pins, every frame is
//! immediately evictable after access — the conditions under which the
//! paper (via libCacheSim) evaluates, so miss ratios are comparable.
//!
//! Formats:
//! - `vscsi` (CloudPhysics, v1/v2 auto-detected): little-endian records,
//!   v1 = 32 B `{sn u32, len u32, nSG u32, cmd u16, ver u16, lbn u64,
//!   ts u64}`, v2 = 40 B `{cmd u16, ver u16, sn u32, len u32, nSG u32,
//!   lbn u64, ts u64, rt u64}` (libCacheSim `vscsi.h`). The LBN sits at
//!   bytes 16..24 in both. One access per record regardless of request
//!   length (A16); reads and writes both included (A17).
//! - `oracleGeneral` (`.bin`, libCacheSim; the format the public
//!   CloudPhysics S3 mirror ships): 24 B little-endian records
//!   `{clock_time u32, obj_id u64, obj_size u32, next_access_vtime i64}`.
//!   `obj_id` is the raw vscsi LBN — verified record-for-record against
//!   the vscsi twin of the libCacheSim sample trace.
//! - `pages` (IDB-native): raw little-endian u32 per access — the BPM
//!   page-request stream captured by `tpcc --capture-trace`. Replay
//!   with `--fanout 1`: these already ARE metadata-page accesses.
//! - `txt`: one decimal block id per line.
//!
//! Usage:
//!   trace_sim --trace data/traces/cloudPhysicsIO.vscsi --fanout 200
//!   trace_sim --trace w01.oracleGeneral.bin --fanout 200 \
//!             --fractions 0.01,0.1 --policies clock,s3fifo-2bit,clock2q+

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::exit;

use interchangedb::buffer::replacer::{
    ArcReplacer, Clock2QPlusReplacer, ClockReplacer, EvictionPolicy, FifoReplacer, LruKCrpReplacer,
    LruKReplacer, LruReplacer, MovementStats, TwoQReplacer,
};
use interchangedb::common::{FrameId, PageId};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The paper's evaluation points: cache size as a fraction of footprint.
const PAPER_FRACTIONS: [f64; 4] = [0.005, 0.01, 0.05, 0.1];

/// Default comparison set: Clock baseline (the paper's Improvement
/// denominator), IDB's existing policies, and the reproduced lineage.
const DEFAULT_POLICIES: &str = "clock,fifo,lru,lruk,2q,arc,s3fifo,s3fifo-2bit,clock2q,clock2q+";

struct Config {
    trace: PathBuf,
    fanout: u64,
    fractions: Vec<f64>,
    policies: Vec<String>,
}

impl Config {
    fn parse(args: &[String]) -> Config {
        let mut trace: Option<PathBuf> = None;
        let mut fanout: u64 = 1;
        let mut fractions: Vec<f64> = PAPER_FRACTIONS.to_vec();
        let mut policies: Vec<String> = DEFAULT_POLICIES.split(',').map(str::to_string).collect();

        let mut i = 0;
        while i < args.len() {
            let arg = args[i].as_str();
            let value = |i: usize| -> &str {
                args.get(i + 1)
                    .unwrap_or_else(|| panic!("{} requires a value", args[i]))
            };
            match arg {
                "--trace" => trace = Some(PathBuf::from(value(i))),
                "--fanout" => fanout = value(i).parse().expect("--fanout: not a u64"),
                "--fractions" => {
                    fractions = value(i)
                        .split(',')
                        .map(|s| s.parse().expect("--fractions: not a float"))
                        .collect();
                }
                "--policies" => {
                    policies = value(i).split(',').map(str::to_string).collect();
                }
                "--help" | "-h" => {
                    eprintln!(
                        "usage: trace_sim --trace <file.vscsi|file.txt> \
                         [--fanout N] [--fractions f1,f2,..] [--policies p1,p2,..]"
                    );
                    exit(0);
                }
                other => panic!("unknown argument: {}", other),
            }
            i += 2;
        }

        let trace = trace.expect("--trace is required (see --help)");
        assert!(fanout >= 1, "--fanout must be >= 1");
        assert!(!fractions.is_empty(), "--fractions must be non-empty");
        for &f in &fractions {
            assert!(f > 0.0 && f <= 1.0, "fraction {} outside (0, 1]", f);
        }
        assert!(!policies.is_empty(), "--policies must be non-empty");
        Config {
            trace,
            fanout,
            fractions,
            policies,
        }
    }
}

/// Policy registry by CLI name.
/// NOTE: duplicates `make_policy` in `src/bin/tpcc.rs` — two binaries
/// can't share private items; lift into the lib if a third consumer
/// appears.
fn make_policy(name: &str, capacity: usize) -> Box<dyn EvictionPolicy> {
    match name {
        "fifo" => Box::new(FifoReplacer::new()),
        "lru" => Box::new(LruReplacer::new(capacity)),
        "lruk" => Box::new(LruKReplacer::new(2)),
        "clock" => Box::new(ClockReplacer::new()),
        "2q" => Box::new(TwoQReplacer::new(capacity)),
        "arc" => Box::new(ArcReplacer::new(capacity)),
        "clock2q+" => Box::new(Clock2QPlusReplacer::new(capacity)),
        "s3fifo" => Box::new(Clock2QPlusReplacer::s3_fifo(capacity)),
        "s3fifo-2bit" => Box::new(Clock2QPlusReplacer::s3_fifo_2bit(capacity)),
        "clock2q" => Box::new(Clock2QPlusReplacer::clock_two_q(capacity)),
        // LRU-2 with O'Neil's Correlated Reference Period + Retained
        // Information Period (the 1993 algorithm as specified — our
        // experiment, A18/A19). Suffix = CRP as % of capacity; RIP
        // fixed at capacity.
        "lruk-crp1" => Box::new(LruKCrpReplacer::new(
            2,
            (capacity as u64 / 100).max(1),
            capacity,
        )),
        "lruk-crp5" => Box::new(LruKCrpReplacer::new(
            2,
            (capacity as u64 * 5 / 100).max(1),
            capacity,
        )),
        "lruk-crp25" => Box::new(LruKCrpReplacer::new(
            2,
            (capacity as u64 * 25 / 100).max(1),
            capacity,
        )),
        // Correlation-window sensitivity variants (paper Fig 13: window
        // at 10/30/50% of the Small FIFO; 50% is the clock2q+ default).
        "clock2q+w10" => Box::new(Clock2QPlusReplacer::with_fractions(
            capacity,
            10,
            50,
            10,
            1,
            "clock2q+w10",
        )),
        "clock2q+w30" => Box::new(Clock2QPlusReplacer::with_fractions(
            capacity,
            10,
            50,
            30,
            1,
            "clock2q+w30",
        )),
        other => panic!(
            "unknown policy: {} (fifo|lru|lruk|clock|2q|arc|clock2q|clock2q+|s3fifo|s3fifo-2bit)",
            other
        ),
    }
}

// ---------------------------------------------------------------------------
// Trace loading
// ---------------------------------------------------------------------------

/// Extract one LBN per record from a vscsi trace (v1 or v2).
///
/// Stride detection: file length divisible by exactly one of {32, 40}
/// decides; divisible by both (multiples of 160) falls back to the
/// version byte — v1 has `ver`'s high byte (offset 15) == 1, v2 has it
/// (offset 3) == 2 — mirroring libCacheSim's detection.
fn parse_vscsi(bytes: &[u8]) -> Vec<u64> {
    assert!(!bytes.is_empty(), "empty vscsi trace");
    let div32 = bytes.len() % 32 == 0;
    let div40 = bytes.len() % 40 == 0;
    let stride = match (div32, div40) {
        (true, false) => 32,
        (false, true) => 40,
        (true, true) => {
            if bytes[15] == 1 {
                32
            } else {
                40
            }
        }
        (false, false) => panic!(
            "not a vscsi trace: {} bytes is neither 32- nor 40-record aligned",
            bytes.len()
        ),
    };
    // Pair assertion: the stride chosen by length must agree with the
    // version byte of the first record.
    if stride == 32 {
        assert_eq!(bytes[15], 1, "32-byte stride but ver high byte != 1");
    } else {
        assert_eq!(bytes[3], 2, "40-byte stride but ver high byte != 2");
    }

    let mut lbns = Vec::with_capacity(bytes.len() / stride);
    for record in bytes.chunks_exact(stride) {
        // LBN at bytes 16..24 in both layouts (A16: one access per
        // record; A17: reads and writes both kept, so cmd is not read).
        lbns.push(u64::from_le_bytes(record[16..24].try_into().unwrap()));
    }
    assert_eq!(lbns.len(), bytes.len() / stride);
    lbns
}

/// Extract one obj_id (= raw LBN for CloudPhysics) per oracleGeneral
/// record: 24 B `{clock_time u32, obj_id u64, obj_size u32,
/// next_access_vtime i64}`, little-endian (libCacheSim).
fn parse_oracle_general(bytes: &[u8]) -> Vec<u64> {
    assert!(!bytes.is_empty(), "empty oracleGeneral trace");
    assert_eq!(
        bytes.len() % 24,
        0,
        "not an oracleGeneral trace: {} bytes is not 24-record aligned",
        bytes.len()
    );
    let mut obj_ids = Vec::with_capacity(bytes.len() / 24);
    for record in bytes.chunks_exact(24) {
        obj_ids.push(u64::from_le_bytes(record[4..12].try_into().unwrap()));
    }
    assert_eq!(obj_ids.len(), bytes.len() / 24);
    obj_ids
}

/// Extract page ids from an IDB `.pages` capture: raw LE u32 stream
/// (written by `tpcc --capture-trace`).
fn parse_pages(bytes: &[u8]) -> Vec<u64> {
    assert!(!bytes.is_empty(), "empty pages trace");
    assert_eq!(
        bytes.len() % 4,
        0,
        "not a pages trace: {} bytes is not u32-aligned",
        bytes.len()
    );
    let mut page_ids = Vec::with_capacity(bytes.len() / 4);
    for record in bytes.chunks_exact(4) {
        page_ids.push(u64::from(u32::from_le_bytes(record.try_into().unwrap())));
    }
    assert_eq!(page_ids.len(), bytes.len() / 4);
    page_ids
}

/// Parse a text trace: one decimal block id per line, blanks skipped.
fn parse_text(text: &str) -> Vec<u64> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.trim().parse().expect("txt trace: not a u64"))
        .collect()
}

/// Load a trace and apply the metadata derivation (LBN ÷ fanout, §2.3).
fn load_accesses(config: &Config) -> Vec<u64> {
    let extension = config.trace.extension().map(|ext| ext.to_string_lossy());
    let lbns = match extension.as_deref() {
        Some("vscsi") => parse_vscsi(&std::fs::read(&config.trace).expect("read trace file")),
        Some("bin") => {
            parse_oracle_general(&std::fs::read(&config.trace).expect("read trace file"))
        }
        Some("pages") => parse_pages(&std::fs::read(&config.trace).expect("read trace file")),
        _ => parse_text(&std::fs::read_to_string(&config.trace).expect("read trace file")),
    };
    assert!(!lbns.is_empty(), "trace produced no accesses");
    lbns.into_iter().map(|lbn| lbn / config.fanout).collect()
}

/// Remap block ids to dense u32s in first-occurrence order.
///
/// Policies only need identity, and `PageId` is u32 — dense ids remove
/// any overflow concern for large-disk LBNs and yield the footprint
/// (distinct-block count) as a byproduct.
fn remap_dense(blocks: &[u64]) -> (Vec<u32>, usize) {
    let mut ids: HashMap<u64, u32> = HashMap::new();
    let mut accesses = Vec::with_capacity(blocks.len());
    for &block in blocks {
        let next = u32::try_from(ids.len()).expect("more than u32::MAX distinct blocks");
        let id = *ids.entry(block).or_insert(next);
        accesses.push(id);
    }
    assert!(ids.len() <= accesses.len());
    (accesses, ids.len())
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

struct ReplayResult {
    requests: u64,
    misses: u64,
    movement: Option<MovementStats>,
}

/// Replay `accesses` through `policy` with `cache_size` frames.
///
/// Pure cache simulation: hit ⇒ `record_access`; miss ⇒ take a free
/// frame or evict, then `record_access`. Every access is followed by
/// `set_evictable(true)` — nothing is ever pinned, so `evict()` must
/// always succeed once the pool is full (asserted).
fn replay(
    mut policy: Box<dyn EvictionPolicy>,
    cache_size: usize,
    accesses: &[u32],
) -> ReplayResult {
    assert!(cache_size >= 2, "cache_size {} < 2", cache_size);
    assert!(!accesses.is_empty(), "empty access stream");

    let mut resident: HashMap<u32, FrameId> = HashMap::with_capacity(cache_size * 2);
    let mut frame_block: Vec<u32> = vec![0; cache_size];
    let mut frames_used: usize = 0;
    let mut misses: u64 = 0;

    for &block in accesses {
        let page_id = PageId::new(block);
        if let Some(&frame_id) = resident.get(&block) {
            policy.record_access(frame_id, page_id);
            policy.set_evictable(frame_id, true);
        } else {
            misses += 1;
            let frame_id = if frames_used < cache_size {
                frames_used += 1;
                FrameId::new(frames_used - 1)
            } else {
                let victim = policy
                    .evict_for_page(page_id)
                    .expect("nothing pinned: evict must succeed");
                let evicted = resident.remove(&frame_block[victim.0]);
                assert!(evicted.is_some(), "victim frame held no resident block");
                victim
            };
            frame_block[frame_id.0] = block;
            resident.insert(block, frame_id);
            policy.record_access(frame_id, page_id);
            policy.set_evictable(frame_id, true);
        }
    }

    assert!(misses <= accesses.len() as u64);
    assert!(resident.len() <= cache_size, "residency exceeded capacity");
    ReplayResult {
        requests: accesses.len() as u64,
        misses,
        movement: policy.movement_stats(),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = Config::parse(&args);

    let blocks = load_accesses(&config);
    let (accesses, footprint) = remap_dense(&blocks);
    drop(blocks);
    eprintln!(
        "trace: {} — {} requests, {} distinct blocks (fanout {})",
        config.trace.display(),
        accesses.len(),
        footprint,
        config.fanout
    );

    println!(
        "trace,fanout,policy,cache_frac,cache_size,requests,footprint,misses,\
         miss_ratio,improvement_vs_clock,small_to_main,small_to_ghost,ghost_to_main"
    );
    let trace_name = config
        .trace
        .file_name()
        .expect("trace path has a file name")
        .to_string_lossy();

    for &fraction in &config.fractions {
        let cache_size = ((footprint as f64) * fraction).round() as usize;
        let cache_size = cache_size.max(2); // floor: Small and Main each need a slot
        eprintln!("fraction {} -> cache_size {}", fraction, cache_size);

        // Collect the whole fraction first: the Improvement column needs
        // Clock's miss ratio, which may run anywhere in the policy list.
        let mut rows: Vec<(String, ReplayResult)> = Vec::with_capacity(config.policies.len());
        for name in &config.policies {
            let policy = make_policy(name, cache_size);
            let result = replay(policy, cache_size, &accesses);
            eprintln!("  {}: {} misses", name, result.misses);
            rows.push((name.clone(), result));
        }

        let clock_miss_ratio: Option<f64> = rows
            .iter()
            .find(|(name, _)| name == "clock")
            .map(|(_, r)| r.misses as f64 / r.requests as f64);

        for (name, result) in &rows {
            let miss_ratio = result.misses as f64 / result.requests as f64;
            // Paper §5.2: Improvement = (MR_Clock − MR_algo) / MR_Clock.
            let improvement = match clock_miss_ratio {
                Some(clock_mr) if clock_mr > 0.0 => {
                    format!("{:.6}", (clock_mr - miss_ratio) / clock_mr)
                }
                _ => String::new(),
            };
            let (s2m, s2g, g2m) = match result.movement {
                Some(m) => (
                    m.small_to_main.to_string(),
                    m.small_to_ghost.to_string(),
                    m.ghost_to_main.to_string(),
                ),
                None => (String::new(), String::new(), String::new()),
            };
            println!(
                "{},{},{},{},{},{},{},{},{:.6},{},{},{},{}",
                trace_name,
                config.fanout,
                name,
                fraction,
                cache_size,
                result.requests,
                footprint,
                result.misses,
                miss_ratio,
                improvement,
                s2m,
                s2g,
                g2m
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — parser layouts, derivation, and replay semantics.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build one vscsi v1 record (32 B): lbn at 16..24, ver 0x0100 at 14..16.
    fn v1_record(lbn: u64) -> [u8; 32] {
        let mut record = [0u8; 32];
        record[14..16].copy_from_slice(&0x0100u16.to_le_bytes());
        record[16..24].copy_from_slice(&lbn.to_le_bytes());
        record
    }

    /// Build one vscsi v2 record (40 B): lbn at 16..24, ver 0x0200 at 2..4.
    fn v2_record(lbn: u64) -> [u8; 40] {
        let mut record = [0u8; 40];
        record[2..4].copy_from_slice(&0x0200u16.to_le_bytes());
        record[16..24].copy_from_slice(&lbn.to_le_bytes());
        record
    }

    // Layout check: v1 records round-trip their LBNs. 3 records = 96
    // bytes — divisible by 32 only, so stride comes from length alone.
    #[test]
    fn vscsi_v1_lbns_parse() {
        let mut bytes = Vec::new();
        for lbn in [42932745u64, 7, u64::from(u32::MAX) + 1] {
            bytes.extend_from_slice(&v1_record(lbn));
        }
        assert_eq!(
            parse_vscsi(&bytes),
            vec![42932745, 7, u64::from(u32::MAX) + 1]
        );
    }

    // Layout check: v2 records (40 B stride, ver at offset 2) round-trip.
    #[test]
    fn vscsi_v2_lbns_parse() {
        let mut bytes = Vec::new();
        for lbn in [1u64, 2, 3] {
            bytes.extend_from_slice(&v2_record(lbn));
        }
        assert_eq!(parse_vscsi(&bytes), vec![1, 2, 3]);
    }

    // 5 v1 records = 160 bytes — divisible by both strides. The version
    // byte must disambiguate to v1 (32).
    #[test]
    fn vscsi_ambiguous_length_uses_version_byte() {
        let mut bytes = Vec::new();
        for lbn in 0..5u64 {
            bytes.extend_from_slice(&v1_record(lbn));
        }
        assert_eq!(bytes.len(), 160);
        assert_eq!(parse_vscsi(&bytes), vec![0, 1, 2, 3, 4]);
        // And 4 v2 records = 160 bytes disambiguate to v2 (40).
        let mut bytes = Vec::new();
        for lbn in 0..4u64 {
            bytes.extend_from_slice(&v2_record(lbn));
        }
        assert_eq!(bytes.len(), 160);
        assert_eq!(parse_vscsi(&bytes), vec![0, 1, 2, 3]);
    }

    // Layout check: oracleGeneral records (24 B, obj_id at 4..12)
    // round-trip their ids.
    #[test]
    fn oracle_general_obj_ids_parse() {
        let mut bytes = Vec::new();
        for obj_id in [42932745u64, 7, u64::from(u32::MAX) + 1] {
            let mut record = [0u8; 24];
            record[4..12].copy_from_slice(&obj_id.to_le_bytes());
            record[20] = 0xFF; // non-zero next_vtime bytes must not leak into obj_id
            bytes.extend_from_slice(&record);
        }
        assert_eq!(
            parse_oracle_general(&bytes),
            vec![42932745, 7, u64::from(u32::MAX) + 1]
        );
    }

    // Layout check: .pages records (raw LE u32) round-trip.
    #[test]
    fn pages_capture_parses() {
        let mut bytes = Vec::new();
        for page in [7u32, 0, u32::MAX] {
            bytes.extend_from_slice(&page.to_le_bytes());
        }
        assert_eq!(parse_pages(&bytes), vec![7, 0, u64::from(u32::MAX)]);
    }

    // The paper's derivation (§2.3): adjacent data LBNs collapse onto the
    // same metadata block under ÷fanout.
    #[test]
    fn fanout_derivation_collapses_adjacent_lbns() {
        let lbns = [0u64, 199, 200, 399, 400];
        let derived: Vec<u64> = lbns.iter().map(|lbn| lbn / 200).collect();
        assert_eq!(derived, vec![0, 0, 1, 1, 2]);
    }

    #[test]
    fn dense_remap_preserves_identity_and_counts_footprint() {
        let (accesses, footprint) = remap_dense(&[500, 7, 500, 9, 7]);
        assert_eq!(accesses, vec![0, 1, 0, 2, 1]);
        assert_eq!(footprint, 3);
    }

    // Hand-computed LRU: cache 2, trace [1,2,1,3,1]:
    // miss 1, miss 2, hit 1, miss 3 (evicts LRU=2), hit 1 → 3 misses.
    #[test]
    fn replay_lru_matches_hand_computation() {
        let result = replay(make_policy("lru", 2), 2, &[1, 2, 1, 3, 1]);
        assert_eq!(result.requests, 5);
        assert_eq!(result.misses, 3);
        assert!(result.movement.is_none(), "LRU has no movement counters");
    }

    // Every registered policy replays a mixed loop+hot synthetic without
    // panicking, and misses stay within [footprint, requests] — the first
    // touch of each distinct block is always a miss, and misses can never
    // exceed requests.
    #[test]
    fn replay_all_policies_within_bounds() {
        let mut trace: Vec<u32> = Vec::new();
        for round in 0..50u32 {
            for block in 0..40 {
                trace.push(block); // 40-block loop, cache-larger working set
            }
            trace.push(round % 4); // small hot set
        }
        let (accesses, footprint) =
            remap_dense(&trace.iter().map(|&b| u64::from(b)).collect::<Vec<u64>>());
        for name in DEFAULT_POLICIES.split(',') {
            let result = replay(make_policy(name, 10), 10, &accesses);
            assert!(
                result.misses >= footprint as u64,
                "{}: misses {} < footprint {}",
                name,
                result.misses,
                footprint
            );
            assert!(
                result.misses <= result.requests,
                "{}: misses exceed requests",
                name
            );
        }
    }

    // C8 instrumentation path: lineage policies surface movement counters
    // through the trait; on a churn trace all three counters engage.
    #[test]
    fn movement_counters_surface_through_trait() {
        let mut trace: Vec<u32> = Vec::new();
        for round in 0..20u32 {
            for block in 0..30 {
                trace.push(block);
                if block % 3 == 0 {
                    trace.push(block); // immediate re-touch (correlated)
                }
            }
            trace.push(round % 5); // recurring hot blocks
        }
        let result = replay(make_policy("clock2q+", 12), 12, &trace);
        let movement = result.movement.expect("clock2q+ reports movement");
        assert!(movement.small_to_ghost > 0, "churn must demote via Small");
    }
}
