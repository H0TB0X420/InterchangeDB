//! Shared utilities for engine-comparison benchmarks.
//!
//! Each bench file pulls this in via:
//! ```ignore
//! #[path = "common/mod.rs"]
//! mod common;
//! ```
//!
//! Cargo only auto-discovers `.rs` files directly inside `benches/` as bench
//! targets, so placing this code under `benches/common/` keeps it out of the
//! target list while letting siblings include it.
//!
//! Modules:
//! - [`encoding`]: key/value encoding shared with the existing benches
//! - [`rng`]: deterministic RNGs (LCG + Zipfian) — no `rand` dep
//! - [`hist`]: latency histogram with percentile queries
//! - [`output`]: CSV writer + Markdown table printer
//! - [`poller`]: SSTable-directory poller for LSM write amplification
//! - [`budget`]: helpers to keep BPM and memtable byte budgets aligned

#![allow(dead_code)]

pub mod encoding {
    //! Shared key/value encoding. Matches `btree_bench.rs` and `lsm_bench.rs`
    //! so cross-bench numbers are directly comparable.

    /// Encode an `i64` key for big-endian, sign-correct lexicographic order.
    /// XOR'ing the sign bit maps `i64::MIN..=i64::MAX` onto `u64::MIN..=u64::MAX`
    /// while preserving order.
    pub fn encode_key_i64(val: i64) -> [u8; 8] {
        let unsigned = (val as u64) ^ (1u64 << 63);
        unsigned.to_be_bytes()
    }

    /// Same as `encode_key_i64`, but returns a `Vec<u8>` (LSM API takes owned).
    pub fn encode_key_vec(val: i64) -> Vec<u8> {
        encode_key_i64(val).to_vec()
    }

    /// Encode a `u64` value as little-endian 8 bytes.
    pub fn encode_value_u64(val: u64) -> [u8; 8] {
        val.to_le_bytes()
    }

    /// Same as `encode_value_u64`, but returns a `Vec<u8>` (LSM API takes owned).
    pub fn encode_value_vec(val: u64) -> Vec<u8> {
        encode_value_u64(val).to_vec()
    }
}

pub mod rng {
    //! Deterministic RNGs implemented inline (no `rand` dep).
    //!
    //! - `Lcg64` is the wrapping mul-add LCG already used in `btree_bench` and
    //!   `lsm_bench`. Cheap, decent quality for uniform sampling.
    //! - `Zipfian` follows the Gray-Sandholm-Shenker formulation used by the
    //!   YCSB benchmark suite. Theta = 0.99 is the standard "skewed" setting.

    /// Linear congruential generator (PCG-style multiplier).
    /// Same constants as the existing benches so seeds are directly comparable.
    pub struct Lcg64 {
        state: u64,
    }

    impl Lcg64 {
        /// Create a new LCG seeded with `seed`. Seed 0 is fine (state stays nonzero
        /// after the first step because the additive constant is odd).
        pub fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        /// Advance and return the next 64-bit raw output.
        #[inline]
        pub fn next_u64(&mut self) -> u64 {
            self.state = self
                .state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.state >> 33
        }

        /// Uniform integer in `[0, n)` via modulo. n must be > 0.
        /// Modulo bias is negligible for the `n` values we use (≤ 10^7 vs 2^31).
        #[inline]
        pub fn gen_range(&mut self, n: usize) -> usize {
            (self.next_u64() as usize) % n
        }

        /// Uniform `f64` in `[0, 1)`.
        #[inline]
        pub fn next_f64(&mut self) -> f64 {
            // Use the top 53 bits to fill an f64 mantissa.
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    /// Zipfian distribution over `[0, n)`. `theta` controls skew:
    /// theta = 0    → uniform
    /// theta = 0.99 → standard YCSB skew (top 1% of keys ≈ 50% of accesses)
    /// theta → 1    → highly skewed
    ///
    /// Implements the Gray-Sandholm-Shenker inversion from
    /// "Quickly Generating Billion-Record Synthetic Databases" (Gray et al. 1994),
    /// the same algorithm used by YCSB's ZipfianGenerator.
    pub struct Zipfian {
        n: usize,
        theta: f64,
        alpha: f64,
        zetan: f64,
        eta: f64,
        zeta2: f64,
    }

    impl Zipfian {
        /// Build a generator over `[0, n)`. Pre-computes zetan in O(n).
        /// For very large n the constructor is the dominant cost; use sparingly.
        pub fn new(n: usize, theta: f64) -> Self {
            assert!(n > 0, "Zipfian requires n > 0");
            assert!((0.0..1.0).contains(&theta), "theta must be in [0, 1)");

            let zetan = zeta(n, theta);
            let zeta2 = zeta(2, theta);
            let alpha = 1.0 / (1.0 - theta);
            let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);

            Self {
                n,
                theta,
                alpha,
                zetan,
                eta,
                zeta2,
            }
        }

        /// Draw the next sample. Lower indices are hotter.
        pub fn sample(&self, rng: &mut Lcg64) -> usize {
            let u = rng.next_f64();
            let uz = u * self.zetan;
            if uz < 1.0 {
                return 0;
            }
            if uz < 1.0 + 0.5_f64.powf(self.theta) {
                return 1;
            }
            let idx = (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as usize;
            idx.min(self.n - 1)
        }
    }

    /// Generalized harmonic number sum_{i=1..=n} (1/i)^theta.
    fn zeta(n: usize, theta: f64) -> f64 {
        let mut sum = 0.0;
        for i in 1..=n {
            sum += 1.0 / (i as f64).powf(theta);
        }
        sum
    }
}

pub mod hist {
    //! Latency histogram with sort-and-index percentile queries.
    //!
    //! Trades constant-time inserts for O(n log n) on the first `percentile()`
    //! call. Fine for our bench sizes (≤ 1M samples). No external HDR dep.

    use std::time::Duration;

    pub struct LatencyHistogram {
        samples_ns: Vec<u64>,
        sorted: bool,
        /// Optional cap. Samples beyond this are dropped (but `dropped_count`
        /// still increments so the caller can audit). `None` = unlimited.
        cap: Option<usize>,
        dropped_count: u64,
    }

    impl LatencyHistogram {
        pub fn new() -> Self {
            Self {
                samples_ns: Vec::new(),
                sorted: true,
                cap: None,
                dropped_count: 0,
            }
        }

        pub fn with_capacity(cap: usize) -> Self {
            Self {
                samples_ns: Vec::with_capacity(cap),
                sorted: true,
                cap: None,
                dropped_count: 0,
            }
        }

        /// Build a histogram that retains at most `max_samples` records;
        /// further samples are dropped from the histogram (op count is the
        /// caller's job). 8 bytes per sample, so ~8 MB for 1 M samples.
        pub fn with_cap(max_samples: usize) -> Self {
            Self {
                samples_ns: Vec::with_capacity(max_samples.min(1 << 16)),
                sorted: true,
                cap: Some(max_samples),
                dropped_count: 0,
            }
        }

        /// Record a duration sample. Sub-nanosecond precision is lost.
        pub fn record(&mut self, d: Duration) {
            if let Some(cap) = self.cap {
                if self.samples_ns.len() >= cap {
                    self.dropped_count += 1;
                    return;
                }
            }
            self.samples_ns.push(d.as_nanos() as u64);
            self.sorted = false;
        }

        /// Number of recorded samples.
        pub fn len(&self) -> usize {
            self.samples_ns.len()
        }
        pub fn is_empty(&self) -> bool {
            self.samples_ns.is_empty()
        }
        pub fn dropped(&self) -> u64 {
            self.dropped_count
        }

        /// Percentile in `[0.0, 1.0]`. Returns `Duration::ZERO` if empty.
        /// Uses nearest-rank: index = ceil(p * n) - 1, clamped to [0, n-1].
        pub fn percentile(&mut self, p: f64) -> Duration {
            assert!((0.0..=1.0).contains(&p), "p must be in [0,1]");
            if self.samples_ns.is_empty() {
                return Duration::ZERO;
            }
            self.ensure_sorted();
            let n = self.samples_ns.len();
            let idx = ((p * n as f64).ceil() as usize)
                .saturating_sub(1)
                .min(n - 1);
            Duration::from_nanos(self.samples_ns[idx])
        }

        /// Mean latency. `Duration::ZERO` if empty.
        pub fn mean(&self) -> Duration {
            if self.samples_ns.is_empty() {
                return Duration::ZERO;
            }
            let total: u128 = self.samples_ns.iter().map(|&x| x as u128).sum();
            Duration::from_nanos((total / self.samples_ns.len() as u128) as u64)
        }

        /// Maximum sample. `Duration::ZERO` if empty.
        pub fn max(&self) -> Duration {
            self.samples_ns
                .iter()
                .copied()
                .max()
                .map(Duration::from_nanos)
                .unwrap_or(Duration::ZERO)
        }

        fn ensure_sorted(&mut self) {
            if !self.sorted {
                self.samples_ns.sort_unstable();
                self.sorted = true;
            }
        }
    }

    impl Default for LatencyHistogram {
        fn default() -> Self {
            Self::new()
        }
    }
}

pub mod output {
    //! CSV writer + Markdown table printer.
    //!
    //! Both target two consumers:
    //! - CSV: spreadsheet / plotter pipeline
    //! - Markdown: pasted directly into blog posts

    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    /// Path used by every bench: `target/bench-results/<bench>.csv`.
    /// Created on demand.
    pub fn results_path(bench_name: &str) -> PathBuf {
        let dir = Path::new("target").join("bench-results");
        fs::create_dir_all(&dir).expect("create target/bench-results");
        dir.join(format!("{bench_name}.csv"))
    }

    /// Append-mode CSV writer. Auto-writes the header on first row.
    pub struct CsvWriter {
        path: PathBuf,
        header: Vec<String>,
        wrote_header: bool,
    }

    impl CsvWriter {
        /// Open (truncating) at `path`. Stores the header but defers writing
        /// it until the first row, so an unused writer leaves no empty file.
        pub fn create<P: AsRef<Path>>(path: P, header: &[&str]) -> Self {
            let path = path.as_ref().to_path_buf();
            let _ = fs::remove_file(&path);
            Self {
                path,
                header: header.iter().map(|s| s.to_string()).collect(),
                wrote_header: false,
            }
        }

        /// Append a row. Field count must match the header.
        pub fn row(&mut self, fields: &[&str]) {
            assert_eq!(
                fields.len(),
                self.header.len(),
                "CSV row width must match header"
            );
            let mut f = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .expect("open csv");
            if !self.wrote_header {
                writeln!(f, "{}", self.header.join(",")).unwrap();
                self.wrote_header = true;
            }
            writeln!(f, "{}", fields.join(",")).unwrap();
        }

        pub fn path(&self) -> &Path {
            &self.path
        }
    }

    /// Collects rows in memory and prints a Markdown table at the end.
    pub struct MarkdownTable {
        header: Vec<String>,
        rows: Vec<Vec<String>>,
        align_right: Vec<bool>,
    }

    impl MarkdownTable {
        /// Create a table. `align_right[i] == true` right-aligns column i
        /// (use for numeric columns).
        pub fn new(header: &[&str], align_right: &[bool]) -> Self {
            assert_eq!(header.len(), align_right.len());
            Self {
                header: header.iter().map(|s| s.to_string()).collect(),
                rows: Vec::new(),
                align_right: align_right.to_vec(),
            }
        }

        pub fn row(&mut self, fields: &[&str]) {
            assert_eq!(fields.len(), self.header.len());
            self.rows
                .push(fields.iter().map(|s| s.to_string()).collect());
        }

        /// Print to stdout in GitHub-flavoured Markdown.
        pub fn print(&self) {
            // Column widths.
            let mut widths: Vec<usize> = self.header.iter().map(|h| h.len()).collect();
            for row in &self.rows {
                for (i, cell) in row.iter().enumerate() {
                    widths[i] = widths[i].max(cell.len());
                }
            }

            // Header.
            print!("|");
            for (i, h) in self.header.iter().enumerate() {
                print!(" {:>w$} |", h, w = widths[i]);
            }
            println!();

            // Separator with alignment hints.
            print!("|");
            for (i, &right) in self.align_right.iter().enumerate() {
                let bar = "-".repeat(widths[i] + 2);
                if right {
                    // ` ----: |` (right-align)
                    print!("{}:|", &bar[..bar.len() - 1]);
                } else {
                    print!("{}|", bar);
                }
            }
            println!();

            // Rows.
            for row in &self.rows {
                print!("|");
                for (i, cell) in row.iter().enumerate() {
                    if self.align_right[i] {
                        print!(" {:>w$} |", cell, w = widths[i]);
                    } else {
                        print!(" {:<w$} |", cell, w = widths[i]);
                    }
                }
                println!();
            }
        }
    }
}

pub mod poller {
    //! Filesystem-level write-amp tracker for the LSM.
    //!
    //! No source-code instrumentation needed: a background thread polls the
    //! `data_dir/sst/` directory at a fixed cadence and tracks per-file sizes.
    //! When a file vanishes between polls (compaction deleted it), its last
    //! recorded size is added to a "cumulative_deleted_bytes" counter.
    //!
    //! At the end of a run:
    //!   total_bytes_written ≈ cumulative_deleted_bytes + current_disk_size
    //!   WAF                 ≈ total_bytes_written / user_bytes_inserted
    //!
    //! ## Caveat
    //! Sub-poll-interval flush + compact storms are missed (file appears and
    //! disappears between two polls). The reported number is a lower bound.
    //! A 50 ms cadence is safe for 100 MB-class workloads.

    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    pub struct SstDirPoller {
        stop: Arc<AtomicBool>,
        deleted_bytes: Arc<AtomicU64>,
        current_bytes: Arc<AtomicU64>,
        handle: Option<JoinHandle<()>>,
    }

    impl SstDirPoller {
        /// Spawn a poller thread for `sst_dir` at `interval` cadence.
        /// Drop the returned poller (or call `finish`) to stop and read totals.
        pub fn start(sst_dir: PathBuf, interval: Duration) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let deleted_bytes = Arc::new(AtomicU64::new(0));
            let current_bytes = Arc::new(AtomicU64::new(0));

            let stop_t = stop.clone();
            let deleted_t = deleted_bytes.clone();
            let current_t = current_bytes.clone();

            let handle = thread::spawn(move || {
                let mut last: HashMap<u64, u64> = HashMap::new();
                while !stop_t.load(Ordering::Relaxed) {
                    let snapshot = scan_sst_dir(&sst_dir);
                    let mut total = 0u64;

                    // Detect deletions: anything in `last` not in `snapshot` was deleted.
                    for (id, size) in &last {
                        if !snapshot.contains_key(id) {
                            deleted_t.fetch_add(*size, Ordering::Relaxed);
                        }
                    }

                    for &size in snapshot.values() {
                        total += size;
                    }
                    current_t.store(total, Ordering::Relaxed);
                    last = snapshot;

                    thread::sleep(interval);
                }

                // Final pass: any remaining files contribute to current_bytes.
                let snapshot = scan_sst_dir(&sst_dir);
                let total: u64 = snapshot.values().sum();
                current_t.store(total, Ordering::Relaxed);
            });

            Self {
                stop,
                deleted_bytes,
                current_bytes,
                handle: Some(handle),
            }
        }

        /// Cumulative bytes of SST files deleted by compaction.
        pub fn deleted_bytes(&self) -> u64 {
            self.deleted_bytes.load(Ordering::Relaxed)
        }

        /// Current total bytes across all SST files.
        pub fn current_bytes(&self) -> u64 {
            self.current_bytes.load(Ordering::Relaxed)
        }

        /// Stop the poller and return (deleted_bytes, current_bytes).
        pub fn finish(mut self) -> (u64, u64) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
            (self.deleted_bytes(), self.current_bytes())
        }
    }

    impl Drop for SstDirPoller {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }
    }

    /// Scan one snapshot of the SST dir. Returns `{id -> size}`.
    /// Filename format is `{id:06}.sst`; non-matching files are ignored.
    fn scan_sst_dir(sst_dir: &std::path::Path) -> HashMap<u64, u64> {
        let mut out = HashMap::new();
        let entries = match std::fs::read_dir(sst_dir) {
            Ok(e) => e,
            Err(_) => return out, // dir not yet created
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let s = name.to_string_lossy();
            // Strip ".sst" suffix and parse the rest as u64.
            let id_str = match s.strip_suffix(".sst") {
                Some(x) => x,
                None => continue,
            };
            let id = match id_str.parse::<u64>() {
                Ok(x) => x,
                Err(_) => continue,
            };
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            out.insert(id, size);
        }
        out
    }
}

pub mod budget {
    //! Helpers to keep BPM and memtable byte budgets aligned across engines.
    //!
    //! Both engines should "see" the same RAM budget so cross-engine numbers
    //! are comparable. The B+Tree's BPM holds N frames of 4096 bytes each;
    //! the LSM's memtable_size_limit is given directly in bytes.

    /// Page size used by the B+Tree's BPM.
    pub const PAGE_SIZE: usize = 4096;

    /// Convert a frame count to a byte budget.
    pub const fn pool_to_bytes(frames: usize) -> usize {
        frames * PAGE_SIZE
    }

    /// Convert a byte budget to the equivalent frame count.
    pub const fn bytes_to_pool(bytes: usize) -> usize {
        bytes / PAGE_SIZE
    }
}
