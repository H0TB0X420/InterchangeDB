//! TPC-H harness (Phase H5).
//!
//! A standalone driver that generates a deterministic, spec-shaped TPC-H
//! dataset at a small scale factor and runs the 22 TPC-H queries through the
//! SQL surface. Three roles:
//!
//!   - **Generate** (default, or `--csv-out DIR`): build the dataset from a
//!     seed. With `--csv-out` it writes one RFC-4180 CSV per table plus a
//!     `schema.sql` (the oracle's input, `scripts/tpch_oracle.sh`); otherwise
//!     it loads the data through a `Session` and exits.
//!   - **Validate** (`--validate`): load the generated data, run every
//!     `queries/tpch/*.sql`, and diff each result against the committed oracle
//!     output in `tests/tpch/expected/*.csv` under TPC-H normalization, across
//!     all six planner × execution-model configurations. Fails the process on
//!     any mismatch.
//!   - **Time** (`--time`): an informational per-query timing table (not a
//!     gate).
//!
//! Determinism: everything derives from `--seed` (fixed default) via an inline
//! SplitMix64 PRNG — no external RNG dependency, no `dbgen`. Regenerating with
//! the same seed and scale factor reproduces byte-identical CSVs.
//!
//! ## The Session stack
//!
//! Loading uses the tests' default tier: an in-memory `MemoryDiskManager`
//! behind a `BufferPoolManager`, a `BTreeEngine`, and a WAL in `NoSync` mode
//! (durability is not under test here — correctness is). This mirrors
//! `tests/it/slt.rs` and `tests/it/tpcc_transactions_test.rs`.
//!
//! Run:
//!   cargo run --release --bin tpch -- --validate
//!   cargo run --release --bin tpch -- --csv-out /tmp/tpch_csv
//!   cargo run --release --bin tpch -- --time

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::ExecModel;
use interchangedb::session::{QueryResult, Session};
use interchangedb::sql::{Planner, RuleBasedPlanner, SelingerPlanner, VolcanoPlanner};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::{civil, ColumnType, Decimal, Value};
use interchangedb::wal::SyncMode;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Fixed default seed — TPC-H's start date, chosen for memorability. Every
/// value in the dataset derives deterministically from it.
const DEFAULT_SEED: u64 = 19_920_101;

/// Per-query wall-clock guard (seconds). A cell that exceeds it FAILS loudly
/// with a FEASIBILITY marker instead of hanging the whole matrix — a grind is
/// then diagnosable from the progress log and rerunnable via `--queries`.
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 120;

/// Upper bound on `--scale-factor`. This harness materializes the whole dataset
/// in memory before loading (SF 1.0 ≈ 6M lineitem rows), so an unbounded factor
/// turns a typo (`--scale-factor 100`) into an OOM. Limit-on-everything: refuse
/// loudly and name the bound rather than let the allocator take the machine
/// down. SF 1.0 is already far past this harness's intended small-scale role.
const SCALE_FACTOR_MAX: f64 = 1.0;

struct Config {
    scale_factor: f64,
    seed: u64,
    csv_out: Option<PathBuf>,
    validate: bool,
    time: bool,
    queries_dir: PathBuf,
    expected_dir: PathBuf,
    /// Which query numbers to run (`--queries q09,q21`); defaults to all 22.
    queries: Vec<u8>,
    /// Which planner/exec-model cells to run (`--configs rule-based/volcano`);
    /// defaults to all six.
    configs: Vec<(&'static str, &'static str)>,
    timeout_secs: u64,
}

impl Config {
    fn from_args() -> Config {
        let manifest = env!("CARGO_MANIFEST_DIR");
        let mut c = Config {
            scale_factor: 0.01,
            seed: DEFAULT_SEED,
            csv_out: None,
            validate: false,
            time: false,
            queries_dir: PathBuf::from(manifest).join("queries/tpch"),
            expected_dir: PathBuf::from(manifest).join("tests/tpch/expected"),
            queries: QUERY_IDS.to_vec(),
            configs: configs(),
            timeout_secs: DEFAULT_QUERY_TIMEOUT_SECS,
        };
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            let mut value = || args.next().expect("flag needs a value");
            match flag.as_str() {
                "--scale-factor" => c.scale_factor = value().parse().expect("float"),
                "--seed" => c.seed = value().parse().expect("int"),
                "--csv-out" => c.csv_out = Some(PathBuf::from(value())),
                "--validate" => c.validate = true,
                "--time" => c.time = true,
                "--queries-dir" => c.queries_dir = PathBuf::from(value()),
                "--expected-dir" => c.expected_dir = PathBuf::from(value()),
                "--queries" => c.queries = parse_query_list(&value()),
                "--configs" => c.configs = parse_config_list(&value()),
                "--timeout-secs" => c.timeout_secs = value().parse().expect("int"),
                other => panic!("unknown flag: {}", other),
            }
        }
        assert!(c.scale_factor > 0.0, "scale factor must be positive");
        assert!(
            c.scale_factor <= SCALE_FACTOR_MAX,
            "scale factor {} exceeds the {SCALE_FACTOR_MAX} maximum — this harness \
             materializes the whole dataset in memory; a larger factor would OOM \
             (raise SCALE_FACTOR_MAX deliberately if you truly need it)",
            c.scale_factor
        );
        assert!(c.timeout_secs > 0, "timeout must be positive");
        c
    }
}

/// Parse `--queries` values like `q09,q21` (or bare `9,21`) into query ids.
fn parse_query_list(text: &str) -> Vec<u8> {
    let ids: Vec<u8> = text
        .split(',')
        .map(|part| {
            let digits = part.trim().trim_start_matches(['q', 'Q']);
            let id: u8 = digits
                .parse()
                .unwrap_or_else(|_| panic!("bad query id '{part}' (want e.g. q09)"));
            assert!(QUERY_IDS.contains(&id), "query id {id} out of range 1..=22");
            id
        })
        .collect();
    assert!(!ids.is_empty(), "--queries needs at least one id");
    ids
}

/// Parse `--configs` values like `rule-based/volcano,memo/push` against the
/// closed set of six known cells.
fn parse_config_list(text: &str) -> Vec<(&'static str, &'static str)> {
    let all = configs();
    let picked: Vec<(&'static str, &'static str)> = text
        .split(',')
        .map(|part| {
            let want = part.trim();
            *all.iter()
                .find(|(planner, exec_model)| format!("{planner}/{exec_model}") == want)
                .unwrap_or_else(|| panic!("bad config '{want}' (want e.g. rule-based/volcano)"))
        })
        .collect();
    assert!(!picked.is_empty(), "--configs needs at least one cell");
    picked
}

// ---------------------------------------------------------------------------
// Deterministic PRNG — inline SplitMix64, no external crate.
// ---------------------------------------------------------------------------

/// SplitMix64 (Steele et al. 2014). A single 64-bit state advanced by the
/// golden-ratio increment; each `next` runs the finalizing mix. Chosen over an
/// LCG for better low-bit quality (the generator picks categories with small
/// moduli, where LCG low bits are visibly patterned).
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`. `n` must be positive.
    fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0);
        self.next_u64() % n
    }

    /// Uniform in `[lo, hi]` (inclusive). `lo <= hi`.
    fn between(&mut self, lo: i64, hi: i64) -> i64 {
        assert!(lo <= hi);
        lo + self.below((hi - lo + 1) as u64) as i64
    }

    /// A uniformly chosen element of `slice`.
    fn pick<'a, T>(&mut self, slice: &'a [T]) -> &'a T {
        &slice[self.below(slice.len() as u64) as usize]
    }
}

// ---------------------------------------------------------------------------
// Domains — spec-shaped value sets.
// ---------------------------------------------------------------------------

const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

/// (nation name, region key). Index is the nation key — the phone country code
/// is `nation_key + 10`, so Q22's country codes ('13' → key 3, etc.) fall out.
const NATIONS: [(&str, i64); 25] = [
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
];

const SEGMENTS: [&str; 5] = [
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "HOUSEHOLD",
    "MACHINERY",
];

const TYPE_S1: [&str; 6] = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
const TYPE_S2: [&str; 5] = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"];
const TYPE_S3: [&str; 5] = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"];

const CONT_S1: [&str; 5] = ["SM", "LG", "MED", "JUMBO", "WRAP"];
const CONT_S2: [&str; 8] = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"];

const SHIPMODES: [&str; 7] = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
const SHIPINSTRUCT: [&str; 4] = [
    "DELIVER IN PERSON",
    "COLLECT COD",
    "NONE",
    "TAKE BACK RETURN",
];
const PRIORITIES: [&str; 5] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

/// Color adjectives for `p_name` (spec uses a 92-word list). Kept to <= 10
/// chars each so five joined by spaces fit `VARCHAR(55)`. Includes 'forest'
/// (Q20's `LIKE 'forest%'`) and 'green' (Q9's `LIKE '%green%'`).
const COLORS: [&str; 40] = [
    "almond",
    "antique",
    "azure",
    "beige",
    "bisque",
    "black",
    "blue",
    "blush",
    "brown",
    "coral",
    "cornsilk",
    "cream",
    "cyan",
    "drab",
    "floral",
    "forest",
    "frosted",
    "ghost",
    "goldenrod",
    "green",
    "grey",
    "honeydew",
    "hot",
    "ivory",
    "khaki",
    "lavender",
    "lemon",
    "lime",
    "magenta",
    "maroon",
    "medium",
    "metallic",
    "midnight",
    "mint",
    "misty",
    "navy",
    "olive",
    "orange",
    "orchid",
    "yellow",
];

/// Filler words for comments (comma/quote-free so CSV cells never need
/// quoting).
const WORDS: [&str; 16] = [
    "carefully",
    "furiously",
    "slyly",
    "blithely",
    "quickly",
    "final",
    "regular",
    "ironic",
    "bold",
    "express",
    "pending",
    "unusual",
    "even",
    "silent",
    "quiet",
    "brave",
];

// ---------------------------------------------------------------------------
// Cardinalities
// ---------------------------------------------------------------------------

/// Row counts at a scale factor, using the spec's ratios. `nation` and
/// `region` are fixed; the rest scale linearly with SF.
struct Cardinality {
    suppliers: i64,
    parts: i64,
    customers: i64,
    orders: i64,
}

impl Cardinality {
    fn from_scale(sf: f64) -> Cardinality {
        // Round to the nearest whole row; guard a floor of one so tiny SFs
        // still produce a non-empty table.
        let scaled = |base: f64| ((sf * base).round() as i64).max(1);
        Cardinality {
            suppliers: scaled(10_000.0),
            parts: scaled(200_000.0),
            customers: scaled(150_000.0),
            orders: scaled(1_500_000.0),
        }
    }
}

// ---------------------------------------------------------------------------
// Schema (single source of truth for both the loader and the CSV oracle).
// ---------------------------------------------------------------------------

/// The eight table definitions, in dependency-free creation order. Written to
/// `schema.sql` under `--csv-out` so DuckDB and this engine share one schema.
/// Types are chosen so every query plans on this engine: `l_quantity` is
/// `DECIMAL` (not `INT`) so Q9's `l_extendedprice*(1-l_discount) -
/// ps_supplycost*l_quantity` has matching scale-4 operands on both sides of the
/// subtraction.
const SCHEMA: [&str; 8] = [
    "CREATE TABLE region (\
        r_regionkey INT PRIMARY KEY, \
        r_name CHAR(25), \
        r_comment VARCHAR(152))",
    "CREATE TABLE nation (\
        n_nationkey INT PRIMARY KEY, \
        n_name CHAR(25), \
        n_regionkey INT, \
        n_comment VARCHAR(152))",
    "CREATE TABLE supplier (\
        s_suppkey INT PRIMARY KEY, \
        s_name CHAR(25), \
        s_address VARCHAR(40), \
        s_nationkey INT, \
        s_phone CHAR(15), \
        s_acctbal DECIMAL(12,2), \
        s_comment VARCHAR(101))",
    "CREATE TABLE part (\
        p_partkey INT PRIMARY KEY, \
        p_name VARCHAR(55), \
        p_mfgr CHAR(25), \
        p_brand CHAR(10), \
        p_type VARCHAR(25), \
        p_size INT, \
        p_container CHAR(10), \
        p_retailprice DECIMAL(12,2), \
        p_comment VARCHAR(23))",
    "CREATE TABLE partsupp (\
        ps_partkey INT, \
        ps_suppkey INT, \
        ps_availqty INT, \
        ps_supplycost DECIMAL(12,2), \
        ps_comment VARCHAR(199), \
        PRIMARY KEY (ps_partkey, ps_suppkey))",
    "CREATE TABLE customer (\
        c_custkey INT PRIMARY KEY, \
        c_name VARCHAR(25), \
        c_address VARCHAR(40), \
        c_nationkey INT, \
        c_phone CHAR(15), \
        c_acctbal DECIMAL(12,2), \
        c_mktsegment CHAR(10), \
        c_comment VARCHAR(117))",
    "CREATE TABLE orders (\
        o_orderkey INT PRIMARY KEY, \
        o_custkey INT, \
        o_orderstatus CHAR(1), \
        o_totalprice DECIMAL(12,2), \
        o_orderdate DATE, \
        o_orderpriority CHAR(15), \
        o_clerk CHAR(15), \
        o_shippriority INT, \
        o_comment VARCHAR(79))",
    "CREATE TABLE lineitem (\
        l_orderkey INT, \
        l_partkey INT, \
        l_suppkey INT, \
        l_linenumber INT, \
        l_quantity DECIMAL(12,2), \
        l_extendedprice DECIMAL(12,2), \
        l_discount DECIMAL(3,2), \
        l_tax DECIMAL(3,2), \
        l_returnflag CHAR(1), \
        l_linestatus CHAR(1), \
        l_shipdate DATE, \
        l_commitdate DATE, \
        l_receiptdate DATE, \
        l_shipinstruct CHAR(25), \
        l_shipmode CHAR(10), \
        l_comment VARCHAR(44), \
        PRIMARY KEY (l_orderkey, l_linenumber))",
];

/// Table names in creation / load order, paired with `SCHEMA`.
const TABLE_NAMES: [&str; 8] = [
    "region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem",
];

// ---------------------------------------------------------------------------
// Generated dataset
// ---------------------------------------------------------------------------

/// One fully-materialized dataset. Rows are generated once, deterministically,
/// then either loaded through a `Session` or written to CSV — so the loaded and
/// the CSV data are provably identical.
struct Dataset {
    /// One `Vec<row>` per table, in `TABLE_NAMES` order.
    tables: Vec<Vec<Vec<Value>>>,
}

fn dec2(cents: i64) -> Value {
    Value::Decimal(Decimal::from_i64_with_scale(cents, 2))
}

/// The four distinct suppliers that supply part `pk` (1-based). A per-part
/// rotation (`(pk-1) mod S`) spread by a constant `S/4` stride keeps `partsupp`
/// and each `lineitem`'s `(partkey, suppkey)` on a real `partsupp` row (Q9/Q20
/// join on it). The `while` guard fills the rare stride collision (when `S` is
/// not a multiple of 4, e.g. at very small scale factors) with the next free
/// supplier, so the four are always distinct for `S >= 4`.
fn part_suppliers(pk: i64, suppliers: i64) -> [i64; 4] {
    assert!(suppliers >= 4, "need at least 4 suppliers per part");
    let stride = (suppliers / 4).max(1);
    let mut out = [0i64; 4];
    for i in 0..4 {
        let mut sk = (pk - 1 + i as i64 * stride) % suppliers + 1;
        while out[..i].contains(&sk) {
            sk = sk % suppliers + 1;
        }
        out[i] = sk;
    }
    out
}

fn phone(rng: &mut Rng, nation_key: i64) -> String {
    let country = nation_key + 10;
    let a = rng.between(100, 999);
    let b = rng.between(100, 999);
    let c = rng.between(1000, 9999);
    format!("{country:02}-{a:03}-{b:03}-{c:04}")
}

/// A short space-joined phrase of `count` filler words (deterministic).
fn phrase(rng: &mut Rng, count: usize) -> String {
    let mut parts = Vec::with_capacity(count);
    for _ in 0..count {
        parts.push(*rng.pick(&WORDS));
    }
    parts.join(" ")
}

/// Generate the whole dataset from the seed.
fn generate(config: &Config) -> Dataset {
    let card = Cardinality::from_scale(config.scale_factor);
    let mut rng = Rng::new(config.seed);

    let region = gen_region(&mut rng);
    let nation = gen_nation(&mut rng);
    let supplier = gen_supplier(&mut rng, card.suppliers);
    let (part, retail_cents) = gen_part(&mut rng, card.parts);
    let partsupp = gen_partsupp(&mut rng, card.parts, card.suppliers);
    let customer = gen_customer(&mut rng, card.customers);
    let (orders, lineitem) = gen_orders_lineitem(&mut rng, &card, &retail_cents);

    Dataset {
        tables: vec![
            region, nation, supplier, part, partsupp, customer, orders, lineitem,
        ],
    }
}

fn gen_region(rng: &mut Rng) -> Vec<Vec<Value>> {
    REGIONS
        .iter()
        .enumerate()
        .map(|(key, name)| {
            vec![
                Value::Int32(key as i32),
                Value::Char(name.to_string()),
                Value::Varchar(phrase(rng, 6)),
            ]
        })
        .collect()
}

fn gen_nation(rng: &mut Rng) -> Vec<Vec<Value>> {
    NATIONS
        .iter()
        .enumerate()
        .map(|(key, (name, region_key))| {
            vec![
                Value::Int32(key as i32),
                Value::Char(name.to_string()),
                Value::Int32(*region_key as i32),
                Value::Varchar(phrase(rng, 6)),
            ]
        })
        .collect()
}

fn gen_supplier(rng: &mut Rng, suppliers: i64) -> Vec<Vec<Value>> {
    (1..=suppliers)
        .map(|key| {
            let nation_key = rng.below(25) as i64;
            // ~6% of suppliers carry a "Customer ... Complaints" comment so
            // Q16's `NOT IN (… LIKE '%Customer%Complaints%')` excludes a real
            // subset; another ~6% carry "Recommends" (spec-shaped, unused).
            let flag = rng.below(100);
            let comment = if flag < 6 {
                format!("{} Customer {} Complaints", phrase(rng, 2), phrase(rng, 1))
            } else if flag < 12 {
                format!("{} Customer {} Recommends", phrase(rng, 2), phrase(rng, 1))
            } else {
                phrase(rng, 6)
            };
            vec![
                Value::Int32(key as i32),
                Value::Char(format!("Supplier#{key:09}")),
                Value::Varchar(phrase(rng, 3)),
                Value::Int32(nation_key as i32),
                Value::Char(phone(rng, nation_key)),
                dec2(rng.between(-99_999, 999_999)),
                Value::Varchar(comment),
            ]
        })
        .collect()
}

/// Returns the `part` rows and, alongside, each part's retail price in cents
/// (indexed by `partkey - 1`) so `lineitem` can derive `l_extendedprice`.
fn gen_part(rng: &mut Rng, parts: i64) -> (Vec<Vec<Value>>, Vec<i64>) {
    let mut rows = Vec::with_capacity(parts as usize);
    let mut retail_cents = Vec::with_capacity(parts as usize);
    for key in 1..=parts {
        // Five distinct colors.
        let mut colors: Vec<&str> = Vec::with_capacity(5);
        while colors.len() < 5 {
            let c = *rng.pick(&COLORS);
            if !colors.contains(&c) {
                colors.push(c);
            }
        }
        let mfgr = rng.between(1, 5);
        let brand_n = rng.between(1, 5);
        let p_type = format!(
            "{} {} {}",
            rng.pick(&TYPE_S1),
            rng.pick(&TYPE_S2),
            rng.pick(&TYPE_S3)
        );
        let container = format!("{} {}", rng.pick(&CONT_S1), rng.pick(&CONT_S2));
        // Spec retail-price formula → ~900.00 .. 2100.00 (in cents).
        let cents = 90_000 + ((key / 10) % 20_001) + 100 * (key % 1_000);
        retail_cents.push(cents);
        rows.push(vec![
            Value::Int32(key as i32),
            Value::Varchar(colors.join(" ")),
            Value::Char(format!("Manufacturer#{mfgr}")),
            Value::Char(format!("Brand#{mfgr}{brand_n}")),
            Value::Varchar(p_type),
            Value::Int32(rng.between(1, 50) as i32),
            Value::Char(container),
            dec2(cents),
            Value::Varchar(phrase(rng, 2)),
        ]);
    }
    (rows, retail_cents)
}

fn gen_partsupp(rng: &mut Rng, parts: i64, suppliers: i64) -> Vec<Vec<Value>> {
    let mut rows = Vec::with_capacity((parts * 4) as usize);
    for pk in 1..=parts {
        for sk in part_suppliers(pk, suppliers) {
            rows.push(vec![
                Value::Int32(pk as i32),
                Value::Int32(sk as i32),
                Value::Int32(rng.between(1, 9_999) as i32),
                dec2(rng.between(100, 100_000)), // 1.00 .. 1000.00
                Value::Varchar(phrase(rng, 8)),
            ]);
        }
    }
    rows
}

fn gen_customer(rng: &mut Rng, customers: i64) -> Vec<Vec<Value>> {
    (1..=customers)
        .map(|key| {
            let nation_key = rng.below(25) as i64;
            vec![
                Value::Int32(key as i32),
                Value::Varchar(format!("Customer#{key:09}")),
                Value::Varchar(phrase(rng, 3)),
                Value::Int32(nation_key as i32),
                Value::Char(phone(rng, nation_key)),
                dec2(rng.between(-99_999, 999_999)),
                Value::Char(rng.pick(&SEGMENTS).to_string()),
                Value::Varchar(phrase(rng, 6)),
            ]
        })
        .collect()
}

// Date anchors (days since epoch), computed once.
fn order_date_span() -> (i32, i32) {
    // Order dates span 1992-01-01 .. 1998-08-02 so the derived ship dates
    // (order + up to 121 days) stay within the spec's 1998-12-01 ceiling.
    (
        civil::days_from_ymd(1992, 1, 1),
        civil::days_from_ymd(1998, 8, 2),
    )
}

/// The linestatus/returnflag cutoff: lines received on or before this are
/// "closed" ('F' status; returnflag 'R'/'A'), later ones are open ('O'/'N').
fn status_cutoff() -> i32 {
    civil::days_from_ymd(1995, 6, 17)
}

/// Generate `orders` and `lineitem` together: each order's total price is the
/// sum of its lines' charge, and its status is derived from its lines'
/// linestatus, so both tables are internally consistent.
fn gen_orders_lineitem(
    rng: &mut Rng,
    card: &Cardinality,
    retail_cents: &[i64],
) -> (Vec<Vec<Value>>, Vec<Vec<Value>>) {
    let (order_lo, order_hi) = order_date_span();
    let cutoff = status_cutoff();
    let mut orders = Vec::with_capacity(card.orders as usize);
    let mut lineitem = Vec::new();

    for order_key in 1..=card.orders {
        // ~1/3 of customers (those whose key is a multiple of 3) never order —
        // Q13/Q22's NOT-EXISTS / count-0 customers depend on this.
        let mut cust = rng.between(1, card.customers);
        if cust % 3 == 0 {
            cust -= 1; // >= 1 since cust >= 3 when divisible by 3
        }
        let order_day = rng.between(order_lo as i64, order_hi as i64) as i32;

        let line_count = rng.between(1, 7);
        let mut total_cents = 0i64;
        let mut all_closed = true;
        let mut all_open = true;

        for line_no in 1..=line_count {
            let part_key = rng.between(1, card.parts);
            let suppliers = part_suppliers(part_key, card.suppliers);
            let supp_key = suppliers[rng.below(4) as usize];
            let qty = rng.between(1, 50);
            let retail = retail_cents[(part_key - 1) as usize];
            let extended_cents = retail * qty;
            let discount = rng.below(11) as i64; // 0.00 .. 0.10
            let tax = rng.below(9) as i64; // 0.00 .. 0.08

            let ship_day = order_day + rng.between(1, 121) as i32;
            let commit_day = order_day + rng.between(30, 90) as i32;
            let receipt_day = ship_day + rng.between(1, 30) as i32;

            // Charge = extended * (1 - discount) * (1 + tax), rounded to cents.
            let charge = (extended_cents * (100 - discount) * (100 + tax) + 5_000) / 10_000;
            total_cents += charge;

            let closed = ship_day <= cutoff;
            all_closed &= closed;
            all_open &= !closed;
            let linestatus = if closed { "F" } else { "O" };
            let returnflag = if receipt_day <= cutoff {
                if rng.below(2) == 0 {
                    "R"
                } else {
                    "A"
                }
            } else {
                "N"
            };

            lineitem.push(vec![
                Value::Int32(order_key as i32),
                Value::Int32(part_key as i32),
                Value::Int32(supp_key as i32),
                Value::Int32(line_no as i32),
                dec2(qty * 100),
                dec2(extended_cents),
                Value::Decimal(Decimal::from_i64_with_scale(discount, 2)),
                Value::Decimal(Decimal::from_i64_with_scale(tax, 2)),
                Value::Char(returnflag.to_string()),
                Value::Char(linestatus.to_string()),
                Value::Date(ship_day),
                Value::Date(commit_day),
                Value::Date(receipt_day),
                Value::Char(rng.pick(&SHIPINSTRUCT).to_string()),
                Value::Char(rng.pick(&SHIPMODES).to_string()),
                Value::Varchar(phrase(rng, 3)),
            ]);
        }

        let status = if all_closed {
            "F"
        } else if all_open {
            "O"
        } else {
            "P"
        };
        // ~45% of orders carry a "special ... requests" comment so Q13's
        // `o_comment NOT LIKE '%special%requests%'` excludes a real subset.
        let comment = if rng.below(100) < 45 {
            format!("special {} requests {}", phrase(rng, 1), phrase(rng, 1))
        } else {
            phrase(rng, 6)
        };

        orders.push(vec![
            Value::Int32(order_key as i32),
            Value::Int32(cust as i32),
            Value::Char(status.to_string()),
            dec2(total_cents),
            Value::Date(order_day),
            Value::Char(rng.pick(&PRIORITIES).to_string()),
            Value::Char(format!("Clerk#{:09}", rng.between(1, 1_000))),
            Value::Int32(0),
            Value::Varchar(comment),
        ]);
    }

    (orders, lineitem)
}

// ---------------------------------------------------------------------------
// Value rendering
// ---------------------------------------------------------------------------

/// `mantissa * 10^-scale` with an explicit decimal point (matches
/// `tests/it/slt.rs`'s `value_text` and DuckDB's `.mode csv` decimal render).
fn decimal_text(d: &Decimal) -> String {
    let digits = d.mantissa().abs().to_string();
    let scale = d.scale() as usize;
    let sign = if d.mantissa() < 0 { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{digits}")
    } else if digits.len() <= scale {
        format!("{sign}0.{digits:0>scale$}")
    } else {
        let (int, frac) = digits.split_at(digits.len() - scale);
        format!("{sign}{int}.{frac}")
    }
}

/// Plain text form of a value for a CSV cell or a query-result cell. NULL is
/// the empty field (DuckDB's `.mode csv` convention).
fn plain_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Varchar(s) | Value::Char(s) => s.clone(),
        Value::Bytes(b) => format!("{b:02x?}"),
        Value::Timestamp(us) => us.to_string(),
        Value::Date(days) => {
            let (y, m, d) = civil::ymd_from_days(*days);
            format!("{y:04}-{m:02}-{d:02}")
        }
        Value::Decimal(d) => decimal_text(d),
    }
}

/// Render one engine *result* value for comparison. Identical to `plain_text`
/// except a typed NULL becomes `NULL_MARKER` instead of the empty string, so
/// the comparator can distinguish an engine NULL from an empty Varchar and
/// never reads a literal "NULL" string as a null (see `classify_actual`).
fn result_text(v: &Value) -> String {
    match v {
        Value::Null => NULL_MARKER.to_string(),
        other => plain_text(other),
    }
}

/// SQL literal form of a value, for building INSERT statements.
fn sql_literal(v: &Value) -> String {
    match v {
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Decimal(d) => decimal_text(d),
        Value::Date(days) => {
            let (y, m, d) = civil::ymd_from_days(*days);
            format!("DATE '{y:04}-{m:02}-{d:02}'")
        }
        Value::Varchar(s) | Value::Char(s) => format!("'{}'", s.replace('\'', "''")),
        other => panic!("no SQL literal for {other:?}"),
    }
}

/// One CSV field, quoted per RFC-4180 only when it contains a comma, quote, or
/// newline. Generated strings avoid all three, so this rarely quotes.
fn csv_field(text: &str) -> String {
    if text.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", text.replace('"', "\"\""))
    } else {
        text.to_string()
    }
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

/// The top-level column-definition segments of a `CREATE TABLE` body — split on
/// commas at paren depth 0, so neither a composite `PRIMARY KEY (...)` clause
/// nor a `DECIMAL(p,s)` inner comma splits a definition. Shared by
/// `column_names` (wants each segment's leading identifier) and `column_types`
/// (wants its type token).
fn column_defs(create_sql: &str) -> Vec<&str> {
    let open = create_sql.find('(').expect("schema has column list");
    let body = &create_sql[open + 1..create_sql.rfind(')').unwrap()];
    let mut defs = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, &b) in body.as_bytes().iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                defs.push(body[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    defs.push(body[start..].trim());
    defs
}

/// Column names per table (header rows), parsed out of `SCHEMA` once.
fn column_names(create_sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    for def in column_defs(create_sql) {
        push_column_name(def, &mut names);
    }
    names
}

/// The declared `ColumnType` of each column, in `column_names` order and
/// cardinality. `leaf_capacity` needs the declared `Char(n)` width because the
/// tuple codec writes a Char as exactly `n` bytes regardless of string length.
fn column_types(create_sql: &str) -> Vec<ColumnType> {
    let mut types = Vec::new();
    for def in column_defs(create_sql) {
        // The composite `PRIMARY KEY (...)` table constraint declares no column.
        let mut tokens = def.split_whitespace();
        let name = tokens.next().unwrap_or("");
        if name.is_empty() || name.eq_ignore_ascii_case("PRIMARY") {
            continue;
        }
        let spec = tokens.next().expect("column definition has a type");
        types.push(parse_column_type(spec));
    }
    types
}

/// If `spec` is `KEYWORD(inner)`, return `inner`; else `None`. Peels the
/// width/precision out of a parameterized type token (e.g. `CHAR(25)` → `25`).
fn paren_args<'a>(spec: &'a str, keyword: &str) -> Option<&'a str> {
    spec.strip_prefix(keyword)?
        .strip_prefix('(')?
        .strip_suffix(')')
}

/// Parse one SQL type token exactly as `SCHEMA` writes it (`INT`, `DATE`,
/// `CHAR(n)`, `VARCHAR(n)`, `DECIMAL(p,s)`) into a `ColumnType`. A fixture
/// parser for our own schema constant — not a general SQL type parser.
fn parse_column_type(spec: &str) -> ColumnType {
    let upper = spec.to_ascii_uppercase();
    if upper == "INT" {
        ColumnType::Int32
    } else if upper == "DATE" {
        ColumnType::Date
    } else if let Some(inner) = paren_args(&upper, "CHAR") {
        ColumnType::Char(inner.parse().expect("Char(n) width"))
    } else if let Some(inner) = paren_args(&upper, "VARCHAR") {
        ColumnType::Varchar(inner.parse().expect("Varchar(n) width"))
    } else if let Some(inner) = paren_args(&upper, "DECIMAL") {
        let (precision, scale) = inner.split_once(',').expect("DECIMAL(p,s)");
        ColumnType::Decimal {
            precision: precision.trim().parse().expect("DECIMAL precision"),
            scale: scale.trim().parse().expect("DECIMAL scale"),
        }
    } else {
        panic!("unsupported schema column type: {spec}");
    }
}

/// Take the first token of a column definition as its name, skipping the
/// `PRIMARY KEY (...)` table constraint.
fn push_column_name(segment: &str, names: &mut Vec<String>) {
    let token = segment.split_whitespace().next().unwrap_or("");
    if token.eq_ignore_ascii_case("PRIMARY") {
        return;
    }
    if !token.is_empty() {
        names.push(token.to_string());
    }
}

fn write_csv(dataset: &Dataset, dir: &Path) {
    std::fs::create_dir_all(dir).expect("create csv dir");
    // The schema file is the oracle's single source of truth for DuckDB.
    std::fs::write(dir.join("schema.sql"), SCHEMA.join(";\n") + ";\n").expect("write schema.sql");

    for (idx, name) in TABLE_NAMES.iter().enumerate() {
        let header = column_names(SCHEMA[idx]).join(",");
        let mut out = String::with_capacity(1 << 20);
        out.push_str(&header);
        out.push('\n');
        for row in &dataset.tables[idx] {
            let mut first = true;
            for cell in row {
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str(&csv_field(&plain_text(cell)));
            }
            out.push('\n');
        }
        std::fs::write(dir.join(format!("{name}.csv")), out).expect("write table csv");
    }
    eprintln!(
        "wrote {} tables + schema.sql to {}",
        TABLE_NAMES.len(),
        dir.display()
    );
}

// ---------------------------------------------------------------------------
// Session stack + loading
// ---------------------------------------------------------------------------

struct Db {
    session: Session<BTreeEngine>,
    /// Shared handles for the guarded query runner: each guarded query builds
    /// its own `Session` from these on a worker thread (the tpcc pattern), so
    /// a wall-clock overrun can be reported without hanging the driver.
    database: Arc<Database<BTreeEngine>>,
    catalog: Arc<Catalog<BTreeEngine>>,
    dir: PathBuf,
}

impl Drop for Db {
    fn drop(&mut self) {
        // Best-effort cleanup of the WAL scratch directory.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A byte upper bound on one column value as the tuple codec *actually* encodes
/// it, used to pick a byte-safe leaf fan-out.
///
/// Bounds are read off the codec's `encode` (crates/idb-core/src/types/tuple.rs):
///   - `Int32` / `Date`          → 4 bytes  (bounded here by 9)
///   - `Int64` / `Timestamp`     → 8 bytes  (bounded here by 9)
///   - `Decimal` (mantissa only) → 8 bytes  (bounded here by 12)
///   - `Boolean`                 → 1 byte
///   - `Char(n)`                 → EXACTLY `n` bytes, right-padded — so the
///     bound is the DECLARED width `n`, NOT `s.len()`
///   - `Varchar(n)` / `Bytes(n)` → `u16` length prefix (2) + payload; +1 slack
///
/// The `Char` arm is the honest fix: the old code sized both Char and Varchar as
/// `s.len() + 3`, which UNDER-counts a short string in a wide Char column (e.g.
/// "NONE" in `l_shipinstruct CHAR(25)` encodes to 25 bytes, not 7). That stayed
/// byte-safe only by accident of the fixed-width over-estimates above — an
/// accident, not a bound. Sizing Char by its declared width makes it a real one.
fn cell_upper_bytes(ty: &ColumnType, v: &Value) -> usize {
    match ty {
        ColumnType::Char(n) => *n as usize,
        ColumnType::Varchar(_) | ColumnType::Bytes(_) => match v {
            Value::Varchar(s) => s.len() + 3,
            Value::Bytes(b) => b.len() + 3,
            other => unreachable!("varlen column holds a non-varlen value: {other:?}"),
        },
        ColumnType::Int32 | ColumnType::Date => 9,
        ColumnType::Int64 | ColumnType::Timestamp => 9,
        ColumnType::Decimal { .. } => 12,
        ColumnType::Boolean => 1,
    }
}

/// Choose a leaf-node entry count that keeps a full leaf within the 4 KB page.
///
/// `BTreeEngine::new` sizes its count-based leaf split for 64-byte values;
/// TPC-H rows (lineitem ≈ 150 bytes encoded) overflow the page before the count
/// limit is reached — a genuine engine limitation (byte-unaware leaf split).
/// The harness works around it via `BTreeEngine::with_sizes`, deriving the
/// count from the widest generated row so a packed leaf always fits.
fn leaf_capacity(dataset: &Dataset) -> u16 {
    // Declared column types per table, in `dataset.tables` order — so the Char
    // bound below is the codec's exact width, not the stored string length.
    let schemas: Vec<Vec<ColumnType>> = SCHEMA.iter().map(|&s| column_types(s)).collect();
    let mut max_entry = 0usize;
    for (table, types) in dataset.tables.iter().zip(&schemas) {
        for row in table {
            let bytes: usize = row
                .iter()
                .zip(types)
                .map(|(v, ty)| cell_upper_bytes(ty, v))
                .sum();
            max_entry = max_entry.max(bytes);
        }
    }
    // + key (table_id 4 + composite PK, small) + per-entry framing.
    let per_entry = max_entry + 32;
    // Leave the leaf header + tombstone headroom out of the usable page.
    let available = 4096 - 64;
    (available / per_entry).clamp(4, 128) as u16
}

fn open_db(leaf_max_size: u16) -> Db {
    // A unique scratch dir under the system temp for the WAL (`NoSync`, so no
    // fsync cost). The page store itself is in-memory (`MemoryDiskManager`).
    let unique = format!(
        "interchangedb_tpch_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let dm = MemoryDiskManager::new();
    // A generous pool caches the whole SF-0.01 working set (~2k pages) in
    // memory; correctness does not depend on the size.
    let bpm = BufferPoolManager::new(16_384, dm);
    // Internal fan-out 64 is byte-safe for the small integer PK separator keys.
    let engine = BTreeEngine::with_sizes(bpm, leaf_max_size, 64, 0).expect("engine");
    let database =
        Arc::new(Database::open_with_sync_mode(&dir, engine, SyncMode::NoSync).expect("open"));
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).expect("catalog"));
    Db {
        session: Session::new(database.clone(), catalog.clone()),
        database,
        catalog,
        dir,
    }
}

/// Rows per multi-row INSERT — bounds each statement's size while amortizing
/// parse/bind over many rows.
const LOAD_BATCH: usize = 500;

fn load(session: &mut Session<BTreeEngine>, dataset: &Dataset) {
    for sql in SCHEMA {
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("schema failed [{sql}]: {e}"));
    }
    for (idx, name) in TABLE_NAMES.iter().enumerate() {
        let rows = &dataset.tables[idx];
        session.execute("BEGIN").expect("begin load txn");
        for chunk in rows.chunks(LOAD_BATCH) {
            let mut sql = format!("INSERT INTO {name} VALUES ");
            for (i, row) in chunk.iter().enumerate() {
                if i > 0 {
                    sql.push(',');
                }
                sql.push('(');
                for (j, cell) in row.iter().enumerate() {
                    if j > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&sql_literal(cell));
                }
                sql.push(')');
            }
            session
                .execute(&sql)
                .unwrap_or_else(|e| panic!("insert into {name} failed: {e}"));
        }
        session.execute("COMMIT").expect("commit load txn");
    }
}

// ---------------------------------------------------------------------------
// Query loading + execution
// ---------------------------------------------------------------------------

/// The 22 query numbers, in order.
const QUERY_IDS: [u8; 22] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
];

fn query_path(dir: &Path, id: u8) -> PathBuf {
    dir.join(format!("q{id:02}.sql"))
}

/// Read a query file and strip `--` line comments and the trailing semicolon,
/// leaving one statement `Session::execute` accepts.
fn load_query(dir: &Path, id: u8) -> String {
    let raw = std::fs::read_to_string(query_path(dir, id))
        .unwrap_or_else(|e| panic!("read q{id:02}.sql: {e}"));
    let mut sql = String::with_capacity(raw.len());
    for line in raw.lines() {
        let code = match line.find("--") {
            Some(pos) => &line[..pos],
            None => line,
        };
        sql.push_str(code);
        sql.push('\n');
    }
    sql.trim().trim_end_matches(';').trim().to_string()
}

/// Run one query with a wall-clock guard. The query executes on its own worker
/// thread (fresh `Session` over the shared database/catalog Arcs — same MVCC
/// data, statement-scoped snapshot); the driver waits `timeout_secs` on a
/// channel. On overrun the cell fails loudly with a FEASIBILITY marker and the
/// driver moves on. NOTE: the overrunning worker thread cannot be preempted —
/// it is detached and keeps burning a core until its query finishes; acceptable
/// for a diagnosis harness (after the FROM-order fixes no cell should trip).
fn run_query_guarded(
    db: &Db,
    planner: &'static str,
    exec_model: &'static str,
    sql: &str,
    timeout_secs: u64,
) -> Result<Vec<Vec<String>>, String> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let database = db.database.clone();
    let catalog = db.catalog.clone();
    let sql = sql.to_string();
    std::thread::spawn(move || {
        let mut session = Session::new(database, catalog);
        set_config(&mut session, planner, exec_model);
        let outcome = match session.execute(&sql) {
            Ok(QueryResult::Rows { rows, .. }) => Ok(rows
                .iter()
                .map(|r| r.iter().map(result_text).collect())
                .collect()),
            Ok(other) => Err(format!("expected Rows, got {other:?}")),
            Err(e) => Err(format!("query error: {e}")),
        };
        // A send failure means the driver already timed out and dropped the
        // receiver — nothing left to report to.
        let _ = sender.send(outcome);
    });
    match receiver.recv_timeout(std::time::Duration::from_secs(timeout_secs)) {
        Ok(outcome) => outcome,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "FEASIBILITY: exceeded {timeout_secs}s wall clock (plan-shape grind?) — \
             rerun alone via --queries"
        )),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err("query worker thread panicked".to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// Normalization + comparison (TPC-H §2 answer tolerance)
// ---------------------------------------------------------------------------

/// A normalized cell. Integers compare exactly; decimals/floats within TPC-H's
/// 0.01 absolute tolerance; everything else (strings, dates) exactly; NULL is
/// its own token. The two sides interpret NULL differently, so classification
/// is side-aware (`classify_expected` vs `classify_actual`).
#[derive(Debug, Clone)]
enum Cell {
    Null,
    Int(i64),
    Num(f64),
    Text(String),
}

/// The token the actual (engine) side renders a typed NULL to, so the
/// comparator can tell an engine NULL apart from a literal string "NULL" or an
/// empty string. The expected (oracle CSV) side never uses it — that side keeps
/// the CSV null convention (empty field, or a literal `NULL`). A NUL byte can't
/// occur in any generated value (the tuple codec's Char encoder rejects
/// embedded NULs), so this marker can never collide with real result text.
const NULL_MARKER: &str = "\0NULL\0";

/// Classify a known-non-null cell's text: integer-shaped → `Int`, else
/// numeric → `Num`, else `Text`. Shared by both sides' classifiers.
fn classify_value(t: &str) -> Cell {
    // Integer-shaped: an optional sign then only digits.
    let is_int = {
        let digits = t.strip_prefix('-').unwrap_or(t);
        !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
    };
    if is_int {
        if let Ok(i) = t.parse::<i64>() {
            return Cell::Int(i);
        }
    }
    if let Ok(f) = t.parse::<f64>() {
        return Cell::Num(f);
    }
    Cell::Text(t.to_string())
}

/// Classify an EXPECTED (oracle CSV) cell. DuckDB's `.mode csv` renders NULL as
/// an empty field; some oracle setups emit the literal token `NULL`. Both mean
/// NULL — but this null convention is DuckDB's, so it is honored ONLY here.
fn classify_expected(raw: &str) -> Cell {
    let t = raw.trim();
    if t.is_empty() || t.eq_ignore_ascii_case("NULL") {
        return Cell::Null;
    }
    classify_value(t)
}

/// Classify an ACTUAL (engine result) cell. A typed NULL arrives as
/// `NULL_MARKER`; every other string is literal — in particular a `Varchar`
/// whose contents spell "NULL" is text, not a null. This is the finding-4 scope
/// fix: the CSV null convention belongs to the oracle side, not ours, so the
/// engine side interprets its own typed NULLs and nothing else.
fn classify_actual(raw: &str) -> Cell {
    if raw == NULL_MARKER {
        return Cell::Null;
    }
    classify_value(raw.trim())
}

/// The spec's answer tolerance (§2.1.3.5): a computed value matches the
/// reference if it is within 0.01 absolute. Integers must match exactly; a
/// value rendered as an integer on one side and a decimal on the other (e.g.
/// `1300` vs `1300.00`) is compared numerically under the same tolerance.
const TOLERANCE: f64 = 0.01;

fn cells_match(expected: &Cell, actual: &Cell) -> bool {
    match (expected, actual) {
        (Cell::Null, Cell::Null) => true,
        (Cell::Int(a), Cell::Int(b)) => a == b,
        (Cell::Int(a), Cell::Num(b)) | (Cell::Num(b), Cell::Int(a)) => {
            (*a as f64 - b).abs() <= TOLERANCE
        }
        (Cell::Num(a), Cell::Num(b)) => (a - b).abs() <= TOLERANCE,
        (Cell::Text(a), Cell::Text(b)) => a == b,
        _ => false,
    }
}

fn rows_match(expected: &[Cell], actual: &[Cell]) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual.iter())
            .all(|(e, a)| cells_match(e, a))
}

/// Whether one actual cell could tolerantly match BOTH of these cells at once.
/// For numerics that means their tolerance windows overlap — the gap is within
/// `2 × TOLERANCE`, so a value in the middle matches each. For exact cells
/// (int/text/null) the single actual would have to equal both, i.e. they are
/// equal. Integers can never be within `2 × TOLERANCE` without being equal (the
/// gap is 0 or ≥ 1), so integer sort keys are inherently safe.
fn cells_confusable(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (Cell::Null, Cell::Null) => true,
        (Cell::Int(x), Cell::Int(y)) => x == y,
        (Cell::Text(x), Cell::Text(y)) => x == y,
        (Cell::Int(x), Cell::Num(y)) | (Cell::Num(y), Cell::Int(x)) => {
            (*x as f64 - y).abs() <= 2.0 * TOLERANCE
        }
        (Cell::Num(x), Cell::Num(y)) => (x - y).abs() <= 2.0 * TOLERANCE,
        _ => false,
    }
}

/// The first pair `(i, j)` of expected rows that are mutually confusable on
/// EVERY compared column, or `None` if the fixture is safe. This is the
/// invariant greedy multiset matching depends on (see `compare`): if such a
/// pair exists, one actual row could match both and first-fit could consume the
/// wrong one. `O(rows² · cols)`, run only on the small multiset fixtures.
fn confusable_expected_pair(exp: &[Vec<Cell>]) -> Option<(usize, usize)> {
    for i in 0..exp.len() {
        for j in (i + 1)..exp.len() {
            if exp[i].len() == exp[j].len()
                && exp[i]
                    .iter()
                    .zip(&exp[j])
                    .all(|(a, b)| cells_confusable(a, b))
            {
                return Some((i, j));
            }
        }
    }
    None
}

fn normalize(rows: &[Vec<String>], classify: fn(&str) -> Cell) -> Vec<Vec<Cell>> {
    rows.iter()
        .map(|r| r.iter().map(|c| classify(c)).collect())
        .collect()
}

/// Whether a query's output is compared as an ordered sequence (order verified
/// too) or as a multiset (set-equality).
///
/// Rule: multiset ONLY where the committed fixture *provably* ties on the
/// query's ORDER BY key — i.e. two output rows share the full sort key, so
/// their relative order is a legitimate engine-to-engine coin flip. Everywhere
/// else, ordered compare is available and strictly STRONGER (it also catches a
/// wrong ORDER BY *direction*, which a multiset compare silently passes), so it
/// is the default.
///
/// Q3, Q5, Q10, Q11 order only by a revenue/value aggregate with no unique-key
/// tiebreak, so ties are *possible in principle* — but their committed expected
/// CSVs have all-distinct sort keys (verified), so the total order is real and
/// ordered compare is both sound and stronger. Only Q18 is kept multiset: it
/// orders by `o_totalprice DESC, o_orderdate` with no unique tiebreak and is
/// structurally tie-possible (two orders can share price and date), so a future
/// regenerate could legitimately reorder equal rows.
fn is_ordered(id: u8) -> bool {
    id != 18
}

/// Compare one query's output against the oracle. Returns `Ok(())` on a match,
/// or `Err(diff excerpt)`.
fn compare(id: u8, expected: &[Vec<String>], actual: &[Vec<String>]) -> Result<(), String> {
    let exp = normalize(expected, classify_expected);
    let act = normalize(actual, classify_actual);

    if exp.len() != act.len() {
        return Err(format!(
            "row count: expected {}, got {}\n  first expected: {:?}\n  first actual:   {:?}",
            exp.len(),
            act.len(),
            exp.first(),
            act.first()
        ));
    }

    if is_ordered(id) {
        for (i, (e, a)) in exp.iter().zip(act.iter()).enumerate() {
            if !rows_match(e, a) {
                return Err(format!(
                    "row {i} differs (ordered compare)\n  expected: {e:?}\n  actual:   {a:?}"
                ));
            }
        }
        Ok(())
    } else {
        // Multiset via greedy first-fit. This is sound only while tolerant match
        // acts like an EQUIVALENCE relation over these rows — i.e. no single
        // actual row could match two different expected rows. Tolerant match is
        // NOT transitive (overlapping tolerance windows break it), so the
        // guarantee must come from the fixture: no two expected rows may sit
        // within 2× tolerance on EVERY compared column. If two did, an actual
        // row between them could match both and first-fit might consume the
        // wrong one, turning a real mismatch into a false pass. Assert that
        // invariant loudly (a validation error, not a panic): a violation means
        // this query must move to exact-match diagnostics — it is not a verdict
        // on the engine.
        if let Some((i, j)) = confusable_expected_pair(&exp) {
            return Err(format!(
                "fixture invariant violated for multiset compare: expected rows {i} and {j} \
                 are within 2× tolerance on every column, so greedy matching is unsound — \
                 switch Q{id:02} to exact-match diagnostics\n  row {i}: {:?}\n  row {j}: {:?}",
                exp[i], exp[j]
            ));
        }
        // Greedily match each expected row to an unused actual row.
        let mut used = vec![false; act.len()];
        for e in &exp {
            let hit = act
                .iter()
                .enumerate()
                .position(|(j, a)| !used[j] && rows_match(e, a));
            match hit {
                Some(j) => used[j] = true,
                None => return Err(format!("unmatched expected row (multiset compare): {e:?}")),
            }
        }
        Ok(())
    }
}

/// Minimal RFC-4180 line reader: splits one CSV record into fields, honoring
/// double-quoted fields (with `""` escapes). One record per line is sufficient
/// — the generator never emits embedded newlines.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' => in_quotes = true,
            ',' if !in_quotes => {
                fields.push(std::mem::take(&mut field));
            }
            _ => field.push(c),
        }
    }
    fields.push(field);
    fields
}

/// Read an expected-output CSV, dropping the header row. Trailing `\r` (CRLF)
/// is stripped.
fn read_expected(dir: &Path, id: u8) -> Vec<Vec<String>> {
    let path = dir.join(format!("q{id:02}.csv"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read expected {}: {e}", path.display()));
    text.lines()
        .skip(1) // header
        .filter(|l| !l.is_empty())
        .map(|l| parse_csv_line(l.trim_end_matches('\r')))
        .collect()
}

// ---------------------------------------------------------------------------
// Planner / execution-model matrix
// ---------------------------------------------------------------------------

const PLANNERS: [&str; 3] = ["rule-based", "selinger", "memo"];
const EXEC_MODELS: [&str; 2] = ["volcano", "push"];

fn set_config(session: &mut Session<BTreeEngine>, planner: &str, exec_model: &str) {
    session.set_planner(match planner {
        "rule-based" => Planner::RuleBased(RuleBasedPlanner),
        "selinger" => Planner::Selinger(SelingerPlanner::default()),
        "memo" => Planner::VolcanoMemo(VolcanoPlanner::default()),
        other => panic!("unknown planner {other}"),
    });
    session.set_execution_model(match exec_model {
        "volcano" => ExecModel::Volcano,
        "push" => ExecModel::Push,
        other => panic!("unknown exec-model {other}"),
    });
}

// ---------------------------------------------------------------------------
// Validate + time
// ---------------------------------------------------------------------------

/// One validation-matrix cell outcome.
#[derive(Clone, Copy, PartialEq)]
enum CellState {
    Pass,
    Fail,
    Timeout,
}

impl CellState {
    fn label(self) -> &'static str {
        match self {
            CellState::Pass => "PASS",
            CellState::Fail => "FAIL",
            CellState::Timeout => "TIMEOUT",
        }
    }
}

/// Run the (selected queries) × 6 validation matrix. Returns the number of
/// failing cells. One progress line per cell as it completes, so a grinding
/// cell is diagnosable from the streamed output.
fn validate(config: &Config) -> usize {
    let dataset = generate(config);
    let mut db = open_db(leaf_capacity(&dataset));
    let load_start = Instant::now();
    load(&mut db.session, &dataset);
    println!(
        "loaded SF {} in {:.2}s (seed {})",
        config.scale_factor,
        load_start.elapsed().as_secs_f64(),
        config.seed
    );

    let queries: Vec<(u8, String)> = config
        .queries
        .iter()
        .map(|&id| (id, load_query(&config.queries_dir, id)))
        .collect();
    let expected: Vec<(u8, Vec<Vec<String>>)> = config
        .queries
        .iter()
        .map(|&id| (id, read_expected(&config.expected_dir, id)))
        .collect();

    // Per-query × per-config outcome matrix. `matrix[q][config]`.
    let mut matrix = vec![vec![CellState::Pass; config.configs.len()]; queries.len()];
    let mut failures = 0usize;

    for (col, (planner, exec_model)) in config.configs.iter().enumerate() {
        for (qi, (id, sql)) in queries.iter().enumerate() {
            let start = Instant::now();
            let outcome = run_query_guarded(&db, planner, exec_model, sql, config.timeout_secs);
            let elapsed = start.elapsed().as_secs_f64();
            match outcome {
                Ok(actual) => {
                    let rows = actual.len();
                    match compare(*id, &expected[qi].1, &actual) {
                        Ok(()) => {
                            matrix[qi][col] = CellState::Pass;
                            println!(
                                "done Q{:02} [{planner}/{exec_model}] {elapsed:>7.1}s {rows:>6} rows  ok",
                                id
                            );
                        }
                        Err(diff) => {
                            matrix[qi][col] = CellState::Fail;
                            failures += 1;
                            println!(
                                "done Q{:02} [{planner}/{exec_model}] {elapsed:>7.1}s {rows:>6} rows  MISMATCH\n  {}",
                                id,
                                diff.replace('\n', "\n  ")
                            );
                        }
                    }
                }
                Err(reason) => {
                    let timed_out = reason.starts_with("FEASIBILITY");
                    matrix[qi][col] = if timed_out {
                        CellState::Timeout
                    } else {
                        CellState::Fail
                    };
                    failures += 1;
                    println!(
                        "done Q{:02} [{planner}/{exec_model}] {elapsed:>7.1}s   FAIL: {reason}",
                        id
                    );
                }
            }
        }
    }

    print_matrix(&expected, &matrix, &config.configs);
    failures
}

/// The six planner × execution-model configurations, column order fixed.
fn configs() -> Vec<(&'static str, &'static str)> {
    let mut out = Vec::with_capacity(6);
    for planner in PLANNERS {
        for exec_model in EXEC_MODELS {
            out.push((planner, exec_model));
        }
    }
    out
}

fn print_matrix(
    expected: &[(u8, Vec<Vec<String>>)],
    matrix: &[Vec<CellState>],
    cols: &[(&'static str, &'static str)],
) {
    println!(
        "\n=== TPC-H validation matrix ({} queries × {} configs) ===",
        expected.len(),
        cols.len()
    );
    print!("{:<7}{:>6}", "query", "rows");
    for (planner, exec_model) in cols {
        print!(" {:>14}", format!("{}/{}", planner, exec_model));
    }
    println!();
    for (qi, (id, rows)) in expected.iter().enumerate() {
        print!("Q{:<6}{:>6}", id, rows.len());
        for col in 0..cols.len() {
            print!(" {:>14}", matrix[qi][col].label());
        }
        let mode = if is_ordered(*id) {
            "ordered"
        } else {
            "multiset"
        };
        println!("   [{mode}]");
    }
}

/// Informational timing table (rule-based / volcano only). Not a gate; a
/// query that trips the wall-clock guard prints a FEASIBILITY line and the
/// sweep continues.
fn time_queries(config: &Config) {
    let dataset = generate(config);
    let mut db = open_db(leaf_capacity(&dataset));
    load(&mut db.session, &dataset);

    println!(
        "\n=== TPC-H query timing (rule-based / volcano, SF {}) ===",
        config.scale_factor
    );
    println!("{:<7}{:>10}{:>8}", "query", "ms", "rows");
    for &id in config.queries.iter() {
        let sql = load_query(&config.queries_dir, id);
        let start = Instant::now();
        match run_query_guarded(&db, "rule-based", "volcano", &sql, config.timeout_secs) {
            Ok(rows) => println!(
                "Q{:<6}{:>10.1}{:>8}",
                id,
                start.elapsed().as_secs_f64() * 1000.0,
                rows.len()
            ),
            Err(reason) => println!("Q{:<6}{:>10}{:>8}   {reason}", id, "-", "-"),
        }
    }
}

fn main() {
    let config = Config::from_args();

    if let Some(dir) = &config.csv_out {
        write_csv(&generate(&config), dir);
        return;
    }

    if config.validate {
        let failures = validate(&config);
        if config.time {
            time_queries(&config);
        }
        if failures > 0 {
            eprintln!("\nVALIDATION FAILED: {failures} query/config mismatch(es)");
            std::process::exit(1);
        }
        println!(
            "\nVALIDATION PASSED: {} queries × {} configs match the oracle",
            config.queries.len(),
            config.configs.len()
        );
        return;
    }

    if config.time {
        time_queries(&config);
        return;
    }

    // Default: generate + load, prove the dataset builds and loads clean.
    let dataset = generate(&config);
    let mut db = open_db(leaf_capacity(&dataset));
    let start = Instant::now();
    load(&mut db.session, &dataset);
    println!(
        "generated + loaded SF {} in {:.2}s (seed {})",
        config.scale_factor,
        start.elapsed().as_secs_f64(),
        config.seed
    );
}
