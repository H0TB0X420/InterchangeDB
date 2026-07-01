//! TPC-C harness (Phase 16).
//!
//! A standalone driver that loads a TPC-C-shaped dataset and runs the
//! five transactions (NewOrder, Payment, OrderStatus, Delivery,
//! StockLevel) in their standard weighted mix through the SQL surface.
//!
//! ## Stages
//!
//!   - **Smoke** (`--terminals 1`): one warehouse, one thread, fixed
//!     iterations — proves the full mix runs end-to-end, zero errors.
//!   - **Baseline** (`--terminals N --warehouses M`): N concurrent
//!     terminals, each pinned to a home warehouse, producing a measured
//!     throughput / tpmC figure.
//!
//! ## Interchangeable storage engine
//!
//! `--engine btree|lsm` runs the *same* workload through either
//! `StorageEngine` impl — the whole driver is generic over `E`. The B+Tree
//! engine routes every access through the `BufferPoolManager`; the LSM
//! engine (memtable + SSTables) bypasses it entirely. Comparing the two
//! under TPC-C is a direct test both of the interchangeability thesis and
//! of whether the buffer pool's global latches are the scaling ceiling.
//!
//! ## Concurrency model
//!
//! Each terminal is a thread with its own `Session` over the shared
//! `Arc<Database>`. Terminal `t` is pinned to home warehouse
//! `(t % warehouses) + 1` and mints unique PKs in a disjoint id range
//! (and its Delivery drains only that range), so with
//! `warehouses >= terminals` no two terminals share writable rows. Each
//! transaction is one `BEGIN..COMMIT`; a write conflict rolls back and is
//! counted as an abort (no retry — the abort rate is itself a signal).
//!
//! Schemas mirror `tests/tpcc_transactions_test.rs` (simplified
//! single-column synthetic PKs).
//!
//! Run: `cargo run --release --bin tpcc -- --engine lsm --warehouses 8 --terminals 8 --duration-secs 30`

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use interchangedb::buffer::replacer::{
    ArcReplacer, ClockReplacer, EvictionPolicy, FifoReplacer, LruKReplacer, LruReplacer,
    TwoQReplacer,
};
use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::catalog::Catalog;
use interchangedb::common::Result as DbResult;
use interchangedb::database::Database;
use interchangedb::execution::ExecModel;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::session::{PreparedStatement, QueryResult, Session};
use interchangedb::sql::{Planner, RuleBasedPlanner, SelingerPlanner};
use interchangedb::storage::FileDiskManager;
use interchangedb::types::Value;
use interchangedb::{LsmEngine, StorageEngine};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Config {
    engine: String,
    /// Buffer-pool eviction policy (B+Tree engine only): fifo|lru|lruk|clock|2q|arc.
    policy: String,
    /// Buffer-pool frame count (B+Tree engine only). Shrink below the
    /// working set to create memory pressure where the policy matters.
    pool_size: usize,
    warehouses: i32,
    terminals: i32,
    iterations: u64,
    duration_secs: u64,
    seed: u64,
    data_dir: PathBuf,
    /// Diagnostic mode: run only read transactions (OrderStatus) — no
    /// write locks, no WAL commits. Lets us see whether the read path
    /// alone scales, isolating read- vs write-path bottlenecks.
    read_only: bool,
    /// Execution model for the worker sessions: volcano (pull) | push.
    exec_model: String,
    /// Planner strategy for the worker sessions: rule-based | selinger.
    planner: String,
    /// Sweep mode: ignore --engine/--exec-model/--planner and run the full
    /// engine × exec-model × planner matrix, printing a comparison table.
    sweep: bool,
    /// PK-lookup probe mode (read-only): run only a single-row `WHERE c_id = $1`
    /// point lookup on the large customer table — the access the PK-lookup
    /// lever optimizes. Isolates that lever (PkLookup vs SeqScan+Filter), which
    /// the OrderStatus read-only mode can't (its reads are non-PK).
    pk_probe: bool,
    /// Table cardinality preset (`--scale smoke|big`). Default `smoke`.
    cardinality: Cardinality,
}

impl Config {
    fn from_args() -> Config {
        let mut c = Config {
            engine: "btree".to_string(),
            policy: "arc".to_string(),
            pool_size: POOL_SIZE,
            warehouses: 1,
            terminals: 1,
            iterations: 2_000,
            duration_secs: 0,
            seed: 1,
            data_dir: std::env::temp_dir().join("interchangedb_tpcc"),
            read_only: false,
            exec_model: "volcano".to_string(),
            planner: "rule-based".to_string(),
            sweep: false,
            pk_probe: false,
            cardinality: SMOKE,
        };
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            let mut value = || args.next().expect("flag needs a value");
            match flag.as_str() {
                "--engine" => c.engine = value(),
                "--policy" => c.policy = value(),
                "--pool-size" => c.pool_size = value().parse().expect("int"),
                "--warehouses" => c.warehouses = value().parse().expect("int"),
                "--terminals" => c.terminals = value().parse().expect("int"),
                "--iterations" => c.iterations = value().parse().expect("int"),
                "--duration-secs" => c.duration_secs = value().parse().expect("int"),
                "--seed" => c.seed = value().parse().expect("int"),
                "--data-dir" => c.data_dir = PathBuf::from(value()),
                "--read-only" => c.read_only = true,
                "--exec-model" => c.exec_model = value(),
                "--planner" => c.planner = value(),
                "--sweep" => c.sweep = true,
                "--pk-probe" => c.pk_probe = true,
                "--scale" => c.cardinality = parse_scale(&value()),
                other => panic!("unknown flag: {}", other),
            }
        }
        assert!(c.warehouses >= 1 && c.terminals >= 1);
        // The loader numbers seed orders [SEED_ORDER_BASE, SEED_ORDER_BASE +
        // warehouses*districts*orders); that span must not reach PK_BASE, or
        // seed orders would collide with run-time-minted order PKs.
        let seed_order_span = c.warehouses as i64
            * c.cardinality.districts_per_w as i64
            * c.cardinality.initial_orders_per_d as i64;
        assert!(
            SEED_ORDER_BASE as i64 + seed_order_span < PK_BASE as i64,
            "seed orders ({} warehouses) overflow into the minted-PK range",
            c.warehouses
        );
        c
    }
}

/// Per-warehouse table cardinalities, chosen at runtime via `--scale`.
#[derive(Clone, Copy)]
struct Cardinality {
    districts_per_w: i32,
    customers_per_d: i32,
    items: i32,
    initial_orders_per_d: i32,
}

// `smoke` keeps the read-path tables tiny — the realistic fsync-bound full-mix
// regime (and the historical tpmC baseline). `big` grows customer/orders/
// order_line to thousands of rows so the read-path levers (PK lookup, lazy MVCC
// scan) cost enough to measure (pair with `--read-only` / `--pk-probe`). Both
// stay far below TPC-C spec scale (3000 customers/district, 100K items). NOTE:
// the full mix is scan/join-bound at `big` (StockLevel's nested-loop join over
// order_line × stock is super-linear), so high-concurrency full-mix runs there
// are slow — `big` is for the read-path diagnostics, `smoke` for tpmC.
const SMOKE: Cardinality = Cardinality {
    districts_per_w: 2,
    customers_per_d: 10,
    items: 20,
    initial_orders_per_d: 5,
};
const BIG: Cardinality = Cardinality {
    districts_per_w: 10,
    customers_per_d: 100,
    items: 100,
    initial_orders_per_d: 100,
};

// Id-encoding invariants (see `district_id`/`customer_id`/`stock_id`): the
// digit budgets the synthetic-PK packing depends on. Checked on the larger
// preset at compile time — a scale-up that violates one silently collides PKs.
const _: () = assert!(BIG.districts_per_w < 100, "district_id packs d in 2 digits");
const _: () = assert!(
    BIG.customers_per_d < 1000,
    "customer_id packs c in 3 digits"
);
const _: () = assert!(BIG.items < 100_000, "stock_id packs item in 5 digits");

/// Map the `--scale` flag to a cardinality preset.
fn parse_scale(name: &str) -> Cardinality {
    match name {
        "smoke" => SMOKE,
        "big" => BIG,
        other => panic!("unknown scale: {} (smoke|big)", other),
    }
}

// Per-terminal unique-PK range: terminal t owns `[BASE + t*STRIDE, …)`.
const PK_BASE: i32 = 2_000_000;
const PK_STRIDE: i32 = 10_000_000;

// Loader seed orders number from 1_000_000 upward; they must stay below
// PK_BASE so they never collide with the PKs terminals mint at run time.
const SEED_ORDER_BASE: i32 = 1_000_000;

// Buffer pool frames (B+Tree engine).
const POOL_SIZE: usize = 8192;

/// Build the requested eviction policy. `capacity` is the pool size for
/// the size-parameterized policies. (LSM ignores this — no buffer pool.)
fn make_policy(name: &str, capacity: usize) -> Box<dyn EvictionPolicy> {
    match name {
        "fifo" => Box::new(FifoReplacer::new()),
        "lru" => Box::new(LruReplacer::new(capacity)),
        "lruk" => Box::new(LruKReplacer::new(2)),
        "clock" => Box::new(ClockReplacer::new()),
        "2q" => Box::new(TwoQReplacer::new(capacity)),
        "arc" => Box::new(ArcReplacer::new(capacity)),
        other => panic!("unknown policy: {} (fifo|lru|lruk|clock|2q|arc)", other),
    }
}

// ---------------------------------------------------------------------------
// Deterministic RNG (LCG) — no external crate.
// ---------------------------------------------------------------------------

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng {
            state: seed.wrapping_mul(2862933555777941757).wrapping_add(1),
        }
    }
    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
    fn below(&mut self, n: i32) -> i32 {
        debug_assert!(n > 0);
        ((self.next() >> 33) % n as u64) as i32
    }
    fn between(&mut self, lo: i32, hi: i32) -> i32 {
        lo + self.below(hi - lo + 1)
    }
}

// ---------------------------------------------------------------------------
// Id encoding — global synthetic PKs that embed the warehouse.
// ---------------------------------------------------------------------------

fn district_id(w: i32, d: i32) -> i32 {
    w * 100 + d
}
fn customer_id(w: i32, d: i32, c: i32) -> i32 {
    district_id(w, d) * 1000 + c
}
fn stock_id(w: i32, item: i32) -> i32 {
    w * 100_000 + item
}

// ---------------------------------------------------------------------------
// Schema + load
// ---------------------------------------------------------------------------

const SCHEMA: [&str; 9] = [
    "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_ytd BIGINT)",
    "CREATE TABLE district (d_id INT PRIMARY KEY, d_w_id INT, d_ytd BIGINT, d_next_o_id INT)",
    "CREATE TABLE customer (c_id INT PRIMARY KEY, c_balance BIGINT, c_last VARCHAR(20), c_first VARCHAR(20))",
    "CREATE TABLE orders (o_id INT PRIMARY KEY, o_c_id INT, o_carrier_id INT)",
    "CREATE TABLE new_orders (no_o_id INT PRIMARY KEY)",
    "CREATE TABLE order_line (ol_id INT PRIMARY KEY, ol_o_id INT, ol_i_id INT, ol_amount BIGINT)",
    "CREATE TABLE item (i_id INT PRIMARY KEY, i_price BIGINT)",
    "CREATE TABLE stock (s_id INT PRIMARY KEY, s_i_id INT, s_quantity INT)",
    "CREATE TABLE history (h_id INT PRIMARY KEY, h_c_id INT, h_amount BIGINT)",
];

/// Counter for the unique PKs the transactions mint, within one
/// terminal's disjoint range.
struct IdGen {
    next: i32,
}
impl IdGen {
    fn take(&mut self) -> i32 {
        let id = self.next;
        self.next += 1;
        id
    }
}

fn exec<E: StorageEngine + 'static>(session: &mut Session<E>, sql: &str) {
    session
        .execute(sql)
        .unwrap_or_else(|e| panic!("load failed [{}]: {}", sql, e));
}

/// Load all warehouses' smoke-scale data (single-threaded, before the
/// terminals start).
fn load<E: StorageEngine + 'static>(session: &mut Session<E>, config: &Config) {
    let card = config.cardinality;
    for sql in SCHEMA {
        exec(session, sql);
    }
    // Bulk-load in transactions, not auto-commit: every auto-commit INSERT
    // costs one WAL fsync (~12ms here), so a row-at-a-time load is fsync-bound
    // and scales linearly with cardinality. Batching collapses it to one fsync
    // per batch. The `item` table is one batch; each warehouse is its own batch
    // (a natural bound on the open write set — one warehouse's rows, not all).
    exec(session, "BEGIN");
    for item in 1..=card.items {
        exec(
            session,
            &format!("INSERT INTO item VALUES ({}, {})", item, 100 + item),
        );
    }
    exec(session, "COMMIT");

    let mut seed_order_id = SEED_ORDER_BASE; // below PK_BASE; loader-only
    for w in 1..=config.warehouses {
        exec(session, "BEGIN");
        exec(session, &format!("INSERT INTO warehouse VALUES ({}, 0)", w));
        for item in 1..=card.items {
            exec(
                session,
                &format!(
                    "INSERT INTO stock VALUES ({}, {}, 10000)",
                    stock_id(w, item),
                    item
                ),
            );
        }
        for d in 1..=card.districts_per_w {
            exec(
                session,
                &format!(
                    "INSERT INTO district VALUES ({}, {}, 0, {})",
                    district_id(w, d),
                    w,
                    card.initial_orders_per_d + 1
                ),
            );
            for c in 1..=card.customers_per_d {
                let cid = customer_id(w, d, c);
                exec(
                    session,
                    &format!(
                        "INSERT INTO customer VALUES ({}, 5000, 'last{}', 'first{}')",
                        cid, c, c
                    ),
                );
            }
            for _ in 0..card.initial_orders_per_d {
                let oid = seed_order_id;
                seed_order_id += 1;
                let cid = customer_id(w, d, 1 + (oid % card.customers_per_d));
                exec(
                    session,
                    &format!("INSERT INTO orders VALUES ({}, {}, 0)", oid, cid),
                );
                exec(
                    session,
                    &format!(
                        "INSERT INTO order_line VALUES ({}, {}, {}, 100)",
                        oid,
                        oid,
                        1 + (oid % card.items)
                    ),
                );
            }
        }
        exec(session, "COMMIT");
    }
}

// ---------------------------------------------------------------------------
// Prepared statements (one parse, reused every iteration).
// ---------------------------------------------------------------------------

struct Statements {
    next_o_id: PreparedStatement,
    bump_o_id: PreparedStatement,
    insert_order: PreparedStatement,
    insert_new_order: PreparedStatement,
    item_price: PreparedStatement,
    dec_stock: PreparedStatement,
    insert_order_line: PreparedStatement,

    bump_w_ytd: PreparedStatement,
    bump_d_ytd: PreparedStatement,
    cust_balance: PreparedStatement,
    debit_cust: PreparedStatement,
    insert_history: PreparedStatement,

    max_order: PreparedStatement,
    lines_for_order: PreparedStatement,

    min_new_order: PreparedStatement,
    del_new_order: PreparedStatement,
    order_customer: PreparedStatement,
    sum_lines: PreparedStatement,
    credit_cust: PreparedStatement,
    set_carrier: PreparedStatement,

    stock_level: PreparedStatement,
}

impl Statements {
    fn prepare<E: StorageEngine + 'static>(s: &mut Session<E>) -> Statements {
        let p = |s: &mut Session<E>, sql: &str| s.prepare(sql).expect("prepare");
        Statements {
            next_o_id: p(s, "SELECT d_next_o_id FROM district WHERE d_id = $1"),
            bump_o_id: p(
                s,
                "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = $1",
            ),
            insert_order: p(s, "INSERT INTO orders VALUES ($1, $2, 0)"),
            insert_new_order: p(s, "INSERT INTO new_orders VALUES ($1)"),
            item_price: p(s, "SELECT i_price FROM item WHERE i_id = $1"),
            dec_stock: p(
                s,
                "UPDATE stock SET s_quantity = s_quantity - $1 WHERE s_id = $2",
            ),
            insert_order_line: p(s, "INSERT INTO order_line VALUES ($1, $2, $3, $4)"),

            bump_w_ytd: p(s, "UPDATE warehouse SET w_ytd = w_ytd + $1 WHERE w_id = $2"),
            bump_d_ytd: p(s, "UPDATE district SET d_ytd = d_ytd + $1 WHERE d_id = $2"),
            cust_balance: p(s, "SELECT c_balance FROM customer WHERE c_id = $1"),
            debit_cust: p(
                s,
                "UPDATE customer SET c_balance = c_balance - $1 WHERE c_id = $2",
            ),
            insert_history: p(s, "INSERT INTO history VALUES ($1, $2, $3)"),

            max_order: p(s, "SELECT MAX(o_id) FROM orders WHERE o_c_id = $1"),
            lines_for_order: p(
                s,
                "SELECT ol_i_id, ol_amount FROM order_line WHERE ol_o_id = $1 ORDER BY ol_id ASC",
            ),

            min_new_order: p(
                s,
                "SELECT MIN(no_o_id) FROM new_orders WHERE no_o_id >= $1 AND no_o_id < $2",
            ),
            del_new_order: p(s, "DELETE FROM new_orders WHERE no_o_id = $1"),
            order_customer: p(s, "SELECT o_c_id FROM orders WHERE o_id = $1"),
            sum_lines: p(
                s,
                "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = $1",
            ),
            credit_cust: p(
                s,
                "UPDATE customer SET c_balance = c_balance + $1 WHERE c_id = $2",
            ),
            set_carrier: p(s, "UPDATE orders SET o_carrier_id = $1 WHERE o_id = $2"),

            stock_level: p(
                s,
                "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock \
                 WHERE ol_o_id = $1 AND s_i_id = ol_i_id AND s_quantity < $2",
            ),
        }
    }
}

fn run<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    ps: &PreparedStatement,
    params: &[Value],
) -> DbResult<QueryResult> {
    s.execute_prepared(ps, params)
}
fn rows_of(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {:?}", other),
    }
}
fn as_i32(v: &Value) -> Option<i32> {
    match v {
        Value::Int32(i) => Some(*i),
        Value::Int64(i) => Some(*i as i32),
        _ => None,
    }
}
fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int64(i) => Some(*i),
        Value::Int32(i) => Some(*i as i64),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Transactions — each returns Err on a statement failure (e.g. write
// conflict); the driver rolls back and counts the abort.
// ---------------------------------------------------------------------------

struct Ctx {
    home_w: i32,
    ids: IdGen,
    id_lo: i32,
    id_hi: i32,
    rng: Rng,
    last_order_id: i32,
    card: Cardinality,
}

fn new_order<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    st: &Statements,
    ctx: &mut Ctx,
) -> DbResult<()> {
    let w = ctx.home_w;
    let d = ctx.rng.between(1, ctx.card.districts_per_w);
    let c = ctx.rng.between(1, ctx.card.customers_per_d);
    let did = district_id(w, d);
    let cid = customer_id(w, d, c);

    let _ = rows_of(run(s, &st.next_o_id, &[Value::Int32(did)])?);
    run(s, &st.bump_o_id, &[Value::Int32(did)])?;

    let oid = ctx.ids.take();
    run(s, &st.insert_order, &[Value::Int32(oid), Value::Int32(cid)])?;
    run(s, &st.insert_new_order, &[Value::Int32(oid)])?;
    ctx.last_order_id = oid;

    let line_count = ctx.rng.between(3, 6);
    for _ in 0..line_count {
        let item = ctx.rng.between(1, ctx.card.items);
        let price = rows_of(run(s, &st.item_price, &[Value::Int32(item)])?);
        let amount = match price.first().and_then(|r| as_i64(&r[0])) {
            Some(p) => p,
            None => continue,
        };
        run(
            s,
            &st.dec_stock,
            &[Value::Int32(1), Value::Int32(stock_id(w, item))],
        )?;
        let ol_id = ctx.ids.take();
        run(
            s,
            &st.insert_order_line,
            &[
                Value::Int32(ol_id),
                Value::Int32(oid),
                Value::Int32(item),
                Value::Int64(amount),
            ],
        )?;
    }
    Ok(())
}

fn payment<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    st: &Statements,
    ctx: &mut Ctx,
) -> DbResult<()> {
    let w = ctx.home_w;
    let d = ctx.rng.between(1, ctx.card.districts_per_w);
    let c = ctx.rng.between(1, ctx.card.customers_per_d);
    let did = district_id(w, d);
    let cid = customer_id(w, d, c);
    let amount = ctx.rng.between(1, 500) as i64;

    run(s, &st.bump_w_ytd, &[Value::Int64(amount), Value::Int32(w)])?;
    run(
        s,
        &st.bump_d_ytd,
        &[Value::Int64(amount), Value::Int32(did)],
    )?;
    let _ = rows_of(run(s, &st.cust_balance, &[Value::Int32(cid)])?);
    run(
        s,
        &st.debit_cust,
        &[Value::Int64(amount), Value::Int32(cid)],
    )?;
    let hid = ctx.ids.take();
    run(
        s,
        &st.insert_history,
        &[Value::Int32(hid), Value::Int32(cid), Value::Int64(amount)],
    )?;
    Ok(())
}

fn order_status<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    st: &Statements,
    ctx: &mut Ctx,
) -> DbResult<()> {
    let w = ctx.home_w;
    let d = ctx.rng.between(1, ctx.card.districts_per_w);
    let c = ctx.rng.between(1, ctx.card.customers_per_d);
    let cid = customer_id(w, d, c);

    let max = rows_of(run(s, &st.max_order, &[Value::Int32(cid)])?);
    let oid = match max.first().and_then(|r| as_i32(&r[0])) {
        Some(o) => o,
        None => return Ok(()),
    };
    let _lines = rows_of(run(s, &st.lines_for_order, &[Value::Int32(oid)])?);
    Ok(())
}

fn delivery<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    st: &Statements,
    ctx: &mut Ctx,
) -> DbResult<()> {
    let min = rows_of(run(
        s,
        &st.min_new_order,
        &[Value::Int32(ctx.id_lo), Value::Int32(ctx.id_hi)],
    )?);
    let oid = match min.first().and_then(|r| as_i32(&r[0])) {
        Some(o) => o,
        None => return Ok(()),
    };
    run(s, &st.del_new_order, &[Value::Int32(oid)])?;

    let cust = rows_of(run(s, &st.order_customer, &[Value::Int32(oid)])?);
    let cid = match cust.first().and_then(|r| as_i32(&r[0])) {
        Some(c) => c,
        None => return Ok(()),
    };
    let sum = rows_of(run(s, &st.sum_lines, &[Value::Int32(oid)])?);
    if let Some(total) = sum.first().and_then(|r| as_i64(&r[0])) {
        run(
            s,
            &st.credit_cust,
            &[Value::Int64(total), Value::Int32(cid)],
        )?;
    }
    run(s, &st.set_carrier, &[Value::Int32(7), Value::Int32(oid)])?;
    Ok(())
}

fn stock_level<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    st: &Statements,
    ctx: &mut Ctx,
) -> DbResult<()> {
    if ctx.last_order_id == 0 {
        return Ok(());
    }
    let threshold = ctx.rng.between(10, 9000);
    let _ = rows_of(run(
        s,
        &st.stock_level,
        &[Value::Int32(ctx.last_order_id), Value::Int32(threshold)],
    )?);
    Ok(())
}

/// PK-lookup probe: a single-row read of the customer table by primary key
/// (`SELECT c_balance FROM customer WHERE c_id = $1`). This is the exact
/// access the PK-lookup planner lever turns from a SeqScan+Filter into a
/// PkLookup. Run alone (`--pk-probe`, read-only, no WAL) over the large
/// customer table, it isolates the lever's effect — the OrderStatus
/// read-only mode can't, since its reads are non-PK (MAX over o_c_id,
/// filter+sort over ol_o_id).
fn customer_lookup<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    st: &Statements,
    ctx: &mut Ctx,
) -> DbResult<()> {
    let w = ctx.home_w;
    let d = ctx.rng.between(1, ctx.card.districts_per_w);
    let c = ctx.rng.between(1, ctx.card.customers_per_d);
    let cid = customer_id(w, d, c);
    let _ = rows_of(run(s, &st.cust_balance, &[Value::Int32(cid)])?);
    Ok(())
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Txn {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
    /// Not part of the standard mix — only run in `--pk-probe` mode to
    /// isolate the PK-lookup lever (a single-row read by primary key).
    CustomerLookup,
}

/// Standard TPC-C mix: NewOrder ~45%, Payment ~43%, the rest ~4% each.
fn pick(roll: i32) -> Txn {
    match roll {
        0..=44 => Txn::NewOrder,
        45..=87 => Txn::Payment,
        88..=91 => Txn::OrderStatus,
        92..=95 => Txn::Delivery,
        _ => Txn::StockLevel,
    }
}

#[derive(Default, Clone, Copy)]
struct Counts {
    new_order: u64,
    payment: u64,
    order_status: u64,
    delivery: u64,
    stock_level: u64,
    customer_lookup: u64,
    aborts: u64,
}

// ---------------------------------------------------------------------------
// Latency capture (T16.2) — allocation-free log2 histogram.
// ---------------------------------------------------------------------------

/// Sub-buckets per octave: 16 linear steps within each power of two gives
/// ~6% value resolution — enough to compare p99s across tuning runs without
/// per-sample allocation or sorting.
const LAT_SUB: usize = 16;
/// Octaves: bucket 0 covers 0..16µs linearly; the last covers ~2^42µs
/// (~100 days). Anything above saturates into the final bucket.
const LAT_OCTAVES: usize = 40;
const LAT_BUCKETS: usize = LAT_OCTAVES * LAT_SUB;

/// Fixed-size latency histogram over microseconds. `record` is two integer
/// ops + an increment; `percentile` reconstructs the bucket's lower bound.
struct LatencyHistogram {
    counts: [u64; LAT_BUCKETS],
    total: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: [0; LAT_BUCKETS],
            total: 0,
        }
    }
}

impl LatencyHistogram {
    fn bucket_index(us: u64) -> usize {
        if us < LAT_SUB as u64 {
            return us as usize;
        }
        let exp = 63 - us.leading_zeros() as usize; // ≥ 4 here
        let octave = exp - 3;
        let sub = ((us >> (exp - 4)) & 0xF) as usize;
        (octave * LAT_SUB + sub).min(LAT_BUCKETS - 1)
    }

    /// Lower bound (µs) of bucket `idx` — the value `percentile` reports.
    fn bucket_floor_us(idx: usize) -> u64 {
        let octave = idx / LAT_SUB;
        let sub = (idx % LAT_SUB) as u64;
        if octave == 0 {
            sub
        } else {
            (LAT_SUB as u64 + sub) << (octave - 1)
        }
    }

    fn record(&mut self, elapsed: Duration) {
        self.counts[Self::bucket_index(elapsed.as_micros() as u64)] += 1;
        self.total += 1;
    }

    fn merge(&mut self, other: &LatencyHistogram) {
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a += b;
        }
        self.total += other.total;
    }

    /// Value (µs) at quantile `q` in (0, 1]: the lower bound of the bucket
    /// where the cumulative count crosses `ceil(q · total)`. 0 if empty.
    fn percentile_us(&self, q: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let rank = ((q * self.total as f64).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (idx, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Self::bucket_floor_us(idx);
            }
        }
        Self::bucket_floor_us(LAT_BUCKETS - 1)
    }
}

/// Per-transaction-type histograms (committed txns only — aborts are
/// tracked as a rate so a "fix" that trades throughput for aborts is
/// visible, not averaged in).
#[derive(Default)]
struct Latencies {
    per_txn: [LatencyHistogram; 6],
    overall: LatencyHistogram,
}

impl Latencies {
    fn record(&mut self, txn: Txn, elapsed: Duration) {
        self.per_txn[txn as usize].record(elapsed);
        self.overall.record(elapsed);
    }

    fn merge(&mut self, other: &Latencies) {
        for (a, b) in self.per_txn.iter_mut().zip(other.per_txn.iter()) {
            a.merge(b);
        }
        self.overall.merge(&other.overall);
    }
}

impl Counts {
    fn record(&mut self, txn: Txn) {
        match txn {
            Txn::NewOrder => self.new_order += 1,
            Txn::Payment => self.payment += 1,
            Txn::OrderStatus => self.order_status += 1,
            Txn::Delivery => self.delivery += 1,
            Txn::StockLevel => self.stock_level += 1,
            Txn::CustomerLookup => self.customer_lookup += 1,
        }
    }
    fn committed(&self) -> u64 {
        self.new_order
            + self.payment
            + self.order_status
            + self.delivery
            + self.stock_level
            + self.customer_lookup
    }
    fn add(&mut self, o: &Counts) {
        self.new_order += o.new_order;
        self.payment += o.payment;
        self.order_status += o.order_status;
        self.delivery += o.delivery;
        self.stock_level += o.stock_level;
        self.customer_lookup += o.customer_lookup;
        self.aborts += o.aborts;
    }
}

fn dispatch<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    st: &Statements,
    ctx: &mut Ctx,
    txn: Txn,
) -> DbResult<()> {
    match txn {
        Txn::NewOrder => new_order(s, st, ctx),
        Txn::Payment => payment(s, st, ctx),
        Txn::OrderStatus => order_status(s, st, ctx),
        Txn::Delivery => delivery(s, st, ctx),
        Txn::StockLevel => stock_level(s, st, ctx),
        Txn::CustomerLookup => customer_lookup(s, st, ctx),
    }
}

/// Map the `--exec-model` flag to the execution model the session swaps to.
fn parse_exec_model(name: &str) -> ExecModel {
    match name {
        "volcano" => ExecModel::Volcano,
        "push" => ExecModel::Push,
        other => panic!("unknown exec-model: {} (volcano|push)", other),
    }
}

/// Map the `--planner` flag to the planner strategy the session swaps to.
fn parse_planner(name: &str) -> Planner {
    match name {
        "rule-based" => Planner::RuleBased(RuleBasedPlanner),
        "selinger" => Planner::Selinger(SelingerPlanner::default()),
        other => panic!("unknown planner: {} (rule-based|selinger)", other),
    }
}

/// One terminal: its own session, pinned home warehouse and id range,
/// looping until the deadline (or its iteration budget).
fn run_terminal<E: StorageEngine + Send + Sync + 'static>(
    database: Arc<Database<E>>,
    catalog: Arc<Catalog<E>>,
    config: &Config,
    terminal: i32,
    deadline: Option<Instant>,
    iters: u64,
) -> (Counts, Latencies) {
    let mut session = Session::new(database, catalog);
    session.set_execution_model(parse_exec_model(&config.exec_model));
    session.set_planner(parse_planner(&config.planner));
    let statements = Statements::prepare(&mut session);
    let base = PK_BASE + terminal * PK_STRIDE;
    let mut ctx = Ctx {
        home_w: (terminal % config.warehouses) + 1,
        ids: IdGen { next: base },
        id_lo: base,
        id_hi: base + PK_STRIDE,
        rng: Rng::new(config.seed.wrapping_add(terminal as u64)),
        last_order_id: 0,
        card: config.cardinality,
    };
    let mut counts = Counts::default();
    let mut latencies = Latencies::default();

    let mut done = 0u64;
    loop {
        match deadline {
            Some(dl) if Instant::now() >= dl => break,
            None if done >= iters => break,
            _ => {}
        }
        // Diagnostic modes (both read-only — no write locks, read-only commit
        // skips the WAL): `--pk-probe` runs only the PK point-lookup (isolates
        // the PK-lookup lever); `--read-only` runs only OrderStatus (scan-heavy
        // read path). Otherwise the standard weighted mix.
        let txn = if config.pk_probe {
            Txn::CustomerLookup
        } else if config.read_only {
            Txn::OrderStatus
        } else {
            pick(ctx.rng.below(100))
        };
        // Latency = full user-visible transaction: BEGIN through COMMIT
        // (including the commit's group-fsync wait). Committed only.
        let txn_start = Instant::now();
        session.execute("BEGIN").expect("begin");
        match dispatch(&mut session, &statements, &mut ctx, txn) {
            Ok(()) => match session.execute("COMMIT") {
                Ok(_) => {
                    counts.record(txn);
                    latencies.record(txn, txn_start.elapsed());
                }
                Err(_) => counts.aborts += 1,
            },
            Err(_) => {
                let _ = session.execute("ROLLBACK");
                counts.aborts += 1;
            }
        }
        done += 1;
    }
    (counts, latencies)
}

/// Aggregated outcome of one workload run, for the single-run report and the
/// sweep comparison table.
struct RunMetrics {
    total: Counts,
    latencies: Latencies,
    elapsed_s: f64,
    fsyncs: u64,
}

impl RunMetrics {
    fn committed_per_s(&self) -> f64 {
        self.total.committed() as f64 / self.elapsed_s
    }
    fn tpmc(&self) -> f64 {
        self.total.new_order as f64 / self.elapsed_s * 60.0
    }
    fn abort_pct(&self) -> f64 {
        let attempts = self.total.committed() + self.total.aborts;
        if attempts > 0 {
            self.total.aborts as f64 / attempts as f64 * 100.0
        } else {
            0.0
        }
    }
}

/// Load + run the concurrent terminals against an already-opened database,
/// returning the run's metrics. Generic over the storage engine.
fn run_workload<E: StorageEngine + Send + Sync + 'static>(
    database: Arc<Database<E>>,
    catalog: Arc<Catalog<E>>,
    config: &Config,
) -> RunMetrics {
    {
        let mut loader = Session::new(database.clone(), catalog.clone());
        let load_start = Instant::now();
        load(&mut loader, config);
        println!("loaded in {:.2}s", load_start.elapsed().as_secs_f64());
    }

    let deadline = (config.duration_secs > 0)
        .then(|| Instant::now() + Duration::from_secs(config.duration_secs));
    let per_terminal_iters = (config.iterations / config.terminals as u64).max(1);

    let run_start = Instant::now();
    let per_terminal: Vec<(Counts, Latencies)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..config.terminals)
            .map(|t| {
                let database = database.clone();
                let catalog = catalog.clone();
                scope.spawn(move || {
                    run_terminal(database, catalog, config, t, deadline, per_terminal_iters)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("terminal panicked"))
            .collect()
    });
    let elapsed = run_start.elapsed().as_secs_f64();

    let mut total = Counts::default();
    let mut latencies = Latencies::default();
    for (c, l) in &per_terminal {
        total.add(c);
        latencies.merge(l);
    }
    RunMetrics {
        total,
        latencies,
        elapsed_s: elapsed,
        fsyncs: database.wal_fsync_count(),
    }
}

/// One report line: count plus p50/p95/p99 (ms) when any samples exist.
fn print_txn_line(name: &str, count: u64, h: &LatencyHistogram) {
    if count == 0 {
        println!("  {:<11} {:>8}", name, count);
        return;
    }
    println!(
        "  {:<11} {:>8}   p50 {:>7.2}ms  p95 {:>7.2}ms  p99 {:>7.2}ms",
        name,
        count,
        h.percentile_us(0.50) as f64 / 1000.0,
        h.percentile_us(0.95) as f64 / 1000.0,
        h.percentile_us(0.99) as f64 / 1000.0,
    );
}

/// Detailed single-run report (the per-transaction breakdown + headline rates).
fn print_report(label: &str, m: &RunMetrics) {
    let t = &m.total;
    let l = &m.latencies;
    let committed = t.committed();
    println!(
        "\n--- {}: {} committed, {} aborted in {:.2}s ---",
        label, committed, t.aborts, m.elapsed_s
    );
    print_txn_line("NewOrder", t.new_order, &l.per_txn[Txn::NewOrder as usize]);
    print_txn_line("Payment", t.payment, &l.per_txn[Txn::Payment as usize]);
    print_txn_line(
        "OrderStatus",
        t.order_status,
        &l.per_txn[Txn::OrderStatus as usize],
    );
    print_txn_line("Delivery", t.delivery, &l.per_txn[Txn::Delivery as usize]);
    print_txn_line(
        "StockLevel",
        t.stock_level,
        &l.per_txn[Txn::StockLevel as usize],
    );
    print_txn_line(
        "CustLookup",
        t.customer_lookup,
        &l.per_txn[Txn::CustomerLookup as usize],
    );
    print_txn_line("ALL", committed, &l.overall);
    println!("  abort rate  {:.1}%", m.abort_pct());
    println!("  fsyncs      {}", m.fsyncs);
    println!("  throughput  {:.0} committed txn/s", m.committed_per_s());
    println!("  tpmC (NewOrder/min) {:.0}", m.tpmc());
}

/// Build the requested engine for `config`, run the workload, and return its
/// metrics. Shared by the single-run and sweep paths.
fn run_one_config(config: &Config) -> RunMetrics {
    match config.engine.as_str() {
        "btree" => {
            let dm = FileDiskManager::create(config.data_dir.join("tpcc.db")).expect("db file");
            let bpm = BufferPoolManager::new(config.pool_size, dm);
            // Swap in the requested eviction policy (default ARC). Cold mode:
            // the pool is empty at startup, so there's nothing to transfer.
            let _ = bpm.swap_policy(
                make_policy(&config.policy, config.pool_size),
                SwapMode::Cold,
            );
            let engine = BTreeEngine::new(bpm).expect("engine");
            let database = Arc::new(Database::open(&config.data_dir, engine).expect("open db"));
            let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).expect("catalog"));
            run_workload(database, catalog, config)
        }
        "lsm" => {
            let lsm_dir = config.data_dir.join("lsm");
            std::fs::create_dir_all(&lsm_dir).expect("lsm dir");
            let engine = LsmEngine::new(&lsm_dir).expect("engine");
            let database = Arc::new(Database::open(&config.data_dir, engine).expect("open db"));
            let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).expect("catalog"));
            run_workload(database, catalog, config)
        }
        other => panic!("unknown engine: {} (use btree|lsm)", other),
    }
}

/// Run the engine × exec-model × planner matrix, each in its own fresh data
/// dir, and print a comparison table sorted by tpmC.
fn run_sweep(base: &Config) {
    let mode = if base.duration_secs > 0 {
        format!("{}s", base.duration_secs)
    } else {
        format!("{} iters", base.iterations)
    };
    println!(
        "TPC-C sweep: {} warehouse(s), {} terminal(s), {}, seed {}",
        base.warehouses, base.terminals, mode, base.seed
    );

    let mut results: Vec<(String, RunMetrics)> = Vec::new();
    for engine in ["btree", "lsm"] {
        for exec_model in ["volcano", "push"] {
            for planner in ["rule-based", "selinger"] {
                let mut cfg = base.clone();
                cfg.engine = engine.to_string();
                cfg.exec_model = exec_model.to_string();
                cfg.planner = planner.to_string();
                cfg.data_dir = base
                    .data_dir
                    .join(format!("{engine}-{exec_model}-{planner}"));
                let _ = std::fs::remove_dir_all(&cfg.data_dir);
                std::fs::create_dir_all(&cfg.data_dir).expect("cell dir");
                eprintln!("  running {engine} / {exec_model} / {planner} ...");
                let label = format!("{engine:<6} {exec_model:<8} {planner}");
                results.push((label, run_one_config(&cfg)));
            }
        }
    }
    results.sort_by(|a, b| b.1.tpmc().partial_cmp(&a.1.tpmc()).unwrap());

    println!("\n=== TPC-C config bake-off (sorted by tpmC) ===");
    println!(
        "{:<24} {:>8} {:>10} {:>8} {:>8} {:>8}",
        "engine exec     planner", "tpmC", "txn/s", "p99 ms", "abort%", "fsyncs"
    );
    for (label, m) in &results {
        println!(
            "{:<24} {:>8.0} {:>10.0} {:>8.2} {:>7.1}% {:>8}",
            label,
            m.tpmc(),
            m.committed_per_s(),
            m.latencies.overall.percentile_us(0.99) as f64 / 1000.0,
            m.abort_pct(),
            m.fsyncs
        );
    }
}

fn main() {
    let config = Config::from_args();
    let _ = std::fs::remove_dir_all(&config.data_dir);
    std::fs::create_dir_all(&config.data_dir).expect("create data dir");

    if config.sweep {
        run_sweep(&config);
        return;
    }

    let mode = if config.duration_secs > 0 {
        format!("{}s", config.duration_secs)
    } else {
        format!("{} iters", config.iterations)
    };
    let policy_label = if config.engine == "btree" {
        config.policy.as_str()
    } else {
        "n/a"
    };
    println!(
        "TPC-C [{} / {} / {} / {}]: {} warehouse(s), {} terminal(s), {}, seed {}",
        config.engine,
        policy_label,
        config.exec_model,
        config.planner,
        config.warehouses,
        config.terminals,
        mode,
        config.seed
    );
    print_report("complete", &run_one_config(&config));
}

#[cfg(test)]
mod latency_tests {
    use super::*;

    // Bucketing must be monotone and every bucket's floor must map back
    // into that bucket — the pair of invariants percentile() relies on.
    #[test]
    fn bucket_index_monotone_and_floor_consistent() {
        let mut prev = 0;
        for us in [
            0u64,
            1,
            15,
            16,
            17,
            31,
            32,
            100,
            1_000,
            65_535,
            1_000_000,
            1 << 40,
        ] {
            let idx = LatencyHistogram::bucket_index(us);
            assert!(idx >= prev, "bucket index not monotone at {}µs", us);
            prev = idx;
            let floor = LatencyHistogram::bucket_floor_us(idx);
            assert!(floor <= us, "floor {} above value {}", floor, us);
            assert_eq!(
                LatencyHistogram::bucket_index(floor),
                idx,
                "floor of bucket {} maps elsewhere",
                idx
            );
        }
    }

    // Known distribution: 99 samples at ~1ms, 1 at ~100ms. p50 ≈ 1ms
    // (within one bucket's ~6% resolution), p99 must reach the outlier's
    // bucket. Also proves merge() = record()-on-one.
    #[test]
    fn percentiles_split_the_known_outlier() {
        let mut a = LatencyHistogram::default();
        let mut b = LatencyHistogram::default();
        for _ in 0..99 {
            a.record(Duration::from_micros(1_000));
        }
        b.record(Duration::from_micros(100_000));
        a.merge(&b);

        let p50 = a.percentile_us(0.50);
        assert!((900..=1_000).contains(&p50), "p50 {}µs not ~1ms", p50);
        let p99 = a.percentile_us(0.99);
        assert!(
            (900..=1_000).contains(&p99),
            "p99 {}µs should still be ~1ms (rank 99 of 100)",
            p99
        );
        let p999 = a.percentile_us(0.999);
        assert!(
            p999 >= 96_000,
            "p99.9 {}µs must reach the 100ms outlier",
            p999
        );
    }

    #[test]
    fn empty_histogram_reports_zero() {
        let h = LatencyHistogram::default();
        assert_eq!(h.percentile_us(0.99), 0);
    }
}
