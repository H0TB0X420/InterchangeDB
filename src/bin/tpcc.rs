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
use interchangedb::index::btree::BTreeEngine;
use interchangedb::session::{PreparedStatement, QueryResult, Session};
use interchangedb::storage::FileDiskManager;
use interchangedb::types::Value;
use interchangedb::{LsmEngine, StorageEngine};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

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
                other => panic!("unknown flag: {}", other),
            }
        }
        assert!(c.warehouses >= 1 && c.terminals >= 1);
        c
    }
}

// Smoke-scale cardinalities (NOT TPC-C spec scale — enough to exercise
// every transaction's code path).
const DISTRICTS_PER_W: i32 = 2;
const CUSTOMERS_PER_D: i32 = 10;
const ITEMS: i32 = 20;
const INITIAL_ORDERS_PER_D: i32 = 5;

// Per-terminal unique-PK range: terminal t owns `[BASE + t*STRIDE, …)`.
const PK_BASE: i32 = 2_000_000;
const PK_STRIDE: i32 = 10_000_000;

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
    for sql in SCHEMA {
        exec(session, sql);
    }
    for item in 1..=ITEMS {
        exec(
            session,
            &format!("INSERT INTO item VALUES ({}, {})", item, 100 + item),
        );
    }

    let mut seed_order_id = 1_000_000; // below PK_BASE; loader-only
    for w in 1..=config.warehouses {
        exec(session, &format!("INSERT INTO warehouse VALUES ({}, 0)", w));
        for item in 1..=ITEMS {
            exec(
                session,
                &format!(
                    "INSERT INTO stock VALUES ({}, {}, 10000)",
                    stock_id(w, item),
                    item
                ),
            );
        }
        for d in 1..=DISTRICTS_PER_W {
            exec(
                session,
                &format!(
                    "INSERT INTO district VALUES ({}, {}, 0, {})",
                    district_id(w, d),
                    w,
                    INITIAL_ORDERS_PER_D + 1
                ),
            );
            for c in 1..=CUSTOMERS_PER_D {
                let cid = customer_id(w, d, c);
                exec(
                    session,
                    &format!(
                        "INSERT INTO customer VALUES ({}, 5000, 'last{}', 'first{}')",
                        cid, c, c
                    ),
                );
            }
            for _ in 0..INITIAL_ORDERS_PER_D {
                let oid = seed_order_id;
                seed_order_id += 1;
                let cid = customer_id(w, d, 1 + (oid % CUSTOMERS_PER_D));
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
                        1 + (oid % ITEMS)
                    ),
                );
            }
        }
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
}

fn new_order<E: StorageEngine + 'static>(
    s: &mut Session<E>,
    st: &Statements,
    ctx: &mut Ctx,
) -> DbResult<()> {
    let w = ctx.home_w;
    let d = ctx.rng.between(1, DISTRICTS_PER_W);
    let c = ctx.rng.between(1, CUSTOMERS_PER_D);
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
        let item = ctx.rng.between(1, ITEMS);
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
    let d = ctx.rng.between(1, DISTRICTS_PER_W);
    let c = ctx.rng.between(1, CUSTOMERS_PER_D);
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
    let d = ctx.rng.between(1, DISTRICTS_PER_W);
    let c = ctx.rng.between(1, CUSTOMERS_PER_D);
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
    aborts: u64,
}

impl Counts {
    fn record(&mut self, txn: Txn) {
        match txn {
            Txn::NewOrder => self.new_order += 1,
            Txn::Payment => self.payment += 1,
            Txn::OrderStatus => self.order_status += 1,
            Txn::Delivery => self.delivery += 1,
            Txn::StockLevel => self.stock_level += 1,
        }
    }
    fn committed(&self) -> u64 {
        self.new_order + self.payment + self.order_status + self.delivery + self.stock_level
    }
    fn add(&mut self, o: &Counts) {
        self.new_order += o.new_order;
        self.payment += o.payment;
        self.order_status += o.order_status;
        self.delivery += o.delivery;
        self.stock_level += o.stock_level;
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
) -> Counts {
    let mut session = Session::new(database, catalog);
    let statements = Statements::prepare(&mut session);
    let base = PK_BASE + terminal * PK_STRIDE;
    let mut ctx = Ctx {
        home_w: (terminal % config.warehouses) + 1,
        ids: IdGen { next: base },
        id_lo: base,
        id_hi: base + PK_STRIDE,
        rng: Rng::new(config.seed.wrapping_add(terminal as u64)),
        last_order_id: 0,
    };
    let mut counts = Counts::default();

    let mut done = 0u64;
    loop {
        match deadline {
            Some(dl) if Instant::now() >= dl => break,
            None if done >= iters => break,
            _ => {}
        }
        // Read-only diagnostic mode runs only OrderStatus (pure MVCC
        // reads, no write locks, read-only commit skips the WAL).
        let txn = if config.read_only {
            Txn::OrderStatus
        } else {
            pick(ctx.rng.below(100))
        };
        session.execute("BEGIN").expect("begin");
        match dispatch(&mut session, &statements, &mut ctx, txn) {
            Ok(()) => match session.execute("COMMIT") {
                Ok(_) => counts.record(txn),
                Err(_) => counts.aborts += 1,
            },
            Err(_) => {
                let _ = session.execute("ROLLBACK");
                counts.aborts += 1;
            }
        }
        done += 1;
    }
    counts
}

/// Load + run the concurrent terminals against an already-opened
/// database, then report. Generic over the storage engine.
fn run_workload<E: StorageEngine + Send + Sync + 'static>(
    database: Arc<Database<E>>,
    catalog: Arc<Catalog<E>>,
    config: &Config,
) {
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
    let per_terminal: Vec<Counts> = std::thread::scope(|scope| {
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
    for c in &per_terminal {
        total.add(c);
    }
    let committed = total.committed();
    let attempts = committed + total.aborts;

    println!(
        "\n--- complete: {} committed, {} aborted in {:.2}s ---",
        committed, total.aborts, elapsed
    );
    println!("  NewOrder    {}", total.new_order);
    println!("  Payment     {}", total.payment);
    println!("  OrderStatus {}", total.order_status);
    println!("  Delivery    {}", total.delivery);
    println!("  StockLevel  {}", total.stock_level);
    println!(
        "  abort rate  {:.1}%",
        if attempts > 0 {
            total.aborts as f64 / attempts as f64 * 100.0
        } else {
            0.0
        }
    );
    println!("  fsyncs      {}", database.wal_fsync_count());
    println!(
        "  throughput  {:.0} committed txn/s",
        committed as f64 / elapsed
    );
    println!(
        "  tpmC (NewOrder/min) {:.0}",
        total.new_order as f64 / elapsed * 60.0
    );
}

fn main() {
    let config = Config::from_args();

    let _ = std::fs::remove_dir_all(&config.data_dir);
    std::fs::create_dir_all(&config.data_dir).expect("create data dir");

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
        "TPC-C [{} / {}]: {} warehouse(s), {} terminal(s), {}, seed {}",
        config.engine, policy_label, config.warehouses, config.terminals, mode, config.seed
    );

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
            run_workload(database, catalog, &config);
        }
        "lsm" => {
            let lsm_dir = config.data_dir.join("lsm");
            std::fs::create_dir_all(&lsm_dir).expect("lsm dir");
            let engine = LsmEngine::new(&lsm_dir).expect("engine");
            let database = Arc::new(Database::open(&config.data_dir, engine).expect("open db"));
            let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).expect("catalog"));
            run_workload(database, catalog, &config);
        }
        other => panic!("unknown engine: {} (use btree|lsm)", other),
    }
}
