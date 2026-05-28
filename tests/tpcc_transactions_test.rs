//! P13.8: All 5 TPC-C transactions as end-to-end prepared statements.
//!
//! Phase 13 milestone: every TPC-C transaction (NewOrder, Payment,
//! OrderStatus, Delivery, StockLevel) is *runnable* through the SQL
//! surface as a prepared statement. Performance is not a target here —
//! that's Phase 16. Correctness is.
//!
//! Schemas are simplified vs. the TPC-C spec: single-column synthetic
//! PKs throughout, fewer columns, single warehouse/district. The point
//! is to exercise the SQL shapes each transaction needs:
//! - NewOrder: point lookups, INSERT, column-self UPDATE (`x = x + 1`).
//! - Payment: ORDER BY + LIMIT for c_last fallback, decrement UPDATE.
//! - OrderStatus: MAX aggregate, follow-up point lookup.
//! - Delivery: MIN aggregate, SUM aggregate, DELETE, increment UPDATE.
//! - StockLevel: JOIN + COUNT(DISTINCT) — the marquee test.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::storage::FileDiskManager;
use interchangedb::types::Value;

fn fresh_session() -> (Session<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let bpm = BufferPoolManager::new(1024, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database = Arc::new(Database::open(dir.path(), engine).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    let session = Session::new(database, catalog);
    (session, dir)
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got: {:?}", other),
    }
}

/// Create all TPC-C-shaped tables we use across the 5 transaction tests.
/// Schemas are intentionally minimal — just the columns each transaction
/// touches.
fn create_schemas(s: &mut Session<BTreeEngine>) {
    let tables = [
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
    for sql in tables {
        s.execute(sql).unwrap();
    }
}

/// Seed a tiny baseline dataset reused by multiple transactions.
fn seed_baseline(s: &mut Session<BTreeEngine>) {
    s.execute("INSERT INTO warehouse VALUES (1, 0)").unwrap();
    s.execute("INSERT INTO district VALUES (10, 1, 0, 100)").unwrap();
    // Two customers, different last names.
    s.execute("INSERT INTO customer VALUES (1, 5000, 'kim', 'alice')").unwrap();
    s.execute("INSERT INTO customer VALUES (2, 3000, 'kim', 'bob')").unwrap();
    s.execute("INSERT INTO customer VALUES (3, 1000, 'smith', 'carol')").unwrap();
    // Three items.
    s.execute("INSERT INTO item VALUES (101, 250)").unwrap();
    s.execute("INSERT INTO item VALUES (102, 500)").unwrap();
    s.execute("INSERT INTO item VALUES (103, 1000)").unwrap();
    // Stock for each item.
    s.execute("INSERT INTO stock VALUES (1, 101, 50)").unwrap();
    s.execute("INSERT INTO stock VALUES (2, 102, 100)").unwrap();
    s.execute("INSERT INTO stock VALUES (3, 103, 20)").unwrap();
}

// ---- NewOrder ----

#[test]
fn new_order_runs_through_lookups_inserts_and_increment_update() {
    let (mut s, _d) = fresh_session();
    create_schemas(&mut s);
    seed_baseline(&mut s);

    // Look up next o_id from the district.
    let ps_next = s.prepare("SELECT d_next_o_id FROM district WHERE d_id = $1").unwrap();
    let r = rows(s.execute_prepared(&ps_next, &[Value::Int32(10)]).unwrap());
    assert_eq!(r[0][0], Value::Int32(100));

    // Bump d_next_o_id (column-self increment via UPDATE).
    let ps_bump = s
        .prepare("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = $1")
        .unwrap();
    s.execute_prepared(&ps_bump, &[Value::Int32(10)]).unwrap();
    let r = rows(s.execute_prepared(&ps_next, &[Value::Int32(10)]).unwrap());
    assert_eq!(r[0][0], Value::Int32(101));

    // Insert the new order.
    let ps_order = s
        .prepare("INSERT INTO orders VALUES ($1, $2, 0)")
        .unwrap();
    s.execute_prepared(&ps_order, &[Value::Int32(100), Value::Int32(1)])
        .unwrap();
    s.execute_prepared(&ps_order, &[Value::Int32(101), Value::Int32(2)])
        .unwrap();
    // Confirm both orders present.
    let r = rows(s.execute("SELECT o_id FROM orders ORDER BY o_id ASC").unwrap());
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(100));
    assert_eq!(r[1][0], Value::Int32(101));

    // For each line item: look up price, decrement stock, insert order_line.
    let ps_price = s.prepare("SELECT i_price FROM item WHERE i_id = $1").unwrap();
    let price = rows(s.execute_prepared(&ps_price, &[Value::Int32(101)]).unwrap());
    assert_eq!(price[0][0], Value::Int64(250));

    let ps_stock_dec = s
        .prepare("UPDATE stock SET s_quantity = s_quantity - $1 WHERE s_i_id = $2")
        .unwrap();
    s.execute_prepared(&ps_stock_dec, &[Value::Int32(3), Value::Int32(101)])
        .unwrap();
    let after = rows(
        s.execute("SELECT s_quantity FROM stock WHERE s_i_id = 101")
            .unwrap(),
    );
    assert_eq!(after[0][0], Value::Int32(47));  // 50 - 3

    let ps_ol = s
        .prepare("INSERT INTO order_line VALUES ($1, $2, $3, $4)")
        .unwrap();
    s.execute_prepared(
        &ps_ol,
        &[Value::Int32(1), Value::Int32(100), Value::Int32(101), Value::Int64(750)],
    )
    .unwrap();
}

// ---- Payment ----

#[test]
fn payment_runs_with_c_last_fallback_via_order_by() {
    let (mut s, _d) = fresh_session();
    create_schemas(&mut s);
    seed_baseline(&mut s);

    // Bump warehouse / district YTD by the payment amount.
    let ps_w = s
        .prepare("UPDATE warehouse SET w_ytd = w_ytd + $1 WHERE w_id = $2")
        .unwrap();
    let ps_d = s
        .prepare("UPDATE district SET d_ytd = d_ytd + $1 WHERE d_id = $2")
        .unwrap();
    let amount = Value::Int64(100);
    s.execute_prepared(&ps_w, &[amount.clone(), Value::Int32(1)])
        .unwrap();
    s.execute_prepared(&ps_d, &[amount.clone(), Value::Int32(10)])
        .unwrap();

    // Customer lookup by last name (alphabetical by first name, take
    // first match). Two customers share the last name "kim".
    let ps_cust_by_last = s
        .prepare("SELECT c_id, c_balance FROM customer WHERE c_last = $1 ORDER BY c_first ASC LIMIT 1")
        .unwrap();
    let r = rows(
        s.execute_prepared(&ps_cust_by_last, &[Value::Varchar("kim".into())])
            .unwrap(),
    );
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));  // c_id for "kim, alice"

    // Debit the customer.
    let ps_debit = s
        .prepare("UPDATE customer SET c_balance = c_balance - $1 WHERE c_id = $2")
        .unwrap();
    s.execute_prepared(&ps_debit, &[amount.clone(), Value::Int32(1)])
        .unwrap();
    let after = rows(
        s.execute("SELECT c_balance FROM customer WHERE c_id = 1")
            .unwrap(),
    );
    assert_eq!(after[0][0], Value::Int64(4900));  // 5000 - 100

    // History row.
    let ps_h = s
        .prepare("INSERT INTO history VALUES ($1, $2, $3)")
        .unwrap();
    s.execute_prepared(
        &ps_h,
        &[Value::Int32(1), Value::Int32(1), amount.clone()],
    )
    .unwrap();
}

// ---- OrderStatus ----

#[test]
fn order_status_finds_most_recent_order_via_max() {
    let (mut s, _d) = fresh_session();
    create_schemas(&mut s);
    seed_baseline(&mut s);
    // Three orders for customer 1.
    s.execute("INSERT INTO orders VALUES (10, 1, 0)").unwrap();
    s.execute("INSERT INTO orders VALUES (20, 1, 0)").unwrap();
    s.execute("INSERT INTO orders VALUES (30, 1, 0)").unwrap();
    // Order lines for the most recent.
    s.execute("INSERT INTO order_line VALUES (1, 30, 101, 100)").unwrap();
    s.execute("INSERT INTO order_line VALUES (2, 30, 102, 200)").unwrap();

    let ps_max = s.prepare("SELECT MAX(o_id) FROM orders WHERE o_c_id = $1").unwrap();
    let r = rows(s.execute_prepared(&ps_max, &[Value::Int32(1)]).unwrap());
    assert_eq!(r[0][0], Value::Int32(30));

    // Now fetch the order lines for the latest order.
    let ps_lines = s
        .prepare("SELECT ol_i_id, ol_amount FROM order_line WHERE ol_o_id = $1 ORDER BY ol_id ASC")
        .unwrap();
    let lines = rows(s.execute_prepared(&ps_lines, &[Value::Int32(30)]).unwrap());
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0][0], Value::Int32(101));
    assert_eq!(lines[1][0], Value::Int32(102));
}

// ---- Delivery ----

#[test]
fn delivery_drains_oldest_new_order_and_sums_lines() {
    let (mut s, _d) = fresh_session();
    create_schemas(&mut s);
    seed_baseline(&mut s);
    // Three pending orders in new_orders. Delivery takes the smallest.
    s.execute("INSERT INTO new_orders VALUES (10)").unwrap();
    s.execute("INSERT INTO new_orders VALUES (20)").unwrap();
    s.execute("INSERT INTO new_orders VALUES (30)").unwrap();
    // Matching order rows + lines for order 10.
    s.execute("INSERT INTO orders VALUES (10, 1, 0)").unwrap();
    s.execute("INSERT INTO order_line VALUES (100, 10, 101, 250)").unwrap();
    s.execute("INSERT INTO order_line VALUES (101, 10, 102, 750)").unwrap();

    // MIN gives the oldest pending order id.
    let ps_min = s.prepare("SELECT MIN(no_o_id) FROM new_orders").unwrap();
    let r = rows(s.execute_prepared(&ps_min, &[]).unwrap());
    assert_eq!(r[0][0], Value::Int32(10));

    // Remove it from new_orders.
    let ps_del = s.prepare("DELETE FROM new_orders WHERE no_o_id = $1").unwrap();
    s.execute_prepared(&ps_del, &[Value::Int32(10)]).unwrap();
    let remaining = rows(s.execute("SELECT no_o_id FROM new_orders ORDER BY no_o_id ASC").unwrap());
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0][0], Value::Int32(20));

    // Find the customer for that order.
    let ps_cust = s.prepare("SELECT o_c_id FROM orders WHERE o_id = $1").unwrap();
    let r = rows(s.execute_prepared(&ps_cust, &[Value::Int32(10)]).unwrap());
    let customer_id = match r[0][0] { Value::Int32(i) => i, _ => panic!() };

    // Sum the order line amounts.
    let ps_sum = s
        .prepare("SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = $1")
        .unwrap();
    let r = rows(s.execute_prepared(&ps_sum, &[Value::Int32(10)]).unwrap());
    assert_eq!(r[0][0], Value::Int64(1000));  // 250 + 750

    // Credit the customer's balance.
    let ps_credit = s
        .prepare("UPDATE customer SET c_balance = c_balance + $1 WHERE c_id = $2")
        .unwrap();
    s.execute_prepared(&ps_credit, &[Value::Int64(1000), Value::Int32(customer_id)])
        .unwrap();
    let after = rows(
        s.execute(&format!("SELECT c_balance FROM customer WHERE c_id = {}", customer_id))
            .unwrap(),
    );
    assert_eq!(after[0][0], Value::Int64(6000));  // 5000 + 1000

    // Update orders.o_carrier_id (the actual delivery step).
    let ps_carrier = s
        .prepare("UPDATE orders SET o_carrier_id = $1 WHERE o_id = $2")
        .unwrap();
    s.execute_prepared(&ps_carrier, &[Value::Int32(7), Value::Int32(10)])
        .unwrap();
}

// ---- StockLevel ----

#[test]
fn stock_level_counts_distinct_items_via_join() {
    // The marquee Phase 13 test: JOIN + COUNT(DISTINCT) + WHERE.
    let (mut s, _d) = fresh_session();
    create_schemas(&mut s);
    seed_baseline(&mut s);

    // Build some order_line rows referencing items, with one duplicate
    // (item 101 appears twice) so COUNT(DISTINCT) ≠ COUNT.
    s.execute("INSERT INTO order_line VALUES (1, 1000, 101, 0)").unwrap();
    s.execute("INSERT INTO order_line VALUES (2, 1000, 102, 0)").unwrap();
    s.execute("INSERT INTO order_line VALUES (3, 1001, 101, 0)").unwrap();  // duplicate i_id
    s.execute("INSERT INTO order_line VALUES (4, 1001, 103, 0)").unwrap();

    // Lower stock of item 101 below threshold 30; others above.
    s.execute("UPDATE stock SET s_quantity = 5 WHERE s_i_id = 101").unwrap();
    s.execute("UPDATE stock SET s_quantity = 50 WHERE s_i_id = 102").unwrap();
    s.execute("UPDATE stock SET s_quantity = 100 WHERE s_i_id = 103").unwrap();

    // StockLevel: COUNT(DISTINCT s_i_id) joined against order_line where
    // stock < threshold. Implicit cross-product + WHERE form (still a
    // valid SQL form — the equijoin predicate lives in WHERE).
    let ps_stock_level = s
        .prepare(
            "SELECT COUNT(DISTINCT s_i_id) \
             FROM order_line, stock \
             WHERE ol_o_id = $1 AND s_i_id = ol_i_id AND s_quantity < $2",
        )
        .unwrap();

    // Order 1000 references items 101 and 102. Threshold = 30.
    // Both items match (101 has stock 5, 102 has stock 50 — wait 50 >= 30).
    // So only item 101 qualifies. → 1.
    let r = rows(
        s.execute_prepared(&ps_stock_level, &[Value::Int32(1000), Value::Int32(30)])
            .unwrap(),
    );
    assert_eq!(r[0][0], Value::Int64(1));

    // Higher threshold: include item 102 too.
    let r = rows(
        s.execute_prepared(&ps_stock_level, &[Value::Int32(1000), Value::Int32(60)])
            .unwrap(),
    );
    assert_eq!(r[0][0], Value::Int64(2));

    // Order 1001 references items 101 and 103. Threshold = 10.
    // Only item 101 (stock 5) qualifies. → 1.
    let r = rows(
        s.execute_prepared(&ps_stock_level, &[Value::Int32(1001), Value::Int32(10)])
            .unwrap(),
    );
    assert_eq!(r[0][0], Value::Int64(1));
}
