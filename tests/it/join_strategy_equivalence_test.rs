//! E11 — join-strategy equivalence differential.
//!
//! The same logical join must return the same rows regardless of which
//! physical strategy the planner picks. Before the canonical-comparator
//! fix, the three strategies used three different equality definitions:
//! NestedLoopJoin the compiled predicate (numeric cross-promotion),
//! HashJoin the derived `Value` `Hash`/`Eq` (Int32(5) ≠ Int64(5)), and
//! IndexNestedLoopJoin the index key encoding (errors on a mismatched
//! probe type). A mixed Int32/Int64 join key made the SAME query return
//! different rows per strategy.
//!
//! Methodology: table `a` carries an Int64 (nullable) join key, table `b`
//! an Int32 key with a secondary index (so INLJ is constructible). The key
//! sets overlap on {1, 2}, include a NULL (must match nothing under SQL
//! equi-join), an Int64 value exceeding i32 range (can match nothing), and
//! an unmatched value per side. The same join runs under all three
//! strategies; result multisets must be identical.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::ExecModel;
use interchangedb::layout::RowLayout;
use interchangedb::sql::ir::expr::{CompareOp, Expression, Predicate};
use interchangedb::sql::ir::logical::{JoinKind, OrderDir};
use interchangedb::sql::ir::physical::PhysOp;
use interchangedb::storage::MemoryDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Value};

struct Env {
    engine: Arc<BTreeEngine>,
    catalog: Arc<Catalog<BTreeEngine>>,
    _dir: tempfile::TempDir,
}

/// `a(a_id INT PK, a_key BIGINT NULL)` and `b(b_id INT PK, b_key INT)`
/// with a secondary index on `b_key`. Key overlap chosen to exercise:
/// cross-type match (Int64 vs Int32), NULL exclusion, out-of-i32-range
/// exclusion, and plain non-matches.
fn setup() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let engine = Arc::new(BTreeEngine::new(BufferPoolManager::new(256, dm)).unwrap());
    let catalog = Arc::new(
        Catalog::open_persistent(
            engine.clone(),
            dir.path().join("indexes"),
            interchangedb::default_index_opener(),
        )
        .unwrap(),
    );

    catalog
        .create_table(
            "a".into(),
            Schema {
                name: "a".into(),
                table_id: TableId(0),
                columns: vec![
                    ColumnDef {
                        name: "a_id".into(),
                        ty: ColumnType::Int32,
                        nullable: false,
                        default: None,
                    },
                    ColumnDef {
                        name: "a_key".into(),
                        ty: ColumnType::Int64,
                        nullable: true,
                        default: None,
                    },
                ],
                primary_key: vec![0],
            },
        )
        .unwrap();
    let b_id = catalog
        .create_table(
            "b".into(),
            Schema {
                name: "b".into(),
                table_id: TableId(0),
                columns: vec![
                    ColumnDef {
                        name: "b_id".into(),
                        ty: ColumnType::Int32,
                        nullable: false,
                        default: None,
                    },
                    ColumnDef {
                        name: "b_key".into(),
                        ty: ColumnType::Int32,
                        nullable: false,
                        default: None,
                    },
                ],
                primary_key: vec![0],
            },
        )
        .unwrap();
    catalog
        .create_index(IndexDef {
            name: "b_by_key".into(),
            table_id: b_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();

    let a_schema = catalog.get_table("a").unwrap();
    let a_indexes = catalog
        .indexes_for_table(a_schema.table_id, &a_schema)
        .unwrap();
    let a = Table::with_indexes(engine.clone(), a_schema, RowLayout, a_indexes);
    let b_schema = catalog.get_table("b").unwrap();
    let b_indexes = catalog
        .indexes_for_table(b_schema.table_id, &b_schema)
        .unwrap();
    let b = Table::with_indexes(engine.clone(), b_schema, RowLayout, b_indexes);

    // a_key: 1 and 2 match b; NULL matches nothing; 5B exceeds i32 range;
    // 9 has no partner in b.
    let a_rows = [
        (1, Some(1i64)),
        (2, Some(2)),
        (3, None),
        (4, Some(5_000_000_000)),
        (5, Some(9)),
    ];
    for (id, key) in a_rows {
        a.insert(&[
            Value::Int32(id),
            key.map(Value::Int64).unwrap_or(Value::Null),
        ])
        .unwrap();
    }
    // b_key: 1 twice (dup-match fan-out), 2 once, 7 unmatched.
    let b_rows = [(10, 1), (11, 1), (12, 2), (13, 7)];
    for (id, key) in b_rows {
        b.insert(&[Value::Int32(id), Value::Int32(key)]).unwrap();
    }

    Env {
        engine,
        catalog,
        _dir: dir,
    }
}

fn seq(table: &str) -> Box<PhysOp> {
    Box::new(PhysOp::SeqScan {
        table: table.into(),
    })
}

/// A table scan sorted ascending on `key_col` — MergeJoin's inputs must
/// arrive sorted (T17-B.1); the planner supplies the Sort, so the test
/// does too.
fn sorted_seq(table: &str, key_col: usize) -> Box<PhysOp> {
    Box::new(PhysOp::Sort {
        input: seq(table),
        keys: vec![(key_col, OrderDir::Asc)],
    })
}

fn run_sorted(env: &Env, model: &ExecModel, plan: &PhysOp) -> Vec<Vec<Value>> {
    let (_schema, mut rows) = model.execute(plan, &env.engine, &env.catalog).unwrap();
    // Values carry no Ord; Debug text is a stable total order for a test.
    rows.sort_by_key(|r| format!("{:?}", r));
    rows
}

// The joined tuple is `a || b` (4 columns): a_key is global column 1,
// b_key is global column 3 (local 1 on the inner side). All four
// strategies × both execution models must agree — the strategy axis is
// E11 (merge added in T17-B.1), the model axis is scope note S10's
// uncovered cross. Merge's Sort wrappers don't change the row multiset.
#[test]
fn all_strategies_and_models_return_identical_rows_on_mixed_int_keys() {
    let env = setup();

    let plans = [
        (
            "nlj",
            PhysOp::NestedLoopJoin {
                outer: seq("a"),
                inner: seq("b"),
                on: Some(Predicate::Compare {
                    op: CompareOp::Eq,
                    left: Expression::Column(1),
                    right: Expression::Column(3),
                }),
                kind: JoinKind::Inner,
            },
        ),
        (
            "hash",
            PhysOp::HashJoin {
                outer: seq("a"),
                inner: seq("b"),
                outer_key_col: 1,
                inner_key_col: 1,
                kind: JoinKind::Inner,
                residual: None,
            },
        ),
        (
            "inlj",
            PhysOp::IndexNestedLoopJoin {
                outer: seq("a"),
                inner_table: "b".into(),
                inner_index: "b_by_key".into(),
                outer_key_cols: vec![1],
            },
        ),
        (
            "merge",
            PhysOp::MergeJoin {
                left: sorted_seq("a", 1),
                right: sorted_seq("b", 1),
                left_key_col: 1,
                right_key_col: 1,
            },
        ),
    ];

    // Ground truth from Volcano NLJ: a_key ∈ {1 (×2 fan-out), 2} → 3 rows.
    // NULL, 5_000_000_000 (exceeds i32), and 9 match nothing.
    let truth = run_sorted(&env, &ExecModel::Volcano, &plans[0].1);
    assert_eq!(truth.len(), 3, "NLJ ground truth:\n{:?}", truth);

    for model in &[ExecModel::Volcano, ExecModel::Push] {
        for (name, plan) in &plans {
            let rows = run_sorted(&env, model, plan);
            assert_eq!(
                rows,
                truth,
                "{}/{} diverges from Volcano NLJ",
                name,
                model.name()
            );
        }
    }
}

// H3b — the two outer-capable algorithms must agree on the SAME LEFT OUTER
// join with a COMPOUND ON. The equi conjunct `a_key = b_key` drives the
// hash key; the residual `b_key = 1` is the extra conjunct — NLJ-outer runs
// the whole ON per pair, hash-outer runs the equi via the hash table and the
// residual per candidate. Both must null-pad identically. This one query
// exercises every outer path at once:
//   a1 (key 1): two b rows with b_key 1 pass the residual → fan-out (2 rows)
//   a2 (key 2): its candidate (b_key 2) FAILS the residual → padded
//   a3 (NULL) : NULL key matches nothing → padded
//   a4 (5e9)  : out-of-i32-range, no candidate → padded
//   a5 (9)    : no candidate → padded
// Forcing the two algorithms directly (not via the planner) is the mechanism
// the spec allows: the heuristic would pick hash-outer for this equi ON, so
// direct construction is the only way to pin NLJ-outer against it.
#[test]
fn outer_nlj_and_hash_agree_on_compound_on() {
    let env = setup();

    // Global columns of `a || b`: a_id 0, a_key 1, b_id 2, b_key 3.
    let equi = || Predicate::Compare {
        op: CompareOp::Eq,
        left: Expression::Column(1),
        right: Expression::Column(3),
    };
    let residual = || Predicate::Compare {
        op: CompareOp::Eq,
        left: Expression::Column(3),
        right: Expression::Literal(Value::Int32(1)),
    };

    // NLJ-outer: the FULL ON (`a_key = b_key AND b_key = 1`) per pair (R1).
    let nlj_outer = PhysOp::NestedLoopJoin {
        outer: seq("a"),
        inner: seq("b"),
        on: Some(Predicate::And(Box::new(equi()), Box::new(residual()))),
        kind: JoinKind::LeftOuter,
    };
    // Hash-outer: equi via the hash key, `b_key = 1` as the residual ON.
    let hash_outer = PhysOp::HashJoin {
        outer: seq("a"),
        inner: seq("b"),
        outer_key_col: 1,
        inner_key_col: 1,
        kind: JoinKind::LeftOuter,
        residual: Some(residual()),
    };

    // Ground truth: one fan-out group of 2 (a1) + four padded lefts = 6 rows.
    let truth = run_sorted(&env, &ExecModel::Volcano, &nlj_outer);
    assert_eq!(truth.len(), 6, "outer ground truth:\n{:?}", truth);
    // Exactly four rows are padded (b side all NULL): a2, a3, a4, a5.
    let padded = truth
        .iter()
        .filter(|r| r[2] == Value::Null && r[3] == Value::Null)
        .count();
    assert_eq!(padded, 4, "four unmatched lefts must pad:\n{:?}", truth);

    for model in &[ExecModel::Volcano, ExecModel::Push] {
        for (name, plan) in [("nlj-outer", &nlj_outer), ("hash-outer", &hash_outer)] {
            let rows = run_sorted(&env, model, plan);
            assert_eq!(rows, truth, "{}/{} diverges", name, model.name());
        }
    }
}
