//! Shared test fixtures for the memo planner's unit tests (test builds
//! only — the module is `#[cfg(test)]`-gated in `mod.rs`).
//!
//! Two kinds of fixture:
//!   - a catalog-backed [`Env`] (wh / dist / cust tables, `dist_by_w`
//!     index) for tests that exercise binding, leaf lowering, and stats
//!     wiring end to end;
//!   - hand-built [`NormalizedQuery`] pieces ([`rel`] / [`edge`] /
//!     [`query`]) for tests that exercise pure graph shape and cost
//!     arithmetic without a catalog.

use std::sync::Arc;

use tempfile::TempDir;

use crate::buffer::BufferPoolManager;
use crate::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use crate::engines::btree::BTreeEngine;
use crate::sql::binder::Binder;
use crate::sql::frontend::parse;
use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
use crate::sql::ir::logical::LogicalPlan;
use crate::sql::optimizer::join_order::RelId;
use crate::sql::optimizer::memo::normalize::{
    Edge, LocalPred, NormalizedQuery, RelInfo, SpineParts,
};
use crate::sql::optimizer::stats::{MockStatsProvider, QueryStats};
use crate::storage::FileDiskManager;
use crate::types::{ColumnType, Value};

pub(crate) struct Env {
    pub(crate) catalog: Arc<Catalog<BTreeEngine>>,
    pub(crate) binder: Binder<BTreeEngine>,
    pub(crate) wh_id: TableId,
    pub(crate) dist_id: TableId,
    pub(crate) _dir: TempDir,
}

pub(crate) fn int32_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty: ColumnType::Int32,
        nullable: false,
        default: None,
    }
}

pub(crate) fn int64_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty: ColumnType::Int64,
        nullable: false,
        default: None,
    }
}

/// Three-table fixture: wh(w_id PK, w_ytd), dist(d_id PK, d_w_id
/// [indexed], d_ytd), cust(c_id PK, c_d_id). Widths 2/3/2 make the
/// tuple-global → local rebasing visible in assertions.
pub(crate) fn setup() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("t.db")).unwrap();
    let bpm = BufferPoolManager::new(256, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let catalog =
        Arc::new(Catalog::open_persistent(engine.clone(), dir.path().join("indexes")).unwrap());

    let wh_id = catalog
        .create_table(
            "wh".into(),
            Schema {
                name: "wh".into(),
                table_id: TableId(0),
                columns: vec![int32_col("w_id"), int64_col("w_ytd")],
                primary_key: vec![0],
            },
        )
        .unwrap();
    let dist_id = catalog
        .create_table(
            "dist".into(),
            Schema {
                name: "dist".into(),
                table_id: TableId(0),
                columns: vec![int32_col("d_id"), int32_col("d_w_id"), int64_col("d_ytd")],
                primary_key: vec![0],
            },
        )
        .unwrap();
    catalog
        .create_index(IndexDef {
            name: "dist_by_w".into(),
            table_id: dist_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();
    catalog
        .create_table(
            "cust".into(),
            Schema {
                name: "cust".into(),
                table_id: TableId(0),
                columns: vec![int32_col("c_id"), int32_col("c_d_id")],
                primary_key: vec![0],
            },
        )
        .unwrap();

    let binder = Binder::new(catalog.clone());
    Env {
        catalog,
        binder,
        wh_id,
        dist_id,
        _dir: dir,
    }
}

pub(crate) fn bind(env: &Env, sql: &str) -> LogicalPlan {
    let stmts = parse(sql).unwrap();
    env.binder.bind(stmts.into_iter().next().unwrap()).unwrap()
}

/// Empty snapshot — every accessor answers with its fallback default
/// (row_count = DEFAULT_ROW_COUNT, ndv = 0), like an un-ANALYZEd DB.
pub(crate) fn default_stats() -> QueryStats {
    QueryStats::gather(&MockStatsProvider::new(), &[]).unwrap()
}

/// Minimal single-column relation — hand-built fixtures exercise graph
/// shape and cost arithmetic, never the schema or predicate content, so
/// one Int32 column and a placeholder conjunct carrying
/// `local_selectivity` suffice. No primary key and no indexes, so
/// access-path lowering is structurally impossible (review fix #5:
/// SeqScan-only reference models can't be surprised by a future
/// widening of the lowering rules).
pub(crate) fn rel(rows: f64, local_selectivity: f64) -> RelInfo {
    let local_preds = if local_selectivity < 1.0 {
        vec![LocalPred {
            pred: Predicate::Compare {
                op: CompareOp::Lt,
                left: Expression::Column(0),
                right: Expression::Literal(Value::Int32(0)),
            },
            selectivity: local_selectivity,
        }]
    } else {
        Vec::new()
    };
    RelInfo {
        name: "r".into(),
        schema: Arc::new(Schema {
            name: "r".into(),
            table_id: TableId(0),
            columns: vec![int32_col("c")],
            primary_key: vec![],
        }),
        indexes: Vec::new(),
        local_preds,
        row_count: rows,
    }
}

pub(crate) fn edge(left_rel: RelId, right_rel: RelId, selectivity: f64) -> Edge {
    Edge {
        left_rel,
        right_rel,
        left_col: 0,
        right_col: 0,
        left_indexed: false,
        right_indexed: false,
        selectivity,
    }
}

pub(crate) fn query(relations: Vec<RelInfo>, edges: Vec<Edge>) -> NormalizedQuery {
    NormalizedQuery {
        relations,
        edges,
        residual: Vec::new(),
        spine: SpineParts {
            projection: Vec::new(),
            aggregates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        },
    }
}
