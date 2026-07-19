//! sqllogictest harness: SQL correctness as declarative `.slt` data files
//! under `tests/slt/`, run against a full `Session` on the default tier
//! (memory engine + WAL `NoSync`). Adopted from production practice
//! (RisingWave / Databend / DataFusion all test SQL through
//! `sqllogictest-rs`; see `docs/build-times/production-practice.md`) —
//! editing a SQL test no longer recompiles anything, and the same corpus
//! can later run against every engine config.
//!
//! One `#[test]` per `.slt` file keeps failures addressable by name.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::{ColumnType, Value};
use interchangedb::wal::SyncMode;
use interchangedb::Database;
use sqllogictest::{DBOutput, DefaultColumnType};

struct SltDb {
    session: Session<BTreeEngine>,
    _dir: tempfile::TempDir,
}

fn slt_db() -> SltDb {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database =
        Arc::new(Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap());
    let catalog = Arc::new(
        Catalog::open_persistent(
            database.engine_arc().clone(),
            dir.path().join("indexes"),
            interchangedb::default_index_opener(),
        )
        .unwrap(),
    );
    SltDb {
        session: Session::new(database, catalog),
        _dir: dir,
    }
}

/// slt cell rendering. The conventions are sqllogictest's: `NULL` for SQL
/// NULL, `(empty)` for the empty string (so a blank cell is never
/// ambiguous), everything else in its canonical text form.
fn value_text(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Varchar(s) | Value::Char(s) => {
            if s.is_empty() {
                "(empty)".to_string()
            } else {
                s.clone()
            }
        }
        Value::Bytes(b) => format!("{b:02x?}"),
        Value::Timestamp(us) => us.to_string(),
        // Zero-padded `YYYY-MM-DD` from the day-count (see `types::civil`).
        Value::Date(days) => {
            let (y, m, d) = interchangedb::types::civil::ymd_from_days(*days);
            format!("{y:04}-{m:02}-{d:02}")
        }
        // mantissa * 10^(-scale), rendered with an explicit decimal point.
        Value::Decimal(d) => {
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
    }
}

fn column_code(ty: &ColumnType) -> DefaultColumnType {
    match ty {
        ColumnType::Int32 | ColumnType::Int64 => DefaultColumnType::Integer,
        // Date renders as `YYYY-MM-DD` text, so it takes the text column code.
        ColumnType::Varchar(_) | ColumnType::Char(_) | ColumnType::Date => DefaultColumnType::Text,
        _ => DefaultColumnType::Any,
    }
}

impl sqllogictest::DB for SltDb {
    type Error = interchangedb::Error;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, Self::Error> {
        match self.session.execute(sql)? {
            QueryResult::Rows { schema, rows } => Ok(DBOutput::Rows {
                types: schema.columns.iter().map(|c| column_code(&c.ty)).collect(),
                rows: rows
                    .iter()
                    .map(|r| r.iter().map(value_text).collect())
                    .collect(),
            }),
            QueryResult::Affected(n) => Ok(DBOutput::StatementComplete(n)),
            QueryResult::Ack => Ok(DBOutput::StatementComplete(0)),
            QueryResult::Explain(text) => Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: text.lines().map(|l| vec![l.trim().to_string()]).collect(),
            }),
        }
    }
}

fn run_slt(name: &str) {
    let path = format!("{}/tests/slt/{}", env!("CARGO_MANIFEST_DIR"), name);
    let mut runner = sqllogictest::Runner::new(|| async { Ok(slt_db()) });
    futures::executor::block_on(runner.run_file_async(path)).unwrap();
}

#[test]
fn slt_order_by() {
    run_slt("order_by.slt");
}

#[test]
fn slt_aggregate() {
    run_slt("aggregate.slt");
}

#[test]
fn slt_e2e() {
    run_slt("e2e.slt");
}

#[test]
fn slt_create_index() {
    run_slt("create_index.slt");
}

#[test]
fn slt_join() {
    run_slt("join.slt");
}

#[test]
fn slt_outer_join() {
    run_slt("outer_join.slt");
}

#[test]
fn slt_unique() {
    run_slt("unique.slt");
}

#[test]
fn slt_insert() {
    run_slt("insert.slt");
}

#[test]
fn slt_group_by() {
    run_slt("group_by.slt");
}

#[test]
fn slt_aggregate_expr() {
    run_slt("aggregate_expr.slt");
}

#[test]
fn slt_computed_select() {
    run_slt("computed_select.slt");
}

#[test]
fn slt_date() {
    run_slt("date.slt");
}

#[test]
fn slt_scalar() {
    run_slt("scalar.slt");
}

#[test]
fn slt_derived() {
    run_slt("derived.slt");
}

#[test]
fn slt_subquery() {
    run_slt("subquery.slt");
}

#[test]
fn slt_tpch_subquery() {
    run_slt("tpch_subquery.slt");
}

#[test]
fn slt_tpch_q11() {
    run_slt("tpch_q11.slt");
}

#[test]
fn slt_tpch_q15() {
    run_slt("tpch_q15.slt");
}
