//! `Session<E>` — user-facing SQL entry point.
//!
//! Owns a `Database<E>` (for txn lifecycle) and a `Catalog<E>` (for schema
//! lookup). One `Session` per connection. `execute(sql)` does the full
//! parse → bind → plan → run pipeline and returns a `QueryResult`.
//!
//! ## Transaction lifecycle
//!
//! - `BEGIN` / `COMMIT` / `ROLLBACK` toggle `current_txn`.
//! - Writes (INSERT/UPDATE/DELETE) outside an explicit txn are wrapped in
//!   an implicit single-statement transaction — auto-begin on entry,
//!   auto-commit on Ok, auto-abort on Err.
//! - Reads (SELECT) outside an explicit txn use the `AUTO_COMMIT` TxnEngine
//!   handle — cheap, no WAL begin/commit pair.
//!
//! ## What's NOT here (yet)
//!
//! - Multi-statement `execute("a; b")` — single statement per call.
//!   Callers wanting batches loop.
//! - Workload log — Step 8.
//!
//! ## Prepared statements (P13.7)
//!
//! `Session::prepare(sql)` parses + binds once, returning a
//! `PreparedStatement` that holds the resulting `LogicalPlan`. Every
//! call to `Session::execute_prepared(ps, params)` substitutes the
//! parameter values, re-plans (cheap — no parsing/binding), and runs.
//!
//! Parameter placeholders use `$1`, `$2`, … syntax (1-based). The
//! anonymous `?` form requires occurrence-order tracking in the binder
//! and is deferred — Phase 14 territory.

use std::sync::Arc;

use crate::catalog::{Catalog, ColumnDef, Schema};
use crate::common::{Error, Result};
use crate::database::Database;
use crate::execution::{ExecModel, Tuple};
use crate::sql::binder::Binder;
use crate::sql::frontend::parse;
use crate::sql::ir::logical::LogicalPlan;
use crate::sql::planner::{PhysicalPlan, Planner};
use crate::sql::workload_log::WorkloadLog;
use crate::storage::StorageEngine;
use crate::txn::{TxnId, TxnMode};
use crate::types::Value;

/// A parsed + bound SQL statement, ready for repeated execution with
/// different parameter bindings. Returned by `Session::prepare`.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub logical: LogicalPlan,
}

/// Outcome of a single `Session::execute` call.
#[derive(Debug)]
pub enum QueryResult {
    /// SELECT result. `schema` describes the columns of `rows`.
    Rows { schema: Schema, rows: Vec<Tuple> },
    /// Row count from INSERT / UPDATE / DELETE.
    Affected(u64),
    /// DDL or transaction-control acknowledgment.
    Ack,
    /// EXPLAIN plan as a rendered tree.
    Explain(String),
}

pub struct Session<E: StorageEngine + 'static> {
    database: Arc<Database<E>>,
    catalog: Arc<Catalog<E>>,
    binder: Binder<E>,
    current_txn: Option<TxnId>,
    log: Option<Arc<WorkloadLog>>,
    planner: Planner,
    execution_model: ExecModel,
}

impl<E: StorageEngine + 'static> Session<E> {
    pub fn new(database: Arc<Database<E>>, catalog: Arc<Catalog<E>>) -> Self {
        let binder = Binder::new(catalog.clone());
        Self {
            database,
            catalog,
            binder,
            current_txn: None,
            log: None,
            planner: Planner::default(),
            execution_model: ExecModel::default(),
        }
    }

    /// Swap the planner strategy for subsequent statements. Defaults to
    /// rule-based; the Phase 16/18 harness uses this to run the same
    /// workload through each `PlannerStrategy`. (Future: a `SET planner`
    /// pragma drives this.)
    pub fn set_planner(&mut self, planner: Planner) {
        self.planner = planner;
    }

    /// Name of the active planner strategy.
    pub fn planner_name(&self) -> &'static str {
        self.planner.name()
    }

    /// Swap the execution model (Volcano pull vs. push) for subsequent
    /// statements. Defaults to Volcano. Both produce identical results; the
    /// Phase 16 harness uses this to benchmark the two.
    pub fn set_execution_model(&mut self, model: ExecModel) {
        self.execution_model = model;
    }

    /// Name of the active execution model.
    pub fn execution_model_name(&self) -> &'static str {
        self.execution_model.name()
    }

    /// Attach a workload log. Each subsequent `execute` call appends one
    /// entry before parsing — captures intent including malformed SQL.
    pub fn with_log(mut self, log: Arc<WorkloadLog>) -> Self {
        self.log = Some(log);
        self
    }

    /// Run a single SQL statement.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        if let Some(ref log) = self.log {
            if let Err(e) = log.append(sql) {
                // Best-effort logging. Disk-full / IO errors shouldn't
                // crash a working session; surface to stderr and move on.
                eprintln!("workload log append failed: {}", e);
            }
        }
        let stmts = parse(sql)?;
        if stmts.is_empty() {
            return Ok(QueryResult::Ack);
        }
        if stmts.len() > 1 {
            return Err(Error::SqlParse(
                "session: multiple statements per execute() not supported".into(),
            ));
        }
        let stmt = stmts.into_iter().next().unwrap();
        let logical = self.binder.bind(stmt)?;
        self.execute_logical(logical)
    }

    /// Whether the session currently has an explicit transaction open.
    pub fn in_transaction(&self) -> bool {
        self.current_txn.is_some()
    }

    /// P13.7: Parse + bind a SQL statement, returning a
    /// `PreparedStatement` that can be executed many times with
    /// different parameter values. Parsing and binding are paid
    /// once; each `execute_prepared` call substitutes parameters and
    /// re-plans against the current catalog state.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let stmts = parse(sql)?;
        if stmts.is_empty() {
            return Err(Error::SqlParse("prepare: empty SQL".into()));
        }
        if stmts.len() > 1 {
            return Err(Error::SqlParse(
                "prepare: multiple statements per prepare() not supported".into(),
            ));
        }
        let stmt = stmts.into_iter().next().unwrap();
        let logical = self.binder.bind(stmt)?;
        Ok(PreparedStatement { logical })
    }

    /// Execute a previously-prepared statement with bound parameters.
    /// `params[i]` substitutes for `$(i+1)` placeholders in the SQL.
    pub fn execute_prepared(
        &mut self,
        ps: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let logical = ps.logical.clone().substitute_params(params)?;
        self.execute_logical(logical)
    }

    // -----------------------------------------------------------------------
    // Logical dispatch
    // -----------------------------------------------------------------------

    fn execute_logical(&mut self, logical: LogicalPlan) -> Result<QueryResult> {
        match logical {
            LogicalPlan::BeginTxn => self.handle_begin(),
            LogicalPlan::CommitTxn => self.handle_commit(),
            LogicalPlan::AbortTxn => self.handle_abort(),
            LogicalPlan::CreateTable {
                name,
                columns,
                primary_key,
            } => self.handle_create_table(name, columns, primary_key),
            LogicalPlan::CreateIndex {
                name,
                table,
                columns,
                unique,
                backend,
            } => self.handle_create_index(name, table, columns, unique, backend),
            LogicalPlan::Analyze { table } => self.handle_analyze(table),
            // Everything else (Select/Insert/Update/Delete/Explain) needs
            // an engine handle and possibly an implicit txn.
            other => self.execute_engine_bound(other),
        }
    }

    fn execute_engine_bound(&mut self, logical: LogicalPlan) -> Result<QueryResult> {
        let is_select_shape = matches!(&logical, LogicalPlan::Select { .. });
        let needs_write = is_write(&logical);

        // Determine the txn the engine handle is bound to.
        let (txn_id, implicit) = match self.current_txn {
            Some(t) => (t, false),
            None if needs_write => {
                let t = self.database.begin_txn(TxnMode::ReadWrite)?;
                (t, true)
            }
            None => (TxnId::AUTO_COMMIT, false),
        };

        let engine_handle = match self.database.txn_engine_handle(txn_id) {
            Ok(h) => Arc::new(h),
            Err(e) => {
                if implicit {
                    let _ = self.database.txn_abort(txn_id);
                }
                return Err(e);
            }
        };

        let physical = match self.planner.plan(logical, &self.catalog) {
            Ok(p) => p,
            Err(e) => {
                if implicit {
                    let _ = self.database.txn_abort(txn_id);
                }
                return Err(e);
            }
        };

        let result = self.run_physical(physical, is_select_shape, &engine_handle);

        if implicit {
            match &result {
                Ok(_) => self.database.commit_txn(txn_id)?,
                Err(_) => {
                    let _ = self.database.txn_abort(txn_id);
                }
            }
        }
        result
    }

    fn run_physical<H>(
        &self,
        physical: PhysicalPlan,
        is_select_shape: bool,
        engine: &Arc<H>,
    ) -> Result<QueryResult>
    where
        H: StorageEngine + 'static,
    {
        match physical {
            PhysicalPlan::Query(physop) => {
                let (schema, rows) =
                    self.execution_model
                        .execute(&physop, engine, &self.catalog)?;
                if is_select_shape {
                    Ok(QueryResult::Rows { schema, rows })
                } else {
                    // DML — yields exactly one [Int64(count)] tuple.
                    let count = extract_count(rows)?;
                    Ok(QueryResult::Affected(count))
                }
            }
            PhysicalPlan::Explain(text) => Ok(QueryResult::Explain(text)),
            // DDL / TC variants are handled in execute_logical before plan()
            // is even called — they shouldn't reach here.
            other => Err(Error::SqlParse(format!(
                "session: unexpected physical plan in run_physical: {:?}",
                kind_name(&other)
            ))),
        }
    }

    // -----------------------------------------------------------------------
    // Transaction control
    // -----------------------------------------------------------------------

    fn handle_begin(&mut self) -> Result<QueryResult> {
        if self.current_txn.is_some() {
            return Err(Error::SqlParse(
                "session: nested transactions not supported".into(),
            ));
        }
        let t = self.database.begin_txn(TxnMode::ReadWrite)?;
        self.current_txn = Some(t);
        Ok(QueryResult::Ack)
    }

    fn handle_commit(&mut self) -> Result<QueryResult> {
        let t = self
            .current_txn
            .ok_or_else(|| Error::SqlParse("session: COMMIT with no active txn".into()))?;
        self.database.commit_txn(t)?;
        self.current_txn = None;
        Ok(QueryResult::Ack)
    }

    fn handle_abort(&mut self) -> Result<QueryResult> {
        let t = self
            .current_txn
            .ok_or_else(|| Error::SqlParse("session: ROLLBACK with no active txn".into()))?;
        self.database.txn_abort(t)?;
        self.current_txn = None;
        Ok(QueryResult::Ack)
    }

    // -----------------------------------------------------------------------
    // DDL
    // -----------------------------------------------------------------------

    fn handle_create_table(
        &self,
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Vec<usize>,
    ) -> Result<QueryResult> {
        // Catalog assigns the real `table_id` — we pass 0 as a placeholder.
        let schema = Schema {
            name: name.clone(),
            table_id: crate::catalog::TableId(0),
            columns,
            primary_key,
        };
        self.catalog.create_table(name, schema)?;
        Ok(QueryResult::Ack)
    }

    /// P14.2: ANALYZE TABLE. Single full-table scan that materializes
    /// row count + per-column NDV + null counts + equi-width histograms
    /// for Int32/Int64 columns. Persists everything via the catalog's
    /// stats accessors so the planner (P14.7) can read it back.
    ///
    /// Reads at AUTO_COMMIT — ANALYZE is observational; no implicit
    /// txn needed and stat rows write straight to the engine.
    /// `CREATE INDEX`: register the index with the catalog (which builds
    /// its engine via the injected opener), then backfill entries for
    /// every row that existed before the index did — scanned at the
    /// session's transaction scope, same as SELECT. Rows arriving later
    /// are maintained by the normal mutation paths.
    fn handle_create_index(
        &mut self,
        name: String,
        table: String,
        columns: Vec<usize>,
        unique: bool,
        backend: crate::catalog::IndexBackend,
    ) -> Result<QueryResult> {
        let schema = self.catalog.get_table(&table)?;
        let txn_id = match self.current_txn {
            Some(t) => t,
            None => crate::txn::TxnId::AUTO_COMMIT,
        };
        let txn_engine = self.database.txn_engine_handle(txn_id)?;
        if unique {
            // No DROP INDEX exists yet, so duplicate detection must run
            // BEFORE registration — a failing backfill must not strand a
            // half-created index.
            let plain = crate::table::Table::new(
                Arc::new(self.database.txn_engine_handle(txn_id)?),
                schema.clone(),
                crate::layout::RowLayout,
            );
            plain.assert_unique_feasible(&columns, &name)?;
        }
        let index_id = self.catalog.create_index(crate::catalog::IndexDef {
            name,
            table_id: schema.table_id,
            columns,
            unique,
            backend,
        })?;

        let handles = self.catalog.indexes_for_table(schema.table_id, &schema)?;
        let new_index: Vec<_> = handles.into_iter().filter(|h| h.id == index_id).collect();
        assert_eq!(
            new_index.len(),
            1,
            "created index must resolve to exactly one handle"
        );
        let backfill_table = crate::table::Table::with_indexes(
            Arc::new(txn_engine),
            schema,
            crate::layout::RowLayout,
            new_index,
        );
        backfill_table.backfill_indexes()?;

        // Same durability posture as ANALYZE: catalog/index writes don't
        // go through the WAL, so flush before returning.
        self.catalog.engine().flush()?;
        Ok(QueryResult::Ack)
    }

    fn handle_analyze(&mut self, table_name: String) -> Result<QueryResult> {
        // Read through the MVCC layer at AUTO_COMMIT so we see committed
        // user rows (not raw versioned engine bytes) — same pattern as
        // SELECT. The stats computation itself lives with the planner's
        // stats machinery: `sql::optimizer::stats::analyze_table`.
        let txn_id = match self.current_txn {
            Some(t) => t,
            None => crate::txn::TxnId::AUTO_COMMIT,
        };
        let txn_engine = self.database.txn_engine_handle(txn_id)?;
        crate::sql::optimizer::stats::analyze_table(&self.catalog, &txn_engine, &table_name)?;
        Ok(QueryResult::Ack)
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn is_write(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. } => {
            true
        }
        // EXPLAIN is read-only at the storage level; it just builds the
        // executor tree for description. Even EXPLAIN INSERT doesn't insert.
        LogicalPlan::Explain(_) => false,
        _ => false,
    }
}

fn extract_count(rows: Vec<Tuple>) -> Result<u64> {
    let first = rows
        .into_iter()
        .next()
        .ok_or_else(|| Error::SqlParse("session: DML produced no count tuple".into()))?;
    let v = first
        .into_iter()
        .next()
        .ok_or_else(|| Error::SqlParse("session: DML count tuple is empty".into()))?;
    match v {
        Value::Int64(n) if n >= 0 => Ok(n as u64),
        other => Err(Error::SqlParse(format!(
            "session: DML count not Int64: {:?}",
            other
        ))),
    }
}

fn kind_name(p: &PhysicalPlan) -> &'static str {
    match p {
        PhysicalPlan::Query(_) => "Query",
        PhysicalPlan::CreateTable { .. } => "CreateTable",
        PhysicalPlan::CreateIndex { .. } => "CreateIndex",
        PhysicalPlan::Analyze { .. } => "Analyze",
        PhysicalPlan::BeginTxn => "BeginTxn",
        PhysicalPlan::CommitTxn => "CommitTxn",
        PhysicalPlan::AbortTxn => "AbortTxn",
        PhysicalPlan::Explain(_) => "Explain",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::engines::btree::BTreeEngine;
    use crate::storage::FileDiskManager;
    use tempfile::TempDir;

    struct TestSession {
        session: Session<BTreeEngine>,
        _dir: TempDir,
    }

    fn setup() -> TestSession {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&db_path).unwrap();
        let bpm = BufferPoolManager::new(512, dm);
        let engine = BTreeEngine::new(bpm).unwrap();
        let database = Arc::new(Database::open(dir.path(), engine).unwrap());
        let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
        let session = Session::new(database, catalog);
        TestSession { session, _dir: dir }
    }

    fn count_rows(result: QueryResult) -> usize {
        match result {
            QueryResult::Rows { rows, .. } => rows.len(),
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    // ---- DDL ----

    #[test]
    fn session_create_table_then_describe_via_select() {
        let mut s = setup();
        let ack = s
            .session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))")
            .unwrap();
        assert!(matches!(ack, QueryResult::Ack));

        // SELECT against empty table works.
        let r = s.session.execute("SELECT * FROM t").unwrap();
        assert_eq!(count_rows(r), 0);
    }

    // ---- Auto-commit INSERT + SELECT ----

    #[test]
    fn session_implicit_txn_for_insert_is_visible_after() {
        let mut s = setup();
        s.session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, n BIGINT NOT NULL)")
            .unwrap();
        let r = s
            .session
            .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
            .unwrap();
        match r {
            QueryResult::Affected(n) => assert_eq!(n, 2),
            other => panic!("expected Affected(2), got {:?}", other),
        }

        // SELECT confirms the insert is visible.
        let sel = s.session.execute("SELECT * FROM t").unwrap();
        assert_eq!(count_rows(sel), 2);
    }

    // ---- Explicit BEGIN/COMMIT ----

    #[test]
    fn session_explicit_txn_commit_persists() {
        let mut s = setup();
        s.session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, n BIGINT NOT NULL)")
            .unwrap();

        s.session.execute("BEGIN").unwrap();
        assert!(s.session.in_transaction());
        s.session.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        s.session.execute("COMMIT").unwrap();
        assert!(!s.session.in_transaction());

        let sel = s.session.execute("SELECT * FROM t").unwrap();
        assert_eq!(count_rows(sel), 1);
    }

    #[test]
    fn session_rollback_discards_writes() {
        let mut s = setup();
        s.session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, n BIGINT NOT NULL)")
            .unwrap();

        s.session.execute("BEGIN").unwrap();
        s.session.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        s.session.execute("ROLLBACK").unwrap();
        assert!(!s.session.in_transaction());

        let sel = s.session.execute("SELECT * FROM t").unwrap();
        assert_eq!(count_rows(sel), 0, "ROLLBACK must discard the insert");
    }

    // ---- TPC-C-shaped UPDATE end-to-end ----

    #[test]
    fn session_payment_style_update_persists() {
        let mut s = setup();
        s.session
            .execute(
                "CREATE TABLE warehouse (w_id INT NOT NULL, w_ytd BIGINT NOT NULL, PRIMARY KEY (w_id))",
            )
            .unwrap();
        s.session
            .execute("INSERT INTO warehouse VALUES (1, 1000), (2, 2000)")
            .unwrap();

        let r = s
            .session
            .execute("UPDATE warehouse SET w_ytd = w_ytd + 100 WHERE w_id = 1")
            .unwrap();
        match r {
            QueryResult::Affected(n) => assert_eq!(n, 1),
            other => panic!("expected Affected(1), got {:?}", other),
        }

        let sel = s
            .session
            .execute("SELECT w_ytd FROM warehouse WHERE w_id = 1")
            .unwrap();
        match sel {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Int64(1100));
            }
            other => panic!("got {:?}", other),
        }

        // The non-matching row is untouched.
        let sel2 = s
            .session
            .execute("SELECT w_ytd FROM warehouse WHERE w_id = 2")
            .unwrap();
        match sel2 {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::Int64(2000));
            }
            _ => panic!(),
        }
    }

    // ---- DELETE ----

    #[test]
    fn session_delete_removes_filtered_rows() {
        let mut s = setup();
        s.session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, n BIGINT NOT NULL)")
            .unwrap();
        s.session
            .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();

        let r = s.session.execute("DELETE FROM t WHERE id = 2").unwrap();
        assert!(matches!(r, QueryResult::Affected(1)));

        let sel = s.session.execute("SELECT * FROM t").unwrap();
        assert_eq!(count_rows(sel), 2);
    }

    // ---- EXPLAIN ----

    #[test]
    fn session_explain_returns_plan_string() {
        let mut s = setup();
        s.session
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .unwrap();
        let r = s
            .session
            .execute("EXPLAIN SELECT * FROM t WHERE id = 1")
            .unwrap();
        match r {
            QueryResult::Explain(text) => {
                assert!(text.contains("PkLookup(t)"));
            }
            other => panic!("expected Explain, got {:?}", other),
        }
    }

    // ---- Error paths ----

    #[test]
    fn session_commit_without_begin_errors() {
        let mut s = setup();
        let err = s.session.execute("COMMIT").unwrap_err();
        assert!(matches!(err, Error::SqlParse(ref m) if m.contains("no active")));
    }

    #[test]
    fn session_nested_begin_errors() {
        let mut s = setup();
        s.session.execute("BEGIN").unwrap();
        let err = s.session.execute("BEGIN").unwrap_err();
        assert!(matches!(err, Error::SqlParse(ref m) if m.contains("nested")));
    }

    #[test]
    fn session_multiple_statements_per_execute_rejected() {
        let mut s = setup();
        let err = s.session.execute("SELECT 1; SELECT 2").unwrap_err();
        assert!(matches!(err, Error::SqlParse(ref m) if m.contains("multiple")));
    }

    #[test]
    fn session_empty_input_returns_ack() {
        let mut s = setup();
        let r = s.session.execute("").unwrap();
        assert!(matches!(r, QueryResult::Ack));
    }
}
