//! Physical planner — `LogicalPlan` → `PhysicalPlan`.
//!
//! Rule-based: each LogicalPlan variant maps deterministically to one
//! shape of executor tree (or to a non-executable descriptor for DDL /
//! transaction control). No cost model, no statistics, no choice
//! between alternative plans. Phase 14 (Selinger) and Phase 17/18
//! (Cascades) will replace this with cost-based selection.
//!
//! Tree shapes produced:
//!
//!   SELECT → SeqScan → [Filter] → [Projection] → [Limit]
//!   INSERT → Insert
//!   UPDATE → Update wrapping (SeqScan → [Filter])
//!   DELETE → Delete wrapping (SeqScan → [Filter])
//!
//! `[ … ]` brackets indicate optional wrappers — emitted only when the
//! corresponding clause is present in the logical plan.

use std::sync::Arc;

use crate::catalog::{Catalog, ColumnDef};
use crate::common::Result;
use crate::execution::{Delete, Executor, Filter, Insert, Limit, Projection, SeqScan, SetExpr, Update};
use crate::layout::RowLayout;
use crate::sql::expr::Predicate;
use crate::sql::logical::LogicalPlan;
use crate::storage::StorageEngine;
use crate::table::Table;

/// Output of the planner: either an executable operator tree, a
/// descriptor for a side-effect-only plan, or an EXPLAIN string.
pub enum PhysicalPlan {
    Executor(Box<dyn Executor>),
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Vec<usize>,
    },
    BeginTxn,
    CommitTxn,
    AbortTxn,
    Explain(String),
}

/// Plan a single logical statement.
///
/// `engine` is the storage handle DML operators read/write through —
/// in production this is `Arc<TxnEngine<E>>` so MVCC + locking apply.
/// `catalog` is the schema lookup source. They're independent generic
/// params: the catalog uses raw storage (DDL is non-transactional in
/// V2), while DML wraps that same raw storage in a TxnEngine handle.
pub fn plan<TblE, CatE>(
    logical: LogicalPlan,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<PhysicalPlan>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    match logical {
        LogicalPlan::CreateTable { name, columns, primary_key } => {
            Ok(PhysicalPlan::CreateTable { name, columns, primary_key })
        }
        LogicalPlan::BeginTxn => Ok(PhysicalPlan::BeginTxn),
        LogicalPlan::CommitTxn => Ok(PhysicalPlan::CommitTxn),
        LogicalPlan::AbortTxn => Ok(PhysicalPlan::AbortTxn),
        LogicalPlan::Explain(inner) => {
            let inner_phys = plan(*inner, engine, catalog)?;
            Ok(PhysicalPlan::Explain(render_explain(&inner_phys)))
        }
        LogicalPlan::Select { table, projection, filter, limit } => {
            let exec = plan_select(table, projection, filter, limit, engine, catalog)?;
            Ok(PhysicalPlan::Executor(exec))
        }
        LogicalPlan::Insert { table, rows } => {
            let exec = plan_insert(table, rows, engine, catalog)?;
            Ok(PhysicalPlan::Executor(exec))
        }
        LogicalPlan::Update { table, set_clauses, filter } => {
            let exec = plan_update(table, set_clauses, filter, engine, catalog)?;
            Ok(PhysicalPlan::Executor(exec))
        }
        LogicalPlan::Delete { table, filter } => {
            let exec = plan_delete(table, filter, engine, catalog)?;
            Ok(PhysicalPlan::Executor(exec))
        }
    }
}

fn plan_select<TblE, CatE>(
    table_name: String,
    projection: Vec<usize>,
    filter: Option<Predicate>,
    limit: Option<usize>,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let schema = catalog.get_table(&table_name)?;
    let table = Arc::new(Table::new(engine, schema, RowLayout));

    let mut current: Box<dyn Executor> = Box::new(SeqScan::new(&*table)?);

    if let Some(pred) = filter {
        current = Box::new(Filter::from_boxed(current, pred.compile()));
    }
    if !projection.is_empty() {
        current = Box::new(Projection::new(current, projection)?);
    }
    if let Some(n) = limit {
        current = Box::new(Limit::new(current, n));
    }
    Ok(current)
}

fn plan_insert<TblE, CatE>(
    table_name: String,
    rows: Vec<Vec<crate::types::Value>>,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let schema = catalog.get_table(&table_name)?;
    let table = Arc::new(Table::new(engine, schema, RowLayout));
    Ok(Box::new(Insert::new(table, rows)))
}

fn plan_update<TblE, CatE>(
    table_name: String,
    set_clauses: Vec<(usize, crate::sql::expr::Expression)>,
    filter: Option<Predicate>,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let schema = catalog.get_table(&table_name)?;
    let table = Arc::new(Table::new(engine, schema, RowLayout));

    let mut child: Box<dyn Executor> = Box::new(SeqScan::new(&*table)?);
    if let Some(pred) = filter {
        child = Box::new(Filter::from_boxed(child, pred.compile()));
    }

    let set_exprs: Vec<SetExpr> = set_clauses
        .into_iter()
        .map(|(idx, expr)| (idx, expr.compile()))
        .collect();

    Ok(Box::new(Update::new(table, child, set_exprs)))
}

fn plan_delete<TblE, CatE>(
    table_name: String,
    filter: Option<Predicate>,
    engine: Arc<TblE>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    TblE: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let schema = catalog.get_table(&table_name)?;
    let table = Arc::new(Table::new(engine, schema, RowLayout));

    let mut child: Box<dyn Executor> = Box::new(SeqScan::new(&*table)?);
    if let Some(pred) = filter {
        child = Box::new(Filter::from_boxed(child, pred.compile()));
    }
    Ok(Box::new(Delete::new(table, child)))
}

/// Render an EXPLAIN string for a `PhysicalPlan`. For executor trees this
/// delegates to the operator's own `explain`; for descriptors it emits a
/// one-line summary.
fn render_explain(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Executor(exec) => exec.explain(0),
        PhysicalPlan::CreateTable { name, .. } => format!("CreateTable({})\n", name),
        PhysicalPlan::BeginTxn => "BeginTxn\n".to_string(),
        PhysicalPlan::CommitTxn => "CommitTxn\n".to_string(),
        PhysicalPlan::AbortTxn => "AbortTxn\n".to_string(),
        PhysicalPlan::Explain(_) => "Explain[nested]\n".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{Schema, TableId};
    use crate::common::Error;
    use crate::index::btree::BTreeEngine;
    use crate::sql::binder::Binder;
    use crate::sql::frontend::parse;
    use crate::storage::DiskManager;
    use crate::types::{ColumnType, Value};
    use tempfile::TempDir;

    struct TestEnv {
        engine: Arc<BTreeEngine>,
        catalog: Arc<Catalog<BTreeEngine>>,
        binder: Binder<BTreeEngine>,
        _dir: TempDir,
    }

    fn setup() -> TestEnv {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(512, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());
        let binder = Binder::new(catalog.clone());
        TestEnv { engine, catalog, binder, _dir: dir }
    }

    fn create_warehouse(env: &TestEnv) {
        let schema = Schema {
            name: "warehouse".into(),
            table_id: TableId(0),
            columns: vec![
                ColumnDef { name: "w_id".into(), ty: ColumnType::Int32, nullable: false, default: None },
                ColumnDef { name: "w_ytd".into(), ty: ColumnType::Int64, nullable: false, default: None },
            ],
            primary_key: vec![0],
        };
        env.catalog.create_table("warehouse".into(), schema).unwrap();
    }

    fn plan_sql(env: &TestEnv, sql: &str) -> PhysicalPlan {
        let stmts = parse(sql).unwrap();
        let logical = env.binder.bind(stmts.into_iter().next().unwrap()).unwrap();
        plan(logical, env.engine.clone(), &env.catalog).unwrap()
    }

    // ---- DDL / TC variants ----

    #[test]
    fn plans_create_table_to_descriptor() {
        let env = setup();
        let p = plan_sql(&env, "CREATE TABLE t (id INT PRIMARY KEY)");
        assert!(matches!(p, PhysicalPlan::CreateTable { .. }));
    }

    #[test]
    fn plans_transaction_control_to_descriptors() {
        let env = setup();
        assert!(matches!(plan_sql(&env, "BEGIN"), PhysicalPlan::BeginTxn));
        assert!(matches!(plan_sql(&env, "COMMIT"), PhysicalPlan::CommitTxn));
        assert!(matches!(plan_sql(&env, "ROLLBACK"), PhysicalPlan::AbortTxn));
    }

    // ---- SELECT shapes ----

    #[test]
    fn plans_select_star_to_seq_scan_only() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "SELECT * FROM warehouse");
        match p {
            PhysicalPlan::Executor(exec) => {
                let tree = exec.explain(0);
                assert_eq!(tree, "SeqScan(warehouse)\n");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_select_with_where_to_filter_chain() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "SELECT w_id FROM warehouse WHERE w_id = 1");
        match p {
            PhysicalPlan::Executor(exec) => {
                let tree = exec.explain(0);
                let expected = "\
Projection([0])
  Filter
    SeqScan(warehouse)
";
                assert_eq!(tree, expected);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_select_with_full_chain() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(
            &env,
            "SELECT w_id FROM warehouse WHERE w_id = 1 LIMIT 3",
        );
        match p {
            PhysicalPlan::Executor(exec) => {
                let tree = exec.explain(0);
                // Order is: SeqScan → Filter → Projection → Limit (outermost wraps inward)
                let expected = "\
Limit(3)
  Projection([0])
    Filter
      SeqScan(warehouse)
";
                assert_eq!(tree, expected);
            }
            _ => panic!(),
        }
    }

    // ---- INSERT / UPDATE / DELETE ----

    #[test]
    fn plans_insert_to_insert_op_and_drives() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "INSERT INTO warehouse VALUES (1, 1000), (2, 2000)");
        match p {
            PhysicalPlan::Executor(mut exec) => {
                assert_eq!(exec.next().unwrap(), Some(vec![Value::Int64(2)]));
                assert_eq!(exec.next().unwrap(), None);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_update_with_where_and_drives() {
        let env = setup();
        create_warehouse(&env);
        // Seed: 1, 1000; 2, 2000.
        let seed = plan_sql(
            &env,
            "INSERT INTO warehouse VALUES (1, 1000), (2, 2000)",
        );
        if let PhysicalPlan::Executor(mut e) = seed {
            e.next().unwrap();
        }
        // UPDATE w_ytd to 9999 WHERE w_id = 1.
        let p = plan_sql(&env, "UPDATE warehouse SET w_ytd = 9999 WHERE w_id = 1");
        match p {
            PhysicalPlan::Executor(mut exec) => {
                assert_eq!(exec.next().unwrap(), Some(vec![Value::Int64(1)]));
                // Verify the explain has the right shape.
                let tree = exec.explain(0);
                assert!(tree.starts_with("Update(warehouse, set_cols=[1])"));
                assert!(tree.contains("Filter"));
                assert!(tree.contains("SeqScan(warehouse)"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_delete_with_where() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(&env, "DELETE FROM warehouse WHERE w_id = 99");
        match p {
            PhysicalPlan::Executor(exec) => {
                let tree = exec.explain(0);
                assert!(tree.starts_with("Delete(warehouse)"));
                assert!(tree.contains("Filter"));
                assert!(tree.contains("SeqScan(warehouse)"));
            }
            _ => panic!(),
        }
    }

    // ---- EXPLAIN ----

    #[test]
    fn plans_explain_returns_tree_string() {
        let env = setup();
        create_warehouse(&env);
        let p = plan_sql(
            &env,
            "EXPLAIN SELECT w_id FROM warehouse WHERE w_id = 1",
        );
        match p {
            PhysicalPlan::Explain(text) => {
                assert!(text.contains("Projection"));
                assert!(text.contains("Filter"));
                assert!(text.contains("SeqScan(warehouse)"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn plans_explain_for_ddl_emits_descriptor_line() {
        let env = setup();
        let p = plan_sql(&env, "EXPLAIN CREATE TABLE t (id INT PRIMARY KEY)");
        match p {
            PhysicalPlan::Explain(text) => {
                assert!(text.starts_with("CreateTable(t)"));
            }
            _ => panic!(),
        }
    }

    // ---- Error paths ----

    #[test]
    fn plan_on_unknown_table_errors() {
        // The binder normally catches unknown tables. Bypass it by
        // constructing a `LogicalPlan` directly to exercise the planner's
        // own catalog lookup path.
        let env = setup();
        let logical = LogicalPlan::Select {
            table: "nonexistent".to_string(),
            projection: vec![],
            filter: None,
            limit: None,
        };
        match plan(logical, env.engine.clone(), &env.catalog) {
            Err(Error::TableNotFound { name }) => assert_eq!(name, "nonexistent"),
            other => panic!("expected TableNotFound, got {:?}", other.err()),
        }
    }
}
