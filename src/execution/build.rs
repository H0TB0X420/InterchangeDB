//! `build_executor` — compile a `PhysOp` IR into a Volcano pull-operator tree.
//!
//! This is the build half of the `Volcano` `ExecutionModel`: it walks a
//! model-neutral [`PhysOp`] plan and constructs the concrete
//! `Box<dyn Executor>` tree the pull driver runs. All the table/index handle
//! resolution and predicate/expression compilation that used to live inline
//! in the planner lives here — the planner emits data, the builder makes it
//! runnable.
//!
//! Engine generics live on the builder (it constructs `Table<E, RowLayout>`);
//! the IR stays monomorphic. User and system tables are `RowLayout` in V2.

use std::sync::Arc;

use crate::catalog::Catalog;
use crate::common::{Error, Result};
use crate::execution::{
    AggregateFn, Delete, Executor, Filter, HashAggregate, HashJoin, IndexNestedLoopJoin, IndexScan,
    Insert, JoinPredicate, Limit, MergeJoin, NestedLoopJoin, PkLookup, Projection, SeqScan,
    SetExpr, Sort, SortDir, Update,
};
use crate::layout::RowLayout;
use crate::sql::ir::logical::{AggregateSpec, OrderDir};
use crate::sql::ir::physical::PhysOp;
use crate::storage::StorageEngine;
use crate::table::{IndexHandle, Table};

/// A resolved `RowLayout` table handle paired with its index handles.
type ResolvedTable<E> = (Arc<Table<E, RowLayout>>, Vec<IndexHandle>);

/// Compile a physical plan into a runnable pull-operator tree. Recurses into
/// child plans; resolves table/index names against `catalog`; re-compiles
/// `Predicate`/`Expression` ASTs into the closures operators drive per tuple.
pub(crate) fn build_executor<E, CatE>(
    plan: &PhysOp,
    engine: &Arc<E>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    E: StorageEngine + 'static,
    CatE: StorageEngine,
{
    match plan {
        PhysOp::SeqScan { table } => {
            let (tbl, _indexes) = resolve_table(table, engine, catalog)?;
            Ok(Box::new(SeqScan::new(&*tbl)?))
        }
        PhysOp::IndexScan {
            table,
            index,
            prefix,
        } => {
            let (tbl, indexes) = resolve_table(table, engine, catalog)?;
            let handle = find_index(&indexes, index)?;
            Ok(Box::new(IndexScan::new(&*tbl, &handle, prefix)?))
        }
        PhysOp::PkLookup { table, pk } => {
            let (tbl, _indexes) = resolve_table(table, engine, catalog)?;
            Ok(Box::new(PkLookup::new(&*tbl, pk)?))
        }
        PhysOp::Filter { input, predicate } => {
            let child = build_executor(input, engine, catalog)?;
            Ok(Box::new(Filter::from_boxed(
                child,
                predicate.clone().compile(),
            )))
        }
        PhysOp::Projection { input, cols } => {
            let child = build_executor(input, engine, catalog)?;
            Ok(Box::new(Projection::new(child, cols.clone())?))
        }
        PhysOp::NestedLoopJoin { outer, inner, on } => {
            let outer_ex = build_executor(outer, engine, catalog)?;
            let inner_ex = build_executor(inner, engine, catalog)?;
            // Reconstruct the join predicate over the concatenated
            // `outer || inner` tuple — column indices are tuple-global because
            // joins are lowered in textual order. `None` is a cross product.
            let pred_fn: JoinPredicate = match on {
                Some(p) => {
                    let f = p.clone().compile();
                    Box::new(move |outer, inner| {
                        let mut combined = outer.clone();
                        combined.extend_from_slice(inner);
                        f(&combined)
                    })
                }
                None => Box::new(|_, _| true),
            };
            Ok(Box::new(NestedLoopJoin::new(outer_ex, inner_ex, pred_fn)?))
        }
        PhysOp::IndexNestedLoopJoin {
            outer,
            inner_table,
            inner_index,
            outer_key_cols,
        } => {
            let outer_ex = build_executor(outer, engine, catalog)?;
            let (tbl, indexes) = resolve_table(inner_table, engine, catalog)?;
            let handle = find_index(&indexes, inner_index)?;
            Ok(Box::new(IndexNestedLoopJoin::new(
                outer_ex,
                tbl,
                handle,
                outer_key_cols.clone(),
            )?))
        }
        PhysOp::HashJoin {
            outer,
            inner,
            outer_key_col,
            inner_key_col,
        } => {
            let outer_ex = build_executor(outer, engine, catalog)?;
            let inner_ex = build_executor(inner, engine, catalog)?;
            Ok(Box::new(HashJoin::new(
                outer_ex,
                inner_ex,
                *outer_key_col,
                *inner_key_col,
            )?))
        }
        PhysOp::MergeJoin {
            left,
            right,
            left_key_col,
            right_key_col,
        } => {
            let left_ex = build_executor(left, engine, catalog)?;
            let right_ex = build_executor(right, engine, catalog)?;
            Ok(Box::new(MergeJoin::new(
                left_ex,
                right_ex,
                *left_key_col,
                *right_key_col,
            )?))
        }
        PhysOp::HashAggregate { input, aggregates } => {
            let child = build_executor(input, engine, catalog)?;
            let agg_fns: Vec<AggregateFn> = aggregates
                .iter()
                .cloned()
                .map(translate_aggregate_spec)
                .collect();
            Ok(Box::new(HashAggregate::new(child, agg_fns)?))
        }
        PhysOp::Sort { input, keys } => {
            let child = build_executor(input, engine, catalog)?;
            let sort_keys: Vec<(usize, SortDir)> = keys
                .iter()
                .map(|(col, dir)| {
                    let dir = match dir {
                        OrderDir::Asc => SortDir::Asc,
                        OrderDir::Desc => SortDir::Desc,
                    };
                    (*col, dir)
                })
                .collect();
            Ok(Box::new(Sort::new(child, sort_keys)?))
        }
        PhysOp::Limit { input, max_rows } => {
            let child = build_executor(input, engine, catalog)?;
            Ok(Box::new(Limit::new(child, *max_rows)))
        }
        PhysOp::Insert { table, rows } => {
            let (tbl, _indexes) = resolve_table(table, engine, catalog)?;
            Ok(Box::new(Insert::new(tbl, rows.clone())))
        }
        PhysOp::Update { table, input, set } => {
            let (tbl, _indexes) = resolve_table(table, engine, catalog)?;
            let child = build_executor(input, engine, catalog)?;
            let set_exprs: Vec<SetExpr> = set
                .iter()
                .map(|(idx, expr)| (*idx, expr.clone().compile()))
                .collect();
            Ok(Box::new(Update::new(tbl, child, set_exprs)))
        }
        PhysOp::Delete { table, input } => {
            let (tbl, _indexes) = resolve_table(table, engine, catalog)?;
            let child = build_executor(input, engine, catalog)?;
            Ok(Box::new(Delete::new(tbl, child)))
        }
    }
}

/// Resolve a table name to a `RowLayout` table handle plus its index handles.
/// Returns the indexes alongside so callers that need a specific index can
/// pick it without a second catalog read.
fn resolve_table<E, CatE>(
    name: &str,
    engine: &Arc<E>,
    catalog: &Catalog<CatE>,
) -> Result<ResolvedTable<E>>
where
    E: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let schema = catalog.get_table(name)?;
    let indexes = catalog.indexes_for_table(schema.table_id, &schema)?;
    let table = Arc::new(Table::with_indexes(
        engine.clone(),
        schema,
        RowLayout,
        indexes.clone(),
    ));
    Ok((table, indexes))
}

/// Find the index handle named `name` among a table's indexes.
fn find_index(indexes: &[IndexHandle], name: &str) -> Result<IndexHandle> {
    indexes
        .iter()
        .find(|ix| ix.def.name == name)
        .cloned()
        .ok_or_else(|| Error::IndexNotFound {
            name: name.to_string(),
        })
}

/// Translate a logical aggregate spec into the executor's aggregate function.
fn translate_aggregate_spec(spec: AggregateSpec) -> AggregateFn {
    match spec {
        AggregateSpec::CountStar => AggregateFn::CountStar,
        AggregateSpec::Count {
            col,
            distinct: false,
        } => AggregateFn::Count(col),
        AggregateSpec::Count {
            col,
            distinct: true,
        } => AggregateFn::CountDistinct(col),
        AggregateSpec::Sum(col) => AggregateFn::Sum(col),
        AggregateSpec::Min(col) => AggregateFn::Min(col),
        AggregateSpec::Max(col) => AggregateFn::Max(col),
        AggregateSpec::Avg(col) => AggregateFn::Avg(col),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{ColumnDef, Schema, TableId};
    use crate::engines::btree::BTreeEngine;
    use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
    use crate::storage::FileDiskManager;
    use crate::types::{ColumnType, Value};

    // A fresh BTree-backed catalog + engine over a tempdir, plus a seeded
    // two-row `warehouse` table. Drains build_executor's tree directly (not
    // via an ExecutionModel) so this stays a pure builder test.
    fn seeded() -> (
        Arc<BTreeEngine>,
        Arc<Catalog<BTreeEngine>>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
        let engine = Arc::new(BTreeEngine::new(BufferPoolManager::new(512, dm)).unwrap());
        let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());

        let schema = Schema {
            name: "warehouse".into(),
            table_id: TableId(0),
            columns: vec![
                ColumnDef {
                    name: "w_id".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "w_ytd".into(),
                    ty: ColumnType::Int64,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec![0],
        };
        catalog.create_table("warehouse".into(), schema).unwrap();
        let schema = catalog.get_table("warehouse").unwrap();
        let indexes = catalog.indexes_for_table(schema.table_id, &schema).unwrap();
        let table = Table::with_indexes(engine.clone(), schema, RowLayout, indexes);
        table.insert(&[Value::Int32(1), Value::Int64(100)]).unwrap();
        table.insert(&[Value::Int32(2), Value::Int64(200)]).unwrap();

        (engine, catalog, dir)
    }

    fn drain(mut exec: Box<dyn Executor>) -> Vec<crate::execution::Tuple> {
        let mut rows = Vec::new();
        while let Some(t) = exec.next().unwrap() {
            rows.push(t);
        }
        rows
    }

    // build_executor resolves the table by name; the built SeqScan yields
    // every row.
    #[test]
    fn builds_and_runs_seq_scan() {
        let (engine, catalog, _dir) = seeded();
        let plan = PhysOp::SeqScan {
            table: "warehouse".into(),
        };
        let exec = build_executor(&plan, &engine, &catalog).unwrap();
        assert_eq!(drain(exec).len(), 2);
    }

    // Recursion + predicate compilation: Filter over SeqScan keeps only the
    // matching row.
    #[test]
    fn builds_and_runs_filter_over_seq_scan() {
        let (engine, catalog, _dir) = seeded();
        let plan = PhysOp::Filter {
            predicate: Predicate::Compare {
                op: CompareOp::Eq,
                left: Expression::Column(0),
                right: Expression::Literal(Value::Int32(1)),
            },
            input: Box::new(PhysOp::SeqScan {
                table: "warehouse".into(),
            }),
        };
        let exec = build_executor(&plan, &engine, &catalog).unwrap();
        assert_eq!(drain(exec), vec![vec![Value::Int32(1), Value::Int64(100)]]);
    }
}
