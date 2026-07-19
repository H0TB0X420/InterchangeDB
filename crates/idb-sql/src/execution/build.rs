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
    AggregateFn, Compute, Delete, DerivedScan, Executor, Filter, HashAggregate, HashJoin,
    IndexNestedLoopJoin, IndexScan, Insert, JoinPredicate, Limit, MergeJoin, NestedLoopJoin,
    PkLookup, Projection, SeqScan, SetExpr, Sort, SortDir, Tuple, Update,
};
use crate::layout::RowLayout;
use crate::sql::ir::expr::InSubquerySet;
use crate::sql::ir::logical::{AggregateSpec, JoinKind, OrderDir};
use crate::sql::ir::physical::PhysOp;
use crate::storage::StorageEngine;
use crate::table::{IndexHandle, Table};

/// A resolved `RowLayout` table handle paired with its index handles.
type ResolvedTable<E> = (Arc<Table<E, RowLayout>>, Vec<IndexHandle>);

/// Compile a physical plan into a runnable pull-operator tree, with no
/// materialized subquery sets — a test convenience for plans that carry no
/// `InSubquery`. Production callers go through the model, which threads the
/// statement's sets into `build_executor_with_subqueries`.
#[cfg(test)]
pub(crate) fn build_executor<E, CatE>(
    plan: &PhysOp,
    engine: &Arc<E>,
    catalog: &Catalog<CatE>,
) -> Result<Box<dyn Executor>>
where
    E: StorageEngine + 'static,
    CatE: StorageEngine,
{
    build_executor_with_subqueries(plan, engine, catalog, &[])
}

/// Compile a physical plan into a runnable pull-operator tree. Recurses into
/// child plans; resolves table/index names against `catalog`; re-compiles
/// `Predicate`/`Expression` ASTs into the closures operators drive per tuple.
/// `subqueries` are the statement's materialized `InSubquery` sets (empty
/// unless the plan filters on an uncorrelated IN/EXISTS subquery), captured by
/// each `Filter`'s compiled predicate closure.
pub(crate) fn build_executor_with_subqueries<E, CatE>(
    plan: &PhysOp,
    engine: &Arc<E>,
    catalog: &Catalog<CatE>,
    subqueries: &[InSubquerySet],
) -> Result<Box<dyn Executor>>
where
    E: StorageEngine + 'static,
    CatE: StorageEngine,
{
    let build_executor = |plan: &PhysOp, engine: &Arc<E>, catalog: &Catalog<CatE>| {
        build_executor_with_subqueries(plan, engine, catalog, subqueries)
    };
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
        PhysOp::DerivedScan {
            alias,
            subplan,
            schema,
        } => {
            // A derived table (H4a): build the subplan's executor and wrap it
            // in a DerivedScan carrying the alias-named output schema. No
            // catalog lookup — the schema travels in the IR (a derived table
            // has no catalog entry).
            let child = build_executor(subplan, engine, catalog)?;
            Ok(Box::new(DerivedScan::new(
                alias.clone(),
                child,
                schema.clone(),
            )))
        }
        PhysOp::Filter { input, predicate } => {
            let child = build_executor(input, engine, catalog)?;
            // The only operator that can carry an `InSubquery`: compile with
            // the statement's materialized sets so the closure captures them.
            Ok(Box::new(Filter::from_boxed(
                child,
                predicate.clone().compile_with_subqueries(subqueries),
            )))
        }
        PhysOp::Projection { input, cols } => {
            let child = build_executor(input, engine, catalog)?;
            Ok(Box::new(Projection::new(child, cols.clone())?))
        }
        PhysOp::Compute { input, exprs } => {
            let child = build_executor(input, engine, catalog)?;
            Ok(Box::new(Compute::new(child, exprs.clone())?))
        }
        PhysOp::NestedLoopJoin {
            outer,
            inner,
            on,
            kind,
        } => {
            let outer_ex = build_executor(outer, engine, catalog)?;
            let inner_ex = build_executor(inner, engine, catalog)?;
            // Reconstruct the join predicate over the concatenated
            // `outer || inner` tuple — column indices are tuple-global because
            // joins are lowered in textual order. `None` is a cross product.
            // Under LEFT OUTER this is the FULL ON (R1): the whole predicate
            // gates the match, and the operator pads unmatched outer rows.
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
            match kind {
                JoinKind::Inner => Ok(Box::new(NestedLoopJoin::new(outer_ex, inner_ex, pred_fn)?)),
                JoinKind::LeftOuter => Ok(Box::new(NestedLoopJoin::new_left_outer(
                    outer_ex, inner_ex, pred_fn,
                )?)),
            }
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
            kind,
            residual,
        } => {
            let outer_ex = build_executor(outer, engine, catalog)?;
            let inner_ex = build_executor(inner, engine, catalog)?;
            match kind {
                JoinKind::Inner => Ok(Box::new(HashJoin::new(
                    outer_ex,
                    inner_ex,
                    *outer_key_col,
                    *inner_key_col,
                )?)),
                JoinKind::LeftOuter => {
                    // Compile the residual ON (compound-ON conjuncts beyond
                    // the hashed equi key) over the concatenated pair, same
                    // coordinates as the NLJ predicate above.
                    let residual_fn: Option<JoinPredicate> = match residual {
                        Some(p) => {
                            let f = p.clone().compile();
                            Some(Box::new(move |outer: &Tuple, inner: &Tuple| {
                                let mut combined = outer.clone();
                                combined.extend_from_slice(inner);
                                f(&combined)
                            }))
                        }
                        None => None,
                    };
                    Ok(Box::new(HashJoin::new_left_outer(
                        outer_ex,
                        inner_ex,
                        *outer_key_col,
                        *inner_key_col,
                        residual_fn,
                    )?))
                }
            }
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
        PhysOp::HashAggregate {
            input,
            group_by,
            aggregates,
        } => {
            let child = build_executor(input, engine, catalog)?;
            let agg_fns: Vec<AggregateFn> = aggregates
                .iter()
                .cloned()
                .map(translate_aggregate_spec)
                .collect();
            Ok(Box::new(HashAggregate::new(
                child,
                group_by.clone(),
                agg_fns,
            )?))
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
pub(crate) fn translate_aggregate_spec(spec: AggregateSpec) -> AggregateFn {
    match spec {
        AggregateSpec::CountStar => AggregateFn::CountStar,
        AggregateSpec::Count {
            arg,
            distinct: false,
        } => AggregateFn::Count(arg),
        AggregateSpec::Count {
            arg,
            distinct: true,
        } => AggregateFn::CountDistinct(arg),
        AggregateSpec::Sum(arg) => AggregateFn::Sum(arg),
        AggregateSpec::Min(arg) => AggregateFn::Min(arg),
        AggregateSpec::Max(arg) => AggregateFn::Max(arg),
        AggregateSpec::Avg(arg) => AggregateFn::Avg(arg),
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
