//! Push-based execution (Phase 15, Increment 2b) — the second `ExecutionModel`.
//!
//! Where `Volcano` *pulls* (a parent calls `child.next()`), a push pipeline
//! *drives from the leaf*: a `Source` iterates rows and pushes each one
//! downstream through a chain of `Sink`s, ending at a `Collector`. Control
//! flows bottom-up (data-driven) — the inverse of pull. Both models compile
//! the same [`PhysOp`]; that they can is the whole point of the IR seam.
//!
//! ## The interface
//!
//! - `Sink::push` handles one tuple and returns `Flow` — `Continue`, or
//!   `Stop` when the downstream has had enough (e.g. `LIMIT` satisfied).
//! - `Sink::finish` signals end-of-input.
//! - A `Source` owns the head of the sink chain and `run()`s it.
//!
//! ## What runs push vs. delegates
//!
//! Linear pipeline operators — scan, filter, projection, limit — run as genuine
//! push sinks. Everything that materializes anyway and has no streaming to gain
//! (indexed access, joins, `Sort`, `HashAggregate`, and DML) is built by the
//! shared `build_executor` and bridged into the pipeline by `ExecutorSource`
//! — reusing one operator set, so Push is trivially equivalent to Volcano there.
//!
//! Build threads the downstream sink *up toward the source* (the opposite of
//! pull's bottom-up wrap): `Filter{input}` becomes `build_push(input,
//! FilterSink{out})`, so the input pipeline pushes into the filter, which
//! pushes into the original `out`.
//!
//! ## Streaming scan
//!
//! `ScanSource` reads the table through the *lazy* [`DataLayout::scan_table`]
//! iterator (not `Table::scan`, which materializes a `Vec`), so a `Stop` from
//! downstream halts the iteration early — `SELECT … LIMIT 10` builds ~10
//! downstream tuples instead of pull's whole-table `Vec`. The win is real
//! (~20% on a 20k-row scan+LIMIT, see `benches/push_vs_volcano.rs`) but
//! bounded: through the MVCC layer the underlying range scan still materializes
//! the range before yielding, so the early `Stop` saves the executor-level
//! tuple building, not the engine read. A lazy MVCC scan would unlock the rest.
//!
//! `Push` is the `ExecutionModel` impl; a `Session` swaps to it via
//! `set_execution_model` (and the `--exec-model push` tpcc flag), so the same
//! SQL runs through either model at runtime.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::catalog::{Catalog, Schema};
use crate::common::Result;
use crate::execution::build::{build_executor, translate_aggregate_spec};
use crate::execution::model::ExecutionModel;
use crate::execution::operators::hash_aggregate::{
    build_aggregate_schema, compare_keys, finalize_state, update_state, AggState, MAX_GROUP_COUNT,
};
use crate::execution::{AggregateFn, Executor, Tuple};
use crate::layout::{DataLayout, LayoutCtx, RowLayout};
use crate::sql::ir::physical::PhysOp;
use crate::storage::StorageEngine;
use crate::types::Value;

/// Whether a sink wants more tuples after the one it just received.
pub(crate) enum Flow {
    Continue,
    Stop,
}

/// A push operator: receives one tuple from upstream, does its work, and pushes
/// the result to its own downstream. `finish` marks end-of-input.
pub(crate) trait Sink {
    fn push(&mut self, tuple: Tuple) -> Result<Flow>;
    fn finish(&mut self) -> Result<()>;
}

/// Drives a push pipeline by producing rows into the sink chain it owns.
pub(crate) trait Source {
    fn run(&mut self) -> Result<()>;
}

/// Root sink: appends every pushed tuple to a shared buffer the driver reads
/// once the source has run to completion.
struct Collector {
    rows: Rc<RefCell<Vec<Tuple>>>,
}

impl Sink for Collector {
    fn push(&mut self, tuple: Tuple) -> Result<Flow> {
        self.rows.borrow_mut().push(tuple);
        Ok(Flow::Continue)
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Keeps only tuples for which `predicate` holds.
struct FilterSink {
    predicate: Box<dyn Fn(&Tuple) -> bool + Send>,
    out: Box<dyn Sink>,
}

impl Sink for FilterSink {
    fn push(&mut self, tuple: Tuple) -> Result<Flow> {
        if (self.predicate)(&tuple) {
            self.out.push(tuple)
        } else {
            Ok(Flow::Continue)
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.out.finish()
    }
}

/// Narrows / reorders columns by index into the input tuple.
struct ProjectionSink {
    cols: Vec<usize>,
    out: Box<dyn Sink>,
}

impl Sink for ProjectionSink {
    fn push(&mut self, tuple: Tuple) -> Result<Flow> {
        let projected = self.cols.iter().map(|&i| tuple[i].clone()).collect();
        self.out.push(projected)
    }

    fn finish(&mut self) -> Result<()> {
        self.out.finish()
    }
}

/// Passes through at most `remaining` tuples, then signals `Stop` so the source
/// halts. The `Stop` propagates up the chain to the scan, ending it early.
struct LimitSink {
    remaining: usize,
    out: Box<dyn Sink>,
}

impl Sink for LimitSink {
    fn push(&mut self, tuple: Tuple) -> Result<Flow> {
        if self.remaining == 0 {
            return Ok(Flow::Stop);
        }
        let downstream = self.out.push(tuple)?;
        self.remaining -= 1;
        if self.remaining == 0 {
            Ok(Flow::Stop)
        } else {
            Ok(downstream)
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.out.finish()
    }
}

/// Streaming full-table scan: lazily iterates `RowLayout::scan_table` and pushes
/// each row, stopping the moment downstream returns `Stop`.
struct ScanSource<E: StorageEngine> {
    engine: Arc<E>,
    schema: Arc<Schema>,
    out: Box<dyn Sink>,
}

impl<E: StorageEngine + 'static> Source for ScanSource<E> {
    fn run(&mut self) -> Result<()> {
        // `column_types` is a local that must outlive the borrowed scan. The
        // lazy iterator borrows `engine` (shared) + `column_types`; `out` is
        // borrowed mutably per push — disjoint, so this type-checks.
        let column_types = self.schema.column_types();
        let ctx = LayoutCtx {
            column_types: &column_types,
            table_id: self.schema.table_id,
        };
        let scan = RowLayout.scan_table(&*self.engine, ctx);
        for item in scan {
            let (_pk, row) = item?;
            if let Flow::Stop = self.out.push(row)? {
                break;
            }
        }
        self.out.finish()
    }
}

/// Grouped aggregation as a native push sink — the canonical push
/// pipeline breaker: accumulate per key on `push`, emit finalized groups
/// on `finish` (canonical key order, same as the pull operator — the
/// equivalence suite compares the two byte-for-byte).
///
/// `input_schema` is a shared slot filled by the build arm after
/// recursion returns the input pipeline's schema (sinks thread downward
/// before schemas flow back up — same `Rc<RefCell>` idiom as
/// `Collector`). It is always `Some` by the time the source runs.
struct GroupedAggregateSink {
    group_by: Vec<usize>,
    aggregates: Vec<AggregateFn>,
    input_schema: Rc<RefCell<Option<Schema>>>,
    groups: HashMap<Vec<Value>, Vec<AggState>>,
    out: Box<dyn Sink>,
}

impl Sink for GroupedAggregateSink {
    fn push(&mut self, tuple: Tuple) -> Result<Flow> {
        let schema_slot = self.input_schema.borrow();
        let input_schema = schema_slot
            .as_ref()
            .expect("input_schema filled at build time");
        let key: Vec<Value> = self.group_by.iter().map(|&c| tuple[c].clone()).collect();
        let group_count = self.groups.len();
        let states = match self.groups.entry(key) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(v) => {
                assert!(
                    group_count < MAX_GROUP_COUNT,
                    "GROUP BY exceeded {} distinct groups",
                    MAX_GROUP_COUNT
                );
                let states = self
                    .aggregates
                    .iter()
                    .map(|a| AggState::new_for(a, input_schema))
                    .collect::<Result<Vec<_>>>()?;
                v.insert(states)
            }
        };
        for (agg, state) in self.aggregates.iter().zip(states.iter_mut()) {
            update_state(agg, state, &tuple)?;
        }
        // A downstream `Stop` can't arrive during the input phase (nothing
        // has been emitted yet), and the aggregate must see every input
        // row regardless — always continue.
        Ok(Flow::Continue)
    }

    fn finish(&mut self) -> Result<()> {
        let schema_slot = self.input_schema.borrow();
        let input_schema = schema_slot
            .as_ref()
            .expect("input_schema filled at build time");
        let mut keyed: Vec<(Vec<Value>, Vec<AggState>)> = self.groups.drain().collect();
        keyed.sort_by(|(a, _), (b, _)| compare_keys(a, b));
        for (key, states) in keyed {
            let mut row = key;
            for (agg, state) in self.aggregates.iter().zip(states.into_iter()) {
                row.push(finalize_state(agg, state, input_schema)?);
            }
            if let Flow::Stop = self.out.push(row)? {
                break;
            }
        }
        self.out.finish()
    }
}

/// Bridges a pull operator tree into the push pipeline: drains the executor and
/// pushes each tuple downstream, honoring `Stop`. Used for the operators Push
/// delegates to `build_executor` — indexed access, joins, breakers, and DML,
/// none of which gain from streaming.
struct ExecutorSource {
    exec: Box<dyn Executor>,
    out: Box<dyn Sink>,
}

impl Source for ExecutorSource {
    fn run(&mut self) -> Result<()> {
        while let Some(tuple) = self.exec.next()? {
            if let Flow::Stop = self.out.push(tuple)? {
                break;
            }
        }
        self.out.finish()
    }
}

/// Compile a `PhysOp` into a push pipeline whose source feeds `out`, returning
/// the source paired with the pipeline's output [`Schema`]. The schema is
/// computed as we build: a leaf scan gives the table schema, `Projection`
/// narrows it, pass-throughs preserve it, and delegated operators report the
/// schema of the `build_executor` tree they bridge.
fn build_push<E, CatE>(
    plan: &PhysOp,
    out: Box<dyn Sink>,
    engine: &Arc<E>,
    catalog: &Catalog<CatE>,
) -> Result<(Box<dyn Source>, Schema)>
where
    E: StorageEngine + 'static,
    CatE: StorageEngine,
{
    match plan {
        PhysOp::SeqScan { table } => {
            let schema = catalog.get_table(table)?;
            let out_schema = (*schema).clone();
            let source = Box::new(ScanSource {
                engine: engine.clone(),
                schema,
                out,
            });
            Ok((source, out_schema))
        }
        // Filter and Limit pass tuples through unchanged — output schema is the
        // input's, so just return the recursive result.
        PhysOp::Filter { input, predicate } => {
            let sink = Box::new(FilterSink {
                predicate: predicate.clone().compile(),
                out,
            });
            build_push(input, sink, engine, catalog)
        }
        PhysOp::Limit { input, max_rows } => {
            let sink = Box::new(LimitSink {
                remaining: *max_rows,
                out,
            });
            build_push(input, sink, engine, catalog)
        }
        PhysOp::Projection { input, cols } => {
            let sink = Box::new(ProjectionSink {
                cols: cols.clone(),
                out,
            });
            let (source, in_schema) = build_push(input, sink, engine, catalog)?;
            // Validate column range at build time, exactly like Volcano's
            // `Projection::new` (E16): an invalid plan must fail the same
            // clean way under both models, never panic per-tuple in the
            // sink's `tuple[i]`.
            for &c in cols {
                if c >= in_schema.columns.len() {
                    return Err(crate::common::Error::Internal(format!(
                        "projection column index {} out of range (child has {} columns)",
                        c,
                        in_schema.columns.len()
                    )));
                }
            }
            Ok((source, project_schema(&in_schema, cols)))
        }
        // Grouped aggregation runs as a native push sink (H1 decision:
        // accumulate-on-push/emit-on-finish is the shape push models are
        // best at; bridging would quietly retire Push on every grouped
        // query). Whole-table aggregation (group_by empty, one output
        // row) has nothing to stream and stays on the delegated path.
        PhysOp::HashAggregate {
            input,
            group_by,
            aggregates,
        } if !group_by.is_empty() => {
            let agg_fns: Vec<AggregateFn> = aggregates
                .iter()
                .cloned()
                .map(translate_aggregate_spec)
                .collect();
            let schema_slot = Rc::new(RefCell::new(None));
            let sink = Box::new(GroupedAggregateSink {
                group_by: group_by.clone(),
                aggregates: agg_fns.clone(),
                input_schema: schema_slot.clone(),
                groups: HashMap::new(),
                out,
            });
            let (source, in_schema) = build_push(input, sink, engine, catalog)?;
            let out_schema = build_aggregate_schema(&in_schema, group_by, &agg_fns);
            *schema_slot.borrow_mut() = Some(in_schema);
            Ok((source, out_schema))
        }
        // Everything else materializes regardless (indexed access, joins,
        // Sort, ungrouped HashAggregate, DML) — build it with the shared
        // operator builder and bridge its output into the push pipeline. The
        // bridged executor's own `schema()` is the output schema. New
        // operators default to this safe path until given a push sink of
        // their own.
        _ => {
            let exec = build_executor(plan, engine, catalog)?;
            let out_schema = exec.schema().clone();
            Ok((Box::new(ExecutorSource { exec, out }), out_schema))
        }
    }
}

/// Output schema of a `Projection`: the input schema narrowed to `cols`,
/// dropping the PK constraint. Mirrors `Projection::new`.
fn project_schema(input: &Schema, cols: &[usize]) -> Schema {
    Schema {
        name: input.name.clone(),
        table_id: input.table_id,
        columns: cols.iter().map(|&c| input.columns[c].clone()).collect(),
        primary_key: vec![],
    }
}

/// Push-based execution model: builds a push pipeline from the plan, drives it
/// from the leaf, and returns the collected rows with their output schema.
pub(crate) struct Push;

impl ExecutionModel for Push {
    fn execute<E, CatE>(
        &self,
        plan: &PhysOp,
        engine: &Arc<E>,
        catalog: &Catalog<CatE>,
    ) -> Result<(Schema, Vec<Tuple>)>
    where
        E: StorageEngine + 'static,
        CatE: StorageEngine,
    {
        let rows = Rc::new(RefCell::new(Vec::new()));
        let collector = Box::new(Collector { rows: rows.clone() });
        let (mut source, schema) = build_push(plan, collector, engine, catalog)?;
        source.run()?;
        let collected = rows.borrow().clone();
        Ok((schema, collected))
    }

    fn name(&self) -> &'static str {
        "push"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{ColumnDef, TableId};
    use crate::engines::btree::BTreeEngine;
    use crate::execution::ExecutionModel;
    use crate::sql::ir::expr::{CompareOp, Expression, Predicate};
    use crate::storage::FileDiskManager;
    use crate::table::Table;
    use crate::types::{ColumnType, Value};

    // A BTree-backed catalog/engine with a `nums` table seeded `count` rows:
    // (n INT PK, doubled BIGINT) = (0,0), (1,2), (2,4), …
    fn seeded(
        count: i32,
    ) -> (
        Arc<BTreeEngine>,
        Arc<Catalog<BTreeEngine>>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
        let engine = Arc::new(BTreeEngine::new(BufferPoolManager::new(512, dm)).unwrap());
        let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());
        let schema = Schema {
            name: "nums".into(),
            table_id: TableId(0),
            columns: vec![
                ColumnDef {
                    name: "n".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "doubled".into(),
                    ty: ColumnType::Int64,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec![0],
        };
        catalog.create_table("nums".into(), schema).unwrap();
        let schema = catalog.get_table("nums").unwrap();
        let indexes = catalog.indexes_for_table(schema.table_id, &schema).unwrap();
        let table = Table::with_indexes(engine.clone(), schema, RowLayout, indexes);
        for n in 0..count {
            table
                .insert(&[Value::Int32(n), Value::Int64(n as i64 * 2)])
                .unwrap();
        }
        (engine, catalog, dir)
    }

    fn scan(table: &str) -> PhysOp {
        PhysOp::SeqScan {
            table: table.into(),
        }
    }

    // n >= 2  (Int32 column 0 compared to a literal)
    fn n_ge_2(input: PhysOp) -> PhysOp {
        PhysOp::Filter {
            input: Box::new(input),
            predicate: Predicate::Compare {
                op: CompareOp::Gte,
                left: Expression::Column(0),
                right: Expression::Literal(Value::Int32(2)),
            },
        }
    }

    // The push pipeline SeqScan → Filter(n>=2) → Projection([0]) yields the
    // matching keys, driven leaf-first through the sink chain.
    #[test]
    fn push_filters_and_projects() {
        let (engine, catalog, _dir) = seeded(5); // n = 0..4
        let plan = PhysOp::Projection {
            cols: vec![0],
            input: Box::new(n_ge_2(scan("nums"))),
        };
        let (_schema, rows) = Push.execute(&plan, &engine, &catalog).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Int32(2)],
                vec![Value::Int32(3)],
                vec![Value::Int32(4)],
            ]
        );
    }

    // Limit yields exactly `max_rows` and (via the streaming scan + Stop)
    // does not require consuming the whole table.
    #[test]
    fn push_limit_caps_output() {
        let (engine, catalog, _dir) = seeded(100);
        let plan = PhysOp::Limit {
            max_rows: 3,
            input: Box::new(scan("nums")),
        };
        let (_schema, rows) = Push.execute(&plan, &engine, &catalog).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Int32(0));
        assert_eq!(rows[2][0], Value::Int32(2));
    }

    // Limit 0 produces no rows.
    #[test]
    fn push_limit_zero_yields_nothing() {
        let (engine, catalog, _dir) = seeded(10);
        let plan = PhysOp::Limit {
            max_rows: 0,
            input: Box::new(scan("nums")),
        };
        let (_schema, rows) = Push.execute(&plan, &engine, &catalog).unwrap();
        assert!(rows.is_empty());
    }

    // A delegated operator (HashAggregate) runs via the `build_executor` bridge
    // and its result row flows into the push collector.
    #[test]
    fn push_delegates_aggregate() {
        use crate::sql::ir::logical::AggregateSpec;
        let (engine, catalog, _dir) = seeded(5);
        let plan = PhysOp::HashAggregate {
            input: Box::new(scan("nums")),
            group_by: vec![],
            aggregates: vec![AggregateSpec::CountStar],
        };
        let (_schema, rows) = Push.execute(&plan, &engine, &catalog).unwrap();
        assert_eq!(rows, vec![vec![Value::Int64(5)]]);
    }

    // A delegated breaker (Sort) bridges into the pipeline; rows come back in
    // the operator's order. Then a push `Limit` on top caps the bridged output —
    // exercising push-over-delegated composition.
    #[test]
    fn push_limit_over_delegated_sort() {
        use crate::sql::ir::logical::OrderDir;
        let (engine, catalog, _dir) = seeded(4); // n = 0..3
        let plan = PhysOp::Limit {
            max_rows: 2,
            input: Box::new(PhysOp::Sort {
                input: Box::new(scan("nums")),
                keys: vec![(0, OrderDir::Desc)],
            }),
        };
        let (_schema, rows) = Push.execute(&plan, &engine, &catalog).unwrap();
        // desc → 3, 2, 1, 0; limit 2 → first two.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int32(3));
        assert_eq!(rows[1][0], Value::Int32(2));
    }
}
