//! Execution models — *how* a physical plan is evaluated into result rows.
//!
//! An `Executor` is a single pull operator; an [`ExecutionModel`] is the
//! driver that turns a whole [`PhysOp`] plan into rows. The model owns both
//! halves: *build* (compile the model-neutral plan into its runnable form) and
//! *run* (drive that form to completion). [`Volcano`] builds a pull-operator
//! tree (via `build_executor`) and pulls it to exhaustion; a push-based
//! second impl will build a push pipeline from the same [`PhysOp`].
//!
//! The model takes `&PhysOp` plus the engine + catalog it resolves table
//! handles against — never a pre-built operator tree, so a second model is
//! free to build differently. That is the execution-model interchange seam.

use std::sync::Arc;

use crate::catalog::{Catalog, Schema};
use crate::common::Result;
use crate::execution::build::build_executor_with_subqueries;
use crate::execution::Tuple;
use crate::sql::ir::expr::{CorrelatedEvaluator, InSubquerySet};
use crate::sql::ir::physical::PhysOp;
use crate::storage::StorageEngine;

/// How a physical plan is evaluated into result rows. The model owns build +
/// drive; the session swaps models to change evaluation strategy (the
/// interchangeability promise).
pub trait ExecutionModel {
    /// Build `plan` into a runnable form and drive it to completion. Returns
    /// the output schema (for framing `SELECT` results) alongside the rows.
    /// `subqueries` are the statement's materialized uncorrelated `InSubquery`
    /// sets (empty for a plan with no IN/EXISTS subquery filter); `correlated`
    /// its per-outer-row correlated evaluators (empty for a plan with no
    /// correlated EXISTS / scalar). `Err(_)` if building or any operator faults.
    fn execute<E, CatE>(
        &self,
        plan: &PhysOp,
        engine: &Arc<E>,
        catalog: &Catalog<CatE>,
        subqueries: &[InSubquerySet],
        correlated: &[CorrelatedEvaluator],
    ) -> Result<(Schema, Vec<Tuple>)>
    where
        E: StorageEngine + 'static,
        CatE: StorageEngine;

    /// Short identifier for logging / introspection.
    fn name(&self) -> &'static str;
}

/// Pull-based (Volcano) execution: build a pull-operator tree, then `next()`
/// the root to exhaustion.
pub struct Volcano;

impl ExecutionModel for Volcano {
    fn execute<E, CatE>(
        &self,
        plan: &PhysOp,
        engine: &Arc<E>,
        catalog: &Catalog<CatE>,
        subqueries: &[InSubquerySet],
        correlated: &[CorrelatedEvaluator],
    ) -> Result<(Schema, Vec<Tuple>)>
    where
        E: StorageEngine + 'static,
        CatE: StorageEngine,
    {
        let mut root =
            build_executor_with_subqueries(plan, engine, catalog, subqueries, correlated)?;
        let schema = root.schema().clone();
        // Pull discipline: ask the root for its next tuple until end-of-stream.
        // Each `?` propagates a fatal operator error.
        let mut rows = Vec::new();
        while let Some(tuple) = root.next()? {
            rows.push(tuple);
        }
        Ok((schema, rows))
    }

    fn name(&self) -> &'static str {
        "volcano"
    }
}

/// Runtime-selectable execution model held by a `Session`.
///
/// Enum dispatch (like the `Planner` enum) rather than `Box<dyn ExecutionModel>`
/// because `execute` is generic over the storage engine — so the trait isn't
/// object-safe — and the model set is closed (`Volcano`, `Push`). The session
/// swaps this to change evaluation strategy at runtime.
/// `Copy` so the session can capture the active model by value into a
/// correlated evaluator's `'static` closure (H4c).
#[derive(Default, Clone, Copy)]
pub enum ExecModel {
    #[default]
    Volcano,
    Push,
}

impl ExecModel {
    /// Execute `plan` with the selected model, returning its output schema and
    /// rows. `subqueries` are the statement's materialized uncorrelated
    /// `InSubquery` sets (empty when the plan has no IN/EXISTS filter);
    /// `correlated` its per-outer-row correlated evaluators (empty when none).
    pub fn execute<E, CatE>(
        &self,
        plan: &PhysOp,
        engine: &Arc<E>,
        catalog: &Catalog<CatE>,
        subqueries: &[InSubquerySet],
        correlated: &[CorrelatedEvaluator],
    ) -> Result<(Schema, Vec<Tuple>)>
    where
        E: StorageEngine + 'static,
        CatE: StorageEngine,
    {
        match self {
            ExecModel::Volcano => Volcano.execute(plan, engine, catalog, subqueries, correlated),
            ExecModel::Push => {
                crate::execution::push::Push.execute(plan, engine, catalog, subqueries, correlated)
            }
        }
    }

    /// Identifier of the active model, for logging / introspection.
    pub fn name(&self) -> &'static str {
        match self {
            ExecModel::Volcano => Volcano.name(),
            ExecModel::Push => crate::execution::push::Push.name(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `execute` (build + drive) is exercised end-to-end by every SELECT/DML in
    // the e2e suite, and its build half by the `execution::build` tests; here
    // we just pin the models' identities.
    #[test]
    fn model_names() {
        assert_eq!(Volcano.name(), "volcano");
        assert_eq!(ExecModel::default().name(), "volcano");
        assert_eq!(ExecModel::Push.name(), "push");
    }
}
