//! The SQL layer's three plan representations, in lowering order:
//!
//!   - [`expr`]     — `Expression` / `Predicate`: the scalar AST shared
//!     by every stage (bound by the binder, costed by the optimizer,
//!     compiled to closures by the executor builders).
//!   - [`logical`]  — `LogicalPlan`: the catalog-resolved statement IR
//!     the binder produces and planners consume.
//!   - [`physical`] — `PhysOp`: the model-neutral physical plan IR a
//!     planner emits and an `ExecutionModel` compiles to a runnable tree.

pub mod expr;
pub mod logical;
pub mod physical;
