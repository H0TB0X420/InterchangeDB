//! Type system for the relational layer.
//!
//! `Value` is a runtime-tagged value of a particular `ColumnType`. The split
//! mirrors the SQL distinction between values and types and is the foundation
//! every layer above the storage engine builds on (catalog, layout, table,
//! executor, planner).
//!
//! Phase 9 build-out:
//! - Task 9.0 (this commit): minimal `ColumnType` / `Value` enums + `keyenc` skeleton.
//! - Task 9.1 (later): methods, decimal arithmetic, serde, full constructors.
//! - Task 9.2 (later): tuple encoding for row-store storage values.

pub mod decimal;
pub mod keyenc;
pub mod tuple;
pub mod value;

pub use decimal::Decimal;
pub use value::{ColumnType, Value};

/// A materialized row flowing between operators: one `Value` per column.
/// Lives in the vocabulary crate — both the IR (`sql`) and the executors
/// (`execution`) reference it, so neither may own it.
pub type Tuple = Vec<Value>;
