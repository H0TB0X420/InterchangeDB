//! Test-only utilities for the execution layer.
//!
//! Provides a `VecExecutor` mock that yields tuples from a pre-built Vec.
//! Lets pipeline-operator tests (`Filter`, `Projection`, `Limit`) construct
//! input streams without standing up a `Table` + storage stack.

use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{Executor, Tuple};

/// Yields tuples from an owned `Vec<Tuple>` in insertion order, then None.
pub struct VecExecutor {
    schema: Arc<Schema>,
    rows: std::vec::IntoIter<Tuple>,
}

impl VecExecutor {
    pub fn new(schema: Schema, rows: Vec<Tuple>) -> Self {
        Self {
            schema: Arc::new(schema),
            rows: rows.into_iter(),
        }
    }
}

impl Executor for VecExecutor {
    fn next(&mut self) -> Result<Option<Tuple>> {
        Ok(self.rows.next())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
