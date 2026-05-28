//! `IndexScan` — leaf operator: scan a secondary index for a key
//! prefix or range, dereference each entry to a PK lookup, and yield
//! the resulting rows.
//!
//! MVCC-aware via the table's engine: when the table is wrapped in a
//! `TxnEngine`, `get_by_pk` honors the snapshot. Index entries that
//! reference rows invisible at the current snapshot are silently
//! skipped — the secondary index is an auxiliary structure that can
//! lead with stale pointers; correctness lives on the PK-table read.
//!
//! Eager-collect like `SeqScan`: avoid self-referential structs at
//! the cost of buffering. Phase-11 streaming refactor will revisit.

use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{Executor, Tuple};
use crate::layout::DataLayout;
use crate::storage::StorageEngine;
use crate::table::{IndexHandle, Table};
use crate::types::{keyenc, Value};

pub struct IndexScan {
    schema: Arc<Schema>,
    rows: std::vec::IntoIter<Tuple>,
    index_name: String,
}

impl IndexScan {
    /// Scan `index` for every entry whose leading components equal
    /// `prefix`, dereferencing each to a row in `table`. The prefix
    /// must match the *leading* columns of the index's `columns` list
    /// — partial-prefix lookups are supported (e.g. (last_name) on
    /// a (last_name, first_name) composite index); full-key lookups
    /// are the common case.
    pub fn new<E: StorageEngine, L: DataLayout>(
        table: &Table<E, L>,
        index: &IndexHandle,
        prefix: &[Value],
    ) -> Result<Self> {
        if prefix.len() > index.def.columns.len() {
            return Err(crate::common::Error::IndexLookupArity {
                index: index.def.name.clone(),
                expected: index.def.columns.len(),
                actual: prefix.len(),
            });
        }
        // Encode the prefix using the leading types in `key_types`. The
        // half-open range `[encoded_prefix, byte_increment(encoded_prefix))`
        // captures every key sharing the prefix — `byte_increment` returns
        // the lexicographically-next byte sequence, which is the canonical
        // exclusive upper bound for prefix scans.
        let prefix_refs: Vec<&Value> = prefix.iter().collect();
        let prefix_types = &index.key_types[..prefix.len()];
        let encoded_prefix = keyenc::encode_key_components(&prefix_refs, prefix_types)?;
        // `prefix_increment` returns the exclusive upper bound for "starts
        // with this prefix" scans. `byte_increment` is the wrong tool here
        // (see its doc) — it produces a bound that misses keys whose PK
        // suffix begins with a byte greater than the appended 0x00.
        let end_bound = match keyenc::prefix_increment(&encoded_prefix) {
            Some(end) => std::ops::Bound::Excluded(end),
            None => std::ops::Bound::Unbounded,
        };

        let indexed_len = index.def.columns.len();
        let mut rows = Vec::new();
        let scan = index.engine.scan_range(
            std::ops::Bound::Included(encoded_prefix),
            end_bound,
        );
        for entry in scan {
            let (key, _) = entry?;
            // Decode the full key (indexed cols + PK cols). The PK suffix
            // is what we feed to `get_by_pk` to materialize the row.
            let all_values = keyenc::decode_key_components(&key, &index.key_types)?;
            let pk_values: Vec<Value> = all_values.into_iter().skip(indexed_len).collect();
            if let Some(row) = table.get_by_pk(&pk_values)? {
                rows.push(row);
            }
            // None → row invisible (MVCC) or already deleted while the
            // index still points to it. Either way: skip, don't error.
        }
        let schema = Arc::new(table.schema().clone());
        Ok(Self {
            schema,
            rows: rows.into_iter(),
            index_name: index.def.name.clone(),
        })
    }
}

impl Executor for IndexScan {
    fn next(&mut self) -> Result<Option<Tuple>> {
        Ok(self.rows.next())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        format!(
            "{}IndexScan({}, on {})\n",
            "  ".repeat(indent),
            self.schema.name,
            self.index_name
        )
    }
}
