//! `Sort` — in-memory sort operator.
//!
//! Eagerly drains the child, sorts the buffered rows by a sequence of
//! `(column, direction)` keys, then yields rows in sorted order. The
//! spill-to-disk hook is reserved for Phase 20 (TPC-H SF1+); Phase 13
//! workloads (TPC-C, OLTP) fit in memory.
//!
//! ## NULL handling
//!
//! NULLs sort *last* under both `Asc` and `Desc` — matches OLTP expectations
//! and avoids surprising "NULLs at the top" output. A future `NULLS FIRST` /
//! `NULLS LAST` modifier can be added per sort key when the binder gains
//! the surface for it.
//!
//! ## Comparator
//!
//! Multi-key sort with stable tie-breaking via `slice::sort_by` (Rust's
//! `sort_by` is stable). Per-key comparison reuses the same numeric
//! cross-promotion + same-scale Decimal rules as `eval_compare` /
//! `compare_values`.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{Executor, Tuple};
use crate::types::Value;

/// One sort-key direction. Multi-column ORDER BY produces a Vec of
/// `(col, dir)` pairs that the comparator walks until tie-broken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

pub struct Sort {
    schema: Arc<Schema>,
    child: Box<dyn Executor>,
    keys: Vec<(usize, SortDir)>,
    /// `None` until the first `next()` call. After draining + sorting,
    /// holds the sorted buffer as an iterator the operator pops from.
    sorted: Option<std::vec::IntoIter<Tuple>>,
}

impl Sort {
    pub fn new(child: Box<dyn Executor>, keys: Vec<(usize, SortDir)>) -> Result<Self> {
        if keys.is_empty() {
            return Err(crate::common::Error::SqlParse(
                "Sort requires at least one sort key".into(),
            ));
        }
        let schema = Arc::new(child.schema().clone());
        Ok(Self {
            schema,
            child,
            keys,
            sorted: None,
        })
    }

    fn build_sorted(&mut self) -> Result<()> {
        let mut rows: Vec<Tuple> = Vec::new();
        while let Some(t) = self.child.next()? {
            rows.push(t);
        }
        let keys = self.keys.clone();
        rows.sort_by(|a, b| compare_rows(a, b, &keys));
        self.sorted = Some(rows.into_iter());
        Ok(())
    }
}

impl Executor for Sort {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.sorted.is_none() {
            self.build_sorted()?;
        }
        Ok(self.sorted.as_mut().unwrap().next())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let keys: Vec<String> = self
            .keys
            .iter()
            .map(|(c, d)| {
                format!(
                    "{}{}",
                    c,
                    match d {
                        SortDir::Asc => " ASC",
                        SortDir::Desc => " DESC",
                    }
                )
            })
            .collect();
        let mut out = format!("{}Sort[{}]\n", pad, keys.join(", "));
        out.push_str(&self.child.explain(indent + 1));
        out
    }
}

fn compare_rows(a: &Tuple, b: &Tuple, keys: &[(usize, SortDir)]) -> Ordering {
    for (col, dir) in keys {
        let ord = compare_key(&a[*col], &b[*col], *dir);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Per-key comparator. NULL is +∞ — larger than every value — and the
/// direction reversal applies to it like any other value: last under ASC,
/// FIRST under DESC. This matches the Postgres/Oracle/DB2 default (the
/// engines the project differential-tests against); MySQL differs
/// (NULL = −∞). Non-NULL pairs use the canonical `Value::compare_sql` —
/// the same ordering predicates and MIN/MAX use, so a query's ORDER BY
/// can never disagree with its WHERE.
fn compare_key(a: &Value, b: &Value, dir: SortDir) -> Ordering {
    let raw = match (a, b) {
        // NULL = +∞: resolved BEFORE direction so DESC flips it too.
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => match a.compare_sql(b) {
            Some(ord) => ord,
            None => {
                // Incomparable non-NULL pair ⇒ mixed-type data in one sort
                // column — an upstream constraint bug (the catalog enforces
                // single-typed columns). Crash in dev; keep the order total
                // in release rather than mis-order silently.
                debug_assert!(false, "Sort: incomparable values {:?} vs {:?}", a, b);
                Ordering::Equal
            }
        },
    };
    match dir {
        SortDir::Asc => raw,
        SortDir::Desc => raw.reverse(),
    }
}
