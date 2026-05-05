//! Column types and runtime values.
//!
//! Phase 9 Task 9.1 fleshes out methods (matches, type_of, decimal arithmetic,
//! serde derives). Task 9.0 only needs the enum *shapes* to compile and to
//! pattern-match against in keyenc.

/// SQL column type. Determines storage representation and value compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Int32,
    Int64,
    Varchar(u16),
    Char(u16),
    Bytes(u16),
    Decimal { precision: u8, scale: u8 },
    Timestamp,
    Boolean,
}

/// A runtime value, tagged with its type. `Null` is its own variant rather
/// than wrapping every other variant in `Option` — keeps `Vec<Value>` flat
/// (one layer of nullability) instead of `Vec<Option<Value>>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Varchar(String),
    Char(String),
    Bytes(Vec<u8>),
    Decimal { mantissa: i64, scale: u8 },
    Timestamp(i64), // microseconds since Unix epoch
    Boolean(bool),
    Null,
}
