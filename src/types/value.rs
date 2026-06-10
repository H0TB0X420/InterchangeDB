//! Column types and runtime values.
//!
//! Phase 9 Task 9.1 fleshes out methods (matches, type_of, decimal arithmetic,
//! serde derives). Task 9.0 only needs the enum *shapes* to compile and to
//! pattern-match against in keyenc.

use serde::{Deserialize, Serialize};

use crate::types::Decimal;

/// SQL column type. Determines storage representation and value compatibility.
///
/// `Copy` because every variant is small (≤ 4 bytes of data); cheap-to-copy
/// avoids `Clone` calls in the catalog and constraint layers that thread
/// schemas through query execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Varchar(String),
    Char(String),
    Bytes(Vec<u8>),
    Decimal(Decimal),
    Timestamp(i64), // microseconds since Unix epoch
    Boolean(bool),
    Null,
}

impl Value {
    /// True if this value is shape-compatible with the given column type.
    ///
    /// `Null` matches any column type — null *permission* is the column's
    /// `nullable` flag, checked separately by the constraint layer. `matches`
    /// is purely about runtime variant agreement.
    ///
    /// For `Decimal`, scale must agree (cross-scale values are not comparable).
    /// For `Varchar` / `Char` / `Bytes`, length is *not* checked here — that's
    /// `check_value_bounds`' job.
    pub fn matches(&self, ty: &ColumnType) -> bool {
        match (self, ty) {
            (Value::Null, _) => true,
            (Value::Int32(_), ColumnType::Int32) => true,
            (Value::Int64(_), ColumnType::Int64) => true,
            (Value::Varchar(_), ColumnType::Varchar(_)) => true,
            (Value::Char(_), ColumnType::Char(_)) => true,
            (Value::Bytes(_), ColumnType::Bytes(_)) => true,
            (Value::Decimal(d), ColumnType::Decimal { scale, .. }) => d.scale() == *scale,
            (Value::Timestamp(_), ColumnType::Timestamp) => true,
            (Value::Boolean(_), ColumnType::Boolean) => true,
            _ => false,
        }
    }

    /// Best-effort `ColumnType` the value inhabits. `Null` returns `None`
    /// (it has no intrinsic type).
    ///
    /// For varlen variants, the returned type's length parameter is the value's
    /// actual byte length, saturated at `u16::MAX`. For `Decimal`, precision is
    /// reported as `Decimal::MAX_PRECISION` since the value alone doesn't carry
    /// the column's declared precision.
    ///
    /// Used primarily for human-readable error messages from the constraint
    /// layer (e.g., "expected Int32, got Varchar(13)").
    pub fn type_of(&self) -> Option<ColumnType> {
        match self {
            Value::Null => None,
            Value::Int32(_) => Some(ColumnType::Int32),
            Value::Int64(_) => Some(ColumnType::Int64),
            Value::Varchar(s) => Some(ColumnType::Varchar(saturating_u16(s.len()))),
            Value::Char(s) => Some(ColumnType::Char(saturating_u16(s.len()))),
            Value::Bytes(b) => Some(ColumnType::Bytes(saturating_u16(b.len()))),
            Value::Decimal(d) => Some(ColumnType::Decimal {
                precision: Decimal::MAX_PRECISION,
                scale: d.scale(),
            }),
            Value::Timestamp(_) => Some(ColumnType::Timestamp),
            Value::Boolean(_) => Some(ColumnType::Boolean),
        }
    }
}

fn saturating_u16(n: usize) -> u16 {
    n.min(u16::MAX as usize) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each non-null variant must match its corresponding ColumnType.
    #[test]
    fn matches_each_variant_against_its_type() {
        assert!(Value::Int32(42).matches(&ColumnType::Int32));
        assert!(Value::Int64(42).matches(&ColumnType::Int64));
        assert!(Value::Boolean(true).matches(&ColumnType::Boolean));
        assert!(Value::Timestamp(0).matches(&ColumnType::Timestamp));
        assert!(Value::Varchar("a".into()).matches(&ColumnType::Varchar(10)));
        assert!(Value::Char("a".into()).matches(&ColumnType::Char(10)));
        assert!(Value::Bytes(vec![0]).matches(&ColumnType::Bytes(10)));
        assert!(
            Value::Decimal(Decimal::from_i64_with_scale(0, 2)).matches(&ColumnType::Decimal {
                precision: 10,
                scale: 2
            })
        );
    }

    /// Cross-variant pairs must not match.
    #[test]
    fn matches_rejects_wrong_variants() {
        assert!(!Value::Int32(42).matches(&ColumnType::Int64));
        assert!(!Value::Int64(42).matches(&ColumnType::Int32));
        assert!(!Value::Boolean(true).matches(&ColumnType::Int32));
        assert!(!Value::Varchar("a".into()).matches(&ColumnType::Char(10)));
        assert!(!Value::Char("a".into()).matches(&ColumnType::Varchar(10)));
        assert!(!Value::Bytes(vec![0]).matches(&ColumnType::Varchar(10)));
        assert!(!Value::Timestamp(0).matches(&ColumnType::Int64));
    }

    /// Null is type-shape-compatible with any column. The nullable flag is
    /// what enforces null permission, not the type system.
    #[test]
    fn null_matches_any_type() {
        let types = [
            ColumnType::Int32,
            ColumnType::Int64,
            ColumnType::Varchar(10),
            ColumnType::Char(10),
            ColumnType::Bytes(10),
            ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
            ColumnType::Timestamp,
            ColumnType::Boolean,
        ];
        for ty in &types {
            assert!(Value::Null.matches(ty), "Null should match {:?}", ty);
        }
    }

    /// Decimal matches checks scale; precision is not a matches concern
    /// (precision is bounds-checking, handled by `Decimal::exceeds_precision`).
    #[test]
    fn decimal_matches_only_when_scale_agrees() {
        let d = Value::Decimal(Decimal::from_i64_with_scale(123, 2));
        // Same scale, different precision: matches.
        assert!(d.matches(&ColumnType::Decimal {
            precision: 5,
            scale: 2
        }));
        assert!(d.matches(&ColumnType::Decimal {
            precision: 18,
            scale: 2
        }));
        // Different scale: doesn't match.
        assert!(!d.matches(&ColumnType::Decimal {
            precision: 10,
            scale: 3
        }));
        assert!(!d.matches(&ColumnType::Decimal {
            precision: 10,
            scale: 0
        }));
    }

    #[test]
    fn type_of_null_is_none() {
        assert_eq!(Value::Null.type_of(), None);
    }

    #[test]
    fn type_of_returns_matching_type_for_simple_variants() {
        assert_eq!(Value::Int32(0).type_of(), Some(ColumnType::Int32));
        assert_eq!(Value::Int64(0).type_of(), Some(ColumnType::Int64));
        assert_eq!(Value::Boolean(false).type_of(), Some(ColumnType::Boolean));
        assert_eq!(Value::Timestamp(0).type_of(), Some(ColumnType::Timestamp));
    }

    /// Varlen variants report their actual byte length in the returned type.
    /// Useful for error messages like "expected Varchar(10), got Varchar(13)".
    #[test]
    fn type_of_varlen_carries_actual_length() {
        assert_eq!(
            Value::Varchar("hello".into()).type_of(),
            Some(ColumnType::Varchar(5))
        );
        assert_eq!(
            Value::Char("ab".into()).type_of(),
            Some(ColumnType::Char(2))
        );
        assert_eq!(
            Value::Bytes(vec![0, 1, 2]).type_of(),
            Some(ColumnType::Bytes(3))
        );
    }

    /// Strings longer than u16::MAX bytes saturate (rather than panicking
    /// from a `try_into` failure). type_of should never panic.
    #[test]
    fn type_of_varlen_saturates_at_u16_max() {
        let huge = "x".repeat(u16::MAX as usize + 100);
        match Value::Varchar(huge).type_of() {
            Some(ColumnType::Varchar(n)) => assert_eq!(n, u16::MAX),
            other => panic!("unexpected: {:?}", other),
        }
    }

    /// Decimal type_of reports the value's actual scale; precision is the
    /// MAX_PRECISION sentinel since the value alone doesn't know its
    /// declared column's precision.
    #[test]
    fn type_of_decimal_carries_value_scale() {
        let d = Value::Decimal(Decimal::from_i64_with_scale(0, 4));
        match d.type_of() {
            Some(ColumnType::Decimal { precision, scale }) => {
                assert_eq!(scale, 4);
                assert_eq!(precision, Decimal::MAX_PRECISION);
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    /// Every Value variant round-trips through bincode (covers the serde
    /// derive on Value + the embedded Decimal serde derive).
    #[test]
    fn serde_roundtrip_value_via_bincode() {
        let cases = vec![
            Value::Int32(-42),
            Value::Int64(i64::MIN),
            Value::Varchar("hello\0world".into()),
            Value::Char("ab".into()),
            Value::Bytes(vec![0, 1, 2, 0xFF]),
            Value::Decimal(Decimal::from_i64_with_scale(12345, 2)),
            Value::Timestamp(1_700_000_000_000_000),
            Value::Boolean(true),
            Value::Null,
        ];
        for v in &cases {
            let bytes = bincode::serialize(v).unwrap();
            let back: Value = bincode::deserialize(&bytes).unwrap();
            assert_eq!(&back, v, "value roundtrip failed for {:?}", v);
        }
    }

    /// ColumnType serde roundtrip — covers the types we'll persist into
    /// __sys_columns.type_blob in Phase 9 Task 9.5.
    #[test]
    fn serde_roundtrip_column_type_via_bincode() {
        let cases = vec![
            ColumnType::Int32,
            ColumnType::Int64,
            ColumnType::Varchar(64),
            ColumnType::Char(8),
            ColumnType::Bytes(256),
            ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
            ColumnType::Timestamp,
            ColumnType::Boolean,
        ];
        for ty in &cases {
            let bytes = bincode::serialize(ty).unwrap();
            let back: ColumnType = bincode::deserialize(&bytes).unwrap();
            assert_eq!(&back, ty, "column type roundtrip failed for {:?}", ty);
        }
    }
}
