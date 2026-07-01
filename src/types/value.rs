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

impl Value {
    /// Canonical SQL comparison — THE single source of truth for value
    /// ordering and equality across the engine. `eval_compare` (filter and
    /// join predicates), `Sort`, MIN/MAX aggregation, and join-key matching
    /// all derive from this so every execution strategy stays equivalent.
    ///
    /// Returns `None` when the pair is not comparable under SQL semantics:
    /// either operand is NULL (the comparison is UNKNOWN) or the types are
    /// incompatible (e.g. Int vs Varchar). Callers pick the policy —
    /// predicates treat `None` as UNKNOWN (row dropped); `Sort` asserts
    /// incomparable non-NULL pairs can't happen (single-typed columns).
    ///
    /// Numerics compare by mathematical value across representations:
    /// Int32/Int64 promote to i64; Int↔Decimal and cross-scale Decimals
    /// compare exactly via i128 widening — |mantissa| < 2^63 times
    /// 10^MAX_SCALE (= 1e18) stays below i128::MAX, so no overflow and no
    /// rounding.
    pub fn compare_sql(&self, other: &Value) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Int32(a), Value::Int32(b)) => Some(a.cmp(b)),
            (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
            (Value::Int32(a), Value::Int64(b)) => Some((*a as i64).cmp(b)),
            (Value::Int64(a), Value::Int32(b)) => Some(a.cmp(&(*b as i64))),
            (Value::Varchar(a), Value::Varchar(b)) => Some(a.cmp(b)),
            (Value::Char(a), Value::Char(b)) => Some(a.cmp(b)),
            // Char and Varchar are one comparable string domain (values are
            // stored unpadded, so plain string compare is exact). Keeps
            // `WHERE char_col = 'lit'` (literal binds Varchar) meaningful.
            (Value::Varchar(a), Value::Char(b)) => Some(a.cmp(b)),
            (Value::Char(a), Value::Varchar(b)) => Some(a.cmp(b)),
            (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
            (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
            (Value::Decimal(a), Value::Decimal(b)) => Some(compare_decimals(a, b)),
            (Value::Int32(a), Value::Decimal(d)) => Some(compare_int_decimal(*a as i64, d)),
            (Value::Int64(a), Value::Decimal(d)) => Some(compare_int_decimal(*a, d)),
            (Value::Decimal(d), Value::Int32(b)) => {
                Some(compare_int_decimal(*b as i64, d).reverse())
            }
            (Value::Decimal(d), Value::Int64(b)) => Some(compare_int_decimal(*b, d).reverse()),
            _ => None,
        }
    }

    /// Canonical join-key form: two values normalize to the same key IFF
    /// `compare_sql` says they are Equal. Lets `HashJoin`'s hash-table
    /// matching (derived `Hash`/`Eq` on the normalized value) agree exactly
    /// with the predicate-driven strategies — strategy equivalence (E11).
    ///
    /// Int32 widens to Int64; Decimals strip trailing fractional zeros, and
    /// integral Decimals collapse to Int64 (the mantissa IS an i64, so it
    /// always fits); Char collapses to Varchar (one string domain). All
    /// other variants are already canonical.
    pub fn join_key_normalized(&self) -> Value {
        match self {
            Value::Int32(a) => Value::Int64(*a as i64),
            Value::Char(s) => Value::Varchar(s.clone()),
            Value::Decimal(d) => {
                let mut mantissa = d.mantissa();
                let mut scale = d.scale();
                // Strip trailing zeros: 5.00 → 5, 1.50 → 1.5. Bounded by
                // MAX_SCALE iterations.
                while scale > 0 && mantissa % 10 == 0 {
                    mantissa /= 10;
                    scale -= 1;
                }
                if scale == 0 {
                    Value::Int64(mantissa)
                } else {
                    Value::Decimal(Decimal::from_i64_with_scale(mantissa, scale))
                }
            }
            other => other.clone(),
        }
    }

    /// Value-preserving conversion to `ty`. `Some` only when the converted
    /// value is mathematically equal to `self` (no truncation, no rounding);
    /// `None` means no value of type `ty` can equal `self` — callers treat
    /// that as "matches nothing" (e.g. an Int64 join key of 5 billion can
    /// never equal any Int32). Used to encode index-probe keys whose source
    /// column type differs from the indexed column's type.
    pub fn coerce_exact(&self, ty: &ColumnType) -> Option<Value> {
        match (self, ty) {
            (Value::Int64(n), ColumnType::Int32) => i32::try_from(*n).ok().map(Value::Int32),
            (Value::Int32(n), ColumnType::Int64) => Some(Value::Int64(*n as i64)),
            (Value::Int32(n), ColumnType::Decimal { scale, .. }) => {
                int_to_decimal_exact(*n as i64, *scale)
            }
            (Value::Int64(n), ColumnType::Decimal { scale, .. }) => {
                int_to_decimal_exact(*n, *scale)
            }
            (Value::Decimal(d), ColumnType::Int32) => {
                decimal_to_int_exact(d).and_then(|n| i32::try_from(n).ok().map(Value::Int32))
            }
            (Value::Decimal(d), ColumnType::Int64) => decimal_to_int_exact(d).map(Value::Int64),
            (Value::Decimal(d), ColumnType::Decimal { scale, .. }) if d.scale() != *scale => {
                rescale_decimal_exact(d, *scale)
            }
            // One string domain, two storage variants (both unpadded).
            (Value::Varchar(s), ColumnType::Char(_)) => Some(Value::Char(s.clone())),
            (Value::Char(s), ColumnType::Varchar(_)) => Some(Value::Varchar(s.clone())),
            // Same-shape values (incl. same-scale Decimal) pass through;
            // anything else is not representable in `ty`.
            (v, _) if v.matches(ty) => Some(v.clone()),
            _ => None,
        }
    }
}

/// Exact cross-scale Decimal compare: m_a/10^s_a vs m_b/10^s_b ⟺
/// m_a·10^s_b vs m_b·10^s_a in i128 (overflow-free, see `compare_sql`).
fn compare_decimals(a: &Decimal, b: &Decimal) -> std::cmp::Ordering {
    if a.scale() == b.scale() {
        return a.mantissa().cmp(&b.mantissa());
    }
    let lhs = a.mantissa() as i128 * pow10_i128(b.scale());
    let rhs = b.mantissa() as i128 * pow10_i128(a.scale());
    lhs.cmp(&rhs)
}

/// Exact Int-vs-Decimal compare: i vs m/10^s ⟺ i·10^s vs m in i128.
fn compare_int_decimal(i: i64, d: &Decimal) -> std::cmp::Ordering {
    let lhs = i as i128 * pow10_i128(d.scale());
    let rhs = d.mantissa() as i128;
    lhs.cmp(&rhs)
}

fn pow10_i128(scale: u8) -> i128 {
    debug_assert!(scale <= Decimal::MAX_SCALE, "scale {} out of range", scale);
    10i128.pow(scale as u32)
}

/// `i` as a Decimal with exactly `scale`: i·10^scale, `None` on i64
/// mantissa overflow (then no Decimal at that scale equals `i`).
fn int_to_decimal_exact(i: i64, scale: u8) -> Option<Value> {
    let mantissa = i.checked_mul(10i64.checked_pow(scale as u32)?)?;
    Some(Value::Decimal(Decimal::from_i64_with_scale(
        mantissa, scale,
    )))
}

/// The Decimal's integer value when it has no fractional part, else None.
fn decimal_to_int_exact(d: &Decimal) -> Option<i64> {
    let divisor = 10i64.pow(d.scale() as u32);
    if d.mantissa() % divisor == 0 {
        Some(d.mantissa() / divisor)
    } else {
        None
    }
}

/// Rescale to `scale` only when exact: widening multiplies the mantissa
/// (None on overflow); narrowing requires the dropped digits be zero.
fn rescale_decimal_exact(d: &Decimal, scale: u8) -> Option<Value> {
    if scale > d.scale() {
        let factor = 10i64.checked_pow((scale - d.scale()) as u32)?;
        let mantissa = d.mantissa().checked_mul(factor)?;
        Some(Value::Decimal(Decimal::from_i64_with_scale(
            mantissa, scale,
        )))
    } else {
        let factor = 10i64.pow((d.scale() - scale) as u32);
        if d.mantissa() % factor == 0 {
            Some(Value::Decimal(Decimal::from_i64_with_scale(
                d.mantissa() / factor,
                scale,
            )))
        } else {
            None
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

    // ---- compare_sql: the canonical comparator ----

    fn dec(mantissa: i64, scale: u8) -> Value {
        Value::Decimal(Decimal::from_i64_with_scale(mantissa, scale))
    }

    /// NULL on either side is UNKNOWN — never an ordering.
    #[test]
    fn compare_sql_null_is_unknown() {
        assert_eq!(Value::Null.compare_sql(&Value::Int32(1)), None);
        assert_eq!(Value::Int32(1).compare_sql(&Value::Null), None);
        assert_eq!(Value::Null.compare_sql(&Value::Null), None);
    }

    /// Cross-representation numerics compare by mathematical value:
    /// Int32↔Int64 promotion, Int↔Decimal, and cross-scale Decimals.
    #[test]
    fn compare_sql_numeric_promotions() {
        use std::cmp::Ordering::*;
        assert_eq!(Value::Int32(5).compare_sql(&Value::Int64(5)), Some(Equal));
        assert_eq!(Value::Int64(4).compare_sql(&Value::Int32(5)), Some(Less));
        // 5 vs 5.00 (Int vs Decimal, both directions).
        assert_eq!(Value::Int64(5).compare_sql(&dec(500, 2)), Some(Equal));
        assert_eq!(dec(500, 2).compare_sql(&Value::Int32(5)), Some(Equal));
        // 0 < 0.01: the O2 shape `WHERE c_balance > 0` must work.
        assert_eq!(Value::Int64(0).compare_sql(&dec(1, 2)), Some(Less));
        // Cross-scale Decimals compare exactly: 1.50 == 1.5, 0.999 < 1.0.
        assert_eq!(dec(150, 2).compare_sql(&dec(15, 1)), Some(Equal));
        assert_eq!(dec(999, 3).compare_sql(&dec(10, 1)), Some(Less));
        // Extreme mantissa/scale spread must not overflow (i128 widening).
        assert_eq!(
            dec(i64::MAX, 0).compare_sql(&dec(i64::MIN, 18)),
            Some(Greater)
        );
    }

    /// Ordering exists for every same-typed orderable pair — including the
    /// Timestamp/Char/Bytes gaps eval_compare used to silently drop (O2).
    #[test]
    fn compare_sql_covers_all_orderable_types() {
        use std::cmp::Ordering::*;
        assert_eq!(
            Value::Timestamp(1).compare_sql(&Value::Timestamp(2)),
            Some(Less)
        );
        assert_eq!(
            Value::Char("a".into()).compare_sql(&Value::Char("b".into())),
            Some(Less)
        );
        assert_eq!(
            Value::Bytes(vec![1]).compare_sql(&Value::Bytes(vec![2])),
            Some(Less)
        );
        assert_eq!(
            Value::Boolean(false).compare_sql(&Value::Boolean(true)),
            Some(Less)
        );
    }

    /// Type-incompatible pairs are UNKNOWN, not false-equal or false-less:
    /// the negative space of the comparison matrix.
    #[test]
    fn compare_sql_incompatible_pairs_are_unknown() {
        assert_eq!(
            Value::Int32(1).compare_sql(&Value::Varchar("1".into())),
            None
        );
        assert_eq!(Value::Timestamp(5).compare_sql(&Value::Int64(5)), None);
        assert_eq!(Value::Boolean(true).compare_sql(&Value::Int32(1)), None);
    }

    /// Char and Varchar form one string domain (both stored unpadded):
    /// `WHERE char_col = 'lit'` binds the literal Varchar and must match.
    #[test]
    fn compare_sql_char_varchar_one_domain() {
        use std::cmp::Ordering::*;
        assert_eq!(
            Value::Varchar("a".into()).compare_sql(&Value::Char("a".into())),
            Some(Equal)
        );
        assert_eq!(
            Value::Char("a".into()).compare_sql(&Value::Varchar("b".into())),
            Some(Less)
        );
    }

    /// THE property join-key matching relies on: two values normalize to
    /// the same key IFF compare_sql says Equal. Checked over a catalog of
    /// pairs covering every equality-relevant representation split.
    #[test]
    fn join_key_normalized_agrees_with_compare_sql() {
        let values = [
            Value::Int32(5),
            Value::Int64(5),
            Value::Int32(6),
            dec(500, 2), // 5.00
            dec(5, 0),   // 5
            dec(550, 2), // 5.50
            dec(55, 1),  // 5.5
            dec(0, 2),   // 0.00
            Value::Int64(0),
            Value::Timestamp(5),
            Value::Varchar("5".into()),
            Value::Char("5".into()),
            Value::Boolean(true),
        ];
        for a in &values {
            for b in &values {
                let cmp_equal = a.compare_sql(b) == Some(std::cmp::Ordering::Equal);
                let norm_equal = a.join_key_normalized() == b.join_key_normalized();
                assert_eq!(
                    cmp_equal, norm_equal,
                    "normalize/compare disagree for {:?} vs {:?}",
                    a, b
                );
            }
        }
    }

    /// Negative mantissas strip trailing zeros correctly.
    #[test]
    fn join_key_normalized_negative_decimal() {
        // -15.00 → -15 (integral → Int64); -15.5 keeps its fraction.
        assert_eq!(dec(-1500, 2).join_key_normalized(), Value::Int64(-15));
        assert_eq!(dec(-155, 1).join_key_normalized(), dec(-155, 1));
    }

    // ---- coerce_exact ----

    /// Exact conversions succeed; value-changing ones return None.
    #[test]
    fn coerce_exact_int_and_decimal() {
        // Int64 → Int32 in range / out of range.
        assert_eq!(
            Value::Int64(5).coerce_exact(&ColumnType::Int32),
            Some(Value::Int32(5))
        );
        assert_eq!(
            Value::Int64(5_000_000_000).coerce_exact(&ColumnType::Int32),
            None
        );
        // Int → Decimal(scale 2): 5 → 5.00.
        assert_eq!(
            Value::Int32(5).coerce_exact(&ColumnType::Decimal {
                precision: 10,
                scale: 2
            }),
            Some(dec(500, 2))
        );
        // Decimal → Int only when integral.
        assert_eq!(
            dec(500, 2).coerce_exact(&ColumnType::Int64),
            Some(Value::Int64(5))
        );
        assert_eq!(dec(550, 2).coerce_exact(&ColumnType::Int64), None);
        // Decimal rescale: widening always exact, narrowing only when the
        // dropped digits are zero.
        assert_eq!(
            dec(55, 1).coerce_exact(&ColumnType::Decimal {
                precision: 10,
                scale: 2
            }),
            Some(dec(550, 2))
        );
        assert_eq!(
            dec(550, 2).coerce_exact(&ColumnType::Decimal {
                precision: 10,
                scale: 1
            }),
            Some(dec(55, 1))
        );
        assert_eq!(
            dec(555, 2).coerce_exact(&ColumnType::Decimal {
                precision: 10,
                scale: 1
            }),
            None
        );
        // Incompatible shapes.
        assert_eq!(
            Value::Varchar("5".into()).coerce_exact(&ColumnType::Int32),
            None
        );
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
