//! Constraint check helpers used by `Table::insert` / `update_*`.
//!
//! Each helper validates one well-defined property and returns
//! `Error::ConstraintViolation` with a typed `ConstraintRule` if the property
//! is violated. The chokepoint pattern (Postgres `ExecConstraints`,
//! Cockroach `row.check.*`) keeps Phase 11's SQL surface trivial: parser
//! emits values, `Table::insert` runs each helper in sequence, errors
//! propagate as a single uniform shape.
//!
//! Helpers that operate on a whole row and helpers that operate on a single
//! column are both provided. The single-column variants are used by partial
//! updates (`Table::update_columns`), where re-validating untouched columns
//! would be wasteful and wrong (existing values may already be at-bound).

use crate::catalog::Schema;
use crate::common::{ConstraintRule, Error, Result};
use crate::types::{ColumnType, Decimal, Value};

/// `values.len() == schema.columns.len()`.
pub fn check_arity(schema: &Schema, values: &[Value]) -> Result<()> {
    if values.len() != schema.columns.len() {
        return Err(Error::ConstraintViolation {
            column: format!("<row of {}>", schema.name),
            rule: ConstraintRule::Arity {
                expected: schema.columns.len(),
                actual: values.len(),
            },
        });
    }
    Ok(())
}

/// Every NOT NULL column without a default must have a non-NULL value.
/// Defaults are applied by `apply_defaults` *before* this check, so by the
/// time we get here, any remaining NULL in a non-nullable column means the
/// caller failed to provide a value.
pub fn check_nullability(schema: &Schema, values: &[Value]) -> Result<()> {
    for (col, val) in schema.columns.iter().zip(values) {
        if matches!(val, Value::Null) && !col.nullable && col.default.is_none() {
            return Err(Error::ConstraintViolation {
                column: col.name.clone(),
                rule: ConstraintRule::NotNull,
            });
        }
    }
    Ok(())
}

/// Every value's variant must match its column's type (or be NULL).
pub fn check_type_compat(schema: &Schema, values: &[Value]) -> Result<()> {
    for (idx, (col, val)) in schema.columns.iter().zip(values).enumerate() {
        check_type_compat_one(schema, idx, val)?;
        let _ = col; // keep iterator paired
    }
    Ok(())
}

/// Single-column variant of `check_type_compat`. Used by partial updates.
pub fn check_type_compat_one(schema: &Schema, col_idx: usize, val: &Value) -> Result<()> {
    let col = &schema.columns[col_idx];
    if !val.matches(&col.ty) {
        return Err(Error::ConstraintViolation {
            column: col.name.clone(),
            rule: ConstraintRule::TypeMismatch {
                expected: col.ty,
                actual: val.type_of(),
            },
        });
    }
    Ok(())
}

/// Length / precision bounds: Varchar/Char/Bytes lengths, decimal precision.
/// (Decimal scale is checked by `check_type_compat` since matches() includes
/// scale agreement; precision is bounds territory.)
pub fn check_value_bounds(schema: &Schema, values: &[Value]) -> Result<()> {
    for (idx, val) in values.iter().enumerate() {
        check_value_bounds_one(schema, idx, val)?;
    }
    Ok(())
}

/// Single-column variant of `check_value_bounds`.
pub fn check_value_bounds_one(schema: &Schema, col_idx: usize, val: &Value) -> Result<()> {
    let col = &schema.columns[col_idx];
    match (&col.ty, val) {
        (ColumnType::Varchar(max), Value::Varchar(s)) if s.len() > *max as usize => {
            return Err(Error::ConstraintViolation {
                column: col.name.clone(),
                rule: ConstraintRule::VarcharTooLong { max: *max },
            });
        }
        (ColumnType::Char(max), Value::Char(s)) if s.len() > *max as usize => {
            return Err(Error::ConstraintViolation {
                column: col.name.clone(),
                rule: ConstraintRule::CharTooLong { max: *max },
            });
        }
        (ColumnType::Bytes(max), Value::Bytes(b)) if b.len() > *max as usize => {
            return Err(Error::ConstraintViolation {
                column: col.name.clone(),
                rule: ConstraintRule::BytesTooLong { max: *max },
            });
        }
        (ColumnType::Decimal { precision, scale }, Value::Decimal(d)) => {
            if d.scale() != *scale {
                return Err(Error::ConstraintViolation {
                    column: col.name.clone(),
                    rule: ConstraintRule::DecimalScaleMismatch {
                        expected: *scale,
                        actual: d.scale(),
                    },
                });
            }
            if d.exceeds_precision(*precision) {
                return Err(Error::ConstraintViolation {
                    column: col.name.clone(),
                    rule: ConstraintRule::DecimalPrecisionExceeded { max: *precision },
                });
            }
            // Suppress unused warning for the constant fields when the i128
            // intermediate isn't needed.
            let _ = Decimal::MAX_PRECISION;
        }
        _ => {}
    }
    Ok(())
}

/// Every PK column must be non-NULL. PKs cannot be NULL by SQL convention
/// (and ours: NULL has no defined sort order against itself).
pub fn check_pk_not_null(schema: &Schema, values: &[Value]) -> Result<()> {
    for &pk_idx in &schema.primary_key {
        if matches!(values[pk_idx], Value::Null) {
            return Err(Error::ConstraintViolation {
                column: schema.columns[pk_idx].name.clone(),
                rule: ConstraintRule::PkNotNull,
            });
        }
    }
    Ok(())
}

/// Substitute defaults for any `Value::Null` values in non-nullable columns
/// that have a declared default. Returns a new `Vec<Value>`; doesn't mutate.
///
/// Run *before* `check_nullability` — that's how a NOT NULL column with a
/// default doesn't error when the caller passes NULL. The default fills in.
pub fn apply_defaults(schema: &Schema, values: &[Value]) -> Vec<Value> {
    if values.len() != schema.columns.len() {
        return values.to_vec(); // arity error will surface in check_arity
    }
    schema
        .columns
        .iter()
        .zip(values)
        .map(|(col, val)| match val {
            Value::Null => col.default.clone().unwrap_or(Value::Null),
            other => other.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, TableId};

    fn schema_with_one_int_pk() -> Schema {
        Schema {
            name: "t".into(),
            table_id: TableId(1),
            columns: vec![ColumnDef {
                name: "id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            }],
            primary_key: vec![0],
        }
    }

    fn three_col_schema() -> Schema {
        Schema {
            name: "t".into(),
            table_id: TableId(1),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "name".into(),
                    ty: ColumnType::Varchar(5),
                    nullable: true,
                    default: None,
                },
                ColumnDef {
                    name: "amount".into(),
                    ty: ColumnType::Decimal {
                        precision: 5,
                        scale: 2,
                    },
                    nullable: false,
                    default: Some(Value::Decimal(Decimal::from_i64_with_scale(0, 2))),
                },
            ],
            primary_key: vec![0],
        }
    }

    #[test]
    fn check_arity_matching_lengths_passes() {
        let s = three_col_schema();
        let v = vec![Value::Int32(1), Value::Null, Value::Null];
        assert!(check_arity(&s, &v).is_ok());
    }

    #[test]
    fn check_arity_mismatched_lengths_errors() {
        let s = three_col_schema();
        let err = check_arity(&s, &[Value::Int32(1)]).unwrap_err();
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::Arity {
                    expected: 3,
                    actual: 1
                },
                ..
            }
        ));
    }

    #[test]
    fn check_nullability_null_in_non_nullable_no_default_errors() {
        let s = schema_with_one_int_pk();
        let err = check_nullability(&s, &[Value::Null]).unwrap_err();
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::NotNull,
                ..
            }
        ));
    }

    #[test]
    fn check_nullability_null_in_nullable_passes() {
        let s = three_col_schema();
        // Column 1 (name) is nullable.
        let v = vec![
            Value::Int32(1),
            Value::Null,
            Value::Decimal(Decimal::from_i64_with_scale(0, 2)),
        ];
        assert!(check_nullability(&s, &v).is_ok());
    }

    #[test]
    fn check_nullability_null_with_default_passes() {
        // Column 2 (amount) has a default, so NULL is OK here (default applies).
        let s = three_col_schema();
        let v = vec![Value::Int32(1), Value::Null, Value::Null];
        assert!(check_nullability(&s, &v).is_ok());
    }

    #[test]
    fn check_type_compat_wrong_variant_errors() {
        let s = schema_with_one_int_pk();
        let err = check_type_compat(&s, &[Value::Boolean(true)]).unwrap_err();
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::TypeMismatch { .. },
                ..
            }
        ));
    }

    #[test]
    fn check_value_bounds_varchar_too_long_errors() {
        let s = three_col_schema();
        let v = vec![
            Value::Int32(1),
            Value::Varchar("toolongtoooo".into()), // > 5
            Value::Decimal(Decimal::from_i64_with_scale(0, 2)),
        ];
        let err = check_value_bounds(&s, &v).unwrap_err();
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::VarcharTooLong { max: 5 },
                ..
            }
        ));
    }

    #[test]
    fn check_value_bounds_decimal_scale_mismatch_errors() {
        let s = three_col_schema();
        let v = vec![
            Value::Int32(1),
            Value::Null,
            Value::Decimal(Decimal::from_i64_with_scale(123, 3)), // scale=3, col wants scale=2
        ];
        let err = check_value_bounds(&s, &v).unwrap_err();
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::DecimalScaleMismatch {
                    expected: 2,
                    actual: 3
                },
                ..
            }
        ));
    }

    #[test]
    fn check_value_bounds_decimal_precision_exceeded_errors() {
        let s = three_col_schema();
        let v = vec![
            Value::Int32(1),
            Value::Null,
            // precision=5 means max |mantissa| = 99999. 100000 exceeds.
            Value::Decimal(Decimal::from_i64_with_scale(100_000, 2)),
        ];
        let err = check_value_bounds(&s, &v).unwrap_err();
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::DecimalPrecisionExceeded { max: 5 },
                ..
            }
        ));
    }

    #[test]
    fn check_pk_not_null_errors() {
        let s = schema_with_one_int_pk();
        let err = check_pk_not_null(&s, &[Value::Null]).unwrap_err();
        assert!(matches!(
            err,
            Error::ConstraintViolation {
                rule: ConstraintRule::PkNotNull,
                ..
            }
        ));
    }

    #[test]
    fn apply_defaults_substitutes_for_null_with_default() {
        let s = three_col_schema();
        let v = vec![Value::Int32(1), Value::Null, Value::Null];
        let out = apply_defaults(&s, &v);
        // Column 1 stays NULL (no default), column 2 gets default.
        assert_eq!(out[0], Value::Int32(1));
        assert_eq!(out[1], Value::Null);
        assert_eq!(out[2], Value::Decimal(Decimal::from_i64_with_scale(0, 2)));
    }

    #[test]
    fn apply_defaults_doesnt_touch_present_values() {
        let s = three_col_schema();
        let original = vec![
            Value::Int32(1),
            Value::Varchar("ok".into()),
            Value::Decimal(Decimal::from_i64_with_scale(42, 2)),
        ];
        assert_eq!(apply_defaults(&s, &original), original);
    }
}
