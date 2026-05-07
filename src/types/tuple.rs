//! Tuple encoding for row-blob storage.
//!
//! Encodes a row's values into a single byte string used as the engine's
//! storage *value* by `RowLayout`. Distinct from `keyenc`:
//!   - `keyenc` produces sortable bytes for storage *keys* (PKs, index keys).
//!     Byte-lex order must match logical order across all components.
//!   - `tuple` produces compact bytes for storage *values*. No ordering
//!     requirement — optimizes for size and partial-decode speed.
//!
//! # Layout
//!
//! ```text
//! [null_bitmap: ⌈n_cols/8⌉ bytes] [value bytes in schema order]
//! ```
//!
//! - The bitmap covers *all* columns. Bit `i` of byte `i/8` is set iff the
//!   value at column `i` is NULL. (~1 bit/column wasted at most; simpler than
//!   tracking which columns are nullable.)
//! - Non-null values are written in column order:
//!   - `Int32`: 4 bytes LE
//!   - `Int64` / `Timestamp` / `Decimal` (mantissa): 8 bytes LE
//!   - `Boolean`: 1 byte (0x00 or 0x01)
//!   - `Char(n)`: exactly `n` bytes, right-padded with `0x00`
//!   - `Varchar(n)` / `Bytes(n)`: `u16 length || bytes`
//! - NULL values contribute nothing to the value bytes; the bitmap bit is
//!   the entire signal.

use crate::common::{Error, Result};
use crate::types::{ColumnType, Decimal, Value};

/// Encode a row of values into the tuple bytes.
///
/// `types` and `values` must agree in arity. Each value must be either NULL
/// or shape-compatible with its column type — same compatibility rules as
/// `Value::matches`. Length bounds (`Varchar(n)`, `Char(n)`, `Bytes(n)`)
/// are enforced here; the constraint layer in Task 9.6 enforces them again
/// pre-encode for better error reporting, but tuple encoding refuses to
/// silently truncate or extend.
pub fn encode(types: &[ColumnType], values: &[Value]) -> Result<Vec<u8>> {
    if types.len() != values.len() {
        return Err(Error::StorageCorrupted(format!(
            "tuple::encode: arity mismatch: {} types, {} values",
            types.len(),
            values.len()
        )));
    }
    let n = types.len();
    let bitmap_len = bitmap_byte_count(n);
    let mut out = vec![0u8; bitmap_len];

    for (i, (ty, v)) in types.iter().zip(values).enumerate() {
        if matches!(v, Value::Null) {
            out[i / 8] |= 1 << (i % 8);
            continue;
        }
        encode_value(ty, v, &mut out)?;
    }
    Ok(out)
}

/// Decode a tuple back into a vector of values.
pub fn decode(types: &[ColumnType], bytes: &[u8]) -> Result<Vec<Value>> {
    let n = types.len();
    let bitmap_len = bitmap_byte_count(n);
    if bytes.len() < bitmap_len {
        return Err(Error::StorageCorrupted(format!(
            "tuple::decode: not enough bytes for null bitmap: need {}, got {}",
            bitmap_len,
            bytes.len()
        )));
    }
    let bitmap = &bytes[..bitmap_len];
    let mut cursor = bitmap_len;
    let mut out = Vec::with_capacity(n);

    for (i, ty) in types.iter().enumerate() {
        if is_null_bit_set(bitmap, i) {
            out.push(Value::Null);
            continue;
        }
        let (value, consumed) = decode_value(ty, &bytes[cursor..])?;
        out.push(value);
        cursor += consumed;
    }
    if cursor != bytes.len() {
        return Err(Error::StorageCorrupted(format!(
            "tuple::decode: trailing bytes: {} unread",
            bytes.len() - cursor
        )));
    }
    Ok(out)
}

/// Decode a single column out of a tuple without materializing the others.
///
/// Used by `RowLayout::get_projection` to read just the requested columns.
/// Walks left-to-right, skipping each prior column by its byte size (computed
/// from the type, or read from the length prefix for varlen types) — never
/// allocates or copies a column's payload bytes for skipped columns.
pub fn decode_column(types: &[ColumnType], bytes: &[u8], col_idx: usize) -> Result<Value> {
    if col_idx >= types.len() {
        return Err(Error::StorageCorrupted(format!(
            "tuple::decode_column: index {} out of range for {} columns",
            col_idx,
            types.len()
        )));
    }
    let n = types.len();
    let bitmap_len = bitmap_byte_count(n);
    if bytes.len() < bitmap_len {
        return Err(Error::StorageCorrupted(
            "tuple::decode_column: not enough bytes for null bitmap".into(),
        ));
    }
    let bitmap = &bytes[..bitmap_len];

    // Fast path: target column is NULL — no need to walk preceding columns
    // at all. (This is the killer optimization for sparse-row OLTP workloads.)
    if is_null_bit_set(bitmap, col_idx) {
        return Ok(Value::Null);
    }

    let mut cursor = bitmap_len;
    for (i, ty) in types.iter().enumerate() {
        if is_null_bit_set(bitmap, i) {
            continue; // NULL: no bytes to skip
        }
        if i == col_idx {
            let (value, _) = decode_value(ty, &bytes[cursor..])?;
            return Ok(value);
        }
        cursor += value_byte_size(ty, &bytes[cursor..])?;
    }
    unreachable!("col_idx range checked at function entry");
}

// ---- internals ------------------------------------------------------------

fn bitmap_byte_count(n: usize) -> usize {
    n.div_ceil(8)
}

fn is_null_bit_set(bitmap: &[u8], col: usize) -> bool {
    (bitmap[col / 8] >> (col % 8)) & 1 == 1
}

fn encode_value(ty: &ColumnType, value: &Value, out: &mut Vec<u8>) -> Result<()> {
    match (ty, value) {
        (ColumnType::Int32, Value::Int32(n)) => {
            out.extend_from_slice(&n.to_le_bytes());
            Ok(())
        }
        (ColumnType::Int64, Value::Int64(n)) => {
            out.extend_from_slice(&n.to_le_bytes());
            Ok(())
        }
        (ColumnType::Boolean, Value::Boolean(b)) => {
            out.push(if *b { 0x01 } else { 0x00 });
            Ok(())
        }
        (ColumnType::Timestamp, Value::Timestamp(t)) => {
            out.extend_from_slice(&t.to_le_bytes());
            Ok(())
        }
        (ColumnType::Decimal { .. }, Value::Decimal(d)) => {
            // Scale is recovered from the column type during decode; we only
            // need the mantissa bytes here. Constraint layer (Task 9.6)
            // enforces that d.scale() == column scale before encoding.
            out.extend_from_slice(&d.mantissa().to_le_bytes());
            Ok(())
        }
        (ColumnType::Char(n), Value::Char(s)) => {
            if s.as_bytes().contains(&0x00) {
                return Err(Error::StorageCorrupted(
                    "tuple::encode Char: embedded NUL byte not allowed".into(),
                ));
            }
            if s.len() > *n as usize {
                return Err(Error::StorageCorrupted(format!(
                    "tuple::encode Char: value length {} exceeds Char({})",
                    s.len(),
                    n
                )));
            }
            out.extend_from_slice(s.as_bytes());
            let pad = *n as usize - s.len();
            out.resize(out.len() + pad, 0x00);
            Ok(())
        }
        (ColumnType::Varchar(n), Value::Varchar(s)) => {
            if s.len() > *n as usize {
                return Err(Error::StorageCorrupted(format!(
                    "tuple::encode Varchar: value length {} exceeds Varchar({})",
                    s.len(),
                    n
                )));
            }
            out.extend_from_slice(&(s.len() as u16).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
            Ok(())
        }
        (ColumnType::Bytes(n), Value::Bytes(b)) => {
            if b.len() > *n as usize {
                return Err(Error::StorageCorrupted(format!(
                    "tuple::encode Bytes: value length {} exceeds Bytes({})",
                    b.len(),
                    n
                )));
            }
            out.extend_from_slice(&(b.len() as u16).to_le_bytes());
            out.extend_from_slice(b);
            Ok(())
        }
        (t, v) => Err(Error::StorageCorrupted(format!(
            "tuple::encode: type mismatch: value {:?} cannot encode under column type {:?}",
            v, t
        ))),
    }
}

fn decode_value(ty: &ColumnType, bytes: &[u8]) -> Result<(Value, usize)> {
    match ty {
        ColumnType::Int32 => {
            if bytes.len() < 4 {
                return Err(Error::StorageCorrupted("tuple::decode Int32: need 4 bytes".into()));
            }
            let n = i32::from_le_bytes(bytes[..4].try_into().unwrap());
            Ok((Value::Int32(n), 4))
        }
        ColumnType::Int64 => {
            if bytes.len() < 8 {
                return Err(Error::StorageCorrupted("tuple::decode Int64: need 8 bytes".into()));
            }
            let n = i64::from_le_bytes(bytes[..8].try_into().unwrap());
            Ok((Value::Int64(n), 8))
        }
        ColumnType::Boolean => {
            if bytes.is_empty() {
                return Err(Error::StorageCorrupted(
                    "tuple::decode Boolean: need 1 byte".into(),
                ));
            }
            match bytes[0] {
                0x00 => Ok((Value::Boolean(false), 1)),
                0x01 => Ok((Value::Boolean(true), 1)),
                b => Err(Error::StorageCorrupted(format!(
                    "tuple::decode Boolean: invalid byte 0x{:02x}",
                    b
                ))),
            }
        }
        ColumnType::Timestamp => {
            if bytes.len() < 8 {
                return Err(Error::StorageCorrupted(
                    "tuple::decode Timestamp: need 8 bytes".into(),
                ));
            }
            let t = i64::from_le_bytes(bytes[..8].try_into().unwrap());
            Ok((Value::Timestamp(t), 8))
        }
        ColumnType::Decimal { scale, .. } => {
            if bytes.len() < 8 {
                return Err(Error::StorageCorrupted(
                    "tuple::decode Decimal: need 8 bytes".into(),
                ));
            }
            let mantissa = i64::from_le_bytes(bytes[..8].try_into().unwrap());
            Ok((
                Value::Decimal(Decimal::from_i64_with_scale(mantissa, *scale)),
                8,
            ))
        }
        ColumnType::Char(n) => {
            let len = *n as usize;
            if bytes.len() < len {
                return Err(Error::StorageCorrupted(format!(
                    "tuple::decode Char: need {} bytes, got {}",
                    len,
                    bytes.len()
                )));
            }
            let raw = &bytes[..len];
            // Strip trailing 0x00 padding — embedded NULs are forbidden at
            // encode time, so any trailing 0x00 bytes are guaranteed padding.
            let end = raw.iter().rposition(|&b| b != 0x00).map_or(0, |i| i + 1);
            let s = String::from_utf8(raw[..end].to_vec()).map_err(|e| {
                Error::StorageCorrupted(format!("tuple::decode Char: invalid UTF-8: {}", e))
            })?;
            Ok((Value::Char(s), len))
        }
        ColumnType::Varchar(_) => {
            if bytes.len() < 2 {
                return Err(Error::StorageCorrupted(
                    "tuple::decode Varchar: need length prefix".into(),
                ));
            }
            let len = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
            if bytes.len() < 2 + len {
                return Err(Error::StorageCorrupted(format!(
                    "tuple::decode Varchar: need {} bytes after prefix, got {}",
                    len,
                    bytes.len() - 2
                )));
            }
            let s = String::from_utf8(bytes[2..2 + len].to_vec()).map_err(|e| {
                Error::StorageCorrupted(format!("tuple::decode Varchar: invalid UTF-8: {}", e))
            })?;
            Ok((Value::Varchar(s), 2 + len))
        }
        ColumnType::Bytes(_) => {
            if bytes.len() < 2 {
                return Err(Error::StorageCorrupted(
                    "tuple::decode Bytes: need length prefix".into(),
                ));
            }
            let len = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
            if bytes.len() < 2 + len {
                return Err(Error::StorageCorrupted(format!(
                    "tuple::decode Bytes: need {} bytes after prefix, got {}",
                    len,
                    bytes.len() - 2
                )));
            }
            Ok((Value::Bytes(bytes[2..2 + len].to_vec()), 2 + len))
        }
    }
}

/// Compute the byte size of an encoded value at `bytes` *without copying* the
/// payload. For fixed-width types, returns a constant. For varlen, reads the
/// length prefix and returns `2 + len`. Used by `decode_column` to skip
/// over preceding columns.
fn value_byte_size(ty: &ColumnType, bytes: &[u8]) -> Result<usize> {
    match ty {
        ColumnType::Int32 => Ok(4),
        ColumnType::Int64 | ColumnType::Timestamp | ColumnType::Decimal { .. } => Ok(8),
        ColumnType::Boolean => Ok(1),
        ColumnType::Char(n) => Ok(*n as usize),
        ColumnType::Varchar(_) | ColumnType::Bytes(_) => {
            if bytes.len() < 2 {
                return Err(Error::StorageCorrupted(
                    "tuple::value_byte_size: need length prefix".into(),
                ));
            }
            let len = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
            Ok(2 + len)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_types_schema() -> Vec<ColumnType> {
        vec![
            ColumnType::Int32,
            ColumnType::Int64,
            ColumnType::Boolean,
            ColumnType::Timestamp,
            ColumnType::Decimal {
                precision: 18,
                scale: 4,
            },
            ColumnType::Char(5),
            ColumnType::Varchar(64),
            ColumnType::Bytes(64),
        ]
    }

    fn all_types_row() -> Vec<Value> {
        vec![
            Value::Int32(-7),
            Value::Int64(i64::MIN),
            Value::Boolean(true),
            Value::Timestamp(1_700_000_000_000_000),
            Value::Decimal(Decimal::from_i64_with_scale(12345, 4)),
            Value::Char("ab".into()),
            Value::Varchar("hello".into()),
            Value::Bytes(vec![0x00, 0x01, 0xFF]),
        ]
    }

    /// Encode + decode a multi-column row touching every supported type.
    #[test]
    fn roundtrip_all_types_no_nulls() {
        let types = all_types_schema();
        let values = all_types_row();
        let bytes = encode(&types, &values).unwrap();
        let back = decode(&types, &bytes).unwrap();
        assert_eq!(back, values);
    }

    /// Every column NULL: bitmap is all-ones, value section is empty.
    #[test]
    fn roundtrip_all_nulls() {
        let types = all_types_schema();
        let values: Vec<Value> = (0..types.len()).map(|_| Value::Null).collect();
        let bytes = encode(&types, &values).unwrap();
        // Bitmap should be the only data: ⌈8/8⌉ = 1 byte, all bits set.
        assert_eq!(bytes, vec![0xFF]);
        let back = decode(&types, &bytes).unwrap();
        assert_eq!(back, values);
    }

    /// Mixed null/non-null pattern with varlen columns interleaved.
    #[test]
    fn roundtrip_mixed_nulls() {
        let types = all_types_schema();
        let mut values = all_types_row();
        values[1] = Value::Null; // Int64 null
        values[5] = Value::Null; // Char null
        values[7] = Value::Null; // Bytes null
        let bytes = encode(&types, &values).unwrap();
        let back = decode(&types, &bytes).unwrap();
        assert_eq!(back, values);
    }

    /// Empty schema (zero columns) round-trips to an empty byte string.
    #[test]
    fn roundtrip_empty_schema() {
        let types: Vec<ColumnType> = vec![];
        let values: Vec<Value> = vec![];
        let bytes = encode(&types, &values).unwrap();
        assert!(bytes.is_empty(), "empty schema should produce empty bytes");
        let back = decode(&types, &bytes).unwrap();
        assert!(back.is_empty());
    }

    /// Empty / minimal varlen values: empty Varchar, zero-byte Bytes,
    /// empty Char (which decodes to "" since all padding strips).
    #[test]
    fn roundtrip_empty_varlen_values() {
        let types = vec![
            ColumnType::Varchar(64),
            ColumnType::Bytes(64),
            ColumnType::Char(5),
        ];
        let values = vec![
            Value::Varchar(String::new()),
            Value::Bytes(vec![]),
            Value::Char(String::new()),
        ];
        let bytes = encode(&types, &values).unwrap();
        let back = decode(&types, &bytes).unwrap();
        assert_eq!(back, values);
    }

    /// Bitmap byte-count for various arities. Boundary-tests the ⌈n/8⌉ math.
    #[test]
    fn bitmap_size_at_arity_boundaries() {
        for (n, expected) in [(0, 0), (1, 1), (7, 1), (8, 1), (9, 2), (16, 2), (17, 3)] {
            let types: Vec<ColumnType> = (0..n).map(|_| ColumnType::Int32).collect();
            let values: Vec<Value> = (0..n).map(|i| Value::Int32(i as i32)).collect();
            let bytes = encode(&types, &values).unwrap();
            assert_eq!(
                bytes.len(),
                expected + 4 * n,
                "n={}: expected bitmap={} + values={}",
                n,
                expected,
                4 * n
            );
        }
    }

    // ---- decode_column ----

    /// Partial decode: first column.
    #[test]
    fn decode_column_first() {
        let types = all_types_schema();
        let values = all_types_row();
        let bytes = encode(&types, &values).unwrap();
        let v = decode_column(&types, &bytes, 0).unwrap();
        assert_eq!(v, Value::Int32(-7));
    }

    /// Partial decode: last column. Requires walking past every prior column,
    /// including varlen ones, by reading their length prefixes.
    #[test]
    fn decode_column_last() {
        let types = all_types_schema();
        let values = all_types_row();
        let bytes = encode(&types, &values).unwrap();
        let last = types.len() - 1;
        let v = decode_column(&types, &bytes, last).unwrap();
        assert_eq!(v, Value::Bytes(vec![0x00, 0x01, 0xFF]));
    }

    /// Partial decode of a column that comes after a varlen column. Exercises
    /// the length-prefix-skip path in `value_byte_size`.
    #[test]
    fn decode_column_after_varlen() {
        // Schema: Int32, Varchar, Boolean. Want column 2 (Boolean) — must skip
        // Int32 (fixed 4 bytes) and Varchar (length-prefixed) without copying.
        let types = vec![ColumnType::Int32, ColumnType::Varchar(64), ColumnType::Boolean];
        let values = vec![
            Value::Int32(42),
            Value::Varchar("hello".into()),
            Value::Boolean(true),
        ];
        let bytes = encode(&types, &values).unwrap();
        let v = decode_column(&types, &bytes, 2).unwrap();
        assert_eq!(v, Value::Boolean(true));
    }

    /// Partial decode of a NULL column should hit the bitmap fast path and
    /// return `Value::Null` without touching value bytes.
    #[test]
    fn decode_column_null_returns_null() {
        let types = vec![ColumnType::Int32, ColumnType::Varchar(64), ColumnType::Boolean];
        let values = vec![Value::Int32(42), Value::Null, Value::Boolean(true)];
        let bytes = encode(&types, &values).unwrap();
        let v = decode_column(&types, &bytes, 1).unwrap();
        assert_eq!(v, Value::Null);
    }

    /// Out-of-range col_idx is a programmer error — return error rather than
    /// panic. (Phase 11's planner will trust the schema, but defensive.)
    #[test]
    fn decode_column_out_of_range_errors() {
        let types = vec![ColumnType::Int32];
        let values = vec![Value::Int32(0)];
        let bytes = encode(&types, &values).unwrap();
        let err = decode_column(&types, &bytes, 5)
            .expect_err("out-of-range col_idx should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("out of range")));
    }

    // ---- error paths ----

    #[test]
    fn encode_arity_mismatch_errors() {
        let err = encode(
            &[ColumnType::Int32, ColumnType::Boolean],
            &[Value::Int32(0)],
        )
        .expect_err("arity mismatch should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("arity")));
    }

    #[test]
    fn encode_type_mismatch_errors() {
        let err = encode(&[ColumnType::Int32], &[Value::Boolean(true)])
            .expect_err("type mismatch should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("type mismatch")));
    }

    #[test]
    fn encode_char_embedded_nul_errors() {
        let err = encode(&[ColumnType::Char(5)], &[Value::Char("a\0b".into())])
            .expect_err("embedded NUL should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("NUL")));
    }

    #[test]
    fn encode_varchar_overlength_errors() {
        let huge = "x".repeat(100);
        let err = encode(&[ColumnType::Varchar(10)], &[Value::Varchar(huge)])
            .expect_err("oversize Varchar should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("exceeds Varchar")));
    }

    #[test]
    fn decode_truncated_bitmap_errors() {
        let types = vec![ColumnType::Int32, ColumnType::Int32];
        let err = decode(&types, &[]).expect_err("missing bitmap should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("null bitmap")));
    }

    #[test]
    fn decode_trailing_bytes_errors() {
        let types = vec![ColumnType::Int32];
        let mut bytes = encode(&types, &[Value::Int32(42)]).unwrap();
        bytes.push(0xFF); // extra trailing byte
        let err = decode(&types, &bytes).expect_err("trailing bytes should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("trailing")));
    }

    #[test]
    fn decode_invalid_boolean_byte_errors() {
        // Manual byte construction: 1-bit bitmap (0x00 — column 0 not null),
        // then byte 0x42 as a "Boolean" value.
        let types = vec![ColumnType::Boolean];
        let bytes = vec![0x00, 0x42];
        let err = decode(&types, &bytes).expect_err("invalid Boolean byte should error");
        assert!(matches!(err, Error::StorageCorrupted(ref m) if m.contains("0x42")));
    }
}
