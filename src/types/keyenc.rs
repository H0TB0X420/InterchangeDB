//! Order-preserving composite-key encoder.                                                           
//!             
//! Encodes a tuple of `Value`s into a single `Vec<u8>` such that lexicographic
//! byte order on the encoded bytes matches logical tuple order on the values.                        
//! This is what makes range scans over storage keys (byte-sorted) correspond
//! to range scans over user-facing PK values.                                                        
//!             
//! # Encoding scheme                                                                                 
//!             
//! Each component's encoded bytes:
//!     [null_sentinel: 1 byte] [value_bytes: per-type, only if non-null]
//!                                                                                                   
//! null_sentinel:
//!     0x00 = NULL (no value bytes follow; sorts before any non-null)                                
//!     0x01 = present
//!                                                                                                   
//! Per-type value_bytes (filled in across Steps 4–N):
//!     Int32 / Int64 / Timestamp / Decimal:  big-endian + sign-bit XOR, fixed width                  
//!     Boolean:                               0x00 = false, 0x01 = true                              
//!     Char(n):                               exactly n bytes, zero-padded right                     
//!     Varchar(n) / Bytes(n):                 byte-stuffed escape with terminator                    

use crate::common::{Error, Result};
use crate::types::{ColumnType, Value};

/// Sentinel byte for a NULL component. Sorts before any present value.                               
const NULL_SENTINEL: u8 = 0x00;
/// Sentinel byte for a present (non-null) component.                                                 
const PRESENT_SENTINEL: u8 = 0x01;

/// Encode a tuple of values into a single byte string with the order-preserving
/// property: `encode(a) < encode(b)` lexicographically iff `a < b` logically.
///
/// `types` is required (in addition to `values`) because some encodings need
/// the column type to encode correctly — e.g. `Char(n)` needs `n` to know the
/// fixed width. Symmetric with `decode_key_components`.
pub fn encode_key_components(values: &[&Value], types: &[ColumnType]) -> Result<Vec<u8>> {
    if values.len() != types.len() {
        return Err(Error::StorageCorrupted(format!(
            "encode_key_components: arity mismatch: {} values, {} types",
            values.len(),
            types.len()
        )));
    }
    let mut out = Vec::new();
    for (v, t) in values.iter().zip(types) {
        encode_one(v, t, &mut out)?;
    }
    Ok(out)
}

/// Decode the inverse of `encode_key_components`. Requires the column types
/// in tuple order to know each component's shape.
pub fn decode_key_components(bytes: &[u8], types: &[ColumnType]) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(types.len());
    let mut cursor = 0;
    for ty in types {
        let (value, consumed) = decode_one(&bytes[cursor..], ty)?;
        out.push(value);
        cursor += consumed;
    }
    if cursor != bytes.len() {
        return Err(Error::StorageCorrupted(format!(
            "trailing bytes after decoding {} components: {} unread",
            types.len(),
            bytes.len() - cursor
        )));
    }
    Ok(out)
}

// ---- per-type dispatch ----------------------------------------------------

fn encode_one(value: &Value, ty: &ColumnType, out: &mut Vec<u8>) -> Result<()> {
    // NULL is allowed regardless of column type.
    if matches!(value, Value::Null) {
        out.push(NULL_SENTINEL);
        return Ok(());
    }
    match (value, ty) {
        (Value::Null, _) => unreachable!("handled above"),
        (Value::Int32(n), ColumnType::Int32) => {
            out.push(PRESENT_SENTINEL);
            encode_int32(*n, out);
            Ok(())
        }
        (Value::Boolean(b), ColumnType::Boolean) => {
            out.push(PRESENT_SENTINEL);
            encode_boolean(*b, out);
            Ok(())
        }
        (Value::Int64(n), ColumnType::Int64) => {
            out.push(PRESENT_SENTINEL);
            encode_int64(*n, out);
            Ok(())
        }
        (Value::Timestamp(t), ColumnType::Timestamp) => {
            // Timestamp = i64 microseconds since Unix epoch. Same encoding as Int64.
            out.push(PRESENT_SENTINEL);
            encode_int64(*t, out);
            Ok(())
        }
        (Value::Decimal { mantissa, .. }, ColumnType::Decimal { .. }) => {
            // Cross-scale ordering is undefined; all Decimal values in a single
            // column must share the column's scale (constraint layer enforces).
            // The encoded bytes are just the mantissa as Int64.
            out.push(PRESENT_SENTINEL);
            encode_int64(*mantissa, out);
            Ok(())
        }
        (Value::Char(s), ColumnType::Char(n)) => {
            out.push(PRESENT_SENTINEL);
            encode_char(s, *n, out)?;
            Ok(())
        }
        (Value::Varchar(s), ColumnType::Varchar(_)) => {
            out.push(PRESENT_SENTINEL);
            encode_escaped(s.as_bytes(), out);
            Ok(())
        }
        (Value::Bytes(b), ColumnType::Bytes(_)) => {
            out.push(PRESENT_SENTINEL);
            encode_escaped(b, out);
            Ok(())
        }
        (v, t) => Err(Error::StorageCorrupted(format!(
            "encode_one: type mismatch: value {:?} cannot encode under column type {:?}",
            v, t
        ))),
    }
}
fn decode_one(bytes: &[u8], ty: &ColumnType) -> Result<(Value, usize)> {
    if bytes.is_empty() {
        return Err(Error::StorageCorrupted("decode_one: empty input".into()));
    }
    let sentinel = bytes[0];
    if sentinel == NULL_SENTINEL {
        return Ok((Value::Null, 1));
    }
    if sentinel != PRESENT_SENTINEL {
        return Err(Error::StorageCorrupted(format!(
            "decode_one: invalid sentinel 0x{:02x}",
            sentinel
        )));
    }
    // Sentinel says "present"; consume value_bytes per-type.
    let body = &bytes[1..];
    match ty {
        ColumnType::Int32 => {
            let (n, consumed) = decode_int32(body)?;
            Ok((Value::Int32(n), 1 + consumed))
        }
        ColumnType::Boolean => {
            let (b, consumed) = decode_boolean(body)?;
            Ok((Value::Boolean(b), 1 + consumed))
        }
        ColumnType::Int64 => {
            let (n, consumed) = decode_int64(body)?;
            Ok((Value::Int64(n), 1 + consumed))
        }
        ColumnType::Timestamp => {
            let (t, consumed) = decode_int64(body)?;
            Ok((Value::Timestamp(t), 1 + consumed))
        }
        ColumnType::Decimal { scale, .. } => {
            let (mantissa, consumed) = decode_int64(body)?;
            Ok((Value::Decimal { mantissa, scale: *scale }, 1 + consumed))
        }
        ColumnType::Char(n) => {
            let (s, consumed) = decode_char(body, *n)?;
            Ok((Value::Char(s), 1 + consumed))
        }
        ColumnType::Varchar(_) => {
            let (raw, consumed) = decode_escaped(body)?;
            let s = String::from_utf8(raw).map_err(|e| {
                Error::StorageCorrupted(format!("decode Varchar: invalid UTF-8: {}", e))
            })?;
            Ok((Value::Varchar(s), 1 + consumed))
        }
        ColumnType::Bytes(_) => {
            let (raw, consumed) = decode_escaped(body)?;
            Ok((Value::Bytes(raw), 1 + consumed))
        }
    }
}

// ---- Int32 ----------------------------------------------------------------

/// XOR mask that flips the sign bit of an i32-cast-to-u32. Applied during                            
/// encode (and again during decode — XOR is its own inverse) so that
/// byte-lex order on the encoded bytes matches numeric order on the i32.                             
const INT32_SIGN_MASK: u32 = 1 << 31;

fn encode_int32(n: i32, out: &mut Vec<u8>) {
    let bits = (n as u32) ^ INT32_SIGN_MASK;
    out.extend_from_slice(&bits.to_be_bytes());
}

fn decode_int32(bytes: &[u8]) -> Result<(i32, usize)> {
    if bytes.len() < 4 {
        return Err(Error::StorageCorrupted(
            "decode_int32: need 4 bytes, got fewer".into(),
        ));
    }
    let bits = u32::from_be_bytes(bytes[..4].try_into().unwrap());
    let n = (bits ^ INT32_SIGN_MASK) as i32;
    Ok((n, 4))
}

// ---- Boolean --------------------------------------------------------------

fn encode_boolean(b: bool, out: &mut Vec<u8>) {
    out.push(if b { 0x01 } else { 0x00 });
}

fn decode_boolean(bytes: &[u8]) -> Result<(bool, usize)> {
    if bytes.is_empty() {
        return Err(Error::StorageCorrupted(
            "decode_boolean: need 1 byte, got 0".into(),
        ));
    }
    match bytes[0] {
        0x00 => Ok((false, 1)),
        0x01 => Ok((true, 1)),
        b => Err(Error::StorageCorrupted(format!(
            "decode_boolean: invalid byte 0x{:02x}",
            b
        ))),
    }
}

// ---- Int64 ----------------------------------------------------------------

/// Sign-bit mask for i64. Same trick as INT32_SIGN_MASK on bit 63 of u64.
const INT64_SIGN_MASK: u64 = 1 << 63;

fn encode_int64(n: i64, out: &mut Vec<u8>) {
    let bits = (n as u64) ^ INT64_SIGN_MASK;
    out.extend_from_slice(&bits.to_be_bytes());
}

fn decode_int64(bytes: &[u8]) -> Result<(i64, usize)> {
    if bytes.len() < 8 {
        return Err(Error::StorageCorrupted(
            "decode_int64: need 8 bytes, got fewer".into(),
        ));
    }
    let bits = u64::from_be_bytes(bytes[..8].try_into().unwrap());
    let n = (bits ^ INT64_SIGN_MASK) as i64;
    Ok((n, 8))
}

// ---- Char(n) --------------------------------------------------------------

/// Encode a Char(n) value. The caller is expected to pass a string whose
/// UTF-8 byte length is exactly `target_len`. Shorter strings are zero-padded
/// to fit. Embedded NUL (0x00) is rejected — it would conflict with the
/// padding and produce ambiguous decodings.
fn encode_char(s: &str, target_len: u16, out: &mut Vec<u8>) -> Result<()> {
    if s.as_bytes().contains(&0x00) {
        return Err(Error::StorageCorrupted(
            "encode_char: embedded NUL byte not allowed in Char".into(),
        ));
    }
    if s.len() > target_len as usize {
        return Err(Error::StorageCorrupted(format!(
            "encode_char: value length {} exceeds Char({})",
            s.len(),
            target_len
        )));
    }
    out.extend_from_slice(s.as_bytes());
    // Right-pad with 0x00 to the column's fixed width. 0x00 sorts smallest,
    // so "abc" + padding < "abd" + padding even at different actual lengths.
    let pad = target_len as usize - s.len();
    out.resize(out.len() + pad, 0x00);
    Ok(())
}

fn decode_char(bytes: &[u8], target_len: u16) -> Result<(String, usize)> {
    let n = target_len as usize;
    if bytes.len() < n {
        return Err(Error::StorageCorrupted(format!(
            "decode_char: need {} bytes, got {}",
            n,
            bytes.len()
        )));
    }
    let raw = &bytes[..n];
    // Strip trailing 0x00 padding — embedded NULs are forbidden at encode time,
    // so any trailing 0x00s are guaranteed to be padding.
    let end = raw.iter().rposition(|&b| b != 0x00).map_or(0, |i| i + 1);
    let s = String::from_utf8(raw[..end].to_vec()).map_err(|e| {
        Error::StorageCorrupted(format!("decode_char: invalid UTF-8: {}", e))
    })?;
    Ok((s, n))
}

// ---- Escaped (Varchar / Bytes) --------------------------------------------

/// Byte-stuffed escape for variable-length components.
///
/// Replaces every 0x00 in the input with the two-byte sequence 0x00 0xFF,
/// then appends the terminator 0x00 0x00. Properties:
///   - Lex order is preserved: shorter inputs sort before longer same-prefix
///     because the terminator 0x00 0x00 is less than any escaped byte 0x00 0xFF.
///   - The terminator is unambiguous: an escaped 0x00 byte cannot be confused
///     with the terminator (escape produces 0xFF after 0x00; terminator
///     produces 0x00 after 0x00).
fn encode_escaped(input: &[u8], out: &mut Vec<u8>) {
    for &b in input {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

fn decode_escaped(bytes: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x00 {
            if i + 1 >= bytes.len() {
                return Err(Error::StorageCorrupted(
                    "decode_escaped: truncated after 0x00".into(),
                ));
            }
            match bytes[i + 1] {
                0x00 => return Ok((out, i + 2)), // terminator
                0xFF => {
                    out.push(0x00);
                    i += 2;
                }
                b => {
                    return Err(Error::StorageCorrupted(format!(
                        "decode_escaped: invalid escape 0x00 0x{:02x}",
                        b
                    )))
                }
            }
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Err(Error::StorageCorrupted(
        "decode_escaped: missing terminator".into(),
    ))
}

// ---- byte_increment --------------------------------------------------------

/// Return a byte sequence strictly greater than `bytes` in lexicographic order.
///
/// Implementation: append 0x00 to the input. This is always > input (a longer
/// sequence with the input as a strict prefix) and never collides with any
/// other valid encoded PK in our schemes (composite arities are fixed by
/// schema; varlen encodings end in 0x00 0x00 terminator, so suffix-extension
/// can't produce another valid encoding of the same arity).
///
/// Used by Phase 11 SQL translation: `WHERE k BETWEEN x AND y` (inclusive)
/// becomes `scan_range(encode(x), byte_increment(encode(y)))` (half-open).
pub fn byte_increment(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 1);
    out.extend_from_slice(bytes);
    out.push(0x00);
    out
}

// ---- tests ---------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode + decode a curated set of i32 boundary values; assert exact
    /// roundtrip. Catches off-by-one errors in the sign-bit math at the
    /// extremes (i32::MIN, i32::MAX) and at zero.
    #[test]
    fn int32_roundtrip_zero_and_extremes() {
        let cases = [
            0_i32,
            1,
            -1,
            42,
            -42,
            1_000_000,
            -1_000_000,
            i32::MIN,
            i32::MAX,
        ];
        for n in cases {
            let v = Value::Int32(n);
            let bytes = encode_key_components(&[&v], &[ColumnType::Int32]).unwrap();
            let back = decode_key_components(&bytes, &[ColumnType::Int32]).unwrap();
            assert_eq!(back, vec![Value::Int32(n)], "roundtrip failed for {}", n);
        }
    }

    /// The reason `INT32_SIGN_MASK` exists. Numerically-ordered i32s that
    /// cross zero must produce lexicographically-ordered byte strings.
    /// Without the sign-bit XOR, i32::MIN's bit pattern (0x80000000) would
    /// sort *after* i32::MAX's (0x7FFFFFFF) byte-wise — which is the bug
    /// this test prevents.
    #[test]
    fn int32_order_preserved_across_sign_boundary() {
        let nums = [i32::MIN, -1_000_000, -1, 0, 1, 1_000_000, i32::MAX];
        let encoded: Vec<Vec<u8>> = nums
            .iter()
            .map(|n| encode_key_components(&[&Value::Int32(*n)], &[ColumnType::Int32]).unwrap())
            .collect();
        for window in encoded.windows(2) {
            assert!(
                window[0] < window[1],
                "byte order doesn't match int order: {:?} (for some n) should be < {:?} (for next n)",
                window[0],
                window[1]
            );
        }
    }

    /// NULL must sort strictly before any present value. This is the
    /// Postgres-style "NULLS FIRST" convention baked into our sentinel
    /// choice (NULL_SENTINEL = 0x00 < PRESENT_SENTINEL = 0x01). Test
    /// fails immediately if the sentinel constants are ever swapped.
    #[test]
    fn null_sorts_before_any_int32() {
        let null_bytes = encode_key_components(&[&Value::Null], &[ColumnType::Int32]).unwrap();
        let min_int_bytes =
            encode_key_components(&[&Value::Int32(i32::MIN)], &[ColumnType::Int32]).unwrap();
        assert!(
            null_bytes < min_int_bytes,
            "NULL should sort before Int32(i32::MIN), got NULL={:?} MIN={:?}",
            null_bytes,
            min_int_bytes
        );
    }

    /// NULL roundtrip: encode produces one sentinel byte; decode consumes
    /// exactly that byte and yields Value::Null. Verifies decode_one's
    /// early-return path doesn't accidentally read into the next component.
    #[test]
    fn null_roundtrip() {
        let bytes = encode_key_components(&[&Value::Null], &[ColumnType::Int32]).unwrap();
        assert_eq!(
            bytes,
            vec![NULL_SENTINEL],
            "NULL should encode to a single sentinel byte"
        );
        let back = decode_key_components(&bytes, &[ColumnType::Int32]).unwrap();
        assert_eq!(back, vec![Value::Null]);
    }

    /// Smallest interesting composite test: two-component (i32, i32) tuples
    /// in known logical order must encode to byte strings in the same order.
    /// This is the property that makes future (table_id, user_pk) storage
    /// keys work — per-component encoding is meaningless without it.
    #[test]
    fn composite_int32_pair_order_preserved() {
        // (1, 2) < (1, 3) < (2, 0) lexicographically must match logically.
        let pairs = [(1_i32, 2_i32), (1, 3), (2, 0)];
        let encoded: Vec<Vec<u8>> = pairs
            .iter()
            .map(|(a, b)| {
                encode_key_components(
                    &[&Value::Int32(*a), &Value::Int32(*b)],
                    &[ColumnType::Int32, ColumnType::Int32],
                )
                .unwrap()
            })
            .collect();
        assert!(encoded[0] < encoded[1], "(1,2) should encode before (1,3)");
        assert!(encoded[1] < encoded[2], "(1,3) should encode before (2,0)");
    }

    /// Boolean roundtrip for both values. Locks in the byte mapping
    /// (true=0x01, false=0x00) — flipping it in `encode_boolean` fails here.
    #[test]
    fn boolean_roundtrip_both_values() {
        for b in [false, true] {
            let v = Value::Boolean(b);
            let bytes = encode_key_components(&[&v], &[ColumnType::Boolean]).unwrap();
            assert_eq!(
                bytes,
                vec![PRESENT_SENTINEL, if b { 0x01 } else { 0x00 }],
                "encoded bytes wrong for {}",
                b
            );
            let back = decode_key_components(&bytes, &[ColumnType::Boolean]).unwrap();
            assert_eq!(back, vec![Value::Boolean(b)], "roundtrip failed for {}", b);
        }
    }

    /// `false` must sort strictly before `true` byte-wise. Matches Rust's
    /// `bool: Ord` (false < true) and SQL standard ordering. Fails immediately
    /// if the byte mapping is reversed.
    #[test]
    fn boolean_false_sorts_before_true() {
        let f = encode_key_components(&[&Value::Boolean(false)], &[ColumnType::Boolean]).unwrap();
        let t = encode_key_components(&[&Value::Boolean(true)], &[ColumnType::Boolean]).unwrap();
        assert!(f < t, "Boolean(false)={:?} should sort before Boolean(true)={:?}", f, t);
    }

    /// Decode must reject any byte other than 0x00 or 0x01 under a Boolean
    /// column. Matches the project's "crash on corruption, never silently
    /// coerce" stance — a corrupt page byte that lands here should not be
    /// silently interpreted as true/false.
    #[test]
    fn boolean_decode_rejects_invalid_byte() {
        let corrupted = vec![PRESENT_SENTINEL, 0x42];
        let err = decode_key_components(&corrupted, &[ColumnType::Boolean])
            .expect_err("decode should reject invalid Boolean byte");
        match err {
            Error::StorageCorrupted(msg) => {
                assert!(
                    msg.contains("0x42"),
                    "error message should name the bad byte, got: {}",
                    msg
                );
            }
            other => panic!("expected StorageCorrupted, got: {:?}", other),
        }
    }

    // ---- Int64 / Timestamp / Decimal -------------------------------------

    /// Int64 covers the same boundary + sign-cross cases as Int32. Combined
    /// into one test because the underlying logic is identical (just wider).
    #[test]
    fn int64_roundtrip_and_sign_boundary_order() {
        // Strictly increasing.  -10^12 is more negative than -2^31-1, so it
        // comes first; +10^12 is less than i64::MAX, so it comes before MAX.
        let nums = [
            i64::MIN,
            -1_000_000_000_000,
            i64::from(i32::MIN) - 1,
            -1,
            0,
            1,
            i64::from(i32::MAX) + 1,
            1_000_000_000_000,
            i64::MAX,
        ];
        let mut last_bytes: Option<Vec<u8>> = None;
        for n in nums {
            let v = Value::Int64(n);
            let bytes = encode_key_components(&[&v], &[ColumnType::Int64]).unwrap();
            let back = decode_key_components(&bytes, &[ColumnType::Int64]).unwrap();
            assert_eq!(back, vec![Value::Int64(n)], "roundtrip failed for {}", n);
            if let Some(prev) = &last_bytes {
                assert!(prev < &bytes, "byte order broken at {}: {:?} >= {:?}", n, prev, bytes);
            }
            last_bytes = Some(bytes);
        }
    }

    /// Timestamp is a Value::Timestamp tag wrapping an i64. Encoding shares
    /// Int64's bytes; the type tag makes them distinct in the type system.
    #[test]
    fn timestamp_roundtrip() {
        let cases = [0_i64, 1_700_000_000_000_000, -1_000_000_000, i64::MAX, i64::MIN];
        for t in cases {
            let v = Value::Timestamp(t);
            let bytes = encode_key_components(&[&v], &[ColumnType::Timestamp]).unwrap();
            let back = decode_key_components(&bytes, &[ColumnType::Timestamp]).unwrap();
            assert_eq!(back, vec![Value::Timestamp(t)], "roundtrip failed for ts={}", t);
        }
    }

    /// Decimal: encoded bytes are just the i64 mantissa. The scale is recovered
    /// from `ColumnType::Decimal { scale, .. }` during decode and re-attached
    /// to the returned Value. Verifies that encode + decode reconstruct the
    /// original Value (scale included) when the column scale matches.
    #[test]
    fn decimal_roundtrip_preserves_scale_from_column() {
        let col = ColumnType::Decimal { precision: 12, scale: 2 };
        let cases = [
            Value::Decimal { mantissa: 0, scale: 2 },
            Value::Decimal { mantissa: 12345, scale: 2 },     // 123.45
            Value::Decimal { mantissa: -12345, scale: 2 },    // -123.45
            Value::Decimal { mantissa: i64::MAX, scale: 2 },
            Value::Decimal { mantissa: i64::MIN, scale: 2 },
        ];
        for v in &cases {
            let bytes = encode_key_components(&[v], std::slice::from_ref(&col)).unwrap();
            let back = decode_key_components(&bytes, std::slice::from_ref(&col)).unwrap();
            assert_eq!(back, vec![v.clone()], "decimal roundtrip failed for {:?}", v);
        }
    }

    /// Decimal mantissa ordering uses the same sign-bit XOR as Int64. Negatives
    /// must sort before positives. Verifies the trick carries over to Decimal.
    #[test]
    fn decimal_mantissa_order_preserved_across_sign() {
        let col = ColumnType::Decimal { precision: 18, scale: 4 };
        let mantissas = [i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX];
        let encoded: Vec<Vec<u8>> = mantissas
            .iter()
            .map(|m| {
                encode_key_components(
                    &[&Value::Decimal { mantissa: *m, scale: 4 }],
                    std::slice::from_ref(&col),
                )
                .unwrap()
            })
            .collect();
        for w in encoded.windows(2) {
            assert!(w[0] < w[1], "decimal mantissa order broken: {:?} >= {:?}", w[0], w[1]);
        }
    }

    // ---- Char(n) ----------------------------------------------------------

    /// Char(n) pads short values with 0x00 to fixed width. Decode strips the
    /// trailing zeros and returns the original UTF-8 string. Tests several
    /// lengths to confirm padding math.
    #[test]
    fn char_roundtrip_with_padding() {
        let col = ColumnType::Char(5);
        for s in ["", "a", "abc", "abcde"] {
            let v = Value::Char(s.to_string());
            let bytes = encode_key_components(&[&v], std::slice::from_ref(&col)).unwrap();
            // Encoded body should be exactly 5 bytes (the column width) after the sentinel.
            assert_eq!(
                bytes.len(),
                1 + 5,
                "Char(5) should encode to 1 sentinel + 5 body, got {}",
                bytes.len()
            );
            let back = decode_key_components(&bytes, std::slice::from_ref(&col)).unwrap();
            assert_eq!(back, vec![Value::Char(s.to_string())], "char roundtrip failed for {:?}", s);
        }
    }

    /// Char ordering: shorter values padded with 0x00 sort before longer ones
    /// with the same prefix. "ab" (padded) < "ac" (padded) < "b" (padded).
    #[test]
    fn char_order_preserved() {
        let col = ColumnType::Char(5);
        let strings = ["", "a", "aa", "ab", "b"];
        let encoded: Vec<Vec<u8>> = strings
            .iter()
            .map(|s| {
                encode_key_components(
                    &[&Value::Char(s.to_string())],
                    std::slice::from_ref(&col),
                )
                .unwrap()
            })
            .collect();
        for w in encoded.windows(2) {
            assert!(w[0] < w[1], "char order broken: {:?} >= {:?}", w[0], w[1]);
        }
    }

    /// Char rejects embedded NUL (0x00) — it's reserved for padding and
    /// allowing it would create ambiguous decodings.
    #[test]
    fn char_rejects_embedded_nul() {
        let col = ColumnType::Char(5);
        let v = Value::Char("a\0b".to_string());
        let err = encode_key_components(&[&v], std::slice::from_ref(&col))
            .expect_err("encode should reject embedded NUL");
        assert!(
            matches!(err, Error::StorageCorrupted(ref msg) if msg.contains("NUL")),
            "expected NUL error, got: {:?}",
            err
        );
    }

    // ---- Varchar / Bytes (escaped) ---------------------------------------

    /// Varchar's whole reason for the byte-stuffed escape: round-tripping a
    /// string that contains an embedded 0x00 byte. Without the escape, the
    /// 0x00 would collide with the terminator.
    #[test]
    fn varchar_roundtrip_with_embedded_nul() {
        let col = ColumnType::Varchar(64);
        let cases = [
            String::new(),
            "hello".to_string(),
            "a\0b".to_string(),
            "\0\0\0".to_string(),
            "\0".to_string(),
        ];
        for s in &cases {
            let v = Value::Varchar(s.clone());
            let bytes = encode_key_components(&[&v], std::slice::from_ref(&col)).unwrap();
            let back = decode_key_components(&bytes, std::slice::from_ref(&col)).unwrap();
            assert_eq!(back, vec![Value::Varchar(s.clone())], "varchar roundtrip failed for {:?}", s);
        }
    }

    /// Varchar ordering: shorter strings sort before longer same-prefix strings,
    /// because the terminator (0x00 0x00) is less than any escaped 0x00 (0x00 0xFF)
    /// or any non-zero byte.
    #[test]
    fn varchar_order_preserved() {
        let col = ColumnType::Varchar(64);
        let strings = ["", "a", "aa", "ab", "b"];
        let encoded: Vec<Vec<u8>> = strings
            .iter()
            .map(|s| {
                encode_key_components(
                    &[&Value::Varchar(s.to_string())],
                    std::slice::from_ref(&col),
                )
                .unwrap()
            })
            .collect();
        for w in encoded.windows(2) {
            assert!(w[0] < w[1], "varchar order broken: {:?} >= {:?}", w[0], w[1]);
        }
    }

    /// Bytes is identical to Varchar minus the UTF-8 validation. Round-trip
    /// arbitrary binary data including all-zeros and all-0xFF.
    #[test]
    fn bytes_roundtrip_arbitrary_binary() {
        let col = ColumnType::Bytes(256);
        let cases: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x00],
            vec![0xFF],
            vec![0x00, 0xFF, 0x00, 0xFF],
            (0..=255u8).collect(),
        ];
        for b in &cases {
            let v = Value::Bytes(b.clone());
            let bytes = encode_key_components(&[&v], std::slice::from_ref(&col)).unwrap();
            let back = decode_key_components(&bytes, std::slice::from_ref(&col)).unwrap();
            assert_eq!(back, vec![Value::Bytes(b.clone())], "bytes roundtrip failed for {:?}", b);
        }
    }

    // ---- byte_increment ---------------------------------------------------

    /// byte_increment must always produce a strictly-greater byte sequence,
    /// including for all-0xFF input (where appending 0x00 is the only option).
    #[test]
    fn byte_increment_strictly_greater() {
        let cases: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x00],
            vec![0x05],
            vec![0xFF],
            vec![0x05, 0xFF],
            vec![0xFF, 0xFF, 0xFF],
        ];
        for input in &cases {
            let next = byte_increment(input);
            assert!(
                input.as_slice() < next.as_slice(),
                "byte_increment({:?}) = {:?} not strictly greater",
                input,
                next
            );
        }
    }

    // ---- Mixed-type composite + type-mismatch validation ------------------

    /// Mixed-type composite: a 3-component PK (Int32, Char(3), Varchar) must
    /// round-trip exactly and preserve order across components. This is the
    /// realistic case for TPC-C tables (e.g., (w_id, d_id, c_last)).
    #[test]
    fn mixed_type_composite_roundtrip_and_order() {
        let types = [
            ColumnType::Int32,
            ColumnType::Char(3),
            ColumnType::Varchar(64),
        ];
        let rows = [
            (1_i32, "AAA", "alice"),
            (1, "AAA", "bob"),
            (1, "AAB", "alice"),
            (2, "AAA", "alice"),
        ];
        let encoded: Vec<Vec<u8>> = rows
            .iter()
            .map(|(a, b, c)| {
                let values = [
                    Value::Int32(*a),
                    Value::Char(b.to_string()),
                    Value::Varchar(c.to_string()),
                ];
                let refs: Vec<&Value> = values.iter().collect();
                encode_key_components(&refs, &types).unwrap()
            })
            .collect();
        for w in encoded.windows(2) {
            assert!(w[0] < w[1], "composite order broken: {:?} >= {:?}", w[0], w[1]);
        }
        // And the first row decodes back to the original triple.
        let back = decode_key_components(&encoded[0], &types).unwrap();
        assert_eq!(
            back,
            vec![
                Value::Int32(1),
                Value::Char("AAA".to_string()),
                Value::Varchar("alice".to_string())
            ]
        );
    }

    /// Encoder rejects (Value, ColumnType) mismatches with a clear error.
    /// Catches caller bugs at write time rather than producing garbage bytes.
    #[test]
    fn encode_rejects_value_type_mismatch() {
        let v = Value::Int32(42);
        let err = encode_key_components(&[&v], &[ColumnType::Int64])
            .expect_err("encode should reject Int32 value under Int64 column");
        assert!(
            matches!(err, Error::StorageCorrupted(ref m) if m.contains("type mismatch")),
            "expected type-mismatch error, got: {:?}",
            err
        );
    }

    /// Encoder rejects arity mismatches between values and types.
    #[test]
    fn encode_rejects_arity_mismatch() {
        let err =
            encode_key_components(&[&Value::Int32(1)], &[ColumnType::Int32, ColumnType::Int32])
                .expect_err("encode should reject arity mismatch");
        assert!(
            matches!(err, Error::StorageCorrupted(ref m) if m.contains("arity")),
            "expected arity-mismatch error, got: {:?}",
            err
        );
    }
}
