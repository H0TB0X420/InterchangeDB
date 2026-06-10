//! Property-based fuzzing of the decode-arbitrary-bytes surfaces.
//!
//! Stability doc pillar B1 (`Q-29`). The codebase is full of functions that
//! turn untrusted bytes back into structured data — key encodings, tuple
//! blobs, WAL records, SSTable files, the LSM manifest. Each has a round-trip
//! invariant (`decode(encode(x)) == x`) already exercised by example tests,
//! but example tests cannot cover the *adversarial* half: what does `decode`
//! do with bytes that `encode` would never produce?
//!
//! Two property kinds per surface:
//!
//!   1. **Panic-freedom.** Feeding arbitrary bytes to `decode` must return
//!      `Ok`/`Err`, never panic. proptest turns any panic into a failing case
//!      and *shrinks* it to a minimal trigger — so a latent out-of-bounds
//!      slice (the classic `&bytes[cursor..]` over-consume) surfaces as a
//!      tiny reproducer, not a mystery crash in production.
//!   2. **Round-trip / order.** On *valid* inputs, `decode∘encode` is the
//!      identity, and for the key encoder byte order matches logical order.
//!
//! This is the stable-Rust, CI-resident half of B1. The coverage-guided
//! `cargo-fuzz` targets in `fuzz/` cover the same surfaces with deeper
//! exploration but run manually (nightly toolchain), off CI by design.

use std::cmp::Ordering;

use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;

use interchangedb::index::lsm::manifest::Manifest;
use interchangedb::index::lsm::sstable::SSTableReader;
use interchangedb::types::{keyenc, tuple, ColumnType, Decimal, Value};
use interchangedb::wal::{LogPayload, LogRecord, Lsn};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// An arbitrary column type. Var-length widths are kept small so generated
/// rows stay cheap; the decode paths don't care about absolute size.
fn arb_column_type() -> impl Strategy<Value = ColumnType> {
    prop_oneof![
        Just(ColumnType::Int32),
        Just(ColumnType::Int64),
        Just(ColumnType::Boolean),
        Just(ColumnType::Timestamp),
        // scale <= precision <= 18 (Decimal is i64-backed, max scale 18).
        (0u8..=18).prop_flat_map(|scale| {
            (scale.max(1)..=18).prop_map(move |precision| ColumnType::Decimal { precision, scale })
        }),
        (1u16..=32).prop_map(ColumnType::Char),
        (0u16..=64).prop_map(ColumnType::Varchar),
        (0u16..=64).prop_map(ColumnType::Bytes),
    ]
}

/// A value that is *valid* for `ty` (so it round-trips), or `Null` (which is
/// legal under any column type) one time in eight.
///
/// `Char` is restricted to alphanumerics: the encoder right-pads with NUL and
/// the decoder trims trailing NULs, so a non-NUL, non-trailing-space payload
/// round-trips unambiguously. `Decimal` is built at the column's scale because
/// decode recovers scale from the type, not the bytes.
fn arb_value(ty: ColumnType) -> BoxedStrategy<Value> {
    let present: BoxedStrategy<Value> = match ty {
        ColumnType::Int32 => any::<i32>().prop_map(Value::Int32).boxed(),
        ColumnType::Int64 => any::<i64>().prop_map(Value::Int64).boxed(),
        ColumnType::Boolean => any::<bool>().prop_map(Value::Boolean).boxed(),
        ColumnType::Timestamp => any::<i64>().prop_map(Value::Timestamp).boxed(),
        ColumnType::Decimal { scale, .. } => any::<i64>()
            .prop_map(move |m| Value::Decimal(Decimal::from_i64_with_scale(m, scale)))
            .boxed(),
        ColumnType::Char(n) => proptest::string::string_regex(&format!("[a-zA-Z0-9]{{0,{}}}", n))
            .unwrap()
            .prop_map(Value::Char)
            .boxed(),
        ColumnType::Varchar(n) => proptest::string::string_regex(&format!("[ -~]{{0,{}}}", n))
            .unwrap()
            .prop_map(Value::Varchar)
            .boxed(),
        ColumnType::Bytes(n) => vec(any::<u8>(), 0..=(n as usize))
            .prop_map(Value::Bytes)
            .boxed(),
    };
    prop_oneof![7 => present, 1 => Just(Value::Null)].boxed()
}

/// A schema plus a row of values valid for that schema.
fn arb_schema_and_row() -> impl Strategy<Value = (Vec<ColumnType>, Vec<Value>)> {
    vec(arb_column_type(), 1..6).prop_flat_map(|types| {
        let value_strategies: Vec<BoxedStrategy<Value>> =
            types.iter().cloned().map(arb_value).collect();
        (Just(types), value_strategies)
    })
}

/// A signed-numeric column and two values for the order-preservation check.
/// These three types share the sign-bit-XOR + big-endian trick, which is the
/// non-trivial part of order preservation.
fn arb_ordered_pair() -> impl Strategy<Value = (ColumnType, Value, Value)> {
    prop_oneof![
        (any::<i32>(), any::<i32>()).prop_map(|(a, b)| (
            ColumnType::Int32,
            Value::Int32(a),
            Value::Int32(b)
        )),
        (any::<i64>(), any::<i64>()).prop_map(|(a, b)| (
            ColumnType::Int64,
            Value::Int64(a),
            Value::Int64(b)
        )),
        (any::<i64>(), any::<i64>()).prop_map(|(a, b)| (
            ColumnType::Timestamp,
            Value::Timestamp(a),
            Value::Timestamp(b)
        )),
    ]
}

/// Logical ordering for the numeric values produced by `arb_ordered_pair`.
fn numeric_cmp(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Int32(x), Value::Int32(y)) => x.cmp(y),
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        _ => unreachable!("arb_ordered_pair only yields matching numeric variants"),
    }
}

/// An arbitrary, structurally-valid WAL payload across every variant.
fn arb_payload() -> impl Strategy<Value = LogPayload> {
    let small = || vec(any::<u8>(), 0..32);
    prop_oneof![
        (small(), small()).prop_map(|(key, value)| LogPayload::Put { key, value }),
        small().prop_map(|key| LogPayload::Delete { key }),
        Just(LogPayload::Begin),
        any::<u64>().prop_map(|commit_ts| LogPayload::Commit { commit_ts }),
        Just(LogPayload::Abort),
        (vec(any::<u64>(), 0..8), any::<u64>(), any::<u64>()).prop_map(
            |(active_txn_ids, oracle_ts, next_txn_id)| LogPayload::Checkpoint {
                active_txn_ids,
                oracle_ts,
                next_txn_id,
            }
        ),
        (small(), small(), option::of(small())).prop_map(|(key, value, old_value)| {
            LogPayload::TxnPut {
                key,
                value,
                old_value,
            }
        }),
        (small(), option::of(small()))
            .prop_map(|(key, old_value)| LogPayload::TxnDelete { key, old_value }),
    ]
}

// ---------------------------------------------------------------------------
// keyenc — order-preserving composite key encoder
// ---------------------------------------------------------------------------

proptest! {
    /// Arbitrary bytes under an arbitrary schema must never crash the decoder.
    #[test]
    fn keyenc_decode_never_panics(
        types in vec(arb_column_type(), 0..6),
        bytes in vec(any::<u8>(), 0..128),
    ) {
        let _ = keyenc::decode_key_components(&bytes, &types);
    }

    /// decode(encode(row)) == row for any valid row.
    #[test]
    fn keyenc_roundtrip((types, row) in arb_schema_and_row()) {
        let refs: Vec<&Value> = row.iter().collect();
        let encoded = keyenc::encode_key_components(&refs, &types);
        prop_assume!(encoded.is_ok());
        let encoded = encoded.unwrap();
        let decoded = keyenc::decode_key_components(&encoded, &types).unwrap();
        prop_assert_eq!(decoded, row);
    }

    /// Byte order of the encoding matches logical order of the values.
    #[test]
    fn keyenc_order_preserving((ty, a, b) in arb_ordered_pair()) {
        let ea = keyenc::encode_key_components(&[&a], &[ty]).unwrap();
        let eb = keyenc::encode_key_components(&[&b], &[ty]).unwrap();
        prop_assert_eq!(ea.cmp(&eb), numeric_cmp(&a, &b));
    }
}

// ---------------------------------------------------------------------------
// tuple — row blob encoder with null bitmap
// ---------------------------------------------------------------------------

proptest! {
    /// Arbitrary bytes under an arbitrary schema must never crash decode.
    #[test]
    fn tuple_decode_never_panics(
        types in vec(arb_column_type(), 0..6),
        bytes in vec(any::<u8>(), 0..128),
    ) {
        let _ = tuple::decode(&types, &bytes);
    }

    /// decode_column over arbitrary bytes and an arbitrary index never crashes.
    #[test]
    fn tuple_decode_column_never_panics(
        types in vec(arb_column_type(), 0..6),
        bytes in vec(any::<u8>(), 0..128),
        col_idx in 0usize..8,
    ) {
        let _ = tuple::decode_column(&types, &bytes, col_idx);
    }

    /// decode(encode(row)) == row, and per-column decode agrees with the
    /// full decode for every column index.
    #[test]
    fn tuple_roundtrip_and_column_agreement((types, row) in arb_schema_and_row()) {
        let encoded = tuple::encode(&types, &row);
        prop_assume!(encoded.is_ok());
        let encoded = encoded.unwrap();

        let decoded = tuple::decode(&types, &encoded).unwrap();
        prop_assert_eq!(&decoded, &row);

        for (i, expected) in row.iter().enumerate() {
            let col = tuple::decode_column(&types, &encoded, i).unwrap();
            prop_assert_eq!(&col, expected);
        }
    }
}

// ---------------------------------------------------------------------------
// wal — LogRecord binary format with CRC32
// ---------------------------------------------------------------------------

proptest! {
    /// Arbitrary bytes must never crash the record decoder.
    #[test]
    fn wal_record_decode_never_panics(bytes in vec(any::<u8>(), 0..256)) {
        let _ = LogRecord::decode(&bytes);
    }

    /// decode(encode(record)) == (record, encoded_len) for every payload.
    #[test]
    fn wal_record_roundtrip(
        lsn in any::<u64>(),
        txn_id in any::<u64>(),
        prev_lsn in any::<u64>(),
        payload in arb_payload(),
    ) {
        let record = LogRecord {
            lsn: Lsn::new(lsn),
            txn_id,
            prev_lsn: Lsn::new(prev_lsn),
            payload,
        };
        let bytes = record.encode();
        let (decoded, consumed) = LogRecord::decode(&bytes).unwrap();
        prop_assert_eq!(decoded, record);
        prop_assert_eq!(consumed, bytes.len());
    }
}

// ---------------------------------------------------------------------------
// lsm — SSTable reader and manifest replay (file-backed surfaces)
// ---------------------------------------------------------------------------

proptest! {
    // File I/O per case — keep the case count modest.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Opening a file of arbitrary bytes as an SSTable must reject it with an
    /// error, never panic on a malformed footer / offsets / block index.
    #[test]
    fn sstable_open_arbitrary_bytes_never_panics(bytes in vec(any::<u8>(), 0..512)) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fuzz.sst");
        std::fs::write(&path, &bytes).unwrap();
        let _ = SSTableReader::open(&path, 1);
    }

    /// Opening a manifest of arbitrary text lines must not panic — this drives
    /// the private `replay_line` parser through the public open path.
    #[test]
    fn manifest_open_arbitrary_lines_never_panics(
        lines in vec(proptest::string::string_regex("[ -~]{0,48}").unwrap(), 0..16),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let sst_dir = dir.path().join("sst");
        std::fs::create_dir_all(&sst_dir).unwrap();
        std::fs::write(&manifest_path, lines.join("\n")).unwrap();
        let _ = Manifest::open(&manifest_path, &sst_dir);
    }
}
