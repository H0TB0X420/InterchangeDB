//! Shared helpers for the fuzz targets.
//!
//! The decode surfaces in `types::{keyenc, tuple}` take a *schema*
//! (`&[ColumnType]`) plus a byte blob. To let the coverage-guided fuzzer
//! explore schema shapes as well as payload bytes, we mirror `ColumnType`
//! with an `Arbitrary`-derivable enum and map it across.

use arbitrary::Arbitrary;
use interchangedb::types::{ColumnType, Decimal};

/// `Arbitrary`-derivable mirror of `ColumnType`. Width/precision/scale fields
/// are taken raw (unclamped) on purpose — the decoders must tolerate any
/// value, so feeding them junk widths is exactly the point.
///
/// This is a HAND-MAINTAINED mirror: nothing forces it to stay in sync with
/// `ColumnType`. Every new `ColumnType` variant MUST be added here (and to the
/// `From` impl below), or the fuzzer can never generate schemas of that type
/// and the corresponding decode arms stay unreachable from coverage-guided
/// fuzzing.
#[derive(Arbitrary, Debug, Clone)]
pub enum FuzzColumnType {
    Int32,
    Int64,
    Varchar(u16),
    Char(u16),
    Bytes(u16),
    Decimal { precision: u8, scale: u8 },
    Timestamp,
    Boolean,
    Date,
}

impl From<&FuzzColumnType> for ColumnType {
    fn from(f: &FuzzColumnType) -> ColumnType {
        match f {
            FuzzColumnType::Int32 => ColumnType::Int32,
            FuzzColumnType::Int64 => ColumnType::Int64,
            FuzzColumnType::Varchar(n) => ColumnType::Varchar(*n),
            FuzzColumnType::Char(n) => ColumnType::Char(*n),
            FuzzColumnType::Bytes(n) => ColumnType::Bytes(*n),
            FuzzColumnType::Decimal { precision, scale } => {
                // Clamp to the catalog's validated range (1 <= precision <= 18,
                // scale <= precision): the binder rejects anything else at
                // CREATE TABLE (Q-31), so the decode paths only ever see valid
                // Decimal types. Fuzzing arbitrary *bytes* under a *valid*
                // schema is the real threat model; feeding an out-of-contract
                // scale would just re-trip `from_i64_with_scale`'s precondition
                // assert, which is correct behavior, not a bug.
                let scale = *scale % (Decimal::MAX_SCALE + 1);
                let precision = (*precision % Decimal::MAX_PRECISION + 1).max(scale);
                ColumnType::Decimal { precision, scale }
            }
            FuzzColumnType::Timestamp => ColumnType::Timestamp,
            FuzzColumnType::Boolean => ColumnType::Boolean,
            FuzzColumnType::Date => ColumnType::Date,
        }
    }
}

/// Map a fuzzer-generated schema into the real `ColumnType` slice.
pub fn to_column_types(fuzz: &[FuzzColumnType]) -> Vec<ColumnType> {
    fuzz.iter().map(ColumnType::from).collect()
}
