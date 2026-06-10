#![no_main]
//! Fuzz `tuple::decode` and `tuple::decode_column` — neither may panic on
//! arbitrary (schema, bytes, index) triples. `decode_column` is the one that
//! Q-29 caught panicking via an unchecked cursor; we exercise both an
//! in-range and a raw (possibly out-of-range) column index.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use interchangedb::types::tuple;
use interchangedb_fuzz::{to_column_types, FuzzColumnType};

#[derive(Arbitrary, Debug)]
struct Input {
    types: Vec<FuzzColumnType>,
    bytes: Vec<u8>,
    col_idx: usize,
}

fuzz_target!(|input: Input| {
    let types = to_column_types(&input.types);
    let _ = tuple::decode(&types, &input.bytes);

    // Raw index (may be out of range — must Err, not panic).
    let _ = tuple::decode_column(&types, &input.bytes, input.col_idx);

    // And a guaranteed in-range index to hit the column-skip walk.
    if !types.is_empty() {
        let _ = tuple::decode_column(&types, &input.bytes, input.col_idx % types.len());
    }
});
