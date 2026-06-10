#![no_main]
//! Fuzz `keyenc::decode_key_components` — must never panic on arbitrary
//! (schema, bytes) pairs, only return Ok/Err.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use interchangedb::types::keyenc;
use interchangedb_fuzz::{to_column_types, FuzzColumnType};

#[derive(Arbitrary, Debug)]
struct Input {
    types: Vec<FuzzColumnType>,
    bytes: Vec<u8>,
}

fuzz_target!(|input: Input| {
    let types = to_column_types(&input.types);
    let _ = keyenc::decode_key_components(&input.bytes, &types);
});
