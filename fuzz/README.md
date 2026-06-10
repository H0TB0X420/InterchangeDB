# Fuzz targets (Q-29, stability.md pillar B1)

Coverage-guided [`cargo-fuzz`](https://rust-fuzz.github.io/book/cargo-fuzz.html)
/ libFuzzer targets over the decode-arbitrary-bytes surfaces. This is the
**deep, manual** half of B1; the **stable-Rust, CI-resident** half lives in
`tests/decode_fuzz_test.rs` (proptest) and runs on every push.

These targets are **deliberately off CI**: libFuzzer needs a nightly
toolchain + sanitizer instrumentation, and fuzzing is continuous (runs until
you stop it), not a per-push gate. This crate is a standalone workspace, so
`cargo build` / `test` / `clippy` at the repo root never touch it.

## One-time setup

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
```

## Run

```sh
# From the repo root. Each target runs until you Ctrl-C or it finds a crash.
cargo +nightly fuzz run keyenc_decode
cargo +nightly fuzz run tuple_decode
cargo +nightly fuzz run wal_record_decode
cargo +nightly fuzz run sstable_open
cargo +nightly fuzz run manifest_open

# Time-boxed smoke (e.g. 60s) instead of open-ended:
cargo +nightly fuzz run tuple_decode -- -max_total_time=60

# List targets:
cargo +nightly fuzz list
```

A crash writes a reproducer to `fuzz/artifacts/<target>/`. Replay it with:

```sh
cargo +nightly fuzz run <target> fuzz/artifacts/<target>/crash-<hash>
```

## Targets

| Target | Surface | Property |
| --- | --- | --- |
| `keyenc_decode` | `types::keyenc::decode_key_components` | no panic on arbitrary (schema, bytes) |
| `tuple_decode` | `types::tuple::decode` + `decode_column` | no panic on arbitrary (schema, bytes, index) |
| `wal_record_decode` | `wal::LogRecord::decode` | no panic on arbitrary bytes |
| `sstable_open` | `index::lsm::sstable::SSTableReader::open` | no panic on an arbitrary file |
| `manifest_open` | `index::lsm::manifest::Manifest::open` | no panic on an arbitrary manifest (drives `replay_line`) |

The two bugs these surfaces' proptest counterparts found (a `tuple::decode_column`
cursor overrun and an `SSTableReader::open` too-small-file `assert!`) are fixed;
re-run after any change to the decode paths.
