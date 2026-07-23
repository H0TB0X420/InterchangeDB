# InterchangeDB

<img src="docs/architecture.svg" width="100%">

**Docs:** start at [`docs/README.md`](docs/README.md) (documentation map) and
[`CONTEXT.md`](CONTEXT.md) (domain glossary).

## Development

Inner-loop commands:

```bash
cargo check --lib      # fastest signal on src/ edits
cargo test --lib       # unit tests only, no integration binaries
cargo clippy --lib     # lint without building benches
cargo test --test <harness> <filter>   # one integration suite
```

Full `cargo test --workspace`, `cargo test --workspace --release`, and
`cargo clippy --workspace --all-targets -- -D warnings` are the pre-commit
gates (mirrored in CI) — run them before pushing, not in the tight loop.

### Test tiers

Tests run in three named tiers:

- **Default tier** — in-memory backends (`MemoryDiskManager`, WAL
  `SyncMode::NoSync`): fast on any machine; the subject of these suites is
  never the disk. New tests belong here unless the disk is their subject.
- **Durability tier** — real files, real fsync (`Durable`): every suite
  whose *subject* is crash durability, recovery, persistence, or fsync
  semantics. Enumerated in the plan doc; a test with reopen/crash
  semantics must live here.
- **Conformance matrix** — testkit's `for_each_*!` registries run every
  backend (including `FileDiskManager`) against the shared contracts,
  which is what makes the default tier's memory backends a
  zero-coverage-loss choice.

If your editor runs rust-analyzer, give it its own target directory so it
doesn't fight terminal builds for the `target/` lock (VS Code:
`"rust-analyzer.cargo.targetDir": true`; Neovim lspconfig:
`settings = { ["rust-analyzer"] = { cargo = { targetDir = true } } }`).
Symptom of the fight: a build that sits 10–30 s doing nothing before it
starts.
