# InterchangeDB

<img src="docs/architecture.svg" width="100%">

## Development

Inner-loop commands — measured rationale in `docs/plan-build-times.md`:

```bash
cargo check --lib      # fastest signal on src/ edits
cargo test --lib       # unit tests only, no integration binaries
cargo clippy --lib     # lint without building benches
cargo test --test <harness> <filter>   # one integration suite
```

Full `cargo test`, `cargo test --release`, and
`cargo clippy --all-targets -- -D warnings` are the pre-commit gates — run
them before pushing, not in the tight loop.

If your editor runs rust-analyzer, give it its own target directory so it
doesn't fight terminal builds for the `target/` lock (VS Code:
`"rust-analyzer.cargo.targetDir": true`; Neovim lspconfig:
`settings = { ["rust-analyzer"] = { cargo = { targetDir = true } } }`).
Symptom of the fight: a build that sits 10–30 s doing nothing before it
starts.
