# testkit — how to write tests with the conformance matrix

`testkit` is the shared test/bench support crate (Q-33, `docs/stability.md`
pillar G). It exists so the test suite **exploits the source's
interchangeability**: the database is a set of swappable traits, so we write
each contract **once** and run it across **every** implementation, and we assert
that swapping an implementation changes performance, *never* correctness.

It is a `dev-dependency` of the root crate — `tests/` and `benches/` see it; the
library (`src/`) never does (no cycle, source stays clean).

---

## The mental model

The matrix is built from **two primitives**:

1. **A config registry** (the *where* — which implementations). One x-macro per
   swappable axis in `matrix.rs`: `for_each_policy!`, `for_each_disk!`,
   `for_each_engine!`. **These are the single source of truth for which configs
   exist.**
2. **Workloads as data** (the *what* — which operations). `workload::Op` streams,
   either `seeded(...)` (deterministic) or `op_strategy(...)` (proptest). Pure
   data, so the same workload is *asserted* by tests, *measured* by benches, and
   (later) *replayed* under shuttle.

Every test that touches a swappable axis is a generator (one of the above)
crossed with the registry. **Do not write a test per config.** That is the
anti-pattern this crate eliminates.

---

## The golden rules

1. **Matrix-first.** If behavior varies by a swappable axis (`StorageEngine`,
   `EvictionPolicy`, `DiskManager`), write the contract/property once and run it
   across every config via the registry. A failure must name the config.
2. **One place to add a config.** Add a configuration in exactly one spot — the
   relevant `for_each_*!` in `matrix.rs` (+ its ctor in `policy`/`disk`/`engine`).
   Every conformance suite, differential, and bench then covers it for free.
3. **Workloads are data.** Express a workload as `Vec<Op>`; never inline an ad-hoc
   op loop that a bench can't reuse.
4. **Keep impl-specific behavior out of the matrix.** Eviction *order* (FIFO
   oldest, LRU least-recent), LSM compaction, File persistence-across-reopen —
   those are plain per-impl tests. The matrix tests only what *all* impls of a
   trait must agree on.

---

## The three test shapes

| Shape | Question it answers | Helper | Example |
| --- | --- | --- | --- |
| **Conformance** | does each config satisfy the trait's contract? | `<axis>::assert_contract` | `tests/replacer_conformance.rs` |
| **Equivalence** | do all configs produce identical results? | `equivalence::assert_all_equal` | `tests/config_equivalence.rs` (seeded), `tests/config_proptest.rs` (proptest) |
| **Head-to-head bench** | how do configs compare on the *same* workload? | criterion group | `benches/config_matrix.rs` |

---

## Recipes (copy these)

### Add a new config to an existing axis
One line in the registry + one ctor. Example — a 7th eviction policy:

```rust
// testkit/src/policy.rs
pub fn my_policy() -> Box<dyn EvictionPolicy> { Box::new(MyReplacer::new(CAP)) }

// testkit/src/matrix.rs — inside for_each_policy!
$cb!(my_policy, $crate::policy::my_policy);
```

Done. `replacer_conformance`, `config_equivalence`, `config_proptest`, and the
`config_matrix` bench now all cover `my_policy`.

### A conformance suite for an axis (one `#[test]` per config)

```rust
// tests/<axis>_conformance.rs
macro_rules! contract {
    ($name:ident, $ctor:path) => {
        #[test]
        fn $name() {
            let mut built = $ctor();
            testkit::disk::assert_contract(stringify!($name), built.get_mut());
        }
    };
}
testkit::for_each_disk!(contract);
```

`EvictionPolicy` makers are bare `fn() -> Box<dyn EvictionPolicy>` (no `Built`);
`DiskManager`/`StorageEngine` ctors return `Built<T>` — call `.get()` / `.get_mut()`.

### An equivalence differential — deterministic

```rust
let ops = workload::seeded(SEED, LEN, KEYS);
let states: Vec<(&str, State)> = testkit::policy::makers()
    .into_iter()
    .map(|(name, make)| (name, run_btree_with_policy(make, &ops)))
    .collect();
assert_all_equal(&states);          // names the diverging config on failure
```

### An equivalence differential — property-based (proptest)

```rust
proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn all_engines_agree(ops in workload::op_strategy(300, 200)) {
        let mut states: Vec<(&str, State)> = Vec::new();
        macro_rules! run { ($n:ident, $ty:ty, $ctor:path) => {
            let built = $ctor();
            workload::apply(built.get(), &ops);
            states.push((stringify!($n), workload::snapshot(built.get())));
        };}
        testkit::for_each_engine!(run);
        assert_all_equal(&states);
    }
}
```

### A `Database<E>` / txn-level test across engines (compile-time)

`Database<E>` is **generic, not `dyn`** — so engine-level/txn/durability tests
instantiate per engine at compile time rather than iterating a runtime `Vec`.
Pattern (see `tests/dst_recovery_test.rs`):

```rust
type DbMaker<E> = fn(&Path) -> Database<E>;
fn btree_db(dir: &Path) -> Database<BTreeEngine> { /* memory-backed */ }
fn lsm_db(dir: &Path)   -> Database<LsmEngine>   { Database::open(dir, LsmEngine::new(dir)?)? }
fn run_sweep<E: StorageEngine>(label: &str, make_db: DbMaker<E>, /* … */) { /* … */ }

#[test] fn btree() { run_sweep("btree", btree_db, /* … */); }
#[test] fn lsm()   { run_sweep("lsm",   lsm_db,   /* … */); }
```

### A head-to-head bench
Consume the **same** registry + workload (see `benches/config_matrix.rs`): a
criterion `benchmark_group`, one `bench_function` per config from
`testkit::policy::makers()` (runtime) or `for_each_engine!` (macro).

### A brand-new swappable axis (new trait)
1. `for_each_<axis>!` in `matrix.rs` (the registry).
2. `testkit/src/<axis>.rs` — the ctors + `assert_contract`.
3. `tests/<axis>_conformance.rs` — the per-config `#[test]`s.
4. If the axis is correctness-neutral, an equivalence differential.

### Rip a new engine into the matrix (the payoff)
Add one line to `for_each_engine!` + its ctor in `engine.rs`, and add a
`Database` maker for the durability sweep. It then inherits: the engine contract,
the equivalence differentials (seeded + proptest), the crash-recovery sweep, and
a bench slot — for free.

---

## Mechanics & gotchas

- **Object-safety dictates the mechanism.** `EvictionPolicy`, `DiskManager`, and
  `StorageEngine` are deliberately dyn-compatible (`scan<R>` is fenced
  `where Self: Sized`), so their registries are uniform runtime `Vec`s of makers.
  `Database<E>` is generic and *not* dyn — its tests use compile-time
  per-engine instantiation (a small macro or explicit `fn`s).
- **Eviction needs pressure to be meaningful.** A small pool (but **≥ 4** — a
  B-tree split simultaneously pins parent + children) plus enough keys to exceed
  the pool (~200 keys/leaf at default node sizes, so ~1000 keys over pool 4).
  Use `MemoryDiskManager` for speed. Without eviction, a policy differential is
  vacuous (the policy is never invoked).
- **`FileDiskManager` fsyncs every write.** Keep file-backed workloads **small**
  (a few hundred ops) or the test takes minutes. The file backend's correctness
  is covered fast by `disk_manager_conformance`; differentials don't need a huge
  file workload.
- **Maker `Vec`s need a type alias** (`PolicyMaker`, …) or clippy's
  `type_complexity` fires under `-D warnings`.
- **The x-macro callback pattern:** `for_each_*!($cb)` expands to
  `$cb!(name, ctor); …` once per config. Write a callback macro that emits a
  `#[test]`, a `v.push(...)`, or a `bench_function` — one registry, every view.
- **Gates:** every test must pass `cargo fmt --all --check` and
  `cargo clippy --all-targets -- -D warnings`. Run both before declaring done.

---

## Module map

| Module | Holds |
| --- | --- |
| `matrix` | the `for_each_*!` registries (single source of truth) |
| `policy` / `disk` / `engine` | per-axis ctors + `assert_contract` + (policy) `makers()` |
| `workload` | `Op`, `seeded`, `op_strategy`, `apply`, `snapshot` |
| `equivalence` | `assert_all_equal` |
| `handles` | `Built<T>` (keeps a `TempDir` alive alongside the subject) |
| `faults` | `FaultInjectionDiskManager` (I/O errors + torn node writes) |

## Future generators

The matrix is a *seam*; more modalities plug into it as op-stream generators.
proptest landed (`config_proptest`). **shuttle × workload** (concurrency) and
**fuzz × matrix** (coverage-guided) are scoped with concrete entailment in
`docs/stability.md` → pillar G → "Future generators on this seam".
