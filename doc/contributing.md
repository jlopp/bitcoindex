# Contributing

Developer setup, the test and coverage policy, how to change the schema, and
the pull-request flow for `bitcoindex`.

## Contents

- [Dev setup](#dev-setup)
- [Workspace layout](#workspace-layout)
- [Running tests](#running-tests)
- [Coverage policy](#coverage-policy)
- [Changing the schema](#changing-the-schema)
- [Performance work](#performance-work)
- [Style and lint](#style-and-lint)
- [Pull-request flow](#pull-request-flow)

## Dev setup

The build needs a C/C++ toolchain and `libclang` for RocksDB's bindgen step,
plus the system `libzmq` for live mode:

```bash
sudo apt-get install -y clang libclang-dev llvm-dev cmake pkg-config libzmq3-dev

export LIBCLANG_PATH=/usr/lib/llvm-18/lib       # wherever libclang lives
export BINDGEN_EXTRA_CLANG_ARGS="-I$PWD/.build-shim/include"
export CXXFLAGS="-include cstdint"               # rocksdb 8.x transitive <cstdint>
cargo build --release
```

The two `export`s are the workarounds the `Dockerfile` also bakes in:
`BINDGEN_EXTRA_CLANG_ARGS` points bindgen at `.build-shim/include` (gcc's
builtins) so `stdbool.h` resolves, and `CXXFLAGS=-include cstdint` covers
rocksdb 8.x's reliance on transitive `<cstdint>` includes that GCC 15 no longer
provides. If `stdbool.h` or `<cstdint>` errors appear, these are why.

## Workspace layout

Seven crates, each with one responsibility:

| Crate | Responsibility |
|-------|----------------|
| `bidx-core` | Wire-format cursor, double-SHA256 / hash160, script classification, shared row types, disk-space helpers. |
| `bidx-parser` | `blk*.dat` reader, two-pass orchestration, tx parser. |
| `bidx-store` | RocksDB UTXO set + spend linking; Arrow/Parquet buffered writers. |
| `bidx-loader` | ClickHouse DDL and Parquet bulk loader. |
| `bidx-live` | Node RPC client, ZMQ subscriber, live tip tracker with reorg handling. |
| `bidx-serve` | Read-only query layer: Esplora REST, Electrum JSON-RPC, metrics, health. |
| `bidx-cli` | The `bidx` binary: subcommands, pipeline orchestration, bench harness. |

See [architecture.md](architecture.md) for how the crates fit together.

Integration tests live under each crate's `tests/` directory
(`bidx-parser/tests/genesis.rs`, `bidx-store/tests/parquet_roundtrip.rs`,
`bidx-store/tests/utxo_reorg.rs`); unit tests live in a `mod tests` in the file
they test.

## Running tests

```bash
cargo test --release --workspace     # the whole suite
cargo test --release -p bidx-parser  # one crate
```

CI runs the suite in **debug** (`cargo test --locked --workspace`) because
RocksDB + Arrow/Parquet are very heavy to compile in release with `lto = "fat"`;
debug catches the same correctness issues at a fraction of the compile time.
Run release locally when you are timing or when the debug build behaves
differently.

## Coverage policy

**Every Rust file a change touches must be at 100% line coverage.** This is
non-negotiable and enforced per changed file — see
`.cursor/rules/test-coverage.mdc` for the full rule and the reasoning (a bad
parse breaks a multi-hour run; a wrong UTXO undo entry corrupts live reorg
handling).

Measure with [cargo-tarpaulin](https://github.com/xd009642/tarpaulin):

```bash
cargo tarpaulin --workspace --out Json --output-dir target/coverage

# Fail if any file changed against origin/master is below 100%:
scripts/coverage/check-changed.sh target/coverage/tarpaulin-report.json origin/master
```

Config lives in `.tarpaulin.toml`; `engine = "Llvm"` is required because the
ptrace engine mis-handles the forked `clickhouse-client` children the loader
tests spawn and silently under-reports lines as covered.

The gate is **per changed file**, not workspace-wide: the long-running
`bidx-live` and pipeline paths that need a live bitcoind/ZMQ are not fully
covered, and a workspace threshold would either block all work or invite
blanket exclusions. `.github/workflows/coverage.yml` runs the same check on
every PR, diffing against the PR's base branch.

### Writing tests that count

The policy is about proving behaviour, not hitting a line count. What the repo
expects:

- **Roundtrip tests** for every encode/decode pair (`UtxoValue::encode/decode`,
  `BlockUndo::encode/decode`).
- **Truncation/bounds tests** for every parser entry point (e.g. `BlockUndo::decode`
  rejects truncated input).
- **Synthetic full-flow tests** via a fake UTXO store entry
  (`bidx-store/tests/utxo_reorg.rs` builds tiny in-memory blocks, runs
  `apply_block` then `disconnect_block`, and asserts the UTXO set equals the
  genesis fixture).
- **Round-trip through real I/O** for anything persisted
  (`bidx-store/tests/parquet_roundtrip.rs` reads the genesis Parquet back with
  `parquet-arrow` and checks the field values).
- **Both directions** for consensus/endianness quirks — wire-vs-display hash
  hex, varint size classes — with assertions on each direction.

Tests that only call a function and assert its return without checking the
state it mutated do not count, and neither do tests that pass "because nothing
crashed".

### Exemptions

A path may be left untested only when it is provably unreachable in the
module's domain. Mark it with a `coverage-exempt` comment explaining *why*; the
gate honours it:

- bare `coverage-exempt` exempts the item the comment heads — a single
  statement, or, for a function/loop/closure, the whole brace block;
- `// coverage-exempt: 12-14, 20` exempts exactly those lines when the
  construct spans lines the heuristic cannot bracket.

Every exemption must still be justified; the marker is not a way to opt out of
testing reachable code.

## Changing the schema

All DDL lives in `crates/bidx-loader/src/schema.rs`. The rules:

- **Additive DDL** (a new index table, a new materialized view) is guarded with
  `IF NOT EXISTS` and needs no version bump — re-running `init-db` applies it.
  Add the table to the `tables()` / `all_ddl()` lists and the MV to the MV list.
- **A change an operator must migrate** (a new column on a base table, a
  changed key, a dropped index, a changed engine) requires **bumping
  `SCHEMA_VERSION`** and adding a `(version, statements)` entry to
  `migrations()`. Because `migrations()` is defined at module scope where the
  database name is unknown, write statements with the `__DB__` placeholder;
  `materialize_migrations(db)` substitutes the real name when the migration is
  applied. The v2 `address_stats` rebuild is the worked example.
- **Changing an engine cannot be done with `ALTER`.** Rebuild: create a
  `*_new` table with the target engine, backfill it from the base tables, then
  `DROP` the old table and `RENAME` the new one into place (rename last, so a
  failure leaves the old table intact for a retry). A materialized view stays
  bound to the table *name*, so it keeps feeding the renamed table.
- **New columns go at the END** of a table's column list; never reorder
  existing columns. The Parquet schema is positional and the ClickHouse DDL
  maps by name, so reordering silently mis-associates columns.
- **Hashes in Parquet are `FixedSizeBinary(32)`**, never `Utf8` hex strings.
- The schema module has structural tests (MV presence, `address_stats` engine,
  `OP_RETURN` exclusion) that assert on the generated DDL — extend them with
  any new invariant, and keep every changed line covered.

There is a `BIDX_DDL_DUMP=<path>` escape hatch on the schema test that writes
the full generated DDL to a file, handy for diffing a change:

```bash
BIDX_DDL_DUMP=/tmp/bidx-ddl.sql cargo test --release -p bidx-loader dump_ddl_when_requested
```

Document a schema change in the same PR: update [schema.md](schema.md) (and
[data-model-rationale.md](data-model-rationale.md) if the *why* changed). The
[doc index](README.md#keeping-these-documents-honest) maps each document to its
source of truth.

## Performance work

This is a Bitcoin indexer; ingest throughput is the product. Before changing
anything on the hot path (`bidx-parser`, `bidx-store`, the `pipeline` module,
`ThreadFileCache`, or the `consume_ordered` consumer thread), read
`.cursor/rules/rust-performance.mdc` and benchmark both sides:

```bash
bidx bench --mode full --blocks-dir <test blocks>   # before
# ... change ...
bidx bench --mode full --blocks-dir <test blocks>   # after
```

Record before/after in the PR or commit message when the difference is > 2%.
Bench modes are orthogonal stage toggles (`headers`, `parse`, `drain`, `utxo`,
`sink`, `full`) so you can attribute a regression to a stage — see
[cli.md](cli.md#bench) and [PERF.md](../PERF.md).

The rules that bite most often: no per-item allocation on the parse hot path,
no `format!`/`to_string()` on the hot path, `FxHashMap` not the default `SipHash`
hasher, never hold a `Mutex` across I/O, never copy a slice you only read, and
no `.unwrap()`/`.expect()` in production paths (use `?` with
`anyhow::Context`).

## Style and lint

- Format with `cargo fmt`, lint with `cargo clippy --workspace --all-targets`.
- **No `unwrap()`/`expect()` outside tests** — use `?` with a contextual
  `anyhow::Context`.
- Supply chain is gated by `cargo deny check` (advisories, licenses, bans,
  sources) in `.github/workflows/deny.yml`; a new dependency must pass it. See
  `deny.toml`.
- Commit messages describe the *why*; reference the issue where one exists.

## Pull-request flow

Four CI workflows run on every PR:

| Workflow | What it does |
|----------|--------------|
| `test.yml` | `cargo test --locked --workspace` (debug). |
| `coverage.yml` | Measures coverage and enforces 100% on changed files. |
| `deny.yml` | `cargo deny check` — advisories, licenses, bans, sources. |
| `release.yml` | On a `v*` tag: builds prebuilt binaries, SBOM, and `SHA256SUMS`. |

A PR is ready to merge when all four are green. Before opening one:

1. `cargo fmt` and `cargo clippy --workspace --all-targets`.
2. `cargo test --release --workspace`.
3. `scripts/coverage/check-changed.sh` against your base branch.
4. If you touched the hot path, a `bidx bench` before/after.
5. If you changed behaviour, update the matching document in `doc/` (the
   source-of-truth mapping is in [doc/README.md](README.md#keeping-these-documents-honest)).
