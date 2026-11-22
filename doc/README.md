# bitcoindex documentation

Documentation for the `bitcoindex` pipeline, its ClickHouse schema, and the
query surfaces it exposes.

| Document | Covers |
|----------|--------|
| [schema.md](schema.md) | Every ClickHouse table and field a user can query: the five base entity tables, the index tables, `meta_schema`, in-place indexes, materialized views, and the script-type / history-kind enumerations. |
| [query-cookbook.md](query-cookbook.md) | Practical SQL recipes: blocks, transactions, inputs/outputs, spends, addresses, UTXO set, miners, and cross-table joins — plus performance notes. |
| [cli.md](cli.md) | Every `bidx` subcommand and flag: `headers`, `parse`, `live`, `init-db`, `load`, `migrate`, `index`, `serve`, `verify`, `inspect`, `bench`. |
| [architecture.md](architecture.md) | How the pipeline works end to end: header pass, parallel parse, the RocksDB UTXO store and undo log, the Parquet sink, live reorg handling, the disk-space guard, and backpressure/memory safety. |
| [serving-api.md](serving-api.md) | The serving surfaces: Esplora-compatible REST, Electrum JSON-RPC, ops endpoints, metrics, and the address/hash conventions. |
| [deployment-ops.md](deployment-ops.md) | Building and deploying the binary and Compose stack, plus day-two ops: readiness, metrics, the disk-space guard, reorgs, backup, upgrades, and troubleshooting. |
| [data-model-rationale.md](data-model-rationale.md) | Why the schema is shaped the way it is: binary hashes, address representation, canonical tx identity, the precomputed `spends` join, partitioning, and the merge-engine caveats. |
| [contributing.md](contributing.md) | Dev setup, the workspace layout, the per-file coverage policy, how to change the schema, and the PR flow. |

## Suggested reading order

- **New to the project** — [architecture.md](architecture.md) first, then
  [schema.md](schema.md).
- **Writing queries** — [schema.md](schema.md) for the shape,
  [query-cookbook.md](query-cookbook.md) for recipes.
- **Operating the service** — [cli.md](cli.md), then
  [serving-api.md](serving-api.md).

## Keeping these documents honest

The authoritative sources are the Rust code, not these files. When behaviour
changes, update the matching document in the same change:

| Topic | Source of truth |
|-------|-----------------|
| Tables, fields, indexes, MVs | `crates/bidx-loader/src/schema.rs` |
| CLI subcommands and flags | `crates/bidx-cli/src/main.rs` (plus `bench/mod.rs`, `live.rs`) |
| Pipeline stages | `crates/bidx-cli/src/pipeline.rs`, `crates/bidx-store/src/utxo.rs` |
| REST routes | `crates/bidx-serve/src/service.rs`, `esplora.rs` |
| Electrum methods | `crates/bidx-serve/src/electrum.rs` |
| Config keys | `crates/bidx-serve/src/config.rs` |

The repo-level [README.md](../README.md) is the entry point; [PERF.md](../PERF.md)
holds the performance measurements referenced by `architecture.md`.
