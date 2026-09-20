# SKILLS.md — bidx (Bitcoin blockchain indexer)

> **MUST READ before making any code change in this repository.**
>
> This project is a high-performance Rust Bitcoin blockchain indexer that
> ingests `blk*.dat` files, links inputs to previous outputs via an
> embedded RocksDB UTXO store, and exports Parquet files for bulk load into
> ClickHouse. Ingest throughput is the product. Changes that trade
> throughput for elegance are regressions; the benchmarks below exist to
> catch them.

## Mandatory reading (in order)

1. **`.cursor/rules/rust-performance.mdc`** — Rust performance directives
   for this codebase. Read this before changing any `.rs` file on a hot
   path (parser, pipeline, UTXO store, Parquet writers, live tracker).
2. **`.cursor/rules/test-coverage.mdc`** — whenever you change a Rust file,
   that file must have 100% test coverage before the change is considered
   complete. Non-negotiable.

If your agent framework ingests `.cursor/rules/*.mdc` files
(Cursor, Continue, Cursor-compatible editors), they are already
registered. Otherwise read them directly from `.cursor/rules/`.

## Project layout (5 crates + CLI)

| Crate | Role | Hot path? |
|---|---|---|
| `crates/bidx-core` | Hashes, types, cursor, script classification, disk-space bookkeeping | parsing helpers |
| `crates/bidx-parser` | `blk*.dat` reader, chain reconstruction, full block/tx parsing | **YES — parse** |
| `crates/bidx-store` | RocksDB UTXO store + undo log, Parquet/Arrow writers | **YES — sequential consumer** |
| `crates/bidx-live` | ZMQ subscriber, RPC client, live tip tracker, reorg handling | live loop only |
| `crates/bidx-loader` | ClickHouse schema DDL + Parquet bulk loader | load time only |
| `crates/bidx-cli` | `bidx` binary: `headers`, `parse`, `live`, `load`, `bench`, `inspect` | glue |

Hot paths (the ones where microsecond decisions matter, and where the
bench measures them):

- `bidx_parser::parse_block_full` / `parse_tx` — the per-block parse.
- `pipeline::consume_ordered` — the ordered consumer thread. Runs once per
  block; currently the throughput bottleneck (UTXO apply + Parquet sink are
  both sequential). Any change that widens this bottleneck will show up
  immediately in `bidx bench --mode full` throughput.
- `bidx_store::UtxoStore::apply_block` — one RocksDB `WriteBatch` per block.
- `bidx_store::parquet_writers` — Arrow builder batch fill.

## How to benchmark before/after a change

```bash
# Test data on this machine:
#   mainnet: ~/.bitcoin/blocks          (genesis block only here)
#   testnet4: ~/.bitcoin/testnet4/blocks (24,820 blocks — the usable set)
#   signet: ~/.bitcoin/signet/blocks    (~85 MiB)

# Full pipeline benchmark (default: all stages)
cargo build --release && ./target/release/bidx bench \
  --mode full --blocks-dir ~/.bitcoin/testnet4/blocks

# Isolate a single stage — see README's bench section for the mode that
# isolates the stage you changed:
./target/release/bidx bench --mode parse  --blocks-dir ~/.bitcoin/testnet4/blocks
./target/release/bidx bench --mode utxo   --blocks-dir ~/.bitcoin/testnet4/blocks
./target/release/bidx bench --mode sink   --blocks-dir ~/.bitcoin/testnet4/blocks
```

Compare before/after throughput on the same dataset, same mode. Record the
before/after `blocks/s` and per-stage wall times in your PR or commit if
the change touches a hot path and the delta is >2%.

## The mandatory rules, condensed

- **Maximum ingest performance.** Allocate less, batch more, never copy
  what you can borrow, never hash hex on the hot path, never structure
  RocksDB writes as per-key calls, never drop batches to <100k rows.
  Directives in `.cursor/rules/rust-performance.mdc`.
- **100% test coverage on any Rust file you change.** Read
  `.cursor/rules/test-coverage.mdc`.

## Useful in-repo examples to crib from

| Need | Look at |
|---|---|
| Zero-copy slice parsing with bounds checks | `bidx-core/src/cursor.rs` |
| Little-endian wire → struct header parse | `bidx-core/src/types.rs:BlockHeader::parse` |
| Fixed-size, stack-allocated outpoint keys | `bidx-store/src/utxo.rs` (UTXO key = 36-byte array) |
| Per-thread init + lazy cache on Rayon workers | `bidx-cli/src/pipeline.rs` — `ThreadFileCache` |
| Backpressured crossbeam channel | `bidx-cli/src/pipeline.rs` — `bounded(512)` |
| Atomic UTXO + undo write in one RocksDB batch | `bidx-store/src/utxo.rs::apply_block` |
| Columnar batch builder for Parquet/Arrow | `bidx-store/src/parquet_writers.rs::Col` |
| Reorg-safe undo log | `bidx-store/src/utxo.rs::BlockUndo::encode/decode` |
| Per-network disk checkpoint format + regression growth model | `bidx-core/src/disk.rs` |

## What the pipeline does, in one breath

`bidx headers` builds a `ChainIndex` (height → blk file location) from the
80-byte headers in the blk files, then `bidx parse` fan-outs block parse
across a Rayon pool while a single ordered consumer thread replays the
blocks in height order, applies one RocksDB WriteBatch per block (UTXO
apply, with fees computed from prevout lookups), and pushes columnar
batches to rotating Parquet part files every 10k blocks. `bidx load` bulk-
imports those Parquet files into ClickHouse; `bidx live` maintains the tip
in live mode with reorg handling via a per-block undo log.
