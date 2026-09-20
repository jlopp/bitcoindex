# bitcoindex

A high-performance Bitcoin blockchain indexer. It reads raw `blk*.dat` files
directly from a synced Bitcoin Core node, parses blocks/transactions/inputs/
outputs in parallel, links spends through an embedded RocksDB UTXO set
(computing accurate fees), writes columnar Parquet intermediates, and
bulk-loads them into ClickHouse for fast analytical querying.

No Bitcoin Core RPC or JSON is involved in the hot path — we parse the raw
block files.

## Architecture

```
blk*.dat ──▶ header pass ──▶ ChainIndex (hash→height, cumulative-work main chain)
                │
                ▼
   parallel full parse (Rayon pool, per-thread mmap'd blk file cache)
                │  out-of-order results
                ▼
   reorder buffer (bounded channel, BTreeMap by height)
                │  strict height order
                ▼
   UTXO apply (RocksDB, single-threaded) ──▶ SpendRows + per-tx fees
                │
                ▼
   Parquet sink (5 entity part files, zstd, rotating every N blocks)
                │
                ▼
   ClickHouse bulk load (file() table function over Parquet globs)
```

### Crates

| Crate | Responsibility |
|-------|----------------|
| `bidx-core` | Wire-format cursor, double-SHA256 / hash160, script classification, shared row types |
| `bidx-parser` | `blk*.dat` mmap reader, two-pass orchestration (headers → full parse), tx parser |
| `bidx-store` | RocksDB UTXO set + spend linking; Arrow/Parquet buffered writers |
| `bidx-loader` | ClickHouse DDL and Parquet bulk loader (via `clickhouse-client`) |
| `bidx-cli` | `bidx` binary: `headers`, `parse`, `init-db`, `load`, `inspect` |

### Why these choices

- **Parallel ETL + bulk load, not MapReduce.** Parsing is embarrassingly
  parallel and CPU-bound; the expensive stage is DB load/index build. We
  parse to Parquet and bulk-load with minimal indexing, then rely on
  ClickHouse's sort keys / projections.
- **Two-pass parse.** Pass 1 reads only 80-byte headers to reconstruct the
  main chain (blocks in `blk*.dat` are *not* in height order, and stale
  branches exist). Pass 2 re-reads by recorded file offset at full
  parallelism, assigning correct heights.
- **RocksDB UTXO set.** Fees and address history require resolving each
  input's previous output. We keep the live UTXO set in an embedded RocksDB
  (no WAL during bulk load, LZ4/ZSTD) and apply blocks in height order —
  the single sequential stage. Same-block spend chains are resolved via an
  in-block pending map so created-and-spent-in-one-block outputs never
  touch the DB.
- **Binary hashes.** All hashes stored as `FixedString(32)` in wire order,
  not hex. Address-relevant data is stored as `(script_type, address_hash)`
  — 20/32-byte hashes, never base58/bech32 strings.
- **Canonical tx id.** `(height, tx_index)`, since early BIP30-era txids are
  not unique (heights 91812/91817, 91842/91880).

## Data model

Five core Parquet/ClickHouse tables: `blocks`, `transactions`, `inputs`,
`outputs`, `spends`. The `spends` table is the derived join
(`input → previous output`) enriched with the spent output's value, script
type, address hash, and creation height — this is what powers fees,
balances, and coin-flow queries.

ClickHouse additionally gets query-optimized projections:
`transactions_by_txid`, `outputs_by_address`, `inputs_by_prev_output`.

## Build

Requires a C toolchain and clang headers for RocksDB's bindgen step. On a
machine where `stdbool.h` isn't in clang's default path, point bindgen at a
shim (this repo creates `.build-shim/include` from gcc's builtins):

```bash
export LIBCLANG_PATH=/usr/lib/llvm-18/lib   # wherever libclang lives
export BINDGEN_EXTRA_CLANG_ARGS="-I$PWD/.build-shim/include"
cargo build --release
```

## Usage

```bash
# 1. Header pass only — sanity-check the chain
bidx headers --blocks-dir ~/.bitcoin/blocks

# 2. Full parse -> Parquet (parallel; UTXO apply is the sequential stage)
bidx parse \
  --blocks-dir ~/.bitcoin/blocks \
  --out ./out \
  --utxo ./utxo-db \
  --blocks-per-part 10000

# 3. Create ClickHouse schema
bidx init-db --database bitcoin

# 4. Bulk load Parquet -> ClickHouse
bidx load --input ./out --database bitcoin

# 5. Track the tip live: reconcile with the node, apply new blocks,
#    and handle reorgs via the UTXO undo log. Enable ZMQ in bitcoin.conf:
#      zmqpubhashblock=tcp://127.0.0.1:28332
#    then:
bidx live \
  --utxo ./utxo-db \
  --cookie ~/.bitcoin/.cookie \
  --zmq tcp://127.0.0.1:28332 \
  --out ./out-live

# Debug: dump one block
bidx inspect --blocks-dir ~/.bitcoin/blocks --height 0

# 6. Benchmark the ingest pipeline — isolate stage bottlenecks
#    Modes are orthogonal stage toggles so you can attribute time:
#      headers → header-pass only
#      parse   → header pass + parallel parse (no UTXO, no sink)
#      sink    → full stages but UTXO apply skipped (isolates Parquet cost)
#      utxo    → full stages but sink skipped (isolates RocksDB UTXO cost)
#      full    → everything
bidx bench --mode full   --blocks-dir ~/.bitcoin/blocks
bidx bench --mode parse  --blocks-dir ~/.bitcoin/blocks
bidx bench --mode sink   --blocks-dir ~/.bitcoin/blocks
bidx bench --mode utxo   --blocks-dir ~/.bitcoin/blocks

# Repeat trials, JSON report, custom UTXO dir, parser thread cap:
bidx bench --mode full --blocks-dir ~/.bitcoin/blocks \
  --repeat 3 --threads 16 --utxo /tmp/bidx-bench-utxo \
  --csv /tmp/bench.json
```

#### What a bench report looks like

Run against testnet4 (`~/.bitcoin/testnet4/blocks`, 24,820 blocks):

```
=== trial 0 (full) ===
blocks: 24820
blocks/s: 23898.0
stages:
                     header_pass  wall   0.022s
        parse (Σ worker CPU-ish)  wall   0.643s
         utxo_apply (sequential)  wall   0.452s
                    parquet_sink  wall   0.350s
             pipeline_wall_total  wall   1.039s  blocks/s 23898
```

Reading it:

- **Parallel parse is not the bottleneck.** Σ per-block parse time across all
  workers scales with cores; throughput in `parse` mode was ~280k blocks/s.
- **The sequential consumer stages dominate.** `utxo_apply` (RocksDB write
  batch per block) and `parquet_sink` (Arrow encode + zstd) each add
  ~0.3–0.5 s here, dropping effective throughput from ~280k to ~59k and
  ~46k blocks/s respectively; together (~12× vs parse-only) they set the
  end-to-end throughput ceiling on this dataset.
- Optimizations that matter are therefore on the sequential side:
  batching UTXO writes across multiple blocks before committing, increasing
  Parquet row-group size, or overlapping sink I/O with UTXO apply.

### Live mode & reorgs

`bidx live` opens the UTXO store in *live mode* (WAL synced per block, undo
logging enabled). Each applied block writes an undo record (spent outputs +
created outpoints) atomically with its UTXO mutations, keyed by height.

- **Startup reconcile**: the stored tip's hash is compared against the node
  at the same height. On mismatch (reorg while offline), blocks are
  disconnected via the undo log until a common ancestor is found, then the
  canonical chain is applied forward. `--max-reorg-depth` (default 100)
  bounds this; deeper reorgs abort with an error asking for a rescan.
- **New blocks**: a ZMQ `hashblock` notification triggers an idempotent
  `catch_up` (single `getblockhash` + `getblock`), which is also robust to
  out-of-order or stale-branch notifications. Without `--zmq`, it polls every
  2s.
- **Live Parquet**: with `--out`, each applied block (plus its spend rows)
  is appended to rolling part files (`out-live/<entity>/part-NNNNN.parquet`).
  Note: rows already flushed for a *disconnected* height are not retracted
  from a closed part — on load, drop ClickHouse partitions `>=` the reorg
  floor before reloading the affected range.

### Resumability

Parquet part files are named by block range (`out/blocks/part-00042.parquet`
etc.), and the UTXO set is durable in RocksDB. Re-running `parse` with
`--start`/`--end` reprocesses a range without redoing the whole chain.
Loading is per-entity via Parquet globs, so a failed load retries cleanly.

## Querying

Hashes are stored as raw bytes. To match a display hex txid, reverse it.
Example ClickHouse snippet to render a txid as conventional hex:

```sql
SELECT height, tx_index, lower(hex(reverse(txid))) AS txid_hex
FROM bitcoin.transactions
WHERE height = 0;
```

Find all outputs to an address hash (here a P2PKH hash160, left-padded to 32
bytes in the `address_hash` column's low 20 bytes):

```sql
SELECT height, txid, output_index, value_sat
FROM bitcoin.outputs_by_address
WHERE address_hash = <32-byte-hash>;
```

## Status / roadmap

- [x] Header pass with cumulative-work main-chain selection
- [x] Parallel full parse (txs, inputs, outputs, witness metrics)
- [x] RocksDB UTXO spend linking + fee computation
- [x] Parquet intermediates (zstd, rotating parts)
- [x] ClickHouse schema + bulk loader
- [x] Live tip tracking (ZMQ `hashblock` + RPC catch-up) with undo-log reorg handling
- [x] `bidx bench` — per-stage timing with orthogonal mode toggles (headers / parse / utxo / sink / full) to attribute bottlenecks
- [ ] Sink & UTXO overlap: currently both run on the consumer thread; could overlap sink I/O with RocksDB commit
- [ ] Retraction of live-Parquet rows for disconnected blocks (today: reload affected partition)
- [ ] Direct ClickHouse live inserts (bypass Parquet) for sub-minute tip freshness
- [ ] Optional witness payload table (kept out of the hot tables today)
- [ ] Base58/bech32 rendering helpers (SQL views or a small UDF crate)

## License

MIT
