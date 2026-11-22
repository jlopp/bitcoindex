# Architecture

How `bitcoindex` turns raw `blk*.dat` files into a queryable ClickHouse index,
and why each stage is built the way it is.

## Contents

- [The pipeline at a glance](#the-pipeline-at-a-glance)
- [Crates](#crates)
- [Stage 1: header pass](#stage-1-header-pass)
- [Stage 2: parallel full parse](#stage-2-parallel-full-parse)
- [Stage 3: ordered consumer](#stage-3-ordered-consumer)
- [The UTXO store](#the-utxo-store)
- [Stage 4: Parquet sink](#stage-4-parquet-sink)
- [Stage 5: ClickHouse load](#stage-5-clickhouse-load)
- [Live mode and reorgs](#live-mode-and-reorgs)
- [Disk-space guard](#disk-space-guard)
- [Backpressure and memory safety](#backpressure-and-memory-safety)
- [Data-model decisions](#data-model-decisions)
- [Where the time goes](#where-the-time-goes)

## The pipeline at a glance

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

Parsing is embarrassingly parallel and CPU-bound; the expensive part of the
job is building the index. So we parse to Parquet at full parallelism and
bulk-load with minimal indexing, letting ClickHouse's sort keys and projections
do the query-side work.

## Crates

| Crate | Responsibility |
|-------|----------------|
| `bidx-core` | Wire-format cursor, double-SHA256 / hash160, script classification, shared row types, disk-space helpers. |
| `bidx-parser` | `blk*.dat` reader, two-pass orchestration, tx parser. |
| `bidx-store` | RocksDB UTXO set + spend linking; Arrow/Parquet buffered writers. |
| `bidx-loader` | ClickHouse DDL and Parquet bulk loader (via `clickhouse-client`). |
| `bidx-live` | Node RPC client, ZMQ subscriber, live tip tracker with reorg handling. |
| `bidx-serve` | Read-only query layer: Esplora REST, Electrum JSON-RPC, metrics, health. |
| `bidx-cli` | The `bidx` binary: all subcommands, pipeline orchestration, bench harness. |

## Stage 1: header pass

`bidx_parser::build_chain_index` scans every `blkNNNNN.dat`, reading only the
80-byte block headers. Blocks in the files are **not** in height order and
stale (orphaned) branches exist, so the pass reconstructs the main chain by
**cumulative work**: starting from genesis, it extends the chain whose total
proof-of-work is greatest, not merely the longest.

The output is a `ChainIndex`:

- `by_height: Vec<BlockLocation>` — for each main-chain height, which file and
  byte offset holds the full block.
- `hash → height` for lookups.
- Stats: files scanned, records read, unique blocks, tip height/hash, orphan
  count.

This is why the pipeline is **two-pass**: the header pass discovers where each
main-chain block lives, and pass 2 re-reads those offsets at full parallelism.
It also means the header pass is a fixed whole-corpus cost regardless of the
`--start`/`--end` range — which is why `bidx bench` reports a
`blocks/s incl. header pass` figure separately.

## Stage 2: parallel full parse

`pipeline::run_parse` collects the heights in `[start, end)` and parses them
with a Rayon pool (`par_iter().try_for_each_init(...)`).

Each worker:

1. Looks up the block's file/offset in the `ChainIndex`.
2. Reads the raw bytes from a shared, bounded **`ThreadFileCache`**.
3. Re-parses the 80-byte header and computes the block hash with `dsha256`.
4. Calls `parse_block_full` to produce a `FullBlock` (block row, txs, inputs,
   outputs).
5. Sends the result into a bounded channel tagged with its height.

### The `ThreadFileCache`

`blk*.dat` files are memory-mapped and decoded, and each open `BlkFile` costs
~128 MiB resident (signet's XOR-obfuscated files allocate a 128 MiB decoded
buffer on open). A naive "one file per worker" scheme with 12 workers over 149
files would hold ~19 GiB. Instead, one cache is created and **cloned into each
worker**, so all workers share a single 16-file LRU (`MAX_CACHED_FILES`),
capping decoded-file memory at ~2 GiB. XOR decode (~150 ms per 128 MiB) only
runs on a cache miss.

This is a deliberate deviation from the rule of thumb "give each thread its own
cache": the point is to bound *total* memory, and to reproduce the
cross-worker eviction contention production actually has.

### Out-of-order results

Workers finish heights in arbitrary order, so each result carries its height
and is reordered downstream. A skipped height (an orphaned block) is sent as a
`WorkItem::Skipped { height }` so the ordered consumer can still advance past
it without emitting rows.

## Stage 3: ordered consumer

A single thread (`consume_ordered`) is the only sequential stage. It:

1. Receives `WorkItem`s and inserts blocks into a `BTreeMap<u32, FullBlock>`
   keyed by height.
2. Drains contiguous heights starting at `next`.
3. Applies UTXO changes via `UtxoStore::apply_block`.
4. Enriches transactions with fees and sets the block's `total_fee`.
5. Hands the enriched block to a dedicated **sink thread** over a bounded
   channel; the sink thread pushes rows into the Parquet sink and rotates part
   files every `blocks_per_part` blocks.

Why one consumer thread? Because the UTXO set must be mutated in strict height
order, and the RocksDB `WriteBatch` per block is the natural unit. Parallelising
apply would require partitioning the UTXO set, which is not worth the
complexity for a stage that is I/O-and-CPU bound per key rather than
bandwidth-bound.

### Sink overlap

The consumer's per-block work is the RocksDB commit (a WAL-synced `WriteBatch`
in live mode), and the sink's is Parquet column encoding plus file I/O. These
are independent, so the consumer hands each enriched block to a **sink thread**
(`sink_thread_main`) over a bounded `crossbeam_channel` (`SinkMsg::Block`) and
immediately proceeds to the next block. The two stages therefore run
concurrently: the sink compresses and writes one block while the consumer
commits the next. A `SinkMsg::Finish` sentinel flushes the last part and the
sink thread is joined, so a sink failure is still reported even on a complete
range. The bound on the channel keeps a slow sink from letting the consumer's
memory grow without limit.

### Fee enrichment

Fees come out of `apply_block`: each non-coinbase input's previous output value
minus the transaction's outputs. The consumer writes `tx.fee` back into each
`TxRow` and accumulates `total_fee` over non-coinbase transactions. The
coinbase flag is derived in **one pass** over inputs (a `Vec<bool>`), not by
re-scanning inputs for every transaction — the previous form was O(txs × inputs),
~16M comparisons on a 4k-tx block, on the single-threaded consumer.

## The UTXO store

`bidx_store::UtxoStore` is an embedded RocksDB keyed by **outpoint**
(`prev_txid` 32 bytes + `prev_vout` 4 bytes = 36-byte key). The value
(`UtxoValue`) holds `value_sat`, `script_type`, `height`, and `address_hash` —
exactly the fields `SpendRow` and fee computation need.

### `apply_block`

For one block, in order:

1. Insert every output into an in-memory `pending` map (`FxHashMap<[u8;36], UtxoValue>`).
2. For each non-coinbase input:
   - If the outpoint is in `pending`, it is a **same-block spend**: resolve from
     memory, never touch RocksDB. Track it in a `same_block` set so the undo
     record doesn't try to delete a key it also spent.
   - Else consult the **recent-UTXO cache** if enabled.
   - Else read from RocksDB (`get_pinned`), the baseline path.
3. Build a `WriteBatch` of deletes (spent keys) and puts (created keys).
4. If undo is enabled (live mode), write the `BlockUndo` record atomically with
   the batch.

The `pending` map is what makes same-block spend chains (a transaction spending
an output created earlier in the same block) never touch the database — a
common pattern in dense blocks.

### Undo log and reorgs

`BlockUndo` records what a block did, so it can be reversed:

- `spent`: outputs the block spent (re-inserted on disconnect).
- `created`: outpoints the block created (deleted on disconnect).

The encoding is a compact varint-prefixed form (81 bytes per spent entry, 36
per created). Undo records are keyed by height (`height.to_be_bytes()`) and
written **in the same `WriteBatch`** as the UTXO mutation — never as a separate
write. That atomicity is what makes reorg handling correct under crash.

Undo is **disabled in bulk load** (`parse` mode, no WAL, bulk-load RocksDB
settings) and **enabled in live mode** (WAL synced per block).

### The recent-UTXO cache

An optional bounded in-memory cache of the most recently created outputs,
enabled with `--utxo-cache-mb` (bulk mode only; `None` in live mode where
correctness outweighs speed). It answers a spend of a recent output without a
RocksDB lookup.

Measurement note (PERF.md §9): on testnet4, 71.6% of non-coinbase inputs were
database lookups costing ~35% of apply wall time, but a `batched_multi_get_cf`
prefetch over exactly that key set changed apply wall time by 0.04% (noise).
The cost is per-key RocksDB work, not I/O, so batching does not pay. The
instrumentation counters (`db_lookups`, `db_nanos`, `pending_hits`,
`cache_hits`) are kept because the question is worth re-asking on mainnet,
where the UTXO set is far larger.

### RocksDB tuning

The block cache is shared across all column families and sized well above
RocksDB's 32 MiB default (which is ~2000x smaller than the UTXO data set, so
nearly every point lookup had to fault a data block off disk). The tuning
`Options` are built once and cloned into **every** `ColumnFamilyDescriptor`:
the C API applies per-CF options to that family, so setting tuning only on the
DB-level `Options` has no effect on any column family.

## Stage 4: Parquet sink

`bidx_store::ParquetSink` writes five entity part files per part directory
(`blocks`, `transactions`, `inputs`, `outputs`, `spends`), zstd-compressed.
Part files are named by block range (`part-00042.parquet`), which makes
re-running a range idempotent at the file level.

Writers are **columnar**: rows are accumulated into type-erased column builders
(`Col` in `parquet_writers.rs`) and flushed as Arrow record batches of
`SinkConfig::batch_size` rows. This avoids per-row Arrow array allocation. The
consumer keeps building across blocks until a batch is full — it does not
force-flush per block, which would defeat zstd compression and row-group size
targets.

## Stage 5: ClickHouse load

`bidx-loader` shells out to `clickhouse-client` and uses ClickHouse's native
`file()` table function with a Parquet glob:

```sql
INSERT INTO bitcoin.blocks
SELECT * FROM file('/out/blocks/*.parquet', Parquet)
```

This keeps the heavy lifting in ClickHouse's vectorised Parquet reader rather
than round-tripping rows through the client. Every derived query index is an
auto-maintained table fed by a `MATERIALIZED VIEW` over a base table, so
loading the five base entities keeps the whole index catalogue in sync with no
extra write path. See [schema.md](schema.md).

## Live mode and reorgs

`bidx live` opens the UTXO store in **live mode** (WAL synced per block, undo
enabled) and drives `bidx_live::LiveTracker`.

### Startup reconcile

1. Fetch the node's tip height (`getblockcount`).
2. Compare the stored tip's hash against the node's hash **at the same height**.
3. On mismatch (a reorg happened while offline), `disconnect_block` via the
   undo log until a common ancestor is found, then apply the canonical chain
   forward.
4. `--max-reorg-depth` (default 100) bounds the walk; deeper reorgs abort with
   an error asking for a rescan.

### Staying current

A ZMQ `hashblock` notification (or a 2s poll fallback) triggers an **idempotent**
`catch_up`. `catch_up` deliberately ignores the notification's hash/height and
re-derives the difference from the node — that makes it robust to out-of-order,
duplicate, or stale-branch notifications, and cheap in the common case (one
`getblockhash` + one `getblock`).

### Live Parquet

With `--out`, each applied block plus its spend rows is buffered in memory for
the open part and written to rolling part files
(`out-live/<entity>/part-NNNNN.parquet`, 1,000 blocks/part) when the part
rotates or on shutdown. Because the open part is held in memory, a **shallow**
reorg (a disconnected height still in the open part) is retracted *exactly*:
those rows are dropped from the buffer before any file is written. A **deep**
reorg below the open part cannot rewrite a closed file, so the retraction floor
is recorded to `out-live/.live-reorg-floor`; on load, drop ClickHouse
partitions `>=` that floor before reloading the affected range.

### Direct ClickHouse live inserts

`bidx live --ch-live` bypasses Parquet entirely and streams each applied block
straight into ClickHouse for sub-minute tip freshness. One `clickhouse-client`
process is kept open per table (`INSERT INTO … FORMAT RowBinary` reading from
stdin), so a block costs a single buffered write per table instead of a process
spawn. Materialized views fire exactly as they do for a bulk load, so the
derived indexes stay in sync. A disconnected height is deleted by `height`
(`DELETE FROM … WHERE height >= floor`), since those rows were inserted on
apply. Witness payloads are inserted only when `--capture-witness` is set.

## Disk-space guard

Loading the full mainnet chain can need more space than the disk has, so
`parse` and `live` keep a per-network history of index size vs height and refuse
to start when the projected final size would leave less than 10 GiB free.

- The checkpoint history lives in `<parent-of-utxo>/bidx-disk-checkpoints.txt`
  (override with `--checkpoint-file`); one `network <name>` section per chain,
  lines are `height total_bytes`.
- **Recording**: every `CHECKPOINT_HEIGHT_STEP` blocks (100,000), the pipeline
  measures the on-disk size of the UTXO db + Parquet out dirs (including
  RocksDB WAL/SSTs) and appends `(height, total_bytes)`.
- **Projection**: at startup, `parse --rpc-url <url>` and `live` read the local
  indexed tip (highest recorded checkpoint), fetch the node tip via
  `getblockcount`, fit a linear regression through the history to get
  bytes/block growth, and project the index's size at the node tip. If that
  would leave <10 GiB free, the program refuses to start and says how much more
  space is needed.
- With no checkpoints yet (first mainnet run below height 100,000), the
  projection is skipped with a warning — there is no history to extrapolate
  from. The checkpoints written during that run prevent a repeat.

`parse` without `--rpc-url` logs a notice and skips the projection; `live`
always runs the guard (it already has a node connection). `--skip-disk-check`
bypasses entirely.

## Backpressure and memory safety

Three mechanisms keep the pipeline from exhausting memory on a large chain:

### Bounded channel

The worker → consumer channel is a bounded `crossbeam_channel` (`CHANNEL_DEPTH`
= 512). An unbounded channel is a memory leak under backpressure.

### Reorder watermark

A shared atomic counter tracks the consumer's reorder-buffer depth. When it
exceeds `REORDER_HIGH` (32 parsed blocks, ~320 MB worst case), workers spin
briefly until the consumer drains below it. Two guards make the spin safe:

- The worker whose height equals the consumer's `next_height` is **exempt**.
  Without that exemption the pipeline deadlocks: the one worker that can
  unblock the consumer would park, and nothing would ever arrive to let the
  consumer drain.
- The spin polls the OOM watchdog, so an aborted consumer turns into an error
  rather than a hang.

### OOM watchdog

`OomWatchdog` runs on a dedicated thread and only sets a flag when process RSS
exceeds a ceiling. It cannot itself abort the pipeline (unwinding from an
arbitrary thread would leave RocksDB and the Parquet writers in an unknown
state); instead the ordered consumer polls `is_tripped` once per block and
returns a clean, diagnostic error.

The ceiling (`pipeline::max_rss_kb`) is computed from **`MemAvailable`**, not
`MemTotal`, minus a 10% reserve and 2 GiB for the runtime. Using `MemTotal`
ignores every other process on the box, so on a busy machine the ceiling can
sit above where the kernel's OOM killer steps in — exactly how a 30 GiB machine
kills bidx at 21 GiB with no diagnostic.

## Data-model decisions

- **Binary hashes.** All hashes are `FixedString(32)` in wire order, never hex.
  Hex conversion happens only in the query layer for display.
- **Addresses as `(script_type, address_hash)`.** We store the 20- or 32-byte
  program right-padded to 32 bytes, never base58/bech32 strings. Rendering and
  parsing live in `bidx_serve::address` and are exhaustively tested against
  BIP173/BIP350 and the genesis address, because a silent codec bug corrupts
  every address-keyed query.
- **Canonical tx identity is `(height, tx_index)`.** Early BIP30-era txids are
  not unique (heights 91812/91817 and 91842/91880), so txid alone is not a key.
- **The `spends` table is the derived join.** Rather than looking up each
  input's previous output at query time, the consumer already resolved it to
  compute fees — so it is persisted, enriched with the spent output's value,
  script type, address hash, and creation height.
- **`OP_RETURN` is excluded from address indexes.** Its `address_hash` is all
  zeros and it carries no spendable value.

## Where the time goes

From the bench harness (testnet4, 24,820 blocks):

```
                     header_pass  wall   0.022s
        parse (Σ worker CPU-ish)  wall   0.643s
         utxo_apply (sequential)  wall   0.452s
                    parquet_sink  wall   0.350s
             pipeline_wall_total  wall   1.039s  blocks/s 23898
```

- **Parallel parse is not the bottleneck.** Summed per-block parse time scales
  with cores (~280k blocks/s in `parse` mode).
- **The sequential consumer stages dominate.** `utxo_apply` (RocksDB write batch
  per block) and `parquet_sink` (Arrow encode + zstd) each add ~0.3–0.5 s,
  dropping effective throughput from ~280k to ~46k blocks/s. Together they set
  the end-to-end ceiling.
- Optimizations that matter are therefore on the sequential side: batching UTXO
  writes, larger Parquet row groups, or overlapping sink I/O with UTXO apply
  (now implemented via the dedicated sink thread — see
  [Stage 3: ordered consumer](#stage-3-ordered-consumer)).

See [PERF.md](../PERF.md) for the full measurements and
[cli.md](cli.md#bench) for how to reproduce them.

## See also

- [schema.md](schema.md) — the ClickHouse tables and fields.
- [query-cookbook.md](query-cookbook.md) — practical SQL recipes.
- [cli.md](cli.md) — every subcommand and flag.
- [serving-api.md](serving-api.md) — the Esplora REST + Electrum JSON-RPC surface.
- [PERF.md](../PERF.md) — performance measurements and history.
