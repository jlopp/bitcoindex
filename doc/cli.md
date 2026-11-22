# CLI reference

The `bidx` binary is the single entry point for the whole pipeline: header
scan, full parse, ClickHouse schema management, bulk load, live tip tracking,
serving, verification, and benchmarking.

```
bidx [--log <filter>] <command> [options]
```

`--log` is a global tracing filter (default `info`); use `bidx=debug` for
verbose library logs, e.g. `bidx --log bidx=debug parse ...`.

## Contents

- [`headers`](#headers)
- [`parse`](#parse)
- [`live`](#live)
- [`init-db`](#init-db)
- [`load`](#load)
- [`migrate`](#migrate)
- [`index list` / `index rebuild`](#index)
- [`serve`](#serve)
- [`verify`](#verify)
- [`inspect`](#inspect)
- [`bench`](#bench)
- [Typical workflows](#typical-workflows)

---

## headers

Header pass only: scan `blk*.dat`, reconstruct the main chain by cumulative
work, print stats. This is the fast sanity check before a full parse — it reads
only the 80-byte headers.

```bash
bidx headers --blocks-dir ~/.bitcoin/blocks
```

| Flag | Default | Description |
|------|---------|-------------|
| `--blocks-dir <PATH>` | *(required)* | Bitcoin Core blocks directory (contains `blkNNNNN.dat`). |

Prints files scanned, records read, unique blocks, main-chain tip
height/hash, orphan count, and chain-index entry count.

---

## parse

Full ingest: parse blocks in parallel, link spends through the RocksDB UTXO
set (computing fees), and write zstd Parquet intermediates.

```bash
bidx parse \
  --blocks-dir ~/.bitcoin/blocks \
  --out ./out \
  --utxo ./utxo-db \
  --blocks-per-part 10000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--blocks-dir <PATH>` | *(required)* | Core blocks directory. |
| `--out <PATH>` | *(required)* | Output directory for Parquet part files. |
| `--utxo <PATH>` | *(required)* | RocksDB UTXO store path (created if missing). |
| `--start <H>` | `0` | Start height (inclusive). |
| `--end <H>` | chain tip | End height (exclusive). |
| `--blocks-per-part <N>` | `10000` | Blocks per Parquet part file. |
| `--threads <N>` | `num_cpus` | Parser worker threads. |
| `--rpc-url <URL>` | *(none)* | Enables the disk-space pre-flight (queries `getblockcount`). |
| `--rpc-cookie <PATH>` | `~/.bitcoin/.cookie` for mainnet | Cookie file for the RPC call. |
| `--skip-disk-check` | off | Skip the disk-space startup check entirely. |
| `--network <NET>` | auto-detected | `mainnet`, `testnet3`, `testnet4`, `signet`, `regtest`. |
| `--checkpoint-file <PATH>` | `<parent-of-utxo>/bidx-disk-checkpoints.txt` | Override the disk-check checkpoint path. |
| `--capture-witness` | off | Also write raw witness stack elements to the optional `witnesses` Parquet table. Kept out of the hot tables by default. |

Notes:

- Without `--rpc-url` the disk-space projection is skipped (there is no chain
  tip to compare against). `--network` is auto-detected from the first `blk`
  file when absent.
- The UTXO apply stage is the single sequential stage; everything before it is
  parallel. See [architecture.md](architecture.md).
- When the pre-flight succeeds it also fetches the node's orphaned-block hashes
  and skips them during parse.

---

## live

Track the chain tip: reconcile with the node, apply new blocks, handle reorgs
via the UTXO undo log, and optionally append to Parquet.

```bash
bidx live \
  --utxo ./utxo-db \
  --cookie ~/.bitcoin/.cookie \
  --zmq tcp://127.0.0.1:28332 \
  --out ./out-live
```

| Flag | Default | Description |
|------|---------|-------------|
| `--rpc-url <URL>` | `http://127.0.0.1:8332` | Bitcoin Core RPC URL. |
| `--cookie <PATH>` | *(none)* | Node `.cookie` file (used if user/password absent). |
| `--rpc-user <USER>` | *(none)* | Explicit RPC user. |
| `--rpc-password <PASS>` | *(none)* | Explicit RPC password. |
| `--zmq <ENDPOINT>` | *(none)* | ZMQ `hashblock` endpoint, e.g. `tcp://127.0.0.1:28332`. Without it, polls every 2s. |
| `--utxo <PATH>` | *(required)* | RocksDB UTXO store path (must match the one `parse` used). |
| `--out <PATH>` | *(none)* | Optional Parquet output dir; live blocks are buffered for the open part and written to rolling part files (1,000 blocks/part). |
| `--max-reorg-depth <N>` | `100` | Max reorg depth to auto-handle before aborting. |
| `--blocks-dir <PATH>` | mainnet | Used only to auto-detect the network for the checkpoint section. |
| `--network <NET>` | auto-detected | Chain network override. |
| `--skip-disk-check` | off | Skip the disk-space startup check. |
| `--checkpoint-file <PATH>` | `<parent-of-utxo>/bidx-disk-checkpoints.txt` | Override the checkpoint path. |
| `--capture-witness` | off | Also append raw witness stack elements to the optional `witnesses` Parquet table (requires `--out`) and the `witnesses` ClickHouse table. |
| `--ch-live` | off | Insert each applied block **directly** into ClickHouse, bypassing Parquet, for sub-minute tip freshness. |
| `--ch-host <HOST>` | `localhost` | ClickHouse host for `--ch-live`. |
| `--ch-port <PORT>` | `9000` | ClickHouse native port for `--ch-live`. |
| `--ch-user <USER>` | `default` | ClickHouse user for `--ch-live`. |
| `--ch-password <PASS>` | *(none)* | ClickHouse password for `--ch-live`. |
| `--ch-database <DB>` | `bitcoin` | ClickHouse database for `--ch-live`. |

Notes:

- Enable ZMQ in `bitcoin.conf`: `zmqpubhashblock=tcp://127.0.0.1:28332`.
- A **shallow** reorg (disconnected height still in the open part) is retracted
  exactly from the in-memory buffer. A **deep** reorg below the open part writes
  the retraction floor to `<out>/.live-reorg-floor` — on load, drop ClickHouse
  partitions `>=` that floor before reloading the affected range.
- With `--ch-live`, rows are inserted on apply, so a disconnected height is
  removed by `height` (`DELETE FROM … WHERE height >= floor`).
- `--ch-live` and `--out` are independent; supplying both streams to ClickHouse
  *and* keeps rolling Parquet parts.

---

## init-db

Create the ClickHouse database, all base + index tables, and all materialized
views. Idempotent — safe to re-run. Records the schema version in
`meta_schema`.

```bash
bidx init-db --database bitcoin
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host <HOST>` | `localhost` | ClickHouse host. |
| `--port <PORT>` | `9000` | **Native** protocol port (used via `clickhouse-client`). |
| `--user <USER>` | `default` | ClickHouse user. |
| `--password <PASS>` | *(none)* | ClickHouse password. |
| `--database <DB>` | `bitcoin` | Database name. |

Requires `clickhouse-client` on `PATH` (or `BIDX_CLICKHOUSE_CLIENT` pointing at
a client binary).

---

## load

Bulk-load Parquet files into ClickHouse using the `file()` table function over
a Parquet glob, one entity at a time. Derived index tables are populated
automatically by the materialized views.

```bash
bidx load --input ./out --database bitcoin
```

| Flag | Default | Description |
|------|---------|-------------|
| `--input <PATH>` | *(required)* | Parquet output directory from `parse`. |
| `--host <HOST>` | `localhost` | ClickHouse host. |
| `--port <PORT>` | `9000` | Native protocol port. |
| `--user <USER>` | `default` | ClickHouse user. |
| `--password <PASS>` | *(none)* | ClickHouse password. |
| `--database <DB>` | `bitcoin` | Database name. |

Run `bidx init-db` first. Loading is per-entity, so a failed load retries
cleanly.

---

## migrate

Apply pending schema migrations and report the schema version. Forward-only
and idempotent.

```bash
bidx migrate --database bitcoin
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host <HOST>` | `localhost` | ClickHouse host. |
| `--port <PORT>` | `9000` | Native protocol port. |
| `--user <USER>` | `default` | ClickHouse user. |
| `--password <PASS>` | *(none)* | ClickHouse password. |
| `--database <DB>` | `bitcoin` | Database name. |

Prints `schema up to date (version N)` when nothing was pending, otherwise
`applied migrations: [...]`.

---

## index

Inspect and maintain the query-index catalogue.

### `index list`

Print every index: name, mechanism, and target table.

```bash
bidx index list
```

### `index rebuild <name>`

Rebuild a **backfilled** index. Only three names are valid;
every other index is maintained automatically by its materialized view and
will be rejected with `unknown index '<name>'`.

| Name | What it does |
|------|--------------|
| `coinstats_by_height` | Recompute the UTXO-set snapshot per 10k-block checkpoint from funding/spend history. |
| `scripthash_map` | Rebuild the Electrum scripthash → `(script_type, address_hash)` map from `outputs`. Run once after upgrading a database loaded before the MV existed. |
| `address_prefix` | Rebuild the `address` → `(script_type, address_hash)` map: stream distinct pairs from `outputs`, render base58/bech32 in Rust, insert. Pass `--network` to match the chain. |

```bash
bidx index rebuild coinstats_by_height --database bitcoin
bidx index rebuild address_prefix --network mainnet --database bitcoin
```

| Flag | Default | Description |
|------|---------|-------------|
| `<name>` | *(required)* | Index name (see `index list`). |
| `--network <NET>` | `mainnet` | Chain network for `address_prefix` rendering: `mainnet`, `testnet3`, or `regtest`. Ignored by the SQL-only backfills. |
| `--host <HOST>` | `localhost` | ClickHouse host. |
| `--port <PORT>` | `9000` | Native protocol port. |
| `--user <USER>` | `default` | ClickHouse user. |
| `--password <PASS>` | *(none)* | ClickHouse password. |
| `--database <DB>` | `bitcoin` | Database name. |

---

## serve

Serve the query API: Esplora-compatible REST, Electrum JSON-RPC, and
Prometheus metrics / health endpoints. Configuration comes from a TOML file
plus `BIDX_*` environment overrides (see [serving-api.md](serving-api.md)).

```bash
bidx serve --config /etc/bidx/bidx.toml
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config <PATH>` | *(none)* | TOML config file. Without one, defaults + `BIDX_*` env are used. |

Two listeners:

- `api_addr` (default `0.0.0.0:8432`) — REST + JSON-RPC.
- `metrics_addr` (default `0.0.0.0:9101`) — `/metrics`, `/healthz`, `/readyz`.

The serve layer talks to ClickHouse over its **HTTP** port (`http_port`,
default `8123`), not the native `9000` port used by `init-db`/`load`.

---

## verify

Verify index integrity: structural invariants plus optional agreement with a
Bitcoin Core node tip. Exits non-zero when any hard check fails.

```bash
bidx verify --database bitcoin --rpc-url http://127.0.0.1:8332
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host <HOST>` | `localhost` | ClickHouse host. |
| `--port <PORT>` | `9000` | Native protocol port. |
| `--user <USER>` | `default` | ClickHouse user. |
| `--password <PASS>` | *(none)* | ClickHouse password. |
| `--database <DB>` | `bitcoin` | Database name. |
| `--rpc-url <URL>` | *(none)* | Enables the node-agreement check. |
| `--rpc-cookie <PATH>` | *(none)* | Node `.cookie` file. |
| `--rpc-user <USER>` | *(none)* | Explicit RPC user. |
| `--rpc-password <PASS>` | *(none)* | Explicit RPC password. |

Checks performed:

| Check | Kind | Fails when |
|-------|------|-----------|
| `unique_block_heights` | hard | Duplicate heights present. |
| `one_coinbase_per_block` | hard | Coinbase-input count != block count. |
| `spend_linking` | hard | `spends` count != non-coinbase input count. |
| `node_agreement` | hard / warn | Indexed height is ahead of the node (hard); node unreachable (warn). |
| `full_chain_from_genesis` | warn | Index is a partial range. |

Output is one line per check: `[PASS]`, `[FAIL]`, or `[WARN]`, then
`verify: OK` or a non-zero exit with the failure count.

---

## inspect

Parse a single block at a height and print it as JSON (debug).

```bash
bidx inspect --blocks-dir ~/.bitcoin/blocks --height 0
```

| Flag | Default | Description |
|------|---------|-------------|
| `--blocks-dir <PATH>` | *(required)* | Core blocks directory. |
| `--height <H>` | *(required)* | Block height to dump. |

---

## bench

Benchmark the ingest pipeline against a blocks directory, reporting per-stage
timing, wall-clock throughput, CPU utilisation, and peak RSS. Modes are
orthogonal stage toggles so you can attribute the bottleneck.

```bash
bidx bench --mode full --blocks-dir ~/.bitcoin/blocks
```

### Modes

| Mode | Header pass | Parallel parse | UTXO apply | Parquet sink | Purpose |
|------|:-----------:|:--------------:|:----------:|:------------:|---------|
| `headers` | ✓ | | | | Chain-index construction only. |
| `parse` | ✓ | ✓ | | | Parse + reorder/enrich, no UTXO/sink. |
| `drain` | ✓ | ✓ | | | Pure parse ceiling: consumer discards every block. |
| `utxo` | ✓ | ✓ | ✓ | | Isolates RocksDB UTXO apply cost. |
| `sink` | ✓ | ✓ | | ✓ | Isolates Parquet encode + I/O cost. |
| `full` | ✓ | ✓ | ✓ | ✓ | Everything (default). |

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--blocks-dir <PATH>` | *(required)* | Core blocks directory. |
| `--mode <MODE>` | `full` | `full`, `headers`, `parse`, `drain`, `utxo`, `sink`. |
| `--start <H>` | `0` | Start height (inclusive). |
| `--end <H>` | chain tip | End height (exclusive). |
| `--threads <N>` | `num_cpus` | Parser worker threads. |
| `--utxo <PATH>` | throwaway under `--utxo-dir` | UTXO store path. |
| `--utxo-dir <PATH>` | `./bidx-perf` | Where to create the throwaway UTXO store. |
| `--out-dir <PATH>` | `./bidx-perf/<unique>` | Parquet output dir for sink/full/utxo modes. |
| `--blocks-per-part <N>` | `10000` | Blocks per Parquet part file. |
| `--sink-batch-rows <N>` | production value (`250000`) | Rows per Parquet record batch. |
| `--reorder-high <N>` | production (`32`) | Reorder-buffer high watermark. |
| `--channel-depth <N>` | `512` | Worker → consumer channel depth. |
| `--repeat <N>` | `1` | Trial runs; results are aggregated (min/median/max). |
| `--allow-missing-utxos` | off | Accept a cold/dirty UTXO store (see below). |
| `--utxo-cache-mb <MiB>` | `0` | Enable the in-memory recent-UTXO cache (bulk mode only). |
| `--rocksdb-stats` | off | Emit a RocksDB internal-properties snapshot per trial. |
| `--csv <PATH>` | *(none)* | Write per-trial JSON to this path; `-` for stdout. |

### Reading the output

```
=== trial 0 (full) ===
blocks: 24820
blocks/s: 23898.0
blocks/s incl. header pass: 22410.1
user cpu: 3.4s peak rss: 412.0 MB
missing utxos: 0
utxo lookups: 128 db / 4096 pending (3.0% db)  db_time: 0.041s (8.9% of apply)
stages:
                     header_pass  wall   0.022s  cpu_util 12%  peak_rss     64 MB
       parse (Σ worker CPU-ish)  wall   0.643s  cpu_util  0%  peak_rss      0 MB
         utxo_apply (sequential)  wall   0.452s  cpu_util  0%  peak_rss      0 MB
                    parquet_sink  wall   0.350s  cpu_util  0%  peak_rss      0 MB
             pipeline_wall_total  wall   1.039s  cpu_util 78%  peak_rss    412 MB
per-block parse   p50    0.03ms  p99     0.09ms  max     1.20ms  (n=24820)
per-block apply   p50    0.02ms  p99     0.06ms  max     0.80ms  (n=24820)
```

- `parse (Σ worker CPU-ish)` is the **sum** of per-block parse time across all
  workers (a CPU-effort proxy), not wall clock. Sequential stages (`utxo_apply`,
  `parquet_sink`) run on one consumer thread, so their sums are true wall time.
- `blocks/s incl. header pass` is the end-to-end figure — quote this one for a
  full-corpus run, since the header pass is a fixed whole-corpus scan that
  dominates short ranges.
- `db_lookups` is the only set a batched UTXO prefetch could cover, and
  `db_time` its ceiling. If `db_lookups == 0`, no prefetch would help.

> **`missing utxos` must be 0.** A nonzero count means `apply_block` could not
> resolve a spent outpoint, so the run measured a *cold* UTXO store and its
> `utxo_apply` number is meaningless. The bench fails a trial in `full`/`utxo`
> mode when this happens unless you pass `--allow-missing-utxos`.

`--utxo-dir` defaults to `./bidx-perf`, **not** the system temp dir: on many
machines `/tmp` is tmpfs (RAM-backed), which flatters RocksDB by absorbing
writes at memory speed and hides the stalls the benchmark exists to find.

Bench runs deliberately do not write disk checkpoints — use `parse` for real
runs that train the disk-space model.

---

## Typical workflows

### Full mainnet index from genesis

```bash
bidx headers --blocks-dir ~/.bitcoin/blocks
bidx parse --blocks-dir ~/.bitcoin/blocks --out ./out --utxo ./utxo-db \
  --rpc-url http://127.0.0.1:8332
bidx init-db --database bitcoin
bidx load --input ./out --database bitcoin
bidx verify --database bitcoin --rpc-url http://127.0.0.1:8332
```

### Resume a partial parse

```bash
bidx parse --blocks-dir ~/.bitcoin/blocks --out ./out --utxo ./utxo-db \
  --start 500000 --end 600000
```

### Run the serving API

```bash
bidx serve --config bidx.toml
curl -s http://127.0.0.1:8432/blocks/tip/height
curl -s http://127.0.0.1:9101/readyz
```

### Refresh a backfilled index after upgrading

```bash
bidx index list
bidx index rebuild scripthash_map --database bitcoin
bidx index rebuild coinstats_by_height --database bitcoin
bidx index rebuild address_prefix --network mainnet --database bitcoin
```

## Environment variables

| Variable | Effect |
|----------|--------|
| `BIDX_CLICKHOUSE_CLIENT` | Path to a specific `clickhouse-client` binary (overrides `PATH`). |
| `BIDX_NETWORK` | `serve` network override. |
| `BIDX_API_ADDR`, `BIDX_METRICS_ADDR` | `serve` listener addresses. |
| `BIDX_ESPLORA_ENABLED`, `BIDX_ELECTRUM_ENABLED` | Toggle the two serving surfaces. |
| `BIDX_RUST_LOG` | `serve` log filter. |
| `BIDX_CLICKHOUSE__{HOST,PORT,HTTP_PORT,USER,PASSWORD,DATABASE,TIMEOUT_SECS}` | `serve` ClickHouse settings (`__` nests). |
| `BIDX_NODE__{RPC_URL,COOKIE}` | `serve` node connection. |

See [serving-api.md](serving-api.md) for the full configuration reference.
