# Deployment and operations

How to build, deploy, and run `bitcoindex`: the binary, the Compose stack, the
ClickHouse store, and the day-two operations (readiness, metrics, disk-space
guard, reorgs, backup, upgrades, troubleshooting).

For the commands themselves see [cli.md](cli.md); for the serving surface see
[serving-api.md](serving-api.md). For Kubernetes, managed ClickHouse, and
scaling a query fleet, see [cloud-deployment.md](cloud-deployment.md).

## Contents

- [Building the binary](#building-the-binary)
- [The Compose stack](#the-compose-stack)
- [First run](#first-run)
- [Configuration](#configuration)
- [Health, readiness, and metrics](#health-readiness-and-metrics)
- [Disk-space guard](#disk-space-guard)
- [Live mode operations](#live-mode-operations)
- [Reorgs](#reorgs)
- [Backup and restore](#backup-and-restore)
- [Upgrades and migrations](#upgrades-and-migrations)
- [Troubleshooting](#troubleshooting)

## Building the binary

`bidx` links RocksDB (via `librocksdb-sys`, which runs bindgen) and, for live
mode, the system `libzmq`. Building from source needs a C/C++ toolchain,
`libclang`, `cmake`, `pkg-config`, and `libzmq3-dev`:

```bash
sudo apt-get install -y clang libclang-dev llvm-dev cmake pkg-config libzmq3-dev

# If stdbool.h isn't in clang's default include path, generate the shim from
# gcc's builtins and point bindgen at it:
./scripts/build-shim.sh
export LIBCLANG_PATH=/usr/lib/llvm-18/lib       # wherever libclang lives
export BINDGEN_EXTRA_CLANG_ARGS="-I$PWD/.build-shim/include"
export CXXFLAGS="-include cstdint"               # rocksdb 8.x transitive <cstdint>
cargo build --release
```

The `Dockerfile` bakes in both workarounds — it runs `scripts/build-shim.sh`
inside the builder stage — so the container build needs no special environment.
The shim itself is machine-specific and gitignored, which is why it is generated
rather than shipped.

Prebuilt binaries for `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`,
and `aarch64-unknown-linux-gnu` are attached to each `v*` tag release, together
with a CycloneDX SBOM and a `SHA256SUMS` file. Verify the checksum before
running a downloaded binary.

## The Compose stack

`docker-compose.yml` brings up two services:

- **`clickhouse`** — the serving store, `clickhouse/clickhouse-server:24.8`,
  database `bitcoin`. Data persists in `./data/clickhouse`.
- **`bidx`** — runs `bidx serve`, exposing the API on `:8432` and metrics on
  `:9101`. It waits for ClickHouse's healthcheck before starting.

The ingest side (`bidx parse` / `bidx load`) runs as **one-off commands**, not
long-lived services — see [First run](#first-run).

```
./data/
├── blocks/       # your Bitcoin Core blocks dir, mounted read-only
├── clickhouse/   # ClickHouse data dir (persisted)
├── out/          # Parquet intermediates (also mounted into ClickHouse as user_files)
└── utxo/         # RocksDB UTXO store
```

The loader uses ClickHouse's `file()` table function, which is confined to the
server's `user_files` directory — that is why `./data/out` is mounted at
`/var/lib/clickhouse/user_files/parquet` **read-only**.

## First run

```bash
mkdir -p data/{utxo,out,clickhouse,blocks}
# point BLOCKS_DIR at your synced Bitcoin Core blocks directory
export BLOCKS_DIR=~/.bitcoin/blocks

docker compose up -d                       # ClickHouse + serve
docker compose run --rm bidx init-db       # create schema
docker compose run --rm bidx load --input /data/out
```

For the ingest itself, run `parse` (or `live`) on the host or as a one-off with
the blocks dir mounted. The canonical order from the README:

```bash
bidx headers --blocks-dir ~/.bitcoin/blocks           # sanity-check the chain
bidx parse   --blocks-dir ~/.bitcoin/blocks \
             --out ./out --utxo ./utxo-db \
             --rpc-url http://127.0.0.1:8332          # enables the disk guard
bidx init-db --database bitcoin
bidx load    --input ./out --database bitcoin
bidx verify  --database bitcoin --rpc-url http://127.0.0.1:8332
```

## Configuration

The serving layer reads a TOML file plus `BIDX_*` environment overrides.
Precedence is **defaults < TOML < environment**. Start from
`bidx.toml.example`; nested keys use `__` (e.g. `BIDX_CLICKHOUSE__HOST`).

Key operational settings:

| Setting | Default | Notes |
|---------|---------|-------|
| `network` | `mainnet` | `mainnet`/`testnet3`/`testnet4`/`signet`/`regtest`; must match the indexed chain. |
| `api_addr` | `0.0.0.0:8432` | REST + Electrum. |
| `metrics_addr` | `0.0.0.0:9101` | `/metrics`, `/healthz`, `/readyz`. |
| `esplora_enabled` / `electrum_enabled` | `true` | At least one must be true. |
| `clickhouse.port` | `9000` | Native protocol — `init-db` / `load`. |
| `clickhouse.http_port` | `8123` | HTTP — the **serve** layer. Don't set this to 9000. |
| `clickhouse.database` | `bitcoin` | Must be non-empty. |
| `clickhouse.timeout_secs` | `30` | Must be > 0. |
| `clickhouse.host` | `localhost` | `serve` accepts a comma-separated replica list with failover. |
| `clickhouse.scheme` | `http` | `https` for TLS to ClickHouse's HTTP interface (managed services). |
| `server.workers` / `server.queue` | `256` / `1024` | Worker pool and accept queue; beyond the queue, connections get `503`. |
| `server.drain_delay_secs` / `server.shutdown_timeout_secs` | `5` / `25` | SIGTERM drain sequence; keep their sum under your orchestrator's grace period. |
| `cache.enabled` | `true` | In-process response cache plus `Cache-Control` headers. |
| `auth.required` | `false` | Require an API key on the API surface. |
| `node.rpc_url` | *(none)* | Optional; enables the readiness lag metric and `verify`'s node check. |

`api_addr` and `metrics_addr` must differ, and the service refuses to start
otherwise. Prefer `BIDX_CLICKHOUSE__PASSWORD` over a password in the file so
the secret is not on disk.

## Health, readiness, and metrics

| Endpoint | Meaning |
|----------|---------|
| `GET /healthz` | Always `200 ok` — process liveness. |
| `GET /readyz` | `200` when ready, else `503`, with a per-check JSON body. |
| `GET /metrics` | Prometheus text exposition. |

`/readyz` requires a verified ClickHouse connection **and** a non-zero indexed
tip: a freshly `init-db`'d but unloaded database is *not* ready to serve. It
also returns 503 from SIGTERM onward while the process drains. Wire it into your orchestrator's readiness probe (not liveness) so
traffic does not reach an empty index.

Key metrics for dashboards and alerts:

- `bidx_indexed_height` and `bidx_node_height` — the two heights to compare.
- `bidx_indexer_lag_blocks` — node height minus indexed height; alert when it
  grows and does not recover.
- `bidx_queries_total`, `bidx_query_errors_total` — throughput and error rate.
- `bidx_address_not_found_total` — address misses (expected for fresh
  addresses; a spike can indicate an indexing gap).
- `bidx_requests_by_surface_total{surface}` and
  `bidx_responses_by_status_total{status}` — traffic and status mix.
- `bidx_request_duration_seconds` — latency histogram (p50/p99 via
  `histogram_quantile`).
- `bidx_inflight_requests`, `bidx_shed_total` — saturation; any shedding
  means scale out.
- `bidx_cache_hits_total` / `bidx_cache_misses_total` — cache effectiveness.
- `bidx_rate_limited_total`, `bidx_unauthorized_total`, and the per-tenant
  `bidx_tenant_*` families — API-key usage.

The serving loop refreshes health/metrics once at startup and every 10 seconds
thereafter; `/metrics` renders on scrape.

## Disk-space guard

Loading full mainnet can need more space than the disk has, so `bidx parse`
(with `--rpc-url`) and `bidx live` run a **pre-flight guard** and refuse to
start when the projected final index size would leave less than **10 GiB** free.

- Checkpoint history lives in
  `<parent-of-utxo>/bidx-disk-checkpoints.txt` (override with
  `--checkpoint-file`), one `network <name>` section per chain, lines are
  `height total_bytes`.
- Every 100,000 blocks the pipeline records the on-disk size of the UTXO db +
  Parquet out dirs (including RocksDB WAL/SSTs).
- At startup it fits a linear regression through that history, projects the
  size at the node tip, and aborts with the shortfall if the projection leaves
  < 10 GiB free.

Operational notes:

- `bidx parse` **without** `--rpc-url` skips the projection (there is no chain
  tip to compare against) and logs a notice. Pass `--rpc-url` to guard bulk
  loads too.
- `--skip-disk-check` bypasses entirely — use it deliberately, not to silence
  a real warning.
- With fewer than one checkpoint (a first run below height 100,000) the
  projection is skipped with a warning; the checkpoints that run writes prevent
  a repeat.
- `bidx bench` deliberately does **not** write checkpoints — use `parse` for
  real runs that train the model.

## Live mode operations

`bidx live` keeps the index at the node tip. It opens the UTXO store in **live
mode** (WAL synced per block, undo enabled) and needs the node reachable.

```bash
bidx live \
  --utxo ./utxo-db \
  --cookie ~/.bitcoin/.cookie \
  --zmq tcp://127.0.0.1:28332 \
  --out ./out-live
```

- Enable ZMQ in `bitcoin.conf`: `zmqpubhashblock=tcp://127.0.0.1:28332`. Without
  `--zmq` it polls every 2s.
- `catch_up` is **idempotent** and ignores the notification's hash/height,
  re-deriving the difference from the node — robust to duplicate, out-of-order,
  or stale-branch notifications.
- **Live Parquet caveat:** with `--out`, each applied block is appended to
  rolling part files (1,000 blocks/part). Rows already flushed for a
  *disconnected* height are **not retracted**. On load, drop the ClickHouse
  partitions `>=` the reorg floor before reloading the affected range.
- Direct live inserts into ClickHouse (bypassing Parquet) are on the roadmap.

## Reorgs

Reorg handling is driven by the undo log, which records what each block did
(spent outputs to re-insert, created outpoints to delete). Undo records are
written **in the same `WriteBatch`** as the UTXO mutation, so a crash cannot
leave them inconsistent.

- **On startup**, the stored tip's hash is compared against the node's hash at
  the same height. On mismatch (a reorg happened while offline), blocks are
  disconnected via the undo log until a common ancestor is found, then the
  canonical chain is applied forward.
- `--max-reorg-depth` (default 100) bounds the walk. A deeper reorg aborts with
  an error asking for a rescan — this is intentional; auto-handling arbitrarily
  deep reorgs is riskier than a rescan.
- Undo is **disabled in bulk load** (`parse` mode: no WAL, bulk-load RocksDB
  settings) and **enabled in live mode**. If you only ever run `parse`, you do
  not pay for undo records.

## Backup and restore

Two independent pieces of state:

- **ClickHouse** (`./data/clickhouse`) — the queryable index. This is
  reconstructible from the Parquet output (`bidx load`) or from a fresh parse.
  Back it up with your normal ClickHouse procedure (filesystem snapshot of the
  data dir, or `BACKUP TABLE`).
- **RocksDB UTXO store** (`./data/utxo`) — the live spend-linking state and,
  in live mode, the undo log. It is also reconstructible (re-run `parse`), but
  a re-parse of the whole chain is the expensive path, so snapshot it if you
  run `live`.

The Parquet intermediates (`./data/out`) are the cheapest restore path: they
let you reload ClickHouse without re-parsing `blk*.dat`. Keep them until you no
longer need to reload.

To rebuild the backfilled indexes after a restore, run
`bidx index rebuild coinstats_by_height` and
`bidx index rebuild scripthash_map` (see [cli.md](cli.md#index)).

## Upgrades and migrations

`bidx init-db` is idempotent and records the schema version in `meta_schema`.
`bidx migrate` applies pending migrations and prints the version:

```bash
bidx migrate --database bitcoin
# → schema up to date (version N)   |   applied migrations: [...]
```

- Additive, `IF NOT EXISTS`-guarded DDL (a new index table, a new MV) needs no
  version bump — re-running `init-db` applies it.
- Anything an operator must *migrate* (a new column on a base table, a changed
  key, a dropped index, a changed engine) requires bumping `SCHEMA_VERSION` and
  adding an entry to `migrations()` in `crates/bidx-loader/src/schema.rs`. The
  mechanism is in place; see [contributing.md](contributing.md#changing-the-schema).

**Migration history**

| Version | What it does |
|---------|--------------|
| 2 | Rebuilds `address_stats` from `AggregatingMergeTree` to `SummingMergeTree` and recomputes its counters. The old engine silently dropped rows for addresses funded more than once, so balances were under-counted. |

An existing database at version 1 upgrades with a single `bidx migrate`; the
migration recomputes `address_stats` from the base tables, so it needs no
re-parse. A fresh `init-db` is created at the current version directly.

After upgrading, re-check the readiness endpoint and confirm `bidx_indexed_height`
matches before routing traffic.

## Troubleshooting

**`/readyz` returns 503 with `clickhouse: false`.**
The serve layer talks to ClickHouse over its **HTTP** port (`http_port`,
default `8123`), not the native `9000` port. A common misconfiguration is
pointing `http_port` at 9000. Also check the `clickhouse` service is healthy
and `BIDX_CLICKHOUSE__HOST` resolves from the `bidx` container.

**`/readyz` returns 503 with `index_has_tip: false`.**
ClickHouse is reachable but `blocks` is empty — run `bidx load`, or `init-db`
created an empty database. This is the intended behaviour: an empty index is
not ready to serve.

**`bidx load` finds no Parquet.**
The loader globs `--input` for the five entity directories. Confirm `bidx parse`
wrote `blocks/`, `transactions/`, `inputs/`, `outputs/`, `spends/` under it, and
that `--input` is the *out* dir, not a per-entity subdir. In Compose, remember
the loader reads the *container* path (`/data/out`), which is mounted read-only
into ClickHouse's `user_files`.

**`bidx parse` refuses to start with a disk-space projection error.**
The guard projected < 10 GiB free at the node tip. Free space, move the index to
a larger filesystem, or (deliberately) pass `--skip-disk-check`. The message
states how much additional space is needed.

**`missing utxos` is non-zero in a `bench` run.**
`apply_block` could not resolve a spent outpoint, so the UTXO store was cold and
the `utxo_apply` timing is meaningless. Point `--utxo` at a store built from the
same chain, or pass `--allow-missing-utxos` to accept it knowingly. Never quote
`utxo_apply` from a cold run.

**A live reorg deeper than `--max-reorg-depth`.**
The run aborts asking for a rescan. Re-run `parse` for the affected range, or
raise `--max-reorg-depth` if you accept the cost of a deeper walk.

**Bindgen fails with a missing `stdbool.h`.**
Run `./scripts/build-shim.sh` and point `BINDGEN_EXTRA_CLANG_ARGS` at the
`.build-shim/include` it writes (see [Building the
binary](#building-the-binary)), or build via the `Dockerfile`, which generates
the shim and sets the variable itself.
