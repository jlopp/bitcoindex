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

ClickHouse additionally gets twelve query-optimized derived tables
(`transactions_by_txid`, `outputs_by_address`, `spends_by_spent_outpoint`,
`address_history`, `utxos_by_address`, `address_stats`, and more) plus four
projections and three skip indexes on the base tables. See
[`doc/schema.md`](doc/schema.md) for the full catalogue and
[`doc/data-model-rationale.md`](doc/data-model-rationale.md) for why each
exists.

## Documentation

Full documentation lives in [`doc/`](doc/). Start with
[`doc/README.md`](doc/README.md) for the reading order.

| Document | Covers |
|----------|--------|
| [doc/schema.md](doc/schema.md) | Every ClickHouse table and field a user can query, plus indexes, MVs, and enumerations. |
| [doc/query-cookbook.md](doc/query-cookbook.md) | Practical SQL recipes: blocks, txs, spends, addresses, UTXO set, miners, joins. |
| [doc/cli.md](doc/cli.md) | Every `bidx` subcommand and flag, with typical workflows. |
| [doc/architecture.md](doc/architecture.md) | How the pipeline works end to end, stage by stage. |
| [doc/serving-api.md](doc/serving-api.md) | The Esplora REST, Electrum JSON-RPC, and ops surfaces. |
| [doc/cloud-deployment.md](doc/cloud-deployment.md) | Kubernetes quickstart, signed images, scaling the query tier, API keys, caching, load testing. |
| [doc/deployment-ops.md](doc/deployment-ops.md) | Building, deploying, and operating the stack. |
| [doc/data-model-rationale.md](doc/data-model-rationale.md) | Why the schema is shaped the way it is. |
| [doc/contributing.md](doc/contributing.md) | Dev setup, tests, coverage, and the PR flow. |

The authoritative sources are the Rust code, not the docs; see
[doc/README.md](doc/README.md#keeping-these-documents-honest) for the mapping.

## Install

One command on Linux (x86_64 or aarch64):

```bash
curl -fsSL https://raw.githubusercontent.com/jlopp/bitcoindex/master/scripts/install.sh | sh
```

Every install channel — the script, the container image, `.deb`/`.rpm`/`.apk`
packages, Homebrew, `cargo install`, Nix, and building from source — is
documented in **[doc/INSTALL.md](doc/INSTALL.md)**. Preview what the installer
would do without changing anything:

```bash
curl -fsSL https://raw.githubusercontent.com/jlopp/bitcoindex/master/scripts/install.sh | sh -s -- --dry-run
```

The installer picks the right prebuilt release for your machine, verifies it
against the release `SHA256SUMS`, and installs the two runtime dependencies the
CLI needs — `libzmq5` (for `bidx live`) and `clickhouse-client` (for
`init-db` / `load` / `migrate` / `verify` / `index rebuild`). Dependency
installation is best-effort: if it fails the binary is still installed and the
manual command is printed. It installs to `/usr/local/bin` when writable
(using `sudo` if available) and falls back to `~/.local/bin` otherwise.

```bash
# Pin a version, or install elsewhere:
curl -fsSL https://raw.githubusercontent.com/jlopp/bitcoindex/master/scripts/install.sh \
  | sh -s -- --version v0.1.0 --bindir ~/.local/bin

# Skip the dependency step (e.g. you manage packages yourself):
curl -fsSL https://raw.githubusercontent.com/jlopp/bitcoindex/master/scripts/install.sh \
  | sh -s -- --no-deps

# No prebuilt for your distro/libc? Build from source instead:
curl -fsSL https://raw.githubusercontent.com/jlopp/bitcoindex/master/scripts/install.sh \
  | sh -s -- --from-source
```

Other channels, in brief (full details in [doc/INSTALL.md](doc/INSTALL.md)):

```bash
docker run --rm ghcr.io/jlopp/bitcoindex:latest --help   # container
brew install jlopp/tap/bitcoindex                        # Homebrew
cargo install --git https://github.com/jlopp/bitcoindex --locked --bin bidx
nix profile install github:jlopp/bitcoindex              # Nix
```

Run `sh scripts/install.sh --help` for every flag. Prebuilt tarballs for
`x86_64-unknown-linux-{gnu,musl}` and `aarch64-unknown-linux-gnu`, plus
`.deb`/`.rpm`/`.apk` packages, a CycloneDX SBOM, and `SHA256SUMS`, are attached
to each `v*` release.

## Build

Requires a C toolchain and clang headers for RocksDB's bindgen step. Source the
dev environment helper, which sets the required workarounds and locates
libclang for you:

```bash
source scripts/dev-env.sh
cargo build --release
```

By hand, that is (`.build-shim/include` holds gcc's copies of the C standard
headers):

```bash
./scripts/build-shim.sh                              # writes .build-shim/include
export LIBCLANG_PATH=/usr/lib/x86_64-linux-gnu       # wherever libclang lives
export BINDGEN_EXTRA_CLANG_ARGS="-I$PWD/.build-shim/include"
export CXXFLAGS="-include cstdint"
cargo build --release
```

`.build-shim/` is machine-specific and gitignored — the `Dockerfile` and the
release workflow run the same script to generate it, so it is never committed
and never copied from the build context. The `LIBCLANG_PATH` above is the
Debian/Ubuntu multiarch directory, not `/usr/lib/llvm-<version>/lib`; use
`ldconfig -p | grep libclang` to find yours.

## Testing & coverage

```bash
cargo test --release --workspace     # the whole suite
```

**Policy: every Rust file a change touches must be at 100% line coverage.**
This is enforced, not advisory — see `.cursor/rules/test-coverage.mdc` for the
full rule and the reasoning behind it.

Measure coverage with [cargo-tarpaulin](https://github.com/xd009642/tarpaulin)
(config in `.tarpaulin.toml`; `--engine llvm` is required because the ptrace
engine mis-handles the subprocesses the loader tests spawn):

```bash
cargo tarpaulin --workspace --out Json --output-dir target/coverage

# Fail if any file changed against origin/master is below 100%:
scripts/coverage/check-changed.sh target/coverage/tarpaulin-report.json origin/master
```

The gate is **per changed file**, not workspace-wide: the long-running
`bidx-live` and pipeline paths that need a live bitcoind/ZMQ are not fully
covered, and holding every PR to a workspace-wide threshold would either block
all work or invite blanket exclusions. `.github/workflows/coverage.yml` runs the
same check on every pull request, diffing against the PR's base branch.

Current workspace total is ~96.7% (4089/4230 lines). Every file under
`bidx-serve`, `bidx-loader`, and the `bidx-core` primitives is at 100%, and
every file changed since the coverage gate was added is at 100% or carries a
documented exemption.

### Documented exemptions

`.cursor/rules/test-coverage.mdc` permits a line to go untested when a comment
explains *why* it is unreachable. `check-changed.sh` honours that: a comment
containing `coverage-exempt` exempts the item it heads — a single statement, or,
for a function/loop/closure, the whole brace block — from the 100% requirement.
Use the explicit form `// coverage-exempt: 12-14, 20` when the untestable code
spans lines the heuristic cannot bracket. Every exemption must still be justified
in the comment; the marker is not a way to opt out of testing reachable code.

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

### Disk-space checkpoints and startup guard

Because loading the full mainnet chain can require more space than the disk
has available, `bidx parse` and `bidx live` keep a per-network history of
index size vs height, and refuse to start when the projected final size
would leave less than 10 GiB free on the index filesystem.

- The checkpoint history lives in
  `<parent-of-utxo>/bidx-disk-checkpoints.txt` (override with
  `--checkpoint-file`). One `network <name>` section per chain; lines are
  `height total_bytes`.
- **Recording**: every `CHECKPOINT_HEIGHT_STEP` blocks (100,000 by default)
  the pipeline / live tracker measures the on-disk size of the UTXO db +
  Parquet out dirs (RocksDB directory size, including WAL/SSTs) and appends
  `(height, total_bytes)` to the checkpoint file.
- **Projection**: at startup, `bidx parse --rpc-url <url>` and `bidx live`
  run a pre-flight check: they read the local indexed tip (the highest
  recorded checkpoint), fetch the node's tip via `getblockcount`, fit a
  linear regression through the checkpoint history to get growth in
  bytes/block, and project the index's total size at the node tip. If the
  projected size would leave less than 10 GiB free on the index filesystem,
  the program refuses to start and prints how much additional free space is
  needed.
- `bidx parse` without `--rpc-url` logs a notice and skips the projection
  (we have no chain tip to compare against). Pass `--rpc-url <url>` (plus
  `--rpc-cookie`) to enable the guard on bulk loads too. `bidx live` always
  runs the guard since it already has a node connection. Use
  `--skip-disk-check` to bypass entirely.
- With no checkpoints yet (first mainnet run at height <100,000), the
  projection is skipped with a warning — there isn't enough history to
  extrapolate from.

### Resumability

Parquet part files are named by block range (`out/blocks/part-00042.parquet`
etc.), and the UTXO set is durable in RocksDB. Re-running `parse` with
`--start`/`--end` reprocesses a range without redoing the whole chain.
Loading is per-entity via Parquet globs, so a failed load retries cleanly.

## Querying

See [`doc/schema.md`](doc/schema.md) for the full table/field reference and
[`doc/query-cookbook.md`](doc/query-cookbook.md) for ready-to-run recipes.
The two rules that explain most surprises: hashes are raw wire-order bytes
(reverse for display), and addresses are `(script_type, address_hash)` pairs.

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
- [x] Sink & UTXO overlap: sink I/O (Parquet encode + file write) runs on a dedicated sink thread, overlapped with the consumer's RocksDB commit via a bounded channel
- [x] Retraction of live-Parquet rows for disconnected blocks (open-part rows are dropped exactly from the in-memory window; a deeper reorg records a `.live-reorg-floor` sidecar for the loader)
- [x] Direct ClickHouse live inserts (bypass Parquet) for sub-minute tip freshness (`bidx live --ch-live`, streaming `RowBinary` over one open `clickhouse-client` pipe per table)
- [x] Optional witness payload table (opt-in `--capture-witness`, kept out of the hot tables)
- [x] Base58/bech32 rendering helpers (executable UDF crate `bidx-udf`, ClickHouse SQL view, and a `bidx index` helper)

## License

MIT
