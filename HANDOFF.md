# bitcoindex — agent handoff

This repo builds a Rust Bitcoin blockchain indexer that ingests raw
`blk*.dat` files (Bitcoin Core ≥ v20 format, including the **v28+ XOR
obfuscation** used by recent Core releases), parses them in a
two-pass pipeline (header pass → full parse), maintains a RocksDB
UTXO set, and emits ClickHouse-ready Parquet batches. A benchmark
subcommand (`bidx bench`) measures end-to-end throughput for ingest,
UTXO maintenance, and Parquet sinking.

The primary purpose of this document is to capture the design
decisions, invariants, and operational rules that make `bidx` work
reliably on real hardware. Read this before editing.

## Code layout

```
crates/
  bidx-core/    # Network, Hash32, BlockHeader, BlockLocation, DiskRecorder, DiskGuard
  bidx-parser/  # BlkFile (blk*.dat + XOR de-obf), HeaderCollector,
                # ChainIndex, parse_block_full
  bidx-store/   # RocksDB UtxoStore (apply_block/disconnect_block with undo),
                # ParquetSink / SinkConfig
  bidx-live/    # ZMQ → RPC chain-reorg-tracker (separate from bulk)
  bidx-cli/     # bidx parse/inspect/bench/live; pipeline; DiskRecorder;
                # ThreadFileCache; DiskRecorder; OomWatchdog
```

Pipeline data flow:

```
blkNNNNN.dat ─► BlkFile (mmap + optional XOR decode)  
                  │
                  ▼
HeaderCollector (Pass 1: header-only scan, builds ChainIndex)
   │ ChainIndex::by_height[height] = BlockLocation { file_id, offset, record_len }
   ▼
ParseWorkers (rayon N threads)
   │ ┌─────────────────────────────────────────────────────────────────┐
   │ │ For each height h in [start..end):                              │
   │ │   ThreadFileCache::block_bytes(loc) (LRU-cached BlkFile)        │
   │ │   parse_block_full(raw) → FullBlock                             │
   │ │   tx.send(WorkItem::Block { h, block })                         │
   │ └─────────────────────────────────────────────────────────────────┘
   ▼ (crossbeam bounded channel, 512 slots)
Ordered consumer (single thread)
   - Reorders by height (BTreeMap + reorder_depth backpressure)
   - apply_block (RocksDB UtxoStore, ATOMIC per block)
   - enrich FullBlock with fee / total_fee
   - sink.push_* (ParquetSink, blocks/txs/inputs/outputs/spends)
   - rotate part files every `blocks_per_part`
   - DiskRecorder::on_block_applied (checkpoint every N blocks)
```

## Invariants and hot rules

These are non-negotiable. Violating any silently breaks correctness or
throughput.

### Read path

- **`blk*.dat` may be XOR-obfuscated.** Bitcoin Core v28 writes an
  8-byte key to `xor.dat` next to `blk*.dat`. If present, every byte of
  the blk file is XOR'd with this 8-byte key repeated. **Always** check
  for `xor.dat` via `BlkFile::open`, which handles both plain and
  obfuscated files and trims trailing preallocated zeros.
- **Don't `mmap.to_vec()` plain (non-obfuscated) files** — that's a
  fresh 128 MiB heap allocation per open. Use the mmap directly.
- **Trim trailing preallocated zeros** against the RAW (pre-decode)
  bytes via `last_raw_nonzero(&mmap)`. After XOR decode those zeros
  become the repeated key pattern and are unreadable.
- Cache `BlkFile` with a **bounded LRU** (16 entries × 128 MiB ≈ 2 GiB
  max). Signet blk files are XOR-obfuscated and cost 128 MiB per open;
  without an LRU, a 149-file corpus pins ~19 GiB.

### UTXO / index semantics

- All UTXO mutations use ONE `rocksdb::WriteBatch` per block. Undo
  records (see `BlockUndo`) live in the same batch.
- Update `BlockUndo::encode` and `decode` symmetrically.
- `UtxoStore` column families: `utxo`, `undo`, `meta`.
- Tip-tracking uses chain markers `chain_tip_height` and
  `chain_tip_hash` in the `meta` CF. `reorg_guard` compares
  `header.prev_hash` to the stored `chain_tip_hash` before applying.

### Worker parallelism

- **Never use unbounded channels** between workers and the ordered
  consumer. Crossbeam's bounded(512) is the contract.
- Workers check **shared `reorder_depth` counter** before each send.
  If it exceeds `REORDER_HIGH` (32), workers sleep until the ordered
  consumer drains below that watermark. This caps the reorder
  `BTreeMap` at ~32 parsed `FullBlock`s (~10 MB each).
- One shared **`ThreadFileCache`** LRU of decoded `BlkFile`s
  cross-worker. `MAX_CACHED_FILES = 16` × 128 MiB ≈ 2 GiB cap.
  (Signet blk files are XOR-obfuscated and cost 128 MiB per open.)

## Decode semantics (blk*.dat + XOR)

Bitcoin Core v28+ may write blk*.dat **XOR-obfuscated** with an 8-byte
key in `xor.dat` (same dir). Signet testnet uses this — every blk
file is 128 MiB of XOR'd data. Decoding rules:

- At `BlkFile::open`, read `xor.dat`. If present and non-zero, decode
  the entire payload once into an owned `Vec<u8>`.
- **Trim trailing zero padding against the RAW bytes** (pre-decode)
  via `last_raw_nonzero(&mmap)`. After decode those zeros become the
  repeated key pattern and look like valid records.
- Plain files: use the mmap directly (no heap copy). `BlkFile::data`
  returns a `&[u8]` regardless of which case.

Memory model:

- **Plain blk file** (mainnet/testnet3): `data()` returns a borrow of
  the mmap; heap cost ~0.
- **XOR-obfuscated** (signet, sometimes mainnet on v28+): full
  128 MiB decoded Vec at open. With 12-worker LRU at `MAX_CACHED_FILES
  = 16`, max decoded memory is 16 × 128 MiB ≈ 2 GiB.

## Memory / throughput levers

| Lever | Default | Effect |
| --- | --- | --- |
| `ParseConfig.threads` | `None` (auto) | Worker count. Higher = faster, decodes more files concurrently. |
| `blocks_per_part` | 10 000 | Parquet part rotation. Lower = more files, faster re-runs. |
| `blocks_per_part` → part N | 10 000 | Forward-only parquet part rotation. |
| `REORDER_HIGH` | 32 | Reorder-buffer watermark; max FullBlocks held in consumer's `BTreeMap`. |
| `MAX_CACHED_FILES` | 16 | LRU cap on decoded blk files (16 × 128 MiB ≈ 2 GiB max). |
| `OomWatchdog` | derives from `/proc/meminfo` | Bails with stderr "watchdog: RSS exceeded…" if memory explodes. |
| Channel depth | 512 | Max in-flight parsed blocks between workers and consumer. |
| `MAX_CACHED_FILES` | 16 | LRU cap on decoded blk files (≈2 GiB max). |

## CLI cheat sheet

```bash
# Bulk parse: parse whole chain from blk files, emit Parquet and maintain UTXO.
bidx parse \
  --blocks-dir ~/.bitcoin/blocks \
  --out /data/bidx/out \
  --utxo /data/bidx/utxo \
  --rpc-url http://127.0.0.1:8432 \
  --blocks-per-part 10000

# Bench: same pipeline but no writes to a destination directory.
bidx bench \
  --blocks-dir ~/.bitcoin/signet/blocks \
  --mode full \
  --threads 12 \
  --reps 1 \
  --utxo /tmp/bidx-signet-utxo

# Inspect a single block at a height.
bidx inspect --blocks-dir ~/.bitcoin/signet/blocks 5000
```

## Running the benchmark on signet

Signet's blk files are **XOR-obfuscated**. `bidx` handles this; it will
pin ~2 GiB of heap (16-file LRU × 128 MiB) plus whatever RocksDB's
block cache decides to pin.

```bash
bidx bench \
  --blocks-dir ~/.bitcoin/signet/blocks \
  --mode full \
  --threads 12 \
  --reps 1 \
  --utxo /tmp/bidx-signet-utxo
```

Expected peak RSS ≈ 14–20 GB (12 threads × XOR decode + RocksDB
write buffers + Parquet row-groups). If the machine has <24 GB
RAM, run with `--threads 8` to shrink concurrent decodes.

## Memory model reference

| Cost | Source | Bound |
| --- | --- | --- |
| 128 MiB per decoded blk file (XOR-obfuscated signet) | `BlkFile::open` decode | `MAX_CACHED_FILES × 128 MiB` ≈ 2 GiB |
| Per in-flight `FullBlock` (parsed rows for one block) | Worker → consumer send | `(channel_depth + REORDER_HIGH) × ~10 MB` ≈ ~5 GB |
| RocksDB UTXO write buffers | `UtxoStore` opendb default | ~4 GiB under load |
| Parquet row-group building | `ParquetSink::push_*` | `batch_size` rows × schema ≈ 500 MB |
| `ChainIndex` (by_height vec) | header pass | `tip_height × 48 B` |

Peak RSS cap (12 workers, signet XOR files) ≈ **20 GiB**. Set
`--threads 8` for machines with <24 GiB RAM.

## Operational rules (preserved verbatim)

- Never open two `UtxoStore` instances to the same path.
- Never write UTXO change and undo separately; always one
  `rocksdb::WriteBatch`.
- Never hold a `Mutex` across an `.await`. `parking_lot::Mutex` is
  used only in the ThreadFileCache LRU, guarded by short contained
  critical sections.
- Every change to `bidx-parser`, `bidx-store`, `pipeline` must run
  `bidx bench` before/after. If throughput shifts by >2%, record
  before/after in the commit.

## Common pitfalls (seen in production)

- **Signet blk files are XOR-obfuscated** — you must check for
  `xor.dat`. `BlkFile::open` handles it; do not bypass by re-reading
  `blk*.dat` directly.
- **Trim trailing zeros against RAW bytes** when XOR-obfuscated. After
  decode those zeros become the repeated key pattern and parse as
  valid records if you don't trim.
- **Cache decoded `BlkFile`s with a bounded LRU.** Signet blk files
  are 128 MiB each after XOR decode; 149 files × 128 MiB = 19 GiB
  without eviction. The `ThreadFileCache` LRU caps at 16.
- **Never share `UtxoStore` across processes**; RocksDB pins locks.
- **Never write UTXO and undo changes separately** — one
  `rocksdb::WriteBatch` per block.

## Bench reference (signet, 12 threads, 2026-09-21)

```
bench trial 0 (full):
  header_pass     31.6 s wall (4% CPU, 97 MB peak)
  parse          728.5 s wall (concurrent with consumer)
  utxo_apply     868.4 s wall
  parquet_sink   132.2 s wall
pipeline_wall_total  1063.7 s (303.7 blocks/s)
peak RSS           20 135 MB
parsed           322 984 blocks
missing utxos    0
orphans skipped  0
```

If the machine has < 24 GiB RAM: reduce `--threads` to 8. If
< 16 GiB available: `--threads 4` and `MAX_CACHED_FILES = 8`.
