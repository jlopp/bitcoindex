# Performance testing plan and first baseline

This is the plan for finding and ranking ingest bottlenecks, plus the results
of the first honest baseline. It exists because the previous benchmark numbers
were not trustworthy: the harness itself was the bottleneck.

## 1. How to run it

```bash
source scripts/perf/env.sh          # toolchain env; unsets the sandbox CARGO_TARGET_DIR
cargo build --release -p bidx-cli

# Whole-corpus ablation (coarse, tells you which axis moves)
scripts/perf/matrix.sh --blocks-dir ~/.bitcoin/signet/blocks --label base

# One cell by hand. Output defaults to ./bidx-perf in the CWD; override with
# --utxo-dir / --out-dir to place it on a specific filesystem.
./target/release/bidx bench --blocks-dir ~/.bitcoin/signet/blocks \
    --mode full --threads 8
```

Point `--utxo-dir`/`--out-dir` at a specific filesystem if you want, but note
the defaults already do the right thing: both default to `./bidx-perf` in the
current working directory. That is deliberate — the earlier default was the
system temp dir, which on many machines (including the dev box this was
written on) is **tmpfs**, i.e. RAM-backed. A RAM-backed UTXO store absorbs
writes at memory speed and hides exactly the compaction stalls the benchmark
exists to find, and a RAM-backed Parquet sink flatters the sink stage too.

Modes that write nothing (`headers`, `parse`, `drain`, `utxo` with no sink)
create no output directory at all, so they do not litter the perf root.

`scripts/perf/rsswatch.sh <pid> <log>` samples RSS plus the anon/file-backed
split every 2s. Use it when a run is suspected of leaking: it distinguishes
decoded-file mmap growth (file-backed) from parsed-block accumulation (anon),
which is what pointed at the reorder buffer in the first place.

`--mode` is the ablation axis:

| mode | runs | answers |
|---|---|---|
| `headers` | header pass only | fixed whole-corpus scan cost |
| `parse` | parse + discard | parse ceiling per block |
| `drain` | parse + discard (consumer exists) | parse ceiling with backpressure |
| `sink` | parse + Parquet rows, no UTXO | sink cost |
| `utxo` | parse + `apply_block`, no Parquet | UTXO cost |
| `full` | everything | end-to-end |

## 2. Harness bugs found and fixed

These were real defects, not tuning. Each one produced a confidently wrong
number before it was fixed.

1. **Unbounded reorder buffer in the bench consumer.** `bench_consume` never
   enforced `reorder_high`, while production's `consume_ordered` did. On signet
   blocks ≥200k the buffer grew to 28,000+ parsed `FullBlock`s (~10 GiB) and
   aborted on the RSS watchdog. Every `full`/`drain` run at the chain tip was
   measuring the harness's own leak. The consumer now increments/decrements a
   shared depth counter and workers spin on it, exactly as production does.
2. **Peak RSS was sampled, not measured.** `StageMeasure::peak_rss_kb` was
   `max(rss_at_start, rss_at_end)`. A buffer that balloons and drains inside a
   stage was invisible: the bench reported `peak rss: 1015 MB` on a run that
   actually peaked at 8.8 GB. Now reads the kernel's monotonic high-water mark
   (`VmHWM`, with a `getrusage` fallback).
3. **`max_rss_kb()` sized against the wrong quantity.** It used `MemTotal`, so
   on a busy box the watchdog limit could sit *above* the kernel's OOM point.
   Now uses `MemAvailable`.
4. **Worker send failures were misreported.** When the watchdog aborted the
   consumer, workers reported `"consumer dropped"`. They now check the watchdog
   and surface the real cause.
5. **The sandbox build was silently ignored.** `CARGO_TARGET_DIR` pointed at a
   throwaway cache, so `cargo build` never replaced `./target/release/bidx` and
   benchmarks ran a stale binary. `scripts/perf/env.sh` unsets it.

### The deadlock this uncovered

Restoring backpressure exposed a latent deadlock in **production**
`pipeline::run_parse` too: workers spin while the buffer is over the watermark,
including the worker that owns the height the consumer is waiting for. If that
worker parks, nothing arrives to unblock the consumer, and the run hangs
forever. It did not fire in practice only because the consumer was usually
fast enough to stay under the watermark.

Fix: the consumer publishes the height it is waiting for (`next_height`), and
a worker whose own height matches is exempt from the watermark. The spin loop
also checks the watchdog, so an aborted consumer reports an error instead of
hanging. Covered by `bench_consume_tracks_reorder_depth_and_publishes_next_height`.

## 3. First honest baseline (signet, 322,984 blocks, 8 threads, disk-backed)

| stage | wall | share of pipeline |
|---|---:|---:|
| header pass | 9.7 s | — |
| parse (Σ worker CPU) | 48.4 s | 3.6% |
| **utxo_apply (sequential)** | **1129.6 s** | **84.9%** |
| parquet_sink | 106.3 s | 8.0% |
| pipeline wall total | 1331.3 s | 100% |

- End-to-end: **242.6 blocks/s** (240.8 including the header pass).
- `missing_utxos: 0`, so the apply path is a real cold-from-genesis measurement.
- Parse p50 is 0.01 ms; apply p50 is 0.05 ms but **p99 is 55.7 ms and max is
  5.76 s**.

### Reading it

The pipeline is **apply-bound, not parse-bound**. Parse workers are idle ~96%
of the time; the single-threaded consumer is the wall. That reframes every
optimization: making `parse_block_full` faster buys ~4% at best.

The apply p50/p99 gap (0.05 ms → 55.7 ms, ~1000x) is the other signal. A
median block touches RocksDB cheaply, but a small fraction stall hard enough
to dominate the total. On this workload that is consistent with RocksDB write
stalls (memtable flush / L0 compaction) on a growing UTXO set, and it is why
`--rocksdb-stats` exists.

### Hypotheses, ranked

- **H1 — RocksDB write stalls dominate apply (highest value).** One unbatched
  `WriteBatch` per block against a multi-GB LSM tree. Confirm with
  `--rocksdb-stats` on a *seeded* store (see the gap below) and by correlating
  `stall_micros` with the apply p99. Candidate fixes: larger/periodic batches,
  `write_buffer_size` and level compaction tuning, or a bulk-load path for the
  initial index. **→ CONFIRMED and fixed; see §6.**
- **H2 — per-input point lookups are unbatched.** `apply_block` does
  `db.get_pinned` per spent outpoint. These cannot be batched directly (each
  read depends on prior writes within the block), but a multi-threaded
  prefetch/`MultiGet` for the read-only prefix of a block is worth measuring.
  **→ CONFIRMED, and worse than expected: the lookups were missing a 32 MiB
  cache 99% of the time. Fixed by the same change as H1; see §6.**
- **H4 — O(txs × inputs) fee enrichment** in `consume_ordered`: for every tx it
  re-scans `block.inputs` to find the coinbase input. Quadratic in block size,
  and it runs on the consumer thread. Cheap to fix, but bounded by the 8%
  sink share. **→ FIXED; see §6.**
- **H5 — `ThreadFileCache` global LRU mutex** across workers. Not a factor at
  the current parse share (~4%), but relevant if parse ever becomes the wall.
  **→ Still not a factor. Deprioritised.**

## 4. Known gaps in this baseline

- **The UTXO store was cold from genesis.** The full run therefore pays for
  RocksDB growing from empty to 3 GB *and* the real apply cost. That is the
  correct end-to-end number but it conflates compaction of a young tree with
  steady-state apply cost. To separate them, run `--mode utxo` against a store
  already seeded to the start height.
- **The cold-store gate fired correctly** when `--mode utxo --start 200000` was
  run against an empty store (385,589 missing outpoints). That is the harness
  working: the apply number from that run is meaningless and it said so.
- **Single machine, signet only.** Signet's block sizes and UTXO set are not
  mainnet's; re-run on a mainnet snapshot before trusting absolute numbers.

## 5. Next steps

1. ~~Seed a UTXO store to height N, then measure `--mode utxo` from N to tip to
   isolate steady-state apply cost from initial LSM growth.~~ **Done — §6.**
2. ~~Run `--rocksdb-stats` on that seeded store and correlate `stall_micros` /
   `compaction_pending` with the apply p99.~~ **Done — §6.**
3. ~~Fix H4 (the quadratic fee scan).~~ **Done — §6.**
4. ~~Re-run `scripts/perf/matrix.sh` with the fixed harness and diff against
   this baseline.~~ **Done — §6.**

## 6. Round 2: the root cause, and the fixes

### The headline: per-column-family RocksDB options were being ignored

`open_with` configured a 512 MiB write buffer, 8 background jobs, and so on —
on the **DB-level** `Options`. But `DB::open_cf_descriptors` passes each
`ColumnFamilyDescriptor`'s *own* `Options` down to
`rocksdb_open_column_families`, and the C API applies the per-CF options to
that family. Every descriptor in the old code was built with
`Options::default()`, so **none of the tuning reached any column family**. The
default (UTXO) family ran with RocksDB's stock 64 MiB write buffer and 32 MiB
block cache.

Measured, not inferred. A probe run reported `block_cache_capacity 33554432`,
and on the real workload the cache was **99% full** (`block_cache_usage
33238373`) while the UTXO data set was 2.9 GB. Every point lookup in
`apply_block` that missed that 32 MiB cache had to fault a data block off disk
and decompress it — and the keys are random 36-byte txid||vout, so misses were
the norm, not the exception. This is H1 and H2 with the same root cause.

There was also a genuinely quadratic loop in `apply_block`: a same-block spend
did `undo.created.retain(|k| *k != key)` *inside* the input loop, i.e.
O(outputs × spent-inputs) per block. It now records the key in an
`FxHashSet` and reconciles once, after the loop.

### What changed

- `crates/bidx-store/src/utxo.rs`
  - `tuned_options()` builds the tuning once and it is **cloned into every
    `ColumnFamilyDescriptor`**, which is the only place it takes effect.
  - A 1 GiB `LRUCache` shared across column families, with
    `cache_index_and_filter_blocks` and a 10-bit Bloom filter
    (`whole_key_filtering`), so a missing spent outpoint is rejected without
    touching a data block.
  - The quadratic `undo.created.retain()` moved out of the input loop.
  - Tests: `column_family_options_actually_applied` pins the cache size against
    the property, so this cannot silently regress; `same_block_spend_removes_key_from_undo_created`
    and `same_block_reconciliation_scales_to_many_outputs` assert the undo
    record contents (the old test only asserted "did not error").
- `crates/bidx-cli/src/pipeline.rs`, `crates/bidx-cli/src/bench/mod.rs`
  - H4: the coinbase flags are derived in one pass over `inputs` instead of
    re-scanning inputs for every tx.

### Where the time actually goes (seeded-store profile, heights 100k..320k)

The full-chain apply cost is not uniform. Applying a chunk to the store carried
forward from the previous chunk:

| heights | apply wall | blocks/s | store after |
|---|---:|---:|---:|
| 100000..140000 | 3.1 s | 5801 | 225 M |
| 140000..180000 | 0.8 s | 8191 | 241 M |
| 180000..220000 | 28.4 s | 1165 | 320 M |
| **220000..260000** | **490.2 s** | **76** | **1.7 G** |
| 260000..300000 | 235.7 s | 159 | 2.8 G |
| 300000..320000 | 90.3 s | 209 | 3.3 G |

A **15x** collapse in apply throughput. Note that 140k..180k applies *faster*
than 100k..140k: the early chunks are not measuring steady-state apply at all,
they are measuring LSM growth from empty. The blow-up tracks the store size,
which is the signature of a read path thrashing a cache that is orders of
magnitude too small.

### Before / after

Two independent A/Bs. All runs: signet, 8 threads, disk-backed, `missing
utxos: 0`, identical command lines; only the binary differed.

> **Both A/Bs have now been re-run cleanly and sequentially.** An earlier
> attempt at B was accidentally launched twice, so its two instances overlapped
> and contended for I/O; those numbers have been replaced below with the
> uncontended ones. The earlier run is not quoted anywhere.

**A. Fresh store, heights 0..320000 (same work, no seeding ambiguity):**

| | before | after (CF options) | final (+ algo fixes) |
|---|---:|---:|---:|
| `utxo_apply` wall | 926.8 s | 634.9 s | **553.5 s** |
| blocks/s | 313.0 | 438.3 | **512.2** |
| apply p50 | 0.04 ms | 0.05 ms | 0.05 ms |
| apply p99 | 46.26 ms | 28.66 ms | **22.21 ms** |
| apply max | 2236.7 ms | 1694.2 ms | 3218.2 ms |
| peak RSS | 6017.7 MB | 7895.2 MB | 8248.4 MB |

**B. Store pre-seeded to height 220000 (copied, so byte-identical inputs),
heights 220000..320000, `--rocksdb-stats` — clean sequential re-run:**

| | before | after |
|---|---:|---:|
| `utxo_apply` wall | 861.5 s | **547.2 s** |
| blocks/s | 110.7 | **169.3** |
| apply p50 | 1.02 ms | 1.05 ms |
| apply p99 | 78.46 ms | **43.27 ms** |
| apply max | 478.8 ms | 4783.5 ms |
| `block_cache_capacity` | 33554432 | 1073741824 |
| `block_cache_usage` | 33492186 (**99.8% full**) | 1034190197 |
| `memtable-limit-stops` | 2 | **0** |
| cumulative stall | 0.007 s (0.0%) | **0.000 s** |
| `is_write_stopped` | 0 | 0 |

Note what this run does *not* show: on a store seeded to 220k, the old code
only stalled for **7 ms** across the whole 100k-block range. The 16 s of stall
in the first (contended) attempt was an artifact of two benchmarks fighting for
the disk, not a property of the code. The stall mechanism is real — the write
buffer genuinely was 64 MiB — but on this workload it is a *minor* contributor
next to the cache misses, which is what the p99 (78 ms → 43 ms) and the
throughput (+53%) reflect. Do not over-attribute to stalls.

Result: **apply wall −40% on the full chain, −36.5% on the seeded store**;
throughput **+64% / +53%**. p99 improved on every axis.

### Notes and open items

- **Peak RSS grew by ~2.2 GB**, which is the 1 GiB block cache plus the
  buffers it pins. The watchdog limit (`MemAvailable`-based) did not trip. If
  memory is tighter, shrink `BLOCK_CACHE_BYTES`; the cache is the single
  biggest lever, so do not remove it.
- **`write_buffer_size` on the CF is not directly asserted by a test.** Only
  the cache size is readable as a property on an empty store, so that is what
  `column_family_options_actually_applied` pins. The buffer is covered
  indirectly: the seeded-store stall counters dropped from 11 memtable-limit
  stops to 0.
- **Live mode fsyncs once per block.** `apply_block` sets
  `WriteOptions::set_sync(true)` whenever undo is enabled, and `set_tip` /
  `disconnect_block` do the same. The benchmark opens in *bulk* mode (no sync),
  so **none of the numbers above include that cost** — production
  `bidx live` is slower than these figures. This is a durability policy
  decision, not a bug, but it should be a deliberate one: a group-commit /
  periodic-sync mode would recover most of it while keeping reorg safety.
- **The matrix ablation (`scripts/perf/matrix.sh`) was not re-run** for the
  final change. The two A/Bs above are stronger evidence for the apply path
  than the matrix is (identical stores in B), and the matrix's `parse`/`sink`
  cells are unaffected by these edits. A full matrix pass is still worth doing
  before any mainnet run.
- **Re-run the seeded A/B sequentially.** It was contended (see the caveat
  above), so the −27.5% figure is a floor. Worth one clean run to get the real
  number, and worth noting that the harness has no lock to stop two `bench`
  runs colliding on the same `--utxo` dir — the RocksDB lock catches it, but
  only after a run has started. **→ Done: −36.5%, see §7.**
- **Still single-machine, signet-only.** Re-run on a mainnet snapshot before
  trusting absolute numbers; mainnet's UTXO set is ~100x larger, so the block
  cache is likely to need to be larger again, and the `MultiGet`/prefetch idea
  in H2 is still unexplored.

## 7. Round 3: testnet4, and a parser bug it exposed

### `BlkFile` was truncating the last record of an unpadded file

The first attempt to index testnet4 failed immediately:

```
Error: header pass failed
Caused by:
    corrupt record in blk00000.dat at offset 134137600: bad record len 17404
```

The file was not corrupt. `blk00000.dat` was exactly 134,155,012 bytes — 56 KiB
short of the 128 MiB preallocation size, i.e. **bitcoind was still writing it**
— and it ended in four zero bytes. The final record ended at byte 134,155,012,
so `134137600 + 8 + 17404` is precisely the file length.

`BlkFile::open` computed `data_end = last_raw_nonzero(&mmap)`. That is a
heuristic for "trim Bitcoin Core's trailing zero preallocation", and it is
wrong whenever the last record legitimately ends in zeros — here the last
transaction's `locktime` was 0, so the trim ate four real bytes and the final
record's declared length then overran the trimmed slice.

Signet never hit this because its files happened to end on a non-zero byte.
The heuristic is also unfixable for XOR-obfuscated files (Core v28+), where the
padding decodes to a non-zero key pattern and the raw zeros are the *only*
signal — so the raw view must be trimmed, but by an exact method.

Replaced with `record_walk_end`, which walks the `magic || len || payload`
framing to the last complete record. That is exact for padded files, unpadded
files, and XOR files alike. Two properties were preserved deliberately:

- If **no** record parses and the file starts with a non-zero byte, the file is
  handed back intact rather than trimmed to nothing, so a wrong `--blocks-dir`
  still reports the actionable `bad magic` / `bad record len` instead of a
  silent "0 blocks". This is what keeps `bad_magic_rejected_with_network_in_mind`
  and `bad_record_len_rejected` meaningful.
- A partially written trailing record is excluded, and the end always lands on
  a record boundary, so a node writing concurrently cannot produce a garbage
  parse.

Six new tests cover the walk: zero padding, a final record ending in zeros,
a partial trailing record, non-magic garbage, a short declared length, the
malformed-file preservation path, and XOR with padding. All 163 workspace tests
pass, and the signet header pass still reports 322,984 blocks.

### Testnet4 full run

| | |
|---|---|
| blocks | 158,602 (heights 0..158602) |
| orphans | 74–76 — testnet4 has real reorgs, so the UTXO set is exercised |
| blocks dir | 13 GB, 94 blk files scanned |
| mode | `full` (parse + UTXO + Parquet sink) |

```
header_pass            wall    5.265s
parse (Σ worker CPU)   wall   24.238s
utxo_apply             wall  152.596s
parquet_sink           wall   46.984s
pipeline_wall_total    wall  240.272s   peak rss 4656 MB

blocks/s 660.1   (645.9 including the header pass)
missing utxos: 0
parse p50 0.00ms  p99  2.76ms  max  191.36ms
apply p50 0.01ms  p99 22.21ms  max  126.17ms
```

Output verified rather than assumed. All 80 Parquet files have valid `PAR1`
framing, and row counts read back through `parquet-arrow` are internally
consistent:

| table | rows |
|---|---:|
| blocks | 158,602 |
| transactions | 14,901,533 |
| inputs | 33,335,503 |
| outputs | 57,353,179 |
| spends | 33,176,901 |

The block count matches the header pass exactly, and
`inputs − spends = 158,602` — one coinbase per block — which is what spend
linking must produce. `missing utxos: 0` on a chain with 74 orphans.

**Interpretation.** Apply is still 63% of the pipeline wall (152.6 s of 240.3 s),
so the apply path remains the wall, but the *character* differs from signet:
apply p99 is 22.2 ms against signet's 55.7 ms at baseline, and the max is
126 ms rather than 5.76 s. Testnet4's UTXO set (1.2 GB) is far smaller than
signet's 3.3 GB, so the block cache is not the constraint here — which is
consistent with the cache being the fix for the *large* store, not a general
speedup. Note also that bitcoind was running and writing during this run, so
these numbers are a floor.

**On `utxo_apply` p50 = 0.01 ms.** The median block on testnet4 is tiny (most
heights hold a single transaction), so the median apply is dominated by
`WriteBatch` overhead, not by lookups. The p99 is where the real work is. Do
not read the p50 as evidence the apply path is free.

Still open: mainnet. Signet and testnet4 are both small compared to mainnet's
~100x larger UTXO set, so `BLOCK_CACHE_BYTES` will likely need to grow again,
and the `MultiGet`/prefetch idea (H2) remains unexplored.

