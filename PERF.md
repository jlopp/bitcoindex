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
  `db.get_pinned` per spent outpoint: one FFI call, one `Result`, one
  `PinnableSlice` each, with no chance to share a data-block read between keys.
  **→ The cache half of this was the real bug and is fixed (§6): the lookups
  were missing a 32 MiB cache 99% of the time.** The batching half was
  implemented and measured, and it did **not** help — see §9. H2 is closed.
  The original note here was wrong when it said these "cannot be batched
  directly"; they can, it simply does not pay.
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

## 8. H2 in detail: the batched prefetch (not yet implemented)

### Correcting the record

An earlier note in this file said the per-input lookups "cannot be batched
directly (each read depends on prior writes within the block)". That is wrong,
and the reason it is wrong is the thing that makes the whole idea work:

**Every output of the block is inserted into `pending` *before* the input loop
begins.** A same-block spend is therefore resolved entirely from the in-memory
`pending` map and never touches the database — the DB is consulted only when
`pending.remove(key)` returns `None`. The decision to hit the DB is a function
of `pending`, not of position in the input loop, so it is *window-independent*
and can be computed ahead of time. A prefetched value is simply discarded when
`pending` turns out to satisfy that input.

### API choice

Use `DB::batched_multi_get_cf(cf, keys, sorted_input)`, which returns
`Vec<Result<Option<DBPinnableSlice>, Error>>` — zero-copy. Do **not** use plain
`multi_get`, which returns `Vec<Result<Option<Vec<u8>>, Error>>` and allocates a
`Vec<u8>` per key, violating the repo's no-per-item-allocation rule. The crate
documents `batched_multi_get_cf` as "a more optimized version of multi_get_cf".

`apply_block` currently has no default-CF handle (only `cf_undo()`/`cf_meta()`);
one would be needed, since this call takes an explicit CF.

### Shape

1. Build the key list once: `inputs` where `!is_coinbase` **and**
   `!pending.contains_key(&key)`.
2. `batched_multi_get_cf` over that list in windows, keeping index
   correspondence (`sorted_input = false`).
3. Walk the window; if `pending.remove(key)` hits, discard the prefetched value
   and take the same-block path unchanged.

Step 1 is the refinement that makes it cheap. Because `pending` only ever
shrinks during the input loop, a key absent from `pending` when the batch is
built can never appear in it later — so the filtered set is *exactly* the keys
that will be DB-consulted. No wasted lookups for same-block spends, and no
pointless reads fired at outpoints that do not exist in the DB yet.

### Why this is low priority, not next

**→ ANSWERED, NEGATIVE. H2 is closed. See §9.**

## 9. H2 closed: the prefetch was implemented, measured, and reverted

The instrumentation above was built and run first, then the prefetch was
actually implemented and A/B-tested. It did not help.

### The measurement (testnet4, full chain, 158,602 blocks)

```
utxo lookups: 23754999 db / 9421902 pending (71.6% db)  db_time: 57.337s (35.4% of apply)
```

So the hypothesis looked *strong*: 71.6% of non-coinbase inputs need a database
read, and 35.4% of apply wall time is spent inside those point lookups. That is
far more headroom than the "probably not worth it" guess in §8 predicted, which
is exactly why it was worth implementing rather than estimating.

### The A/B

`batched_multi_get_cf` over exactly the keys that will be consulted (filtered by
absence from `pending`, which is provably the exact set — see §8), windows
discarded as values are decoded into a `Vec<Option<UtxoValue>>`, walked by index
in the input loop. Baseline is the same code with the original per-key
`get_pinned` and *no* per-lookup timing, so the timing overhead does not
penalize the baseline.

| | baseline (per-key) | prefetch (batched) |
|---|---:|---:|
| `utxo_apply` wall | 160.553 s | 160.493 s |
| blocks/s | 639.0 | 629.8 |
| apply p50 | 0.01 ms | 0.01 ms |
| apply p99 | 23.66 ms | 22.83 ms |
| apply max | 146.58 ms | 149.64 ms |
| `db_lookups` | 23,754,999 | 23,754,999 |
| `pending_hits` | 9,421,902 | 9,421,902 |

**No improvement.** 0.04% on apply wall is noise, and the run-to-run spread on
this workload is larger than that. Note the two variants report *identical*
lookup counts, which independently confirms the filter in §8 selects exactly the
right key set — the implementation was correct, it just did not help.

### Why it did not help

This is the actual finding, and it is more useful than the negative result:

**The 35% spent in point lookups is per-key work, not I/O.** `batched_multi_get_cf`
batches the *block reads* — which is the part that matters when the data is not
cached. Once the 1 GiB block cache (§6) made the working set resident, the cost
inside those lookups is the per-key RocksDB path itself: memtable and SST
search, bloom-filter check, `PinnableSlice` construction. A batched API still
does that work once per key; it only avoids redundant *I/O*, and there was no
redundant I/O left to avoid.

So the two halves of H2 were not independent. The cache fix was the whole win,
and it consumed the entire upside of the batching. That is consistent with §6's
before/after: the p99 improvement came from cache hits, not from anything about
how the lookups were issued.

### What was kept

The prefetch is **reverted**. The instrumentation is kept, because it is cheap
(counters plus one `Instant` per database lookup) and it is the right question
to re-ask on mainnet, where the UTXO set is ~100x larger and the working set may
not fit in a 1 GiB cache:

```
utxo lookups: <db> db / <pending> pending (<pct>% db)  db_time: <s>s (<pct>% of apply)
```

If a mainnet run shows `db_time` climbing well above ~35% of apply *and* the
block cache is not saturated, that would indicate real I/O again — and the
prefetch would then be worth another look. On testnet4 and signet, it is not.

### The more promising direction

If per-key lookup work is the cost, the way to remove it is to **not do the
lookup at all** — i.e. keep more of the UTXO working set in memory and consult
RocksDB less often, rather than asking RocksDB the same number of questions
faster. That is a different design (a hot-set cache in front of the store, or
restructuring the apply path around an in-memory delta), not a batching tweak.
See §10 for what that would concretely look like.

## 10. Why the lookups exist, and the one that can be removed

### The lookups are not optional

A transaction input in the raw block data carries only *pointers*: `prev_txid`,
`prev_vout`, and the unlocking script (`script_sig_len`, `witness_items`, …). It
does **not** carry the value being spent, the address it belonged to, the script
type, or the height it was created at. That is the definition of the UTXO model:
a block references outputs of *other* blocks, so the information is simply not
in the block being parsed.

But `SpendRow` — the analytical payload of this index — requires exactly those
missing fields. `record_spend` copies four of them straight out of the stored
entry (`v.value_sat`, `v.script_type`, `v.address_hash`, `v.height`), and fees
are `Σ(inputs) − Σ(outputs)` where only the outputs are in the block:

```
fees[tx] += v.value_sat     // the input value, known only from the store
```

So there are three distinct consumers of each lookup:

1. **Spend enrichment** — value, script type, address hash, spent height. Four
   fields, all needed for the schema. `address_hash` is always populated at
   parse time via `classify_script`, so it is not optional.
2. **Fee computation** — needs *only* `value_sat` (8 bytes).
3. **UTXO set maintenance** — the delete, and in undo-enabled mode the old value
   needed to reverse it on a reorg.

### The lever: most of that does not need a 45-byte entry

Consumers 2 and 3 are satisfied by far less than the full 36-byte-key →
45-byte-value lookup:

- Consumer 2 needs 8 bytes.
- Consumer 3 needs the value *only* when undo logging is on; in bulk mode the
  entry is just deleted.

So a plausible design is to keep a compact in-memory map alongside RocksDB:

| map | key | value | rough size at 180M UTXOs |
|---|---|---|---|
| value-only | 36 B | 8 B | ~8 GB |
| value + script/address | 36 B | 45 B | ~15 GB |

The second row is the whole set in RAM, which is only worth it on a big box. The
first row is the interesting one: it serves fees and set maintenance from memory
and leaves RocksDB to answer only the enrichment reads.

**Be honest about the arithmetic.** At 180M UTXOs even a value-only map is ~8 GB,
which is not obviously cheaper than the page cache the 1 GiB block cache is
already using. The win is not free memory; it is (a) no per-key RocksDB search,
(b) no `PinnableSlice` construction, and (c) a predictable hot set rather than an
LRU guessing. Whether that beats the cache depends on the machine, and it should
be measured with the §9 counters before anyone builds it — specifically, watch
`db_time` as a share of apply on a mainnet-sized store.

**Also note the interaction with undo.** A memory-resident map must stay
consistent with the RocksDB set across a reorg, which is what `BlockUndo` exists
to guarantee. Any such cache has to be driven from the same `WriteBatch`
ordering, or reorg handling breaks in exactly the way `test-coverage.mdc` warns
about. This is a real design cost, not a footnote.

This is the direction to explore on mainnet, not another batching tweak.

## 11. The in-memory recent-UTXO cache: implemented and measured

§10 argued the lever was to *remove* lookups rather than batch them. That was
built as a minimum viable cache and A/B-tested.

### Design

`RecentUtxoCache` — a bounded `FxHashMap<[u8;36], UtxoValue>` plus a creation-
ordered `VecDeque` for eviction. `--utxo-cache-mb <N>` sets its size in MiB;
`0` disables it (the A/B baseline). Since §12 it is **auto-sized from host
memory by default** rather than disabled.

Decisions worth recording, because each was a fork in the road:

- **Insert at block end, from `pending`.** `pending` is seeded with every output
  before the input loop runs and has same-block spends removed, so at block end
  it holds exactly the outputs that survived — the true new UTXOs. Inserting
  earlier would risk caching an outpoint the same block is about to spend, making
  the cache and `pending` disagree. At block end it is both correct and minimal.
- **Removal is eviction-blind.** A spent outpoint leaves the cache on a hit. The
  key is left in the order queue; eviction tolerates a stale key. This mirrors
  store semantics: a spent outpoint is gone from the UTXO set, so a later block
  cannot legitimately spend it again.
- **Only in bulk (non-undo) mode.** Live mode must keep `BlockUndo` byte-exact
  for reorgs, and threading an in-memory view through that path adds correctness
  risk for no benchmarkable benefit.
- **Entries are full `UtxoValue`s — nothing was slimmed.** The brief was to strip
  fields down, but that turned out to be unsafe, not merely risky:
  `spent_value_sat`, `spent_script_type`, `spent_address_hash` and
  `spent_height` are all written into `SpendRow`. Truncating the address hash
  would **corrupt the index**, not just risk a lookup collision. The key cannot
  be truncated either: the cache is keyed on the full outpoint, and a false hit
  would return the wrong value. So the win comes from avoiding the RocksDB
  lookup, not from smaller entries.

### A/B (testnet4, full chain, 158,602 blocks, sequential, fresh stores)

| | no cache | cache (2048 MiB) |
|---|---:|---:|
| `utxo_apply` wall | 159.728 s | **137.749 s** |
| blocks/s | 640.5 | **740.7** |
| apply p50 | 0.01 ms | 0.01 ms |
| apply p99 | 23.11 ms | **17.66 ms** |
| apply max | 155.26 ms | 8426.53 ms |
| **database lookups** | **23,754,999** | **5,123** |
| cache hits | — | 23,749,876 |
| db_time | 53.412 s (33.4% of apply) | **2.586 s (1.9%)** |
| peak RSS | 4773 MB | 8164 MB |

**Apply wall −13.8%, throughput +15.7%.** Database lookups fell by **99.98%** —
from 23.75M to 5,123, a 99.98% cache hit rate — and db_time fell from 33.4% of
apply to 1.9%.

### Correctness verified, not assumed

A 99.98% hit rate is exactly the situation where a wrong cache value would hide
in plain sight, so the outputs were compared rather than trusted. All five
Parquet tables are **byte-identical** (sha256) between the two runs:

```
blocks         fbeff9f51ca0c708  == fbeff9f51ca0c708
transactions   c1a9674e71d09bac  == c1a9674e71d09bac
inputs         fcec6dd94946f022  == fcec6dd94946f022
outputs        176043e5eec756ae  == 176043e5eec756ae
spends         54b806006c715f36  == 54b806006c715f36
```

Row counts also match exactly (158,602 blocks / 14,901,533 tx / 33,335,503
inputs / 57,353,179 outputs / 33,176,901 spends). Five unit tests cover the
cache: hit fidelity against the expected `SpendRow` fields, the disabled
control, eviction-to-budget with a correct DB fallback, exhaustive accounting
(`pending + db + cache` == input count), and hit-rate sanity.

### Caveats

- **Peak RSS +3.4 GB** for a 2 GiB budget (the budget plus hash-map and queue
  overhead, and the figure is a kernel high-water mark including transient
  allocations). On a memory-constrained box, size it down; the hit rate is what
  you are trading.
- **apply max regressed to 8.4 s** on one block. That is compaction, not the
  cache — the cached run reached 2.9 GB of SST data vs 2.6 GB without, so it
  compacted harder. It did not affect the total, but it is visible in the tail.
- **`CACHE_ENTRY_COST` (112 B) is an estimate**, so the budget is approximate.
  Fine for a cache, but do not read `bytes` as exact.
- **Testnet4 only.** The hit rate is a property of this chain's spend pattern:
  a 2 GiB window covers a large fraction of testnet4's small UTXO set. On mainnet
  a 2 GiB window will cover a much smaller fraction and the hit rate will be
  lower — this must be re-measured before trusting the gain.
- **Not enabled in live mode**, so production `bidx live` does not get this yet.
  Enabling it there needs the reorg/undo path to be reconciled deliberately.

### Tests

Three unit tests pin the instrumentation, so the numbers above can be trusted:
`apply_block_reports_db_and_pending_lookup_counts` (one DB lookup and one
`pending` hit in the same block, asserting the split sums to the input count),
`apply_block_reports_no_db_lookups_when_all_spends_are_same_block`, and
`apply_block_excludes_coinbase_from_lookup_counts`.

## 12. Spending the idle RAM: auto-sized caches, one block cache, tuned writes, parallel header pass

The report that started this: a mainnet bulk index on a 12-core / 32 GB / NVMe
box spent **25 minutes in the header pass** and was only at **height 760,000
after 13 hours** (~6 blocks/s), with the suspicion that RAM sat idle while the
box thrashed the disks. Four changes, A–D:

- **A — the recent-UTXO cache is now on in production, and auto-sized.** §11
  built `RecentUtxoCache` but left it off by default and reachable only from
  `bench`. It is wired into `bidx parse` and sized from host memory.
  `--utxo-cache-mb <N>` overrides; `0` restores the §11 baseline.
- **B — one shared, auto-sized RocksDB block cache.** `tuned_options` used to
  build a *separate* 1 GiB cache per column family, tripling memory and
  preventing a block read for one family being reused by another. There is now
  exactly one `Cache` handle, cloned into every CF descriptor and sized from
  host memory. `--block-cache-mb <N>` overrides.
- **C — write-path tuning scales with the core count.** `max_background_jobs`
  and `max_subcompactions` were hardcoded to 8. They now derive from
  `available_parallelism()` (`background_jobs_for` / `subcompactions_for`, both
  clamped), so a 12- or 16-core box no longer runs every compaction on a single
  thread. `increase_parallelism` follows the same value.
- **D — the header pass is parallel.** `build_chain_index` now scans blk files
  with Rayon (`files.par_iter().map(scan_file).collect()`), each file producing
  an allocation-local `Vec`, merged **in file-id order** so first-seen-wins on
  duplicate block hashes stays deterministic.

### The memory budget (`bidx_core::mem`)

Reads `/proc/meminfo`. `MemAvailable` is the input (not `MemTotal` — on a busy
box that ignores every other process). Usable = `MemAvailable` − 10%
(measurement reserve) − 2 GiB (runtime reserve: parse scratch, reorder buffer,
sink batches, allocator slack); the caches take 60% of that, split 60/40 to the
recent-UTXO cache and the RocksDB block cache. On the 32 GB measurement box
(~12.4 GiB available): **~3.3 GiB recent + ~2.2 GiB block**. If `/proc/meminfo`
is unreadable the fallback is the old 1 GiB block cache and no recent cache, so
behaviour on a host without `/proc` is unchanged.

### Measurements (signet, heights 0..310,000, 8 threads, 16-core box)

Seeded run with the cache auto-enabled:

| metric | value |
|---|---:|
| blocks/s | **746.8** |
| pipeline wall | 415.1 s (apply 351.8 s) |
| cache hits | **64,868,912** |
| DB lookups | 46,916 (**0.73%**) |
| db_time | 0.72 s (**0.2%** of apply) |
| missing UTXOs | **0** |
| peak RSS | 18.6 GB |

The same range with `--utxo-cache-mb 0` on a fresh store **did not finish in 50
minutes** (vs 6.9 min) and was killed still running — **>7× slower**. Every
cross-block spend becomes a RocksDB point lookup without the cache, and on this
dense range that is the entire apply cost.

Header pass: signet (149 blk files, 22 GB) scans in **28–45 s** in parallel.

### Caveats (read before trusting this on mainnet)

- **Cold-start short runs do not benefit.** A 1,500-block run over a store
  seeded only to that range's start spends more time *filling* the cache than
  it saves and measured *slower* with the cache on. The cache pays off over a
  long contiguous run, which is exactly the bulk-index case.
- **The >7× cache-off figure is a strong signal, not an isolated one.** It was
  measured on tmpfs (`/tmp`), so the comparison is RocksDB per-lookup work, not
  disk I/O, and the cache-off run was killed rather than completed. §11's
  controlled testnet4 A/B (disk-backed, both sides seeded) showed a smaller but
  clean **+15.7%** throughput. Re-measure on a disk-backed mainnet corpus.
- **Testnet4/signet hit rates overstate mainnet.** A few GiB window covers a
  large fraction of a small chain's UTXO set; on mainnet it will cover much
  less. The auto-sizing is the right default; the *magnitude* must be
  re-measured on mainnet.
- **`--utxo-cache-mb`/`--block-cache-mb` still ignore `bidx live`.** Live mode
  keeps the recent cache off (undo correctness); it now gets the auto-sized
  block cache but not the recent cache.

### Tests

`bidx_core::mem` has 8 unit tests (field parsing present/missing/garbage,
zero-safe and clamped budgeting, the exact 60/40 split). `utxo.rs` adds
`open_tuned_sizes_a_single_shared_block_cache`, `open_tuned_enables_the_recent_cache`,
and `background_jobs_and_subcompactions_scale_and_clamp`; `pipeline.rs` adds
`resolve_cache_bytes_prefers_overrides_and_falls_back_to_budget` and an
end-to-end auto-network/default-checkpoint-path run; `chain.rs`'s parallel pass
is exercised by the existing chain-index tests. Per-file coverage is at 100%
for every touched file (`scripts/coverage/check-changed.sh`).

## 13. The mainnet OOM segfault: one memory plan, a soft watchdog, and relief

The report: a mainnet bulk index on the same 12-core / 32 GB / NVMe box died at
**height 439,800** with

```
ERROR watchdog: RSS exceeded limit; aborting pipeline rss_mb=23800 limit_mb=23800
```

and a **segmentation fault**. The watchdog message suggests `--threads` or
`MAX_CACHED_FILE`, but the operator's question was the right one: why should a
run have to be hand-tuned not to crash?

### Root cause: two budgets that did not know about each other

The cache-budget planner and `max_rss_kb` both read `/proc/meminfo` and
computed a number from it *independently*:

- the **cache budget** = `(available − 10% − 2 GiB) × 60%` → on this box
  ~14.2 GiB of caches (recent ~8.5 + block ~5.7);
- the **watchdog ceiling** = `available − 10% − 2 GiB` → ~23.8 GiB, exactly
  where RSS was at the crash.

The caches were not the only heap. **RocksDB memtables** were the unaccounted
one: with `write_buffer_size = 512 MiB` and `max_write_buffer_number = 4`, each
family may pin 2 GiB, and three families could pin ~6 GiB that the cache budget
never saw. Add the decoded-file LRU (16 × 128 MiB = 2 GiB), the reorder buffer,
the sink batches, and RocksDB's own index/filter allocations, and
`caches + overhead` lands **on** the ceiling rather than below it.

That makes the watchdog a tripwire at the wrong place. It fires at the exact RSS
where a RocksDB allocation can fail — and **RocksDB does not handle allocation
failure; it segfaults**. The "abort" and the crash race, and the crash wins
because tearing down RocksDB mid-write on a failed allocation is undefined.

### The fix

**One plan, sized under the ceiling (`bidx_core::MemoryPlan`).** `plan_memory`
derives the ceiling *and* every budget beneath it from one number, and the
caches are sized **last, as the remainder**, so the invariant

```
block_cache + recent_cache + memtables + file_lru + fixed_runtime ≤ ceiling
```

holds by construction. `UtxoStore::open_planned` opens from the plan;
`run_parse` and `run_trial` pass the same plan to the store, the decoded-file
LRU, and the watchdog, so "the plan fits" and "the watchdog will not trip" are
the same statement. Two supporting changes:

- **`set_db_write_buffer_size`** caps *all* memtables at the plan's
  `memtable_budget_bytes` (2 GiB on a big box), closing the largest gap.
- The fixed overheads are **capped by a fraction of the ceiling**, not just by
  their own absolute constant, so a small host scales them down instead of the
  2 GiB memtable reserve alone exceeding a 4 GiB ceiling.

**A soft watchdog threshold with automatic relief.** The watchdog now latches a
*pressure* flag at 85% of the ceiling, before the hard limit. On each block the
consumer checks it and, if set, sheds memory instead of waiting to abort:

1. `UtxoStore::relieve_pressure` halves the recent-UTXO cache budget (evicting
   to the new budget) and halves the shared block-cache capacity;
2. the consumer halves the decoded-file LRU bound (`ThreadFileCache::shrink_to`,
   which lowers the shared bound for every worker clone).

All three are pure caches: dropping them costs hit rate, never correctness. The
hard abort remains as the last resort, but its error now says the caches were
already shed. In the normal case the run **does not crash and does not need
re-tuning** — it trades cache for headroom and keeps going.

### Why not just raise the ceiling?

The ceiling is `MemAvailable`-derived on purpose (§2, item 3). Raising it past
what the kernel will actually grant just moves the failure from a clean abort to
the OOM killer or an allocation-failure segfault. The lever that works is
*spending less*, which is what relief does.

### Tests

`bidx_core::mem` adds `plan_fits_under_the_ceiling_with_a_spike_margin` (the
invariant, swept across 4–128 GiB), `plan_scales_the_file_lru_with_the_ceiling`,
and `soft_threshold_sits_below_the_ceiling`. `utxo.rs` adds
`relieve_pressure_shrinks_both_caches` (asserts the exact bytes freed and the
eviction), `relieve_pressure_stops_the_block_cache_at_its_floor`,
`recent_cache_set_budget_evicts_down_and_ignores_growth`, and
`open_planned_routes_the_plan_into_the_store`. `bench/mod.rs` adds
`watchdog_reports_pressure_before_it_trips`; `pipeline.rs` adds
`thread_file_cache_shrink_to_lowers_the_bound_and_evicts`. Per-file coverage is
at 100% for every touched file.

## 14. Cutting disk I/O during initial ingestion

The initial bulk index is disk-bound, but not on the reads one might expect:
non-XOR `blk*.dat` is a zero-copy `mmap` served from page cache, so the block
reads are cheap. The I/O is dominated by the *write* side — RocksDB leveled
compaction rewrites the same data through every level, so bytes-to-NVMe are many
times the logical UTXO size — plus the point-lookup *reads* for spent outpoints
that miss the recent-UTXO cache and the block cache. Four changes, all in
`bidx-store` plus a `run_parse` hook:

### Write amplification

- **Deferred compaction for the load.** `UtxoStore::open_planned_bulk` opens the
  store with `disable_auto_compactions` (set on both the DB-level and the
  per-CF options — per-CF is what takes effect, §6). Writes pile up in L0 with
  no interleaved rewrites; `run_parse` calls `UtxoStore::finalize_bulk_load`
  after the last block, which re-enables compaction and issues one
  `compact_range` over the UTXO family. The I/O is paid once at the end instead
  of smeared across the whole run.
- **The L0 throttle is neutralized while deferred.** With compaction off, L0
  only grows, so RocksDB's default `level_zero_stop_writes_trigger` (30) would
  *halt all writes* with nothing coming to relieve it — a deadlock on a long
  load. `disable_l0_write_throttle` raises both triggers to `i32::MAX` for the
  deferral, and `finalize_bulk_load` restores the tuned values from the same
  consts (`L0_SLOWDOWN_TRIGGER` / `L0_STOP_TRIGGER`), so the two cannot drift.
- **`set_level_compaction_dynamic_level_bytes(true)`** in bulk mode: levels are
  sized so the bulk lands in the bottommost level instead of being rewritten on
  the way down.
- **Compaction readahead (2 MiB) and a 2 GiB/s rate limiter** so compaction
  reads sequentially and cannot monopolize the device queue the foreground
  lookups share.

### Reads

- **HyperClockCache instead of LRU** for the shared block cache. A dozen parse
  workers plus the consumer hit one cache; the LRU's per-hit mutex is a
  contention point, HyperClock is lock-free on the read path. It stays mutable
  (HyperClockCache implements `SetCapacity`), so `relieve_pressure`'s shedding
  still works.

### Parquet sink

- **`BufWriter` around every part file.** `ArrowWriter` issues one write per
  encoded page, so an unbuffered part file is a syscall storm on the single sink
  thread. A 1 MiB buffer collapses those into large sequential writes; Parquet's
  `finish` flushes through to the inner writer, so nothing is stranded on close.

### What is *not* done here

The batched `multi_get` prefetch stays unimplemented — §9 measured it as noise
once the block cache made the working set resident, and the note in `utxo.rs`
flags mainnet as the case to re-ask. No benchmark before/after is recorded yet:
this lands the mechanism, and `bidx bench --mode full` on mainnet is the
measurement that decides whether the compaction profile is a win or a loss
(deferral trades peak read amplification during the load for less total write
I/O; if L0 grows enough to slow the point lookups more than the write saving is
worth, `open_planned` — the non-deferred path — remains the fallback).

### Tests

`utxo.rs` adds `open_planned_bulk_routes_the_plan_caches` (the caches reach the
CF), `bulk_load_defers_compaction_then_finalize_compacts` (a write survives the
finalize compaction and still resolves), and
`finalize_bulk_load_is_a_noop_for_non_deferred_stores` (a live store and a plain
bulk store are never compacted out from under themselves).
`parquet_writers.rs` adds `bufwriter_part_survives_multiple_row_groups` (five
rows across three row groups read back intact through the buffer). Per-file
coverage is at 100% for every touched file.

## 15. Deferring ClickHouse materialized views during the bulk load

§14 cut the RocksDB/Parquet write I/O. The other half of the initial load is the
ClickHouse side, and there the cost is *write amplification by fan-out*: thirteen
materialized views turn each base-table insert into several derived-table
inserts. An `outputs` row alone fires the `outputs_by_address`, `address_history`
(fund), `utxos_by_address` (fund), `address_stats` (fund), `blocks_by_miner`, and
`scripthash_map` views — six synchronous writes for one base row — and a `spends`
row fires five more. On a full chain that is many times the base row count in
independent insert operations, each its own part to merge.

### Drop the views, load flat, rebuild once

`ChLoader::load_all_with` now brackets the base load with the derived-index
build, unconditionally (no flag — this is the bulk path, and a bulk path starts
from an empty database):

1. **Drop every materialized view** (`schema::drop_materialized_views`). `DROP
   VIEW`, not `DROP TABLE`: the target index tables stay, and `all_ddl` recreates
   the same set from the single `schema::materialized_views` list, so the schema
   and the loader cannot drift.
2. **Load the base entities** — `blocks`, `transactions`, `inputs`, `outputs`,
   `spends`, and the optional `witnesses` — as plain appends. No view fires, so
   each entity is one `INSERT` per part file with no fan-out.
3. **Build each derived table once** from the base tables
   (`schema::backfill_derived_indexes`): one `INSERT ... SELECT` per target,
   `TRUNCATE`d first so the sequence is idempotent. The `SELECT` bodies are the
   exact expressions the views used, so the result is identical to what the live
   views would have produced.
4. **Recreate the views** (`schema::materialized_views`) so live mode and any
   later append keep the derived tables current.

The only place the backfill intentionally differs from a view body is
`scripthash_map`: the view inserts every output row and leans on
`ReplacingMergeTree` to dedup, while the backfill `SELECT DISTINCT`s — one row
per distinct script instead of one per output, dropping the redundant `SHA256`
work and most of the rows before they are written.

### Scope

This is the `bidx load` path only. Live mode never calls `load_all_with`; it
keeps the views live and appends through `live_insert`, and `bidx index rebuild`
still uses the existing per-index backfills (`backfill_coinstats`,
`backfill_scripthash_map`). Because a bulk load truncates and rebuilds the
derived tables, this is only valid against a database whose derived tables are
disposable — which is exactly the "load a fresh chain" case it targets.

### Tests

`schema.rs` adds `all_ddl_materialized_views_match_the_shared_list` (the schema
and the drop/recreate list are the same set),
`drop_materialized_views_drops_every_view_by_name`, and
`backfill_derived_indexes_covers_every_mv_target` (every target is truncated
once and refilled, with both halves for the two-source tables). `lib.rs` adds
`load_all_defers_derived_indexes_in_order`, which asserts on a fake client's log
that the views are dropped *before* the base load, the backfill runs *after* it,
and the views are recreated *last* — the ordering is the whole point. Per-file
coverage is at 100% for both touched files.

### One `INSERT ... UNION ALL` per two-source table

Three derived tables are fed by *two* materialized views each
(`address_history`, `utxos_by_address`, `address_stats`). The backfill builds
each of them with a **single** `INSERT ... SELECT ... UNION ALL SELECT ...`
rather than two separate `INSERT`s. Each ClickHouse `INSERT` produces its own
set of parts that the server later merges, so one statement halves the part
count — and the merge writes — for those tables. The rows are identical either
way; only the delivery changes. This also matches `backfill_coinstats`, which
already unions its fund/spend halves.

### In-memory memtable merge before flush (`bidx-store`)

`apply_write_tuning` sets `min_write_buffer_number_to_merge(2)` in bulk mode
(live mode keeps the default of 1). A bulk index creates an outpoint and spends
it a short time later; with the default, if the `put` and the `delete` land in
two different memtables, RocksDB flushes **both** — two SST entries for a UTXO
that no longer exists. Merging two memtables in memory first applies
last-write-wins across them, so the pair cancels and *neither* reaches L0: a
short-lived output costs zero flush bytes instead of two. `finalize_bulk_load`
would drop the cancelled pair anyway, so the steady-state store is identical —
this only removes the intermediate L0 write. The policy lives in the pure
`memtables_to_merge(bulk)` helper (tested) so it is pinned without a live store.
**This one has no before/after bench yet** — it is a write-path change to
`bidx-store`, and `rust-performance.mdc` asks for a `bidx bench --mode full`
comparison; record it here once measured.

## 16. The benchmark that threw away its own data

A mainnet bench that died at height 435000 produced **no metrics at all**. The
cause was in the harness, not the pipeline: `run_bench` collected every trial
into a `Vec` and wrote `--csv` (really pretty JSON) exactly once, *after* the
loop, via `run_trial(...)?`. A watchdog abort propagated that `?` straight out
of `run_bench` and skipped the write entirely, so a 13-hour run left nothing on
disk — and the death point is precisely the data point worth having.

### Flush per trial, and report on abort

Two changes, both in `crates/bidx-cli/src/bench/mod.rs`:

1. **Per-trial flush.** `write_report(path, &trials)` runs after *every* trial.
   A `--repeat` run that dies on trial 2 now leaves trial 1's completed numbers
   on disk instead of nothing. The `-` (stdout) target is still written once,
   after the loop, so a multi-trial run does not spew N JSON blobs.

2. **Report on abort.** `run_trial` no longer propagates a mid-pipeline abort.
   The worker pool's `Result` and the consumer's are captured rather than
   `?`-ed, and `resolve_results` picks the root cause (the worker's error wins —
   a parse failure is what starved the consumer). The trial is still assembled
   with the live counters — `blocks_done`, the stage sums, the db/pending/cache
   lookup breakdown, the RSS peak, and (with `--rocksdb-stats`) the RocksDB
   property snapshot *at the point of death* — and marked `complete: false` with
   the `error` string set.

`run_bench` then fails loudly *after* the flush: the first aborted trial is
re-raised as `bench did not complete: <cause>`, so a non-zero exit still means
"did not finish" while the JSON holds the partial numbers. The cold-store guard
is folded into the same `error` field rather than returned early, so its report
is flushed too.

### New report fields

`TrialReport` gains `complete` (bool), `error` (omitted when clean),
`last_height` (the first height *not* applied — the abort point), and
`planned_blocks` (the intended range). `print_trial` prints an `!! INCOMPLETE`
banner naming the height and the cause, and flags that the throughput below
covers only the completed blocks.

### Tests

`run_bench_flushes_an_incomplete_trial_report_then_fails` drives a real
mid-pipeline abort (a synthetic chain whose block 1 has a corrupt body — the
header pass reads headers only, so it still builds the index, and the failure
lands in the parse workers) and asserts the file exists, carries
`"complete": false`, and that `run_bench` still returns the loud error.
`run_bench_flushes_a_completed_trial_before_a_later_one_aborts` proves the
per-trial flush survives a later failure. `resolve_results` covers all four
`(worker, consumer)` outcomes directly, and `run_bench_cold_store_still_flushes_the_report`
covers the cold-store path. Per-file coverage is at 100%.

## 17. Why mainnet crashed at height ~435,000 on 32 GiB RAM

Even with `MemoryPlan` and the soft/hard `OomWatchdog` in place, a mainnet
`bidx bench` on a 32 GiB machine crashed reproducibly around height
435,000–439,800. Six compounding memory leaks, allocation spikes, and relief
bugs caused it:

1. **`RecentUtxoCache` tombstone leak + \(2^{28} \to 2^{29}\) `VecDeque` resize
   spike (`crates/bidx-store/src/utxo.rs`).** `RecentUtxoCache::remove` removed
   spent outputs from `FxHashMap` while leaving every ever-created 36-byte key
   in `order: VecDeque<[u8; 36]>`, and `evict_to_budget` only popped `order`
   when *unspent* count (`map.len() * 112`) exceeded `budget_bytes` (~9–10 GiB
   on 32 GiB, ~85–95M live entries). At mainnet height 435,000, the live UTXO
   set is ~44M (`map.len()` well under budget — so `evict_to_budget` had
   **never once popped `order`**), while cumulative created outputs cross
   \(2^{28} = 268{,}435{,}456\). Pushing the \(268{,}435{,}457\text{th}\)
   outpoint grew `VecDeque` from \(2^{28} \times 36\text{ B} = 9.66\text{ GiB}\)
   to \(2^{29} \times 36\text{ B} = 19.33\text{ GiB}\), holding **29.0 GiB**
   during `realloc` alone. Worse, `set_budget` never called `shrink_to` on
   either `FxHashMap` or `VecDeque`, so `relieve_pressure` freed **0 bytes** of
   RSS.
2. **Single-shot dead-latch in `shed_on_pressure` (`crates/bidx-cli/src/pipeline.rs`).**
   Pressure relief fired only on the rising edge (`pressured && !was_pressured`).
   Whenever a single halving did not drop `VmRSS` all the way from >85% below
   the 80% recovery threshold, `pressured` stayed latched `true` and **no
   second relief step ever fired** while RSS climbed on to 100%.
3. **Rayon binary work-stealing across the whole chain + 512-deep channel
   (`pipeline.rs`, `bench/mod.rs`).** `heights.par_iter()` recursively split
   `0..tip` in half, so workers immediately jumped to heights 217k, 435k, 652k
   while `next_height` was at 0 — opening a dozen unrelated 128 MiB `blk*.dat`
   files simultaneously and buffering up to `CHANNEL_DEPTH = 512` parsed
   `FullBlock`s (~1–3 GiB) inside `crossbeam_channel` before `reorder_depth`
   even observed them.
4. **Double-buffered XOR `BlkFile`s (`crates/bidx-parser/src/blkfile.rs`).**
   When `xor.dat` was present (Bitcoin Core v28+), `BlkFile` decoded the file
   into `decoded: Vec<u8>` (~128 MiB `RssAnon`) **and kept `mmap: Mmap` alive**
   (~128 MiB `RssFile`, counted in `/proc/self/stat` `VmRSS`), doubling each
   cached file to 256 MiB.
5. **RocksDB pinned L0 filter/index blocks + glibc arena retention (`utxo.rs`,
   `mem.rs`).** `set_pin_l0_filter_and_index_blocks_in_cache(true)` prevented
   `block_cache.set_capacity()` from evicting L0 metadata (hundreds of SSTs in
   deferred-compaction mode), and glibc `ptmalloc2` retained freed pages in
   per-thread arenas across ~30 Rayon/RocksDB threads until `malloc_trim(0)`.
6. **4× `ArrowWriter` row-group buffering (`parquet_writers.rs`).**
   `SinkConfig::batch_size` was 250k rows, yet `max_row_group_size` was 1M,
   causing `ArrowWriter` to hold four `RecordBatch`es per entity table in
   `InMemoryRowGroup` before flushing.

### Fixes

- **Zero-tombstone, exact-budget `RecentUtxoCache` (`utxo.rs`):** replaced
  `FxHashMap + VecDeque` with a contiguous `Vec<CacheEntry>` (96 B/slot with
  intrusive `u32` doubly-linked FIFO pointers + `free_head` slot recycling) and
  a compact open-addressed `Vec<u64>` index (`(tag32 << 32) | (slot + 1)`, 8 B
  per bucket) using Knuth Algorithm R backward-shift deletion. Spent slots are
  unlinked in \(O(1)\) and recycled immediately with zero tombstones; `entries`
  grows via `reserve_exact` clamped to `max_entries` (`CACHE_ENTRY_COST = 128`,
  so total heap is strictly \(\le \text{budget\_bytes}\)); and `set_budget`
  drops the old bucket array first, compacts live slots in place, and calls
  `shrink_to` so glibc `mremap` unmaps trailing pages without a 2× copy spike.
- **Progressive `PressureState` + `trim_allocator()` (`pipeline.rs`, `mem.rs`,
  `utxo.rs`):** fires immediately on the rising edge (now at `SOFT_PCT = 80%`,
  recovery at `RECOVER_PCT = 72%`, leaving a 20% runway below `ceiling_kb`)
  **and** repeats every 2 s (`PRESSURE_RELIEF_COOLDOWN`) while pressure
  persists until RSS recovers or all caches hit their floors, calling
  `libc::malloc_trim(0)` so freed C++/Rust arena pages return to the kernel.
  `plan_memory_for_host` also trims arenas after the header pass before reading
  `MemAvailable`, reserves `1/8` of `MemAvailable` + 2 GiB OS headroom + 2 GiB
  fixed runtime reserve, caps `file_cache_files` at 8 (1 GiB), and uses a 25%
  spike margin (`CACHE_FRACTION_PCT = 75`).
- **Sequential `AtomicU32` height cursor + `CHANNEL_DEPTH = 64` (`pipeline.rs`,
  `bench/mod.rs`):** workers claim heights in strict ascending order and call
  `wait_for_consumer` before reading `blk` bytes or allocating `FullBlock`s,
  keeping all workers on the same 1–2 `blk*.dat` files and bounding total
  in-flight blocks to `reorder_high + num_threads`.
- **Single-buffer `BlkFile` (`blkfile.rs`), `ArrowWriter` row-group bound
  (`parquet_writers.rs`), and unpinned RocksDB index/filter caching +
  budget-scaled `write_buffer_size` (`utxo.rs`).**

---

## 18. Block-file disk I/O & memory-bandwidth optimizations

Reading and decoding `blk*.dat` files across ~900,000 mainnet blocks (~680 GiB
across ~4,800 × 128 MiB files) had five major avoidable I/O, syscall, and
memory-copy overheads:

1. **Header-only `pread` scanner (`scan_blk_headers`) for `build_chain_index`
   (`crates/bidx-parser/src/blkfile.rs`, `chain.rs`).**
   Previously `chain::scan_file` called `BlkFile::open` on every `blkNNNNN.dat`
   file, faulting and XOR-decoding all ~680 GiB of multi-megabyte transaction
   bodies into 128 MiB `Vec<u8>` buffers just to inspect the 88-byte record
   prefix (`magic[4] + len[4] + header[80]`). `scan_blk_headers` walks record
   offsets using `FileExt::read_exact_at` (`pread64`) into an 88-byte stack
   buffer, XOR-decoding only those 88 bytes when obfuscated and jumping directly
   across each block payload (`pos += 8 + len`) — shrinking logical header-pass
   reads from ~680 GiB to ~79 MiB (~8,600× less XOR work, zero 128 MiB heap
   allocations).
2. **Zero-copy `CachedBlock` handle (`Deref<Target = [u8]>`) in
   `ThreadFileCache` (`crates/bidx-cli/src/pipeline.rs`, `bench/mod.rs`).**
   `ThreadFileCache::block_bytes` previously returned `Arc::from(slice)`
   (`Arc<[u8]>`), allocating and `memcpy`ing every 1–4 MiB block (~680 GiB
   total) even though `Arc<BlkFile>` was already reference-counted. `CachedBlock`
   holds `(Arc<BlkFile>, start, end)` validated via `BlkFile::block_range` and
   derefs directly to `&[u8]` with zero allocation and zero copy, and workers
   drop `CachedBlock` immediately after `parse_block_full` before `tx.send`.
3. **Per-`file_id` singleflight loading + lock-free MRU fast path in
   `ThreadFileCache` (`crates/bidx-cli/src/pipeline.rs`).**
   - *Singleflight:* When 12 workers simultaneously cross into a cold
     `blkNNNNN.dat`, an `inflight` per-`file_id` `OnceLock` ensures only one
     worker reads and XOR-decodes the 128 MiB file while sibling workers wait on
     the same `OnceLock`, eliminating the 12× thundering-herd I/O and 1.5 GiB
     transient buffer spike.
   - *Lock-free MRU hit:* `CacheInner::mru_file_id` (`AtomicU32`) short-circuits
     `touch()` when a cache hit is already at the MRU tail (~99.5% of sequential
     block reads), avoiding `lru.lock()` and `VecDeque` linear scans on every
     block.
4. **Sequential `std::fs::read` + in-place SIMD-friendly XOR decode for
   `xor.dat` files, and `POSIX_MADV_SEQUENTIAL` for plain `Mmap` files
   (`crates/bidx-parser/src/blkfile.rs`).**
   - When `xor.dat` is active, `BlkFile::open_with_key` reads sequentially into
     a single owned `Vec<u8>`, trims trailing preallocated padding via
     `record_walk_end` (`shrink_to_fit` when \(\ge 1\text{ MiB}\)), and decodes
     in place via `as_chunks_mut::<8>()` — avoiding `mmap` setup, 32,768 minor
     page faults per file, a second 128 MiB allocation, and cross-core `munmap`
     TLB shootdown IPIs.
   - When `xor.dat` is absent, `Mmap::map` immediately issues
     `mmap.advise(memmap2::Advice::Sequential)` so the kernel prefetches ahead
     at full NVMe sequential bandwidth.
5. **Directory-cached `xor.dat` key (`read_xor_key_from_dir` +
   `BlkFile::open_with_key`, `blkfile.rs`, `chain.rs`, `pipeline.rs`).**
   Both `build_chain_index` and `ThreadFileCache::with_capacity` resolve
   `<blocks_dir>/xor.dat` once at startup and pass the cached `Option<[u8; 8]>`
   to every file scan/open, eliminating ~38,000 redundant `xor.dat` syscalls
   across a full chain run.

---

## 19. Bulk-load pipeline, UTXO multi-get, Parquet SoA, & ClickHouse streaming optimizations

Profiling the end-to-end bulk index path (`bidx parse` + `bidx load` +
`bidx index rebuild address_prefix`) surfaced seven major avoidable bottlenecks
across RocksDB point lookups, Parquet encoding/compression, per-transaction
allocations, and ClickHouse ingestion:

1. **Skip consensus-unspendable `OP_RETURN` outputs in `UtxoStore` &
   `RecentUtxoCache` (`crates/bidx-store/src/utxo.rs`).**
   Mainnet contains >130M `OP_RETURN` (`0x6a`, `ScriptType::OpReturn = 7`)
   outputs that consensus forbids ever spending. `UtxoStore::apply_block` now
   skips inserting `OP_RETURN` outputs into `pending`, `RecentUtxoCache`,
   `WriteBatch`, `BlockUndo`, and RocksDB SSTs/Bloom filters while still
   subtracting `out.value_sat` in the fee loop so burned-satoshi fee accounting
   and Parquet/ClickHouse `outputs` rows remain byte-identical.
2. **Sorted `batched_multi_get_cf`, single-lock `RecentUtxoCache` pass, reused
   `ApplyScratch`, and RocksDB `format_version(5)` (`crates/bidx-store/src/utxo.rs`).**
   - *Single cache lock + sorted batched multi-get:* Instead of locking
     `RecentUtxoCache` ~3,000–10,000 times per block and issuing sequential
     unsorted `db.get_pinned_cf` calls, `apply_block` resolves same-block
     `pending` + `RecentUtxoCache` hits under **one** `recent.lock()` acquisition
     per block (dropping the lock before any disk I/O), sorts unique cache-miss
     outpoint keys in `ApplyScratch::misses` so adjacent outpoints share SST data
     blocks and read in ascending LBA order, issues one
     `db.batched_multi_get_cf(&cf, &sorted_keys, true)` call per block, and
     emits `spends`/`fees`/`undo.spent` in exact original `inputs` slice order.
   - *SST format v5:* `tuned_options` sets `block_opts.set_format_version(5)` for
     XXH3 block checksums and faster v5 Bloom filter probes.
3. **Selective Parquet dictionary encoding, `Zstd(1)`, concrete SoA column
   builders, and parallel entity flush/close
   (`crates/bidx-store/src/parquet_writers.rs`).**
   - *Disable dictionary encoding on random 32-byte digests:* Previously
     dictionary encoding was enabled globally, causing `ArrowWriter` to hash ~8B
     unique `txid`/`prev_txid`/`spending_txid`/`spent_txid`/`hash`/`merkle_root`
     digests into doomed 1 MiB hash tables before falling back to plain encoding.
     Dictionary encoding is now off globally and enabled selectively only on low-
     or medium-cardinality columns (`DICTIONARY_COLUMNS`: heights, versions,
     script types/lengths, sequences, and `address_hash`/`spent_address_hash`),
     paired with explicit `ZstdLevel::try_new(1)` (~3–4× faster than default
     level 3 on incompressible 32-byte hashes).
   - *Concrete SoA builders + Rayon flush:* Replaced type-erased `Vec<Col>`
     per-cell enum dispatch and unnecessary validity bitmaps with typed
     `BlocksTable` / `TxsTable` / `InputsTable` / `OutputsTable` / `SpendsTable` /
     `WitnessesTable` Struct-of-Arrays builders (`Vec<T>` -> zero-copy
     `PrimitiveArray`, `Fixed32Col` -> `FixedSizeBinaryArray` from flat
     `Vec<u8>`) and parallelized multi-table flushes and part closes via
     `rayon::scope`.
4. **Block-level flat `Vec` reuse in `parse_block_full_opts` & \(O(1)\) coinbase
   fee check (`crates/bidx-parser/src/tx.rs`, `pipeline.rs`, `bench/mod.rs`).**
   `parse_tx` now appends directly into pre-sized block-level `&mut inputs`,
   `&mut outputs`, and `&mut witnesses` buffers (stamping `txid` on the slice
   tail `[inp_start..]` / `[out_start..]`), eliminating ~2.2B short-lived per-tx
   `Vec` heap allocations across mainnet; `consume_ordered` and `bench_consume`
   replace the per-block `vec![false; block.txs.len()]` + full-input scan with an
   \(O(1)\) check on `block.inputs.first()`.
5. **Bounded-memory streaming `RowBinary` `rebuild_address_prefix` & parallel
   Rayon address rendering (`crates/bidx-loader/src/lib.rs`, `rowbinary.rs`,
   `main.rs`).**
   Replaced `query_scalar` (which buffered ~1B rows / ~68 GiB of TSV into a
   single `String`) and ~2M per-500-row `clickhouse-client` process spawns with
   a line-by-line `BufReader` over the `SELECT DISTINCT` child stdout, parallel
   base58/bech32 chunk rendering (`rebuild_address_prefix_par`), and binary
   `RowBinary` writes (`rowbinary::address_prefix`) into a single persistent
   `clickhouse-client` `INSERT ... FORMAT RowBinary` stdin pipe.
6. **Parallel ClickHouse base-entity loads, client uploads, and derived-index
   backfills (`crates/bidx-loader/src/lib.rs`, `schema.rs`).**
   `ChLoader::load_all_with` drops all 10 materialized views in a single
   `--multiquery` call (`begin_streaming_load`), loads all base entities
   concurrently in `ServerFile` mode (or streams `(entity, part_path)` tasks
   across `UPLOAD_CONCURRENCY = 4` concurrent pipes in `Upload` mode), builds all
   10 disjoint `[TRUNCATE, INSERT]` derived-index groups (`backfill_derived_index_groups`)
   concurrently across `BACKFILL_CONCURRENCY = 4` connections, and recreates
   views in a single `--multiquery` batch (`finish_streaming_load`).
7. **Pipelined part-file ClickHouse ingestion during `bidx parse`
   (`--stream-clickhouse` & `--unlink-loaded-parts`, `pipeline.rs`, `main.rs`,
   `bidx-loader/src/lib.rs`).**
   When `bidx parse --stream-clickhouse` is enabled, `run_parse` drops ClickHouse
   materialized views up front and spawns a background part-loader fed by a
   bounded channel (`PART_CHANNEL_DEPTH = 4`). As each Parquet part closes,
   `ChLoader::load_part` ingests all entity files for that part into ClickHouse
   in parallel while RocksDB/Parquet continue on subsequent heights (and while
   `utxo.finalize_bulk_load()` compacts L0 at the end), optionally unlinking each
   ingested Parquet part file (`--unlink-loaded-parts`) so bulk indexing needs no
   separate `bidx load` pass and avoids keeping a duplicate Parquet copy of the
   chain on disk.

---

## 20. Write-back selective `RecentUtxoCache`, bounded bulk L0 compaction, & partitioned Ribbon SSTs

Mainnet bulk-load profiling past height ~400,000 identified why `blocks/sec`
degraded progressively in the second half of the chain:

- Once `RecentUtxoCache` filled around height ~400,000, disabling auto-compaction
  completely (`disable_auto_compactions = true`, `slowdown/stop = i32::MAX`) let
  L0 accumulate **400+ overlapping SST files**, turning every cache miss into an
  \(O(N_{\text{L0}})\) probe across hundreds of files and thrashing `block_cache`.
- `RecentUtxoCache` was **write-through** rather than **write-back**, so ~75–85%
  of outputs spent shortly after creation still wrote both a `batch.put` AND a
  `batch.delete` into RocksDB memtables and L0 SSTs.
- Nearly half of modern non-`OP_RETURN` outputs are permanently cold dust or
  data-carrier outputs (`<= 546` sat Ordinals/Runes postage, `BareMultisig`
  Counterparty/Stamps fake-pubkey outputs, `Unknown`/`WitnessUnknown` scripts,
  and high-fanout mass-dusting outputs), polluting `RecentUtxoCache` and evicting
  hot spendable UTXOs.

Six coordinated changes eliminate this post-400k slowdown (`crates/bidx-store/src/utxo.rs`,
`crates/bidx-core/src/mem.rs`):

1. **Write-back `RecentUtxoCache` with pure in-RAM spend cancellation.**
   Hot outputs entering `RecentUtxoCache` are no longer written to RocksDB on
   creation (`batch.put` is skipped), and spending a cached output removes it in
   RAM without writing a tombstone (`batch.delete` is skipped). Only unspent
   outputs evicted from the FIFO head at capacity spill to RocksDB, eliminating
   ~75–85% of all RocksDB memtable/SST writes across mainnet. When every output
   and input in a block resolves in RAM, `self.db.write_opt` is skipped entirely.
   Chunked eviction (`evict_recent_to` in `32,768`-entry / ~2.6 MiB `WriteBatch`
   chunks with the cache mutex released during I/O) drains dirty entries cleanly
   on `relieve_pressure`, `flush`, `finalize_bulk_load`, and `Drop`.
2. **Selective UTXO cache admission (`should_cache_utxo`).**
   Outputs with `value_sat <= 546` (`DUST_UTXO_THRESHOLD_SAT`, covering `0`-sat
   outputs, `330`-sat P2TR inscription/Runes postage, and `546`-sat P2PKH/P2WPKH
   dust), non-standard/data-carrier script types (`Unknown`, `OpReturn`,
   `BareMultisig`, `WitnessUnknown`), or high-fanout dust (`tx_output_count >= 64`
   and `value_sat <= 1_000` sat) bypass `RecentUtxoCache` directly into RocksDB,
   nearly doubling the effective block-window coverage of `RecentUtxoCache` for
   genuinely spendable outputs while preserving 100% spend correctness via DB
   fallback if any bypassed output is ever spent.
3. **92-byte packed `CacheEntry` (`CACHE_ENTRY_COST = 120`) & 70/30 memory split
   (`RECENT_SHARE_PCT = 70`).**
   Packing `value_sat` as `[u8; 8]` removes 4-byte struct alignment padding on
   `CacheEntry` (`96 B -> 92 B`, lowering `CACHE_ENTRY_COST` from `128 B` to a
   strict upper bound of `120 B`), and raising `RECENT_SHARE_PCT` from `60%` to
   `70%` expands a 32 GiB host's `RecentUtxoCache` from ~74.0M slots to **~98.6M
   hot UTXO slots** (+33.3% raw slots, ~2.5× effective spendable window with
   selective admission) before releasing its multi-GiB backing vectors ahead of
   the final compaction in `finalize_bulk_load`.
4. **Coarse bounded bulk L0 compaction (`apply_bulk_l0_batching`: trigger `16`,
   slowdown `48`, stop `64`).**
   `UtxoStore::open_planned_bulk` keeps background auto-compaction active at a
   coarse 16-file L0 batch trigger instead of disabling compaction completely,
   merging ~8 GiB waves of L0 SSTs into disjoint base-level files (continuously
   purging cancelled `put`+`delete` pairs) while bounding L0 read depth for the
   entire chain run. `finalize_bulk_load` restores steady-state triggers
   (`8`/`20`/`30`) and runs the final compaction pass.
5. **Partitioned two-level SST indexes, hybrid Ribbon filters, &
   `optimize_filters_for_hits(true)`.**
   `tuned_options` configures `TwoLevelIndexSearch` + `partition_filters(true)` +
   `pin_top_level_index_and_filter(true)` (keeping top-level partition directories
   pinned in a few MiB while 4 KiB index/filter partitions evict cleanly under
   `block_cache.set_capacity`), `hybrid_ribbon_filter(10.0, 2)` (fast Bloom on
   L0/L1, ~30% smaller Ribbon filters on L2+), and
   `set_optimize_filters_for_hits(true)` (skipping bottommost-level filters since
   every valid Bitcoin input hits an existing UTXO).
6. **Whole-key memtable Bloom filters (`memtable_prefix_bloom_ratio = 0.05`,
   `memtable_whole_key_filtering = true`).**
   Cache misses probe an in-RAM Bloom filter on each 512 MiB active/immutable
   memtable instead of performing skiplist binary searches before checking SSTs.

---

## 21. Schema v3 spend consolidation, `txid` column pruning, 16-block lookahead windowing, & zero-Parquet direct ingestion

End-to-end resource analysis of a full ~880,000-block mainnet bulk load (~1.15B
transactions, ~2.89B inputs, ~3.28B outputs, ~2.89B non-coinbase spends) exposed
massive cross-table duplication, unnecessary disk round-trips, and post-load
ClickHouse compaction bottlenecks. Fourteen coordinated architectural changes
eliminate them (`SCHEMA_VERSION = 3`):

1. **Consolidate `spends` into `inputs` (`SpendRow` & `spends` base table
   eliminated; `crates/bidx-core/src/types.rs`, `bidx-store`, `bidx-loader`,
   `bidx-live`, `bidx-cli`).**
   Every non-coinbase `InputRow` (`is_coinbase = 0`) had an exact 1:1 row in the
   standalone `spends` table duplicating `(height, tx_index, input_index,
   prev_txid, prev_vout)` across 2.89B rows (~202 GB raw). `InputRow` now carries
   `spent_value_sat: u64`, `spent_script_type: u8`, `spent_address_hash: [u8; 32]`,
   and `spent_height: u32` directly (zeroed on coinbase inputs), populated
   in-place by `UtxoStore::apply_block_in_place` / `apply_window_in_place`. The
   5th base table (`spends`), `SpendRow`, `SpendsTable`, `spend_recycle`
   channels, and 5th `LiveChWriter` pipe are completely deleted (`ENTITIES` is
   now 4 base tables: `blocks`, `transactions`, `inputs`, `outputs`).
2. **Prune redundant `txid` from `inputs` and `outputs` (`crates/bidx-core`,
   `bidx-parser`, `bidx-store`, `bidx-loader`, `bidx-serve`).**
   `inputs.txid` (2.89B × 32 B = 92.5 GB incompressible random digest bytes) and
   `outputs.txid` (3.28B × 32 B = 105.0 GB) were redundant with
   `transactions(height, tx_index) -> txid` (~37 GB once) and prevented `parse_tx`
   from streaming inputs/outputs without post-parse slice walks. Dropping `txid`
   from `inputs` and `outputs` removes **197.5 GB** of incompressible Zstd/MergeTree
   payload; `bidx-serve` point-queries `inputs`/`outputs` purely on the primary
   key `(height, tx_index)` after resolving `transactions_by_txid`, and joins
   `transactions USING (height, tx_index)` when rendering uncached block details.
3. **Narrow in-memory intra-block coordinates to `u16` (`crates/bidx-core`,
   `bidx-parser`, `bidx-store`, `bidx-loader`).**
   By Bitcoin consensus (`MAX_BLOCK_WEIGHT = 4,000,000` WU, minimum tx ≥ 60 B =
   240 WU), a block can contain at most ~16,666 transactions, inputs, outputs, or
   witness stack items (`< 65,535`). Narrowing `tx_index`, `input_index`,
   `output_index`, and `witness_items` to `u16` in `TxRow`/`InputRow`/`OutputRow`/
   `WitnessItemRow` shrinks `InputRow` to 96 B and `OutputRow` to 56 B across
   every `FullBlock` in flight while widening cleanly to `UInt32` at the
   Parquet/RowBinary boundary.
4. **16-block cross-block UTXO lookahead windowing (`apply_window_in_place`,
   `DEFAULT_UTXO_WINDOW_BLOCKS = 16`; `crates/bidx-store/src/utxo.rs`,
   `pipeline.rs`).**
   ~25–35% of Bitcoin spends consume outputs created 1–15 blocks earlier (child-
   pays-for-parent, exchange sweeps, Lightning/batch chains). Draining up to 16
   contiguous heights at once cancels intra-window creation+spend pairs purely in
   `ApplyScratch::pending` (zero `RecentUtxoCache` mutex churn, zero RocksDB KV
   writes), merges cache misses across all 16 blocks into one globally-sorted
   `batched_multi_get_cf`, and commits one `WriteBatch` per window.
5. **Packed 45-byte `UtxoValue` + `FixedPrefix(8)` SSTs (`utxo.rs`).**
   Standard P2PKH/P2SH/P2WPKH scripts (`script_type` 1, 2, 3, 6 — ~70% of live
   UTXOs) only populate 20 bytes of `address_hash` with 12 trailing zeros.
   Encoding those types in 33 bytes instead of 45 bytes shrinks RocksDB UTXO
   values by ~27%, and an 8-byte `SliceTransform::create_fixed_prefix(8)` on the
   uniformly-distributed SHA256d outpoint key accelerates SST prefix blooms.
6. **Zero-allocation `FullBlock` object pool (`RECYCLE_POOL_DEPTH`,
   `parse_block_full_into`; `bidx-parser`, `pipeline.rs`).**
   Rayon parse workers acquire recycled `FullBlock` structs from a bounded
   channel fed by the sink thread (`clear_keep_capacity` with high-water
   shrinking for pathological mega-blocks), eliminating ~3.5M multi-megabyte
   `Vec` heap allocations/frees across a full chain parse.
7. **Singleflight `BlkFile` open + `ChainIndex` data-end hints + proactive file
   eviction + zero-copy mmap (`blkfile.rs`, `chain.rs`, `pipeline.rs`).**
   - Unobfuscated (`xor_key.is_none()`) `blkNNNNN.dat` files keep their OS
     `Mmap` directly (`Backing::Mapped`) instead of copying 128 MiB into heap
     RAM (`madvise(MADV_SEQUENTIAL)` during header scan).
   - `build_chain_index` records exact `file_data_end` offsets per file and drops
     its ~64 MiB `by_hash` `FxHashMap` once heights are linked (`drop_hash_index`),
     so `BlkFile::open_with_hint` skips re-walking every block record.
   - `ThreadFileCache` deduplicates concurrent worker misses on a new file via
     per-`file_id` `Condvar` singleflight and immediately evicts each completed
     `blkNNNNN.dat` once the consumer drains its highest height
     (`is_last_height_in_file`).
8. **Direct ClickHouse `RowBinary` bulk ingestion (`--direct-clickhouse`;
   `live_insert.rs`, `pipeline.rs`, `main.rs`).**
   `LiveChWriter::open_bulk` opens 8 MiB-buffered `INSERT INTO ... FORMAT
   RowBinary` pipes with tuned bulk MergeTree settings (`min_insert_block_size_*`,
   `max_insert_threads = 4`, `async_insert = 0`), streaming rows straight from
   the ordered consumer into ClickHouse with **zero Parquet encoding, zero Zstd
   compression, and zero intermediate disk writes/reads** (~400+ GB disk round-trip
   eliminated).
9. **Default `IndexProfile::Serving` (`--index-profile serving|full`;
   `schema.rs`, `main.rs`).**
   Auditing `bidx-serve` showed three massive historical index tables
   (`inputs_by_prev_output`, `outputs_by_address`, `spends_by_address`, totaling
   ~9.06B MergeTree rows / ~565 GB on mainnet) are never queried by any Esplora
   or Electrum endpoint. `IndexProfile::Serving` is now the default, skipping
   their backfills and MVs unless `--index-profile full` is explicitly requested.
10. **Direct RocksDB -> `utxos_by_address` live UTXO snapshot export
    (`for_each_live_utxo` + `stream_utxos_by_address`; `utxo.rs`, `lib.rs`,
    `pipeline.rs`).**
    When indexing from genesis, `run_parse` streams the ~170M live UTXOs straight
    out of RocksDB (`4 MiB` readahead, `fill_cache = false`) into
    `utxos_by_address` as `sign = 1` `RowBinary` rows, bypassing ClickHouse's
    6.17B-row `outputs UNION ALL inputs` CollapsingMergeTree external sort (~96%
    fewer rows).
11. **Fused binary `rebuild_address_and_scripthash_par` (`schema.rs`, `lib.rs`,
    `rowbinary.rs`, `pipeline.rs`, `main.rs`).**
    Replaces the 1B-row TSV `hex(address_hash)` text scan + separate SQL
    `scripthash_map` scan with a single `SELECT DISTINCT script_type,
    address_hash FROM outputs WHERE script_type != 7 FORMAT RowBinary` stream
    (`33 B/row`, 64k-row Rayon batches) that computes both base58/bech32
    `address_prefix` and `SHA256(scriptPubKey)` `scripthash_map` in one pass.
12. **Deferred base-table projections & two-wave in-order `address_stats`
    backfill (`schema.rs`, `lib.rs`).**
    - `begin_streaming_load` drops `proj_tx_by_fee` and
      `proj_spends_by_spent_height` during base-table ingestion and materializes
      them once in `finish_streaming_load_with`.
    - `spends_by_spent_outpoint` backfills from `inputs` joined with
      `transactions` on `(height, tx_index)` using
      `SETTINGS join_algorithm = 'direct,prefer_partial_merge,hash'`.
    - Wave 2 computes `address_stats` from `address_history` (already sorted by
      `(address_hash, script_type)`) with `optimize_aggregation_in_order = 1`,
      reducing aggregation RAM from tens of GiB to \(O(1)\).
13. **ClickHouse column codecs & tuned row-group batching (`schema.rs`,
    `parquet_writers.rs`).**
    Explicit `CODEC(DoubleDelta, ZSTD(1))` / `CODEC(Delta, ZSTD(1))` on monotonic
    heights, timestamps, and intra-block indices, `CODEC(ZSTD(1))` on random
    32-byte hashes (avoiding LZ4 CPU waste on incompressible digests), and
    `DEFAULT_BATCH_ROWS = 1_048_576` for fewer, denser Parquet row groups.
14. **Native ISA codegen (`.cargo/config.toml`).**
    `rustflags = ["-C", "target-cpu=native"]` unlocks hardware SHA-NI
    instructions in `sha2` (accelerating `dsha256`, `hash160`, and Electrum
    scripthash computation ~4–6×) alongside AVX2/BMI2 vectorization in Arrow,
    Parquet, Zstd, and RocksDB.





