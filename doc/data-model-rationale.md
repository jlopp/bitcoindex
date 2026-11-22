# Data-model rationale

Why the ClickHouse schema looks the way it does. This is the "why", not the
"what" — for the tables and fields themselves see [schema.md](schema.md).

Every decision here trades a little query-side awkwardness (you reverse a
hash, you pad an address to 32 bytes) for a large ingest-side win: the ingest
pipeline is the product, and the schema is shaped to keep it fast and
allocation-free. The query-side cost is paid once by the reader; the
ingest-side cost would be paid on every block of a multi-hour run.

## Contents

- [Binary hashes, wire order](#binary-hashes-wire-order)
- [Addresses as `(script_type, address_hash)`](#addresses-as-script_type-address_hash)
- [Canonical transaction identity](#canonical-transaction-identity)
- [`spends` is a precomputed join](#spends-is-a-precomputed-join)
- [`OP_RETURN` is excluded from address indexes](#op_return-is-excluded-from-address-indexes)
- [Partitioning by 10,000-block ranges](#partitioning-by-10000-block-ranges)
- [Projections, skip indexes, and materialized views](#projections-skip-indexes-and-materialized-views)
- [Derived index tables](#derived-index-tables)
- [`FINAL` and merge-engine semantics](#final-and-merge-engine-semantics)
- [Why Parquet in the middle](#why-parquet-in-the-middle)

## Binary hashes, wire order

Every hash — block hashes, txids, merkle roots, prev-hashes — is a
`FixedString(32)` holding the **32 raw bytes in wire order** (the order they
appear in `blk*.dat`), never a hex string.

Why:

- **Fixed-size keys are `Copy` and live on the stack.** `[u8; 32]` costs
  nothing to pass and compares with a `memcmp`; a `String` allocates and a hex
  string is 2x the bytes.
- **No hex conversion on the hot path.** The ingest pipeline never renders a
  hash — it only stores and compares. `lower(hex(...))` is a query-layer
  concern.
- **Matches what consensus hashes are.** `dsha256` produces 32 bytes; storing
  them verbatim means no endianness gymnastics at the parse boundary.

The price is that **display order is reversed** relative to wire order. Bitcoin
conventionally shows hashes byte-reversed, so:

```sql
-- render a stored hash for display
lower(hex(reverse(txid))) AS txid_hex

-- bind a display hash as a predicate (reverse it offline, then unhex)
WHERE txid = unhex('<byte-reversed 64 hex chars>')
```

The `blocks_by_hash` projection and `transactions_by_txid` index are the point
lookups; they store the same wire-order bytes, so a hash literal must be
reversed before binding. This is the single most common "why does this return
nothing?" mistake — see the [query cookbook](query-cookbook.md#helper-expressions).

## Addresses as `(script_type, address_hash)`

Address-keyed data is stored as a **pair**: a `script_type UInt8` and an
`address_hash FixedString(32)` holding the 20- or 32-byte program
right-padded with zeros to 32 bytes. No base58 or bech32 strings are stored.

Why:

- **Encoding-agnostic.** A hash160 is a hash160 whether it is displayed as
  P2PKH or wrapped in P2SH; a 32-byte program is the same bytes for P2WSH and
  P2TR. Storing the program once means a query by address does not care how
  the client spelled it.
- **Fixed-size keys again.** A 32-byte key is `Copy` and stack-allocated; a
  variable-length bech32 string is neither.
- **The script type is needed anyway.** Grouping outputs by script type and
  excluding `OP_RETURN` are core queries, and the type is free at parse time.

The price: a hash160 address occupies 32 bytes even though it needs 20, so 12
trailing zero bytes are "wasted" in every address index. That is deliberate —
fixed-size keys beat compact keys on the ingest path — but you must remember
the padding when binding a literal:

```sql
-- hash160 62e9...f18 as a 32-byte value (20 bytes + 12 zero bytes)
WHERE address_hash = unhex('62e907b15cbf27d5425399ebf6f0fb50ebb88f1800000000000000000000000000')
```

Rendering and parsing (base58check, bech32/bech32m, witness-unknown versions)
live in `bidx-serve` and are tested against BIP173/BIP350 and the genesis
address — a silent codec bug would corrupt every address-keyed query, so the
codec is the one place strings exist.

## Canonical transaction identity

A transaction's identity in this schema is **`(height, tx_index)`**, not `txid`.

Why: txids are **not unique** in early Bitcoin. BIP30 duplicate coinbases exist
at heights 91812/91817 and 91842/91880 — the same txid appears twice on the
main chain. A schema keyed on `txid` alone would silently merge two distinct
transactions. `(height, tx_index)` is unique by construction.

`transactions_by_txid` exists as a **point-lookup helper** for the common case
("give me the tx with this txid"), but it stores `(txid, height, tx_index)` so
the caller can disambiguate. Any query that must be exact should join through
it to the base table rather than trusting `txid` alone.

## `spends` is a precomputed join

The `spends` table is one row per input, enriched with the spent output's
`value_sat`, `script_type`, `address_hash`, and creation `height`.

Why store it rather than join at query time: **the ingest pipeline already had
to resolve every input's previous output to compute the fee.** The UTXO store
looks up the prevout during `apply_block`; persisting what it found is nearly
free, and it turns a query-time join (expensive, and needing the UTXO set
present) into a single-table scan. Fees, address history, coin flow, lifespan,
and coin-days-destroyed all become cheap because this join is materialized.

## `OP_RETURN` is excluded from address indexes

Every address-keyed index (`address_history`, `utxos_by_address`,
`address_stats`, `blocks_by_miner`, `scripthash_map`) filters out `OP_RETURN`
outputs (script type 7).

Why: an `OP_RETURN` output carries no spendable value and its `address_hash`
is all zeros — including it would pollute every address aggregate with a fake
"zero address" that holds provably unspendable value, and would inflate UTXO
counts. The exclusion is applied in the materialized views' `WHERE script_type
!= 7`, so the base `outputs` table still records the output faithfully for
anyone who wants it.

## Partitioning by 10,000-block ranges

Base tables are partitioned by `intDiv(height, 10000)`.

Why: it makes **tip-range maintenance cheap**. To retract rows for a reorg
below the floor, or to reload a range after a re-parse, you drop or replace
whole 10,000-block partitions instead of deleting rows. The granularity
matches the Parquet part-file size (`--blocks-per-part`, default 10000), so a
part file and a partition line up — a re-run of a range is idempotent at both
the file and partition level.

The derived index tables that are not range-scanned (`transactions_by_txid`,
`outputs_by_address`, `spends_by_*`, `address_history`, `utxos_by_address`,
`blocks_by_miner`, `scripthash_map`) are unpartitioned and keyed by their
lookup prefix instead.

## Projections, skip indexes, and materialized views

Three mechanisms, used for three different jobs:

- **Projections** (`blocks_by_hash`, `blocks_by_time`,
  `outputs_by_script_type`, `spends_by_spent_height`) are alternate sort orders
  stored *inside* the base table. ClickHouse selects them automatically for a
  matching query — you never reference them in SQL. They cost no extra write
  path: they are built from the same insert.
- **Skip indexes** (`idx_blocks_time`, `idx_outputs_value`, `idx_txs_fee`) are
  MinMax indexes that let ClickHouse prune granules for a **range predicate**.
  Keep the predicate bare (`time BETWEEN …`, `value_sat >= …`, `fee > …`) so
  the index can be used.
- **Materialized views** feed the derived index tables from inserts into the
  base tables. This is the key design choice: **loading the five base entities
  keeps the whole index catalogue in sync with no extra write path.** See
  [schema.md](schema.md) for the full MV list.

## Derived index tables

The twelve derived tables exist because ClickHouse's sort key is the only
cheap access path. A base table's key is `(height, …)`, which is ideal for
range scans but useless for "find the rows for this txid/address/prevout".
Each derived table re-keys the same data by the lookup you actually make:

| Lookup | Table | Key prefix |
|--------|-------|------------|
| txid → position | `transactions_by_txid` | `txid` |
| prevout → spenders | `inputs_by_prev_output` | `(prev_txid, prev_vout)` |
| address → funding | `outputs_by_address` | `address_hash` |
| outpoint → spender | `spends_by_spent_outpoint` | `(spent_txid, spent_vout)` |
| address → spends | `spends_by_address` | `spent_address_hash` |
| address → signed history | `address_history` | `(address_hash, height)` |
| address → live UTXOs | `utxos_by_address` | `(address_hash, txid, output_index)` |
| address → counters | `address_stats` | `address_hash` |
| height → UTXO snapshot | `coinstats_by_height` | `height` |
| miner → blocks | `blocks_by_miner` | `(miner_hash, height)` |
| address prefix search | `address_prefix` | `address` |
| scripthash → address | `scripthash_map` | `scripthash` |

A query without a filter on one of these prefixes scans everything. This is the
tension the schema accepts: extra storage and extra MV writes in exchange for
O(1)-ish lookups on every surface the serving layer exposes.

## `FINAL` and merge-engine semantics

Three derived tables use non-`MergeTree` engines and need care:

- **`utxos_by_address`** is a `CollapsingMergeTree(sign)`. Fundings insert
  `sign = +1`, spends insert `sign = -1`. Read with `FINAL` and keep `sign = 1`
  to get unspent outputs.
- **`address_stats`** is a `SummingMergeTree` (see the note below).
- **`scripthash_map`** is a `ReplacingMergeTree`; `FINAL` dedupes to the latest
  row per `scripthash`.

`FINAL` forces a merge on read and is expensive, so use it only on these tables
and only when you need deduplicated rows. `SummingMergeTree` is the exception:
because it sums the counter columns, a plain `sum(funded_sum)` is already
correct without `FINAL`.

> **Why `SummingMergeTree`, not `AggregatingMergeTree`.** `address_stats`
> originally used `AggregatingMergeTree` over **plain** counter columns
> (`funded_sum UInt64`, …), not `AggregateFunction` state columns. That engine
> keeps only **one row per sort key** during a background merge, so an address
> funded more than once collapsed to a single row and its counters
> under-counted (a re-merge could even discard a spend). Confirmed against
> ClickHouse 24.8: `(addr, 10)` and `(addr, 20)` collapse to `(addr, 10)`, and
> `OPTIMIZE TABLE … FINAL` does not recover the lost row. The columns are all
> sums/counts, so `SummingMergeTree` — which sums plain columns correctly
> across merges — is both the correct engine and the cheaper one to read.
> Schema version 2 migrates an existing database: it rebuilds the table from
> `outputs`/`spends` and swaps it in (`bidx migrate`); a fresh `init-db` gets
> the right engine directly. See [schema.md](schema.md#address_stats).

## Why Parquet in the middle

The pipeline writes zstd Parquet intermediates and bulk-loads them with
ClickHouse's `file()` table function rather than inserting rows one at a time.

Why: the expensive part of indexing is the load, not the parse. Parsing is
embarrassingly parallel; inserting row-by-row through a client would serialize
that work behind a network round-trip. Columnar Parquet lets ClickHouse's
vectorised reader ingest each entity at once, and the five entity part files
map cleanly onto the five base tables. The materialized views then rebuild
every derived index from those bulk inserts, so the bulk path and the live path
share one write model.
