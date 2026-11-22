# Query cookbook

Practical ClickHouse recipes against the `bitcoin` database. Every example
assumes the schema described in [schema.md](schema.md) — read the
[Conventions](schema.md#conventions) section first, because two rules explain
almost every "why does this return nothing?" moment:

- **Hashes are raw wire-order bytes.** Match a display hash with
  `unhex('<reversed hex>')`; render one with `hex(reverse(col))`.
- **Addresses are `(script_type, address_hash)` pairs.** A hash160 lives in the
  low 20 bytes of the 32-byte `address_hash`.

Throughout, replace `<db>` with your database name (default `bitcoin`) and the
`unhex(...)` literals with your actual bytes.

## Contents

- [Helper expressions](#helper-expressions)
- [Blocks](#blocks)
- [Transactions](#transactions)
- [Inputs and outputs](#inputs-and-outputs)
- [Spends](#spends)
- [Addresses](#addresses)
- [UTXO set](#utxo-set)
- [Miners](#miners)
- [Cross-table joins](#cross-table-joins)
- [Performance notes](#performance-notes)

## Helper expressions

```sql
-- Display a stored hash (wire order) as conventional hex:
lower(hex(reverse(hash))) AS hash_hex

-- Bind a display hash as a column value (reverse the hex, then unhex):
WHERE txid = unhex(reverse('000000000019d6689c085ae165831e93...'))
-- In practice you reverse offline and inline the reversed hex:
WHERE txid = unhex('<byte-reversed 64 hex chars>')
```

A P2PKH `address_hash` is the 20-byte hash160 followed by 12 zero bytes:

```sql
-- hash160 62e907b1...f18 as a 32-byte value:
WHERE address_hash = unhex('62e907b15cbf27d5425399ebf6f0fb50ebb88f1800000000000000000000000000')
```

## Blocks

### Tip height and tip hash

```sql
SELECT max(height) AS tip FROM bitcoin.blocks;

SELECT lower(hex(reverse(hash))) AS tip_hash
FROM bitcoin.blocks
ORDER BY height DESC LIMIT 1;
```

### A block by height

```sql
SELECT
  height,
  lower(hex(reverse(hash)))        AS hash,
  lower(hex(reverse(prev_hash)))   AS prev_hash,
  lower(hex(reverse(merkle_root))) AS merkle_root,
  toDateTime(time)                 AS mined_at,
  tx_count, size, weight, total_fee, coinbase_value
FROM bitcoin.blocks
WHERE height = 840000;
```

### A block by hash

The `blocks_by_hash` projection makes this a point lookup.

```sql
SELECT height, lower(hex(reverse(hash))) AS hash, tx_count, total_fee
FROM bitcoin.blocks
WHERE hash = unhex('<byte-reversed block hash>');
```

### Transaction ids in a block, in order

```sql
SELECT tx_index, lower(hex(reverse(txid))) AS txid
FROM bitcoin.transactions
WHERE height = 840000
ORDER BY tx_index;
```

### Blocks in a date range

The `idx_blocks_time` minmax skip index and the `blocks_by_time` projection
both serve this.

```sql
SELECT height, toDateTime(time) AS mined_at, tx_count, size
FROM bitcoin.blocks
WHERE time BETWEEN toUnixTimestamp('2024-01-01 00:00:00')
               AND toUnixTimestamp('2024-01-02 00:00:00')
ORDER BY height;
```

### Blocks per day over a range

```sql
SELECT toDate(toDateTime(time)) AS day, count() AS blocks, sum(tx_count) AS txs
FROM bitcoin.blocks
WHERE height BETWEEN 800000 AND 840000
GROUP BY day ORDER BY day;
```

## Transactions

### A transaction by txid (position + fee)

Join the point-lookup index to the base table for the full row. The
`(height, tx_index)` pair is the canonical identity.

```sql
SELECT
  x.height, x.tx_index, x.version, x.size, x.weight,
  x.fee, x.fee_rate, x.input_count, x.output_count, x.has_witness
FROM bitcoin.transactions_by_txid t
INNER JOIN bitcoin.transactions x
  ON t.txid = x.txid AND t.height = x.height AND t.tx_index = x.tx_index
WHERE t.txid = unhex('<byte-reversed txid>');
```

### Highest-fee transactions in a block

```sql
SELECT tx_index, lower(hex(reverse(txid))) AS txid, fee, fee_rate
FROM bitcoin.transactions
WHERE height = 840000 AND tx_index != 0   -- skip coinbase
ORDER BY fee DESC LIMIT 10;
```

### Fee distribution for a height range

`fee_rate` is the materialized `sat/vB` column (`fee * 4 / weight`).

```sql
SELECT
  quantile(0.50)(fee_rate) AS p50,
  quantile(0.90)(fee_rate) AS p90,
  quantile(0.99)(fee_rate) AS p99,
  max(fee_rate)            AS max_sat_vb
FROM bitcoin.transactions
WHERE height BETWEEN 839000 AND 840000 AND tx_index != 0;
```

### Transaction size/weight histograms

```sql
SELECT
  intDiv(weight, 1000) * 1000 AS weight_band,
  count()                     AS txs
FROM bitcoin.transactions
WHERE height BETWEEN 839000 AND 840000
GROUP BY weight_band ORDER BY weight_band;
```

## Inputs and outputs

### Outputs of a transaction

```sql
SELECT
  output_index, value_sat, script_type,
  lower(hex(reverse(address_hash))) AS address_hash_hex
FROM bitcoin.outputs
WHERE txid = unhex('<byte-reversed txid>')
ORDER BY output_index;
```

### Inputs of a transaction (what it spends)

```sql
SELECT
  input_index, prev_vout,
  lower(hex(reverse(prev_txid))) AS prev_txid,
  script_sig_len, sequence, witness_items, witness_bytes, is_coinbase
FROM bitcoin.inputs
WHERE txid = unhex('<byte-reversed txid>')
ORDER BY input_index;
```

### Large outputs in a block (value-band filter)

The `idx_outputs_value` minmax skip index prunes granules.

```sql
SELECT
  output_index, value_sat, script_type,
  lower(hex(reverse(txid))) AS txid
FROM bitcoin.outputs
WHERE height = 840000 AND value_sat >= 100000000   -- >= 1 BTC
ORDER BY value_sat DESC;
```

### Outputs grouped by script type

The `outputs_by_script_type` projection serves this over time.

```sql
SELECT script_type, count() AS outputs, sum(value_sat) AS total_sat
FROM bitcoin.outputs
WHERE height BETWEEN 800000 AND 840000
GROUP BY script_type ORDER BY outputs DESC;
```

## Spends

The `spends` table is the derived join: one row per input, enriched with the
spent output's value, script type, address hash, and creation height.

### Is an output spent? (outspend lookup)

`spends_by_spent_outpoint` answers this directly.

```sql
SELECT
  spent_vout,
  lower(hex(reverse(spending_txid))) AS spending_txid,
  height AS spent_at_height
FROM bitcoin.spends_by_spent_outpoint
WHERE spent_txid = unhex('<byte-reversed txid>')
ORDER BY spent_vout;
```

### Every output of a tx with its spend status

```sql
SELECT
  o.output_index,
  o.value_sat,
  s.spending_txid != '' AS spent,
  lower(hex(reverse(s.spending_txid))) AS spending_txid
FROM bitcoin.outputs o
LEFT JOIN bitcoin.spends_by_spent_outpoint s
  ON s.spent_txid = o.txid AND s.spent_vout = o.output_index
WHERE o.txid = unhex('<byte-reversed txid>')
ORDER BY o.output_index;
```

### Total value spent at a height (coin flow)

```sql
SELECT sum(spent_value_sat) AS spent_sat, count() AS inputs
FROM bitcoin.spends
WHERE height = 840000;
```

### How long outputs sat before being spent (lifespan)

The `spends_by_spent_height` projection orders by `(spent_height, height)`.

```sql
SELECT
  (height - spent_height)         AS lifespan_blocks,
  count()                         AS spends,
  sum(spent_value_sat)            AS value_sat
FROM bitcoin.spends
WHERE height BETWEEN 839000 AND 840000
GROUP BY lifespan_blocks
ORDER BY lifespan_blocks;
```

### Coin-days-destroyed (CDD) in a range

CDD = value (in BTC) x age (in days) for each spent output.

```sql
SELECT
  sum(spent_value_sat / 1e8 * (height - spent_height) / 144.0) AS cdd
FROM bitcoin.spends
WHERE height BETWEEN 839000 AND 840000;
```

## Addresses

Every address-keyed index is derived from `outputs`/`spends` and excludes
`OP_RETURN`.

### Address balance and stats (Esplora `chain_stats`)

`address_stats` is a `SummingMergeTree`; combine its rows with `sum()`. The
engine sums the counter columns across merges, so no `FINAL` is needed.
`funded_sum - spent_sum` is the balance.

```sql
SELECT
  sum(funded_count)  AS funded_txo_count,
  sum(funded_sum)    AS funded_txo_sum,
  sum(spent_count)   AS spent_txo_count,
  sum(spent_sum)     AS spent_txo_sum,
  sum(history_count) AS tx_count,
  sum(funded_sum) - sum(spent_sum) AS balance_sat
FROM bitcoin.address_stats
WHERE address_hash = unhex('<32-byte address hash>');
```

The signed `address_history` yields the same balance and is handy when you also
want the individual events:

```sql
-- Balance from the signed history (fundings positive, spends negative):
SELECT sum(value_sat) AS balance_sat
FROM bitcoin.address_history
WHERE address_hash = unhex('<32-byte address hash>');
```

### Unspent outputs (UTXOs) for an address

`utxos_by_address` is a `CollapsingMergeTree`: use `FINAL` and keep `sign = 1`.

```sql
SELECT
  lower(hex(reverse(txid))) AS txid,
  output_index,
  value_sat,
  height
FROM bitcoin.utxos_by_address FINAL
WHERE address_hash = unhex('<32-byte address hash>') AND sign = 1
ORDER BY height DESC;
```

### Full funding + spending history

`address_history` is signed: positive for funding, negative for spending.

```sql
SELECT
  height, tx_index, lower(hex(reverse(txid))) AS txid,
  value_sat,
  kind            -- 0 = funding, 1 = spending
FROM bitcoin.address_history
WHERE address_hash = unhex('<32-byte address hash>')
ORDER BY height, tx_index;
```

### Funding history only (outputs paying the address)

```sql
SELECT
  height, output_index, value_sat, script_type,
  lower(hex(reverse(txid))) AS txid
FROM bitcoin.outputs_by_address
WHERE address_hash = unhex('<32-byte address hash>')
ORDER BY height DESC;
```

### Spend history only (outputs the address spent)

```sql
SELECT
  height, spent_vout, spent_value_sat, spent_height,
  lower(hex(reverse(spent_txid))) AS spent_txid
FROM bitcoin.spends_by_address
WHERE spent_address_hash = unhex('<32-byte address hash>')
ORDER BY height DESC;
```

### Net flow per transaction for an address

```sql
SELECT
  lower(hex(reverse(txid))) AS txid,
  any(height)               AS height,
  sum(value_sat)            AS net_sat
FROM bitcoin.address_history
WHERE address_hash = unhex('<32-byte address hash>')
GROUP BY txid
ORDER BY height;
```

### Address prefix search

`address_prefix` is the one string-keyed table; it is backfilled by
`bidx index rebuild address_prefix` (Rust-rendered, so it takes `--network`).

```sql
SELECT address, script_type
FROM bitcoin.address_prefix
WHERE startsWith(address, 'bc1q')
ORDER BY address LIMIT 100;
```

### Resolve an Electrum scripthash

Electrum sends `sha256(scriptPubKey)` in *display* order; the stored
`scripthash` is internal order. Reverse the incoming hex before binding.

```sql
SELECT script_type, lower(hex(reverse(address_hash))) AS address_hash_hex
FROM bitcoin.scripthash_map
WHERE scripthash = unhex('<byte-reversed scripthash>');
```

## UTXO set

### UTXO-set snapshot at a checkpoint

`coinstats_by_height` records the cumulative UTXO count and value at each
10,000-block checkpoint. Backfilled by `bidx index rebuild coinstats`.

```sql
SELECT height, utxo_count, total_value_sat
FROM bitcoin.coinstats_by_height
ORDER BY height DESC LIMIT 10;
```

### Current UTXO count and total value

`FINAL` collapses the collapsing rows; `sign = 1` keeps survivors.

```sql
SELECT
  countIf(sign = 1)          AS utxo_count,
  sumIf(value_sat, sign = 1) AS total_value_sat
FROM bitcoin.utxos_by_address FINAL;
```

> This scans the whole UTXO view. For a cheap tip snapshot, read the newest
> row of `coinstats_by_height` instead and accept 10k-block granularity.

## Miners

### Blocks by a miner (coinbase payout address)

`blocks_by_miner` keys on the coinbase output address hash — a robust miner
proxy with no name heuristics.

```sql
SELECT
  height, coinbase_value,
  lower(hex(reverse(miner_hash))) AS miner_hash_hex
FROM bitcoin.blocks_by_miner
WHERE miner_hash = unhex('<32-byte address hash>')
ORDER BY height;
```

### Most active miners in a range

```sql
SELECT
  lower(hex(reverse(miner_hash))) AS miner,
  count()                         AS blocks,
  sum(coinbase_value)             AS reward_sat
FROM bitcoin.blocks_by_miner
WHERE height BETWEEN 839000 AND 840000
GROUP BY miner
ORDER BY blocks DESC LIMIT 20;
```

## Cross-table joins

### Follow the money: trace an output forward

```sql
SELECT
  lower(hex(reverse(s.spending_txid))) AS spending_txid,
  s.height AS spent_at,
  o.value_sat
FROM bitcoin.outputs o
JOIN bitcoin.spends s
  ON s.spent_txid = o.txid AND s.spent_vout = o.output_index
WHERE o.txid = unhex('<byte-reversed txid>') AND o.output_index = 0;
```

### Fee revenue per block over a range

```sql
SELECT height, total_fee, coinbase_value,
       total_fee + coinbase_value AS miner_revenue_sat
FROM bitcoin.blocks
WHERE height BETWEEN 839000 AND 840000
ORDER BY height;
```

## Performance notes

- **Projections are automatic.** ClickHouse picks `blocks_by_hash`,
  `blocks_by_time`, `outputs_by_script_type`, `spends_by_spent_height` on its
  own — don't reference them in SQL.
- **Skip indexes** (`idx_blocks_time`, `idx_outputs_value`, `idx_txs_fee`)
  prune granules for range predicates. Keep `time`, `value_sat` and `fee`
  comparisons as bare range predicates to hit them.
- **Always filter on a sort-key prefix** where possible: `height` for base
  tables, `address_hash`/`spent_address_hash` for address indexes, `txid` for
  `transactions_by_txid`. A query without one scans everything.
- **`FINAL` is expensive.** Only use it on the merge-engine tables that need
  collapsing (`utxos_by_address`, `address_stats`, `scripthash_map`), and only
  when you need deduplicated rows.
- **`address_hash` is 32 bytes**, so a hash160 address costs 12 wasted trailing
  zero bytes in every index. That is deliberate (fixed-size keys are `Copy` and
  go on the stack); just don't forget the zero padding when binding a literal.
