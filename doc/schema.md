# ClickHouse schema reference

This document describes every SQL table and field that a user can query in the
`bitcoin` database (the name is configurable via `bidx init-db --database`, but
`bitcoin` is the default and is used throughout this document).

It is generated from the authoritative DDL in
`crates/bidx-loader/src/schema.rs`. If the two ever disagree, the Rust source
wins — see [Keeping this document honest](#keeping-this-document-honest).

## Contents

- [Conventions](#conventions)
- [Base entity tables](#base-entity-tables)
  - [`blocks`](#blocks)
  - [`transactions`](#transactions)
  - [`inputs`](#inputs)
  - [`outputs`](#outputs)
  - [`spends`](#spends)
  - [`witnesses` (optional)](#witnesses-optional)
- [Index tables](#index-tables)
  - [`transactions_by_txid`](#transactions_by_txid)
  - [`inputs_by_prev_output`](#inputs_by_prev_output)
  - [`outputs_by_address`](#outputs_by_address)
  - [`spends_by_spent_outpoint`](#spends_by_spent_outpoint)
  - [`spends_by_address`](#spends_by_address)
  - [`address_history`](#address_history)
  - [`utxos_by_address`](#utxos_by_address)
  - [`address_stats`](#address_stats)
  - [`coinstats_by_height`](#coinstats_by_height)
  - [`blocks_by_miner`](#blocks_by_miner)
  - [`address_prefix`](#address_prefix)
  - [`scripthash_map`](#scripthash_map)
- [Bookkeeping](#bookkeeping)
  - [`meta_schema`](#meta_schema)
- [In-place indexes on base tables](#in-place-indexes-on-base-tables)
- [Materialized views](#materialized-views)
- [Enumerations](#enumerations)
  - [Script types](#script-types)
  - [Address history kinds](#address-history-kinds)
- [Querying hashes and addresses](#querying-hashes-and-addresses)
- [Keeping this document honest](#keeping-this-document-honest)

## Conventions

These hold across every table and are the most common source of "why did my
query return nothing?" surprises.

- **Hashes are raw bytes.** Every hash column is `FixedString(32)` holding the
  32 raw wire-order bytes, *not* hex text. To match a display hex txid/block
  hash you must reverse the byte order and `unhex` it. See
  [Querying hashes and addresses](#querying-hashes-and-addresses).
- **Addresses are never stored as strings** (except `address_prefix`). Address
  data lives as a `(script_type UInt8, address_hash FixedString(32))` pair.
  `address_hash` is the 20- or 32-byte program right-padded with zeros to 32
  bytes.
- **`OP_RETURN` outputs are excluded** from every address-keyed index
  (`address_history`, `utxos_by_address`, `address_stats`, `blocks_by_miner`,
  `scripthash_map`). Their `address_hash` is all zeros and they carry no
  spendable value.
- **The canonical transaction identity is `(height, tx_index)`**, not `txid`.
  Early BIP30-era txids are not unique (heights 91812/91817 and 91842/91880).
- **Amounts are integer satoshis** (`UInt64`), never floating point. Fee rate
  is the only derived value and is `UInt32` sat/vB.
- **Block heights are `UInt32`.** Base tables are partitioned in 10,000-block
  ranges (`intDiv(height, 10000)`), which makes tip-range partition drops
  cheap.
- **`FINAL`** is required when reading `CollapsingMergeTree` / `ReplacingMergeTree`
  tables (`utxos_by_address`, `scripthash_map`, `meta_schema`) if you want
  collapsed/deduplicated rows. `address_stats` is a `SummingMergeTree`, so its
  `sum()`-based reads are correct without `FINAL`.

## Base entity tables

The five core tables are bulk-loaded from Parquet (one part-file set per
entity). They are the source of truth; every index table below is derived from
them.

### `blocks`

One row per main-chain block, ordered by `height`.

| Column | Type | Description |
|--------|------|-------------|
| `height` | `UInt32` | Block height. `0` is genesis. Sort key. |
| `hash` | `FixedString(32)` | Block header hash, raw wire order. |
| `prev_hash` | `FixedString(32)` | Parent block hash, raw wire order. |
| `merkle_root` | `FixedString(32)` | Merkle root from the header. |
| `time` | `UInt32` | Header timestamp (Unix seconds). |
| `bits` | `UInt32` | Compact-format difficulty target. |
| `nonce` | `UInt32` | Header nonce. |
| `version` | `Int32` | Header version (signed). |
| `tx_count` | `UInt32` | Number of transactions in the block. |
| `size` | `UInt32` | Serialized block size in bytes. |
| `weight` | `UInt32` | BIP141 block weight (size×3 + stripped size). |
| `total_fee` | `UInt64` | Sum of all non-coinbase tx fees, in satoshis. |
| `coinbase_value` | `UInt64` | Coinbase output value, in satoshis. |

- Engine: `MergeTree`, `PARTITION BY intDiv(height, 10000)`, `ORDER BY height`.
- Projections: `blocks_by_hash` (order by `hash`), `blocks_by_time` (order by
  `time`).
- Skip index: `idx_blocks_time` (minmax on `time`, granularity 4).

### `transactions`

One row per transaction, ordered by `(height, tx_index)`.

| Column | Type | Description |
|--------|------|-------------|
| `height` | `UInt32` | Block height containing the transaction. |
| `tx_index` | `UInt32` | Position within the block (0 = coinbase). |
| `txid` | `FixedString(32)` | Transaction id, raw wire order. |
| `version` | `Int32` | Transaction version. |
| `locktime` | `UInt32` | nLockTime. |
| `size` | `UInt32` | Serialized size in bytes. |
| `weight` | `UInt32` | BIP141 transaction weight. |
| `fee` | `UInt64` | Fee in satoshis (`sum(inputs) - sum(outputs)`). |
| `input_count` | `UInt32` | Number of inputs. |
| `output_count` | `UInt32` | Number of outputs. |
| `has_witness` | `UInt8` | 1 if any input carries witness data, else 0. |
| `fee_rate` | `UInt32` | **Materialized** — sat/vB = `(fee * 4) / weight`, `0` when `weight = 0`. Not insertable. |

- Engine: `MergeTree`, `PARTITION BY intDiv(height, 10000)`,
  `ORDER BY (height, tx_index)`.
- Skip index: `idx_txs_fee` (minmax on `fee`, granularity 4).

### `inputs`

One row per transaction input, ordered by `(height, tx_index, input_index)`.

| Column | Type | Description |
|--------|------|-------------|
| `height` | `UInt32` | Block height containing the spending transaction. |
| `tx_index` | `UInt32` | Position of the spending transaction in the block. |
| `input_index` | `UInt32` | Position of the input within the transaction. |
| `txid` | `FixedString(32)` | Txid of the *spending* transaction. |
| `prev_txid` | `FixedString(32)` | Txid of the output being spent. |
| `prev_vout` | `UInt32` | Output index being spent. |
| `script_sig_len` | `UInt32` | Length of the scriptSig in bytes. |
| `sequence` | `UInt32` | nSequence. |
| `witness_items` | `UInt32` | Number of witness stack items. |
| `witness_bytes` | `UInt32` | Total witness payload size in bytes. |
| `is_coinbase` | `UInt8` | 1 if this is the coinbase input, else 0. |

- Engine: `MergeTree`, `PARTITION BY intDiv(height, 10000)`,
  `ORDER BY (height, tx_index, input_index)`.

### `outputs`

One row per transaction output, ordered by `(height, tx_index, output_index)`.
This is the table every address-keyed index is derived from.

| Column | Type | Description |
|--------|------|-------------|
| `height` | `UInt32` | Block height containing the transaction. |
| `tx_index` | `UInt32` | Position of the transaction in the block. |
| `output_index` | `UInt32` | Position of the output within the transaction. |
| `txid` | `FixedString(32)` | Txid of the creating transaction. |
| `value_sat` | `UInt64` | Output value in satoshis. |
| `script_pubkey_len` | `UInt32` | Length of the scriptPubKey in bytes. |
| `script_type` | `UInt8` | Classified script type. See [Script types](#script-types). |
| `address_hash` | `FixedString(32)` | 20- or 32-byte program, zero-padded to 32. Zeros for `OP_RETURN`. |

- Engine: `MergeTree`, `PARTITION BY intDiv(height, 10000)`,
  `ORDER BY (height, tx_index, output_index)`.
- Projection: `outputs_by_script_type` (order by `(script_type, height)`).
- Skip index: `idx_outputs_value` (minmax on `value_sat`, granularity 4).

### `spends`

One row per input, enriched with the spent output's data. This is the derived
join that powers fees, balances and coin-flow queries — everything you would
otherwise need to look up in `outputs` by outpoint is already here.

| Column | Type | Description |
|--------|------|-------------|
| `height` | `UInt32` | Block height containing the *spending* transaction. |
| `spending_tx_index` | `UInt32` | Position of the spending transaction in its block. |
| `spending_input_index` | `UInt32` | Position of the spending input within the transaction. |
| `spending_txid` | `FixedString(32)` | Txid of the transaction doing the spending. |
| `spent_txid` | `FixedString(32)` | Txid of the output that was spent. |
| `spent_vout` | `UInt32` | Output index that was spent. |
| `spent_value_sat` | `UInt64` | Value of the spent output, in satoshis. |
| `spent_script_type` | `UInt8` | Script type of the spent output. |
| `spent_address_hash` | `FixedString(32)` | Address hash of the spent output. |
| `spent_height` | `UInt32` | Height at which the spent output was created. |

- Engine: `MergeTree`, `PARTITION BY intDiv(height, 10000)`,
  `ORDER BY (height, spending_tx_index, spending_input_index)`.
- Projection: `spends_by_spent_height` (order by `(spent_height, height)`) —
  serves output-lifespan / coin-days-destroyed queries.

### `witnesses` (optional)

One row per witness stack element, written **only** when `--capture-witness` is
passed to `bidx parse` or `bidx live`. Keeping raw witness payloads out of the
hot `inputs`/`transactions` tables by default is deliberate: witnesses are the
largest and least-queried part of a transaction, and the vast majority of
queries never need the payload, only its size (`inputs.witness_bytes`). The
table is probed before use, so a database loaded without `--capture-witness`
still works.

| Column | Type | Description |
|--------|------|-------------|
| `height` | `UInt32` | Block height. |
| `tx_index` | `UInt32` | Position of the transaction in its block. |
| `input_index` | `UInt32` | Position of the input within the transaction. |
| `item_index` | `UInt32` | Position of this element in the witness stack. |
| `item_len` | `UInt32` | Element length in bytes. |
| `item_bytes` | `String` | Raw witness element bytes. |

- Engine: `MergeTree`, `PARTITION BY intDiv(height, 10000)`,
  `ORDER BY (height, tx_index, input_index, item_index)`.

## Index tables

Every index table is auto-maintained by a `MATERIALIZED VIEW` over a base table
(see [Materialized views](#materialized-views)), so `bidx init-db` + `bidx load`
keeps them in sync with no extra write path. Three indexes
(`coinstats_by_height`, `scripthash_map`, `address_prefix`) are backfills and
are refreshed by `bidx index rebuild <name>`; `address_prefix` additionally
needs a Rust-side address renderer (see below).

### `transactions_by_txid`

Point lookup from a txid to its confirmation position.

| Column | Type | Description |
|--------|------|-------------|
| `txid` | `FixedString(32)` | Transaction id. Sort key. |
| `height` | `UInt32` | Block height. |
| `tx_index` | `UInt32` | Position in the block. |

- Engine: `MergeTree`, `ORDER BY txid`.

### `inputs_by_prev_output`

Maps a spent outpoint to the input that spent it (the `inputs` table re-keyed by
`prev_txid`/`prev_vout`).

| Column | Type | Description |
|--------|------|-------------|
| `prev_txid` | `FixedString(32)` | Txid of the spent output. Sort key (with `prev_vout`). |
| `prev_vout` | `UInt32` | Spent output index. Sort key (with `prev_txid`). |
| `height` | `UInt32` | Height of the spending transaction. |
| `tx_index` | `UInt32` | Position of the spending transaction in its block. |
| `input_index` | `UInt32` | Position of the spending input. |
| `txid` | `FixedString(32)` | Txid of the spending transaction. |

- Engine: `MergeTree`, `ORDER BY (prev_txid, prev_vout)`.

### `outputs_by_address`

Funding history: every output paying an address.

| Column | Type | Description |
|--------|------|-------------|
| `address_hash` | `FixedString(32)` | Address program. Sort key (with the rest). |
| `script_type` | `UInt8` | Script type of the output. |
| `height` | `UInt32` | Height of the creating transaction. |
| `tx_index` | `UInt32` | Position of the creating transaction in its block. |
| `output_index` | `UInt32` | Output position. |
| `txid` | `FixedString(32)` | Creating transaction id. |
| `value_sat` | `UInt64` | Output value in satoshis. |

- Engine: `MergeTree`,
  `ORDER BY (address_hash, height, tx_index, output_index)`.

### `spends_by_spent_outpoint`

Outpoint → the input that spent it. Answers Esplora `/outspends` and Core's
`gettxspendingprevout`.

| Column | Type | Description |
|--------|------|-------------|
| `spent_txid` | `FixedString(32)` | Spent output's txid. Sort key (with `spent_vout`). |
| `spent_vout` | `UInt32` | Spent output index. Sort key (with `spent_txid`). |
| `height` | `UInt32` | Height of the spending transaction. |
| `spending_tx_index` | `UInt32` | Position of the spending transaction in its block. |
| `spending_input_index` | `UInt32` | Position of the spending input. |
| `spending_txid` | `FixedString(32)` | Txid of the spending transaction. |

- Engine: `MergeTree`, `ORDER BY (spent_txid, spent_vout)`.

### `spends_by_address`

Spend history: every output an address spent.

| Column | Type | Description |
|--------|------|-------------|
| `spent_address_hash` | `FixedString(32)` | Address program. Sort key (with the rest). |
| `height` | `UInt32` | Height of the spending transaction. |
| `spending_tx_index` | `UInt32` | Position of the spending transaction in its block. |
| `spending_input_index` | `UInt32` | Position of the spending input. |
| `spent_txid` | `FixedString(32)` | Txid of the spent output. |
| `spent_vout` | `UInt32` | Spent output index. |
| `spent_value_sat` | `UInt64` | Value of the spent output, in satoshis. |
| `spent_height` | `UInt32` | Height at which the spent output was created. |

- Engine: `MergeTree`,
  `ORDER BY (spent_address_hash, height, spending_tx_index, spending_input_index)`.

### `address_history`

Unified funding + spending history per address. This is the union of
`outputs_by_address` and `spends_by_address` with a signed value and a `kind`
discriminator.

| Column | Type | Description |
|--------|------|-------------|
| `address_hash` | `FixedString(32)` | Address program. Sort key (with `height`, `tx_index`). |
| `height` | `UInt32` | Height of the event. |
| `tx_index` | `UInt32` | Position of the transaction in its block. |
| `txid` | `FixedString(32)` | Transaction id of the event. |
| `value_sat` | `Int64` | **Signed**: positive when the address is funded, negative when it spends. |
| `kind` | `UInt8` | 0 = funding, 1 = spending. See [Address history kinds](#address-history-kinds). |

- Engine: `MergeTree`, `PARTITION BY intDiv(height, 10000)`,
  `ORDER BY (address_hash, height, tx_index)`.
- Excludes `OP_RETURN`.

### `utxos_by_address`

Per-address live UTXO view. A `CollapsingMergeTree` keyed on `sign`: a funding
row (`+1`) is cancelled by a matching spend row (`-1`), so a query with `FINAL`
yields the UTXO set.

| Column | Type | Description |
|--------|------|-------------|
| `address_hash` | `FixedString(32)` | Address program. Sort key (with `txid`, `output_index`). |
| `script_type` | `UInt8` | Script type of the output. |
| `height` | `UInt32` | Height at which the output was created. |
| `tx_index` | `UInt32` | Position of the creating transaction in its block. |
| `output_index` | `UInt32` | Output position. |
| `txid` | `FixedString(32)` | Creating transaction id. |
| `value_sat` | `UInt64` | Output value in satoshis. |
| `sign` | `Int8` | `+1` funding row, `-1` spend row. Collapse key. |

- Engine: `CollapsingMergeTree(sign)`,
  `ORDER BY (address_hash, txid, output_index)`.
- Query with `FINAL` and `WHERE sign = 1` to get unspent outputs.
- Excludes `OP_RETURN`.

### `address_stats`

Per-address aggregate counters (Esplora `chain_stats`).

| Column | Type | Description |
|--------|------|-------------|
| `address_hash` | `FixedString(32)` | Address program. Sort key. |
| `funded_count` | `UInt64` | Number of funding outputs. |
| `funded_sum` | `UInt64` | Total value received, in satoshis. |
| `spent_count` | `UInt64` | Number of spending inputs. |
| `spent_sum` | `UInt64` | Total value spent, in satoshis. |
| `history_count` | `UInt64` | Total events (fundings + spends). |

- Engine: `SummingMergeTree`, `ORDER BY address_hash`.
- Rows from the fund/spend MVs combine with `sum()` (see `Store::address_stats`
  in `bidx-serve`). `SummingMergeTree` sums the plain counter columns across
  merges, so the totals are correct with or without `FINAL`.
- Excludes `OP_RETURN`.

### `coinstats_by_height`

UTXO-set snapshot at each 10,000-block checkpoint (the Core `coinstatsindex`
analog). **Backfilled** — run `bidx index rebuild coinstats` to (re)populate.

| Column | Type | Description |
|--------|------|-------------|
| `height` | `UInt32` | Checkpoint height (`intDiv(height, 10000) * 10000`). Sort key. |
| `utxo_count` | `UInt64` | UTXO count at that height. |
| `total_value_sat` | `UInt64` | Total UTXO value, in satoshis. |

- Engine: `MergeTree`, `ORDER BY height`.

### `blocks_by_miner`

Coinbase payout address → blocks. The coinbase output address is used as a
miner-identity proxy (no name heuristics).

| Column | Type | Description |
|--------|------|-------------|
| `miner_hash` | `FixedString(32)` | Coinbase output address hash. Sort key (with `height`). |
| `height` | `UInt32` | Block height. |
| `tx_index` | `UInt32` | Transaction index (always 0 — coinbase). |
| `coinbase_value` | `UInt64` | Coinbase output value, in satoshis. |

- Engine: `MergeTree`, `ORDER BY (miner_hash, height)`.
- Excludes `OP_RETURN` coinbases.

### `address_prefix`

Encoded address string → `(script_type, address_hash)`, for prefix search.

**Backfilled in Rust.** ClickHouse has no base58/bech32 encoder, so unlike the
other backfills this one cannot be pure SQL: `bidx index rebuild address_prefix`
truncates the table, streams the distinct `(script_type, address_hash)` pairs out
of `outputs`, renders each address with the serving layer's codec, and inserts
the results in batches. Pass `--network <mainnet|testnet3|regtest>` so the
rendered strings carry the right prefix. The query is
`SELECT DISTINCT script_type, hex(address_hash) FROM outputs WHERE script_type IN
(1, 2, 3, 4, 5, 6, 8)` — `OP_RETURN` (7), `Unknown` (0) and `WitnessUnknown` (9)
have no canonical encoding and are skipped.

| Column | Type | Description |
|--------|------|-------------|
| `address` | `String` | Rendered base58/bech32 address. Sort key. The only string-keyed column in the schema. |
| `script_type` | `UInt8` | Script type. |
| `address_hash` | `FixedString(32)` | Address program. |

- Engine: `MergeTree`, `ORDER BY address`.

### `scripthash_map`

Electrum scripthash → `(script_type, address_hash)`. Electrum clients send
`sha256(scriptPubKey)` (display order) which cannot be inverted, so the serving
layer resolves it here. Populated from `outputs` by an MV; **also backfillable**
via `bidx index rebuild scripthash-map` for databases loaded before the MV
existed.

| Column | Type | Description |
|--------|------|-------------|
| `scripthash` | `FixedString(32)` | `SHA256(scriptPubKey)` in internal byte order. Sort key. |
| `script_type` | `UInt8` | Script type. |
| `address_hash` | `FixedString(32)` | Address program. |

- Engine: `ReplacingMergeTree`, `ORDER BY scripthash`.
- Only script types 2–6 (P2PKH, P2SH, P2WPKH, P2WSH, P2TR) are mapped;
  P2PK, BareMultisig and WitnessUnknown have no reconstructable scriptPubKey
  from `(script_type, address_hash)` and are excluded.

## Bookkeeping

### `meta_schema`

Schema-version bookkeeping. Written by `bidx init-db` and updated by
migrations.

| Column | Type | Description |
|--------|------|-------------|
| `version` | `UInt32` | Applied schema version. Sort key. |
| `applied_at` | `DateTime` | When the version was recorded (default `now()`). |

- Engine: `ReplacingMergeTree(applied_at)`, `ORDER BY version`.
- Current baseline version: `1` (see `SCHEMA_VERSION` in `schema.rs`).

## In-place indexes on base tables

These do not create new tables; they are added to a base table with `ALTER
TABLE ... ADD ... IF NOT EXISTS`.

| Name | Table | Mechanism | Serves |
|------|-------|-----------|--------|
| `blocks_by_hash` | `blocks` | projection (`ORDER BY hash`) | block detail by hash |
| `outputs_by_script_type` | `outputs` | projection (`ORDER BY (script_type, height)`) | type grouping |
| `blocks_by_time` | `blocks` | projection (`ORDER BY time`) | time-ordered blocks |
| `spends_by_spent_height` | `spends` | projection (`ORDER BY (spent_height, height)`) | lifespan / CDD |
| `idx_blocks_time` | `blocks` | minmax skip index on `time`, gran 4 | block range by date |
| `idx_outputs_value` | `outputs` | minmax skip index on `value_sat`, gran 4 | value-band filters |
| `idx_txs_fee` | `transactions` | minmax skip index on `fee`, gran 4 | fee-band filters |
| `fee_rate` | `transactions` | materialized column | fee-rate filter / percentiles |

ClickHouse selects projections automatically, so queries do not need to name
them.

## Materialized views

Each MV copies rows from a base table into an index table. They exist so that
loading the base entities is sufficient to keep the whole index catalogue in
sync.

| Materialized view | Source | Target |
|-------------------|--------|--------|
| `transactions_by_txid_mv` | `transactions` | `transactions_by_txid` |
| `inputs_by_prev_output_mv` | `inputs` | `inputs_by_prev_output` |
| `outputs_by_address_mv` | `outputs` | `outputs_by_address` |
| `spends_by_spent_outpoint_mv` | `spends` | `spends_by_spent_outpoint` |
| `spends_by_address_mv` | `spends` | `spends_by_address` |
| `address_history_fund_mv` | `outputs` | `address_history` (`kind = 0`) |
| `address_history_spend_mv` | `spends` | `address_history` (`kind = 1`) |
| `utxos_fund_mv` | `outputs` | `utxos_by_address` (`sign = +1`) |
| `utxos_spend_mv` | `spends` | `utxos_by_address` (`sign = -1`) |
| `address_stats_fund_mv` | `outputs` | `address_stats` |
| `address_stats_spend_mv` | `spends` | `address_stats` |
| `blocks_by_miner_mv` | `outputs` (coinbase only) | `blocks_by_miner` |
| `scripthash_map_mv` | `outputs` (types 2–6) | `scripthash_map` |

## Enumerations

### Script types

`script_type` / `spent_script_type` is a compact `UInt8` code (see
`ScriptType` in `bidx-core/src/script.rs`).

| Code | Name | Meaning |
|------|------|---------|
| 0 | `unknown` | Unclassifiable script. |
| 1 | `p2pk` | Pay to public key. `address_hash = hash160(pubkey)`. |
| 2 | `p2pkh` | `OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG`. |
| 3 | `p2sh` | `OP_HASH160 <20> OP_EQUAL`. |
| 4 | `p2wpkh` | `OP_0 <20>`. |
| 5 | `p2wsh` | `OP_0 <32>`. |
| 6 | `p2tr` | `OP_1 <32>` (Taproot key-path). `address_hash = x-only pubkey`. |
| 7 | `op_return` | `OP_RETURN` data carrier. `address_hash` = zeros. Excluded from address indexes. |
| 8 | `bare_multisig` | Bare m-of-n multisig. `address_hash = hash160(scriptPubKey)`. |
| 9 | `witness_unknown` | Other witness-version programs. `address_hash = program`. |

### Address history kinds

`address_history.kind` discriminates the two row sources.

| Value | Meaning | Sign of `value_sat` |
|-------|---------|---------------------|
| 0 | Funding (an output to the address) | positive |
| 1 | Spending (an input from the address) | negative |

## Querying hashes and addresses

Hashes are stored as raw bytes in wire order. To match a display hex value,
reverse the bytes and `unhex` them; to render a stored hash as display hex,
`reverse(hex(...))`.

Render a stored txid as conventional hex:

```sql
SELECT height, tx_index, lower(hex(reverse(txid))) AS txid_hex
FROM bitcoin.transactions
WHERE height = 0;
```

Look up a transaction by its display txid:

```sql
SELECT height, tx_index, fee, fee_rate
FROM bitcoin.transactions
WHERE txid = unhex('<64 hex chars, byte-reversed>');
```

Look up an address's outputs by its hash (a P2PKH hash160 lives in the low 20
bytes of the 32-byte `address_hash`):

```sql
SELECT height, hex(reverse(txid)) AS txid_hex, output_index, value_sat
FROM bitcoin.outputs_by_address
WHERE address_hash = unhex('<hash160 hex><24 zero hex chars>');
```

Unspent outputs for an address (`FINAL` collapses the `CollapsingMergeTree`
rows; `sign = 1` keeps the survivors):

```sql
SELECT hex(reverse(txid)) AS txid_hex, output_index, value_sat, height
FROM bitcoin.utxos_by_address FINAL
WHERE address_hash = unhex('<32-byte hash>') AND sign = 1
ORDER BY height DESC;
```

> **Note:** `hex()` returns the raw byte order. Wrap it in `reverse()` to get
> the conventional display hex. The serving layer's `hex(reverse(...))`
> convention is `reverse_hex` in `bidx-serve/src/store.rs`.

### Rendering addresses in SQL (optional UDF)

By default addresses are never stored as strings, only as `(script_type,
address_hash)`. For ad-hoc queries that want a human-readable address, install
the optional `bidx-udf` executable UDF. Print the config fragment:

```bash
bidx udf-config --udf-bin /usr/local/bin/bidx-udf --network mainnet
```

Add the fragment to `clickhouse-server`'s `config.d/` (the `--network` must
match the chain) and restart the server, then register the view:

```sql
CREATE OR REPLACE VIEW bitcoin.addresses AS
SELECT address, script_type, address_hash FROM bitcoin.address_prefix;
```

Two functions become available:

- `bidxAddress(script_type, hash_hex)` → address string (empty for a type the
  network cannot render).
- `bidxParseAddress(address)` → `script_type\thash_hex` (TabSeparated, for the
  reverse direction).

Neither the UDF nor the view is part of the core DDL — they are opt-in, so a
plain `bidx init-db` stays portable across ClickHouse builds without the UDF
feature.

## Keeping this document honest

The DDL is generated by `crates/bidx-loader/src/schema.rs`. When you change a
table's shape there:

1. Bump `SCHEMA_VERSION` and add a matching `migrations()` entry for any change
   an operator must *migrate* (a new base-table column, a changed key, a dropped
   index). Additive, `IF NOT EXISTS`-guarded DDL needs no bump.
2. Update this document in the same change. A drift between this file and
   `schema.rs` is a documentation bug — the Rust source is authoritative.

To dump the exact DDL for a database named `bitcoin`:

```bash
BIDX_DDL_DUMP=./ddl.sql cargo test --release -p bidx-loader dump_ddl_when_requested
```

This writes `ddl.sql` (every statement, one per line) and `ddl.sql.backfill`
(the backfill queries) for inspection or CI validation against a real
ClickHouse.
