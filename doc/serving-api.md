# Serving API

`bidx serve` turns the ClickHouse index into a queryable service with three
surfaces:

- **Esplora-compatible REST** — the API mempool.space and its clients speak.
- **Electrum JSON-RPC** — the subset Fulcrum/electrs clients use for
  confirmed-chain queries.
- **Ops** — Prometheus `/metrics` and `/healthz` / `/readyz`.

The service is deliberately dependency-free at the HTTP layer (std
`TcpListener`, one request per connection, `Connection: close`,
thread-per-connection so a slow ClickHouse query cannot stall the accept loop).

> **Confirmed-chain only.** There is no mempool: mempool-related responses are
> honest empty results (`get_mempool` returns `[]`, `estimatefee` returns `-1`).

## Contents

- [Running the service](#running-the-service)
- [Configuration](#configuration)
- [Environment variables](#environment-variables)
- [Esplora REST API](#esplora-rest-api)
- [Electrum JSON-RPC](#electrum-json-rpc)
- [Ops endpoints](#ops-endpoints)
- [Metrics reference](#metrics-reference)
- [Address and hash conventions](#address-and-hash-conventions)

## Running the service

```bash
bidx serve --config /etc/bidx/bidx.toml
```

Two listeners come up:

| Listener | Default | Serves |
|----------|---------|--------|
| `api_addr` | `0.0.0.0:8432` | Esplora REST + Electrum JSON-RPC. |
| `metrics_addr` | `0.0.0.0:9101` | `/metrics`, `/healthz`, `/readyz`. |

The serve layer talks to ClickHouse over its **HTTP** port
(`clickhouse.http_port`, default `8123`), not the native `9000` port that
`init-db`/`load` use.

## Configuration

Precedence is **defaults < TOML file < environment**. Start from
`bidx.toml.example`:

```toml
network = "mainnet"                 # mainnet | testnet3 | testnet4 | signet | regtest
api_addr = "0.0.0.0:8432"
metrics_addr = "0.0.0.0:9101"
esplora_enabled = true
electrum_enabled = true
log = "info"

[clickhouse]
host = "clickhouse"
port = 9000                         # native protocol: init-db / load
http_port = 8123                    # HTTP: the serve layer
user = "default"
# password = "..."                  # prefer BIDX_CLICKHOUSE__PASSWORD in prod
database = "bitcoin"
timeout_secs = 30

[node]                              # optional; readiness lag + verify
rpc_url = "http://127.0.0.1:8332"
# cookie = "/home/bitcoin/.bitcoin/.cookie"
# user = "rpcuser"
# password = "rpcpassword"
```

Validation rules (the service refuses to start otherwise):

- `api_addr` and `metrics_addr` must differ.
- `clickhouse.database` must be non-empty.
- `clickhouse.timeout_secs` must be > 0.
- `clickhouse.http_port` must be > 0.
- At least one of `esplora_enabled` / `electrum_enabled` must be true.

## Environment variables

Nested keys use a `__` separator, so container orchestrators can override
hosts and secrets without editing the file:

| Variable | Overrides |
|----------|-----------|
| `BIDX_NETWORK` | `network` (`mainnet`/`main`/`bitcoin` accepted). |
| `BIDX_API_ADDR` | `api_addr`. |
| `BIDX_METRICS_ADDR` | `metrics_addr`. |
| `BIDX_ESPLORA_ENABLED` | `esplora_enabled` (`1/true/yes/on`, `0/false/no/off`). |
| `BIDX_ELECTRUM_ENABLED` | `electrum_enabled`. |
| `BIDX_RUST_LOG` | `log` filter. |
| `BIDX_CLICKHOUSE__HOST` | `clickhouse.host`. |
| `BIDX_CLICKHOUSE__PORT` | `clickhouse.port`. |
| `BIDX_CLICKHOUSE__HTTP_PORT` | `clickhouse.http_port`. |
| `BIDX_CLICKHOUSE__USER` | `clickhouse.user`. |
| `BIDX_CLICKHOUSE__PASSWORD` | `clickhouse.password`. |
| `BIDX_CLICKHOUSE__DATABASE` | `clickhouse.database`. |
| `BIDX_CLICKHOUSE__TIMEOUT_SECS` | `clickhouse.timeout_secs`. |
| `BIDX_NODE__RPC_URL` | `node.rpc_url` (also creates the node section). |
| `BIDX_NODE__COOKIE` | `node.cookie` (ignored if no RPC URL is set). |

## Esplora REST API

All routes are `GET`. List endpoints accept an optional `?limit=N` query
parameter (default 100, capped at 1000). Errors are JSON
(`{"error": "..."}`) with a matching HTTP status.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/blocks/tip/height` | Tip height as a bare number. |
| GET | `/blocks/tip/hash` | Tip block hash (display order). |
| GET | `/blocks/:height` | Block detail by height. |
| GET | `/block/:hash` | Block detail by hash. |
| GET | `/block-height/:height` | Block hash at a height. |
| GET | `/address/:addr` | Confirmed stats + balance. |
| GET | `/address/:addr/utxo` | Confirmed UTXOs. |
| GET | `/address/:addr/txs` | Per-transaction net value summaries. |
| GET | `/tx/:txid` | Transaction detail with vin/vout and status. |
| GET | `/tx/:txid/outspends` | One entry per output, spent or not. |
| GET | `/address-prefix/:prefix` | Addresses matching a prefix. |

`:addr` accepts either an address string **or** a 64-char Electrum scripthash
(display order); both resolve to the stored `(script_type, address_hash)`.

### Block detail

```json
{
  "id": "00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72728a054",
  "height": 100,
  "version": 2,
  "timestamp": 1600000000,
  "tx_count": 2,
  "size": 1000,
  "weight": 3990,
  "merkle_root": "…",
  "previousblockhash": "…",
  "bits": 386089497,
  "nonce": 42,
  "txs": ["…", "…"]
}
```

### Address stats

```json
{
  "address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
  "chain_stats": {
    "funded_txo_count": 2, "funded_txo_sum": 10000,
    "spent_txo_count": 1,  "spent_txo_sum": 4000,
    "tx_count": 3
  },
  "mempool_stats": { "funded_txo_count": 0, "funded_txo_sum": 0,
                     "spent_txo_count": 0, "spent_txo_sum": 0, "tx_count": 0 },
  "balance": 6000
}
```

An unknown address returns 200 with zeroed stats (not 404) — this matches
Esplora's behaviour and lets clients poll a fresh address. An *invalid* address
string returns 400.

### UTXOs

```json
[
  { "txid": "…", "vout": 1, "value": 6000,
    "status": { "confirmed": true, "block_height": 50 } }
]
```

### Transaction detail

```json
{
  "txid": "…", "version": 1, "fee": 1500,
  "status": { "confirmed": true, "block_height": 100, "block_hash": "…" },
  "vin":  [ { "txid": "…", "vout": 0, "prevout": null, "is_coinbase": false } ],
  "vout": [ { "scriptpubkey_address": "…", "value": 5000 } ]
}
```

`vout[].scriptpubkey_address` is rendered from the stored `(script_type,
address_hash)` and is `null` for `OP_RETURN` and unknown scripts.

### Outspends

One entry per output of the transaction, in vout order:

```json
[ { "spent": true, "txid": "…", "status": { "confirmed": true, "block_height": 101 } },
  { "spent": false, "txid": null } ]
```

### Address transactions

History rows are grouped by txid into per-transaction **net value** (positive
if the address received more than it spent in that transaction):

```json
[ { "txid": "…", "status": { "confirmed": true, "block_height": 50 }, "value": 6000 } ]
```

### Address prefix

```json
["bc1qxyz", "bc1qabc"]
```

Prefix must be non-empty and at most 64 characters (else 400). Backed by the
`address_prefix` table, which is populated by `bidx index rebuild address_prefix`
(the only backfill that needs a Rust renderer; pass `--network` to match the
chain).

## Electrum JSON-RPC

`POST /` or `POST /electrum` with a JSON-RPC 2.0 body:

```bash
curl -s http://127.0.0.1:8432/ \
  -d '{"jsonrpc":"2.0","id":1,"method":"server.version","params":[]}'
```

Responses are always HTTP 200 with either `result` or `error`. Error codes:
`-32700` parse error, `-32600` invalid request, `-32601` method not found,
`-32602` invalid params, `-32603` internal error.

### Supported methods

| Method | Params | Result |
|--------|--------|--------|
| `server.version` | — | `["bidx", "1.4"]` |
| `server.banner` | — | `"bitcoindex serving layer"` |
| `server.ping` | — | `null` |
| `blockchain.headers.subscribe` | — | `{ "height": N, "hex": "<160-char header>" }` |
| `blockchain.block.header` | `height` | 80-byte header as hex. |
| `blockchain.block.headers` | `start, count` | `{ "count": N, "max": 2016, "hex": "…" }` |
| `blockchain.scripthash.get_balance` | `scripthash` | `{ "confirmed": N, "unconfirmed": 0 }` |
| `blockchain.scripthash.get_history` | `scripthash` | `[{ "tx_hash", "height" }, …]` |
| `blockchain.scripthash.get_mempool` | `scripthash` | `[]` |
| `blockchain.scripthash.listunspent` | `scripthash` | `[{ "tx_hash", "tx_pos", "height", "value" }, …]` |
| `blockchain.scripthash.subscribe` | `scripthash` | Status hash, or `null` when the address has no history. |
| `blockchain.estimatefee` | — | `-1` |
| `blockchain.relayfee` | — | `0.00001` |
| `mempool.get_fee_histogram` | — | `[]` |

Notes:

- `blockchain.block.headers` stops at the first missing height and caps
  `count` at 2016 (the Electrum protocol limit).
- `blockchain.scripthash.get_history` and `listunspent` cap at 1000 rows.
- **Status hash** (`subscribe`): computed the Electrum way as
  `sha256(concat("tx_hash:height:" for each history entry, in block order))`,
  or `null` for empty history. Without a mempool this is a pure function of
  confirmed history.

## Ops endpoints

| Path | Status | Body |
|------|--------|------|
| `/healthz` | always 200 | `ok` |
| `/readyz` | 200 when ready, else 503 | JSON with per-check detail |
| `/metrics` | 200 | Prometheus text exposition |

`/readyz` body:

```json
{
  "ready": true,
  "checks": {
    "clickhouse": true,
    "index_has_tip": true,
    "indexed_height": 840000,
    "node_height": 840005,
    "lag_blocks": 5
  }
}
```

Readiness requires **both** a verified ClickHouse connection **and** a non-zero
indexed tip — a fresh, empty database is not ready to serve.

## Metrics reference

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `bidx_indexed_height` | gauge | — | Highest height present in the index. |
| `bidx_node_height` | gauge | — | Node tip height as last observed. |
| `bidx_indexer_lag_blocks` | gauge | — | Node height minus indexed height (0 when caught up). |
| `bidx_queries_total` | counter | — | Total served queries. |
| `bidx_query_errors_total` | counter | — | Queries that returned an error (5xx). |
| `bidx_address_not_found_total` | counter | — | Address queries that matched no outputs. |
| `bidx_requests_by_surface_total` | counter | `surface` = `esplora`/`electrum`/`ops` | Requests by surface. |
| `bidx_responses_by_status_total` | counter | `status` = `2xx`/`4xx`/`5xx` | Responses by HTTP status class. |

The serving loop refreshes health/metrics once at startup and every 10 seconds
thereafter; `/metrics` renders on scrape.

## Address and hash conventions

- **Hashes** are stored as raw wire-order bytes. The REST surface renders them
  in **display order** (byte-reversed) as lowercase hex, and accepts display
  order on input.
- **Scripthashes** arrive from Electrum clients in **display order** (reversed
  `sha256(scriptPubKey)`); the service reverses them and resolves through the
  `scripthash_map` table.
- **Addresses** are parsed to `(script_type, address_hash)` and must match the
  configured `network` — a testnet address will not parse on mainnet. Both
  base58check (P2PKH/P2SH) and bech32/bech32m (P2WPKH/P2WSH/P2TR, plus
  witness-unknown versions) are supported.

## See also

- [schema.md](schema.md) — the tables these queries read.
- [query-cookbook.md](query-cookbook.md) — the SQL behind each surface.
- [cli.md](cli.md#serve) — the `bidx serve` command and its flags.
