# Serving API

`bidx serve` turns the ClickHouse index into a queryable service with three
surfaces:

- **Esplora-compatible REST** — the API mempool.space and its clients speak.
- **Electrum JSON-RPC** — the subset Fulcrum/electrs clients use for
  confirmed-chain queries.
- **Ops** — Prometheus `/metrics` and `/healthz` / `/readyz`.

The HTTP layer is a small std-`TcpListener` server built for a fleet behind
a load balancer:

- **Fixed worker pool + bounded queue.** `[server] workers` threads serve
  connections; when `queue` accepted connections are already waiting, new
  ones get an immediate `503` with `Retry-After: 1` instead of piling up.
- **HTTP/1.1 keep-alive**, closed after `max_requests_per_conn` so L4
  balancers rebalance across replicas.
- **Graceful shutdown** on SIGTERM/SIGINT: `/readyz` flips to 503, the API
  keeps serving for `drain_delay_secs`, then stops accepting and waits up to
  `shutdown_timeout_secs` for in-flight requests.
- **Optional API keys + rate limits** (per key and per client IP).
- **In-process response cache** plus `Cache-Control` headers for CDNs.
- **ClickHouse replica failover** across a host list, over HTTP or HTTPS.

> **Confirmed-chain only.** There is no mempool: mempool-related responses are
> honest empty results (`get_mempool` returns `[]`, `estimatefee` returns `-1`).

## Contents

- [Running the service](#running-the-service)
- [Configuration](#configuration)
- [Environment variables](#environment-variables)
- [Authentication and rate limits](#authentication-and-rate-limits)
- [Caching](#caching)
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
(`clickhouse.http_port`, default `8123`; usually `8443` with
`scheme = "https"`), not the native `9000` port that `init-db`/`load` use.

`clickhouse.host` may be a comma-separated list (`"ch-0:8123, ch-1:8123"`;
entries without a port use `http_port`). Queries rotate round-robin; a host
that fails at the connection level is skipped for 5 seconds and the query is
retried on the next one. HTTP error statuses are not retried, since
ClickHouse uses them for query errors that would fail the same everywhere.

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

[server]
workers = 256                       # fixed worker threads
queue = 1024                        # waiting connections before 503 shedding
keep_alive_secs = 5                 # 0 disables keep-alive
max_requests_per_conn = 1000
drain_delay_secs = 5                # SIGTERM: not-ready this long, then stop
shutdown_timeout_secs = 25          # then wait this long for in-flight work
trust_forwarded_for = false         # use X-Forwarded-For as the client IP

[cache]
enabled = true
max_entries = 100000
tip_ttl_ms = 1000
address_ttl_ms = 2000
immutable_ttl_secs = 300
confirmations = 6

[auth]
required = false
# [[auth.keys]]
# name = "acme"                     # metrics label
# token_sha256 = "<hex>"            # or token = "..." (plaintext)
# rate_per_sec = 200.0              # 0 = unlimited
# burst = 400.0                     # default max(rate_per_sec, 1)

[rate_limit]
per_ip_rps = 0.0                    # anonymous traffic; 0 = off
per_ip_burst = 0.0
```

Unknown keys are rejected at startup, so a misspelled setting fails loudly
instead of being silently ignored.

Validation rules (the service refuses to start otherwise):

- `api_addr` and `metrics_addr` must differ.
- `clickhouse.database` must be non-empty.
- `clickhouse.timeout_secs` must be > 0.
- `clickhouse.http_port` must be > 0.
- At least one of `esplora_enabled` / `electrum_enabled` must be true.
- `clickhouse.scheme` is `http` or `https`; `clickhouse.host` lists at least
  one host.
- `server.workers`, `server.queue`, `server.max_requests_per_conn` > 0.
- `cache.max_entries` > 0 when the cache is enabled.
- `auth.required` needs at least one key; key names and tokens are unique.
- A non-zero rate needs `burst >= 1`.

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
| `BIDX_CLICKHOUSE__SCHEME` | `clickhouse.scheme`. |
| `BIDX_SERVER__WORKERS` / `__QUEUE` / `__KEEP_ALIVE_SECS` / `__MAX_REQUESTS_PER_CONN` | `server.*`. |
| `BIDX_SERVER__DRAIN_DELAY_SECS` / `__SHUTDOWN_TIMEOUT_SECS` / `__TRUST_FORWARDED_FOR` | `server.*`. |
| `BIDX_CACHE__ENABLED` / `__MAX_ENTRIES` / `__TIP_TTL_MS` / `__ADDRESS_TTL_MS` / `__IMMUTABLE_TTL_SECS` / `__CONFIRMATIONS` | `cache.*`. |
| `BIDX_AUTH__REQUIRED` | `auth.required`. |
| `BIDX_AUTH__KEYS` | Replaces `auth.keys`: `name:token[:rps[:burst]]`, comma-separated. |
| `BIDX_RATE_LIMIT__PER_IP_RPS` / `__PER_IP_BURST` | `rate_limit.*`. |

## Authentication and rate limits

Ops endpoints (`/metrics`, `/healthz`, `/readyz`) are never authenticated or
throttled, so probes and scrapes keep working under load.

- A key is presented as `Authorization: Bearer <token>` or `X-API-Key: <token>`.
  Keys are matched by SHA-256, so config can hold `token_sha256` instead of
  the plaintext token.
- An **unknown** key is always rejected with `401`, even when anonymous
  access is allowed, so a misconfigured client finds out instead of being
  silently downgraded.
- `auth.required = true`: requests without a valid key get `401` with
  `WWW-Authenticate: Bearer`.
- `auth.required = false` (default): keys are optional. Keyed requests use
  the key's quota and are counted per tenant; anonymous requests share the
  per-IP limit (`rate_limit.per_ip_rps`, if set).
- Over quota → `429 Too Many Requests` with `Retry-After: <seconds>`.
- Quotas are token buckets **per replica**: with N serve pods, a key's
  effective fleet-wide limit is about N × `rate_per_sec`. Size quotas with
  that in mind, or enforce global quotas at your API gateway.
- Behind a load balancer, set `server.trust_forwarded_for = true` so the
  per-IP limit sees the client (the rightmost `X-Forwarded-For` entry), not
  the balancer. Leave it false when clients connect directly, or they can
  spoof the header.

## Caching

Successful (`200`) Esplora responses are cached in process, keyed by path and
query string, and sent with a `Cache-Control` header so a CDN or reverse
proxy can cache them too:

| Route | TTL | `Cache-Control` |
|-------|-----|-----------------|
| `/blocks/tip/*` | `tip_ttl_ms` | `public, max-age=<tip_ttl_ms/1000>` |
| `/blocks/:h`, `/block-height/:h`, `/block/:hash`, `/tx/:txid` buried ≥ `confirmations` deep | `immutable_ttl_secs` | `public, max-age=<immutable_ttl_secs>` |
| The same, shallower than `confirmations` | `tip_ttl_ms` | as tip |
| `/address/*`, `/address-prefix/*`, `/tx/:txid/outspends` | `address_ttl_ms` | `public, max-age=<address_ttl_ms/1000>` |
| Errors, 404s, Electrum, ops | not cached | `no-store` |

A max-age of 0 is sent as `no-cache`. When `auth.required` is on, `public`
becomes `private`, so a shared cache never serves an authenticated response
to an anonymous caller. Setting `cache.enabled = false` turns off the
in-process cache but keeps the headers.

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

`/readyz` body (`draining` turns true, and `ready` false, after SIGTERM):

```json
{
  "ready": true,
  "checks": {
    "clickhouse": true,
    "index_has_tip": true,
    "indexed_height": 840000,
    "node_height": 840005,
    "lag_blocks": 5,
    "draining": false
  }
}
```

Readiness requires a verified ClickHouse connection, a non-zero indexed tip (a
fresh, empty database is not ready to serve), and that the process is not
draining.

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
| `bidx_request_duration_seconds` | histogram | `le` | Time from parsed request to built response (buckets 1 ms – 10 s). |
| `bidx_inflight_requests` | gauge | — | Requests being handled right now. Scale on this when ClickHouse is the bottleneck. |
| `bidx_shed_total` | counter | — | Connections refused with 503 because the worker queue was full. |
| `bidx_cache_hits_total` | counter | — | Responses served from the in-process cache. |
| `bidx_cache_misses_total` | counter | — | Cacheable requests that queried ClickHouse. |
| `bidx_unauthorized_total` | counter | — | Requests rejected with 401. |
| `bidx_rate_limited_total` | counter | — | Requests rejected with 429. |
| `bidx_tenant_requests_total` | counter | `tenant` | Admitted requests per API key (present when keys are configured). |
| `bidx_tenant_rate_limited_total` | counter | `tenant` | 429s per API key. |
| `bidx_draining` | gauge | — | 1 after SIGTERM while draining. |

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
- [cloud-deployment.md](cloud-deployment.md) — running a scaled fleet on Kubernetes.
