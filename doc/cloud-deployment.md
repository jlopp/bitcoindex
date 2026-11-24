# Cloud deployment and scaling

How to run bitcoindex as a production query fleet on a cloud platform: a
horizontally scaled, stateless serve tier in front of ClickHouse, fed by a
single ingest writer.

## Contents

- [Shape of a deployment](#shape-of-a-deployment)
- [The container image](#the-container-image)
- [Quickstart on Kubernetes](#quickstart-on-kubernetes)
- [Bootstrap: the initial index](#bootstrap-the-initial-index)
- [Managed ClickHouse](#managed-clickhouse)
- [Scaling the serve tier](#scaling-the-serve-tier)
- [Multi-tenant API keys](#multi-tenant-api-keys)
- [CDN and edge caching](#cdn-and-edge-caching)
- [Rollouts and shutdown](#rollouts-and-shutdown)
- [Load testing](#load-testing)
- [Monitoring and alerts](#monitoring-and-alerts)
- [Sizing guide](#sizing-guide)

## Shape of a deployment

```mermaid
flowchart LR
  clients[Clients / CDN] --> lb[Ingress / LB]
  lb --> s1[bidx serve]
  lb --> s2[bidx serve]
  lb --> s3[bidx serve ...]
  s1 & s2 & s3 -->|HTTP(S), round-robin + failover| ch[(ClickHouse replicas)]
  node[Bitcoin Core] -->|RPC + ZMQ| ingest[bidx live --ch-live<br/>single writer + UTXO PVC]
  ingest -->|native protocol| ch
```

| Tier | Kubernetes object | Scales | State |
|------|-------------------|--------|-------|
| Serve (`bidx serve`) | Deployment + HPA + PDB | Horizontally, 3 → N | None (in-process cache only) |
| Ingest (`bidx live --ch-live`) | StatefulSet, `replicas: 1` | Never — one writer | UTXO RocksDB on a PVC |
| ClickHouse | Your operator or a managed service | Replicas for reads | The index |
| Bitcoin Core | Usually already run by you | — | Chain |

The serve pods are interchangeable and hold no state, so adding capacity
means adding pods. Read capacity beyond that comes from ClickHouse replicas.
List them all in `clickhouse.host` and every serve pod spreads its queries
across them.

## The container image

Every push to `master` and every `v*` tag publishes a multi-arch
(`linux/amd64`, `linux/arm64`) image to GHCR
([`.github/workflows/image.yml`](../.github/workflows/image.yml)):

| Tag | Meaning |
|-----|---------|
| `ghcr.io/jlopp/bitcoindex:1.2.3`, `:1.2`, `:1` | Releases (semver). |
| `:edge` | Latest `master`. |
| `:sha-<short>` | One specific commit. |

Images carry SLSA build provenance and an SPDX SBOM attestation, and are
signed keyless with cosign. To verify before deploying:

```bash
cosign verify ghcr.io/jlopp/bitcoindex:1.2.3 \
  --certificate-identity-regexp 'https://github.com/jlopp/bitcoindex/.github/workflows/image.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

gh attestation verify oci://ghcr.io/jlopp/bitcoindex:1.2.3 --owner jlopp
```

Pin by digest (`@sha256:…`) in production overlays.

## Quickstart on Kubernetes

Manifests live in [`deploy/kubernetes/`](../deploy/kubernetes/) as a
kustomize base.

```bash
# 1. Secrets (use your secret manager in real deployments)
kubectl create namespace bitcoindex
kubectl -n bitcoindex create secret generic bidx-secrets \
  --from-literal=BIDX_CLICKHOUSE__PASSWORD="$READER_PW" \
  --from-literal=INGEST_CLICKHOUSE_USER=bidx_writer \
  --from-literal=INGEST_CLICKHOUSE_PASSWORD="$WRITER_PW" \
  --from-literal=BIDX_NODE__USER="$RPC_USER" \
  --from-literal=BIDX_NODE__PASSWORD="$RPC_PW" \
  --from-literal=BIDX_AUTH__KEYS=""

# 2. Edit deploy/kubernetes/configmap.yaml: ClickHouse host(s), scheme,
#    node RPC/ZMQ addresses. Then:
kubectl apply -k deploy/kubernetes

# 3. Bootstrap the index (below), then watch readiness:
kubectl -n bitcoindex get pods -w
kubectl -n bitcoindex port-forward svc/bidx-serve 8432:80
curl localhost:8432/blocks/tip/height
```

What you get:

- `bidx-serve`: 3 replicas spread across zones and nodes. Readiness is
  `/readyz`, which waits for ClickHouse and a non-empty index and turns 503
  while draining. Liveness is `/healthz`. The pods run non-root with a
  read-only root filesystem and all capabilities dropped (Pod Security
  "restricted").
- A CPU-based HPA (3–30 replicas) and a PDB (`maxUnavailable: 1`).
- `bidx-ingest`: one StatefulSet pod running `bidx live --ch-live` with a
  100 Gi PVC for the UTXO store.

Optional pieces in [`deploy/kubernetes/optional/`](../deploy/kubernetes/optional/):
an Ingress with TLS, a Prometheus Operator `ServiceMonitor` + alert rules, and
an HPA that also scales on in-flight requests.

Use two ClickHouse users: a read-only one for serve (`SELECT` on the
database) and a writer for ingest/bootstrap (`INSERT`, `ALTER DELETE` for
reorgs, `CREATE` for `init-db`).

## Bootstrap: the initial index

`bidx live` continues from an existing UTXO store, so the first full index
comes from a one-time `bidx parse` over a synced node's `blocks/` directory.
It produces both the Parquet tree and the UTXO RocksDB.

1. On a machine with the node's blocks dir (fast NVMe, many cores):

   ```bash
   bidx parse --blocks-dir /bitcoin/blocks --out /scratch/out --utxo /scratch/utxo
   ```

2. Load the Parquet into ClickHouse. `--upload` streams every file over the
   client connection, so it works against any ClickHouse, including managed
   ones:

   ```bash
   bidx init-db --host "$CH" --secure --port 9440 --user bidx_writer --password "$PW"
   bidx load --input /scratch/out --upload --host "$CH" --secure --port 9440 \
     --user bidx_writer --password "$PW"
   ```

   Or in-cluster: put the tree on a `bidx-parquet` PVC and run
   [`deploy/kubernetes/jobs/bootstrap-job.yaml`](../deploy/kubernetes/jobs/bootstrap-job.yaml).

3. Copy `/scratch/utxo` into the ingest PVC's `utxo/` directory (for example
   with a temporary pod mounting the PVC and `kubectl cp`, or by restoring a
   volume snapshot), then scale `bidx-ingest` up.

From then on `bidx live` keeps the index at the tip. Snapshot the UTXO PVC
periodically (see [deployment-ops.md](deployment-ops.md#backup-and-restore)).

## Managed ClickHouse

ClickHouse Cloud, Altinity.Cloud, Aiven and similar services need TLS and
cannot read your filesystem:

| Component | Setting |
|-----------|---------|
| `bidx serve` | `clickhouse.scheme = "https"`, `clickhouse.http_port = 8443` |
| `init-db`, `load`, `migrate`, `verify`, `index rebuild` | `--secure --port 9440` (or `BIDX_CLICKHOUSE__SECURE=true`, `BIDX_CLICKHOUSE__PORT=9440`) |
| `load` | `--upload` |
| `live --ch-live` | `--ch-secure --ch-port 9440` |

For self-managed replicas, list every replica (or one per AZ) in serve's
`clickhouse.host`:

```toml
[clickhouse]
host = "ch-0.clickhouse:8123, ch-1.clickhouse:8123, ch-2.clickhouse:8123"
```

Queries go round-robin. A replica that refuses connections or times out is
skipped for 5 s and the query is retried on the next one, so losing a replica
costs at most one retry per request.

## Scaling the serve tier

Each serve pod has a fixed worker pool (`server.workers`, default 256)
behind a bounded queue (`server.queue`, default 1024). Under overload the
excess is refused immediately with `503 Retry-After: 1` (counted in
`bidx_shed_total`). Latency stays bounded and the balancer can retry on
another pod.

Scale on these signals:

- **CPU** (the default HPA). Request parsing, JSON rendering, and cache hits
  are CPU work.
- **`bidx_inflight_requests`**. When ClickHouse is the bottleneck, requests
  wait on queries and CPU stays low. Use
  [`optional/hpa-inflight.yaml`](../deploy/kubernetes/optional/hpa-inflight.yaml)
  with prometheus-adapter to scale before the queue fills.
- **`bidx_shed_total` > 0** means you are refusing traffic: add pods, raise
  `workers`, or add ClickHouse read capacity.

Connections are kept alive (HTTP/1.1) and recycled after
`server.max_requests_per_conn` requests, so long-lived client connections
spread across new pods after a scale-out.

## Multi-tenant API keys

```toml
[auth]
required = true

[[auth.keys]]
name = "acme"                       # appears as tenant="acme" in metrics
token_sha256 = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
rate_per_sec = 200.0
burst = 400.0

[[auth.keys]]
name = "internal"
token_sha256 = "…"
rate_per_sec = 0                    # unlimited
```

Or from a secret, without touching the file:
`BIDX_AUTH__KEYS="acme:tok_abc:200:400,internal:tok_xyz"`.

Generate a token and its hash:

```bash
tok=$(openssl rand -hex 24); echo "token: $tok"
printf %s "$tok" | sha256sum | cut -d' ' -f1
```

Clients send `Authorization: Bearer <token>` or `X-API-Key: <token>`. Quotas
are enforced per pod, so a key's fleet-wide ceiling is roughly
`replicas × rate_per_sec`. For hard global quotas, put an API gateway in front
and use bidx's limits as a per-pod safety net. Usage per tenant is in
`bidx_tenant_requests_total` and `bidx_tenant_rate_limited_total`.

With `required = false`, keys are optional. Anonymous clients are limited per
IP (`[rate_limit] per_ip_rps`); behind a load balancer, set
`server.trust_forwarded_for = true` so the limit applies to the real client.

## CDN and edge caching

Explorer traffic is dominated by a few hot objects (the tip, recent blocks,
popular addresses), and deep blocks and transactions never change. Every
successful REST response carries a `Cache-Control` header:

- Blocks and txs at least `cache.confirmations` (6) deep:
  `public, max-age=300` (`cache.immutable_ttl_secs`).
- Tip routes and shallow data: `max-age` from `cache.tip_ttl_ms` (1 s).
- Address routes: `cache.address_ttl_ms` (2 s).
- Errors, 404s, Electrum, and ops: `no-store`.

The same rules drive the in-process cache in each pod, so repeats of hot
requests never reach ClickHouse (`bidx_cache_hits_total`). A CDN in front of
the Ingress absorbs them before they reach your cluster at all. When
`auth.required` is on, responses are `private`, so a CDN will not share them
across callers. Configure the CDN to vary on, or forward, the
`Authorization` header.

## Rollouts and shutdown

On SIGTERM a serve pod:

1. flips `/readyz` to 503 (`draining: true`), so Kubernetes removes it from
   Service endpoints;
2. keeps serving for `server.drain_delay_secs` (5 s) while that removal
   propagates to kube-proxy, the Ingress, and cloud LBs;
3. stops accepting, finishes in-flight requests (up to
   `server.shutdown_timeout_secs`, 25 s), closes keep-alive connections after
   their current response, and exits 0.

`terminationGracePeriodSeconds: 40` in the Deployment covers that sequence.
If you raise either setting, raise the grace period to match. Combined with
`maxUnavailable: 0` rollouts and the PDB, deploys and node drains do not drop
requests.

The ingest StatefulSet stops its old pod before starting the new one, so
there is never more than one writer.

## Load testing

[`scripts/loadtest/`](../scripts/loadtest/) holds a k6 scenario that replays a
realistic explorer/wallet mix (tip polling, recent and deep blocks, txs,
address lookups, Electrum) at a fixed arrival rate:

```bash
BASE_URL=https://api.example.com RATE=2000 DURATION=5m API_KEY=... \
  scripts/loadtest/run.sh
```

It runs local `k6` or the `grafana/k6` container and writes a JSON summary to
`scripts/loadtest/results/`. It fails if errors exceed 1 % or p99 exceeds
`P99_MS` (default 250 ms). Run it before and after changing sizing, cache
TTLs, or ClickHouse topology, and watch `bidx_inflight_requests`,
`bidx_cache_hits_total`, and `bidx_shed_total` while it runs.

## Monitoring and alerts

[`optional/monitoring.yaml`](../deploy/kubernetes/optional/monitoring.yaml)
scrapes every serve pod and defines these alerts:

| Alert | Fires when |
|-------|-----------|
| `BidxIndexLagging` | Index more than 3 blocks behind the node for 15 m (needs `[node] rpc_url`). |
| `BidxHighErrorRate` | More than 1 % of requests return 5xx for 10 m. |
| `BidxLatencyP99High` | p99 of `bidx_request_duration_seconds` above 500 ms for 10 m. |
| `BidxShedding` | Any `bidx_shed_total` growth for 5 m. |
| `BidxNoReadyReplicas` | No serve replica ready for 2 m. |

The full metric list is in [serving-api.md](serving-api.md#metrics-reference).

## Sizing guide

Starting points; confirm with the load test.

| Component | Start with | Notes |
|-----------|-----------|-------|
| Serve pod | 0.5–2 vCPU, 256 Mi–1 Gi | Memory grows with `cache.max_entries` (bodies are mostly < 2 KiB). |
| Serve replicas | 3 (one per AZ) | Scale on CPU / in-flight; add ClickHouse replicas when queries dominate. |
| Ingest pod | 1 vCPU, 2–8 Gi, SSD PVC ≥ 100 Gi | RocksDB UTXO set + undo log. |
| ClickHouse | ≥ 2 replicas, NVMe | Index size: see [deployment-ops.md](deployment-ops.md). |
