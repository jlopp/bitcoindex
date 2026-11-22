#!/usr/bin/env bash
# Run the k6 load test and save a JSON summary next to it, so runs are
# comparable over time (commit it, or upload it from CI).
#
#   scripts/loadtest/run.sh                          # local, 200 rps, 1m
#   BASE_URL=https://api.example.com RATE=2000 DURATION=5m API_KEY=... \
#     scripts/loadtest/run.sh
#
# Uses a local `k6` if present, otherwise the grafana/k6 container.
# Before/after a change, compare the p95/p99 and bidx_shed_or_limited in the
# summaries, and watch the serve pods' /metrics (bidx_inflight_requests,
# bidx_cache_hits_total, bidx_shed_total) while it runs.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${OUT_DIR:-$here/results}"
mkdir -p "$out_dir"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
summary="$out_dir/summary-$stamp.json"

export BASE_URL="${BASE_URL:-http://127.0.0.1:8432}"
export RATE="${RATE:-200}"
export DURATION="${DURATION:-1m}"

echo "load test: $BASE_URL at $RATE rps for $DURATION → $summary"

if command -v k6 >/dev/null 2>&1; then
  k6 run --summary-export "$summary" "$here/bidx.js"
else
  # --network host so 127.0.0.1 reaches a locally running `bidx serve`.
  docker run --rm -i --network host \
    -e BASE_URL -e RATE -e DURATION -e API_KEY -e ADDRESSES -e P99_MS \
    -v "$here:/scripts" -v "$out_dir:/results" \
    grafana/k6:latest run --summary-export "/results/$(basename "$summary")" /scripts/bidx.js
fi
