#!/usr/bin/env bash
# Run a matrix of `bidx bench` configurations and emit one JSON file per cell
# plus a summary table. Designed to be re-runnable and diffable: each cell's
# output file name encodes its parameters, so two runs of the same matrix
# produce the same file names and can be compared cell-by-cell.
#
# Usage:
#   scripts/perf/matrix.sh --blocks-dir ~/.bitcoin/signet/blocks --label base
#
# Options:
#   --blocks-dir DIR     (required) Bitcoin Core blocks directory.
#   --label NAME         Output subdirectory under scripts/perf/results/.
#   --start N            Start height for every cell (default 0).
#   --end N              End height for every cell (default: chain tip).
#   --cells LIST         Comma-separated subset of cell names to run.
#   --list               Print the cell names and exit.
#
# The matrix is deliberately coarse. Its job is to tell you WHICH axis moves
# throughput, after which you narrow down by hand. Do not run the whole thing
# on a large range — the point is a repeatable baseline, not a full re-index.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

BLOCKS_DIR=""
LABEL="$(date -u +%Y%m%dT%H%M%SZ)"
START=0
END=""
CELLS=""
LIST_ONLY=0

while [ $# -gt 0 ]; do
    case "$1" in
        --blocks-dir) BLOCKS_DIR="$2"; shift 2 ;;
        --label)      LABEL="$2"; shift 2 ;;
        --start)      START="$2"; shift 2 ;;
        --end)        END="$2"; shift 2 ;;
        --cells)      CELLS="$2"; shift 2 ;;
        --list)       LIST_ONLY=1; shift ;;
        -h|--help)    sed -n '2,20p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

BIN="$REPO_ROOT/target/release/bidx"
if [ ! -x "$BIN" ]; then
    echo "error: $BIN not found. Build it first:" >&2
    echo "  source scripts/perf/env.sh && cargo build --release -p bidx-cli" >&2
    exit 1
fi

# --- the matrix -------------------------------------------------------------
#
# Each cell is: name|mode|extra bench args
#
# Modes are the ablation axis; the `full` cells then sweep the two knobs most
# likely to explain a sequential-consumer bottleneck: how many parsed blocks
# the consumer is allowed to be behind, and how often the Parquet sink flushes.
#
# `drain` is the parse ceiling: it parses every block and throws the result
# away, so any `full` cell slower than `drain` is consumer-bound, and the gap
# is exactly what the consumer costs.
CELLS_ALL=(
    "headers|headers|"
    "drain|drain|"
    "parse|parse|"
    "sink|sink|"
    "utxo|utxo|"
    "full|full|"
    "full-reorder-4|full|--reorder-high 4 --channel-depth 64"
    "full-reorder-256|full|--reorder-high 256 --channel-depth 4096"
    "full-sinkbatch-1e5|full|--sink-batch-rows 100000"
    "full-sinkbatch-1e6|full|--sink-batch-rows 1000000"
)

if [ "$LIST_ONLY" = "1" ]; then
    for c in "${CELLS_ALL[@]}"; do echo "${c%%|*}"; done
    exit 0
fi

if [ -z "$BLOCKS_DIR" ]; then
    echo "error: --blocks-dir is required" >&2
    exit 2
fi

OUT_DIR="$REPO_ROOT/scripts/perf/results/$LABEL"
mkdir -p "$OUT_DIR"

want_cell() {
    [ -z "$CELLS" ] && return 0
    case ",$CELLS," in *",$1,"*) return 0 ;; *) return 1 ;; esac
}

RANGE_ARGS=(--start "$START")
if [ -n "$END" ]; then RANGE_ARGS+=(--end "$END"); fi

echo "matrix label : $LABEL"
echo "blocks dir   : $BLOCKS_DIR"
echo "range        : ${START}..${END:-tip}"
echo "results      : $OUT_DIR"
echo

for cell in "${CELLS_ALL[@]}"; do
    name="${cell%%|*}"
    rest="${cell#*|}"
    mode="${rest%%|*}"
    extra="${rest#*|}"
    want_cell "$name" || continue

    json="$OUT_DIR/${name}.json"
    log="$OUT_DIR/${name}.log"

    echo "=== $name (mode=$mode ${extra}) ==="
    # `--rocksdb-stats` on the UTXO-applying cells: it is the only way to see
    # whether the write path is stalling, and its overhead is small next to a
    # full apply. It is skipped for cells with no UTXO store.
    stats_args=()
    case "$mode" in
        full|utxo) stats_args=(--rocksdb-stats) ;;
    esac

    # shellcheck disable=SC2086
    "$BIN" bench \
        --blocks-dir "$BLOCKS_DIR" \
        --mode "$mode" \
        "${RANGE_ARGS[@]}" \
        $extra \
        "${stats_args[@]}" \
        --csv "$json" \
        > "$log" 2>&1 || {
            echo "  FAILED (see $log)"
            continue
        }
    # Pull the headline numbers out of the log so the terminal run is readable
    # without opening files.
    awk '/^blocks\/s:/{printf "  %s\n", $0}' "$log"
    awk '/^missing utxos:/{printf "  %s\n", $0}' "$log"
    awk '/per-block apply/{printf "  %s\n", $0}' "$log"
done

# --- summary ---------------------------------------------------------------
echo
echo "=== summary ($LABEL) ==="
printf '%-22s %10s %12s %12s %12s\n' cell blocks_per_sec missing p50_apply_ms max_apply_ms
for f in "$OUT_DIR"/*.json; do
    [ -e "$f" ] || continue
    name="$(basename "$f" .json)"
    # jq is used when present; fall back to a python one-liner otherwise.
    if command -v jq > /dev/null; then
        read -r bps missing p50 max < <(jq -r '
            .[0] | [.blocks_per_sec, .missing_utxos,
                   (.utxo_latency.p50_ms // 0), (.utxo_latency.max_ms // 0)]
            | @tsv' "$f")
    else
        read -r bps missing p50 max < <(python3 -c '
import json,sys
d=json.load(open(sys.argv[1]))[0]
u=d.get("utxo_latency") or {}
print(d["blocks_per_sec"], d["missing_utxos"], u.get("p50_ms",0), u.get("max_ms",0))
' "$f")
    fi
    printf '%-22s %10.1f %12s %12s %12s\n' "$name" "$bps" "$missing" "$p50" "$max"
done
echo
echo "Per-cell JSON and logs are in $OUT_DIR"
