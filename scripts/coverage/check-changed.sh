#!/usr/bin/env bash
#
# Enforce the per-file coverage policy from `.cursor/rules/test-coverage.mdc`:
# **every Rust file a change touches must be at 100% line coverage.**
#
# Usage:
#   scripts/coverage/check-changed.sh <tarpaulin-report.json> [base-ref]
#
# `<base-ref>` defaults to $COVERAGE_BASE, then `origin/master`. The set of
# "changed" files is `git diff --diff-filter=d <base>...HEAD -- '*.rs'`, i.e. the
# same set a pull request reviewer sees.
#
# Exits 0 when every changed source file is fully covered, 1 otherwise, and
# prints a per-file table so a failure is actionable.
#
# Files that are themselves tests (`tests/`, `benches/`, `examples/`) are not
# measured — their execution is the signal — and are skipped, matching the
# `exclude_files` in `.tarpaulin.toml`.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <tarpaulin-report.json> [base-ref]" >&2
    exit 2
fi

REPORT="$1"
BASE="${2:-${COVERAGE_BASE:-origin/master}}"

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

if [[ ! -f "$REPORT" ]]; then
    echo "coverage report not found: $REPORT" >&2
    echo "run: cargo tarpaulin --release --workspace --out Json --output-dir target/coverage" >&2
    exit 2
fi

if ! git rev-parse --verify --quiet "$BASE" >/dev/null; then
    echo "base ref '$BASE' not found; fetch it first (git fetch origin)" >&2
    exit 2
fi

# Changed .rs files, deletions excluded (a deleted file cannot be covered).
#
# Union of three diffs so the gate works in CI (committed branch tip), locally
# (edits not yet committed), and for files added but not yet staged:
#   * BASE...HEAD  — commits on this branch since the merge base, so a base
#                    branch that moved ahead does not show up as "changed".
#   * HEAD         — the working tree (staged + unstaged) against HEAD.
#   * untracked    — brand-new files not yet added; a new file is a change and
#                    must be covered, so it cannot dodge the gate by being
#                    un-staged.
mapfile -t CHANGED < <(
    {
        git diff --name-only --diff-filter=d "$BASE"...HEAD -- '*.rs'
        git diff --name-only --diff-filter=d HEAD -- '*.rs'
        git ls-files --others --exclude-standard -- '*.rs'
    } | sort -u
)

if [[ ${#CHANGED[@]} -eq 0 ]]; then
    echo "No Rust files changed against $BASE — coverage gate passes."
    exit 0
fi

echo "Checking ${#CHANGED[@]} changed Rust file(s) against $BASE"
echo

REPORT="$REPORT" CHANGED_JSON="$(printf '%s\n' "${CHANGED[@]}")" python3 - <<'PY'
import json, os, re, sys

report_path = os.environ["REPORT"]
changed = [l for l in os.environ["CHANGED_JSON"].splitlines() if l]

# Files that are tests themselves are not the thing being measured.
def is_test_path(p: str) -> bool:
    parts = p.split("/")
    return parts[0] in ("tests", "benches", "examples") or "/tests/" in p or "/benches/" in p

with open(report_path) as fh:
    data = json.load(fh)

# Map repo-relative path -> (covered, coverable, uncovered-line-set). tarpaulin's
# `path` is a list of components; join and normalise to a path relative to the
# repo root. `traces` carries one entry per instrumented line; a trace with zero
# `Line` stat is an uncovered line.
repo_root = os.getcwd()
by_path = {}
for f in data.get("files", []):
    raw = f.get("path")
    if isinstance(raw, list):
        raw = "/".join(raw)
    rel = os.path.relpath(os.path.normpath(raw), repo_root)
    uncovered = {int(t["line"]) for t in f.get("traces", []) if t.get("stats", {}).get("Line", 0) == 0}
    by_path[rel] = (int(f.get("covered", 0)), int(f.get("coverable", 0)), uncovered)


def uncovered_lines(by_path_map, path):
    return by_path_map[path][2]


def collapse(lines):
    """Collapse a sorted list of line numbers into a compact 'a, b-c' string."""
    out = []
    for ln in lines:
        if out and ln == out[-1][1] + 1:
            out[-1][1] = ln
        else:
            out.append([ln, ln])
    return ", ".join(str(a) if a == b else f"{a}-{b}" for a, b in out)


# Marker a line (or the comment immediately above it) may carry to declare that
# its non-execution is intentional and documented, per `.cursor/rules/test-
# coverage.mdc`. Two forms:
#   * explicit `coverage-exempt: 12-14, 20` — exempts exactly those lines;
#   * bare `coverage-exempt` — exempts the item the comment heads: a single
#     statement, or a whole brace block (function, loop, closure), so a
#     documented-unreachable function like `main` is exempted whole.
# Returns the set of such line numbers, or None if the file is unreadable.
EXEMPT_MARKER = "coverage-exempt"


def exempt_lines(src_path):
    try:
        with open(src_path, encoding="utf-8") as fh:
            src = fh.read().splitlines()
    except OSError:
        return None
    exempt = set()
    for i, text in enumerate(src):
        if EXEMPT_MARKER not in text:
            continue
        exempt.add(i + 1)
        # Explicit form: `coverage-exempt: 12-14, 20` exempts exactly those
        # lines. Used when the untestable code is a multi-line construct (a
        # `while` whose condition spans lines, a closure, ...) that the
        # next-item heuristic below cannot bracket cleanly.
        m = re.search(re.escape(EXEMPT_MARKER) + r"\s*:\s*([0-9][0-9,\-\s]*)", text)
        if m:
            for part in m.group(1).split(","):
                part = part.strip()
                if not part:
                    continue
                if "-" in part:
                    a, b = part.split("-", 1)
                    exempt.update(range(int(a), int(b) + 1))
                else:
                    exempt.add(int(part))
            continue
        # Bare form: exempt the marker's own line and the item that follows it,
        # skipping any further comment or blank lines (the marker may head a
        # multi-line doc comment). The item is found by scanning forward for the
        # first `{` or `;`/closing brace: a `{` means a block (a function, loop,
        # or closure — possibly with a multi-line signature or condition, so the
        # `{` may be several lines down), and then every line through the one
        # that closes the block is exempted; a `;` or `}` first means a single
        # statement, so only that line is exempted.
        j = i + 1
        while j < len(src) and (src[j].strip() == "" or src[j].lstrip().startswith("//")):
            j += 1
        if j >= len(src):
            continue
        exempt.add(j + 1)
        block_start = None
        k = j
        while k < len(src) and k <= j + 8:
            if "{" in src[k]:
                block_start = k
                break
            if ";" in src[k] or src[k].lstrip().startswith("}"):
                break
            k += 1
        if block_start is not None:
            depth = 0
            k = block_start
            while k < len(src):
                depth += src[k].count("{") - src[k].count("}")
                exempt.add(k + 1)
                if depth <= 0 and k > block_start:
                    break
                k += 1
    return exempt

failures = []
skipped = []
for path in changed:
    if is_test_path(path):
        skipped.append((path, "test file"))
        continue
    if path not in by_path:
        failures.append((path, 0, 0, "not present in the coverage report"))
        continue
    covered, coverable, _ = by_path[path]
    if coverable == 0:
        # A file with no instrumented lines (e.g. a module that only re-exports
        # submodules, or a doc-only file) is trivially fully covered. Treat it as
        # a pass rather than a failure: there is no line to test.
        continue
    if covered < coverable:
        # The rule allows a line to go untested when a comment explains WHY it is
        # unreachable (e.g. an `unreachable!()` arm guarded by the type system, a
        # `main` that cannot run inside a test binary, or an OS path that cannot
        # be forced portably). Such a line is marked with `coverage-exempt:` in a
        # comment on or just above it. An uncovered line carrying that marker is
        # exempt; any other uncovered line fails the gate.
        exempt = exempt_lines(os.path.join(repo_root, path))
        if exempt is None:
            failures.append((path, covered, coverable, "below 100% (file unreadable for exemptions)"))
            continue
        uncovered = uncovered_lines(by_path, path)
        not_exempt = sorted(uncovered - exempt)
        if not_exempt:
            rng = collapse(not_exempt)
            failures.append((path, covered, coverable, f"below 100% (uncovered: {rng})"))

if skipped:
    for path, why in skipped:
        print(f"  SKIP  {path}  ({why})")
    print()

if not failures:
    print(f"OK — all {len(changed) - len(skipped)} changed source file(s) at 100% line coverage.")
    sys.exit(0)

print("FAIL — changed files below 100% line coverage:")
print()
width = max(len(p) for p, *_ in failures)
for path, covered, coverable, why in failures:
    pct = (100.0 * covered / coverable) if coverable else 0.0
    print(f"  {path.ljust(width)}  {covered:>4}/{coverable:<4}  {pct:5.1f}%  {why}")
print()
print(f"{len(failures)} file(s) need tests. See .cursor/rules/test-coverage.mdc.")
sys.exit(1)
PY
