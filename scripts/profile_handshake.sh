#!/usr/bin/env bash
# profile_handshake.sh - produce flamegraph + perf-stat for handshake_bench.
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <branch>" >&2
  exit 2
fi
BRANCH="$1"

if ! command -v perf >/dev/null 2>&1; then
  echo "error: 'perf' not found on PATH" >&2
  exit 1
fi

PARANOID=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 3)
if [ "$PARANOID" -gt 1 ]; then
  echo "error: kernel.perf_event_paranoid=$PARANOID (needs <= 1)" >&2
  exit 1
fi

if ! command -v inferno-flamegraph >/dev/null 2>&1 \
  || ! command -v inferno-collapse-perf >/dev/null 2>&1; then
  echo "inferno tooling not found, installing inferno..."
  cargo install inferno --locked
fi

_detect_cores() {
  if [ -n "${CORES:-}" ]; then echo "$CORES"; return; fi
  if command -v lscpu >/dev/null 2>&1; then
    local n
    n=$(lscpu -b -p=Core,Socket 2>/dev/null \
          | awk -F, '!/^#/ { print $1"-"$2 }' | sort -u | wc -l)
    if [ "${n:-0}" -gt 0 ]; then echo "$n"; return; fi
  fi
  if command -v nproc >/dev/null 2>&1; then nproc; return; fi
  echo 2
}
CORES=$(_detect_cores)
if [ "$CORES" -ge 8 ]; then RESERVED=2; else RESERVED=1; fi
USABLE=$(( CORES - RESERVED ))
[ "$USABLE" -lt 2 ] && USABLE=2
SERVER_THREADS=${SERVER_THREADS:-$(( (USABLE * 7 + 10) / 11 ))}
CLIENT_THREADS=${CLIENT_THREADS:-$(( USABLE - SERVER_THREADS ))}
[ "$SERVER_THREADS" -lt 1 ] && SERVER_THREADS=1
[ "$CLIENT_THREADS" -lt 1 ] && CLIENT_THREADS=1

HS_ARGS=(
  --total 200000
  --concurrency 2048
  --warmup 5000
  --server-threads "$SERVER_THREADS"
  --client-threads "$CLIENT_THREADS"
  --csv
  --label "$BRANCH"
)

if [ ! -d quinn ]; then
  echo "error: must run from quinn-benchmarks repo root (no quinn/ submodule found)" >&2
  exit 1
fi

echo "=== Checking out $BRANCH in quinn/ submodule ==="
( cd quinn && git checkout "$BRANCH" )

echo "=== Building handshake_bench in release mode ==="
unset CARGO_TARGET_DIR
cargo build --release --bin handshake_bench

OUTDIR="bench-results/profile-handshake/$BRANCH"
mkdir -p "$OUTDIR"
cp target/release/handshake_bench "$OUTDIR/handshake_bench"

echo "=== perf record (DWARF call-graph) for $BRANCH ==="
perf record \
  -F 999 \
  --call-graph dwarf,16384 \
  -g \
  -o "$OUTDIR/perf.data" \
  -- target/release/handshake_bench "${HS_ARGS[@]}"

echo "=== Rendering flamegraph via inferno ==="
perf script -i "$OUTDIR/perf.data" \
  | inferno-collapse-perf > "$OUTDIR/perf.folded"
inferno-flamegraph \
  --title "handshake_bench $BRANCH" \
  --subtitle "200k total, 2048 concurrency, DWARF call-graph" \
  < "$OUTDIR/perf.folded" \
  > "$OUTDIR/flamegraph.svg"

PERF_EVENTS=(
  task-clock
  context-switches
  cpu-migrations
  page-faults
  cycles
  instructions
  branches
  branch-misses
  cache-references
  cache-misses
  LLC-loads
  LLC-load-misses
  LLC-stores
  LLC-store-misses
)
PERF_EVENTS_CSV=$(IFS=, ; echo "${PERF_EVENTS[*]}")

echo "=== perf stat run for $BRANCH ==="
perf stat -e "$PERF_EVENTS_CSV" -- target/release/handshake_bench "${HS_ARGS[@]}" \
  2>&1 | tee "$OUTDIR/perf-stat.txt"

echo
echo "=== Artifacts for $BRANCH ==="
ls -la "$OUTDIR"
