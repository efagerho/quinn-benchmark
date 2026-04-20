#!/usr/bin/env bash
# A/B benchmark driver comparing several quinn branches.
#
# For every BRANCH it:
#   1. checks out the branch in the `quinn/` submodule
#   2. rebuilds the benchmark binaries (in release mode)
#   3. runs handshake_bench and ingress_bench REPEATS times, tee-ing every run
#      to a separate file under $OUT/
#
# After every branch has been benchmarked, it reads all CSV lines back out of
# the per-run files and prints a comparison table (median across runs) for
# both benchmarks.
set -euo pipefail

BRANCHES=(
  main
  endpoint-two-phase-accept
  endpoint-ingress-offlock
  endpoint-parking-lot
)
OUT=bench-results
REPEATS=5

mkdir -p "$OUT"

# Common knobs — tuned for an AMD EPYC 9275F (24 physical cores, 48 SMT
# threads, single socket).
#
# The benchmarks run server + client Tokio runtimes in the same process, so
# the combined worker count should stay at (or slightly below) the physical
# core count to avoid SMT-induced cross-worker contention and to leave a
# couple of cores free for kernel softirq / UDP RX handling, which dominates
# when the handshake bench fires 2048 concurrent Initials at the server.
#
# Split: server gets the majority (that's the side we're stressing under
# EndpointInner-lock contention), client gets enough to saturate it. 14 + 8
# = 22, leaves 2 physical cores for softirq + driver task.
SERVER_THREADS=14
CLIENT_THREADS=8

ING_ARGS="--connections 1024 --workers-per-conn 4 --duration-secs 20 \
          --warmup-secs 3 --request-bytes 64 \
          --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --csv"
HS_ARGS="--total 200000 --concurrency 2048 --warmup 5000 \
         --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --csv"

for b in "${BRANCHES[@]}"; do
  echo "=== Building $b ==="
  ( cd quinn && git checkout "$b" )
  cargo build --release --bin ingress_bench --bin handshake_bench
  for bench in ingress handshake; do
    for i in $(seq 1 "$REPEATS"); do
      out="$OUT/${bench}_bench-$b-run$i.txt"
      echo "--- $bench $b run $i ---"
      case "$bench" in
        ingress)   cmd="target/release/ingress_bench   $ING_ARGS --label $b" ;;
        handshake) cmd="target/release/handshake_bench $HS_ARGS --label $b" ;;
      esac
      $cmd | tee "$out"
    done
  done
done

echo
echo "============================================================"
echo "  Comparison tables (median across $REPEATS runs per branch)"
echo "============================================================"

# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------
# $1 = branch name
# $2 = bench name (ingress|handshake)
# Emits all matching CSV lines across REPEATS runs.
_csv_lines() {
  local branch="$1" bench="$2"
  local f
  for f in "$OUT/${bench}_bench-${branch}-run"*.txt; do
    [ -f "$f" ] || continue
    grep '^CSV,' "$f" || true
  done
}

# Compute median of newline-separated numeric values on stdin. Written to
# work on both BSD awk (macOS, no asort) and gawk (Linux).
_median() {
  sort -n | awk '
    { a[NR] = $1 + 0 }
    END {
      if (NR == 0) { print "-"; exit }
      if (NR % 2) print a[(NR+1)/2]
      else        print (a[NR/2] + a[NR/2+1]) / 2.0
    }'
}

# Print a handshake comparison row.  Columns (from handshake_bench CSV):
#   1 CSV
#   2 label
#   3 total
#   4 concurrency
#   5 server_threads
#   6 client_threads
#   7 measured
#   8 errors
#   9 wall_ms
#  10 throughput (conn/s)
#  11 mean_us
#  12 p50_us
#  13 p90_us
#  14 p95_us
#  15 p99_us
#  16 p999_us
#  17 max_us
_handshake_field() {
  local branch="$1" col="$2"
  _csv_lines "$branch" handshake | awk -F, -v c="$col" '{print $c}' | _median
}

# Print an ingress comparison row.  Columns (from ingress_bench CSV):
#   1 CSV
#   2 label
#   3 connections
#   4 workers
#   5 duration
#   6 req_bytes
#   7 server_threads
#   8 client_threads
#   9 measured
#  10 errors
#  11 wall_ms
#  12 throughput (req/s)
#  13 bps
#  14 mean_us
#  15 p50_us
#  16 p90_us
#  17 p95_us
#  18 p99_us
#  19 p999_us
#  20 max_us
_ingress_field() {
  local branch="$1" col="$2"
  _csv_lines "$branch" ingress | awk -F, -v c="$col" '{print $c}' | _median
}

# Format microseconds as milliseconds with two decimals.
_us_to_ms() {
  awk 'BEGIN { x = ARGV[1] + 0; printf("%.2f", x / 1000.0) }' "$1"
}

# Format a plain throughput number with thousand separators.
_fmt_throughput() {
  awk 'BEGIN { x = ARGV[1] + 0; printf("%.1f", x) }' "$1"
}

# ---------------------------------------------------------------------------
# Handshake table
# ---------------------------------------------------------------------------
printf '\nHandshake bench  (conn/s, latency in ms)\n'
printf '%-28s %12s %9s %9s %9s %9s %9s\n' \
  branch throughput mean p50 p99 p99.9 max
printf -- '------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  t=$(_handshake_field "$b" 10)
  mean=$(_handshake_field "$b" 11)
  p50=$(_handshake_field "$b" 12)
  p99=$(_handshake_field "$b" 15)
  p999=$(_handshake_field "$b" 16)
  max=$(_handshake_field "$b" 17)
  if [ "$t" = "-" ]; then
    printf '%-28s %12s %9s %9s %9s %9s %9s\n' "$b" - - - - - -
  else
    printf '%-28s %12s %9s %9s %9s %9s %9s\n' \
      "$b" \
      "$(_fmt_throughput "$t")" \
      "$(_us_to_ms "$mean")" \
      "$(_us_to_ms "$p50")" \
      "$(_us_to_ms "$p99")" \
      "$(_us_to_ms "$p999")" \
      "$(_us_to_ms "$max")"
  fi
done

# ---------------------------------------------------------------------------
# Ingress table
# ---------------------------------------------------------------------------
printf '\nIngress bench  (req/s, latency in ms)\n'
printf '%-28s %12s %9s %9s %9s %9s %9s\n' \
  branch throughput mean p50 p99 p99.9 max
printf -- '------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  t=$(_ingress_field "$b" 12)
  mean=$(_ingress_field "$b" 14)
  p50=$(_ingress_field "$b" 15)
  p99=$(_ingress_field "$b" 18)
  p999=$(_ingress_field "$b" 19)
  max=$(_ingress_field "$b" 20)
  if [ "$t" = "-" ]; then
    printf '%-28s %12s %9s %9s %9s %9s %9s\n' "$b" - - - - - -
  else
    printf '%-28s %12s %9s %9s %9s %9s %9s\n' \
      "$b" \
      "$(_fmt_throughput "$t")" \
      "$(_us_to_ms "$mean")" \
      "$(_us_to_ms "$p50")" \
      "$(_us_to_ms "$p99")" \
      "$(_us_to_ms "$p999")" \
      "$(_us_to_ms "$max")"
  fi
done
echo
