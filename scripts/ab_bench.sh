#!/usr/bin/env bash
# A/B benchmark driver comparing several quinn branches.
#
# For every BRANCH it:
#   1. checks out the branch in the `quinn/` submodule
#   2. rebuilds the benchmark binaries (in release mode)
#   3. runs handshake_bench, ingress_bench, and contention_bench REPEATS
#      times, tee-ing every run to a separate file under $OUT/
#
# After every branch has been benchmarked, it reads all CSV lines back out of
# the per-run files and prints a comparison table (median across runs) for
# every benchmark.
set -euo pipefail

BRANCHES=(
#  main
#  endpoint-two-phase-accept
#  endpoint-ingress-offlock
#  endpoint-ingress-offlock-accepting
#  endpoint-parking-lot
  endpoint-lock-optimization-accept-split
  endpoint-lock-optimization-accept-split-batching
)
OUT=bench-results
REPEATS=5

mkdir -p "$OUT"

# Common knobs — auto-sized for the host.
#
# The benchmarks run server + client Tokio runtimes in the same process, so
# the combined worker count should stay at (or slightly below) the *physical*
# core count to avoid SMT-induced cross-worker contention and to leave a
# couple of cores free for kernel softirq / UDP RX handling, which dominates
# when the handshake bench fires 2048 concurrent Initials at the server.
#
# Split: server gets the majority (that's the side we're stressing under
# EndpointInner-lock contention), client gets enough to saturate it. The
# ratio matches the original hand-tuned config (14 server + 8 client out of
# 24 physical cores on an AMD EPYC 9275F, leaving 2 cores for softirq +
# driver task). Override by exporting CORES, SERVER_THREADS, or
# CLIENT_THREADS before invoking this script.
_detect_cores() {
  if [ -n "${CORES:-}" ]; then
    echo "$CORES"; return
  fi
  case "$(uname -s)" in
    Darwin)
      sysctl -n hw.physicalcpu 2>/dev/null && return ;;
    Linux)
      if command -v lscpu >/dev/null 2>&1; then
        # Count distinct (Core, Socket) pairs = physical cores.
        local n
        n=$(lscpu -b -p=Core,Socket 2>/dev/null \
              | awk -F, '!/^#/ { print $1"-"$2 }' | sort -u | wc -l)
        if [ "${n:-0}" -gt 0 ]; then echo "$n"; return; fi
      fi
      if [ -r /proc/cpuinfo ]; then
        # Fall back to logical CPU count.
        local n
        n=$(awk '/^processor/ { c++ } END { print c+0 }' /proc/cpuinfo)
        if [ "${n:-0}" -gt 0 ]; then echo "$n"; return; fi
      fi ;;
  esac
  if command -v nproc >/dev/null 2>&1; then nproc; return; fi
  if command -v getconf >/dev/null 2>&1; then getconf _NPROCESSORS_ONLN; return; fi
  echo 2
}

CORES=$(_detect_cores)

# Reserve 2 cores on an 8+-core box (1 on smaller boxes) for softirq / UDP RX
# / the endpoint driver task itself.
if [ "$CORES" -ge 8 ]; then
  RESERVED=2
else
  RESERVED=1
fi
USABLE=$(( CORES - RESERVED ))
[ "$USABLE" -lt 2 ] && USABLE=2

# Server/client split: ~7/4 (matches 14/8 at USABLE=22). The +10 in the
# numerator rounds to nearest instead of truncating.
SERVER_THREADS=${SERVER_THREADS:-$(( (USABLE * 7 + 10) / 11 ))}
CLIENT_THREADS=${CLIENT_THREADS:-$(( USABLE - SERVER_THREADS ))}
[ "$SERVER_THREADS" -lt 1 ] && SERVER_THREADS=1
[ "$CLIENT_THREADS" -lt 1 ] && CLIENT_THREADS=1

# Contention bench runs three runtimes in the same process: server,
# throughput-client, churn-client. Split the CLIENT_THREADS budget evenly
# between the two client runtimes so the throughput send-loop does not starve
# the churn connect workers (which would artificially suppress the churn rate
# and mask server-side lock contention). Override via CONT_CLIENT_THREADS /
# CONT_CHURN_THREADS if needed.
CONT_CLIENT_THREADS=${CONT_CLIENT_THREADS:-$(( (CLIENT_THREADS + 1) / 2 ))}
CONT_CHURN_THREADS=${CONT_CHURN_THREADS:-$(( CLIENT_THREADS / 2 ))}
[ "$CONT_CLIENT_THREADS" -lt 1 ] && CONT_CLIENT_THREADS=1
[ "$CONT_CHURN_THREADS" -lt 1 ] && CONT_CHURN_THREADS=1

echo "Detected $CORES physical cores -> server=$SERVER_THREADS client=$CLIENT_THREADS (reserved=$RESERVED)"
echo "Contention split -> throughput=$CONT_CLIENT_THREADS churn=$CONT_CHURN_THREADS"

ING_ARGS="--connections 1024 --workers-per-conn 4 --duration-secs 20 \
          --warmup-secs 3 --request-bytes 64 \
          --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --csv"
# Batching-friendly ingress: same total in-flight workers (4 * 256 = 1024) as
# the standard ingress test, but funnelled through only 4 long-lived
# connections. With BATCH_SIZE=32 in the recv path the expected run length
# of consecutive same-handle datagrams is ~BATCH_SIZE/N, so 4 flows yields
# runs of ~8 on non-GRO platforms and tens-to-hundreds on Linux with GRO --
# i.e. exactly the regime where the recv-path Datagrams coalescer can pay
# off. Keeping in-flight work constant isolates the variable to "how many
# flows" so any throughput delta vs. ING_ARGS is attributable to per-recv
# run length, not client-side CPU saturation.
ING_BATCH_ARGS="--connections 4 --workers-per-conn 256 --duration-secs 20 \
                --warmup-secs 3 --request-bytes 64 \
                --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --csv"
HS_ARGS="--total 200000 --concurrency 2048 --warmup 5000 \
         --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --csv"
# Contention bench: throughput measured first in isolation, then again while a
# dedicated churn-client runtime hammers the endpoint with short-lived
# connections as fast as the server will accept them. `degradation%` in the
# final table is (1 - mixed/baseline).
#
# Stream size is deliberately small (128M) and fanned out across
# connections/streams so the throughput phase is not client-AEAD-bound —
# otherwise the server has endless CPU headroom and server-side lock
# contention cannot show up as a mixed-phase slowdown. Rate limiting on the
# churn driver is disabled; `--churn-workers` is the concurrent-in-flight
# handshake ceiling.
CONT_ARGS="--connections 8 --streams-per-connection 4 --stream-size 128M \
           --duration-secs 15 --warmup-secs 3 \
           --churn-workers 64 \
           --server-threads $SERVER_THREADS \
           --client-threads $CONT_CLIENT_THREADS \
           --churn-threads $CONT_CHURN_THREADS --csv"

for b in "${BRANCHES[@]}"; do
  echo "=== Building $b ==="
  ( cd quinn && git checkout "$b" )
  cargo build --release --bin ingress_bench --bin handshake_bench --bin contention_bench
  for bench in ingress ingress_batch handshake contention; do
    for i in $(seq 1 "$REPEATS"); do
      out="$OUT/${bench}_bench-$b-run$i.txt"
      echo "--- $bench $b run $i ---"
      case "$bench" in
        ingress)       cmd="target/release/ingress_bench    $ING_ARGS       --label $b" ;;
        ingress_batch) cmd="target/release/ingress_bench    $ING_BATCH_ARGS --label $b" ;;
        handshake)     cmd="target/release/handshake_bench  $HS_ARGS        --label $b" ;;
        contention)    cmd="target/release/contention_bench $CONT_ARGS      --label $b" ;;
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
  local branch="$1" col="$2" bench="${3:-ingress}"
  _csv_lines "$branch" "$bench" | awk -F, -v c="$col" '{print $c}' | _median
}

# Print a contention comparison row. Columns (from contention_bench CSV):
#   1 CSV
#   2 label
#   3 connections
#   4 streams_per_connection
#   5 stream_bytes
#   6 churn_rate
#   7 churn_workers
#   8 server_threads
#   9 client_threads
#  10 mixed_wall_ms
#  11 baseline_mib_s
#  12 mixed_mib_s
#  13 achieved_churn (conn/s)
#  14 degradation_pct
_contention_field() {
  local branch="$1" col="$2"
  _csv_lines "$branch" contention | awk -F, -v c="$col" '{print $c}' | _median
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

# ---------------------------------------------------------------------------
# Ingress (batching) table
# ---------------------------------------------------------------------------
# Same binary as the Ingress table above, run with ING_BATCH_ARGS to produce
# long runs of same-connection datagrams in the recv path. This is the
# workload where the Datagrams coalescer should win; if the batching branch
# is not ahead here, the optimization is not earning its keep.
printf '\nIngress bench (batching, 4 conns x 256 workers)  (req/s, latency in ms)\n'
printf '%-28s %12s %9s %9s %9s %9s %9s\n' \
  branch throughput mean p50 p99 p99.9 max
printf -- '------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  t=$(_ingress_field "$b" 12 ingress_batch)
  mean=$(_ingress_field "$b" 14 ingress_batch)
  p50=$(_ingress_field "$b" 15 ingress_batch)
  p99=$(_ingress_field "$b" 18 ingress_batch)
  p999=$(_ingress_field "$b" 19 ingress_batch)
  max=$(_ingress_field "$b" 20 ingress_batch)
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
# Contention table
# ---------------------------------------------------------------------------
printf '\nContention bench  (MiB/s, churn conn/s, degradation %%)\n'
printf '%-28s %12s %12s %12s %9s\n' \
  branch baseline mixed churn degr%
printf -- '------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  baseline=$(_contention_field "$b" 11)
  mixed=$(_contention_field "$b" 12)
  churn=$(_contention_field "$b" 13)
  degr=$(_contention_field "$b" 14)
  if [ "$baseline" = "-" ]; then
    printf '%-28s %12s %12s %12s %9s\n' "$b" - - - -
  else
    printf '%-28s %12s %12s %12s %9s\n' \
      "$b" \
      "$(_fmt_throughput "$baseline")" \
      "$(_fmt_throughput "$mixed")" \
      "$(_fmt_throughput "$churn")" \
      "$(_fmt_throughput "$degr")"
  fi
done
echo
