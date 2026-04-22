#!/usr/bin/env bash
# A/B driver for stream_open_bench: opens a fresh unidirectional stream,
# writes a fixed 16-byte payload, finishes. Compares a list of quinn
# submodule branches.
set -euo pipefail

BRANCHES=(
  endpoint-lock-optimization-accept-split
  endpoint-lock-optimization-accept-split-response-buffer-hoist-plus
)
OUT=bench-results
REPEATS=${REPEATS:-5}
mkdir -p "$OUT"

# Match ab_bench.sh core-sizing conventions so results on the same host are
# comparable across benches.
_detect_cores() {
  if [ -n "${CORES:-}" ]; then echo "$CORES"; return; fi
  case "$(uname -s)" in
    Darwin) sysctl -n hw.physicalcpu 2>/dev/null && return ;;
    Linux)
      if command -v lscpu >/dev/null 2>&1; then
        local n
        n=$(lscpu -b -p=Core,Socket 2>/dev/null \
              | awk -F, '!/^#/ { print $1"-"$2 }' | sort -u | wc -l)
        if [ "${n:-0}" -gt 0 ]; then echo "$n"; return; fi
      fi
      if [ -r /proc/cpuinfo ]; then
        local n
        n=$(awk '/^processor/ { c++ } END { print c+0 }' /proc/cpuinfo)
        if [ "${n:-0}" -gt 0 ]; then echo "$n"; return; fi
      fi ;;
  esac
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

echo "Detected $CORES physical cores -> server=$SERVER_THREADS client=$CLIENT_THREADS"

# Match ingress_bench's standard load profile so the stream-open bench is
# directly comparable: 1024 long-lived connections x 4 workers each, 20 s
# measurement phase, 3 s warmup.
SO_ARGS="--connections 1024 --workers-per-conn 4 --duration-secs 20 \
         --warmup-secs 3 \
         --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --csv"

for b in "${BRANCHES[@]}"; do
  echo "=== Building $b ==="
  ( cd quinn && git checkout "$b" )
  cargo build --release --bin stream_open_bench
  for i in $(seq 1 "$REPEATS"); do
    out="$OUT/stream_open_bench-$b-run$i.txt"
    echo "--- stream_open $b run $i ---"
    target/release/stream_open_bench $SO_ARGS --label "$b" | tee "$out"
  done
done

echo
echo "============================================================"
echo "  stream_open_bench summary (mean across $REPEATS runs)"
echo "============================================================"

_csv_lines() {
  local branch="$1" f
  for f in "$OUT/stream_open_bench-${branch}-run"*.txt; do
    [ -f "$f" ] || continue
    grep '^CSV,' "$f" || true
  done
}

# Stats: mean, stddev, min, max, count for a given CSV column across runs.
_stats() {
  local branch="$1" col="$2"
  _csv_lines "$branch" | awk -F, -v c="$col" '
    {
      n++
      v = $c + 0
      s += v
      ss += v*v
      if (n == 1 || v < mn) mn = v
      if (n == 1 || v > mx) mx = v
    }
    END {
      if (n == 0) { printf "-\t-\t-\t-\t0\n"; exit }
      mean = s / n
      var = ss/n - mean*mean
      if (var < 0) var = 0
      sd = sqrt(var)
      printf "%.1f\t%.1f\t%.1f\t%.1f\t%d\n", mean, sd, mn, mx, n
    }'
}

# CSV columns for stream_open_bench (see bin source for canonical layout):
#  1 CSV, 2 label, 3 connections, 4 workers, 5 duration, 6 payload_bytes,
#  7 server_threads, 8 client_threads, 9 client_sends, 10 server_recvs,
#  11 errors, 12 wall_ms, 13 server_throughput, 14 client_throughput,
#  15 bps, 16 mean_us, 17 p50_us, 18 p90_us, 19 p95_us, 20 p99_us,
#  21 p999_us, 22 max_us, 23 server_shards, 24 client_endpoints
printf '\n%-14s %14s %7s | %14s %7s | %9s %9s %9s %9s\n' \
  branch "srv streams/s" "sd%" "cli streams/s" "sd%" "mean" "p50" "p99" "p99.9"
printf -- '--------------------------------------------------------------------------------------------------\n'

shorten() {
  case "$1" in
    endpoint-lock-optimization-accept-split) echo "accept-split" ;;
    endpoint-lock-optimization-accept-split-response-buffer-hoist-plus) echo "hoist-plus" ;;
    *) echo "$1" ;;
  esac
}

fmt_sd_pct() {
  awk -v m="$1" -v s="$2" 'BEGIN { if (m > 0) printf "(%.2f%%)", 100*s/m; else printf "(-)" }'
}
fmt_us_ms() {
  awk -v v="$1" 'BEGIN { printf "%9.3f", v/1000.0 }'
}
delta_pct() {
  awk -v v="$1" -v b="$2" 'BEGIN { if (b > 0) printf "%+7.2f%%", 100*(v-b)/b; else printf "   -   " }'
}

for b in "${BRANCHES[@]}"; do
  sn=$(shorten "$b")
  read srv srv_sd _ _ _ <<<"$(_stats "$b" 13)"
  read cli cli_sd _ _ _ <<<"$(_stats "$b" 14)"
  read mean _ _ _ _    <<<"$(_stats "$b" 16)"
  read p50  _ _ _ _    <<<"$(_stats "$b" 17)"
  read p99  _ _ _ _    <<<"$(_stats "$b" 20)"
  read p999 _ _ _ _    <<<"$(_stats "$b" 21)"
  printf '%-14s %14.0f %7s | %14.0f %7s | %s %s %s %s\n' \
    "$sn" "$srv" "$(fmt_sd_pct "$srv" "$srv_sd")" \
    "$cli" "$(fmt_sd_pct "$cli" "$cli_sd")" \
    "$(fmt_us_ms "$mean")" "$(fmt_us_ms "$p50")" \
    "$(fmt_us_ms "$p99")" "$(fmt_us_ms "$p999")"
done

# Deltas vs the first branch (treated as baseline).
baseline_branch="${BRANCHES[0]}"
read base_srv _ _ _ _ <<<"$(_stats "$baseline_branch" 13)"
read base_cli _ _ _ _ <<<"$(_stats "$baseline_branch" 14)"
read base_mean _ _ _ _ <<<"$(_stats "$baseline_branch" 16)"
read base_p99 _ _ _ _ <<<"$(_stats "$baseline_branch" 20)"

printf '\nDeltas vs %s:\n' "$(shorten "$baseline_branch")"
printf '%-14s %18s %18s %18s %18s\n' branch "srv throughput" "cli throughput" "mean lat" "p99 lat"
printf -- '--------------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  sn=$(shorten "$b")
  read srv _ _ _ _ <<<"$(_stats "$b" 13)"
  read cli _ _ _ _ <<<"$(_stats "$b" 14)"
  read mean _ _ _ _ <<<"$(_stats "$b" 16)"
  read p99 _ _ _ _ <<<"$(_stats "$b" 20)"
  printf '%-14s %18s %18s %18s %18s\n' \
    "$sn" \
    "$(delta_pct "$srv"  "$base_srv")" \
    "$(delta_pct "$cli"  "$base_cli")" \
    "$(delta_pct "$mean" "$base_mean")" \
    "$(delta_pct "$p99"  "$base_p99")"
done
echo
