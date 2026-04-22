#!/usr/bin/env bash
# Re-run ONLY the aggregation / summary portion of ab_bench.sh against an
# existing results directory, so we don't have to re-run the benchmarks just
# to reprint the tables.
set -euo pipefail

BRANCHES=(
  endpoint-lock-optimization-accept-split
  endpoint-lock-optimization-accept-split-response-buffer-hoist-plus-lazy-streams
)
OUT=${1:-bench-results-accept-split-vs-lazy-streams}
REPEATS=${REPEATS:-5}

_csv_lines() {
  local branch="$1" bench="$2"
  local f
  for f in "$OUT/${bench}_bench-${branch}-run"*.txt; do
    [ -f "$f" ] || continue
    grep '^CSV,' "$f" || true
  done
}

_median() {
  sort -n | awk '
    { a[NR] = $1 + 0 }
    END {
      if (NR == 0) { print "-"; exit }
      if (NR % 2) print a[(NR+1)/2]
      else        print (a[NR/2] + a[NR/2+1]) / 2.0
    }'
}

_handshake_field() {
  local branch="$1" col="$2"
  _csv_lines "$branch" handshake | awk -F, -v c="$col" '{print $c}' | _median
}
_ingress_field() {
  local branch="$1" col="$2" bench="${3:-ingress}"
  _csv_lines "$branch" "$bench" | awk -F, -v c="$col" '{print $c}' | _median
}
_contention_field() {
  local branch="$1" col="$2"
  _csv_lines "$branch" contention | awk -F, -v c="$col" '{print $c}' | _median
}
_stream_open_field() {
  local branch="$1" col="$2"
  _csv_lines "$branch" stream_open | awk -F, -v c="$col" '{print $c}' | _median
}
_us_to_ms() { awk 'BEGIN { x = ARGV[1] + 0; printf("%.2f", x / 1000.0) }' "$1"; }
_fmt()      { awk 'BEGIN { x = ARGV[1] + 0; printf("%.1f", x) }' "$1"; }

echo
echo "============================================================"
echo "  Comparison tables (median across $REPEATS runs per branch)"
echo "  results dir: $OUT"
echo "============================================================"

printf '\nHandshake bench  (conn/s, latency in ms)\n'
printf '%-70s %12s %9s %9s %9s %9s %9s\n' branch throughput mean p50 p99 p99.9 max
printf -- '------------------------------------------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  t=$(_handshake_field "$b" 10)
  if [ "$t" = "-" ]; then
    printf '%-70s %12s %9s %9s %9s %9s %9s\n' "$b" - - - - - -
    continue
  fi
  printf '%-70s %12s %9s %9s %9s %9s %9s\n' "$b" \
    "$(_fmt "$t")" \
    "$(_us_to_ms "$(_handshake_field "$b" 11)")" \
    "$(_us_to_ms "$(_handshake_field "$b" 12)")" \
    "$(_us_to_ms "$(_handshake_field "$b" 15)")" \
    "$(_us_to_ms "$(_handshake_field "$b" 16)")" \
    "$(_us_to_ms "$(_handshake_field "$b" 17)")"
done

printf '\nIngress bench  (req/s, latency in ms)\n'
printf '%-70s %12s %9s %9s %9s %9s %9s\n' branch throughput mean p50 p99 p99.9 max
printf -- '------------------------------------------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  t=$(_ingress_field "$b" 12)
  if [ "$t" = "-" ]; then
    printf '%-70s %12s %9s %9s %9s %9s %9s\n' "$b" - - - - - -
    continue
  fi
  printf '%-70s %12s %9s %9s %9s %9s %9s\n' "$b" \
    "$(_fmt "$t")" \
    "$(_us_to_ms "$(_ingress_field "$b" 14)")" \
    "$(_us_to_ms "$(_ingress_field "$b" 15)")" \
    "$(_us_to_ms "$(_ingress_field "$b" 18)")" \
    "$(_us_to_ms "$(_ingress_field "$b" 19)")" \
    "$(_us_to_ms "$(_ingress_field "$b" 20)")"
done

printf '\nIngress bench (batching, 4 conns x 256 workers)  (req/s, latency in ms)\n'
printf '%-70s %12s %9s %9s %9s %9s %9s\n' branch throughput mean p50 p99 p99.9 max
printf -- '------------------------------------------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  t=$(_ingress_field "$b" 12 ingress_batch)
  if [ "$t" = "-" ]; then
    printf '%-70s %12s %9s %9s %9s %9s %9s\n' "$b" - - - - - -
    continue
  fi
  printf '%-70s %12s %9s %9s %9s %9s %9s\n' "$b" \
    "$(_fmt "$t")" \
    "$(_us_to_ms "$(_ingress_field "$b" 14 ingress_batch)")" \
    "$(_us_to_ms "$(_ingress_field "$b" 15 ingress_batch)")" \
    "$(_us_to_ms "$(_ingress_field "$b" 18 ingress_batch)")" \
    "$(_us_to_ms "$(_ingress_field "$b" 19 ingress_batch)")" \
    "$(_us_to_ms "$(_ingress_field "$b" 20 ingress_batch)")"
done

printf '\nContention bench  (MiB/s, churn conn/s, degradation %%)\n'
printf '%-70s %12s %12s %12s %9s\n' branch baseline mixed churn degr%
printf -- '------------------------------------------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  baseline=$(_contention_field "$b" 11)
  if [ "$baseline" = "-" ]; then
    printf '%-70s %12s %12s %12s %9s\n' "$b" - - - -
    continue
  fi
  printf '%-70s %12s %12s %12s %9s\n' "$b" \
    "$(_fmt "$baseline")" \
    "$(_fmt "$(_contention_field "$b" 12)")" \
    "$(_fmt "$(_contention_field "$b" 13)")" \
    "$(_fmt "$(_contention_field "$b" 14)")"
done

# Stream open table (CSV columns: 13 srv thr, 14 cli thr, 16 mean_us, 17 p50_us,
# 20 p99_us, 21 p999_us, 22 max_us — see scripts/ab_stream_open.sh).
printf '\nStream open bench  (streams/s, latency in ms)\n'
printf '%-70s %14s %14s %9s %9s %9s %9s %9s\n' branch "srv str/s" "cli str/s" mean p50 p99 p99.9 max
printf -- '------------------------------------------------------------------------------------------------------------------------------\n'
for b in "${BRANCHES[@]}"; do
  srv=$(_stream_open_field "$b" 13)
  if [ "$srv" = "-" ]; then
    printf '%-70s %14s %14s %9s %9s %9s %9s %9s\n' "$b" - - - - - - -
    continue
  fi
  printf '%-70s %14s %14s %9s %9s %9s %9s %9s\n' "$b" \
    "$(_fmt "$srv")" \
    "$(_fmt "$(_stream_open_field "$b" 14)")" \
    "$(_us_to_ms "$(_stream_open_field "$b" 16)")" \
    "$(_us_to_ms "$(_stream_open_field "$b" 17)")" \
    "$(_us_to_ms "$(_stream_open_field "$b" 20)")" \
    "$(_us_to_ms "$(_stream_open_field "$b" 21)")" \
    "$(_us_to_ms "$(_stream_open_field "$b" 22)")"
done
echo
