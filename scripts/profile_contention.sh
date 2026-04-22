#!/usr/bin/env bash
# profile_contention.sh - produce a flamegraph + perf-stat report for
# contention_bench on a given quinn branch. Mirrors scripts/profile_ingress.sh
# but drives the contention_bench binary with the same knobs ab_bench.sh
# uses for its Contention table, so profile artefacts and aggregate-table
# numbers refer to the same workload.
#
# Usage (from quinn-benchmarks repo root):
#   ./scripts/profile_contention.sh <branch>
#
# Produces, under bench-results/profile-contention/<branch>/:
#   contention_bench  - archived release binary with DWARF symbols
#   flamegraph.svg    - cargo-flamegraph output for the mixed-phase run
#   perf.data         - raw perf samples from the flamegraph run
#   perf-stat.txt     - hardware counter summary for a separate run
#
# Prerequisites: same as profile_ingress.sh (perf + inferno + perf_event_paranoid<=1).
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <branch>" >&2
  exit 2
fi
BRANCH="$1"

# -----------------------------------------------------------------------------
# Prerequisites (same as profile_ingress.sh)
# -----------------------------------------------------------------------------
if ! command -v perf >/dev/null 2>&1; then
  cat >&2 <<EOF
error: 'perf' not found on PATH.

Install it and re-run:
  Debian/Ubuntu: sudo apt install linux-tools-\$(uname -r) linux-tools-common
  Fedora/RHEL:   sudo dnf install perf
  Arch:          sudo pacman -S perf
EOF
  exit 1
fi

PARANOID=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 3)
if [ "$PARANOID" -gt 1 ]; then
  cat >&2 <<EOF
error: kernel.perf_event_paranoid=$PARANOID (needs to be <= 1 for userspace sampling).

Fix (ephemeral):
  sudo sysctl kernel.perf_event_paranoid=1
EOF
  exit 1
fi

if ! command -v inferno-flamegraph >/dev/null 2>&1 \
  || ! command -v inferno-collapse-perf >/dev/null 2>&1; then
  echo "inferno tooling not found, installing inferno..."
  cargo install inferno --locked
fi

# -----------------------------------------------------------------------------
# Core-detection (mirrors scripts/ab_bench.sh so the workload is identical)
# -----------------------------------------------------------------------------
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

# Contention runs two client runtimes (throughput + churn) in the same
# process. Match ab_bench.sh: split CLIENT_THREADS evenly between them.
CONT_CLIENT_THREADS=${CONT_CLIENT_THREADS:-$(( (CLIENT_THREADS + 1) / 2 ))}
CONT_CHURN_THREADS=${CONT_CHURN_THREADS:-$(( CLIENT_THREADS / 2 ))}
[ "$CONT_CLIENT_THREADS" -lt 1 ] && CONT_CLIENT_THREADS=1
[ "$CONT_CHURN_THREADS" -lt 1 ] && CONT_CHURN_THREADS=1

OUTDIR_TAG=${OUTDIR_TAG:-}

# Mirrors CONT_ARGS from ab_bench.sh. Keep in sync.
CONT_ARGS=(
  --connections 8
  --streams-per-connection 4
  --stream-size 128M
  --duration-secs 15
  --warmup-secs 3
  --churn-workers 64
  --churn-rate 2000
  --server-threads "$SERVER_THREADS"
  --client-threads "$CONT_CLIENT_THREADS"
  --churn-threads "$CONT_CHURN_THREADS"
  --csv
  --label "$BRANCH$OUTDIR_TAG"
)

echo "Host: $CORES physical cores -> server=$SERVER_THREADS throughput=$CONT_CLIENT_THREADS churn=$CONT_CHURN_THREADS"

# -----------------------------------------------------------------------------
# Check out branch + build with debug symbols
# -----------------------------------------------------------------------------
if [ ! -d quinn ]; then
  echo "error: must run from quinn-benchmarks repo root (no quinn/ submodule found)" >&2
  exit 1
fi

echo "=== Checking out $BRANCH in quinn/ submodule ==="
( cd quinn && git checkout "$BRANCH" )

echo "=== Building contention_bench in release mode ==="
unset CARGO_TARGET_DIR
cargo build --release --bin contention_bench

OUTDIR="bench-results/profile-contention/$BRANCH$OUTDIR_TAG"
mkdir -p "$OUTDIR"
cp target/release/contention_bench "$OUTDIR/contention_bench"

# -----------------------------------------------------------------------------
# Flamegraph run
# -----------------------------------------------------------------------------
# The contention bench runs two ~15 s throughput phases back-to-back with a
# 3 s churn warmup between them, so the perf.data covers baseline + mixed in
# a single recording. That is exactly what we want: the flamegraph shows how
# the hot path shifts once churn is in flight.
echo "=== perf record (DWARF call-graph) for $BRANCH ==="
perf record \
  -F 999 \
  --call-graph dwarf,16384 \
  -g \
  -o "$OUTDIR/perf.data" \
  -- target/release/contention_bench "${CONT_ARGS[@]}"

echo "=== Rendering flamegraph via inferno ==="
perf script -i "$OUTDIR/perf.data" \
  | inferno-collapse-perf > "$OUTDIR/perf.folded"
inferno-flamegraph \
  --title "contention_bench $BRANCH" \
  --subtitle "8 conns x 4 streams, 128M, 15s baseline+mixed, 64 churn workers" \
  < "$OUTDIR/perf.folded" \
  > "$OUTDIR/flamegraph.svg"

# -----------------------------------------------------------------------------
# perf stat run (separate run so flamegraph's sampling doesn't perturb counters)
# -----------------------------------------------------------------------------
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
perf stat -e "$PERF_EVENTS_CSV" -- target/release/contention_bench "${CONT_ARGS[@]}" \
  2>&1 | tee "$OUTDIR/perf-stat.txt"

echo
echo "=== Artifacts for $BRANCH ==="
ls -la "$OUTDIR"
