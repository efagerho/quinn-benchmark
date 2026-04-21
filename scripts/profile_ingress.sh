#!/usr/bin/env bash
# profile_ingress.sh - produce a flamegraph + perf-stat report for ingress_bench
# on a given quinn branch, intended for run on the Linux dev host.
#
# Usage (from quinn-benchmarks repo root):
#   ./scripts/profile_ingress.sh <branch>
#
# Produces, under bench-results/profile/<branch>/:
#   ingress_bench   - archived release binary with DWARF symbols
#   flamegraph.svg  - cargo-flamegraph output for the 1024-conn ingress workload
#   perf.data       - raw perf samples from the flamegraph run
#   perf-stat.txt   - hardware counter summary for a separate run of the same workload
#
# Prerequisites (checked + auto-installed where possible):
#   - `cargo`                          installed via rustup, assumed present.
#   - `flamegraph` (cargo-flamegraph)  auto-installed via `cargo install flamegraph --locked`.
#   - `perf`                           must be present on PATH. On Debian/Ubuntu:
#                                        sudo apt install linux-tools-$(uname -r)
#                                      On Fedora/RHEL:
#                                        sudo dnf install perf
#   - kernel.perf_event_paranoid <= 1  run: sudo sysctl kernel.perf_event_paranoid=1
#
# Workload matches scripts/ab_bench.sh's ING_ARGS (1024 conns x 4 workers for 20s)
# so the profile reflects the exact config that showed the regression.
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <branch>" >&2
  exit 2
fi
BRANCH="$1"

# -----------------------------------------------------------------------------
# Prerequisites
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
Fix (persistent):
  echo 'kernel.perf_event_paranoid=1' | sudo tee /etc/sysctl.d/99-perf.conf
  sudo sysctl --system
EOF
  exit 1
fi

if ! command -v flamegraph >/dev/null 2>&1; then
  echo "flamegraph not found, installing cargo-flamegraph..."
  cargo install flamegraph --locked
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

# Same as ING_ARGS in scripts/ab_bench.sh. Keep in sync.
ING_ARGS=(
  --connections 1024
  --workers-per-conn 4
  --duration-secs 20
  --warmup-secs 3
  --request-bytes 64
  --server-threads "$SERVER_THREADS"
  --client-threads "$CLIENT_THREADS"
  --csv
  --label "$BRANCH"
)

echo "Host: $CORES physical cores -> server=$SERVER_THREADS client=$CLIENT_THREADS"

# -----------------------------------------------------------------------------
# Check out branch + build with debug symbols (already enabled in Cargo.toml)
# -----------------------------------------------------------------------------
if [ ! -d quinn ]; then
  echo "error: must run from quinn-benchmarks repo root (no quinn/ submodule found)" >&2
  exit 1
fi

echo "=== Checking out $BRANCH in quinn/ submodule ==="
( cd quinn && git checkout "$BRANCH" )

echo "=== Building ingress_bench in release mode ==="
unset CARGO_TARGET_DIR
cargo build --release --bin ingress_bench

OUTDIR="bench-results/profile/$BRANCH"
mkdir -p "$OUTDIR"
cp target/release/ingress_bench "$OUTDIR/ingress_bench"

# -----------------------------------------------------------------------------
# Flamegraph run
# -----------------------------------------------------------------------------
# --freq 999:      avoids lockstep sampling with any 1 kHz periodic activity.
# --call-graph dwarf: Rust async stacks go through frames that fp-based unwinding
#                     drops. DWARF is more expensive but produces readable traces.
# --no-inline:     leave off; inlining is fine for a release binary and the
#                  flamegraph resolver handles it.
#
# cargo-flamegraph leaves perf.data in the cwd. We move it into $OUTDIR after.
echo "=== Flamegraph run for $BRANCH ==="
rm -f perf.data perf.data.old
flamegraph \
  --freq 999 \
  --call-graph dwarf \
  -o "$OUTDIR/flamegraph.svg" \
  -- target/release/ingress_bench "${ING_ARGS[@]}"
if [ -f perf.data ]; then
  mv perf.data "$OUTDIR/perf.data"
fi
rm -f perf.data.old

# -----------------------------------------------------------------------------
# perf stat run (separate run so flamegraph's sampling doesn't perturb counters)
# -----------------------------------------------------------------------------
# Hardware events chosen to separate instruction-count deltas from cache /
# branch-predictor deltas. LLC-{load,store}-misses highlight cross-core
# coherence traffic, which was the dominant cost before the enum-size fix.
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
perf stat -e "$PERF_EVENTS_CSV" -- target/release/ingress_bench "${ING_ARGS[@]}" \
  2>&1 | tee "$OUTDIR/perf-stat.txt"

echo
echo "=== Artifacts for $BRANCH ==="
ls -la "$OUTDIR"
