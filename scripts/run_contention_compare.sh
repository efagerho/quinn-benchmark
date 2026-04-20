#!/usr/bin/env bash
# Multi-process contention benchmark driver.
#
# Ported from `bench/run_contention_compare.sh` on quinn's
# `endpoint-lock-optimization-accept-split` branch. Drives the standalone
# `contention_server`, `contention_throughput_client` and
# `contention_churn_client` binaries built out of this crate.
#
# Runs two phases against a fresh server on each phase:
#   1. Baseline: throughput-client only.
#   2. Mixed:    churn-client + (after MIXED_WARMUP_SECS) throughput-client.
# Then extracts the key=value lines emitted by the clients and prints a
# summary with baseline / mixed MiB/s, achieved churn conn/s and the
# (1 - mixed/baseline) degradation %.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${BIN_DIR:-$ROOT_DIR/target/release}"

SERVER_BIN="$BIN_DIR/contention_server"
THROUGHPUT_BIN="$BIN_DIR/contention_throughput_client"
NEW_CONN_BIN="$BIN_DIR/contention_churn_client"

LISTEN="${LISTEN:-127.0.0.1:4433}"
SERVER_WORKER_THREADS="${SERVER_WORKER_THREADS:-8}"
SERVER_INITIAL_MTU="${SERVER_INITIAL_MTU:-1200}"
SERVER_MAX_CONCURRENT_UNI_STREAMS="${SERVER_MAX_CONCURRENT_UNI_STREAMS:-131072}"
SERVER_READ_UNORDERED="${SERVER_READ_UNORDERED:-0}"

THROUGHPUT_DURATION_SECS="${THROUGHPUT_DURATION_SECS:-10}"
THROUGHPUT_CONNECTIONS="${THROUGHPUT_CONNECTIONS:-1}"
THROUGHPUT_STREAMS_PER_CONNECTION="${THROUGHPUT_STREAMS_PER_CONNECTION:-1}"
THROUGHPUT_STREAM_SIZE="${THROUGHPUT_STREAM_SIZE:-1600M}"
THROUGHPUT_STREAM_RUNS="${THROUGHPUT_STREAM_RUNS:-1}"
THROUGHPUT_INITIAL_MTU="${THROUGHPUT_INITIAL_MTU:-1200}"

NEW_CONN_RATE="${NEW_CONN_RATE:-2500}"
NEW_CONN_WORKERS="${NEW_CONN_WORKERS:-16}"
NEW_CONN_INITIAL_MTU="${NEW_CONN_INITIAL_MTU:-1200}"
MIXED_WARMUP_SECS="${MIXED_WARMUP_SECS:-2}"

# If NEW_CONN_DURATION_SECS is set explicitly, honour it. Otherwise pick
# something that comfortably overlaps the mixed throughput window:
#   - stream-runs mode: duration-based cutoff is disabled, so let the churn
#     driver run a bit longer than the throughput run is expected to take.
#   - duration mode: a fixed 30s is enough for almost any reasonable
#     THROUGHPUT_DURATION_SECS.
if [[ -n "${NEW_CONN_DURATION_SECS:-}" ]]; then
    CHURN_DURATION_SECS="$NEW_CONN_DURATION_SECS"
elif [[ "$THROUGHPUT_STREAM_RUNS" == "0" ]]; then
    CHURN_DURATION_SECS="$((THROUGHPUT_DURATION_SECS + MIXED_WARMUP_SECS + 5))"
else
    CHURN_DURATION_SECS="30"
fi

TMP_DIR="$(mktemp -d)"
SERVER_PID=""
CHURN_PID=""

cleanup() {
    if [[ -n "$CHURN_PID" ]]; then
        kill "$CHURN_PID" >/dev/null 2>&1 || true
        wait "$CHURN_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    rm -rf "$TMP_DIR"
}

trap cleanup EXIT

require_bin() {
    local bin="$1"
    if [[ ! -x "$bin" ]]; then
        echo "missing binary: $bin" >&2
        echo "build once with: cargo build --release \\" >&2
        echo "    --bin contention_server \\" >&2
        echo "    --bin contention_throughput_client \\" >&2
        echo "    --bin contention_churn_client" >&2
        exit 1
    fi
}

start_server() {
    local log_file="$1"
    local -a args=(
        --listen "$LISTEN"
        --worker-threads "$SERVER_WORKER_THREADS"
        --initial-mtu "$SERVER_INITIAL_MTU"
        --max-concurrent-uni-streams "$SERVER_MAX_CONCURRENT_UNI_STREAMS"
    )
    if [[ "$SERVER_READ_UNORDERED" == "1" ]]; then
        args+=(--read-unordered)
    fi
    "$SERVER_BIN" "${args[@]}" >"$log_file" 2>&1 &
    SERVER_PID="$!"
    # Give the endpoint a chance to bind before the first client dials.
    sleep 1
}

stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
        SERVER_PID=""
    fi
}

extract_value() {
    local key="$1"
    local file="$2"
    awk -F= -v key="$key" '$1 == key { print $2; exit }' "$file"
}

baseline_server_log="$TMP_DIR/server_baseline.log"
mixed_server_log="$TMP_DIR/server_mixed.log"
baseline_throughput_log="$TMP_DIR/throughput_baseline.log"
mixed_throughput_log="$TMP_DIR/throughput_mixed.log"
mixed_new_conn_log="$TMP_DIR/new_conn_mixed.log"

require_bin "$SERVER_BIN"
require_bin "$THROUGHPUT_BIN"
require_bin "$NEW_CONN_BIN"

printf 'Using binaries from %s\n' "$BIN_DIR"
printf 'listen=%s\n' "$LISTEN"
printf 'throughput: connections=%s streams_per_connection=%s stream_size=%s' \
    "$THROUGHPUT_CONNECTIONS" "$THROUGHPUT_STREAMS_PER_CONNECTION" "$THROUGHPUT_STREAM_SIZE"
if [[ "$THROUGHPUT_STREAM_RUNS" != "0" ]]; then
    printf ' stream_runs=%s' "$THROUGHPUT_STREAM_RUNS"
else
    printf ' duration_secs=%s' "$THROUGHPUT_DURATION_SECS"
fi
printf '\n'
printf 'new-conn: rate=%s/s workers=%s duration_secs=%s warmup_secs=%s\n' \
    "$NEW_CONN_RATE" "$NEW_CONN_WORKERS" "$CHURN_DURATION_SECS" "$MIXED_WARMUP_SECS"
printf '\n'

baseline_args=(
    --connect "$LISTEN"
    --duration-secs "$THROUGHPUT_DURATION_SECS"
    --connections "$THROUGHPUT_CONNECTIONS"
    --streams-per-connection "$THROUGHPUT_STREAMS_PER_CONNECTION"
    --stream-size "$THROUGHPUT_STREAM_SIZE"
    --initial-mtu "$THROUGHPUT_INITIAL_MTU"
)
if [[ "$THROUGHPUT_STREAM_RUNS" != "0" ]]; then
    baseline_args+=(--stream-runs "$THROUGHPUT_STREAM_RUNS")
fi

mixed_new_conn_args=(
    --connect "$LISTEN"
    --duration-secs "$CHURN_DURATION_SECS"
    --connections-per-second "$NEW_CONN_RATE"
    --workers "$NEW_CONN_WORKERS"
    --initial-mtu "$NEW_CONN_INITIAL_MTU"
)

start_server "$baseline_server_log"
"$THROUGHPUT_BIN" "${baseline_args[@]}" | tee "$baseline_throughput_log"
stop_server

printf '\n'

start_server "$mixed_server_log"
"$NEW_CONN_BIN" "${mixed_new_conn_args[@]}" >"$mixed_new_conn_log" 2>&1 &
CHURN_PID="$!"
sleep "$MIXED_WARMUP_SECS"
"$THROUGHPUT_BIN" "${baseline_args[@]}" | tee "$mixed_throughput_log"
wait "$CHURN_PID"
CHURN_PID=""
stop_server

baseline_tput="$(extract_value throughput_mib_per_s "$baseline_throughput_log")"
mixed_tput="$(extract_value throughput_mib_per_s "$mixed_throughput_log")"
baseline_elapsed="$(extract_value elapsed_secs "$baseline_throughput_log")"
mixed_elapsed="$(extract_value elapsed_secs "$mixed_throughput_log")"
mixed_rate="$(extract_value achieved_connections_per_second "$mixed_new_conn_log")"

degradation="$(awk -v baseline="$baseline_tput" -v mixed="$mixed_tput" 'BEGIN {
    if (baseline == 0) {
        printf "0.00";
    } else {
        printf "%.2f", (1 - mixed / baseline) * 100;
    }
}')"

printf '\nSummary\n'
printf 'baseline throughput: %s MiB/s (elapsed %ss)\n' "$baseline_tput" "$baseline_elapsed"
printf 'mixed throughput:    %s MiB/s (elapsed %ss)\n' "$mixed_tput" "$mixed_elapsed"
printf 'mixed new-conn rate: %s /s\n' "$mixed_rate"
printf 'degradation:         %s%%\n' "$degradation"
