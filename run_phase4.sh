#!/usr/bin/env bash
# Phase 4 - fault tolerance experiment.
# Runs the 3-server deployment under constant load, kills one server in the
# middle of the run, and plots the per-request latency timeline.
set -euo pipefail

RATE=110
DURATION=20
KILL_DELAY=8                 # Seconds after the benchmark starts to inject the failure
TARGET_SERVER="server2"
FAILURE_MODE="kill"           # "kill" (abrupt crash / SIGKILL) or "stop" (graceful SIGTERM)
LB_ALGORITHM="least_connections"   # least_connections or least_response_time
SEED=42
BENCH_FILES="LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt"

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="Experiments/phase4_${RUN_ID}_${LB_ALGORITHM}"
CLIENT_LOG="$RUN_DIR/client.log"
EVENT_LOG="$RUN_DIR/events.log"
SUMMARY_OUT="$RUN_DIR/results.csv"
TRACE_NAME="server_failure_trace.csv"

flush() {
  docker compose exec -T cache redis-cli FLUSHALL > /dev/null
}

docker compose down --remove-orphans
docker compose build client load_balancer server1
LB_ALGORITHM="$LB_ALGORITHM" docker compose up -d --wait cache server1 server2 server3 load_balancer

mkdir -p "$RUN_DIR"
{
  echo "run_id=$RUN_ID"
  echo "algorithm=$LB_ALGORITHM"
  echo "rate=$RATE"
  echo "duration=$DURATION"
  echo "kill_delay=$KILL_DELAY"
  echo "target_server=$TARGET_SERVER"
  echo "failure_mode=$FAILURE_MODE"
  echo "seed=$SEED"
  echo "bench_files=$BENCH_FILES"
} > "$RUN_DIR/manifest.txt"
: > "$CLIENT_LOG"
: > "$EVENT_LOG"

# Warm-up with all servers up (output discarded)
docker compose run --rm -T --no-deps \
  -e RATE=10 -e DURATION=5 -e BENCH_FILES="$BENCH_FILES" -e SEED="$SEED" \
  client > /dev/null 2>&1

flush

# Background job: wait until the client really starts sending requests
# (the client logs "benchmark start"), then inject the failure KILL_DELAY seconds later.
(
  until grep -q "benchmark start" "$CLIENT_LOG" 2>/dev/null; do sleep 0.05; done
  sleep "$KILL_DELAY"
  echo "[$(date +%H:%M:%S)] injecting fault: $FAILURE_MODE $TARGET_SERVER" >> "$EVENT_LOG"
  if [ "$FAILURE_MODE" = "kill" ]; then
    docker compose kill "$TARGET_SERVER" > /dev/null 2>&1
  else
    docker compose stop -t 1 "$TARGET_SERVER" > /dev/null 2>&1
  fi
) &
KILLER_PID=$!

# --no-deps: do not let compose restart the stopped server as a dependency of the client
if ! line=$(docker compose run --rm -T --no-deps \
  -v "$(pwd)/$RUN_DIR:/results" \
  -e RATE="$RATE" -e DURATION="$DURATION" -e BENCH_FILES="$BENCH_FILES" \
  -e SEED="$SEED" -e HIT_RATE=0 -e TRACE_FILE="/results/$TRACE_NAME" \
  client 2>> "$CLIENT_LOG"); then
  kill "$KILLER_PID" 2>/dev/null || true
  echo "client failed, see $CLIENT_LOG" >&2
  exit 1
fi
wait "$KILLER_PID" || true

echo "rate,duration,avg_ms,p99_ms,max_lag_ms,errors,n_requests,killed_server,kill_delay" > "$SUMMARY_OUT"
echo "$line,$TARGET_SERVER,$KILL_DELAY" >> "$SUMMARY_OUT"

docker compose logs --no-color load_balancer > "$RUN_DIR/load_balancer.log" 2>&1 || true

# Plot the per-request timeline with the failure marker
docker compose run --rm -T --no-deps \
  -v "$(pwd)/$RUN_DIR:/results" \
  client python plot_results.py --trace "/results/$TRACE_NAME" \
  --failure-time "$KILL_DELAY" --output-dir /results

# Bring the failed server back so the deployment is left healthy
docker compose start "$TARGET_SERVER" > /dev/null

echo "Results in $RUN_DIR" >&2
