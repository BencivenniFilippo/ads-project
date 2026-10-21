#!/usr/bin/env bash
set -euo pipefail

RATES=(50) # 70 90 110 130)
REPEATS=1
DURATION=30
KILL_DELAY=15          # Seconds after client starts to stop the server
TARGET_SERVER="server2"
FAILURE_MODE="kill"    # "kill" (abrupt crash / SIGKILL) or "stop" (graceful SIGTERM)

SEED=42
BENCH_FILES="LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt"
mkdir -p Experiments
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
EXP_OUT="Experiments/fault_experiment_${TIMESTAMP}.csv"
LOG_OUT="Experiments/fault_log_${TIMESTAMP}.log"

flush() {
  docker compose exec -T cache redis-cli FLUSHALL > /dev/null
}

run_client() {
  # --no-deps prevents docker compose from automatically restarting stopped dependencies
  local rate="$1"
  local duration="$2"
  local trace_file="${3:-}"

  docker compose run --rm --no-deps -T \
    -v "$(pwd)/Experiments:/app/Experiments" \
    -e RATE="$rate" \
    -e DURATION="$duration" \
    -e BENCH_FILES="$BENCH_FILES" \
    -e SEED="$SEED" \
    -e TRACE_OUT="$trace_file" \
    client
}

docker compose build client load_balancer server1
docker compose up -d --wait cache server1 server2 server3 load_balancer

echo "rate,duration,avg_ms,p99_ms,max_lag_ms,errors,n_requests,repeat,killed_server,kill_delay" > "$EXP_OUT"

# Warm-up
run_client 10 5 > /dev/null 2>&1

for rate in "${RATES[@]}"; do
  for rep in $(seq 1 "$REPEATS"); do
    flush
    client_log=$(mktemp)

    # Schedule the server failure in the background
    (
      sleep "$KILL_DELAY"
      echo "[$(date +%H:%M:%S)] Injecting fault: docker compose $FAILURE_MODE $TARGET_SERVER" >> "$LOG_OUT"
      if [ "$FAILURE_MODE" = "kill" ]; then
        docker compose kill "$TARGET_SERVER" > /dev/null 2>&1
      else
        docker compose stop -t 1 "$TARGET_SERVER" > /dev/null 2>&1
      fi
    ) &
    killer_pid=$!

    # Run the client workload in the foreground
    trace_csv="/app/Experiments/trace_rate${rate}_rep${rep}_${TIMESTAMP}.csv"
    line=$(run_client "$rate" "$DURATION" "$trace_csv" 2> "$client_log")

    # Ensure the background killer job has finished
    wait "$killer_pid" || true

    # Collect client & load balancer warnings/errors
    grep -E "WARNING|ERROR|CRITICAL" "$client_log" >> "$LOG_OUT" || true
    docker compose logs --since "${DURATION}s" load_balancer >> "$LOG_OUT" 2>&1 || true
    rm "$client_log"

    echo "$line,$rep,$TARGET_SERVER,$KILL_DELAY" >> "$EXP_OUT"
    echo "done rate=$rate repeat=$rep (killed $TARGET_SERVER at ${KILL_DELAY}s)" >&2

    # Restart the downed server and wait for the load balancer health check to pick it up
    docker compose start "$TARGET_SERVER" > /dev/null
    sleep 3
  done
done

echo "Results in $EXP_OUT and $LOG_OUT" >&2