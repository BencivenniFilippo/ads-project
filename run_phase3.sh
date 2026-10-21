#!/usr/bin/env bash
set -euo pipefail

RATES=(50)
REPEATS=1
DURATION=10
SEED=42
HIT_RATE="${HIT_RATE:-0}"
LB_ALGORITHM="least_connections" # least_connections or least_response_time
BENCH_FILES="LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="Experiments/phase3_$RUN_ID"
EXP_OUT="$RUN_DIR/results.csv"
CLIENT_LOG="$RUN_DIR/client.log"
SERVER_LOG="$RUN_DIR/servers.log"
ROUTING_OUT="$RUN_DIR/routing.csv"
MANIFEST_OUT="$RUN_DIR/manifest.txt"

flush() {
  docker compose exec -T cache redis-cli FLUSHALL > /dev/null
}

run_client() {
  # --rm: Automatically remove the container when it exits
  # -T: Disable pseudo-TTY (prevents errors for this non-interactive script)
  # -e: Pass environment variables into the container
  docker compose run --rm -T \
    -e RATE="$1" -e DURATION="$2" -e BENCH_FILES="$BENCH_FILES" \
    -e SEED="$SEED" -e HIT_RATE="$HIT_RATE" client
}

docker compose down --remove-orphans
docker compose build client load_balancer server1
LB_ALGORITHM="$LB_ALGORITHM" docker compose up -d --wait cache server1 server2 server3 load_balancer # --wait waits until the services are running and their health checks have passed

mkdir -p "$RUN_DIR"
echo "rate,duration,avg_ms,p99_ms,max_lag_ms,errors,n_requests,repeat,actual_hits,actual_misses,actual_hit_rate" > "$EXP_OUT"
{
  echo "run_id=$RUN_ID"
  echo "algorithm=$LB_ALGORITHM"
  echo "rates=${RATES[*]}"
  echo "repeats=$REPEATS"
  echo "duration=$DURATION"
  echo "seed=$SEED"
  echo "hit_rate=$HIT_RATE%"
  echo "bench_files=$BENCH_FILES"
  echo "services=server1,server2,server3,load_balancer,cache"
} > "$MANIFEST_OUT"

 # Warm-up: initialize Python/RPyC paths and OS file caches without measuring it.
echo "warm-up start | rate=10 req/s | duration=5 s" >> "$CLIENT_LOG"
run_client 10 5 > /dev/null 2>> "$CLIENT_LOG"
echo "warm-up complete" >> "$CLIENT_LOG"
MEASURE_START="$(date --iso-8601=ns)"
BASELINE_SUMMARY=$(docker compose logs --no-color load_balancer | grep "routing summary" | tail -n 1 || true)

for rate in "${RATES[@]}"; do
  for rep in $(seq 1 "$REPEATS"); do
    flush                                   # reset the cache for the selected workload mode
    run_start="$(date --iso-8601=ns)"
    client_log=$(mktemp)
    line=$(run_client "$rate" "$DURATION" 2> "$client_log")
    cat "$client_log" >> "$CLIENT_LOG"
    rm "$client_log"
    measured_logs=$(docker compose logs --since "$run_start" --no-color server1 server2 server3)
    actual_hits=$(printf '%s\n' "$measured_logs" | awk '/cache_status=hit/{count++} END{print count+0}')
    actual_misses=$(printf '%s\n' "$measured_logs" | awk '/cache_status=miss/{count++} END{print count+0}')
    if [[ "$HIT_RATE" == "100" ]]; then
      actual_misses=$((actual_misses - 1))
    fi
    measured_total=$((actual_hits + actual_misses))
    if (( measured_total > 0 )); then
      actual_hit_rate=$(awk -v hits="$actual_hits" -v total="$measured_total" 'BEGIN { printf "%.2f", 100 * hits / total }')
    else
      actual_hit_rate=""
    fi
    echo "$line,$rep,$actual_hits,$actual_misses,$actual_hit_rate" >> "$EXP_OUT"
    echo "done rate=$rate repeat=$rep" >&2
  done
done

docker compose logs --since "$MEASURE_START" --no-color load_balancer server1 server2 server3 > "$SERVER_LOG"
{
  echo "server,handled_requests"
  summary=$(grep "routing summary" "$SERVER_LOG" | tail -n 1 || true)
  for server in server1 server2 server3; do
    measured=$(printf '%s\n' "$summary" | sed -n "s/.*$server=\([0-9][0-9]*\).*/\1/p")
    baseline=$(printf '%s\n' "$BASELINE_SUMMARY" | sed -n "s/.*$server=\([0-9][0-9]*\).*/\1/p")
    echo "$server,$(( ${measured:-0} - ${baseline:-0} ))"
  done
} > "$ROUTING_OUT"

docker compose run --rm -T \
  -v "$(pwd)/$RUN_DIR:/results" \
  client python plot_results.py /results/results.csv --output-dir /results

echo "Results in $RUN_DIR" >&2