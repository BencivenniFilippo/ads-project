#!/usr/bin/env bash
set -euo pipefail

RATES=(50 70 90 110 130)
REPEATS=2
DURATION=10
SEED=42
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
    -e RATE="$1" -e DURATION="$2" -e BENCH_FILES="$BENCH_FILES" -e SEED="$SEED" client
}

docker compose down --remove-orphans
docker compose build client load_balancer server1
LB_ALGORITHM="$LB_ALGORITHM" docker compose up -d --wait cache server1 server2 server3 load_balancer # --wait waits until the services are running and their health checks have passed

mkdir -p "$RUN_DIR"
echo "rate,duration,avg_ms,p99_ms,max_lag_ms,errors,n_requests,repeat" > "$EXP_OUT"
{
  echo "run_id=$RUN_ID"
  echo "algorithm=$LB_ALGORITHM"
  echo "rates=${RATES[*]}"
  echo "repeats=$REPEATS"
  echo "duration=$DURATION"
  echo "seed=$SEED"
  echo "bench_files=$BENCH_FILES"
  echo "services=server1,server2,server3,load_balancer,cache"
} > "$MANIFEST_OUT"

# Warm-up: wakes up connections, imports and the OS file cache. Output discarded.
run_client 10 5 > /dev/null 2>> "$CLIENT_LOG"
MEASURE_START="$(date --iso-8601=ns)"

for rate in "${RATES[@]}"; do
  for rep in $(seq 1 "$REPEATS"); do
    flush                                   # empty cache => every request is a miss
    client_log=$(mktemp)
    line=$(run_client "$rate" "$DURATION" 2> "$client_log")
    cat "$client_log" >> "$CLIENT_LOG"
    rm "$client_log"
    echo "$line,$rep" >> "$EXP_OUT"
    echo "done rate=$rate repeat=$rep" >&2
  done
done

docker compose logs --since "$MEASURE_START" --no-color server1 server2 server3 > "$SERVER_LOG"
{
  echo "server,handled_requests"
  for server in server1 server2 server3; do
    count=$(grep "$server" "$SERVER_LOG" | grep -c "Handled word-count request" || true)
    echo "$server,$count"
  done
} > "$ROUTING_OUT"

docker compose run --rm -T \
  -v "$(pwd)/$RUN_DIR:/results" \
  client python plot_results.py /results/results.csv --output-dir /results

echo "Results in $RUN_DIR" >&2