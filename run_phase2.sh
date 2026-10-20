#!/usr/bin/env bash
set -euo pipefail

RATES=(50 70 90 110 130)
REPEATS=2
DURATION=10
SEED=42
BENCH_FILES="LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="Experiments/phase2_$RUN_ID"
EXP_OUT="$RUN_DIR/results.csv"
CLIENT_LOG="$RUN_DIR/client.log"
MANIFEST_OUT="$RUN_DIR/manifest.txt"

flush() {
  docker compose exec -T cache redis-cli FLUSHALL > /dev/null
}

run_client() {
  docker compose run --rm -T \
    -e LOAD_BALANCER_HOST=server1 \
    -e RATE="$1" -e DURATION="$2" \
    -e BENCH_FILES="$BENCH_FILES" -e SEED="$SEED" client
}

docker compose down --remove-orphans
docker compose build client server1
docker compose up -d --wait cache server1

mkdir -p "$RUN_DIR"
echo "rate,duration,avg_ms,p99_ms,max_lag_ms,errors,n_requests,repeat" > "$EXP_OUT"
{
  echo "run_id=$RUN_ID"
  echo "phase=2"
  echo "rates=${RATES[*]}"
  echo "repeats=$REPEATS"
  echo "duration=$DURATION"
  echo "seed=$SEED"
  echo "bench_files=$BENCH_FILES"
  echo "services=server1,cache"
  echo "client_target=server1"
} > "$MANIFEST_OUT"

# Warm-up: wakes up connections, imports and the OS file cache.
run_client 10 5 > /dev/null 2>> "$CLIENT_LOG"

for rate in "${RATES[@]}"; do
  for rep in $(seq 1 "$REPEATS"); do
    flush
    client_log=$(mktemp)
    line=$(run_client "$rate" "$DURATION" 2> "$client_log")
    cat "$client_log" >> "$CLIENT_LOG"
    rm "$client_log"
    echo "$line,$rep" >> "$EXP_OUT"
    echo "done rate=$rate repeat=$rep" >&2
  done
done

docker compose run --rm -T \
  -v "$(pwd)/$RUN_DIR:/results" \
  client python plot_results.py /results/results.csv --output-dir /results

echo "Results in $RUN_DIR" >&2