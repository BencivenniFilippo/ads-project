#!/usr/bin/env bash
set -euo pipefail

RATES=(50 70 90 110 130)
REPEATS=3
DURATION=20
SEED=42
BENCH_FILES="LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt"
OUT="Experiments/experiment_$(date +%Y%m%d_%H%M%S).csv"

flush() {
  docker compose exec -T cache redis-cli FLUSHALL > /dev/null
}

run_client() {
  # --rm: Automatically remove the container when it exits
  # -T: Disable pseudo-TTY (prevents errors for this non-interactive script)
  # -e: Pass environment variables into the container
  docker compose run --rm -T \ 
    -e RATE="$1" -e DURATION="$2" -e BENCH_FILES="$BENCH_FILES" client
}

docker compose up -d cache server

echo "rate,avg_ms,p99_ms,max_lag_ms,errors,n_requests,repeat" > "$OUT"

# Warm-up: wakes up connections, imports and the OS file cache. Output discarded.
run_client 10 5 > /dev/null

for rate in "${RATES[@]}"; do
  for rep in $(seq 1 "$REPEATS"); do
    flush                                   # empty cache => every request is a miss
    line=$(run_client "$rate" "$DURATION")
    echo "$line,$rep" >> "$OUT"
    echo "done rate=$rate repeat=$rep" >&2
  done
done

echo "Results in $OUT" >&2