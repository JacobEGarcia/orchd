#!/usr/bin/env bash
# Runs three experiments against a live local orchd and prints JSON lines.
set -u
cd "$(dirname "$0")"
BIN=${BIN:-bin/orchd}
run() { # label, orchd flags..., -- loadgen flags...
  label=$1; shift
  flags=(); while [ "$1" != "--" ]; do flags+=("$1"); shift; done; shift
  $BIN -addr 127.0.0.1:8099 "${flags[@]}" >/dev/null 2>&1 & pid=$!
  for i in $(seq 50); do curl -sf 127.0.0.1:8099/healthz >/dev/null && break; sleep 0.1; done
  bin/loadgen -url http://127.0.0.1:8099 -label "$label" "$@"
  kill $pid; wait $pid 2>/dev/null
}
echo "# A. worker scaling (2000 jobs x 20 ms, no failures)"
for w in 1 4 16 64; do run "workers=$w" -workers $w -- -n 2000 -work-ms 20 -timeout 120s; done
echo "# B. retries vs no retries (20% injected failure per attempt, 16 workers, 2000 jobs)"
run "retries=off" -workers 16 -retries=false -- -n 2000 -work-ms 20 -fail 0.2
run "retries=on(max5)" -workers 16 -retries=true -max-attempts 5 -- -n 2000 -work-ms 20 -fail 0.2
echo "# C. hung workers (1% of attempts hang forever, 16 workers, 1000 jobs, 20s timeout)"
run "healthcheck=off" -workers 16 -healthcheck=false -- -n 1000 -work-ms 20 -hang 0.01 -timeout 20s
run "healthcheck=on" -workers 16 -healthcheck=true -- -n 1000 -work-ms 20 -hang 0.01 -timeout 20s
