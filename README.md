# orchd: a small job queue and scheduler (Go)

A single-binary job queue with a worker pool, retries with exponential backoff and jitter, a dead-letter state, heartbeat health checks that requeue work from hung workers, a write-ahead log (WAL) with kill -9 crash recovery, a real CPU job type, and `/healthz`, `/readyz` and `/stats` endpoints. A Go load generator drives it over HTTP. Written with AI assistance (Instinct) under Jacob Garcia's direction; not hand-written. It is a small learning-scale project, not a Kubernetes scheduler.

## What is here
- `cmd/orchd/main.go`: the service (about 250 lines).
- `cmd/loadgen/main.go`: HTTP load generator that waits for the queue to drain and prints one JSON line.
- `bench.sh`: runs the three experiments below against a live local `orchd`.
- `results.txt`: raw output from one run of `bench.sh` on this machine.
- `Dockerfile`, `deploy/orchd.yaml`: container build and a Deployment + Service with liveness and readiness probes.

## How it works
- `POST /jobs {"work_ms":20,"fail_prob":0.2,"hang_prob":0.01}` queues a job. Jobs are simulated: sleep for `work_ms`, then fail with probability `fail_prob`, or hang the worker forever with probability `hang_prob`. This injects faults on purpose so the retry and health logic can be measured.
- Queue is a min-heap on `readyAt`. A failed job is requeued with delay `10ms * 2^(attempt-1)` plus up to 50% jitter, until `-max-attempts` (default 5), then it moves to `dead`.
- Each worker updates a heartbeat every 10 ms while working. A health loop checks heartbeats every `hb-timeout/4`. A worker silent for more than `hb-timeout` (default 200 ms) while holding a job is marked unhealthy, its job is requeued, and a replacement worker starts. Each requeue bumps an epoch on the job so a stale worker cannot complete it twice.
- `/readyz` returns 503 when no worker is healthy.

## Tests
`go test -race -count=2 ./cmd/orchd` runs 8 tests: all jobs complete; always-failing job dead-letters after max attempts; no-retry mode dead-letters on first failure; hung worker is detected, job requeued, replacement spawned; a stale worker cannot complete a requeued job (epoch check); WAL replay requeues only unfinished jobs and tolerates a torn last line; CPU-bound jobs do not trigger false hang detection. All pass under the race detector.

A bug found by the CPU-job benchmark and fixed: heartbeats were only sent between 10 ms work slices, so a long CPU-bound job looked like a hung worker, the health check requeued it and spawned replacements without bound. Fix: heartbeat from inside the CPU loop, and never grow past the configured worker count.

## Results
Machine: 2 vCPU shared sandbox container, Go, loopback HTTP. `bench.sh` (first suite, single runs) output is in `results.txt`; `bench2.py` (repeated runs, WAL, crash recovery, CPU jobs) output is in `results2.txt`. `+/-` is population standard deviation over the stated repeats; N is small, so these are not confidence intervals.

### Repeated runs (bench2.py, N=3)
```
16 workers, 2000x20ms: throughput jobs/s 754.41 +/- 6.03
20% fail, retries OFF: completion % 80.77 +/- 0.39; total s 2.67 +/- 0.02
20% fail, retries ON : completion % 99.92 +/- 0.05; total s 3.49 +/- 0.02
1% hang, health check OFF: completion % 99.17 +/- 0.12
1% hang, health check ON : completion % 100.00 +/- 0.00; total s 1.60 +/- 0.15
```
### Durability
```
# Write-ahead log cost (16 workers, 2000 jobs x 20ms, 32 submit clients)
no WAL: submit phase s 0.20 +/- 0.02; total s 2.64 +/- 0.03
WAL, no fsync: submit phase s 0.21 +/- 0.04; total s 2.66 +/- 0.01
WAL, fsync per submit: submit phase s 1.09 +/- 0.03; total s 3.02 +/- 0.02
# kill -9 crash recovery (WAL with fsync): submit 3000 jobs x 30ms, kill -9 at ~25% done, restart, drain
jobs acknowledged: 3000; done at kill time: 704; after restart, ids with a done/dead record: 3000; lost: 0
# Real CPU jobs (chained SHA-256), 200 jobs x 500,000 rounds, no sleep; machine has 2 cores
workers=1: jobs/s 20.65 +/- 0.02
workers=2: jobs/s 37.71 +/- 0.34
workers=4: jobs/s 39.12 +/- 0.35
workers=16: jobs/s 14.41 +/- 0.69
```
`kill -9` test: 3000 acknowledged jobs, process killed at 704 done, restarted from the WAL; every one of the 3000 ids ends with a done record (checked independently by parsing the WAL, not from server counters). Delivery is at-least-once: a job that finished but whose done record was not yet written reruns after recovery. fsync per submission cost about 5x on submit time (0.20 s to 1.09 s for 2000 jobs) but only about 14% on total time.

### First suite (single runs, results.txt)
```
# A. worker scaling (2000 jobs x 20 ms, no failures)
{"completion_pct":100,"dead":0,"done":2000,"jobs":2000,"label":"workers=1","requeued":0,"retried":0,"submit_s":0.192004362,"throughput_jobs_s":48.08088094016615,"total_s":41.596575622}
{"completion_pct":100,"dead":0,"done":2000,"jobs":2000,"label":"workers=4","requeued":0,"retried":0,"submit_s":0.20646673,"throughput_jobs_s":191.95479650047528,"total_s":10.419119691}
{"completion_pct":100,"dead":0,"done":2000,"jobs":2000,"label":"workers=16","requeued":0,"retried":0,"submit_s":0.17899258,"throughput_jobs_s":758.3083848705652,"total_s":2.637449407}
{"completion_pct":100,"dead":0,"done":2000,"jobs":2000,"label":"workers=64","requeued":0,"retried":0,"submit_s":0.209622325,"throughput_jobs_s":2830.615020604825,"total_s":0.70656023}
# B. retries vs no retries (20% injected failure per attempt, 16 workers, 2000 jobs)
{"completion_pct":80.55,"dead":389,"done":1611,"jobs":2000,"label":"retries=off","requeued":0,"retried":0,"submit_s":0.239014539,"throughput_jobs_s":598.0887291686647,"total_s":2.693580269}
{"completion_pct":99.9,"dead":2,"done":1998,"jobs":2000,"label":"retries=on(max5)","requeued":0,"retried":510,"submit_s":0.175531795,"throughput_jobs_s":566.5116858360507,"total_s":3.52684693}
# C. hung workers (1% of attempts hang forever, 16 workers, 1000 jobs, 20s timeout)
{"completion_pct":98.7,"dead":0,"done":987,"jobs":1000,"label":"healthcheck=off","requeued":0,"retried":0,"submit_s":0.110276753,"throughput_jobs_s":49.32876379453578,"total_s":20.00861007}
{"completion_pct":100,"dead":0,"done":1000,"jobs":1000,"label":"healthcheck=on","requeued":9,"retried":0,"submit_s":0.124096532,"throughput_jobs_s":598.468931546205,"total_s":1.670930515}
```
## Honest scope and limits
- Most work is simulated sleeps with injected failures and hangs. The CPU test is real SHA-256 work: 20.7 jobs/s with 1 worker, 37.7 with 2 (1.8x), 39.1 with 4, then 14.4 with 16 workers. The drop at 16 workers on 2 cores is a measured result I have not diagnosed (candidates: scheduler oversubscription, or health-check requeues of starved workers; I did not record the requeue count for that run), so treat it as an open question, not a finding.
- In memory plus a local WAL in one process. No multi-node scheduling, leader election, auth, cancellation or priorities. At-least-once, not exactly-once.
- The Dockerfile and `deploy/orchd.yaml` were written but never built or applied (no container runtime in the build environment).
- Small N on a shared 2 vCPU host.

## Run it
```
go build -o bin/orchd ./cmd/orchd && go build -o bin/loadgen ./cmd/loadgen
go test -race ./cmd/orchd
./bench.sh && python3 bench2.py
bin/orchd -workers 16 -wal /tmp/orchd.wal
```
