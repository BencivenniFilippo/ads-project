# Distributed Word Count Service

This project is an Architecture of Distributed Systems lab implementation of a
distributed word-count service.

Clients request the number of occurrences of a word in one of the text files
stored by the service. Requests travel through a TCP load balancer to one of
three RPyC server replicas. The servers use Redis as a shared cache, so a
repeated `(file, word)` request can be served without scanning the text again.

## Architecture

The Docker Compose application contains:

- **`client`** — the benchmark client. It builds a workload from the available
  text files and reports average latency, p99 latency, send lag, and errors.
- **`load_balancer`** — an asyncio TCP proxy that forwards each connection to a
  server replica using the algorithm selected through `LB_ALGORITHM`.
- **`server1`, `server2`, `server3`** — RPyC word-count service replicas.
- **`cache`** — a Redis instance shared by all server replicas.

The load balancer forwards the RPyC byte stream; it does not implement the RPyC
protocol itself. Text files are copied into the server image from [`Texts/`](Texts).

## Requirements

- Docker Engine
- Docker Compose v2 (`docker compose`)

Python is installed inside the application images. The images currently use
Python 3.9. A local Python installation is not required to run the Compose
application.

## Quick start

Build the images and start Redis, the server replicas, and the load balancer:

```bash
docker compose build server1 load_balancer client
docker compose up -d --wait cache server1 server2 server3 load_balancer
```

The `--wait` option waits for Redis, all server replicas, and the load balancer
to pass their health checks. The server and load-balancer health checks verify
that their TCP port `7777` is accepting connections. The load balancer listens
on port `7777` inside the Compose network; no host port is published by the
current `docker-compose.yaml`.

These startup health checks verify process and TCP readiness; they do not warm
Python imports, RPyC connection paths, OS file caches, or the application-level
Redis word-count cache. The benchmark scripts therefore perform a separate
warm-up before measurement.

Docker Compose automatically creates a private project network and attaches all
services to it. The load balancer reaches the replicas using the Compose service
names `server1`, `server2`, and `server3`; a separate custom network is not
required for this project.

To run the benchmark client once:

```bash
docker compose run --rm \
  -e RATE=50 \
  -e DURATION=20 \
  -e SEED=42 \
  client
```

The client prints one CSV result line to standard output:

```text
rate,duration,avg_ms,p99_ms,max_lag_ms,errors,n_requests
```

Application logs are written to standard error.

To stop the application containers:

```bash
docker compose down
```

## Benchmarking

The benchmark client sends `RATE * DURATION` requests at the requested rate.
It first retrieves the server's file list and reads the selected files through
RpyC to build a vocabulary. It then creates a deterministic workload using the
configured random seed. `HIT_RATE` supports three experiment modes:

- `0` (default): every measured `(file, word)` pair is unique.
- `50`: the client creates an even workload with half unique pairs. It splits
  the unique half into balanced chunks and sends each chunk twice in sequence,
  creating a controlled repeated-request pattern.
- `100`: the client prewarms one `(file, word)` pair, then sends that pair for
  every measured request.

The 100% prewarm request is excluded from measured latency and from the
reported actual hit/miss counts. The 50% mode is designed so the first
occurrence populates Redis before the repeated occurrence; under severe
overload, concurrent requests can still overlap.

The client supports these environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `RATE` | `50` | Target requests per second |
| `DURATION` | `20` | Benchmark duration in seconds |
| `SEED` | `42` | Random seed used to build the workload |
| `HIT_RATE` | `0` | Cache workload mode: `0`, `50`, or `100` |
| `BENCH_FILES` | `LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt` | Comma-separated files to benchmark |
| `LOAD_BALANCER_HOST` | Set by Compose | Hostname of the load balancer |
| `LOAD_BALANCER_PORT` | `7777` | Load balancer port |

For a repeatable Phase 3 multi-rate experiment, run
[`run_phase3.sh`](run_phase3.sh). It builds the client, load-balancer, and
shared server image, starts the three replicas, waits for health checks, flushes
Redis before each repetition, and creates a timestamped directory under
`Experiments/`. It first runs `docker compose down --remove-orphans` so stale
containers from an earlier Compose configuration are not reused. Each run
directory contains:

```bash
bash run_phase3.sh
```

The load-balancing algorithm can be selected editing the run_phase3.sh script. Then, to run it:

```bash
bash run_phase3.sh
```

The selected algorithm is recorded in that run's `manifest.txt`.

The Phase 2 single-server benchmark is run separately. Both scripts default to
an all-miss workload. Set `HIT_RATE` in the environment or edit its default
near the top of the script to select one of the supported cache modes:

```bash
bash run_phase2.sh
HIT_RATE=50 bash run_phase2.sh
```

For a one-off client run, pass the setting directly:

```bash
docker compose run --rm -e HIT_RATE=50 -e RATE=50 -e DURATION=20 client
```

It starts only `cache` and `server1`, and runs the client directly against
`server1` without the load balancer. Its results are stored under
`Experiments/phase2_<timestamp>/`; Phase 3 results are stored under
`Experiments/phase3_<timestamp>/`.

- `results.csv` — raw one-row-per-rate-and-repetition measurements, including
  server-observed `actual_hits`, `actual_misses`, and `actual_hit_rate`
- `manifest.txt` — rates, repetitions, seed, files, and services used
- `client.log` — benchmark configuration, progress, latency summaries, and errors
- `servers.log` — measured load-balancer and server startup/routing summaries
- `routing.csv` — measured handled-request counts per server, excluding warm-up
- `average_latency.png` — average latency by request rate
- `p99_latency.png` — p99 latency by request rate
- `error_rate.png` — measured request error rate by request rate

The raw CSV remains the source of truth; the plots are derived artifacts that
make the results easier to inspect and include in the report.

The benchmark scripts run a short labeled warm-up before the measured rates.
The warm-up initializes connection paths and runtime/file caches, but its
requests are excluded from measured server logs and routing summaries because
the measurement start timestamp is recorded only after warm-up. The warm-up is
also not written to `results.csv`.

The server containers identify themselves with `SERVER_NAME` and emit one
machine-readable cache status log line for each word-count request. The
benchmark scripts count these lines for each measured repetition and append
the actual hit/miss totals to `results.csv`. The load balancer logs
periodic routing summaries, and `routing.csv` is derived from the latest
measured routing summary. Inspect the distribution with:

```bash
docker compose logs --no-color server1 server2 server3
```

This provides evidence of which replica handled each request. The run's
`routing.csv` provides the corresponding count summary.

## Server API

The RPyC service in [`server.py`](server.py) exposes:

- `exposed_word_count(file, word)` — returns the case-insensitive count of
  `word` in the named text file, using Redis for caching.
- `exposed_txt_file_list()` — returns the names of the available `.txt` files.
- `exposed_read_file(file)` — returns the complete contents of a text file.

The server reads the following environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `RPC_PORT` | `7777` | RPyC listening port |
| `CACHE_HOST` | `cache` | Redis hostname |
| `CACHE_PORT` | `6379` | Redis port |
| `SERVER_NAME` | `server` | Name used in log messages |

The load balancer reads:

| Variable | Default | Description |
| --- | --- | --- |
| `SERVERS` | empty | Comma-separated `host:port` server endpoints |
| `BIND_PORT` | `7777` | Listening port |
| `LB_ALGORITHM` | `least_connections` | `least_connections`, `weighted_least_connections`, or `least_response_time` |
| `WEIGHTS` | empty | Optional positive comma-separated weights, required by weighted least-connections |

Plot generation uses Matplotlib inside the client image. The benchmark script
does not replace raw measurements with plots; it stores both.

## Fault tolerance (Phase 4)

The load balancer runs a health monitor that pings every backend once per
second (RPyC `ping`) and reconnects to servers that came back. Servers that
fail are excluded from routing. If a backend dies while a request is in
flight, the load balancer replays the buffered client bytes to another
backend, so the client does not see the failure.

```bash
bash run_ft_benchmark.sh
```

The script starts the three-replica deployment, runs a constant-rate workload,
kills `server2` after `KILL_DELAY` seconds, and writes to
`Experiments/fault_<timestamp>/`: `results.csv`, the per-request trace
`server_failure_trace.csv`, the latency timeline plot
`server_failure_trace_timeline.png`, `events.log`, and `load_balancer.log`.
The failed server is restarted at the end.

## Current assignment status

Implemented: single-server benchmark (Phase 2), three replicas behind an
asyncio TCP load balancer with least-connections, weighted least-connections
and least-response-time (Phase 3), and health monitoring with rerouting after
a server failure (Phase 4).

## Repository layout

```text
.
├── client.py                 # Benchmark client
├── server.py                 # RPyC server and Redis-backed word counting
├── load_balancer.py          # Asyncio TCP load balancer
├── docker-compose.yaml       # Redis, replicas, load balancer, and client
├── *Dockerfile               # Images for the three Python components
├── Texts/                    # Text corpus copied into server images
├── run_phase2.sh             # Phase 2 single-server benchmark
├── run_phase3.sh             # Phase 3 multi-rate benchmark
├── run_ft_benchmark.sh       # Phase 4 fault-tolerance experiment
├── plot_results.py           # Plots (results and failure timeline)
├── requirements.txt          # Container runtime dependencies
└── pyproject.toml            # Python project metadata and development dependencies
```

## Development notes

The standalone Python package metadata in `pyproject.toml` declares an
`ads-project` console script, but the repository currently contains no
`ads_project` package or module. The supported runtime entry points are the
Dockerfiles and the three Python scripts described above.

The dependency lists are maintained separately in `requirements.txt` and
`pyproject.toml`; use the requirements file when building the Docker images.

## License

No license file is currently included in this repository.