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
  server replica using the currently active least-connections selection.
  Weighted least-connections support is present as a helper but is not yet
  selected by the request handler.
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
RpyC to build a vocabulary. It then samples unique `(file, word)` pairs using
the configured random seed.

The client supports these environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `RATE` | `50` | Target requests per second |
| `DURATION` | `20` | Benchmark duration in seconds |
| `SEED` | `42` | Random seed used to build the workload |
| `BENCH_FILES` | `LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt` | Comma-separated files to benchmark |
| `LOAD_BALANCER_HOST` | Set by Compose | Hostname of the load balancer |
| `LOAD_BALANCER_PORT` | `7777` | Load balancer port |

For a repeatable multi-rate experiment, run
[`run_benchmark.sh`](run_benchmark.sh). It builds the client, load-balancer, and
shared server image, starts the three replicas, waits for health checks, flushes
Redis before each repetition, and creates a timestamped directory under
`Experiments/`. Each run directory contains:

```bash
bash run_benchmark.sh
```

- `results.csv` — raw one-row-per-rate-and-repetition measurements
- `manifest.txt` — rates, repetitions, seed, files, and services used
- `client.log` — warm-up and benchmark client logs
- `servers.log` — server logs captured after the run
- `routing.csv` — handled-request counts per server
- `average_latency.png` — average latency by request rate
- `p99_latency.png` — p99 latency by request rate
- `error_rate.png` — measured request error rate by request rate

The raw CSV remains the source of truth; the plots are derived artifacts that
make the results easier to inspect and include in the report.

The server containers log every handled word-count request with their
`SERVER_NAME` and cumulative request number. Inspect the distribution with:

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
| `WEIGHTS` | empty | Optional comma-separated weights; the current handler uses unweighted least-connections selection |

Plot generation uses Matplotlib inside the client image. The benchmark script
does not replace raw measurements with plots; it stores both.

## Current assignment status

The current repository contains the phase-3 deployment: three explicit server
replicas, a Redis cache, an asyncio TCP load balancer, startup health checks, and
the benchmark client. The active load-balancing algorithm is unweighted
least-connections. Weighted least-connections exists in
[`load_balancer.py`](load_balancer.py), but a runtime switch and comparative
experiments for the second algorithm still need to be added.

The initial smoke test completed successfully with four requests and zero
errors. The full assignment evidence is still pending: phase 3 requires testing
two dynamic algorithms, showing replica distribution, and comparing one-server
and three-server latency. Phase 4 health monitoring and rerouting after a
running server failure are not implemented yet; Compose health checks only
gate startup.

## Repository layout

```text
.
├── client.py                 # Benchmark client
├── server.py                 # RPyC server and Redis-backed word counting
├── load_balancer.py          # Asyncio TCP load balancer
├── docker-compose.yaml       # Redis, replicas, load balancer, and client
├── *Dockerfile               # Images for the three Python components
├── Texts/                    # Text corpus copied into server images
├── run_benchmark.sh          # Multi-rate benchmark helper
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