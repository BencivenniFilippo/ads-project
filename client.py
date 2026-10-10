import concurrent.futures
import logging
import os
import queue
import random
import statistics
import time
import pathlib
import re

import rpyc
from random_word import RandomWords


client_name = os.getenv("CLIENT_NAME", "client")
logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - [{client_name}] - %(levelname)s - %(message)s',
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(client_name)

HOST = os.getenv("LOAD_BALANCER_HOST")
PORT = int(os.getenv("LOAD_BALANCER_PORT", "7777"))
TEXTS_DIR = pathlib.Path(os.getenv("TEXTS_DIR", "/app/Texts"))
WORD_RE = re.compile(r"[a-z]+")  # letters only, see note below
DEFAULT_BENCH_FILES = "LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt"


r = RandomWords()


def connect() -> rpyc.Connection:
    return rpyc.connect(HOST, PORT)


def get_txt_files() -> list:
    conn = connect()
    try:
        # list(...) copies the remote list into a local one before closing
        return list(conn.root.exposed_txt_file_list())
    finally:
        conn.close()


def build_vocabulary(files: list) -> dict:
    conn = connect()

    try:
        vocab = {}
        for f in files:
            text = conn.root.exposed_read_file(f).lower()
            vocab[f] = sorted(set(WORD_RE.findall(text)))
        return vocab
    
    finally:
        conn.close()


def build_workload(vocab: dict, n: int, seed: int) -> list:
    rng = random.Random(seed)
    all_pairs = [(f, w) for f, words in vocab.items() for w in words]
    if n > len(all_pairs):
        raise ValueError(f"Asked for {n} unique pairs, only {len(all_pairs)} exist")
    return rng.sample(all_pairs, n)  # without replacement: every pair is new



def single_word_count_latency(pool: queue.Queue, file: str, word: str, scheduled: float) -> tuple:
    conn = pool.get()
    try:
        start_time = time.perf_counter()
        ans = conn.root.exposed_word_count(file, word)
        latency = time.perf_counter() - start_time # calculate latency according to professor definition (just consider client -> server -> client)
    finally:
        pool.put(conn)  # always give the connection back, even on error
        
    lag = start_time - scheduled  # how late we were vs the planned send time
    return ans, latency, lag



def run_experiment(workload: list, rate: int) -> dict:
    n_requests = len(workload)
    pool_size = 2 * rate

    pool = queue.Queue() # connections queue for assigning connections on the fly
    connections = [] # connections list to keep track of them so that we can close them in the end
    try:
        # Start by adding all connections to both the list and the queue
        for _ in range(pool_size):
            c = connect()
            connections.append(c)
            pool.put(c)

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=pool_size) as executor:
            start = time.perf_counter()
            for i, (file, word) in enumerate(workload):
                # Assure that we start the request after i / rate (if rate = 50, then every request should be sent every 0.02 seconds)
                scheduled = start + i / rate
                delay = scheduled - time.perf_counter()
                if delay >= 0:
                    time.sleep(delay)
                else:
                    logger.warning(f"Client {i}/{rate} isnt on time, lagging by {-delay:.3f} seconds")

                # Send the request, and store the future object of the concurrent.futures class
                future = executor.submit(single_word_count_latency, pool, file, word, scheduled)
                futures.append(future)

        # leaving the with-block waits for every request to finish

        latencies, lags, errors = [], [], 0
        for future in futures:
            try:
                _, latency, lag = future.result()
            except Exception as exc:
                errors += 1
                logger.error(f"A request generated an exception: {exc}")
            else:
                latencies.append(latency)
                lags.append(lag)
    finally:
        for c in connections:
            c.close()

    if len(latencies) < 2:
        logger.error("Not enough successful requests to compute statistics")
        return {"rate": rate, "errors": errors, "n_requests": n_requests}

    avg_ms = sum(latencies) / len(latencies) * 1000
    p99_ms = statistics.quantiles(latencies, n=100, method="inclusive")[98] * 1000
    max_lag_ms = max(lags) * 1000

    logger.info(f"rate={rate} req/s | avg={avg_ms:.2f} ms | p99={p99_ms:.2f} ms | max send lag={max_lag_ms:.2f} ms")
    logger.info(f"Errors: {errors} out of {n_requests} requests ({errors / n_requests * 100:.2f}%)")
    return {"rate": rate, "avg_ms": avg_ms, "p99_ms": p99_ms,
            "max_lag_ms": max_lag_ms, "errors": errors, "n_requests": n_requests}



if __name__ == "__main__":
    rate = int(os.getenv("RATE", "50"))
    duration = int(os.getenv("DURATION", "20"))
    seed = int(os.getenv("SEED", "42"))
    bench_files = [f.strip() for f in os.getenv("BENCH_FILES", DEFAULT_BENCH_FILES).split(",") if f.strip()]
    logger.info(f"config: rate={rate} duration={duration} seed={seed} files={bench_files}")

    server_files = get_txt_files()
    missing = [f for f in bench_files if f not in server_files]
    if missing:
        raise SystemExit(f"Not known to the server: {missing}")

    workload = build_workload(build_vocabulary(bench_files), rate * duration, seed)
    res = run_experiment(workload, rate)

    # CSV line on stdout (logs go to stderr), so the bash script can collect it
    keys = ("rate", "avg_ms", "p99_ms", "max_lag_ms", "errors", "n_requests")
    print(",".join(str(res.get(k, "")) for k in keys), flush=True)
