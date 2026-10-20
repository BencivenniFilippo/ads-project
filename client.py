import concurrent.futures
import logging
import os
import random
import statistics
import time
import pathlib
import re

import rpyc


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
WORD_RE = re.compile(r"[a-z]+(?:'[a-z]+)*")
DEFAULT_BENCH_FILES = "LOTR_1_TheFellowshipOfTheRing.txt,LOTR_3_ReturnOfTheKing.txt,Pulp_Fiction.txt,Forrest_Gump.txt"



def connect() -> rpyc.Connection:
    return rpyc.connect(HOST, PORT) # TCP (default) connection client -> LB

def conn_root_helper(conn: rpyc.Connection, func_name: str, *args, **kwargs) -> any:
    try:
        method = getattr(conn.root, func_name)
        result = method(*args, **kwargs)
        return result
    
    except Exception as e:
        raise RuntimeError(f"Error calling {func_name} on server: {e}") from e
        

def get_txt_files() -> list:
    conn = connect()
    try:
        # list(...) copies the remote list into a local one before closing
        return list(conn_root_helper(conn, "exposed_txt_file_list"))
    finally:
        conn.close()


def build_vocabulary(files: list) -> dict:
    conn = connect()

    try:
        vocab = {}
        for f in files:
            text = conn_root_helper(conn, "exposed_read_file", f).lower()
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



def single_word_count_latency(file: str, word: str, scheduled: float) -> tuple:
    conn = connect()
    try:
        start_time = time.perf_counter()
        ans = conn_root_helper(conn, "exposed_word_count", file, word)
        latency = time.perf_counter() - start_time # calculate latency according to professor definition (just consider client -> server -> client)
    finally:
        conn.close() # always give the connection back, even on error
        
    lag = start_time - scheduled  # how late we were vs the planned send time
    return ans, latency, lag


def run_experiment(workload: list, rate: int) -> dict:
    n_requests = len(workload)
    duration = n_requests / rate
    pool_size = 2 * rate
    logger.info(
        "benchmark start | target_rate=%d req/s | requests=%d | workers=%d",
        rate,
        n_requests,
        pool_size,
    )

    futures = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=pool_size) as executor:
        start = time.perf_counter()
        for i, (file, word) in enumerate(workload):
            # Assure that we start the request after i / rate (if rate = 50, then every request should be sent every 0.02 seconds)
            scheduled = start + i / rate
            delay = scheduled - time.perf_counter()
            if delay >= 0:
                time.sleep(delay)
            elif i > 0:
                logger.warning(f"Client {i % rate} / {rate} isnt on time, lagging by {-delay:.3f} seconds")

            # Send the request, and store the future object of the concurrent.futures class
            future = executor.submit(single_word_count_latency, file, word, scheduled)
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

    if len(latencies) < 2:
        logger.error("Not enough successful requests to compute statistics")
        return {"rate": rate, "errors": errors, "n_requests": n_requests}

    avg_ms = round(sum(latencies) / len(latencies) * 1000, 2) # Avg latency in milliseconds
    p99_ms = round(statistics.quantiles(latencies, n=100, method="inclusive")[98] * 1000, 2) # 99th percentile latency in milliseconds
    max_lag_ms = round(max(lags) * 1000, 2) # How late we were vs the planned send time, in milliseconds

    logger.info(
        "benchmark complete | target_rate=%d req/s | requests=%d | successful=%d "
        "| errors=%d | workers=%d | avg=%.2f ms | p99=%.2f ms | max_send_lag=%.2f ms",
        rate,
        n_requests,
        len(latencies),
        errors,
        pool_size,
        avg_ms,
        p99_ms,
        max_lag_ms,
    )
    return {"rate": rate, "duration": duration, "avg_ms": avg_ms, "p99_ms": p99_ms,
            "max_lag_ms": max_lag_ms, "errors": errors, "n_requests": n_requests}



if __name__ == "__main__":
    rate = int(os.getenv("RATE", "50"))
    duration = int(os.getenv("DURATION", "20"))
    seed = int(os.getenv("SEED", "42"))
    bench_files = [f.strip() for f in os.getenv("BENCH_FILES", DEFAULT_BENCH_FILES).split(",") if f.strip()]
    logger.info(
        "config | rate=%d req/s | duration=%d s | requests=%d | seed=%d | files=%s",
        rate,
        duration,
        rate * duration,
        seed,
        bench_files,
    )

    server_files = get_txt_files()
    missing = [f for f in bench_files if f not in server_files]
    if missing:
        raise SystemExit(f"Not known to the server: {missing}")

    workload = build_workload(build_vocabulary(bench_files), rate * duration, seed)
    res = run_experiment(workload, rate)

    # CSV line on stdout (logs go to stderr), so the bash script can collect it
    keys = ("rate", "duration", "avg_ms", "p99_ms", "max_lag_ms", "errors", "n_requests")
    print(",".join(str(res.get(k, "")) for k in keys), flush=True)
