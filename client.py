import concurrent.futures
import logging
import os
import queue
import random
import statistics
import time

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



def run_experiment(txt_files: list, rate: int = 50, duration: int = 20) -> dict:
    n_requests = rate * duration
    pool_size = rate  # safe while latency < 1 s

    # Everything that costs time is prepared BEFORE the timed section
    words = r.get_random_words(limit=n_requests, maxLength=7)
    if not words or len(words) < n_requests:
        raise RuntimeError(f"Expected {n_requests} words, got {0 if not words else len(words)}")
    
    workload = [(random.choice(txt_files), words[i]) for i in range(n_requests)]

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
                if delay > 0:
                    time.sleep(delay)
                else:
                    logger.warning(f"Client isnt on time according to the rate {rate}")

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
    files = get_txt_files()
    run_experiment(files, rate=50, duration=20)
    run_experiment(files, rate=70, duration=20)
    run_experiment(files, rate=90, duration=20)
    run_experiment(files, rate=110, duration=20)
    run_experiment(files, rate=130, duration=20)
