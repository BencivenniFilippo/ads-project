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


def parse_hit_rate(value: str) -> int:
    try:
        hit_rate = int(value)
    except ValueError as exc:
        raise ValueError("HIT_RATE must be 0, 50, or 100") from exc
    if hit_rate not in (0, 50, 100):
        raise ValueError("HIT_RATE must be 0, 50, or 100")
    return hit_rate



def connect() -> rpyc.Connection:
    return rpyc.connect(HOST, PORT) # TCP (default) connection client -> LB

def conn_root_helper(conn: rpyc.Connection, func_name: str, *args, **kwargs):
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


def build_workload(vocab: dict, n: int, seed: int, hit_rate: int) -> list:
    rng = random.Random(seed)
    all_pairs = [(f, w) for f, words in vocab.items() for w in words]
    if hit_rate == 100:
        if not all_pairs:
            raise ValueError("Cannot create a 100% hit workload without any pairs")
        return [all_pairs[rng.randrange(len(all_pairs))]] * n

    unique_count = n if hit_rate == 0 else n // 2
    if hit_rate == 50 and unique_count < 2:
        raise ValueError("HIT_RATE=50 requires at least two unique pairs")
    if unique_count > len(all_pairs):
        raise ValueError(
            f"Asked for {unique_count} unique pairs, only {len(all_pairs)} exist"
        )

    base_workload = rng.sample(all_pairs, unique_count)
    if hit_rate == 0:
        return base_workload

    if n % 2 != 0:
        raise ValueError("HIT_RATE=50 requires an even request count")

    # Repeat balanced chunks instead of shuffling individual requests. This
    # keeps duplicate pairs apart while mixing cold and repeated requests.
    target_chunks = 4 if unique_count >= 400 else 2
    chunk_count = min(target_chunks, unique_count)
    chunk_size, remainder = divmod(unique_count, chunk_count)
    workload = []
    start = 0
    for chunk_index in range(chunk_count):
        # Put any remainder in the final chunk so it is never a small tail.
        size = chunk_size + (remainder if chunk_index == chunk_count - 1 else 0)
        chunk = base_workload[start:start + size]
        workload.extend(chunk)
        workload.extend(chunk)
        start += size
    return workload


def prewarm_cache(workload: list) -> None:
    file, word = workload[0]
    conn = connect()
    try:
        conn_root_helper(conn, "exposed_word_count", file, word)
    finally:
        conn.close()



import csv

def single_word_count_latency(
    req_id: int, file: str, word: str, scheduled: float, exp_start: float
) -> dict:
    conn = None
    start_time = time.perf_counter()
    try:
        conn = connect()
        # Reset start_time after connect() so successful RPC latency matches professor definition
        start_time = time.perf_counter()
        ans = conn_root_helper(conn, "exposed_word_count", file, word)
        end_time = time.perf_counter()
        return {
            "req_id": req_id,
            "scheduled_s": round(scheduled - exp_start, 4),
            "start_s": round(start_time - exp_start, 4),
            "end_s": round(end_time - exp_start, 4),
            "latency_ms": round((end_time - start_time) * 1000, 2),
            "lag_ms": round((start_time - scheduled) * 1000, 2),
            "status": "ok",
            "ans": ans,
            "error": "",
        }
    except Exception as exc:
        end_time = time.perf_counter()
        return {
            "req_id": req_id,
            "scheduled_s": round(scheduled - exp_start, 4),
            "start_s": round(start_time - exp_start, 4),
            "end_s": round(end_time - exp_start, 4),
            "latency_ms": round((end_time - start_time) * 1000, 2),
            "lag_ms": round((start_time - scheduled) * 1000, 2),
            "status": "error",
            "ans": None,
            "error": str(exc),
        }
    finally:
        if conn is not None:
            conn.close()


def write_trace_csv(trace_path: str, records: list[dict]) -> None:
    path = pathlib.Path(trace_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "req_id", "scheduled_s", "start_s", "end_s",
        "latency_ms", "lag_ms", "status", "error"
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted(records, key=lambda r: r["req_id"]))


def run_experiment(workload: list, rate: int, trace_file: str = "") -> dict:
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
        exp_start = time.perf_counter()
        for i, (file, word) in enumerate(workload):
            scheduled = exp_start + i / rate
            delay = scheduled - time.perf_counter()
            if delay >= 0:
                time.sleep(delay)
            elif i > 0:
                logger.warning(f"Client {i % rate} / {rate} isnt on time, lagging by {-delay:.3f} seconds")

            future = executor.submit(
                single_word_count_latency, i, file, word, scheduled, exp_start
            )
            futures.append(future)

    records = [f.result() for f in futures]
    if trace_file:
        write_trace_csv(trace_file, records)

    latencies_ms = [r["latency_ms"] for r in records if r["status"] == "ok"]
    lags_ms = [r["lag_ms"] for r in records if r["status"] == "ok"]
    errors = sum(1 for r in records if r["status"] != "ok")

    for r in records:
        if r["status"] != "ok":
            logger.error(f"Request {r['req_id']} at t={r['start_s']}s failed: {r['error']}")

    if len(latencies_ms) < 2:
        logger.error("Not enough successful requests to compute statistics")
        return {"rate": rate, "errors": errors, "n_requests": n_requests}

    avg_ms = round(sum(latencies_ms) / len(latencies_ms), 2)
    p99_ms = round(statistics.quantiles(latencies_ms, n=100, method="inclusive")[98], 2)
    max_lag_ms = round(max(lags_ms), 2)

    logger.info(
        "benchmark complete | target_rate=%d req/s | requests=%d | successful=%d "
        "| errors=%d | workers=%d | avg=%.2f ms | p99=%.2f ms | max_send_lag=%.2f ms",
        rate, n_requests, len(latencies_ms), errors, pool_size, avg_ms, p99_ms, max_lag_ms,
    )
    return {
        "rate": rate, "duration": duration, "avg_ms": avg_ms, "p99_ms": p99_ms,
        "max_lag_ms": max_lag_ms, "errors": errors, "n_requests": n_requests,
    }



if __name__ == "__main__":
    rate = int(os.getenv("RATE", "50"))
    duration = int(os.getenv("DURATION", "20"))
    seed = int(os.getenv("SEED", "42"))
    trace_out = os.getenv("TRACE_OUT", "")
    hit_rate = parse_hit_rate(os.getenv("HIT_RATE", "0"))
    bench_files = [f.strip() for f in os.getenv("BENCH_FILES", DEFAULT_BENCH_FILES).split(",") if f.strip()]
    logger.info(
        "config | rate=%d req/s | duration=%d s | requests=%d | seed=%d | "
        "hit_rate=%d%% | files=%s",
        rate,
        duration,
        rate * duration,
        seed,
        hit_rate,
        bench_files,
    )

    server_files = get_txt_files()
    missing = [f for f in bench_files if f not in server_files]
    if missing:
        raise SystemExit(f"Not known to the server: {missing}")

    n_requests = rate * duration
    workload = build_workload(
        build_vocabulary(bench_files), n_requests, seed, hit_rate
    )
    if hit_rate == 100:
        prewarm_cache(workload)
        
    trace_file = os.getenv("TRACE_FILE", "")
    res = run_experiment(workload, rate, trace_file=trace_file)

    keys = ("rate", "duration", "avg_ms", "p99_ms", "max_lag_ms", "errors", "n_requests")
    print(",".join(str(res.get(k, "")) for k in keys), flush=True)