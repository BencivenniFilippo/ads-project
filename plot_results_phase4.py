import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def read_results(path: Path) -> dict[int, dict[str, list[float]]]:
    grouped: dict[int, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    with path.open(newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            rate = int(row["rate"])
            grouped[rate]["avg_ms"].append(float(row["avg_ms"]))
            grouped[rate]["p99_ms"].append(float(row["p99_ms"]))
            grouped[rate]["errors"].append(float(row["errors"]))
            grouped[rate]["n_requests"].append(float(row["n_requests"]))
    return grouped


def plot_metric(
    rates: list[int],
    values: list[float],
    ylabel: str,
    title: str,
    output: Path,
) -> None:
    fig, ax = plt.subplots()
    ax.plot(rates, values, marker="o")
    ax.set_xlabel("Requests per second")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=Path, nargs="?")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--trace", type=Path, help="Path to per-request trace CSV")
    parser.add_argument("--failure-time", type=float, default=None, help="Timestamp (s) when server failed")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.trace and args.trace.exists():
        plot_trace(
            args.trace,
            args.output_dir / f"{args.trace.stem}_timeline.png",
            failure_time=args.failure_time,
        )

    if args.results and args.results.exists():
        grouped = read_results(args.results)
        rates = sorted(grouped)
        avg_values = [
            sum(grouped[rate]["avg_ms"]) / len(grouped[rate]["avg_ms"])
            for rate in rates
        ]
        p99_values = [
            sum(grouped[rate]["p99_ms"]) / len(grouped[rate]["p99_ms"])
            for rate in rates
        ]
        error_values = [
            sum(grouped[rate]["errors"]) / sum(grouped[rate]["n_requests"])
            for rate in rates
        ]

        args.output_dir.mkdir(parents=True, exist_ok=True)
        plot_metric(
            rates,
            avg_values,
            "Average latency (ms)",
            "Average execution latency",
            args.output_dir / "average_latency.png",
        )
        plot_metric(
            rates,
            p99_values,
            "p99 latency (ms)",
            "p99 execution latency",
            args.output_dir / "p99_latency.png",
        )
        plot_metric(
            rates,
            error_values,
            "Error rate",
            "Request error rate",
            args.output_dir / "error_rate.png",
        )

def plot_trace(
    trace_csv: Path,
    output: Path,
    failure_time = None,
    window: int = 25,
) -> None:
    ok_times, ok_latencies = [], []
    err_times, err_latencies = [], []

    with trace_csv.open(newline="") as f:
        for row in csv.DictReader(f):
            t = float(row["start_s"])
            lat = float(row["latency_ms"])
            if row["status"] == "ok":
                ok_times.append(t)
                ok_latencies.append(lat)
            else:
                err_times.append(t)
                err_latencies.append(lat)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(ok_times, ok_latencies, s=10, alpha=0.35, label="Request latency (ok)")

    if len(ok_latencies) >= window:
        rolling_t = ok_times[window - 1 :]
        rolling_avg = [
            sum(ok_latencies[i - window + 1 : i + 1]) / window
            for i in range(window - 1, len(ok_latencies))
        ]
        ax.plot(rolling_t, rolling_avg, color="tab:blue", linewidth=2, label=f"Rolling avg ({window} reqs)")

    if err_times:
        ax.scatter(err_times, err_latencies, color="tab:red", marker="x", s=35, label="Failed requests")

    if failure_time is not None:
        ax.axvline(failure_time, color="tab:red", linestyle="--", linewidth=1.5, label=f"Server failure (t={failure_time:.1f}s)")

    ax.set_xlabel("Time since benchmark start (s)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Per-Request Latency Over Time")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
