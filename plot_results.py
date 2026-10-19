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
    parser.add_argument("results", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

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


if __name__ == "__main__":
    main()
