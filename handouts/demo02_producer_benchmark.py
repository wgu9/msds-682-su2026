from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path
from typing import Literal

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pydantic import BaseModel, Field


TOPIC_NAME = "msds682.demo01.trip-events.v1"


class TripEvent(BaseModel):
    # Local message value model for the ridesharing topic.
    # Kafka itself stores bytes; this model keeps our sample payloads consistent.
    trip_id: str
    event_type: Literal["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    rider_id: str
    event_time: str
    zone: str
    driver_id: str | None = None
    fare: float | None = Field(default=None, ge=0)


class JsonlTransport:
    """Tiny local transport that behaves like an append-only Kafka topic."""

    def __init__(self, run_id: str) -> None:
        self.topic_dir = Path("outputs") / "runs" / run_id / "topics"

    def topic_path(self, topic: str) -> Path:
        safe_name = topic.replace(".", "_")
        return self.topic_dir / f"{safe_name}.jsonl"

    def reset_topic(self, topic: str) -> None:
        path = self.topic_path(topic)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    def produce(self, topic: str, payload: dict) -> None:
        path = self.topic_path(topic)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

    def produce_many(self, topic: str, rows: list[dict]) -> None:
        for row in rows:
            self.produce(topic, row)


def make_trip_event(index: int, rng: random.Random) -> TripEvent:
    # Deterministic fake events: same seed + same count = same local topic log.
    event_types = ["trip_requested", "driver_matched", "trip_started", "trip_completed"]
    event_type = event_types[index % len(event_types)]
    trip_number = 981 + (index // len(event_types))
    return TripEvent(
        trip_id=f"trip_{trip_number}",
        event_type=event_type,
        rider_id=f"rider-{trip_number}",
        driver_id=None if event_type == "trip_requested" else f"driver-{rng.randint(1, 8):03d}",
        fare=round(rng.uniform(10.0, 90.0), 2) if event_type == "trip_completed" else None,
        zone=["north", "south", "west"][index % 3],
        event_time=f"2026-07-04T10:{index % 60:02d}:00Z",
    )


def run_strategy(strategy: str, count: int, batch_size: int, seed: int, run_id: str) -> dict:
    transport = JsonlTransport(run_id)
    # Each strategy starts from a clean local topic so the timing comparison is isolated.
    # The final JSONL file therefore shows the messages from the last strategy run.
    transport.reset_topic(TOPIC_NAME)
    rng = random.Random(seed)
    start = time.perf_counter()

    if strategy == "sync_style":
        # One produce call per event. Easy to understand, usually slower.
        for index in range(count):
            event = make_trip_event(index, rng)
            transport.produce(TOPIC_NAME, event.model_dump(exclude_none=True))
    elif strategy == "batched":
        # Accumulate events in memory, then append a batch at once.
        # This mirrors the producer batching idea without requiring a broker.
        buffer: list[dict] = []
        for index in range(count):
            event = make_trip_event(index, rng)
            buffer.append(event.model_dump(exclude_none=True))
            if len(buffer) >= batch_size:
                transport.produce_many(TOPIC_NAME, buffer)
                buffer = []
        if buffer:
            transport.produce_many(TOPIC_NAME, buffer)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    elapsed = max(time.perf_counter() - start, 0.000001)
    return {
        "strategy": strategy,
        "topic": TOPIC_NAME,
        "message_count": count,
        "batch_size": batch_size,
        "elapsed_seconds": round(elapsed, 6),
        "throughput_msg_per_sec": round(count / elapsed, 2),
    }


def main() -> list[dict]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="lec2-demo02")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=682)
    args = parser.parse_args()

    rows = [
        run_strategy("sync_style", args.count, args.batch_size, args.seed, args.run_id),
        run_strategy("batched", args.count, args.batch_size, args.seed, args.run_id),
    ]

    # CSV/PNG are the comparison artifacts. The JSONL topic log is the message sample.
    output_dir = Path("outputs") / "runs" / args.run_id / "demo02_producer_benchmark"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "producer_benchmark.csv"
    png_path = output_dir / "producer_benchmark.png"
    report_path = output_dir / "producer_benchmark_report.json"

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)

    ax = df.plot.bar(x="strategy", y="throughput_msg_per_sec", legend=False)
    ax.set_ylabel("messages/sec")
    ax.set_title("Local Producer Benchmark")
    plt.tight_layout()
    plt.savefig(png_path)
    plt.close()

    report = {
        "topic": TOPIC_NAME,
        "rows": rows,
        "artifacts": {
            "csv": str(csv_path),
            "png": str(png_path),
        },
    }
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return rows


if __name__ == "__main__":
    main()
