from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from confluent_kafka.aio import AIOConsumer, AIOProducer

from demo02_producer_common import (
    TOPIC_NAME,
    event_key,
    make_trip_events,
    require_producer_config,
    safe_config_report,
    serialize_event,
    write_json_report,
)
from demo03_consumer_common import (
    default_group_id,
    message_to_record,
    require_consumer_config,
    safe_consumer_config_report,
)


async def produce_events(
    config: dict[str, str],
    *,
    count: int,
    seed: int,
    startup_delay: float,
    interval: float,
) -> list[dict[str, Any]]:
    """Produce a finite event set without blocking the asyncio event loop."""

    producer = AIOProducer(config)
    delivery_futures = []
    try:
        # Give the concurrently running consumer time to subscribe and poll.
        await asyncio.sleep(startup_delay)
        for event in make_trip_events(count, seed):
            delivery_future = await producer.produce(
                TOPIC_NAME,
                key=event_key(event),
                value=serialize_event(event),
            )
            delivery_futures.append(delivery_future)
            await asyncio.sleep(interval)
        await producer.flush()
        delivered_messages = await asyncio.gather(*delivery_futures)
        return [
            {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key": message.key().decode("utf-8") if message.key() else None,
            }
            for message in delivered_messages
        ]
    finally:
        await producer.close()


async def consume_events(
    config: dict[str, Any],
    *,
    expected_count: int,
    timeout: float,
) -> list[dict[str, Any]]:
    """Consume with AIOConsumer.poll() while allowing producer work to proceed."""

    consumer = AIOConsumer(config)
    records: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    await consumer.subscribe([TOPIC_NAME])
    try:
        while len(records) < expected_count and loop.time() < deadline:
            remaining = max(deadline - loop.time(), 0.0)
            message = await consumer.poll(timeout=min(1.0, remaining))
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Consumer error: {message.error()}")
            record = message_to_record(message)
            records.append(record)
            print(
                f"Async consumed {record['topic']}[{record['partition']}] "
                f"offset={record['offset']} key={record['key']}"
            )
    finally:
        await consumer.unsubscribe()
        await consumer.close()
    return records


async def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    """Run native asyncio producer and consumer clients concurrently."""

    producer_config = require_producer_config()
    group_id = args.group_id or default_group_id("demo03d-asyncio", args.run_id)
    consumer_config = require_consumer_config(
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        client_id="msds682-demo03d-aio-consumer",
    )
    producer_config = {**producer_config, "client.id": "msds682-demo03d-aio-producer"}

    producer_task = asyncio.create_task(
        produce_events(
            producer_config,
            count=args.count,
            seed=args.seed,
            startup_delay=args.startup_delay,
            interval=args.interval,
        )
    )
    consumer_task = asyncio.create_task(
        consume_events(
            consumer_config,
            expected_count=args.count,
            timeout=args.consumer_timeout,
        )
    )
    delivered, consumed = await asyncio.gather(producer_task, consumer_task)

    return {
        "demo": "demo03d_confluent_asyncio_produce_consume",
        "topic": TOPIC_NAME,
        "requested": args.count,
        "delivered": len(delivered),
        "consumed": len(consumed),
        "producer_connection": safe_config_report(producer_config),
        "consumer_connection": safe_consumer_config_report(consumer_config),
        "delivered_messages": delivered,
        "consumed_records": consumed,
    }


def main() -> dict[str, Any]:
    """Parse a finite asyncio demo, run it, and write secret-free evidence."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="lec3-demo03d")
    parser.add_argument("--group-id")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--seed", type=int, default=682)
    parser.add_argument("--startup-delay", type=float, default=2.0)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--consumer-timeout", type=float, default=15.0)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")

    report = asyncio.run(run_demo(args))
    output_file = write_json_report(
        args.run_id,
        "demo03d_confluent_asyncio_produce_consume",
        report,
    )
    print(json.dumps(report, indent=2))
    print(f"\nWrote {output_file}")
    if report["delivered"] != args.count or report["consumed"] != args.count:
        raise SystemExit("AsyncIO demo did not deliver and consume the requested count.")
    return report


if __name__ == "__main__":
    main()
