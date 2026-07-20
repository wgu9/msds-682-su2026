from __future__ import annotations

import asyncio
import importlib.metadata
import re
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from demo05_app import LocalTripPublisher, create_app, create_local_app
from demo05_common import (
    PublishError,
    deterministic_requests,
    request_to_event,
)
from demo05_kafka import publish_one_event
from trip_event_contract import TripEventV1


COURSE_VERSIONS = {
    "confluent-kafka": "2.15.0",
    "fastapi": "0.139.0",
    "httpx2": "2.7.0",
    "pydantic": "2.13.4",
    "uvicorn": "0.50.0",
}

def test_course_runtime_uses_exact_pinned_versions() -> None:
    assert sys.version_info[:3] == (3, 11, 14)
    assert {
        package: importlib.metadata.version(package)
        for package in COURSE_VERSIONS
    } == COURSE_VERSIONS


def test_demo05_has_no_demo04_runtime_import() -> None:
    source_root = Path(__file__).resolve().parents[1]
    for source_path in source_root.glob("demo05*.py"):
        source = source_path.read_text(encoding="utf-8")
        assert "import demo04" not in source
        assert "from demo04" not in source


def test_handout_includes_every_expected_screenshot() -> None:
    package_root = Path(__file__).resolve().parents[1]
    packaged_readme = package_root / "README.md"
    if packaged_readme.is_file():
        handout_path = packaged_readme
        asset_root = package_root
    else:
        handout_path = package_root / "demo05.md"
        asset_root = package_root.parent

    handout = handout_path.read_text(encoding="utf-8")
    references = set(re.findall(r"\]\((assets/demo05/[^)]+)\)", handout))
    assert references
    assert {Path(reference).name[:7] for reference in references} == {
        "demo05a",
        "demo05b",
        "demo05c",
        "demo05d",
    }
    assert all((asset_root / reference).is_file() for reference in references)


def test_deterministic_requests_are_independent_and_reproducible() -> None:
    first = deterministic_requests(3, seed_offset=7)
    second = deterministic_requests(3, seed_offset=7)
    different = deterministic_requests(3, seed_offset=8)
    assert first == second
    assert first != different
    event = request_to_event(first[0])
    assert event.trip_id == first[0].request_id.replace("request_", "trip_")
    assert event.event_type == "trip_requested"
    assert event.driver_id is None
    assert event.fare is None


def test_local_app_returns_health_202_422_and_openapi() -> None:
    app = create_local_app()
    payload = deterministic_requests(1)[0].model_dump(mode="json")
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok", "mode": "local"}
        accepted = client.post("/trip-requests", json=payload)
        assert accepted.status_code == 202
        assert accepted.json()["delivery"] == "local"
        invalid = dict(payload)
        invalid["zone"] = "invalid"
        invalid["extra"] = True
        rejected = client.post("/trip-requests", json=invalid)
        assert rejected.status_code == 422
        assert "/trip-requests" in client.get("/openapi.json").json()["paths"]


def test_lifespan_closes_exactly_one_publisher() -> None:
    holder: dict[str, LocalTripPublisher] = {}

    async def factory() -> LocalTripPublisher:
        publisher = LocalTripPublisher("test.demo05")
        holder["publisher"] = publisher
        return publisher

    app = create_app(factory, mode="test")
    with TestClient(app) as client:
        payload = deterministic_requests(1)[0].model_dump(mode="json")
        assert client.post("/trip-requests", json=payload).status_code == 202
        assert not holder["publisher"].closed
        assert len(holder["publisher"].events) == 1
    assert holder["publisher"].closed


def test_publish_failure_maps_to_secret_free_503() -> None:
    class FailingPublisher:
        receipts: list = []

        async def publish(self, _event: TripEventV1):
            raise PublishError("private-broker-detail")

        async def close(self) -> None:
            return None

    async def factory() -> FailingPublisher:
        return FailingPublisher()

    app = create_app(factory, mode="test")
    with TestClient(app, raise_server_exceptions=False) as client:
        payload = deterministic_requests(1)[0].model_dump(mode="json")
        response = client.post("/trip-requests", json=payload)
    assert response.status_code == 503
    assert "private-broker-detail" not in response.text
    assert response.json() == {
        "detail": "The event publisher is temporarily unavailable."
    }


def test_native_aio_publish_contract_uses_key_value_and_ack() -> None:
    event = request_to_event(deterministic_requests(1)[0])
    calls: list[dict] = []

    class DeliveryMessage:
        def topic(self) -> str:
            return "test.demo05"

        def partition(self) -> int:
            return 2

        def offset(self) -> int:
            return 17

        def value(self) -> bytes:
            return b"\x00\x00\x00\x00\x03payload"

    class Producer:
        async def produce(self, topic: str, *, key: bytes, value: bytes):
            calls.append({"topic": topic, "key": key, "value": value})
            future = asyncio.get_running_loop().create_future()
            future.set_result(DeliveryMessage())
            return future

    async def serializer(_event: TripEventV1, _context: object) -> bytes:
        return b"\x00\x00\x00\x00\x03payload"

    async def run():
        from confluent_kafka.serialization import MessageField, SerializationContext

        return await publish_one_event(
            Producer(),
            serializer,
            SerializationContext("test.demo05", MessageField.VALUE),
            topic="test.demo05",
            event=event,
            delivery_timeout=1.0,
        )

    receipt = asyncio.run(run())
    assert calls == [
        {
            "topic": "test.demo05",
            "key": event.trip_id.encode("utf-8"),
            "value": b"\x00\x00\x00\x00\x03payload",
        }
    ]
    assert receipt.delivery == "broker_acknowledged"
    assert receipt.partition == 2
    assert receipt.offset == 17
    assert receipt.wire == {
        "magic_byte": 0,
        "schema_id": 3,
        "payload_bytes": 12,
        "avro_body_bytes": 7,
    }
