"""Unit-level tests for the Kafka consumer scaffolding.

Don't spin up Kafka. We exercise the deserialisation, dead-letter, and
handler-error path with a fake AIOKafkaConsumer/Producer pair so the
test runs in milliseconds and stays in the unit-test gate.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from ingestion.kafka_consumers._base import KafkaConsumerBase


class _FakeKafkaMessage:
    def __init__(self, value: bytes, offset: int = 0) -> None:
        self.value = value
        self.offset = offset
        self.topic = "test"
        self.partition = 0


class _FakeConsumer:
    def __init__(self, messages: list[_FakeKafkaMessage]) -> None:
        # Tests drive the iterator directly without calling start().
        self._iter = iter(messages)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration from None


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send_and_wait(self, topic: str, body: bytes) -> None:
        self.sent.append((topic, body))


class _RecordingConsumer(KafkaConsumerBase):
    """Minimal subclass: records handled events, optionally raises."""

    topic = "test.events.v1"
    group_id = "test-group"

    def __init__(self, *, raise_on: list[int] | None = None) -> None:
        super().__init__()
        self.handled: list[dict[str, Any]] = []
        self._raise_on = set(raise_on or [])

    async def handle(self, event: dict[str, Any]) -> None:
        self.handled.append(event)
        if event.get("seq") in self._raise_on:
            raise RuntimeError(f"boom at seq={event['seq']}")


def _consumer(messages: list[_FakeKafkaMessage], **kw):
    c = _RecordingConsumer(**kw)
    c._consumer = _FakeConsumer(messages)
    c._producer = _FakeProducer()
    # Avoid the start() call that would create real AIOKafka clients.
    c._stopped.set()  # so run() exits after the iterator is drained
    return c


def test_handle_processes_each_event() -> None:
    msgs = [
        _FakeKafkaMessage(json.dumps({"seq": 1, "kind": "p2p"}).encode()),
        _FakeKafkaMessage(json.dumps({"seq": 2, "kind": "cashout"}).encode()),
    ]
    c = _consumer(msgs)

    async def run():
        # Drive the loop manually — the real run() calls start()+stop().
        async for m in c._consumer:
            event = c._deserialize(m.value)
            assert event is not None
            await c.handle(event)

    asyncio.run(run())
    assert [e["seq"] for e in c.handled] == [1, 2]


def test_bad_payload_goes_to_dlq() -> None:
    msgs = [_FakeKafkaMessage(b"this is not json")]
    c = _consumer(msgs)

    async def run():
        async for m in c._consumer:
            event = c._deserialize(m.value)
            if event is None:
                await c._dlq(m.value, reason="deserialise_failed")
            else:
                await c.handle(event)

    asyncio.run(run())
    assert c.handled == []
    assert len(c._producer.sent) == 1
    topic, body = c._producer.sent[0]
    assert topic == "test.events.v1.dlq.v1"
    decoded = json.loads(body)
    assert decoded["reason"] == "deserialise_failed"
    assert decoded["raw"] == "this is not json"


def test_handler_exception_routes_to_dlq() -> None:
    msgs = [
        _FakeKafkaMessage(json.dumps({"seq": 1}).encode()),
        _FakeKafkaMessage(json.dumps({"seq": 99}).encode()),  # raises
        _FakeKafkaMessage(json.dumps({"seq": 2}).encode()),
    ]
    c = _consumer(msgs, raise_on=[99])

    async def run():
        # Mirror run()'s try/except around handle().
        async for m in c._consumer:
            event = c._deserialize(m.value)
            if event is None:
                continue
            try:
                await c.handle(event)
            except Exception as exc:  # noqa: BLE001
                await c._dlq(m.value, reason=str(exc))

    asyncio.run(run())
    # 1, 99, 2 were all delivered; 99 raised but didn't kill the loop.
    assert [e["seq"] for e in c.handled] == [1, 99, 2]
    # The bad message ended up on the DLQ.
    assert len(c._producer.sent) == 1
    topic, body = c._producer.sent[0]
    assert topic == "test.events.v1.dlq.v1"
    assert "boom at seq=99" in json.loads(body)["reason"]


def test_topic_required_on_subclass() -> None:
    class _BrokenConsumer(KafkaConsumerBase):
        topic = ""

        async def handle(self, event: dict[str, Any]) -> None:  # noqa: ARG002
            return None

    with pytest.raises(RuntimeError, match="topic is required"):
        _BrokenConsumer()
