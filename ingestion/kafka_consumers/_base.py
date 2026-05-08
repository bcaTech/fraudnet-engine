"""Common Kafka consumer scaffolding.

Each topic-specific consumer subclasses :class:`KafkaConsumerBase` and
implements :meth:`handle`. The base class owns:

- a single ``AIOKafkaConsumer`` reading the topic
- a single ``AIOKafkaProducer`` for the dead-letter topic
- per-message try/except so one bad event can't kill the loop
- structured logging
- graceful shutdown on cancellation

Idempotency is the consumer's responsibility — most ``handle`` methods use
Cypher MERGE semantics so replays are safe.
"""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.logging import get_logger
from config.settings import get_settings

logger = get_logger(__name__)


class KafkaConsumerBase(ABC):
    """Subclasses set ``topic`` and ``group_id`` and implement ``handle``."""

    topic: str = ""
    group_id: str = "fraudnet-engine"

    def __init__(self) -> None:
        if not self.topic:
            raise RuntimeError(f"{type(self).__name__}.topic is required")
        self._settings = get_settings()
        self._consumer: AIOKafkaConsumer | None = None
        self._producer: AIOKafkaProducer | None = None
        self._stopped = asyncio.Event()
        self._dlq_topic = f"{self.topic}.dlq.v1"

    # -- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        bootstrap = self._settings.kafka_bootstrap_servers
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap,
            group_id=self.group_id,
            enable_auto_commit=True,
            auto_offset_reset="latest",
        )
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await self._consumer.start()
        await self._producer.start()
        logger.info("kafka.consumer.started", topic=self.topic, group=self.group_id)

    async def stop(self) -> None:
        self._stopped.set()
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception:  # noqa: BLE001
                pass
        if self._producer is not None:
            try:
                await self._producer.stop()
            except Exception:  # noqa: BLE001
                pass
        logger.info("kafka.consumer.stopped", topic=self.topic)

    async def run(self) -> None:
        """Consume forever; never raises out of the loop unless cancelled."""

        await self.start()
        try:
            assert self._consumer is not None
            async for msg in self._consumer:
                if self._stopped.is_set():
                    break
                event = self._deserialize(msg.value)
                if event is None:
                    await self._dlq(msg.value, reason="deserialise_failed")
                    continue
                try:
                    await self.handle(event)
                except Exception as exc:  # noqa: BLE001 — keep the loop alive
                    logger.error(
                        "kafka.handler.error",
                        topic=self.topic,
                        offset=msg.offset,
                        error=str(exc),
                    )
                    await self._dlq(msg.value, reason=str(exc))
        finally:
            await self.stop()

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _deserialize(raw: bytes | None) -> dict[str, Any] | None:
        if not raw:
            return None
        try:
            decoded: dict[str, Any] = json.loads(raw)
            return decoded
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    async def _dlq(self, raw: bytes | None, *, reason: str) -> None:
        if self._producer is None or raw is None:
            return
        try:
            await self._producer.send_and_wait(
                self._dlq_topic,
                json.dumps({"reason": reason, "raw": raw.decode("utf-8", errors="replace")}).encode("utf-8"),
            )
        except Exception as exc:  # noqa: BLE001 — DLQ failures are non-fatal
            logger.warning("kafka.dlq.send_failed", topic=self._dlq_topic, error=str(exc))

    # -- subclass contract ------------------------------------------------

    @abstractmethod
    async def handle(self, event: dict[str, Any]) -> None:
        """Process a single deserialised event."""
